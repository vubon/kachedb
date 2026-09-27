//! `kachedb-server` — Asynchronous Snapshot Persistence Engine (`dump.kdb`).
//!
//! Provides zero-lock background point-in-time snapshot persistence for KacheDB's
//! SIMD vector indexes and SwissTable in-memory key-value cache with hardware-accelerated
//! authenticated streaming encryption (AES-256-GCM / ChaCha20-Poly1305).
//!
//! Format Specification (v3):
//! - Magic Header: `b"KDB\x03"` (4 bytes)
//! - Created Timestamp: `u64` (8 bytes, Unix epoch seconds)
//! - Flags: `u32` (4 bytes, bit 0: encrypted, bits 1..3: cipher suite)
//! - If encrypted:
//!   - Salt: `[u8; 32]` (32 bytes)
//!   - Master Nonce: `[u8; 12]` (12 bytes)
//!   - Encrypted stream of 64 KiB chunks:
//!     - For each chunk: `chunk_len: u32`, `ciphertext + 16-byte auth tag`
//!     - EOF sentinel: `0u32` (4 bytes of 0)
//! - If unencrypted:
//!   - Payload stream directly:
//!     - Vector Section:
//!       - `num_indexes: u32`
//!       - For each index: `name_len: u16`, `name`, `dim: u32`, `num_entries: u32`
//!         - For each entry: `key_len: u16`, `key`, `dim: u32`, `vector: [f32]`,
//!           `has_payload: u8`, `payload`, `expire_at_secs: u32`, `tag_mask: u64`,
//!           `has_parent_key: u8`, `parent_key`
//!     - KV SwissTable Section:
//!       - `num_entries: u32`
//!       - For each entry: `key_hash: u64`, `val_len: u32`, `val: [u8]`, `expire_at_secs: u32`
//! - Checksum Trailer: `crc32: u32` (4 bytes, IEEE 802.3 CRC32 over all preceding bytes)

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use kachedb_core::{SlabClassType, SlabPool, resolve_slot_ptr};
use kachedb_hash::ShardedSwissTable;
use kachedb_vector::{VectorEntry, VectorIndexRegistry};
use thiserror::Error;

use crate::crypto::{
    ChunkDecryptor, ChunkEncryptor, CipherSuite, CryptoError, EncryptionKey, NONCE_LEN, SALT_LEN,
};

/// Magic bytes for KacheDB snapshot format v2 (Hybrid Context Engine enabled).
pub const KDB_SNAPSHOT_MAGIC_V2: [u8; 4] = *b"KDB\x02";

/// Magic bytes for KacheDB snapshot format v3 (Encrypted Snapshots at Rest).
pub const KDB_SNAPSHOT_MAGIC_V3: [u8; 4] = *b"KDB\x03";

/// Canonical active magic bytes for new snapshots.
#[allow(dead_code)]
pub const KDB_SNAPSHOT_MAGIC: [u8; 4] = KDB_SNAPSHOT_MAGIC_V3;

/// Flag indicating that snapshot payload is encrypted.
pub const FLAG_ENCRYPTED: u32 = 0x01;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Corrupt snapshot magic header")]
    CorruptMagic,
    #[error(
        "Snapshot CRC32 checksum mismatch: expected {expected:#010x}, calculated {calculated:#010x}"
    )]
    ChecksumMismatch { expected: u32, calculated: u32 },
    #[error("Unexpected end of snapshot file")]
    UnexpectedEof,
    #[error("Unsupported snapshot format version")]
    #[allow(dead_code)]
    UnsupportedVersion,
    #[error("Cryptographic error: {0}")]
    Crypto(#[from] CryptoError),
    #[error("Encryption key required to load encrypted snapshot")]
    EncryptionKeyRequired,
}

/// Configuration options for snapshot encryption-at-rest.
#[derive(Debug, Clone, Default)]
pub struct SnapshotEncryptionConfig {
    pub enabled: bool,
    pub cipher: CipherSuite,
    pub key: Option<String>,
    pub key_file: Option<PathBuf>,
}

impl SnapshotEncryptionConfig {
    /// Resolves an `EncryptionKey` from the configured raw file or user passphrase.
    pub fn resolve_key(&self, salt: &[u8; SALT_LEN]) -> Result<EncryptionKey, CryptoError> {
        if let Some(ref path) = self.key_file {
            crate::crypto::load_key_from_file(path, salt)
        } else if let Some(ref k) = self.key {
            crate::crypto::derive_key(k, salt)
        } else {
            Err(CryptoError::KeyDerivationFailed(
                "Snapshot encryption is enabled but neither key nor key_file was provided"
                    .to_string(),
            ))
        }
    }
}

/// Statistics reported after taking or restoring a snapshot.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotStats {
    pub timestamp_sec: u64,
    pub vector_indexes: usize,
    pub total_vectors: usize,
    pub total_kv_entries: usize,
    pub bytes_written: usize,
}

/// Internal streaming payload writer supporting transparent chunked AEAD encryption.
enum PayloadWriter<'a> {
    Plain {
        file: &'a mut File,
        hasher: &'a mut Hasher,
        buf: Vec<u8>,
        total_bytes: &'a mut usize,
    },
    Encrypted {
        file: &'a mut File,
        hasher: &'a mut Hasher,
        encryptor: Box<ChunkEncryptor>,
        buf: Vec<u8>,
        total_bytes: &'a mut usize,
    },
}

impl<'a> PayloadWriter<'a> {
    fn write_bytes(&mut self, data: &[u8]) -> Result<(), SnapshotError> {
        match self {
            PayloadWriter::Plain {
                file,
                hasher,
                buf,
                total_bytes,
            } => {
                buf.extend_from_slice(data);
                if buf.len() >= 128 * 1024 {
                    hasher.update(buf);
                    file.write_all(buf)?;
                    **total_bytes += buf.len();
                    buf.clear();
                }
                Ok(())
            }
            PayloadWriter::Encrypted {
                file,
                hasher,
                encryptor,
                buf,
                total_bytes,
            } => {
                buf.extend_from_slice(data);
                while buf.len() >= crate::crypto::ENCRYPTION_CHUNK_SIZE {
                    let encrypted =
                        encryptor.encrypt_chunk(&buf[..crate::crypto::ENCRYPTION_CHUNK_SIZE])?;
                    buf.drain(..crate::crypto::ENCRYPTION_CHUNK_SIZE);
                    let chunk_len = encrypted.len() as u32;
                    let len_bytes = chunk_len.to_le_bytes();
                    hasher.update(&len_bytes);
                    file.write_all(&len_bytes)?;
                    hasher.update(&encrypted);
                    file.write_all(&encrypted)?;
                    **total_bytes += 4 + encrypted.len();
                }
                Ok(())
            }
        }
    }

    fn finish(self) -> Result<(), SnapshotError> {
        match self {
            PayloadWriter::Plain {
                file,
                hasher,
                buf,
                total_bytes,
            } => {
                if !buf.is_empty() {
                    hasher.update(&buf);
                    file.write_all(&buf)?;
                    *total_bytes += buf.len();
                }
                Ok(())
            }
            PayloadWriter::Encrypted {
                file,
                hasher,
                mut encryptor,
                buf,
                total_bytes,
            } => {
                if !buf.is_empty() {
                    let encrypted = encryptor.encrypt_chunk(&buf)?;
                    let chunk_len = encrypted.len() as u32;
                    let len_bytes = chunk_len.to_le_bytes();
                    hasher.update(&len_bytes);
                    file.write_all(&len_bytes)?;
                    hasher.update(&encrypted);
                    file.write_all(&encrypted)?;
                    *total_bytes += 4 + encrypted.len();
                }
                // EOF sentinel: 0u32 indicates end of chunks
                let sentinel = 0u32.to_le_bytes();
                hasher.update(&sentinel);
                file.write_all(&sentinel)?;
                *total_bytes += 4;
                Ok(())
            }
        }
    }
}

/// Writes an atomic, point-in-time binary snapshot of all vector indexes and SwissTable keys.
pub fn save_snapshot(
    target_path: &Path,
    table: &ShardedSwissTable,
    vectors: &VectorIndexRegistry,
    now_sec: u32,
    encryption: Option<&SnapshotEncryptionConfig>,
) -> Result<SnapshotStats, SnapshotError> {
    let tmp_path = target_path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut total_bytes = 0;
    let mut hasher = Hasher::new();

    let is_encrypted = encryption.map(|e| e.enabled).unwrap_or(false);

    let (flags, encryptor_opt, salt_opt, nonce_opt) = if is_encrypted {
        let enc = encryption.unwrap();
        let cipher = enc.cipher;
        let mut salt = [0u8; SALT_LEN];
        crate::crypto::generate_random_bytes(&mut salt)?;
        let mut master_nonce = [0u8; NONCE_LEN];
        crate::crypto::generate_random_bytes(&mut master_nonce)?;
        let key = enc.resolve_key(&salt)?;
        let encryptor = ChunkEncryptor::new(cipher, &key, master_nonce)?;
        let flags = FLAG_ENCRYPTED | (cipher.flag_code() << 1);
        (flags, Some(encryptor), Some(salt), Some(master_nonce))
    } else {
        (0u32, None, None, None)
    };

    // 1. Magic Header & Metadata
    hasher.update(&KDB_SNAPSHOT_MAGIC_V3);
    file.write_all(&KDB_SNAPSHOT_MAGIC_V3)?;
    hasher.update(&ts.to_le_bytes());
    file.write_all(&ts.to_le_bytes())?;
    hasher.update(&flags.to_le_bytes());
    file.write_all(&flags.to_le_bytes())?;
    total_bytes += 16;

    if is_encrypted {
        let salt = salt_opt.unwrap();
        let nonce = nonce_opt.unwrap();
        hasher.update(&salt);
        file.write_all(&salt)?;
        hasher.update(&nonce);
        file.write_all(&nonce)?;
        total_bytes += SALT_LEN + NONCE_LEN;
    }

    let mut writer = if let Some(encryptor) = encryptor_opt {
        PayloadWriter::Encrypted {
            file: &mut file,
            hasher: &mut hasher,
            encryptor: Box::new(encryptor),
            buf: Vec::with_capacity(128 * 1024),
            total_bytes: &mut total_bytes,
        }
    } else {
        PayloadWriter::Plain {
            file: &mut file,
            hasher: &mut hasher,
            buf: Vec::with_capacity(128 * 1024),
            total_bytes: &mut total_bytes,
        }
    };

    // 2. Vector Indexes Section
    let index_snapshots = vectors.snapshot_all(now_sec);
    let num_indexes = index_snapshots.len() as u32;
    writer.write_bytes(&num_indexes.to_le_bytes())?;

    let mut total_vectors = 0;
    for (name, dim_opt, entries) in index_snapshots {
        let name_len = name.len() as u16;
        writer.write_bytes(&name_len.to_le_bytes())?;
        writer.write_bytes(&name)?;

        let dim = dim_opt.unwrap_or(0) as u32;
        writer.write_bytes(&dim.to_le_bytes())?;

        let num_entries = entries.len() as u32;
        writer.write_bytes(&num_entries.to_le_bytes())?;
        total_vectors += entries.len();

        for entry in entries {
            let key_len = entry.key.len() as u16;
            writer.write_bytes(&key_len.to_le_bytes())?;
            writer.write_bytes(&entry.key)?;

            let entry_dim = entry.vector.len() as u32;
            writer.write_bytes(&entry_dim.to_le_bytes())?;
            for &f in &entry.vector {
                writer.write_bytes(&f.to_ne_bytes())?;
            }

            if let Some(ref payload) = entry.payload {
                writer.write_bytes(&[1])?; // HasPayload = true
                let p_len = payload.len() as u32;
                writer.write_bytes(&p_len.to_le_bytes())?;
                writer.write_bytes(payload)?;
            } else {
                writer.write_bytes(&[0])?; // HasPayload = false
            }

            writer.write_bytes(&entry.expire_at_secs.to_le_bytes())?;
            writer.write_bytes(&entry.tag_mask.to_le_bytes())?;

            if let Some(ref parent_key) = entry.parent_key {
                writer.write_bytes(&[1])?; // HasParentKey = true
                let pk_len = parent_key.len() as u16;
                writer.write_bytes(&pk_len.to_le_bytes())?;
                writer.write_bytes(parent_key)?;
            } else {
                writer.write_bytes(&[0])?; // HasParentKey = false
            }
        }
    }

    // 3. KV SwissTable Section
    let mut total_kv = 0;
    let mut kv_items: Vec<(u64, &[u8], u32)> = Vec::new();
    for shard_idx in 0..table.shard_count() {
        let entries = table.snapshot_shard(shard_idx);
        for (key_hash, entry) in entries {
            let expire_at = entry.expire_at_secs;
            if expire_at > 0 && now_sec > 0 && now_sec >= expire_at {
                continue;
            }
            if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                let val_slice =
                    unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                kv_items.push((key_hash, val_slice, expire_at));
            }
        }
    }

    let num_kv_entries = kv_items.len() as u32;
    writer.write_bytes(&num_kv_entries.to_le_bytes())?;
    total_kv += kv_items.len();

    for (key_hash, val_slice, expire_at) in kv_items {
        writer.write_bytes(&key_hash.to_le_bytes())?;
        let val_len = val_slice.len() as u32;
        writer.write_bytes(&val_len.to_le_bytes())?;
        writer.write_bytes(val_slice)?;
        writer.write_bytes(&expire_at.to_le_bytes())?;
    }

    // 4. Finish payload writer
    writer.finish()?;

    // 5. Checksum Trailer
    let checksum = hasher.finalize();
    file.write_all(&checksum.to_le_bytes())?;
    total_bytes += 4;

    file.flush()?;
    file.sync_all()?;
    drop(file);

    // 6. Atomic Rename Swap
    std::fs::rename(&tmp_path, target_path)?;

    Ok(SnapshotStats {
        timestamp_sec: ts,
        vector_indexes: num_indexes as usize,
        total_vectors,
        total_kv_entries: total_kv,
        bytes_written: total_bytes,
    })
}

/// Hydrates in-memory vector indexes and SwissTable keys from `dump.kdb`.
pub fn load_snapshot(
    path: &Path,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    vectors: &VectorIndexRegistry,
    encryption: Option<&SnapshotEncryptionConfig>,
) -> Result<Option<SnapshotStats>, SnapshotError> {
    if !path.exists() {
        return Ok(None);
    }

    let mut file = File::open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    if data.len() < 20 {
        return Err(SnapshotError::UnexpectedEof);
    }

    // 1. Verify CRC32 Checksum Trailer
    let content_len = data.len() - 4;
    let expected_crc = u32::from_le_bytes([
        data[content_len],
        data[content_len + 1],
        data[content_len + 2],
        data[content_len + 3],
    ]);
    let mut hasher = Hasher::new();
    hasher.update(&data[..content_len]);
    let calculated_crc = hasher.finalize();

    if expected_crc != calculated_crc {
        return Err(SnapshotError::ChecksumMismatch {
            expected: expected_crc,
            calculated: calculated_crc,
        });
    }

    // 2. Validate Magic Header & Determine Encryption
    let magic = &data[0..4];
    let (ts, payload) = if magic == KDB_SNAPSHOT_MAGIC_V2 {
        let ts = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        (ts, std::borrow::Cow::Borrowed(&data[16..content_len]))
    } else if magic == KDB_SNAPSHOT_MAGIC_V3 {
        let ts = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let flags = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        let is_encrypted = (flags & FLAG_ENCRYPTED) != 0;

        if !is_encrypted {
            (ts, std::borrow::Cow::Borrowed(&data[16..content_len]))
        } else {
            if content_len < 16 + SALT_LEN + NONCE_LEN + 4 {
                return Err(SnapshotError::UnexpectedEof);
            }
            let mut salt = [0u8; SALT_LEN];
            salt.copy_from_slice(&data[16..16 + SALT_LEN]);

            let nonce_start = 16 + SALT_LEN;
            let mut master_nonce = [0u8; NONCE_LEN];
            master_nonce.copy_from_slice(&data[nonce_start..nonce_start + NONCE_LEN]);

            let cipher_code = (flags >> 1) & 0x07;
            let cipher = CipherSuite::from_flag_code(cipher_code)?;

            let enc_cfg = encryption.ok_or(SnapshotError::EncryptionKeyRequired)?;
            let key = enc_cfg.resolve_key(&salt)?;
            let mut decryptor = ChunkDecryptor::new(cipher, &key, master_nonce)?;

            let mut cursor = nonce_start + NONCE_LEN;
            let mut decrypted = Vec::with_capacity(content_len);

            loop {
                if cursor + 4 > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let chunk_len = u32::from_le_bytes([
                    data[cursor],
                    data[cursor + 1],
                    data[cursor + 2],
                    data[cursor + 3],
                ]) as usize;
                cursor += 4;

                if chunk_len == 0 {
                    // EOF sentinel reached
                    break;
                }

                if cursor + chunk_len > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }

                let chunk_ciphertext = &data[cursor..cursor + chunk_len];
                cursor += chunk_len;

                let plaintext = decryptor.decrypt_chunk(chunk_ciphertext)?;
                decrypted.extend_from_slice(&plaintext);
            }

            (ts, std::borrow::Cow::Owned(decrypted))
        }
    } else {
        return Err(SnapshotError::CorruptMagic);
    };

    // 3. Hydrate state from payload
    let (num_indexes, total_vectors, total_kv) = hydrate_payload(&payload, table, pool, vectors)?;

    log::info!(
        "Snapshot hydration: restored {} vector indexes ({} vectors), {} KV entries from {:?}",
        num_indexes,
        total_vectors,
        total_kv,
        path
    );

    Ok(Some(SnapshotStats {
        timestamp_sec: ts,
        vector_indexes: num_indexes,
        total_vectors,
        total_kv_entries: total_kv,
        bytes_written: data.len(),
    }))
}

/// Helper function to hydrate vector indexes and SwissTable keys from an unencrypted payload byte slice.
fn hydrate_payload(
    payload: &[u8],
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    vectors: &VectorIndexRegistry,
) -> Result<(usize, usize, usize), SnapshotError> {
    let mut cursor = 0;
    let content_len = payload.len();

    // 1. Vector Section
    if cursor + 4 > content_len {
        return Err(SnapshotError::UnexpectedEof);
    }
    let num_indexes = u32::from_le_bytes([
        payload[cursor],
        payload[cursor + 1],
        payload[cursor + 2],
        payload[cursor + 3],
    ]) as usize;
    cursor += 4;

    let mut total_vectors = 0;
    for _ in 0..num_indexes {
        if cursor + 10 > content_len {
            return Err(SnapshotError::UnexpectedEof);
        }
        let name_len = u16::from_le_bytes([payload[cursor], payload[cursor + 1]]) as usize;
        cursor += 2;
        if cursor + name_len + 8 > content_len {
            return Err(SnapshotError::UnexpectedEof);
        }
        let name = payload[cursor..cursor + name_len].to_vec();
        cursor += name_len;

        let dim = u32::from_le_bytes([
            payload[cursor],
            payload[cursor + 1],
            payload[cursor + 2],
            payload[cursor + 3],
        ]) as usize;
        cursor += 4;

        let num_entries = u32::from_le_bytes([
            payload[cursor],
            payload[cursor + 1],
            payload[cursor + 2],
            payload[cursor + 3],
        ]) as usize;
        cursor += 4;

        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            if cursor + 6 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let key_len = u16::from_le_bytes([payload[cursor], payload[cursor + 1]]) as usize;
            cursor += 2;
            if cursor + key_len + 4 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let key = payload[cursor..cursor + key_len].to_vec();
            cursor += key_len;

            let entry_dim = u32::from_le_bytes([
                payload[cursor],
                payload[cursor + 1],
                payload[cursor + 2],
                payload[cursor + 3],
            ]) as usize;
            cursor += 4;

            let vec_bytes_len = entry_dim * 4;
            if cursor + vec_bytes_len + 1 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let mut vector = Vec::with_capacity(entry_dim);
            for chunk in payload[cursor..cursor + vec_bytes_len].chunks_exact(4) {
                vector.push(f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            cursor += vec_bytes_len;

            let has_payload = payload[cursor];
            cursor += 1;
            let p_opt = if has_payload == 1 {
                if cursor + 4 > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let p_len = u32::from_le_bytes([
                    payload[cursor],
                    payload[cursor + 1],
                    payload[cursor + 2],
                    payload[cursor + 3],
                ]) as usize;
                cursor += 4;
                if cursor + p_len > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let p = payload[cursor..cursor + p_len].to_vec();
                cursor += p_len;
                Some(p)
            } else {
                None
            };

            if cursor + 12 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let expire_at_secs = u32::from_le_bytes([
                payload[cursor],
                payload[cursor + 1],
                payload[cursor + 2],
                payload[cursor + 3],
            ]);
            cursor += 4;

            let tag_mask = u64::from_le_bytes([
                payload[cursor],
                payload[cursor + 1],
                payload[cursor + 2],
                payload[cursor + 3],
                payload[cursor + 4],
                payload[cursor + 5],
                payload[cursor + 6],
                payload[cursor + 7],
            ]);
            cursor += 8;

            let has_parent = payload[cursor];
            cursor += 1;
            let parent_key = if has_parent == 1 {
                if cursor + 2 > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let pk_len = u16::from_le_bytes([payload[cursor], payload[cursor + 1]]) as usize;
                cursor += 2;
                if cursor + pk_len > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let pk = payload[cursor..cursor + pk_len].to_vec();
                cursor += pk_len;
                Some(pk)
            } else {
                None
            };

            entries.push(VectorEntry {
                key,
                vector,
                payload: p_opt,
                expire_at_secs,
                tag_mask,
                parent_key,
            });
        }

        total_vectors += entries.len();
        let dim_opt = if dim > 0 { Some(dim) } else { None };
        vectors.restore_snapshot(&name, dim_opt, entries);
    }

    // 2. Hydrate SwissTable KV Section
    let mut total_kv = 0;
    if cursor + 4 <= content_len {
        let num_kv = u32::from_le_bytes([
            payload[cursor],
            payload[cursor + 1],
            payload[cursor + 2],
            payload[cursor + 3],
        ]) as usize;
        cursor += 4;

        for _ in 0..num_kv {
            if cursor + 16 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let key_hash = u64::from_le_bytes([
                payload[cursor],
                payload[cursor + 1],
                payload[cursor + 2],
                payload[cursor + 3],
                payload[cursor + 4],
                payload[cursor + 5],
                payload[cursor + 6],
                payload[cursor + 7],
            ]);
            cursor += 8;

            let val_len = u32::from_le_bytes([
                payload[cursor],
                payload[cursor + 1],
                payload[cursor + 2],
                payload[cursor + 3],
            ]) as usize;
            cursor += 4;

            if cursor + val_len + 4 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let val_bytes = &payload[cursor..cursor + val_len];
            cursor += val_len;

            let expire_at = u32::from_le_bytes([
                payload[cursor],
                payload[cursor + 1],
                payload[cursor + 2],
                payload[cursor + 3],
            ]);
            cursor += 4;

            if let Some(class) = SlabClassType::for_size(val_len)
                && let Ok(block_id) = pool.allocate(class)
                && let Some(ptr) = unsafe { resolve_slot_ptr(block_id) }
            {
                unsafe {
                    std::ptr::copy_nonoverlapping(val_bytes.as_ptr(), ptr, val_len);
                }
                table.insert_with_ttl(key_hash, block_id, val_len as u32, expire_at);
                total_kv += 1;
            }
        }
    }

    Ok((num_indexes, total_vectors, total_kv))
}

/// Asynchronous background worker thread that writes periodic snapshots.
pub struct SnapshotWorker {
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    #[allow(dead_code)]
    trigger: Arc<AtomicBool>,
    #[allow(dead_code)]
    last_save_sec: Arc<AtomicU64>,
}

impl SnapshotWorker {
    /// Starts the background snapshotter thread.
    pub fn start(
        target_path: PathBuf,
        table: Arc<ShardedSwissTable>,
        vectors: &'static VectorIndexRegistry,
        interval_secs: u64,
        shutdown: Arc<AtomicBool>,
        encryption: Option<SnapshotEncryptionConfig>,
    ) -> Self {
        let trigger = Arc::new(AtomicBool::new(false));
        let last_save_sec = Arc::new(AtomicU64::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        ));

        let worker_shutdown = shutdown.clone();
        let worker_trigger = trigger.clone();
        let worker_last_save = last_save_sec.clone();

        let handle = thread::Builder::new()
            .name("kachedb-snapshotter".into())
            .spawn(move || {
                log::info!(
                    "Snapshot worker active: saving to {:?} every {}s (encryption: {})",
                    target_path,
                    interval_secs,
                    if encryption.as_ref().map(|e| e.enabled).unwrap_or(false) {
                        encryption.as_ref().unwrap().cipher.as_str()
                    } else {
                        "disabled"
                    }
                );

                while !worker_shutdown.load(Ordering::Relaxed) {
                    // Sleep in 500ms intervals to respond quickly to shutdown or trigger
                    for _ in 0..(interval_secs * 2).max(2) {
                        if worker_shutdown.load(Ordering::Relaxed)
                            || worker_trigger.load(Ordering::Relaxed)
                        {
                            break;
                        }
                        thread::sleep(Duration::from_millis(500));
                    }

                    if worker_shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    // Reset trigger flag
                    worker_trigger.store(false, Ordering::Relaxed);

                    let now_sec = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as u32;

                    match save_snapshot(&target_path, &table, vectors, now_sec, encryption.as_ref()) {
                        Ok(stats) => {
                            worker_last_save.store(stats.timestamp_sec, Ordering::Relaxed);
                            log::info!(
                                "Background snapshot complete: {} vectors across {} indexes, {} KV keys ({} bytes) written to {:?}",
                                stats.total_vectors,
                                stats.vector_indexes,
                                stats.total_kv_entries,
                                stats.bytes_written,
                                target_path
                            );
                        }
                        Err(e) => {
                            log::error!("Background snapshot failed: {e}");
                        }
                    }
                }

                // Final save on clean shutdown
                log::info!("Snapshot worker: performing final shutdown snapshot...");
                let now_sec = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as u32;
                if let Err(e) = save_snapshot(&target_path, &table, vectors, now_sec, encryption.as_ref()) {
                    log::error!("Final shutdown snapshot failed: {e}");
                } else {
                    log::info!("Final shutdown snapshot saved successfully to {:?}", target_path);
                }
            })
            .expect("failed to spawn snapshotter thread");

        Self {
            handle: Some(handle),
            shutdown,
            trigger,
            last_save_sec,
        }
    }

    /// Triggers an immediate asynchronous snapshot run.
    #[allow(dead_code)]
    pub fn trigger_save(&self) {
        self.trigger.store(true, Ordering::Relaxed);
    }

    /// Returns the Unix timestamp (seconds) of the last successful snapshot.
    #[allow(dead_code)]
    pub fn last_save_sec(&self) -> u64 {
        self.last_save_sec.load(Ordering::Relaxed)
    }

    /// Stops the snapshot worker and joins the thread.
    pub fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for SnapshotWorker {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_roundtrip_vectors_and_kv_unencrypted() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_snapshot_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        // Populate Vector Index
        let idx = vectors.get_or_create(b"hybrid_memory");
        let v1 = vec![0.6, 0.8];
        idx.insert(
            b"doc:1",
            &v1,
            Some(b"payload content"),
            None,
            0,
            0b011,
            Some(b"parent:1"),
        )
        .unwrap();

        // Populate SwissTable
        let val = b"hello_world";
        let class = SlabClassType::for_size(val.len()).unwrap();
        let block_id = pool.allocate(class).unwrap();
        let ptr = unsafe { resolve_slot_ptr(block_id) }.unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(val.as_ptr(), ptr, val.len());
        }
        let key_hash = 123456789u64;
        table.insert_with_ttl(key_hash, block_id, val.len() as u32, 0);

        // Save unencrypted snapshot
        let save_stats = save_snapshot(&path, &table, &vectors, 0, None).expect("save failed");
        assert_eq!(save_stats.vector_indexes, 1);
        assert_eq!(save_stats.total_vectors, 1);
        assert_eq!(save_stats.total_kv_entries, 1);
        assert!(save_stats.bytes_written > 0);
        assert!(path.exists());

        // Verify on-disk file: magic is KDB\x03 and flags have FLAG_ENCRYPTED == 0 (plaintext)
        let raw_bytes = std::fs::read(&path).unwrap();
        assert_eq!(&raw_bytes[0..4], &KDB_SNAPSHOT_MAGIC_V3);
        let flags =
            u32::from_le_bytes([raw_bytes[12], raw_bytes[13], raw_bytes[14], raw_bytes[15]]);
        assert_eq!(flags & FLAG_ENCRYPTED, 0);

        // Hydrate in new clean database instances without any key (None)
        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let load_stats = load_snapshot(&path, &new_table, &mut new_pool, &new_vectors, None)
            .expect("load failed")
            .expect("snapshot exists");

        assert_eq!(load_stats.vector_indexes, 1);
        assert_eq!(load_stats.total_vectors, 1);
        assert_eq!(load_stats.total_kv_entries, 1);

        // Verify restored vector
        let restored_idx = new_vectors.get(b"hybrid_memory").unwrap();
        let results = restored_idx.search(&v1, 1, 0.9, 0, 0b011).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"doc:1");
        assert_eq!(results[0].payload.as_deref(), Some(&b"payload content"[..]));
        assert_eq!(results[0].parent_key.as_deref(), Some(&b"parent:1"[..]));

        // Verify restored KV
        let lookup_entry = new_table
            .lookup(key_hash)
            .expect("key should exist in table");
        let lookup_ptr = unsafe { resolve_slot_ptr(lookup_entry.slab_block_id) }.unwrap();
        let lookup_val =
            unsafe { std::slice::from_raw_parts(lookup_ptr, lookup_entry.value_len as usize) };
        assert_eq!(lookup_val, b"hello_world");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_corrupt_checksum_detection() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_corrupt_snapshot_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let _ = save_snapshot(&path, &table, &vectors, 0, None).unwrap();

        // Corrupt a byte in the file
        let mut data = std::fs::read(&path).unwrap();
        data[8] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let mut pool = SlabPool::new(0, 1024 * 1024).unwrap();
        let res = load_snapshot(&path, &table, &mut pool, &vectors, None);
        assert!(matches!(res, Err(SnapshotError::ChecksumMismatch { .. })));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_encrypted_snapshot_roundtrip_aes_gcm() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_encrypted_aes_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let enc_config = SnapshotEncryptionConfig {
            enabled: true,
            cipher: CipherSuite::Aes256Gcm,
            key: Some("super_secure_vault_password_2026!".to_string()),
            key_file: None,
        };

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        // Populate index and KV
        let idx = vectors.get_or_create(b"aes_idx");
        let v = vec![0.1, 0.2, 0.3, 0.4];
        idx.insert(
            b"doc:aes",
            &v,
            Some(b"classified"),
            None,
            0,
            0b101,
            Some(b"parent:aes"),
        )
        .unwrap();

        let val = b"encrypted_kv_secret";
        let class = SlabClassType::for_size(val.len()).unwrap();
        let block_id = pool.allocate(class).unwrap();
        let ptr = unsafe { resolve_slot_ptr(block_id) }.unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(val.as_ptr(), ptr, val.len());
        }
        let key_hash = 999888777u64;
        table.insert_with_ttl(key_hash, block_id, val.len() as u32, 0);

        // Save with AES-256-GCM
        let save_stats = save_snapshot(&path, &table, &vectors, 0, Some(&enc_config))
            .expect("save encrypted failed");
        assert_eq!(save_stats.vector_indexes, 1);
        assert_eq!(save_stats.total_vectors, 1);
        assert_eq!(save_stats.total_kv_entries, 1);

        // Verify that raw file on disk is encrypted (magic is KDB\x03, flags indicate encrypted)
        let raw_bytes = std::fs::read(&path).unwrap();
        assert_eq!(&raw_bytes[0..4], &KDB_SNAPSHOT_MAGIC_V3);
        let flags =
            u32::from_le_bytes([raw_bytes[12], raw_bytes[13], raw_bytes[14], raw_bytes[15]]);
        assert_ne!(flags & FLAG_ENCRYPTED, 0);

        // Hydrate in new clean instance
        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let load_stats = load_snapshot(
            &path,
            &new_table,
            &mut new_pool,
            &new_vectors,
            Some(&enc_config),
        )
        .expect("load encrypted failed")
        .expect("snapshot exists");

        assert_eq!(load_stats.vector_indexes, 1);
        assert_eq!(load_stats.total_vectors, 1);
        assert_eq!(load_stats.total_kv_entries, 1);

        // Verify restored vector
        let restored_idx = new_vectors.get(b"aes_idx").unwrap();
        let results = restored_idx.search(&v, 1, 0.99, 0, 0b101).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"doc:aes");
        assert_eq!(results[0].payload.as_deref(), Some(&b"classified"[..]));
        assert_eq!(results[0].parent_key.as_deref(), Some(&b"parent:aes"[..]));

        // Verify restored KV
        let lookup_entry = new_table.lookup(key_hash).expect("key must exist");
        let lookup_ptr = unsafe { resolve_slot_ptr(lookup_entry.slab_block_id) }.unwrap();
        let lookup_val =
            unsafe { std::slice::from_raw_parts(lookup_ptr, lookup_entry.value_len as usize) };
        assert_eq!(lookup_val, b"encrypted_kv_secret");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_encrypted_snapshot_roundtrip_chacha20() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_encrypted_chacha_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let enc_config = SnapshotEncryptionConfig {
            enabled: true,
            cipher: CipherSuite::ChaCha20Poly1305,
            key: Some("chacha20_ultra_safe_key_passphrase".to_string()),
            key_file: None,
        };

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();

        let idx = vectors.get_or_create(b"chacha_idx");
        let v = vec![0.5, 0.5];
        idx.insert(b"doc:chacha", &v, None, None, 0, 0b001, None)
            .unwrap();

        let save_stats = save_snapshot(&path, &table, &vectors, 0, Some(&enc_config))
            .expect("save encrypted failed");
        assert_eq!(save_stats.vector_indexes, 1);

        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let load_stats = load_snapshot(
            &path,
            &new_table,
            &mut new_pool,
            &new_vectors,
            Some(&enc_config),
        )
        .expect("load encrypted failed")
        .expect("snapshot exists");

        assert_eq!(load_stats.total_vectors, 1);

        let restored_idx = new_vectors.get(b"chacha_idx").unwrap();
        let results = restored_idx.search(&v, 1, 0.99, 0, 0b001).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"doc:chacha");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_encrypted_snapshot_wrong_key_fails() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_wrong_key_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let save_config = SnapshotEncryptionConfig {
            enabled: true,
            cipher: CipherSuite::Aes256Gcm,
            key: Some("correct_master_key".to_string()),
            key_file: None,
        };

        let wrong_config = SnapshotEncryptionConfig {
            enabled: true,
            cipher: CipherSuite::Aes256Gcm,
            key: Some("wrong_master_key".to_string()),
            key_file: None,
        };

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        save_snapshot(&path, &table, &vectors, 0, Some(&save_config)).unwrap();

        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 1024 * 1024).unwrap();

        let res = load_snapshot(
            &path,
            &new_table,
            &mut new_pool,
            &new_vectors,
            Some(&wrong_config),
        );
        assert!(matches!(
            res,
            Err(SnapshotError::Crypto(CryptoError::AuthenticationFailed))
        ));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_encrypted_snapshot_missing_key_fails() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_missing_key_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let save_config = SnapshotEncryptionConfig {
            enabled: true,
            cipher: CipherSuite::Aes256Gcm,
            key: Some("correct_master_key".to_string()),
            key_file: None,
        };

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        save_snapshot(&path, &table, &vectors, 0, Some(&save_config)).unwrap();

        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 1024 * 1024).unwrap();

        // Attempt to load without encryption config
        let res = load_snapshot(&path, &new_table, &mut new_pool, &new_vectors, None);
        assert!(matches!(res, Err(SnapshotError::EncryptionKeyRequired)));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_backward_compatibility_v2_plaintext() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_v2_legacy_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        // Manually write a v2 snapshot file: Magic b"KDB\x02", ts, flags=0, 0 indexes, 0 KV, CRC32
        let mut file = File::create(&path).unwrap();
        let mut hasher = Hasher::new();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&KDB_SNAPSHOT_MAGIC_V2);
        bytes.extend_from_slice(&12345678u64.to_le_bytes()); // ts
        bytes.extend_from_slice(&0u32.to_le_bytes()); // flags
        bytes.extend_from_slice(&0u32.to_le_bytes()); // num_indexes
        bytes.extend_from_slice(&0u32.to_le_bytes()); // num_kv
        hasher.update(&bytes);
        file.write_all(&bytes).unwrap();
        let crc = hasher.finalize();
        file.write_all(&crc.to_le_bytes()).unwrap();
        file.flush().unwrap();
        drop(file);

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let mut pool = SlabPool::new(0, 1024 * 1024).unwrap();

        let stats = load_snapshot(&path, &table, &mut pool, &vectors, None)
            .expect("v2 legacy snapshot must load")
            .expect("stats present");
        assert_eq!(stats.timestamp_sec, 12345678);
        assert_eq!(stats.vector_indexes, 0);
        assert_eq!(stats.total_kv_entries, 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_encrypted_snapshot_with_key_file() {
        let dir = std::env::temp_dir();
        let key_file_path = dir.join(format!(
            "test_key_{}.bin",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let snapshot_path = dir.join(format!(
            "test_key_file_snapshot_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        // Write a 32-byte raw key file
        let raw_key = [42u8; 32];
        std::fs::write(&key_file_path, raw_key).unwrap();

        let enc_config = SnapshotEncryptionConfig {
            enabled: true,
            cipher: CipherSuite::Aes256Gcm,
            key: None,
            key_file: Some(key_file_path.clone()),
        };

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let idx = vectors.get_or_create(b"keyfile_idx");
        idx.insert(b"doc:kfile", &[1.0, 2.0], None, None, 0, 0, None)
            .unwrap();

        save_snapshot(&snapshot_path, &table, &vectors, 0, Some(&enc_config)).unwrap();

        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 1024 * 1024).unwrap();

        let stats = load_snapshot(
            &snapshot_path,
            &new_table,
            &mut new_pool,
            &new_vectors,
            Some(&enc_config),
        )
        .unwrap()
        .unwrap();

        assert_eq!(stats.total_vectors, 1);

        let _ = std::fs::remove_file(&key_file_path);
        let _ = std::fs::remove_file(&snapshot_path);
    }

    #[test]
    fn test_encrypted_snapshot_enabled_without_key_fails() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_no_key_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let bad_config = SnapshotEncryptionConfig {
            enabled: true,
            cipher: CipherSuite::Aes256Gcm,
            key: None,
            key_file: None,
        };

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let res = save_snapshot(&path, &table, &vectors, 0, Some(&bad_config));
        assert!(matches!(
            res,
            Err(SnapshotError::Crypto(CryptoError::KeyDerivationFailed(_)))
        ));
    }
}
