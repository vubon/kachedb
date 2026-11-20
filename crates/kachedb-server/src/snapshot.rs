//! `kachedb-server` — Asynchronous Snapshot Persistence Engine (`dump.kdb`).
//!
//! Provides background point-in-time snapshot persistence for KacheDB's
//! SIMD vector indexes and SwissTable in-memory key-value cache with hardware-accelerated
//! authenticated streaming encryption (AES-256-GCM / ChaCha20-Poly1305).
//!
//! Format Specification (v7):
//! - Magic Header: `b"KDB\x07"` (4 bytes)
//! - Created Timestamp: `u64` (8 bytes, Unix epoch seconds)
//! - Flags: `u32` (4 bytes, bit 0: encrypted, bits 1..3: cipher suite)
//! - If encrypted:
//!   - Salt: `[u8; 32]` (32 bytes)
//!   - Master Nonce: `[u8; 12]` (12 bytes)
//!   - Encrypted stream of 64 KiB chunks:
//!     - For each chunk: `chunk_len: u32`, `ciphertext + 16-byte auth tag`
//!     - EOF sentinel: `0u32` (4 bytes of 0)
//! - If unencrypted:
//!   - Payload stream directly (the decrypted stream uses the same layout):
//!     - Checkpoint flag, then generation (16 bytes) and sequence (u64) when present.
//!     - Vector Section:
//!       - `num_indexes: u32`
//!       - Each index begins with a kind byte. Flat is 0 and HNSW is 1.
//!       - Flat indexes: `name_len: u16`, `name`, `dim: u32`, `num_entries: u32`
//!         - Each slot begins with a presence byte. A present entry carries `key_len: u16`, `key`, `dim: u32`, `vector: [f32]`,
//!           `has_payload: u8`, `payload`, `expire_at_secs: u32`, `tag_mask: u64`,
//!           `has_parent_key: u8`, `parent_key`
//!       - HNSW indexes store parameters, node slots, graph edges, and RNG state.
//!     - KV SwissTable Section:
//!       - `num_entries: u32`
//!       - For each entry: `key_hash: u64`, `key_len: u16`, `key: [u8]`, `value_type: u8`,
//!         `val_len: u32`, `val: [u8]`, `expire_at_secs: u32`. Zero-length keys are named.
//! - Checksum Trailer: `crc32: u32` (4 bytes, IEEE 802.3 CRC32 over all preceding bytes)

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use kachedb_core::pool::SlotAllocator;
use kachedb_hash::{ShardedSwissTable, hash_key};
use kachedb_net::durability::{self, Checkpoint};
use kachedb_vector::{VectorEntry, VectorIndexRegistry, index::FlatVectorSnapshot};
use thiserror::Error;

use crate::crypto::{
    ChunkDecryptor, ChunkEncryptor, CipherSuite, CryptoError, EncryptionKey, NONCE_LEN, SALT_LEN,
};

#[path = "snapshot_hnsw.rs"]
mod hnsw;

/// Magic bytes for KacheDB snapshot format v2 (Hybrid Context Engine enabled).
pub const KDB_SNAPSHOT_MAGIC_V2: [u8; 4] = *b"KDB\x02";

/// Magic bytes for KacheDB snapshot format v3 (Encrypted Snapshots at Rest).
pub const KDB_SNAPSHOT_MAGIC_V3: [u8; 4] = *b"KDB\x03";

/// Magic bytes for KacheDB snapshot format v4 (Redis Hash Primitives + Type System).
pub const KDB_SNAPSHOT_MAGIC_V4: [u8; 4] = *b"KDB\x04";

/// Magic bytes for KacheDB snapshot format v5 (Keyspace SCAN + Key Name Persistence).
pub const KDB_SNAPSHOT_MAGIC_V5: [u8; 4] = *b"KDB\x05";

/// Magic bytes for KacheDB snapshot format v6 (all KV entries have original keys).
pub const KDB_SNAPSHOT_MAGIC_V6: [u8; 4] = *b"KDB\x06";

/// Magic bytes for snapshots with journal checkpoints and HNSW indexes.
pub const KDB_SNAPSHOT_MAGIC_V7: [u8; 4] = *b"KDB\x07";

/// Canonical active magic bytes for new snapshots.
pub const KDB_SNAPSHOT_MAGIC: [u8; 4] = KDB_SNAPSHOT_MAGIC_V7;

/// Flag indicating that snapshot payload is encrypted.
pub const FLAG_ENCRYPTED: u32 = 0x01;

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Snapshot slab error: {0}")]
    Slab(#[from] kachedb_core::CoreError),
    #[error("Invalid snapshot: {0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Aof(#[from] crate::aof::AofError),
    #[error("Snapshot checkpoint failed: {0}")]
    Admission(#[from] durability::AdmissionError),
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
    #[error(
        "Snapshot lacks original key bytes; recover from a complete AOF or migrate the snapshot before startup"
    )]
    MissingKeyBytes,
    #[error(
        "Legacy snapshot key digest does not match its stored bytes; recover from a complete AOF or migrate the snapshot before startup"
    )]
    LegacyKeyDigestMismatch,
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
    pub checkpoint: Option<Checkpoint>,
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

struct SnapshotTempFile {
    path: PathBuf,
}

impl SnapshotTempFile {
    fn create(target: &Path) -> Result<(Self, File), SnapshotError> {
        let mut nonce = [0; 8];
        crate::crypto::generate_random_bytes(&mut nonce)?;
        let mut temp = target.as_os_str().to_owned();
        temp.push(format!(".{:016x}.tmp", u64::from_le_bytes(nonce)));
        let path = PathBuf::from(temp);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok((Self { path }, file))
    }
}

impl Drop for SnapshotTempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
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
    let (flat, graphs, kv_items, checkpoint) = durability::with_commit_fence(|| {
        let capture_at = if durability::dispatch().is_some() {
            0
        } else {
            now_sec
        };
        let flat = vectors.snapshot_flat_slots(capture_at);
        let graphs: Vec<_> = vectors
            .hnsw_indexes()
            .into_iter()
            .map(|(name, index)| {
                let graph = index.snapshot_graph();
                (name, index, graph)
            })
            .collect();

        let mut items = Vec::new();
        for shard in 0..table.shard_count() {
            // Workers keep published slots alive until unlinking under the shard write lock.
            let entries = unsafe { table.snapshot_shard_with_payloads(shard, capture_at) }?;
            if entries.iter().any(|entry| !entry.has_key) {
                return Err(SnapshotError::MissingKeyBytes);
            }
            items.extend(entries);
        }
        let checkpoint = durability::dispatch()
            .map(|writer| writer.checkpoint())
            .transpose()?;
        Ok::<_, SnapshotError>((flat, graphs, items, checkpoint))
    })?;
    let checkpoint = checkpoint
        .map(|(checkpoint, synced)| {
            synced
                .recv()
                .map_err(|_| crate::aof::AofError::WriterStopped)?;
            Ok::<_, SnapshotError>(checkpoint)
        })
        .transpose()?;

    let (temporary, mut file) = SnapshotTempFile::create(target_path)?;

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
    hasher.update(&KDB_SNAPSHOT_MAGIC);
    file.write_all(&KDB_SNAPSHOT_MAGIC)?;
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

    writer.write_bytes(&[u8::from(checkpoint.is_some())])?;
    if let Some(checkpoint) = checkpoint {
        writer.write_bytes(&checkpoint.generation)?;
        writer.write_bytes(&checkpoint.sequence.to_le_bytes())?;
    }

    // 2. Vector Indexes Section
    let num_indexes = u32::try_from(flat.len() + graphs.len())
        .map_err(|_| SnapshotError::Invalid("too many indexes"))?;
    writer.write_bytes(&num_indexes.to_le_bytes())?;

    let mut total_vectors = 0;
    for (name, index, graph) in graphs {
        writer.write_bytes(&[1])?;
        total_vectors += hnsw::write_index(&mut writer, &name, &index, &graph)?;
    }
    for FlatVectorSnapshot {
        name,
        dimension,
        slots,
    } in flat
    {
        writer.write_bytes(&[0])?;
        let name_len = u16::try_from(name.len())
            .map_err(|_| SnapshotError::Invalid("index name is too long"))?;
        writer.write_bytes(&name_len.to_le_bytes())?;
        writer.write_bytes(&name)?;

        let dim = u32::try_from(dimension.unwrap_or(0))
            .map_err(|_| SnapshotError::Invalid("index dimension is too large"))?;
        writer.write_bytes(&dim.to_le_bytes())?;

        let num_entries = u32::try_from(slots.len())
            .map_err(|_| SnapshotError::Invalid("too many vector slots"))?;
        writer.write_bytes(&num_entries.to_le_bytes())?;
        for slot in slots {
            writer.write_bytes(&[u8::from(slot.is_some())])?;
            let Some(entry) = slot else { continue };
            total_vectors += 1;
            let key_len = u16::try_from(entry.key.len())
                .map_err(|_| SnapshotError::Invalid("vector key is too long"))?;
            writer.write_bytes(&key_len.to_le_bytes())?;
            writer.write_bytes(&entry.key)?;

            let entry_dim = entry.vector.len() as u32;
            writer.write_bytes(&entry_dim.to_le_bytes())?;
            for &f in &entry.vector {
                writer.write_bytes(&f.to_le_bytes())?;
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
                let pk_len = u16::try_from(parent_key.len())
                    .map_err(|_| SnapshotError::Invalid("vector parent key is too long"))?;
                writer.write_bytes(&pk_len.to_le_bytes())?;
                writer.write_bytes(parent_key)?;
            } else {
                writer.write_bytes(&[0])?; // HasParentKey = false
            }
        }
    }

    // 3. KV SwissTable Section
    let num_kv_entries =
        u32::try_from(kv_items.len()).map_err(|_| SnapshotError::Invalid("too many KV entries"))?;
    writer.write_bytes(&num_kv_entries.to_le_bytes())?;

    for item in kv_items {
        writer.write_bytes(&item.key_hash.to_le_bytes())?;
        let key_len = item.key_bytes.len() as u16;
        writer.write_bytes(&key_len.to_le_bytes())?;
        writer.write_bytes(&item.key_bytes)?;
        writer.write_bytes(&[item.value_type])?;
        let val_len = item.val_bytes.len() as u32;
        writer.write_bytes(&val_len.to_le_bytes())?;
        writer.write_bytes(&item.val_bytes)?;
        writer.write_bytes(&item.expire_at.to_le_bytes())?;
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
    std::fs::rename(&temporary.path, target_path)?;
    let parent = target_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;

    Ok(SnapshotStats {
        timestamp_sec: ts,
        vector_indexes: num_indexes as usize,
        total_vectors,
        total_kv_entries: num_kv_entries as usize,
        bytes_written: total_bytes,
        checkpoint,
    })
}

/// Hydrates in-memory vector indexes and SwissTable keys from `dump.kdb`.
pub fn load_snapshot(
    path: &Path,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
    encryption: Option<&SnapshotEncryptionConfig>,
) -> Result<Option<SnapshotStats>, SnapshotError> {
    load_snapshot_for_recovery(path, table, pool, vectors, encryption, |_| Ok(true))
}

pub fn load_snapshot_for_recovery(
    path: &Path,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
    encryption: Option<&SnapshotEncryptionConfig>,
    select: impl FnOnce(Option<Checkpoint>) -> Result<bool, SnapshotError>,
) -> Result<Option<SnapshotStats>, SnapshotError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
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
    let (version, ts, payload) = if magic == KDB_SNAPSHOT_MAGIC_V2 {
        let ts = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        (2u8, ts, std::borrow::Cow::Borrowed(&data[16..content_len]))
    } else if magic == KDB_SNAPSHOT_MAGIC_V3
        || magic == KDB_SNAPSHOT_MAGIC_V4
        || magic == KDB_SNAPSHOT_MAGIC_V5
        || magic == KDB_SNAPSHOT_MAGIC_V6
        || magic == KDB_SNAPSHOT_MAGIC_V7
    {
        let version = if magic == KDB_SNAPSHOT_MAGIC_V7 {
            7u8
        } else if magic == KDB_SNAPSHOT_MAGIC_V6 {
            6u8
        } else if magic == KDB_SNAPSHOT_MAGIC_V5 {
            5u8
        } else if magic == KDB_SNAPSHOT_MAGIC_V4 {
            4u8
        } else {
            3u8
        };
        let ts = u64::from_le_bytes([
            data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
        ]);
        let flags = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        let is_encrypted = (flags & FLAG_ENCRYPTED) != 0;

        if !is_encrypted {
            (
                version,
                ts,
                std::borrow::Cow::Borrowed(&data[16..content_len]),
            )
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

            (version, ts, std::borrow::Cow::Owned(decrypted))
        }
    } else {
        return Err(SnapshotError::CorruptMagic);
    };

    let (checkpoint, payload) = if version >= 7 {
        read_checkpoint(&payload)?
    } else {
        (None, payload.as_ref())
    };
    if !select(checkpoint)? {
        return Ok(None);
    }
    let (num_indexes, total_vectors, total_kv) =
        hydrate_payload(payload, version, table, pool, vectors)?;

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
        checkpoint,
    }))
}

fn read_checkpoint(payload: &[u8]) -> Result<(Option<Checkpoint>, &[u8]), SnapshotError> {
    match payload.split_first() {
        Some((0, payload)) => Ok((None, payload)),
        Some((1, payload)) if payload.len() >= 24 => Ok((
            Some(Checkpoint {
                generation: payload[..16].try_into().unwrap(),
                sequence: u64::from_le_bytes(payload[16..24].try_into().unwrap()),
            }),
            &payload[24..],
        )),
        Some((flag, _)) if *flag > 1 => Err(SnapshotError::Invalid("invalid checkpoint flag")),
        _ => Err(SnapshotError::UnexpectedEof),
    }
}

fn read_presence(payload: &[u8], cursor: &mut usize) -> Result<bool, SnapshotError> {
    let flag = payload.get(*cursor).ok_or(SnapshotError::UnexpectedEof)?;
    *cursor += 1;
    match flag {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(SnapshotError::Invalid("invalid snapshot presence flag")),
    }
}

/// Helper function to hydrate vector indexes and SwissTable keys from an unencrypted payload byte slice.
fn hydrate_payload(
    payload: &[u8],
    version: u8,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
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
        if version >= 7 {
            let kind = payload.get(cursor).ok_or(SnapshotError::UnexpectedEof)?;
            cursor += 1;
            match kind {
                0 => {}
                1 => {
                    let (remaining, count) = hnsw::read_index(&payload[cursor..], vectors)?;
                    cursor = content_len - remaining.len();
                    total_vectors += count;
                    continue;
                }
                _ => return Err(SnapshotError::Invalid("unknown vector index kind")),
            }
        }
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

        let min_slot_size = if version >= 7 { 1 } else { 20 };
        if num_entries > (content_len - cursor) / min_slot_size {
            return Err(SnapshotError::UnexpectedEof);
        }
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(num_entries)
            .map_err(|_| SnapshotError::Invalid("cannot reserve flat vector slots"))?;
        for _ in 0..num_entries {
            if version >= 7 {
                let flag = read_presence(payload, &mut cursor)?;
                if !flag {
                    slots.push(None);
                    continue;
                }
            }
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

            if entry_dim != dim {
                return Err(SnapshotError::Invalid("vector has the wrong dimension"));
            }
            let vec_bytes_len = entry_dim * 4;
            if cursor + vec_bytes_len + 1 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let decode = if version >= 7 {
                f32::from_le_bytes
            } else {
                f32::from_ne_bytes
            };
            let vector = payload[cursor..cursor + vec_bytes_len]
                .as_chunks::<4>()
                .0
                .iter()
                .copied()
                .map(decode)
                .collect();
            cursor += vec_bytes_len;

            let p_opt = if read_presence(payload, &mut cursor)? {
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

            if cursor + 13 > content_len {
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

            let parent_key = if read_presence(payload, &mut cursor)? {
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

            if key.is_empty() || dim == 0 {
                return Err(SnapshotError::Invalid("empty flat vector key or dimension"));
            }
            slots.push(Some(VectorEntry {
                key,
                vector,
                payload: p_opt,
                expire_at_secs,
                tag_mask,
                parent_key,
            }));
            total_vectors += 1;
        }

        let dimension = (dim != 0).then_some(dim);
        if !vectors.restore_flat_slots(FlatVectorSnapshot {
            name,
            dimension,
            slots,
        }) {
            return Err(SnapshotError::Invalid("duplicate flat index or key"));
        }
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

        if version < 5 && num_kv > 0 {
            return Err(SnapshotError::MissingKeyBytes);
        }
        for _ in 0..num_kv {
            if cursor + 8 + 2 > content_len {
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

            let key_len = u16::from_le_bytes([payload[cursor], payload[cursor + 1]]) as usize;
            cursor += 2;
            if cursor + key_len + 1 + 4 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let key_bytes = &payload[cursor..cursor + key_len];
            cursor += key_len;
            if version == 5 {
                if key_bytes.is_empty() {
                    return Err(SnapshotError::MissingKeyBytes);
                }
                if hash_key(key_bytes) != key_hash {
                    return Err(SnapshotError::LegacyKeyDigestMismatch);
                }
            }

            let value_type = payload[cursor];
            cursor += 1;

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

            crate::aof_recovery::restore_value(
                table, pool, key_hash, key_bytes, value_type, val_bytes, expire_at,
            )?;
            total_kv += 1;
        }
    } else if version >= 7 {
        return Err(SnapshotError::UnexpectedEof);
    }
    if cursor != content_len {
        return Err(SnapshotError::Invalid("trailing snapshot bytes"));
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

                log::info!("Snapshot worker: background thread stopped");
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
    use kachedb_core::{SlabClassType, SlabPool, resolve_slot_ptr};
    use kachedb_hash::{HashSlotFrame, VALUE_TYPE_HASH, VALUE_TYPE_STRING};

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
        table.insert_with_key(key_hash, b"snapshot-key", block_id, val.len() as u32);

        // Save unencrypted snapshot
        let save_stats = save_snapshot(&path, &table, &vectors, 0, None).expect("save failed");
        assert_eq!(save_stats.vector_indexes, 1);
        assert_eq!(save_stats.total_vectors, 1);
        assert_eq!(save_stats.total_kv_entries, 1);
        assert!(save_stats.bytes_written > 0);
        assert!(path.exists());

        // Verify on-disk file: magic is KDB\x06 and flags have FLAG_ENCRYPTED == 0 (plaintext)
        let raw_bytes = std::fs::read(&path).unwrap();
        assert_eq!(&raw_bytes[0..4], &KDB_SNAPSHOT_MAGIC_V7);
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
        table.insert_with_key(key_hash, b"snapshot-key", block_id, val.len() as u32);

        // Save with AES-256-GCM
        let save_stats = save_snapshot(&path, &table, &vectors, 0, Some(&enc_config))
            .expect("save encrypted failed");
        assert_eq!(save_stats.vector_indexes, 1);
        assert_eq!(save_stats.total_vectors, 1);
        assert_eq!(save_stats.total_kv_entries, 1);

        // Verify that raw file on disk is encrypted (magic is KDB\x06, flags indicate encrypted)
        let raw_bytes = std::fs::read(&path).unwrap();
        assert_eq!(&raw_bytes[0..4], &KDB_SNAPSHOT_MAGIC_V7);
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

    #[test]
    fn test_snapshot_roundtrip_with_redis_hash_entries() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_hash_snapshot_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        // 1. Insert a string key
        let str_hash = 111222333u64;
        let str_val = b"hello string value";
        let str_class = SlabClassType::for_size(str_val.len()).unwrap();
        let str_block_id = pool.allocate(str_class).unwrap();
        let str_ptr = unsafe { resolve_slot_ptr(str_block_id).unwrap() };
        unsafe {
            std::ptr::copy_nonoverlapping(str_val.as_ptr(), str_ptr, str_val.len());
        }
        table
            .insert_typed_with_key(
                str_hash,
                b"string-key",
                str_block_id,
                str_val.len() as u32,
                0,
                VALUE_TYPE_STRING,
            )
            .unwrap();

        // 2. Insert a hash key
        let hash_key = 444555666u64;
        let hash_class = SlabClassType::AppSmall;
        let hash_block_id = pool.allocate(hash_class).unwrap();
        let hash_ptr = unsafe { resolve_slot_ptr(hash_block_id).unwrap() };
        let slot = unsafe { std::slice::from_raw_parts_mut(hash_ptr, hash_class.slot_bytes()) };
        HashSlotFrame::init(slot).unwrap();
        HashSlotFrame::upsert(slot, b"field1", b"val1").unwrap();
        HashSlotFrame::upsert(slot, b"field2", b"longer_val_2").unwrap();
        table
            .insert_typed_with_key(
                hash_key,
                b"hash-key",
                hash_block_id,
                hash_class.slot_bytes() as u32,
                0,
                VALUE_TYPE_HASH,
            )
            .unwrap();

        // 3. Save snapshot
        let save_stats = save_snapshot(&path, &table, &vectors, 0, None).expect("save failed");
        assert_eq!(save_stats.total_kv_entries, 2);

        // 4. Load snapshot into clean table & pool
        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let load_stats = load_snapshot(&path, &new_table, &mut new_pool, &new_vectors, None)
            .expect("load failed")
            .expect("stats present");
        assert_eq!(load_stats.total_kv_entries, 2);

        // 5. Verify string entry
        let str_entry = new_table.lookup(str_hash).expect("string key must exist");
        assert_eq!(str_entry.value_type, VALUE_TYPE_STRING);
        let ptr = unsafe { resolve_slot_ptr(str_entry.slab_block_id).unwrap() };
        let restored_str = unsafe { std::slice::from_raw_parts(ptr, str_entry.value_len as usize) };
        assert_eq!(restored_str, str_val);

        // 6. Verify hash entry
        let hash_entry = new_table.lookup(hash_key).expect("hash key must exist");
        assert_eq!(hash_entry.value_type, VALUE_TYPE_HASH);
        let ptr = unsafe { resolve_slot_ptr(hash_entry.slab_block_id).unwrap() };
        let restored_slot =
            unsafe { std::slice::from_raw_parts(ptr, hash_entry.value_len as usize) };
        assert_eq!(
            HashSlotFrame::get(restored_slot, b"field1"),
            Some(b"val1".as_slice())
        );
        assert_eq!(
            HashSlotFrame::get(restored_slot, b"field2"),
            Some(b"longer_val_2".as_slice())
        );
        assert_eq!(HashSlotFrame::get(restored_slot, b"field3"), None);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_shutdown_barrier_preserves_kv_snapshot() {
        use kachedb_net::ShutdownBarrier;

        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_shutdown_barrier_{}.kdb",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let table = Arc::new(ShardedSwissTable::new());
        let vectors = VectorIndexRegistry::new();
        let barrier = Arc::new(ShutdownBarrier::new());

        let table_clone = table.clone();
        let barrier_clone = barrier.clone();

        // Worker thread owning SlabPool
        let worker_handle = thread::spawn(move || {
            let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

            // Insert string key
            let block_id = pool.allocate(SlabClassType::AppSmall).unwrap();
            let ptr = unsafe { resolve_slot_ptr(block_id).unwrap() };
            let val = b"worker_thread_payload";
            unsafe {
                std::ptr::copy_nonoverlapping(val.as_ptr(), ptr, val.len());
            }
            let key_hash = 999888777u64;
            table_clone.insert_with_key(key_hash, b"worker-key", block_id, val.len() as u32);

            // Simulate event loop stopping on shutdown, then waiting at barrier
            barrier_clone.wait_worker();

            // When released, pool is dropped here
            drop(pool);
        });

        // Main thread: wait for worker to pause at barrier
        assert!(barrier.wait_all_stopped(1, Duration::from_secs(2)));

        // Take snapshot while worker is at barrier with pool alive
        let save_stats = save_snapshot(&path, &table, &vectors, 0, None).expect("save failed");
        assert_eq!(save_stats.total_kv_entries, 1);

        // Release worker so it can exit
        barrier.release();
        worker_handle.join().unwrap();

        // Now verify hydration from on-disk snapshot into clean database
        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let load_stats = load_snapshot(&path, &new_table, &mut new_pool, &new_vectors, None)
            .expect("load failed")
            .expect("stats present");
        assert_eq!(load_stats.total_kv_entries, 1);

        let entry = new_table.lookup(999888777u64).expect("key must exist");
        let ptr = unsafe { resolve_slot_ptr(entry.slab_block_id).unwrap() };
        let restored = unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
        assert_eq!(restored, b"worker_thread_payload");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_snapshot_v6_preserves_collisions_and_empty_key_and_rejects_v4_anonymous_key() {
        use std::collections::HashSet;

        let dir = std::env::temp_dir();
        let path_v6 = dir.join(format!(
            "test_v6_scan_keys_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let collision = 42;
        let first = [b'a'; 64];
        let mut second = first;
        second[63] = b'b';
        let keys: [&[u8]; 3] = [&first, &second, b""];
        for (ordinal, key) in keys.into_iter().enumerate() {
            let block_id = pool.allocate(SlabClassType::AppSmall).unwrap();
            let ptr = unsafe { resolve_slot_ptr(block_id) }.unwrap();
            unsafe { ptr.write(ordinal as u8) };
            table.insert_with_key(collision, key, block_id, 1);
        }

        save_snapshot(&path_v6, &table, &vectors, 0, None).unwrap();

        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        load_snapshot(&path_v6, &new_table, &mut new_pool, &new_vectors, None)
            .unwrap()
            .unwrap();
        assert_eq!(new_table.anonymous_key_count(), 0);

        let mut cursor = 0u64;
        let mut scanned = HashSet::new();
        loop {
            cursor = new_table.scan_batch(cursor, None, 100, None, 0, |k| {
                scanned.insert(k.to_vec());
            });
            if cursor == 0 {
                break;
            }
        }
        assert_eq!(scanned.len(), keys.len());
        for (ordinal, key) in keys.into_iter().enumerate() {
            assert!(scanned.contains(key));
            let entry = new_table.lookup_key_checked(collision, key, 0).unwrap();
            let ptr = unsafe { resolve_slot_ptr(entry.slab_block_id).unwrap() };
            assert_eq!(unsafe { ptr.read() }, ordinal as u8);
        }
        let _ = std::fs::remove_file(&path_v6);

        // V4 persisted KV digests without original key bytes.
        let path_v4 = dir.join(format!(
            "test_v4_compat_{}.kdb",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let mut file = File::create(&path_v4).unwrap();
        let mut hasher = Hasher::new();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&KDB_SNAPSHOT_MAGIC_V4);
        bytes.extend_from_slice(&12345678u64.to_le_bytes()); // ts
        bytes.extend_from_slice(&0u32.to_le_bytes()); // flags
        bytes.extend_from_slice(&0u32.to_le_bytes()); // num_indexes = 0
        bytes.extend_from_slice(&1u32.to_le_bytes()); // num_kv = 1
        let legacy_hash = kachedb_hash::hash_key(b"legacy:v4:item");
        bytes.extend_from_slice(&legacy_hash.to_le_bytes());
        bytes.push(VALUE_TYPE_STRING);
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(b"data");
        bytes.extend_from_slice(&0u32.to_le_bytes()); // expire_at
        hasher.update(&bytes);
        file.write_all(&bytes).unwrap();
        let crc = hasher.finalize();
        file.write_all(&crc.to_le_bytes()).unwrap();
        drop(file);

        let v4_table = ShardedSwissTable::new();
        let v4_vectors = VectorIndexRegistry::new();
        let mut v4_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        assert!(matches!(
            load_snapshot(&path_v4, &v4_table, &mut v4_pool, &v4_vectors, None),
            Err(SnapshotError::MissingKeyBytes)
        ));
        assert!(v4_table.is_empty());
        let _ = std::fs::remove_file(&path_v4);
    }
    #[test]
    fn v5_named_keys_load_and_anonymous_keys_require_migration() {
        let table = ShardedSwissTable::new();
        let vectors = VectorIndexRegistry::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let key = b"v5-key";
        let mut payload = Vec::new();
        payload.extend_from_slice(&0u32.to_le_bytes()); // vector indexes
        payload.extend_from_slice(&1u32.to_le_bytes()); // KV entries
        payload.extend_from_slice(&hash_key(key).to_le_bytes());
        payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
        payload.extend_from_slice(key);
        payload.push(VALUE_TYPE_STRING);
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.push(b'x');
        payload.extend_from_slice(&0u32.to_le_bytes()); // deadline

        let (_, _, count) = hydrate_payload(&payload, 5, &table, &mut pool, &vectors).unwrap();
        assert_eq!(count, 1);
        assert!(table.lookup_key_checked(hash_key(key), key, 0).is_some());

        let key_offset = 8 + 8 + 2;
        payload.drain(key_offset..key_offset + key.len());
        payload[key_offset - 2..key_offset].copy_from_slice(&0u16.to_le_bytes());
        assert!(matches!(
            hydrate_payload(&payload, 5, &table, &mut pool, &vectors),
            Err(SnapshotError::MissingKeyBytes)
        ));
    }
}
