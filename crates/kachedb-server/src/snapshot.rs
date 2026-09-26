//! `kachedb-server` — Asynchronous Snapshot Persistence Engine (`dump.kdb`).
//!
//! Provides zero-lock background point-in-time snapshot persistence for KacheDB's
//! SIMD vector indexes and SwissTable in-memory key-value cache.
//!
//! Format Specification:
//! - Magic Header: `b"KDB\x02"` (4 bytes)
//! - Created Timestamp: `u64` (8 bytes, Unix epoch seconds)
//! - Flags: `u32` (4 bytes, compression/encryption flags)
//! - Vector Section:
//!   - `num_indexes: u32`
//!   - For each index: `name_len: u16`, `name`, `dim: u32`, `num_entries: u32`
//!     - For each entry: `key_len: u16`, `key`, `dim: u32`, `vector: [f32]`,
//!       `has_payload: u8`, `payload`, `expire_at_secs: u32`, `tag_mask: u64`,
//!       `has_parent_key: u8`, `parent_key`
//! - KV SwissTable Section:
//!   - `num_entries: u32`
//!   - For each entry: `key_hash: u64`, `val_len: u32`, `val: [u8]`, `expire_at_secs: u32`
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

/// Magic bytes for KacheDB snapshot format v2 (Hybrid Context Engine enabled).
pub const KDB_SNAPSHOT_MAGIC: [u8; 4] = *b"KDB\x02";

#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Corrupt snapshot magic header: expected 'KDB\\x02'")]
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

/// Writes an atomic, point-in-time binary snapshot of all vector indexes and SwissTable keys.
pub fn save_snapshot(
    target_path: &Path,
    table: &ShardedSwissTable,
    vectors: &VectorIndexRegistry,
    now_sec: u32,
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

    let mut buf = Vec::with_capacity(256 * 1024);
    let mut total_bytes = 0;
    let mut hasher = Hasher::new();

    // 1. Magic Header & Metadata
    buf.extend_from_slice(&KDB_SNAPSHOT_MAGIC);
    buf.extend_from_slice(&ts.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // flags

    // 2. Vector Indexes Section
    let index_snapshots = vectors.snapshot_all(now_sec);
    let num_indexes = index_snapshots.len() as u32;
    buf.extend_from_slice(&num_indexes.to_le_bytes());

    let mut total_vectors = 0;
    for (name, dim_opt, entries) in index_snapshots {
        let name_len = name.len() as u16;
        buf.extend_from_slice(&name_len.to_le_bytes());
        buf.extend_from_slice(&name);

        let dim = dim_opt.unwrap_or(0) as u32;
        buf.extend_from_slice(&dim.to_le_bytes());

        let num_entries = entries.len() as u32;
        buf.extend_from_slice(&num_entries.to_le_bytes());
        total_vectors += entries.len();

        for entry in entries {
            let key_len = entry.key.len() as u16;
            buf.extend_from_slice(&key_len.to_le_bytes());
            buf.extend_from_slice(&entry.key);

            let entry_dim = entry.vector.len() as u32;
            buf.extend_from_slice(&entry_dim.to_le_bytes());
            for &f in &entry.vector {
                buf.extend_from_slice(&f.to_ne_bytes());
            }

            if let Some(ref payload) = entry.payload {
                buf.push(1); // HasPayload = true
                let p_len = payload.len() as u32;
                buf.extend_from_slice(&p_len.to_le_bytes());
                buf.extend_from_slice(payload);
            } else {
                buf.push(0); // HasPayload = false
            }

            buf.extend_from_slice(&entry.expire_at_secs.to_le_bytes());
            buf.extend_from_slice(&entry.tag_mask.to_le_bytes());

            if let Some(ref parent_key) = entry.parent_key {
                buf.push(1); // HasParentKey = true
                let pk_len = parent_key.len() as u16;
                buf.extend_from_slice(&pk_len.to_le_bytes());
                buf.extend_from_slice(parent_key);
            } else {
                buf.push(0); // HasParentKey = false
            }

            if buf.len() >= 128 * 1024 {
                hasher.update(&buf);
                file.write_all(&buf)?;
                total_bytes += buf.len();
                buf.clear();
            }
        }
    }

    // 3. KV SwissTable Section
    let mut total_kv = 0;
    // Buffer entry items first to count total valid entries
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
    buf.extend_from_slice(&num_kv_entries.to_le_bytes());
    total_kv += kv_items.len();

    for (key_hash, val_slice, expire_at) in kv_items {
        buf.extend_from_slice(&key_hash.to_le_bytes());
        let val_len = val_slice.len() as u32;
        buf.extend_from_slice(&val_len.to_le_bytes());
        buf.extend_from_slice(val_slice);
        buf.extend_from_slice(&expire_at.to_le_bytes());

        if buf.len() >= 128 * 1024 {
            hasher.update(&buf);
            file.write_all(&buf)?;
            total_bytes += buf.len();
            buf.clear();
        }
    }

    // 4. Checksum Trailer
    hasher.update(&buf);
    file.write_all(&buf)?;
    total_bytes += buf.len();
    buf.clear();

    let checksum = hasher.finalize();
    file.write_all(&checksum.to_le_bytes())?;
    total_bytes += 4;

    file.flush()?;
    file.sync_all()?;
    drop(file);

    // 5. Atomic Rename Swap
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

    // 2. Validate Magic Header
    if data[0..4] != KDB_SNAPSHOT_MAGIC {
        return Err(SnapshotError::CorruptMagic);
    }

    let ts = u64::from_le_bytes([
        data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11],
    ]);
    // Skip 4 bytes flags
    let mut cursor = 16;

    // 3. Hydrate Vector Indexes
    if cursor + 4 > content_len {
        return Err(SnapshotError::UnexpectedEof);
    }
    let num_indexes = u32::from_le_bytes([
        data[cursor],
        data[cursor + 1],
        data[cursor + 2],
        data[cursor + 3],
    ]) as usize;
    cursor += 4;

    let mut total_vectors = 0;
    for _ in 0..num_indexes {
        if cursor + 10 > content_len {
            return Err(SnapshotError::UnexpectedEof);
        }
        let name_len = u16::from_le_bytes([data[cursor], data[cursor + 1]]) as usize;
        cursor += 2;
        if cursor + name_len + 8 > content_len {
            return Err(SnapshotError::UnexpectedEof);
        }
        let name = data[cursor..cursor + name_len].to_vec();
        cursor += name_len;

        let dim = u32::from_le_bytes([
            data[cursor],
            data[cursor + 1],
            data[cursor + 2],
            data[cursor + 3],
        ]) as usize;
        cursor += 4;

        let num_entries = u32::from_le_bytes([
            data[cursor],
            data[cursor + 1],
            data[cursor + 2],
            data[cursor + 3],
        ]) as usize;
        cursor += 4;

        let mut entries = Vec::with_capacity(num_entries);
        for _ in 0..num_entries {
            if cursor + 6 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let key_len = u16::from_le_bytes([data[cursor], data[cursor + 1]]) as usize;
            cursor += 2;
            if cursor + key_len + 4 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let key = data[cursor..cursor + key_len].to_vec();
            cursor += key_len;

            let entry_dim = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;
            cursor += 4;

            let vec_bytes_len = entry_dim * 4;
            if cursor + vec_bytes_len + 1 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let mut vector = Vec::with_capacity(entry_dim);
            for chunk in data[cursor..cursor + vec_bytes_len].chunks_exact(4) {
                vector.push(f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
            }
            cursor += vec_bytes_len;

            let has_payload = data[cursor];
            cursor += 1;
            let payload = if has_payload == 1 {
                if cursor + 4 > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let p_len = u32::from_le_bytes([
                    data[cursor],
                    data[cursor + 1],
                    data[cursor + 2],
                    data[cursor + 3],
                ]) as usize;
                cursor += 4;
                if cursor + p_len > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let p = data[cursor..cursor + p_len].to_vec();
                cursor += p_len;
                Some(p)
            } else {
                None
            };

            if cursor + 12 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let expire_at_secs = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]);
            cursor += 4;

            let tag_mask = u64::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
                data[cursor + 4],
                data[cursor + 5],
                data[cursor + 6],
                data[cursor + 7],
            ]);
            cursor += 8;

            let has_parent = data[cursor];
            cursor += 1;
            let parent_key = if has_parent == 1 {
                if cursor + 2 > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let pk_len = u16::from_le_bytes([data[cursor], data[cursor + 1]]) as usize;
                cursor += 2;
                if cursor + pk_len > content_len {
                    return Err(SnapshotError::UnexpectedEof);
                }
                let pk = data[cursor..cursor + pk_len].to_vec();
                cursor += pk_len;
                Some(pk)
            } else {
                None
            };

            entries.push(VectorEntry {
                key,
                vector,
                payload,
                expire_at_secs,
                tag_mask,
                parent_key,
            });
        }

        total_vectors += entries.len();
        let dim_opt = if dim > 0 { Some(dim) } else { None };
        vectors.restore_snapshot(&name, dim_opt, entries);
    }

    // 4. Hydrate SwissTable KV Section
    let mut total_kv = 0;
    if cursor + 4 <= content_len {
        let num_kv = u32::from_le_bytes([
            data[cursor],
            data[cursor + 1],
            data[cursor + 2],
            data[cursor + 3],
        ]) as usize;
        cursor += 4;

        for _ in 0..num_kv {
            if cursor + 16 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let key_hash = u64::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
                data[cursor + 4],
                data[cursor + 5],
                data[cursor + 6],
                data[cursor + 7],
            ]);
            cursor += 8;

            let val_len = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
            ]) as usize;
            cursor += 4;

            if cursor + val_len + 4 > content_len {
                return Err(SnapshotError::UnexpectedEof);
            }
            let val_bytes = &data[cursor..cursor + val_len];
            cursor += val_len;

            let expire_at = u32::from_le_bytes([
                data[cursor],
                data[cursor + 1],
                data[cursor + 2],
                data[cursor + 3],
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
                    "Snapshot worker active: saving to {:?} every {}s",
                    target_path,
                    interval_secs
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

                    match save_snapshot(&target_path, &table, vectors, now_sec) {
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
                if let Err(e) = save_snapshot(&target_path, &table, vectors, now_sec) {
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
    fn test_snapshot_roundtrip_vectors_and_kv() {
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

        // Save Snapshot
        let save_stats = save_snapshot(&path, &table, &vectors, 0).expect("save failed");
        assert_eq!(save_stats.vector_indexes, 1);
        assert_eq!(save_stats.total_vectors, 1);
        assert_eq!(save_stats.total_kv_entries, 1);
        assert!(save_stats.bytes_written > 0);
        assert!(path.exists());

        // Hydrate in new clean database instances
        let new_table = ShardedSwissTable::new();
        let new_vectors = VectorIndexRegistry::new();
        let mut new_pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();

        let load_stats = load_snapshot(&path, &new_table, &mut new_pool, &new_vectors)
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

        // Clean up test file
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
        let _ = save_snapshot(&path, &table, &vectors, 0).unwrap();

        // Corrupt a byte in the file
        let mut data = std::fs::read(&path).unwrap();
        data[8] ^= 0xFF;
        std::fs::write(&path, &data).unwrap();

        let mut pool = SlabPool::new(0, 1024 * 1024).unwrap();
        let res = load_snapshot(&path, &table, &mut pool, &vectors);
        assert!(matches!(res, Err(SnapshotError::ChecksumMismatch { .. })));

        let _ = std::fs::remove_file(&path);
    }
}
