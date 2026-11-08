//! `kachedb-server` — Background AOF Compaction & Rewrite (BGREWRITEAOF).
//!
//! Compacts historical mutation logs into a minimal point-in-time snapshot of current live keys,
//! writing to a temporary file before performing an atomic rename swap.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use kachedb_core::resolve_slot_ptr;
use kachedb_hash::{HashSlotFrame, ShardedSwissTable, VALUE_TYPE_HASH};
use kachedb_vector::VectorIndexRegistry;

use crate::aof::{AofError, AofOp, encode_frame};

/// Background AOF rewrite execution job.
#[allow(dead_code)]
pub struct BgRewriteJob;

#[allow(dead_code)]
impl BgRewriteJob {
    /// Executes the AOF rewrite asynchronously in a background thread.
    pub fn start(
        target_path: PathBuf,
        table: Arc<ShardedSwissTable>,
        _vectors: Arc<VectorIndexRegistry>,
        now_sec: u32,
    ) {
        thread::Builder::new()
            .name("kachedb-bgrewriteaof".into())
            .spawn(move || {
                let tmp_path = target_path.with_extension("tmp");
                match Self::execute_rewrite(&tmp_path, &table, now_sec) {
                    Ok(count) => {
                        if let Err(e) = std::fs::rename(&tmp_path, &target_path) {
                            log::error!("BGREWRITEAOF: atomic rename failed: {e}");
                        } else {
                            log::info!(
                                "BGREWRITEAOF: rewrite completed successfully, {count} keys written to {target_path:?}"
                            );
                        }
                    }
                    Err(e) => {
                        log::error!("BGREWRITEAOF: rewrite failed: {e}");
                        let _ = std::fs::remove_file(&tmp_path);
                    }
                }
            })
            .expect("failed to spawn bgrewriteaof thread");
    }

    fn execute_rewrite(
        tmp_path: &Path,
        table: &ShardedSwissTable,
        now_sec: u32,
    ) -> Result<usize, AofError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(tmp_path)?;

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut count = 0;
        let mut buf = Vec::with_capacity(64 * 1024);

        // Iterate through all table shards
        for shard_idx in 0..table.shard_count() {
            let entries = table.snapshot_shard_with_keys(shard_idx);
            for (key_hash, key_bytes, entry) in entries {
                let expire_at = entry.expire_at_secs;
                let block_id = entry.slab_block_id;
                let val_len = entry.value_len;
                if expire_at > 0 && now_sec > 0 && now_sec >= expire_at {
                    continue; // Skip expired keys
                }

                if let Some(ptr) = unsafe { resolve_slot_ptr(block_id) } {
                    let val_slice = unsafe { std::slice::from_raw_parts(ptr, val_len as usize) };
                    let fallback_hash_bytes = key_hash.to_le_bytes();
                    let effective_key: &[u8] = if key_bytes.is_empty() {
                        &fallback_hash_bytes
                    } else {
                        &key_bytes
                    };
                    if entry.value_type == VALUE_TYPE_HASH {
                        let mut encoded = Vec::new();
                        HashSlotFrame::encode_pairs(val_slice, &mut encoded);
                        let frame_bytes = encode_frame(AofOp::HSet, effective_key, &encoded, ts);
                        buf.extend_from_slice(&frame_bytes);
                    } else {
                        let frame_bytes = encode_frame(AofOp::Set, effective_key, val_slice, ts);
                        buf.extend_from_slice(&frame_bytes);
                    }
                    if expire_at > 0 {
                        let expire_str = expire_at.to_string();
                        let exp_frame =
                            encode_frame(AofOp::Expire, effective_key, expire_str.as_bytes(), ts);
                        buf.extend_from_slice(&exp_frame);
                    }
                    count += 1;

                    if buf.len() >= 64 * 1024 {
                        file.write_all(&buf)?;
                        buf.clear();
                    }
                }
            }
        }

        if !buf.is_empty() {
            file.write_all(&buf)?;
            buf.clear();
        }

        file.flush()?;
        file.sync_all()?;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof_recovery::replay;
    use kachedb_core::{SlabClassType, SlabPool};
    use kachedb_hash::{VALUE_TYPE_STRING, hash_key};

    #[test]
    fn test_aof_rewrite_preserves_strings_hashes_and_ttl() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_aof_rewrite_{}.kaof",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let table = ShardedSwissTable::new();

        // 1. Insert a string key with TTL = 500
        let str_key = b"session:token:abc";
        let str_val = b"jwt_payload_data";
        let str_block = pool.allocate(SlabClassType::AppSmall).unwrap();
        unsafe {
            let ptr = resolve_slot_ptr(str_block).unwrap();
            std::ptr::copy_nonoverlapping(str_val.as_ptr(), ptr, str_val.len());
        }
        table.insert_with_ttl_and_key(
            hash_key(str_key),
            str_key,
            str_block,
            str_val.len() as u32,
            500,
        );

        // 2. Insert a hash key (`VALUE_TYPE_HASH`) with 2 fields
        let hash_key_bytes = b"user:profile:42";
        let hash_block = pool.allocate(SlabClassType::AppSmall).unwrap();
        let slot_bytes = SlabClassType::AppSmall.slot_bytes();
        unsafe {
            let ptr = resolve_slot_ptr(hash_block).unwrap();
            let slot = std::slice::from_raw_parts_mut(ptr, slot_bytes);
            HashSlotFrame::init(slot).unwrap();
            HashSlotFrame::upsert(slot, b"name", b"Bob").unwrap();
            HashSlotFrame::upsert(slot, b"role", b"staff").unwrap();
        }
        table.with_shard_mut(hash_key(hash_key_bytes), |shard| {
            shard
                .insert_typed_with_key(
                    hash_key(hash_key_bytes),
                    hash_key_bytes,
                    hash_block,
                    slot_bytes as u32,
                    0,
                    VALUE_TYPE_HASH,
                )
                .unwrap();
        });

        // 3. Execute rewrite at now_sec = 100
        let written = BgRewriteJob::execute_rewrite(&path, &table, 100).unwrap();
        assert_eq!(written, 2);

        // 4. Replay into a fresh table
        let recovered_table = Arc::new(ShardedSwissTable::new());
        let vectors = VectorIndexRegistry::new();
        let frames = replay(&path, &recovered_table, &mut pool, &vectors).unwrap();
        // 1 Set + 1 Expire + 1 HSet = 3 frames
        assert_eq!(frames, 3);

        // Verify string key + TTL
        let str_entry = recovered_table
            .lookup_checked(hash_key(str_key), 100)
            .expect("string key recovered");
        assert_eq!(str_entry.value_type, VALUE_TYPE_STRING);
        assert_eq!(str_entry.expire_at_secs, 500);

        // Verify hash key + VALUE_TYPE_HASH + fields
        let hash_entry = recovered_table
            .lookup_checked(hash_key(hash_key_bytes), 100)
            .expect("hash key recovered");
        assert_eq!(hash_entry.value_type, VALUE_TYPE_HASH);
        unsafe {
            let ptr = resolve_slot_ptr(hash_entry.slab_block_id).unwrap();
            let slot = std::slice::from_raw_parts(ptr, hash_entry.value_len as usize);
            assert_eq!(HashSlotFrame::get(slot, b"name"), Some(&b"Bob"[..]));
            assert_eq!(HashSlotFrame::get(slot, b"role"), Some(&b"staff"[..]));
        }

        // Verify both keys are visible to SCAN (key_len > 0)
        assert_eq!(recovered_table.anonymous_key_count(), 0);

        let _ = std::fs::remove_file(&path);
    }
}
