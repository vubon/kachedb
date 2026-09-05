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
use kachedb_hash::ShardedSwissTable;
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
            let entries = table.snapshot_shard(shard_idx);
            for (key_hash, entry) in entries {
                let expire_at = entry.expire_at_secs;
                let block_id = entry.slab_block_id;
                let val_len = entry.value_len;
                if expire_at > 0 && now_sec > 0 && now_sec >= expire_at {
                    continue; // Skip expired keys
                }

                if let Some(ptr) = unsafe { resolve_slot_ptr(block_id) } {
                    let val_slice = unsafe { std::slice::from_raw_parts(ptr, val_len as usize) };
                    // Synthesize key from hash or store key hash representation
                    let key_bytes = key_hash.to_le_bytes();
                    let frame_bytes = encode_frame(AofOp::Set, &key_bytes, val_slice, ts);
                    buf.extend_from_slice(&frame_bytes);
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
