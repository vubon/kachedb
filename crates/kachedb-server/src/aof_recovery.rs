//! `kachedb-server` — AOF recovery and replay engine.
//!
//! Scans existing `.kaof` log at server startup, verifies CRC32 checksums,
//! reconstructs in-memory SwissTable keys and vector indexes, and safely truncates partial tails.

use std::fs::OpenOptions;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use kachedb_core::{SlabClassType, SlabPool, resolve_slot_ptr};
use kachedb_hash::{HashSlotFrame, ShardedSwissTable, VALUE_TYPE_HASH, hash_key};
use kachedb_vector::{QuantizationMode, VectorIndexRegistry, VectorMetric};

use crate::aof::{AofError, AofOp, decode_frame};

/// Replays all valid AOF frames from `path` into the SwissTable and vector index registry.
///
/// Returns the number of recovered mutation frames.
pub fn replay(
    path: &Path,
    table: &Arc<ShardedSwissTable>,
    pool: &mut SlabPool,
    vectors: &VectorIndexRegistry,
) -> Result<usize, AofError> {
    if !path.exists() {
        return Ok(0);
    }

    let mut file = OpenOptions::new().read(true).open(path)?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;

    if data.is_empty() {
        return Ok(0);
    }

    let mut offset = 0;
    let mut count = 0;
    let mut last_valid_offset = 0;

    while offset < data.len() {
        match decode_frame(&data[offset..]) {
            Ok(Some((frame, consumed))) => {
                apply_frame(&frame, table, pool, vectors);
                offset += consumed;
                last_valid_offset = offset;
                count += 1;
            }
            Ok(None) => {
                // Incomplete tail: server probably crashed mid-write
                log::warn!(
                    "AOF recovery: detected incomplete frame at offset {} of {} bytes. Truncating tail.",
                    offset,
                    data.len()
                );
                break;
            }
            Err(e) => {
                // Checksum or magic corruption
                log::warn!(
                    "AOF recovery: corrupt frame at offset {} ({e}). Truncating to last valid offset {}.",
                    offset,
                    last_valid_offset
                );
                break;
            }
        }
    }

    // If file ended with corruption or partial write, truncate to last valid offset
    if last_valid_offset < data.len()
        && let Ok(trunc_file) = OpenOptions::new().write(true).open(path)
    {
        let _ = trunc_file.set_len(last_valid_offset as u64);
        log::info!(
            "AOF recovery: safely truncated {:?} to {} bytes",
            path,
            last_valid_offset
        );
    }

    log::info!("AOF recovery: successfully replayed {count} frames from {path:?}");
    Ok(count)
}

#[inline]
fn hash_slab_class(needed_bytes: usize) -> Option<SlabClassType> {
    if needed_bytes <= 128 {
        Some(SlabClassType::AppSmall)
    } else if needed_bytes <= 512 {
        Some(SlabClassType::AppMedium)
    } else if needed_bytes <= 4096 {
        Some(SlabClassType::AppLarge)
    } else {
        None
    }
}

fn apply_frame(
    frame: &crate::aof::AofFrame,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    vectors: &VectorIndexRegistry,
) {
    match frame.op {
        AofOp::Set => {
            let val_len = frame.value.len();
            if let Some(class) = SlabClassType::for_size(val_len)
                && let Ok(block_id) = pool.allocate(class)
                && let Some(ptr) = unsafe { resolve_slot_ptr(block_id) }
            {
                unsafe {
                    std::ptr::copy_nonoverlapping(frame.value.as_ptr(), ptr, val_len);
                }
                let h = hash_key(&frame.key);
                let old = table.insert(h, block_id, val_len as u32);
                if let Some(old_id) = old {
                    let _ = pool.deallocate(old_id);
                }
            }
        }
        AofOp::Del => {
            let h = hash_key(&frame.key);
            if let Some(entry) = table.remove(h) {
                let _ = pool.deallocate(entry.slab_block_id);
            }
        }
        AofOp::Expire => {
            if let Ok(ts_str) = std::str::from_utf8(&frame.value)
                && let Ok(expire_at) = ts_str.parse::<u32>()
            {
                let h = hash_key(&frame.key);
                table.update_ttl(h, expire_at, 0);
            }
        }
        AofOp::HSet => {
            if frame.value.len() < 2 {
                return;
            }
            let count = u16::from_le_bytes([frame.value[0], frame.value[1]]) as usize;
            let mut offset = 2;
            let mut pairs = Vec::with_capacity(count);
            for _ in 0..count {
                if offset + 2 > frame.value.len() {
                    return;
                }
                let f_len =
                    u16::from_le_bytes([frame.value[offset], frame.value[offset + 1]]) as usize;
                offset += 2;
                if offset + f_len > frame.value.len() {
                    return;
                }
                let f = &frame.value[offset..offset + f_len];
                offset += f_len;

                if offset + 2 > frame.value.len() {
                    return;
                }
                let v_len =
                    u16::from_le_bytes([frame.value[offset], frame.value[offset + 1]]) as usize;
                offset += 2;
                if offset + v_len > frame.value.len() {
                    return;
                }
                let v = &frame.value[offset..offset + v_len];
                offset += v_len;

                pairs.push((f, v));
            }

            let h = hash_key(&frame.key);
            let needed_bytes = table.with_shard(h, |shard| {
                let old_slot = shard.lookup_checked(h, 0).and_then(|e| {
                    if e.value_type == VALUE_TYPE_HASH {
                        unsafe {
                            resolve_slot_ptr(e.slab_block_id)
                                .map(|ptr| std::slice::from_raw_parts(ptr, e.value_len as usize))
                        }
                    } else {
                        None
                    }
                });
                HashSlotFrame::needed_bytes(old_slot, &pairs)
            });

            let target_class = match hash_slab_class(needed_bytes) {
                Some(c) => c,
                None => return,
            };

            table.with_shard_mut(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, 0) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        return;
                    }
                    let cur_class = SlabClassType::for_size(entry.value_len as usize);
                    if cur_class.map(|c| c.slot_bytes()).unwrap_or(0) >= target_class.slot_bytes()
                        && let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) }
                    {
                        let slot = unsafe {
                            std::slice::from_raw_parts_mut(ptr, entry.value_len as usize)
                        };
                        for (f, v) in &pairs {
                            let _ = HashSlotFrame::upsert(slot, f, v);
                        }
                        return;
                    }

                    // Expand slot
                    if let Ok(new_block_id) = pool.allocate(target_class)
                        && let Some(new_ptr) = unsafe { resolve_slot_ptr(new_block_id) }
                    {
                        let new_slot = unsafe {
                            std::slice::from_raw_parts_mut(new_ptr, target_class.slot_bytes())
                        };
                        if HashSlotFrame::init(new_slot).is_ok() {
                            if let Some(old_ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) }
                            {
                                let old_slot = unsafe {
                                    std::slice::from_raw_parts(old_ptr, entry.value_len as usize)
                                };
                                for (f, v) in HashSlotFrame::iter_pairs(old_slot) {
                                    let _ = HashSlotFrame::upsert(new_slot, f, v);
                                }
                            }
                            for (f, v) in &pairs {
                                let _ = HashSlotFrame::upsert(new_slot, f, v);
                            }
                            let old_id = entry.slab_block_id;
                            let expire_at = entry.expire_at_secs;
                            if shard
                                .insert_typed(
                                    h,
                                    new_block_id,
                                    target_class.slot_bytes() as u32,
                                    expire_at,
                                    VALUE_TYPE_HASH,
                                )
                                .is_ok()
                            {
                                let _ = pool.deallocate(old_id);
                            } else {
                                let _ = pool.deallocate(new_block_id);
                            }
                        } else {
                            let _ = pool.deallocate(new_block_id);
                        }
                    }
                } else {
                    // New key
                    if let Ok(block_id) = pool.allocate(target_class)
                        && let Some(ptr) = unsafe { resolve_slot_ptr(block_id) }
                    {
                        let slot = unsafe {
                            std::slice::from_raw_parts_mut(ptr, target_class.slot_bytes())
                        };
                        if HashSlotFrame::init(slot).is_ok() {
                            for (f, v) in &pairs {
                                let _ = HashSlotFrame::upsert(slot, f, v);
                            }
                            if shard
                                .insert_typed(
                                    h,
                                    block_id,
                                    target_class.slot_bytes() as u32,
                                    0,
                                    VALUE_TYPE_HASH,
                                )
                                .is_err()
                            {
                                let _ = pool.deallocate(block_id);
                            }
                        } else {
                            let _ = pool.deallocate(block_id);
                        }
                    }
                }
            });
        }
        AofOp::HDel => {
            if frame.value.len() < 2 {
                return;
            }
            let count = u16::from_le_bytes([frame.value[0], frame.value[1]]) as usize;
            let mut offset = 2;
            let mut fields = Vec::with_capacity(count);
            for _ in 0..count {
                if offset + 2 > frame.value.len() {
                    return;
                }
                let f_len =
                    u16::from_le_bytes([frame.value[offset], frame.value[offset + 1]]) as usize;
                offset += 2;
                if offset + f_len > frame.value.len() {
                    return;
                }
                let f = &frame.value[offset..offset + f_len];
                offset += f_len;
                fields.push(f);
            }

            let h = hash_key(&frame.key);
            let mut block_to_free = None;
            table.with_shard_mut(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, 0) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        return;
                    }
                    if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let slot = unsafe {
                            std::slice::from_raw_parts_mut(ptr, entry.value_len as usize)
                        };
                        for f in fields {
                            let _ = HashSlotFrame::delete(slot, f);
                        }
                        if HashSlotFrame::live_count(slot) == 0 {
                            block_to_free = Some(entry.slab_block_id);
                            shard.remove(h);
                        }
                    }
                }
            });
            if let Some(id) = block_to_free {
                let _ = pool.deallocate(id);
            }
        }
        AofOp::VIndexCreate => {
            // value encoded as: dim (4B) + m (2B) + ef_c (4B) + ef_s (4B) + metric (1B) + quant (1B)
            if frame.value.len() >= 4 {
                let dim = u32::from_le_bytes([
                    frame.value[0],
                    frame.value[1],
                    frame.value[2],
                    frame.value[3],
                ]) as usize;
                vectors.create_hnsw(
                    &frame.key,
                    dim,
                    16,
                    200,
                    50,
                    VectorMetric::Cosine,
                    QuantizationMode::None,
                );
            }
        }
        AofOp::VIndexDrop => {
            vectors.drop_hnsw(&frame.key);
            vectors.delete_index(&frame.key);
        }
        AofOp::VAdd => {
            // key = <index_name>\0<id>
            if let Some(pos) = frame.key.iter().position(|&b| b == 0) {
                let index_name = &frame.key[..pos];
                let id = &frame.key[pos + 1..];
                if frame.value.len() >= 4 {
                    let dim = u32::from_le_bytes([
                        frame.value[0],
                        frame.value[1],
                        frame.value[2],
                        frame.value[3],
                    ]) as usize;
                    let vec_bytes_len = dim * 4;
                    if frame.value.len() >= 4 + vec_bytes_len {
                        let vec_bytes = &frame.value[4..4 + vec_bytes_len];
                        let payload = if frame.value.len() > 4 + vec_bytes_len {
                            Some(&frame.value[4 + vec_bytes_len..])
                        } else {
                            None
                        };

                        #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
                        let floats: Vec<f32> = vec_bytes
                            .chunks_exact(4)
                            .map(|c| f32::from_ne_bytes([c[0], c[1], c[2], c[3]]))
                            .collect();

                        if let Some(hnsw) = vectors.get_hnsw(index_name) {
                            let _ = hnsw.insert(id, &floats, payload, None, 0);
                        } else {
                            let flat = vectors.get_or_create(index_name);
                            let _ = flat.insert(id, &floats, payload, None, 0, 0, None);
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::encode_frame;
    use std::io::Write;

    #[test]
    fn test_aof_replay_and_recovery() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "test_aof_replay_{}.kaof",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let f1 = encode_frame(AofOp::Set, b"foo", b"bar", 100);
        let f2 = encode_frame(AofOp::Set, b"baz", b"qux", 101);
        let f3 = encode_frame(AofOp::Del, b"foo", b"", 102);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.write_all(&f1).unwrap();
        file.write_all(&f2).unwrap();
        file.write_all(&f3).unwrap();
        drop(file);

        let table = Arc::new(ShardedSwissTable::new());
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let vectors = VectorIndexRegistry::new();

        let count = replay(&path, &table, &mut pool, &vectors).unwrap();
        assert_eq!(count, 3);

        // foo was deleted
        assert!(table.lookup(hash_key(b"foo")).is_none());
        // baz is present
        assert!(table.lookup(hash_key(b"baz")).is_some());

        let _ = std::fs::remove_file(&path);
    }
}
