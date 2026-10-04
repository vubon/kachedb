//! Redis Hash command execution handlers (`HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`).

use kachedb_core::{HashedTimingWheel, SlabClassType, SlabPool, resolve_slot_ptr};
use kachedb_hash::{HashSlotFrame, ShardedSwissTable, VALUE_TYPE_HASH, hash_key};
use kachedb_proto_resp::{
    Command, encode_array_header, encode_bulk_string, encode_error, encode_integer, encode_null,
};

use crate::aof_encode::{AofOp, emit_aof};
use crate::error::NetError;

/// Sizing helper for Redis Hash Megaslab allocations, strictly isolating
/// hash slots to AppSmall, AppMedium, or AppLarge classes without touching Tensor classes.
#[inline]
pub fn hash_slab_class(needed_bytes: usize) -> Option<SlabClassType> {
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

/// Executes Redis Hash primitive commands.
pub fn handle_hashes(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    now_sec: u32,
    mut timing_wheel: Option<&mut HashedTimingWheel>,
) -> Result<bool, NetError> {
    match cmd {
        Command::HSet { key, pairs } => {
            let h = hash_key(key);
            let pair_refs: smallvec::SmallVec<[(&[u8], &[u8]); 8]> =
                pairs.iter().map(|(f, v)| (*f, *v)).collect();
            let needed_bytes = table.with_shard(h, |shard| {
                let old_slot = shard.lookup_checked(h, now_sec).and_then(|e| {
                    if e.value_type == VALUE_TYPE_HASH {
                        unsafe {
                            resolve_slot_ptr(e.slab_block_id)
                                .map(|ptr| std::slice::from_raw_parts(ptr, e.value_len as usize))
                        }
                    } else {
                        None
                    }
                });
                HashSlotFrame::needed_bytes(old_slot, &pair_refs)
            });

            if needed_bytes > 4096 {
                encode_error(write_buf, "ERR hash exceeds the 4096-byte limit");
                return Ok(true);
            }

            let target_class = match hash_slab_class(needed_bytes) {
                Some(c) => c,
                None => {
                    encode_error(write_buf, "ERR hash exceeds the 4096-byte limit");
                    return Ok(true);
                }
            };

            let mut new_fields_count = 0i64;
            let mut aof_pairs_payload = Vec::new();

            let res: Result<(), &str> = table.with_shard_mut(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, now_sec) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }

                    let cur_class = SlabClassType::for_size(entry.value_len as usize);
                    let mut in_place_success = false;

                    // Try in-place mutation if slot class is already large enough
                    if cur_class.map(|c| c.slot_bytes()).unwrap_or(0) >= target_class.slot_bytes() {
                        if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                            let slot = unsafe {
                                std::slice::from_raw_parts_mut(ptr, entry.value_len as usize)
                            };
                            let mut local_new_count = 0i64;
                            let mut all_ok = true;

                            for (f, v) in &pairs {
                                match HashSlotFrame::upsert(slot, f, v) {
                                    Ok(is_new) => {
                                        if is_new {
                                            local_new_count += 1;
                                        }
                                    }
                                    Err(_) => {
                                        all_ok = false;
                                        break;
                                    }
                                }
                            }

                            if all_ok {
                                new_fields_count = local_new_count;
                                in_place_success = true;
                            }
                        }
                    }

                    if in_place_success {
                        return Ok(());
                    }

                    // Slot expansion needed: allocate target_class
                    let new_block_id = match pool.allocate(target_class) {
                        Ok(id) => id,
                        Err(_) => {
                            return Err("OOM command not allowed when used memory > 'maxmemory'");
                        }
                    };

                    let new_ptr = match unsafe { resolve_slot_ptr(new_block_id) } {
                        Some(p) => p,
                        None => {
                            let _ = pool.deallocate(new_block_id);
                            return Err("ERR internal slab slot error");
                        }
                    };

                    let new_slot = unsafe {
                        std::slice::from_raw_parts_mut(new_ptr, target_class.slot_bytes())
                    };
                    if HashSlotFrame::init(new_slot).is_err() {
                        let _ = pool.deallocate(new_block_id);
                        return Err("ERR internal slab slot error");
                    }

                    // Copy over existing live pairs
                    if let Some(old_ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let old_slot = unsafe {
                            std::slice::from_raw_parts(old_ptr, entry.value_len as usize)
                        };
                        for (f, v) in HashSlotFrame::iter_pairs(old_slot) {
                            let _ = HashSlotFrame::upsert(new_slot, f, v);
                        }
                    }

                    // Apply new pairs
                    for (f, v) in &pairs {
                        match HashSlotFrame::upsert(new_slot, f, v) {
                            Ok(is_new) => {
                                if is_new {
                                    new_fields_count += 1;
                                }
                            }
                            Err(_) => {
                                let _ = pool.deallocate(new_block_id);
                                return Err("ERR hash frame full during expansion");
                            }
                        }
                    }

                    let old_expire = entry.expire_at_secs;
                    let old_block_id = entry.slab_block_id;
                    match shard.insert_typed(
                        h,
                        new_block_id,
                        target_class.slot_bytes() as u32,
                        old_expire,
                        VALUE_TYPE_HASH,
                    ) {
                        Ok(_) => {
                            let _ = pool.deallocate(old_block_id);
                            if old_expire > 0 {
                                if let Some(ref mut wheel) = timing_wheel {
                                    wheel.schedule(h, new_block_id, old_expire);
                                }
                            }
                            Ok(())
                        }
                        Err(_) => {
                            let _ = pool.deallocate(new_block_id);
                            Err("OOM command not allowed when used memory > 'maxmemory'")
                        }
                    }
                } else {
                    // New key insertion
                    let block_id = match pool.allocate(target_class) {
                        Ok(id) => id,
                        Err(_) => {
                            return Err("OOM command not allowed when used memory > 'maxmemory'");
                        }
                    };

                    let ptr = match unsafe { resolve_slot_ptr(block_id) } {
                        Some(p) => p,
                        None => {
                            let _ = pool.deallocate(block_id);
                            return Err("ERR internal slab slot error");
                        }
                    };

                    let slot =
                        unsafe { std::slice::from_raw_parts_mut(ptr, target_class.slot_bytes()) };
                    if HashSlotFrame::init(slot).is_err() {
                        let _ = pool.deallocate(block_id);
                        return Err("ERR internal slab slot error");
                    }

                    for (f, v) in &pairs {
                        match HashSlotFrame::upsert(slot, f, v) {
                            Ok(is_new) => {
                                if is_new {
                                    new_fields_count += 1;
                                }
                            }
                            Err(_) => {
                                let _ = pool.deallocate(block_id);
                                return Err("ERR hash frame full during initialization");
                            }
                        }
                    }

                    match shard.insert_typed(
                        h,
                        block_id,
                        target_class.slot_bytes() as u32,
                        0,
                        VALUE_TYPE_HASH,
                    ) {
                        Ok(_) => Ok(()),
                        Err(_) => {
                            let _ = pool.deallocate(block_id);
                            Err("OOM command not allowed when used memory > 'maxmemory'")
                        }
                    }
                }
            });

            match res {
                Ok(()) => {
                    aof_pairs_payload.extend_from_slice(&(pairs.len() as u16).to_le_bytes());
                    for (f, v) in &pairs {
                        aof_pairs_payload.extend_from_slice(&(f.len() as u16).to_le_bytes());
                        aof_pairs_payload.extend_from_slice(f);
                        aof_pairs_payload.extend_from_slice(&(v.len() as u16).to_le_bytes());
                        aof_pairs_payload.extend_from_slice(v);
                    }
                    emit_aof(AofOp::HSet, key, &aof_pairs_payload, now_sec);
                    encode_integer(write_buf, new_fields_count);
                }
                Err(err) => {
                    encode_error(write_buf, err);
                }
            }
        }
        Command::HGet { key, field } => {
            let h = hash_key(key);
            table.with_shard(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, now_sec) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        encode_error(
                            write_buf,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    } else if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let slot =
                            unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                        if let Some(val) = HashSlotFrame::get(slot, field) {
                            encode_bulk_string(write_buf, val);
                        } else {
                            encode_null(write_buf);
                        }
                    } else {
                        encode_null(write_buf);
                    }
                } else {
                    encode_null(write_buf);
                }
            });
        }
        Command::HDel { key, fields } => {
            let h = hash_key(key);
            let mut deleted_count = 0i64;
            let mut should_delete_key = false;
            let mut block_to_free = None;
            let mut aof_fields_payload = Vec::new();

            let res: Result<(), &str> = table.with_shard_mut(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, now_sec) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        return Err(
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    }
                    if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let slot = unsafe {
                            std::slice::from_raw_parts_mut(ptr, entry.value_len as usize)
                        };
                        for f in &fields {
                            if HashSlotFrame::delete(slot, f) {
                                deleted_count += 1;
                                aof_fields_payload
                                    .extend_from_slice(&(f.len() as u16).to_le_bytes());
                                aof_fields_payload.extend_from_slice(f);
                            }
                        }
                        if HashSlotFrame::live_count(slot) == 0 {
                            should_delete_key = true;
                            block_to_free = Some(entry.slab_block_id);
                            shard.remove(h);
                        }
                    }
                }
                Ok(())
            });

            match res {
                Ok(()) => {
                    if should_delete_key {
                        if let Some(id) = block_to_free {
                            let _ = pool.deallocate(id);
                        }
                    }
                    if deleted_count > 0 {
                        let mut full_aof = Vec::with_capacity(2 + aof_fields_payload.len());
                        full_aof.extend_from_slice(&(deleted_count as u16).to_le_bytes());
                        full_aof.extend_from_slice(&aof_fields_payload);
                        emit_aof(AofOp::HDel, key, &full_aof, now_sec);
                    }
                    encode_integer(write_buf, deleted_count);
                }
                Err(err) => {
                    encode_error(write_buf, err);
                }
            }
        }
        Command::HExists { key, field } => {
            let h = hash_key(key);
            table.with_shard(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, now_sec) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        encode_error(
                            write_buf,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    } else if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let slot =
                            unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                        encode_integer(
                            write_buf,
                            if HashSlotFrame::exists(slot, field) {
                                1
                            } else {
                                0
                            },
                        );
                    } else {
                        encode_integer(write_buf, 0);
                    }
                } else {
                    encode_integer(write_buf, 0);
                }
            });
        }
        Command::HLen { key } => {
            let h = hash_key(key);
            table.with_shard(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, now_sec) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        encode_error(
                            write_buf,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    } else if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let slot =
                            unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                        encode_integer(write_buf, HashSlotFrame::live_count(slot) as i64);
                    } else {
                        encode_integer(write_buf, 0);
                    }
                } else {
                    encode_integer(write_buf, 0);
                }
            });
        }
        Command::HGetAll { key } => {
            let h = hash_key(key);
            table.with_shard(h, |shard| {
                if let Some(entry) = shard.lookup_checked(h, now_sec) {
                    if entry.value_type != VALUE_TYPE_HASH {
                        encode_error(
                            write_buf,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                    } else if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let slot =
                            unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                        let count = HashSlotFrame::live_count(slot) as usize;
                        encode_array_header(write_buf, count * 2);
                        for (f, v) in HashSlotFrame::iter_pairs(slot) {
                            encode_bulk_string(write_buf, f);
                            encode_bulk_string(write_buf, v);
                        }
                    } else {
                        encode_array_header(write_buf, 0);
                    }
                } else {
                    encode_array_header(write_buf, 0);
                }
            });
        }
        _ => unreachable!("handle_hashes called with non-hash command"),
    }

    Ok(true)
}
