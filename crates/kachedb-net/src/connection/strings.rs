//! Redis String command execution handlers (`GET`, `SET`, `MGET`, `MSet`, `DEL`, `EXISTS`, `INCR`, `DECR`, `APPEND`, `STRLEN`).

use kachedb_core::{HashedTimingWheel, SlabClassType, SlabPool, resolve_slot_ptr};
use kachedb_hash::{ShardedSwissTable, VALUE_TYPE_STRING, hash_key};
use kachedb_proto_resp::{
    Command, encode_array_header, encode_bulk_string, encode_error, encode_integer, encode_null,
    encode_simple_string,
};

use crate::aof_encode::{AofOp, emit_aof};
use crate::error::NetError;

/// Internal helper executing atomic arithmetic mutations (INCR, DECR, INCRBY, DECRBY).
pub fn execute_incr_by(
    key: &[u8],
    delta: i64,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    now_sec: u32,
) -> Result<bool, NetError> {
    let h = hash_key(key);
    let (new_val, expire_at_secs) = if let Some(entry) = table.lookup_checked(h, now_sec) {
        if entry.value_type != VALUE_TYPE_STRING {
            encode_error(
                write_buf,
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            );
            return Ok(true);
        }
        if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
            let val_slice = unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
            let val_str = match std::str::from_utf8(val_slice) {
                Ok(s) => s,
                Err(_) => {
                    encode_error(write_buf, "ERR value is not an integer or out of range");
                    return Ok(true);
                }
            };
            let old_val = match val_str.parse::<i64>() {
                Ok(v) => v,
                Err(_) => {
                    encode_error(write_buf, "ERR value is not an integer or out of range");
                    return Ok(true);
                }
            };
            let new_val = match old_val.checked_add(delta) {
                Some(v) => v,
                None => {
                    encode_error(write_buf, "ERR increment or decrement would overflow");
                    return Ok(true);
                }
            };
            (new_val, entry.expire_at_secs)
        } else {
            encode_error(write_buf, "ERR internal slab slot error");
            return Ok(true);
        }
    } else {
        (delta, 0)
    };

    let num_str = new_val.to_string();
    let val_bytes = num_str.as_bytes();
    let val_len = val_bytes.len();

    match SlabClassType::for_size(val_len) {
        Some(class) => {
            let block_id = match pool.allocate(class) {
                Ok(id) => id,
                Err(_) => {
                    encode_error(
                        write_buf,
                        "OOM command not allowed when used memory > 'maxmemory'",
                    );
                    return Ok(true);
                }
            };

            let slot_ptr = match unsafe { resolve_slot_ptr(block_id) } {
                Some(ptr) => ptr,
                None => {
                    let _ = pool.deallocate(block_id);
                    encode_error(write_buf, "ERR internal slab slot error");
                    return Ok(true);
                }
            };

            unsafe {
                std::ptr::copy_nonoverlapping(val_bytes.as_ptr(), slot_ptr, val_len);
            }

            let old_block_id = table.insert_with_ttl(h, block_id, val_len as u32, expire_at_secs);
            if let Some(old_id) = old_block_id {
                let _ = pool.deallocate(old_id);
            }

            encode_integer(write_buf, new_val);
        }
        None => {
            encode_error(
                write_buf,
                "ERR value exceeds maximum supported slab size (2 MB)",
            );
        }
    }

    Ok(true)
}

/// Executes Redis String primitive commands.
pub fn handle_strings(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    now_sec: u32,
    mut timing_wheel: Option<&mut HashedTimingWheel>,
) -> Result<bool, NetError> {
    match cmd {
        Command::Get { key } => {
            let h = hash_key(key);
            if let Some(entry) = table.lookup_checked(h, now_sec) {
                if entry.value_type != VALUE_TYPE_STRING {
                    encode_error(
                        write_buf,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                } else if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                    let val_slice =
                        unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                    encode_bulk_string(write_buf, val_slice);
                } else {
                    encode_null(write_buf);
                }
            } else {
                encode_null(write_buf);
            }
        }
        Command::Set { key, value, ttl_ms } => {
            let val_len = value.len();
            match SlabClassType::for_size(val_len) {
                Some(class) => {
                    let h = hash_key(key);
                    let expire_at_secs = ttl_ms
                        .map(|ms| {
                            let secs = (ms / 1000).max(1) as u32;
                            if now_sec > 0 { now_sec + secs } else { secs }
                        })
                        .unwrap_or(0);

                    let block_id = match pool.allocate(class) {
                        Ok(id) => id,
                        Err(_) => {
                            encode_error(
                                write_buf,
                                "OOM command not allowed when used memory > 'maxmemory'",
                            );
                            return Ok(true);
                        }
                    };

                    let slot_ptr = match unsafe { resolve_slot_ptr(block_id) } {
                        Some(ptr) => ptr,
                        None => {
                            let _ = pool.deallocate(block_id);
                            encode_error(write_buf, "ERR internal slab slot error");
                            return Ok(true);
                        }
                    };

                    // Copy raw payload directly into slab slot (zero-copy cache-line aligned)
                    unsafe {
                        std::ptr::copy_nonoverlapping(value.as_ptr(), slot_ptr, val_len);
                    }

                    // Insert atomically into the global sharded index (zero heap allocations)
                    let old_block_id =
                        table.insert_with_ttl(h, block_id, val_len as u32, expire_at_secs);

                    // Deallocate old slab slot if this was an update
                    if let Some(old_id) = old_block_id {
                        let _ = pool.deallocate(old_id);
                    }

                    // Schedule in TimingWheel if key has TTL
                    if expire_at_secs > 0 {
                        if let Some(ref mut wheel) = timing_wheel {
                            wheel.schedule(h, block_id, expire_at_secs);
                        }
                    }

                    emit_aof(AofOp::Set, key, value, now_sec);
                    encode_simple_string(write_buf, "OK");
                }
                None => {
                    encode_error(
                        write_buf,
                        "ERR value exceeds maximum supported slab size (2 MB)",
                    );
                }
            }
        }
        Command::MGet { keys } => {
            encode_array_header(write_buf, keys.len());
            for key in keys {
                let h = hash_key(key);
                if let Some(entry) = table.lookup_checked(h, now_sec) {
                    if entry.value_type == VALUE_TYPE_STRING {
                        if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                            let val_slice = unsafe {
                                std::slice::from_raw_parts(ptr, entry.value_len as usize)
                            };
                            encode_bulk_string(write_buf, val_slice);
                            continue;
                        }
                    }
                }
                encode_null(write_buf);
            }
        }
        Command::MSet { pairs } => {
            for (key, value) in pairs {
                let val_len = value.len();
                match SlabClassType::for_size(val_len) {
                    Some(class) => {
                        let h = hash_key(key);
                        let block_id = match pool.allocate(class) {
                            Ok(id) => id,
                            Err(_) => {
                                encode_error(
                                    write_buf,
                                    "OOM command not allowed when used memory > 'maxmemory'",
                                );
                                return Ok(true);
                            }
                        };

                        let slot_ptr = match unsafe { resolve_slot_ptr(block_id) } {
                            Some(ptr) => ptr,
                            None => {
                                let _ = pool.deallocate(block_id);
                                encode_error(write_buf, "ERR internal slab slot error");
                                return Ok(true);
                            }
                        };

                        unsafe {
                            std::ptr::copy_nonoverlapping(value.as_ptr(), slot_ptr, val_len);
                        }

                        let old_block_id = table.insert_with_ttl(h, block_id, val_len as u32, 0);
                        if let Some(old_id) = old_block_id {
                            let _ = pool.deallocate(old_id);
                        }
                    }
                    None => {
                        encode_error(
                            write_buf,
                            "ERR value exceeds maximum supported slab size (2 MB)",
                        );
                        return Ok(true);
                    }
                }
            }
            encode_simple_string(write_buf, "OK");
        }
        Command::Del { keys } => {
            let mut deleted = 0i64;
            for key in keys {
                let h = hash_key(key);
                if let Some(entry) = table.remove(h) {
                    let _ = pool.deallocate(entry.slab_block_id);
                    emit_aof(AofOp::Del, key, &[], now_sec);
                    deleted += 1;
                }
            }
            encode_integer(write_buf, deleted);
        }
        Command::Exists { keys } => {
            let mut count = 0i64;
            for key in keys {
                let h = hash_key(key);
                if table.lookup_checked(h, now_sec).is_some() {
                    count += 1;
                }
            }
            encode_integer(write_buf, count);
        }
        Command::Incr { key } => {
            execute_incr_by(key, 1, write_buf, table, pool, now_sec)?;
        }
        Command::Decr { key } => {
            execute_incr_by(key, -1, write_buf, table, pool, now_sec)?;
        }
        Command::IncrBy { key, delta } => {
            execute_incr_by(key, delta, write_buf, table, pool, now_sec)?;
        }
        Command::DecrBy { key, delta } => {
            execute_incr_by(key, -delta, write_buf, table, pool, now_sec)?;
        }
        Command::Append { key, value } => {
            let h = hash_key(key);
            let (combined_val, expire_at_secs) =
                if let Some(entry) = table.lookup_checked(h, now_sec) {
                    if entry.value_type != VALUE_TYPE_STRING {
                        encode_error(
                            write_buf,
                            "WRONGTYPE Operation against a key holding the wrong kind of value",
                        );
                        return Ok(true);
                    }
                    if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                        let existing =
                            unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                        let mut combined = Vec::with_capacity(existing.len() + value.len());
                        combined.extend_from_slice(existing);
                        combined.extend_from_slice(value);
                        (combined, entry.expire_at_secs)
                    } else {
                        (value.to_vec(), 0)
                    }
                } else {
                    (value.to_vec(), 0)
                };

            let new_len = combined_val.len();
            match SlabClassType::for_size(new_len) {
                Some(class) => {
                    let block_id = match pool.allocate(class) {
                        Ok(id) => id,
                        Err(_) => {
                            encode_error(
                                write_buf,
                                "OOM command not allowed when used memory > 'maxmemory'",
                            );
                            return Ok(true);
                        }
                    };

                    let slot_ptr = match unsafe { resolve_slot_ptr(block_id) } {
                        Some(ptr) => ptr,
                        None => {
                            let _ = pool.deallocate(block_id);
                            encode_error(write_buf, "ERR internal slab slot error");
                            return Ok(true);
                        }
                    };

                    unsafe {
                        std::ptr::copy_nonoverlapping(combined_val.as_ptr(), slot_ptr, new_len);
                    }

                    let old_block_id =
                        table.insert_with_ttl(h, block_id, new_len as u32, expire_at_secs);
                    if let Some(old_id) = old_block_id {
                        let _ = pool.deallocate(old_id);
                    }

                    encode_integer(write_buf, new_len as i64);
                }
                None => {
                    encode_error(
                        write_buf,
                        "ERR string exceeds maximum supported slab size (2 MB)",
                    );
                }
            }
        }
        Command::Strlen { key } => {
            let h = hash_key(key);
            if let Some(entry) = table.lookup_checked(h, now_sec) {
                encode_integer(write_buf, entry.value_len as i64);
            } else {
                encode_integer(write_buf, 0);
            }
        }
        _ => unreachable!("handle_strings called with non-string command"),
    }

    Ok(true)
}
