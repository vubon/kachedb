//! Redis String command execution handlers (`GET`, `SET`, `MGET`, `MSet`, `DEL`, `EXISTS`, `INCR`, `DECR`, `APPEND`, `STRLEN`).

use kachedb_core::{HashedTimingWheel, SlabClassType, SlabPool, resolve_slot_ptr};
use kachedb_hash::{ShardedSwissTable, SwissTable, VALUE_TYPE_STRING, hash_key};
use kachedb_proto_resp::{
    Command, encode_array_header, encode_bulk_string, encode_error, encode_integer, encode_null,
    encode_simple_string,
};

use crate::aof_encode::{AofOp, emit_aof};
use crate::error::NetError;

const WRONG_TYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const SLOT_ERROR: &str = "ERR internal slab slot error";
const OOM_ERROR: &str = "OOM command not allowed when used memory > 'maxmemory'";
const SIZE_ERROR: &str = "ERR value exceeds maximum supported slab size (2 MB)";
const INTEGER_ERROR: &str = "ERR value is not an integer or out of range";

enum Arithmetic {
    Add(i64),
    Subtract(i64),
}

/// Writes a new allocation and publishes it under the key's shard write lock.
fn store_string(
    shard: &mut SwissTable,
    pool: &mut SlabPool,
    hash: u64,
    key: &[u8],
    value: &[u8],
    expires: u32,
    wheel: Option<&mut HashedTimingWheel>,
) -> Result<(), &'static str> {
    let class = SlabClassType::for_size(value.len()).ok_or(SIZE_ERROR)?;
    let block = pool.reserve_non_evicting(class).map_err(|_| OOM_ERROR)?;
    // This slot is unpublished and exclusively owned by the current worker.
    let Some(ptr) = (unsafe { resolve_slot_ptr(block) }) else {
        let _ = pool.deallocate(block);
        return Err(SLOT_ERROR);
    };
    unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, value.len()) };
    let old = match shard.insert_with_ttl_and_key(hash, key, block, value.len() as u32, expires) {
        Ok(old) => old,
        Err(()) => {
            let _ = pool.deallocate(block);
            return Err(OOM_ERROR);
        }
    };
    if let Some(old) = old {
        let _ = pool.deallocate(old);
    }
    if expires != 0
        && let Some(wheel) = wheel
    {
        wheel.schedule_key(hash, key, block, expires);
    }
    Ok(())
}

fn execute_arithmetic(
    key: &[u8],
    arithmetic: Arithmetic,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    now: u32,
    wheel: Option<&mut HashedTimingWheel>,
) {
    let hash = hash_key(key);
    let result = table.with_shard_mut(hash, |shard| {
        let (current, expires) = if let Some(entry) = shard.lookup_key_checked(hash, key, now) {
            if entry.value_type != VALUE_TYPE_STRING {
                return Err(WRONG_TYPE);
            }
            // The shard write lock protects the slot throughout read and replacement.
            let ptr = unsafe { resolve_slot_ptr(entry.slab_block_id) }.ok_or(SLOT_ERROR)?;
            let value = unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
            let number = std::str::from_utf8(value)
                .ok()
                .and_then(|text| text.parse::<i64>().ok())
                .ok_or(INTEGER_ERROR)?;
            (number, entry.expire_at_secs)
        } else {
            (0, 0)
        };
        let next = match arithmetic {
            Arithmetic::Add(delta) => current.checked_add(delta),
            Arithmetic::Subtract(delta) => current.checked_sub(delta),
        }
        .ok_or("ERR increment or decrement would overflow")?;
        store_string(
            shard,
            pool,
            hash,
            key,
            next.to_string().as_bytes(),
            expires,
            wheel,
        )?;
        Ok(next)
    });
    match result {
        Ok(value) => encode_integer(write_buf, value),
        Err(error) => encode_error(write_buf, error),
    }
}

/// Executes Redis String primitive commands.
pub fn handle_strings(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    now_sec: u32,
    timing_wheel: Option<&mut HashedTimingWheel>,
) -> Result<bool, NetError> {
    match cmd {
        Command::Get { key } => {
            let h = hash_key(key);
            // All published entries belong to live pools and are retired under their shard lock.
            let found = unsafe {
                table.with_read_lock_entry(h, key, now_sec, |entry, value| {
                    if entry.value_type == VALUE_TYPE_STRING {
                        encode_bulk_string(write_buf, value);
                    } else {
                        encode_error(write_buf, WRONG_TYPE);
                    }
                })
            };
            if found.is_none() {
                encode_null(write_buf);
            }
        }
        Command::Set { key, value, ttl_ms } => {
            let hash = hash_key(key);
            let expires = ttl_ms.map_or(0, |ms| {
                let seconds = u32::try_from((ms / 1000).max(1)).unwrap_or(u32::MAX);
                now_sec.saturating_add(seconds)
            });
            let result = table.with_shard_mut(hash, |shard| {
                store_string(shard, pool, hash, key, value, expires, timing_wheel)
            });
            match result {
                Ok(()) => {
                    emit_aof(AofOp::Set, key, value, now_sec);
                    encode_simple_string(write_buf, "OK");
                }
                Err(error) => encode_error(write_buf, error),
            }
        }
        Command::MGet { keys } => {
            encode_array_header(write_buf, keys.len());
            for key in keys {
                let hash = hash_key(key);
                // The shard read lock is held until encoding has copied the payload.
                let encoded = unsafe {
                    table.with_read_lock_entry(hash, key, now_sec, |entry, value| {
                        if entry.value_type == VALUE_TYPE_STRING {
                            encode_bulk_string(write_buf, value);
                            true
                        } else {
                            false
                        }
                    })
                };
                if encoded != Some(true) {
                    encode_null(write_buf);
                }
            }
        }
        Command::MSet { pairs } => {
            for (key, value) in pairs {
                let hash = hash_key(key);
                let result = table.with_shard_mut(hash, |shard| {
                    store_string(shard, pool, hash, key, value, 0, None)
                });
                if let Err(error) = result {
                    encode_error(write_buf, error);
                    return Ok(true);
                }
            }
            encode_simple_string(write_buf, "OK");
        }
        Command::Del { keys } => {
            let mut deleted = 0i64;
            for key in keys {
                let h = hash_key(key);
                if let Some(entry) = table.remove_key(h, key) {
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
                if table.lookup_key_checked(h, key, now_sec).is_some() {
                    count += 1;
                }
            }
            encode_integer(write_buf, count);
        }
        Command::Incr { key } => {
            execute_arithmetic(
                key,
                Arithmetic::Add(1),
                write_buf,
                table,
                pool,
                now_sec,
                timing_wheel,
            );
        }
        Command::Decr { key } => {
            execute_arithmetic(
                key,
                Arithmetic::Subtract(1),
                write_buf,
                table,
                pool,
                now_sec,
                timing_wheel,
            );
        }
        Command::IncrBy { key, delta } => {
            execute_arithmetic(
                key,
                Arithmetic::Add(delta),
                write_buf,
                table,
                pool,
                now_sec,
                timing_wheel,
            );
        }
        Command::DecrBy { key, delta } => {
            execute_arithmetic(
                key,
                Arithmetic::Subtract(delta),
                write_buf,
                table,
                pool,
                now_sec,
                timing_wheel,
            );
        }
        Command::Append { key, value } => {
            let hash = hash_key(key);
            let result = table.with_shard_mut(hash, |shard| {
                let (existing, expires) = if let Some(entry) =
                    shard.lookup_key_checked(hash, key, now_sec)
                {
                    if entry.value_type != VALUE_TYPE_STRING {
                        return Err(WRONG_TYPE);
                    }
                    // The shard write lock protects the old payload until the copy is complete.
                    let ptr = unsafe { resolve_slot_ptr(entry.slab_block_id) }.ok_or(SLOT_ERROR)?;
                    let bytes =
                        unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
                    (bytes, entry.expire_at_secs)
                } else {
                    (&[][..], 0)
                };
                let length = existing.len().saturating_add(value.len());
                if SlabClassType::for_size(length).is_none() {
                    return Err("ERR string exceeds maximum supported slab size (2 MB)");
                }
                let mut combined = Vec::with_capacity(length);
                combined.extend_from_slice(existing);
                combined.extend_from_slice(value);
                store_string(shard, pool, hash, key, &combined, expires, timing_wheel)?;
                Ok(length)
            });
            match result {
                Ok(length) => encode_integer(write_buf, length as i64),
                Err(error) => encode_error(write_buf, error),
            }
        }
        Command::Strlen { key } => {
            let h = hash_key(key);
            if let Some(entry) = table.lookup_key_checked(h, key, now_sec) {
                encode_integer(write_buf, entry.value_len as i64);
            } else {
                encode_integer(write_buf, 0);
            }
        }
        _ => unreachable!("handle_strings called with non-string command"),
    }

    Ok(true)
}
