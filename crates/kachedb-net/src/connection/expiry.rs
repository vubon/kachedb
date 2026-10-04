//! Key expiry and TTL command execution handlers (`EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`).

use kachedb_core::{HashedTimingWheel, SlabPool};
use kachedb_hash::{ShardedSwissTable, hash_key};
use kachedb_proto_resp::{Command, encode_integer};

use crate::aof_encode::{AofOp, emit_aof};
use crate::error::NetError;

/// Executes key TTL and expiration commands.
pub fn handle_expiry(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    now_sec: u32,
    mut timing_wheel: Option<&mut HashedTimingWheel>,
) -> Result<bool, NetError> {
    match cmd {
        Command::Expire { key, seconds } => {
            let h = hash_key(key);
            if seconds <= 0 {
                if let Some(entry) = table.remove(h) {
                    let _ = pool.deallocate(entry.slab_block_id);
                    emit_aof(AofOp::Del, key, &[], now_sec);
                    encode_integer(write_buf, 1);
                } else {
                    encode_integer(write_buf, 0);
                }
            } else {
                let expire_at_secs = if now_sec > 0 {
                    now_sec.saturating_add(seconds as u32)
                } else {
                    seconds as u32
                };
                let ok = table.update_ttl(h, expire_at_secs, now_sec);
                if ok {
                    emit_aof(
                        AofOp::Expire,
                        key,
                        expire_at_secs.to_string().as_bytes(),
                        now_sec,
                    );
                    if let Some(entry) = table.lookup_checked(h, now_sec) {
                        if let Some(ref mut wheel) = timing_wheel {
                            wheel.schedule(h, entry.slab_block_id, expire_at_secs);
                        }
                    }
                }
                encode_integer(write_buf, if ok { 1 } else { 0 });
            }
        }
        Command::PExpire { key, milliseconds } => {
            let h = hash_key(key);
            if milliseconds <= 0 {
                if let Some(entry) = table.remove(h) {
                    let _ = pool.deallocate(entry.slab_block_id);
                    emit_aof(AofOp::Del, key, &[], now_sec);
                    encode_integer(write_buf, 1);
                } else {
                    encode_integer(write_buf, 0);
                }
            } else {
                let secs = (milliseconds / 1000).max(1) as u32;
                let expire_at_secs = if now_sec > 0 {
                    now_sec.saturating_add(secs)
                } else {
                    secs
                };
                let ok = table.update_ttl(h, expire_at_secs, now_sec);
                if ok {
                    emit_aof(
                        AofOp::Expire,
                        key,
                        expire_at_secs.to_string().as_bytes(),
                        now_sec,
                    );
                    if let Some(entry) = table.lookup_checked(h, now_sec) {
                        if let Some(ref mut wheel) = timing_wheel {
                            wheel.schedule(h, entry.slab_block_id, expire_at_secs);
                        }
                    }
                }
                encode_integer(write_buf, if ok { 1 } else { 0 });
            }
        }
        Command::ExpireAt { key, timestamp } => {
            let h = hash_key(key);
            if now_sec > 0 && timestamp <= now_sec as i64 {
                if let Some(entry) = table.remove(h) {
                    let _ = pool.deallocate(entry.slab_block_id);
                    emit_aof(AofOp::Del, key, &[], now_sec);
                    encode_integer(write_buf, 1);
                } else {
                    encode_integer(write_buf, 0);
                }
            } else {
                let expire_at_secs = timestamp.max(0) as u32;
                let ok = table.update_ttl(h, expire_at_secs, now_sec);
                if ok {
                    emit_aof(
                        AofOp::Expire,
                        key,
                        expire_at_secs.to_string().as_bytes(),
                        now_sec,
                    );
                    if let Some(entry) = table.lookup_checked(h, now_sec) {
                        if let Some(ref mut wheel) = timing_wheel {
                            wheel.schedule(h, entry.slab_block_id, expire_at_secs);
                        }
                    }
                }
                encode_integer(write_buf, if ok { 1 } else { 0 });
            }
        }
        Command::PExpireAt { key, timestamp_ms } => {
            let h = hash_key(key);
            let ts_sec = timestamp_ms / 1000;
            if now_sec > 0 && ts_sec <= now_sec as i64 {
                if let Some(entry) = table.remove(h) {
                    let _ = pool.deallocate(entry.slab_block_id);
                    emit_aof(AofOp::Del, key, &[], now_sec);
                    encode_integer(write_buf, 1);
                } else {
                    encode_integer(write_buf, 0);
                }
            } else {
                let expire_at_secs = ts_sec.max(0) as u32;
                let ok = table.update_ttl(h, expire_at_secs, now_sec);
                if ok {
                    emit_aof(
                        AofOp::Expire,
                        key,
                        expire_at_secs.to_string().as_bytes(),
                        now_sec,
                    );
                    if let Some(entry) = table.lookup_checked(h, now_sec) {
                        if let Some(ref mut wheel) = timing_wheel {
                            wheel.schedule(h, entry.slab_block_id, expire_at_secs);
                        }
                    }
                }
                encode_integer(write_buf, if ok { 1 } else { 0 });
            }
        }
        Command::Ttl { key } => {
            let h = hash_key(key);
            let ttl = table.get_ttl(h, now_sec);
            encode_integer(write_buf, ttl);
        }
        Command::PTtl { key } => {
            let h = hash_key(key);
            let ttl = table.get_ttl(h, now_sec);
            if ttl > 0 {
                encode_integer(write_buf, ttl * 1000);
            } else {
                encode_integer(write_buf, ttl);
            }
        }
        Command::Persist { key } => {
            let h = hash_key(key);
            let ok = table.persist(h, now_sec);
            encode_integer(write_buf, if ok { 1 } else { 0 });
        }
        _ => unreachable!("handle_expiry called with non-expiry command"),
    }

    Ok(true)
}
