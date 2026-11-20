//! Key expiry and TTL command execution handlers.

use kachedb_core::{HashedTimingWheel, pool::SlotAllocator};
use kachedb_hash::{ShardedSwissTable, hash_key};
use kachedb_proto_resp::{Command, encode_integer};

use crate::error::NetError;

/// Executes key TTL and expiration commands.
pub fn handle_expiry(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    now_sec: u32,
    timing_wheel: Option<&mut HashedTimingWheel>,
) -> Result<bool, NetError> {
    let seconds = |value: i64| u32::try_from(value).unwrap_or(u32::MAX);
    let (key, deadline) = match cmd {
        Command::Expire { key, seconds: ttl } => {
            (key, (ttl > 0).then(|| now_sec.saturating_add(seconds(ttl))))
        }
        Command::PExpire { key, milliseconds } => (
            key,
            (milliseconds > 0)
                .then(|| now_sec.saturating_add(seconds((milliseconds / 1000).max(1)))),
        ),
        Command::ExpireAt { key, timestamp } => (
            key,
            (timestamp > i64::from(now_sec)).then(|| seconds(timestamp)),
        ),
        Command::PExpireAt { key, timestamp_ms } => {
            let timestamp = timestamp_ms / 1000;
            (
                key,
                (timestamp > i64::from(now_sec)).then(|| seconds(timestamp)),
            )
        }
        Command::Ttl { key } | Command::PTtl { key } => {
            let ttl = table.get_key_ttl(hash_key(key), key, now_sec);
            let ttl = if ttl > 0 && matches!(cmd, Command::PTtl { .. }) {
                ttl * 1000
            } else {
                ttl
            };
            encode_integer(write_buf, ttl);
            return Ok(true);
        }
        Command::Persist { key } => {
            let updated = table.persist_key(hash_key(key), key, now_sec);
            encode_integer(write_buf, i64::from(updated));
            return Ok(true);
        }
        _ => unreachable!("handle_expiry called with non-expiry command"),
    };

    let hash = hash_key(key);
    let updated = if let Some(deadline) = deadline {
        table.with_shard_mut(hash, |shard| {
            let Some(block) = shard
                .lookup_key_checked(hash, key, now_sec)
                .map(|entry| entry.slab_block_id)
            else {
                return false;
            };
            if !shard.update_key_ttl(hash, key, deadline, now_sec) {
                return false;
            }
            // Capture the allocation's generation while the shard still protects it.
            if let Some(wheel) = timing_wheel {
                wheel.schedule_key(hash, key, block, deadline);
            }
            true
        })
    } else {
        let removed = table.with_shard_mut(hash, |shard| {
            shard.lookup_key_checked(hash, key, now_sec)?;
            shard.remove_key(hash, key)
        });
        if let Some(entry) = removed {
            let _ = pool.deallocate(entry.slab_block_id);
        }
        removed.is_some()
    };
    encode_integer(write_buf, i64::from(updated));
    Ok(true)
}
