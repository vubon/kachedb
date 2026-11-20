//! Orders network mutations and records the state they published.

use kachedb_core::{HashedTimingWheel, SlabPool};
use kachedb_hash::{HashSlotFrame, MAX_KEY_LEN, ShardedSwissTable, VALUE_TYPE_HASH, hash_key};
use kachedb_proto_resp::{Command, encode_error};
use kachedb_vector::VectorIndexRegistry;
use smallvec::SmallVec;

use super::{ClientState, Connection};
use crate::aof_encode::{
    AofOp, CHECKED_FRAME_OVERHEAD, encode_checked_frame, encode_checked_frame_with, mset_record_len,
};
use crate::durability::{AdmissionError, dispatch, needs_commit_fence, with_commit_fence};
use crate::error::NetError;

const SLOT_LIMIT: usize = 2 * 1024 * 1024;

enum Record<'a> {
    Key {
        key: &'a [u8],
        max_value: usize,
    },
    Batch(Vec<(&'a [u8], &'a [u8])>),
    Command {
        wire: &'a [u8],
        deadlines: SmallVec<[u32; 4]>,
    },
}

impl<'a> Record<'a> {
    fn for_command(command: &Command<'a>, wire: &'a [u8], now: u32) -> Option<Self> {
        let deadline = |ttl: Option<u32>| ttl.map_or(0, |ttl| now.saturating_add(ttl));
        let (key, max_value) = match command {
            Command::Set { key, value, .. } => (*key, value.len()),
            Command::Append { key, .. }
            | Command::Expire { key, .. }
            | Command::PExpire { key, .. }
            | Command::ExpireAt { key, .. }
            | Command::PExpireAt { key, .. }
            | Command::Persist { key } => (*key, SLOT_LIMIT),
            Command::Incr { key }
            | Command::Decr { key }
            | Command::IncrBy { key, .. }
            | Command::DecrBy { key, .. } => (*key, 20),
            Command::HSet { key, .. } | Command::HDel { key, .. } => (*key, 4096),
            Command::MSet { pairs } => return Some(Self::Batch(pairs.to_vec())),
            Command::VAdd { ttl_sec, .. } => {
                return Some(Self::Command {
                    wire,
                    deadlines: smallvec::smallvec![deadline(*ttl_sec)],
                });
            }
            Command::VAddBatch { items, .. } => {
                return Some(Self::Command {
                    wire,
                    deadlines: items.iter().map(|item| deadline(item.ttl_sec)).collect(),
                });
            }
            Command::Del { .. }
            | Command::FlushDb
            | Command::FlushAll
            | Command::VDel { .. }
            | Command::VIndexCreate { .. }
            | Command::VIndexDrop { .. } => {
                return Some(Self::Command {
                    wire,
                    deadlines: SmallVec::new(),
                });
            }
            _ => return None,
        };
        Some(Self::Key { key, max_value })
    }

    fn reservation_size(&self) -> Option<usize> {
        match self {
            Self::Key { key, max_value } => {
                (CHECKED_FRAME_OVERHEAD + 5 + key.len()).checked_add(*max_value)
            }
            Self::Batch(pairs) => mset_record_len(pairs),
            Self::Command { wire, deadlines } => deadlines
                .len()
                .checked_mul(4)?
                .checked_add(CHECKED_FRAME_OVERHEAD + 8)?
                .checked_add(wire.len()),
        }
    }

    fn encode(&self, table: &ShardedSwissTable, now: u32, response: &[u8]) -> Vec<u8> {
        match self {
            Self::Key { key, .. } => {
                // The commit fence excludes all competing writers. The shard
                // read lock keeps the indexed slot alive through the copy.
                let frame = unsafe {
                    table.with_read_lock_entry(hash_key(key), key, 0, |entry, bytes| {
                        let append_metadata = |payload: &mut Vec<u8>| {
                            payload.push(entry.value_type);
                            payload.extend_from_slice(&entry.expire_at_secs.to_le_bytes());
                        };
                        if entry.value_type == VALUE_TYPE_HASH {
                            let mut payload = Vec::with_capacity(5 + bytes.len());
                            append_metadata(&mut payload);
                            HashSlotFrame::encode_pairs(bytes, &mut payload);
                            encode_checked_frame(AofOp::Put, key, &payload, now)
                        } else {
                            encode_checked_frame_with(
                                AofOp::Put,
                                key,
                                bytes.len() + 5,
                                now,
                                |payload| {
                                    append_metadata(payload);
                                    payload.extend_from_slice(bytes);
                                },
                            )
                        }
                    })
                };
                frame.unwrap_or_else(|| encode_checked_frame(AofOp::Del, key, &[], now))
            }
            Self::Batch(pairs) => {
                let length =
                    mset_record_len(pairs).expect("admitted MSET") - CHECKED_FRAME_OVERHEAD;
                encode_checked_frame_with(AofOp::MSet, &[], length, now, |payload| {
                    payload.extend_from_slice(&(pairs.len() as u32).to_le_bytes());
                    for (key, value) in pairs {
                        payload.extend_from_slice(&(key.len() as u16).to_le_bytes());
                        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
                        payload.extend_from_slice(key);
                        payload.extend_from_slice(value);
                    }
                })
            }
            Self::Command { wire, deadlines } => {
                let length = 8 + deadlines.len() * 4 + wire.len();
                encode_checked_frame_with(AofOp::Command, &[], length, now, |payload| {
                    payload.extend_from_slice(&crc32fast::hash(response).to_le_bytes());
                    payload.extend_from_slice(&(deadlines.len() as u32).to_le_bytes());
                    for deadline in deadlines {
                        payload.extend_from_slice(&deadline.to_le_bytes());
                    }
                    payload.extend_from_slice(wire);
                })
            }
        }
    }
}

fn oversized_vector_keys(command: &Command<'_>) -> bool {
    let too_long = |key: &[u8]| key.len() > MAX_KEY_LEN;
    match command {
        Command::VAdd {
            index,
            id,
            parent_key,
            ..
        } => too_long(index) || too_long(id) || parent_key.is_some_and(too_long),
        Command::VAddBatch { index, items } => {
            too_long(index) || items.iter().any(|item| too_long(item.id))
        }
        Command::VIndexCreate { name, .. } => too_long(name),
        _ => false,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn execute<'a>(
    command: Command<'a>,
    wire: &'a [u8],
    output: &mut Vec<u8>,
    table: &ShardedSwissTable,
    pool: &mut SlabPool,
    now: u32,
    vectors: &VectorIndexRegistry,
    wheel: Option<&mut HashedTimingWheel>,
    client: &mut ClientState,
) -> Result<bool, NetError> {
    let record = if needs_commit_fence() && (client.requirepass.is_none() || client.authenticated) {
        Record::for_command(&command, wire, now)
    } else {
        None
    };
    let Some(record) = record else {
        return Connection::execute_command_full(
            command,
            output,
            table,
            pool,
            now,
            vectors,
            wheel,
            Some(client),
        );
    };
    if Connection::has_oversized_write_key(&command) || oversized_vector_keys(&command) {
        encode_error(output, "ERR key exceeds the maximum length of 65535 bytes");
        return Ok(true);
    }

    let permit = match dispatch() {
        Some(aof) => {
            match record
                .reservation_size()
                .ok_or(AdmissionError::Oversized)
                .and_then(|size| aof.reserve(size))
            {
                Ok(permit) => Some(permit),
                Err(error) => {
                    encode_error(output, &error.to_string());
                    return Ok(true);
                }
            }
        }
        None => None,
    };

    let start = output.len();
    let (keep_alive, completion) = with_commit_fence(|| {
        if let Some(aof) = dispatch()
            && !aof.is_healthy()
        {
            encode_error(output, &AdmissionError::Unavailable.to_string());
            return Ok((true, Ok(None)));
        }
        let keep_alive = Connection::execute_command_full(
            command,
            output,
            table,
            pool,
            now,
            vectors,
            wheel,
            Some(client),
        )?;
        let completion = match permit {
            Some(permit) if !output[start..].starts_with(b"-") => {
                permit.submit(record.encode(table, now, &output[start..]))
            }
            _ => Ok(None),
        };
        Ok::<_, NetError>((keep_alive, completion))
    })?;

    if let Err(error) = completion.and_then(|wait| {
        wait.map_or(Ok(()), |receiver| {
            receiver.recv().map_err(|_| AdmissionError::Uncertain)
        })
    }) {
        output.truncate(start);
        encode_error(output, &error.to_string());
    }
    Ok(keep_alive)
}

/// Accepts only the mutations persisted as command records.
pub fn replayable_command(command: &Command<'_>) -> bool {
    matches!(
        command,
        Command::Del { .. }
            | Command::FlushDb
            | Command::FlushAll
            | Command::VAdd { .. }
            | Command::VAddBatch { .. }
            | Command::VDel { .. }
            | Command::VIndexCreate { .. }
            | Command::VIndexDrop { .. }
    )
}

/// Replays a batch using exactly the live command's atomic publication path.
pub fn replay_mset(
    table: &ShardedSwissTable,
    pool: &mut impl kachedb_core::pool::SlotAllocator,
    pairs: &[(&[u8], &[u8])],
) -> Result<(), &'static str> {
    if pairs.is_empty() || pairs.iter().any(|(key, _)| key.len() > MAX_KEY_LEN) {
        return Err("invalid MSET batch");
    }
    super::strings::store_string_batch(table, pool, pairs)
}
