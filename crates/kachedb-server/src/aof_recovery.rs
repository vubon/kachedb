//! Streaming replay and explicit repair of an append-only journal.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, ErrorKind, Read};
use std::path::{Path, PathBuf};

use kachedb_core::{
    CoreError, HashedTimingWheel, SlabClassType, pool::SlotAllocator, resolve_slot_ptr,
};
use kachedb_hash::{
    HashSlotFrame, ShardedSwissTable, VALUE_TYPE_HASH, VALUE_TYPE_STRING, hash_key,
};
use kachedb_net::Connection;
use kachedb_net::aof_encode::{
    AOF_HEADER_OVERHEAD, AOF_MAGIC, CHECKED_FRAME_MAGIC, CHECKED_HEADER_SIZE, MAX_AOF_RECORD_SIZE,
};
use kachedb_net::connection::{replay_mset, replayable_command};
use kachedb_proto_resp::{Command, parse_command_frame};
use kachedb_vector::VectorIndexRegistry;

use crate::aof::{AofError, AofFrame, AofOp, decode_frame};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LogState {
    pub generation: Option<[u8; 16]>,
    pub requires_snapshot: bool,
    pub sequence: u64,
    pub valid_end: u64,
    pub incomplete_tail: bool,
}

pub fn inspect(path: &Path) -> Result<LogState, AofError> {
    let mut state = LogState::default();
    scan(path, &mut state, |_, _| Ok(()))?;
    Ok(state)
}

pub fn replay_from(
    path: &Path,
    after_sequence: u64,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
) -> Result<LogState, AofError> {
    replay_with_hash_from(path, after_sequence, table, pool, vectors, hash_key)
}

fn replay_with_hash_from(
    path: &Path,
    after_sequence: u64,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
    key_hash: impl Fn(&[u8]) -> u64,
) -> Result<LogState, AofError> {
    let mut state = LogState::default();
    let mut wheel = HashedTimingWheel::new(0);
    table.schedule_restored_expirations(&mut wheel);
    let mut expired = Vec::new();
    let mut scheduled = 0;
    scan(path, &mut state, |sequence, frame| {
        if sequence <= after_sequence {
            return Ok(());
        }
        let now = u32::try_from(frame.timestamp_sec)
            .map_err(|_| AofError::InvalidRecord("invalid evaluation time"))?;
        wheel.advance_expired_entries(now, &mut expired);
        for event in expired.drain(..) {
            if let (Some(key), Some(generation)) = (event.key.as_deref(), event.generation)
                && let Some(entry) = table.remove_key_if_expired(
                    event.key_hash,
                    key,
                    event.slab_block_id,
                    event.expire_at_sec,
                    generation,
                    now,
                )
            {
                pool.deallocate(entry.slab_block_id)?;
            }
        }
        apply_frame(frame, table, pool, vectors, &key_hash, now)?;
        if matches!(
            frame.op,
            AofOp::Put | AofOp::Expire | AofOp::HSet | AofOp::HDel
        ) {
            let hash = key_hash(frame.key);
            if let Some(entry) = table.lookup_key_checked(hash, frame.key, 0)
                && entry.expire_at_secs != 0
            {
                wheel.schedule_key(hash, frame.key, entry.slab_block_id, entry.expire_at_secs);
                scheduled += 1;
                // Repeated TTL changes must not retain an event per historical record.
                if scheduled > 4096 + table.len() {
                    wheel = HashedTimingWheel::new(now);
                    table.schedule_restored_expirations(&mut wheel);
                    scheduled = 0;
                }
            }
        }
        Ok(())
    })?;
    Ok(state)
}

#[cfg(test)]
pub fn replay(
    path: &Path,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
) -> Result<usize, AofError> {
    Ok(replay_from(path, 0, table, pool, vectors)?.sequence as usize)
}

#[cfg(test)]
fn replay_with_hash(
    path: &Path,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
    hash: impl Fn(&[u8]) -> u64,
) -> Result<usize, AofError> {
    Ok(replay_with_hash_from(path, 0, table, pool, vectors, hash)?.sequence as usize)
}

pub fn trim_incomplete_tail(path: &Path, state: LogState) -> Result<(), AofError> {
    if state.incomplete_tail {
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(state.valid_end)?;
        file.sync_all()?;
    }
    Ok(())
}

/// Repairs only framing damage and keeps a byte-for-byte copy of the source.
pub fn repair(path: &Path) -> Result<Option<PathBuf>, AofError> {
    let _lock = crate::aof::open_locked(path, false)?;
    let mut state = LogState::default();
    match scan(path, &mut state, |_, _| Ok(())) {
        Ok(()) if !state.incomplete_tail => return Ok(None),
        Ok(()) => {}
        Err(AofError::AtOffset { .. }) => {}
        Err(error) => return Err(error),
    }
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut backup_path = path.as_os_str().to_owned();
    backup_path.push(format!(".corrupt.{suffix}"));
    let backup_path = PathBuf::from(backup_path);
    let mut original = File::open(path)?;
    let mut backup = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&backup_path)?;
    std::io::copy(&mut original, &mut backup)?;
    backup.sync_all()?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    state.incomplete_tail = true;
    trim_incomplete_tail(path, state)?;
    Ok(Some(backup_path))
}

fn scan(
    path: &Path,
    state: &mut LogState,
    mut apply: impl FnMut(u64, &AofFrame<'_>) -> Result<(), AofError>,
) -> Result<(), AofError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let mut reader = BufReader::new(file);
    let mut bytes = Vec::new();
    loop {
        let outcome = next_record(&mut reader, &mut bytes);
        let at_offset = |source| AofError::AtOffset {
            offset: state.valid_end,
            source: Box::new(source),
        };
        match outcome {
            Ok(Record::Eof) => return Ok(()),
            Ok(Record::TornPayload) => {
                state.incomplete_tail = true;
                return Ok(());
            }
            Ok(Record::Frame {
                frame,
                size,
                checked,
            }) => {
                if frame.op == AofOp::Generation {
                    if !checked
                        || state.generation.is_some()
                        || !frame.key.is_empty()
                        || frame.value.len() != 17
                        || frame.value[16] > 1
                        || (frame.value[16] == 1 && state.sequence != 0)
                    {
                        return Err(at_offset(AofError::InvalidRecord(
                            "invalid generation marker",
                        )));
                    }
                    state.generation = Some(frame.value[..16].try_into().unwrap());
                    state.requires_snapshot = frame.value[16] == 1;
                } else {
                    if checked != state.generation.is_some()
                        || (!checked
                            && matches!(frame.op, AofOp::Put | AofOp::MSet | AofOp::Command))
                    {
                        return Err(at_offset(AofError::InvalidRecord(
                            "record does not match log generation",
                        )));
                    }
                    state.sequence = state.sequence.checked_add(1).ok_or_else(|| {
                        at_offset(AofError::InvalidRecord("record sequence overflow"))
                    })?;
                    apply(state.sequence, &frame).map_err(at_offset)?;
                }
                state.valid_end += size as u64;
            }
            Err(error @ AofError::Io(_)) => return Err(error),
            Err(error) => return Err(at_offset(error)),
        }
    }
}

enum Record<'a> {
    Eof,
    TornPayload,
    Frame {
        frame: AofFrame<'a>,
        size: usize,
        checked: bool,
    },
}

fn fixed_read(reader: &mut impl Read, buffer: &mut [u8]) -> Result<(), AofError> {
    reader.read_exact(buffer).map_err(|error| {
        if error.kind() == ErrorKind::UnexpectedEof {
            AofError::InvalidRecord("incomplete or ambiguous frame header")
        } else {
            error.into()
        }
    })
}

fn next_record<'a>(reader: &mut impl Read, bytes: &'a mut Vec<u8>) -> Result<Record<'a>, AofError> {
    let mut magic = [0; 2];
    loop {
        match reader.read(&mut magic[..1]) {
            Ok(0) => return Ok(Record::Eof),
            Ok(_) => break,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error.into()),
        }
    }
    fixed_read(reader, &mut magic[1..])?;
    let (size, checked) = if magic == CHECKED_FRAME_MAGIC {
        let mut header = [0; CHECKED_HEADER_SIZE];
        header[..2].copy_from_slice(&magic);
        fixed_read(reader, &mut header[2..])?;
        let length = u32::from_le_bytes(header[2..6].try_into().unwrap()) as usize;
        let checksum = u32::from_le_bytes(header[6..10].try_into().unwrap());
        if crc32fast::hash(&header[..6]) != checksum
            || !(AOF_HEADER_OVERHEAD..=MAX_AOF_RECORD_SIZE - CHECKED_HEADER_SIZE).contains(&length)
        {
            return Err(AofError::InvalidRecord(
                "bad checked frame length or header checksum",
            ));
        }
        if length > bytes.capacity() {
            bytes.reserve_exact(length - bytes.len());
        }
        bytes.resize(length, 0);
        match reader.read_exact(bytes) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                return Ok(Record::TornPayload);
            }
            Err(error) => return Err(error.into()),
        }
        (length + CHECKED_HEADER_SIZE, true)
    } else if magic == AOF_MAGIC {
        bytes.resize(13, 0);
        bytes[..2].copy_from_slice(&magic);
        fixed_read(reader, &mut bytes[2..])?;
        let key_len = u16::from_le_bytes(bytes[11..13].try_into().unwrap()) as usize;
        let start = 13 + key_len + 4;
        bytes.resize(start, 0);
        fixed_read(reader, &mut bytes[13..])?;
        let value_len = u32::from_le_bytes(bytes[start - 4..start].try_into().unwrap()) as usize;
        let length = start
            .checked_add(value_len)
            .and_then(|size| size.checked_add(4))
            .filter(|&size| size <= MAX_AOF_RECORD_SIZE)
            .ok_or(AofError::InvalidRecord("legacy frame exceeds record limit"))?;
        if length > bytes.capacity() {
            bytes.reserve_exact(length - bytes.len());
        }
        bytes.resize(length, 0);
        fixed_read(reader, &mut bytes[start..])?;
        (length, false)
    } else {
        return Err(AofError::CorruptMagic);
    };
    let (frame, consumed) =
        decode_frame(bytes)?.ok_or(AofError::InvalidRecord("inconsistent frame lengths"))?;
    if consumed != bytes.len() {
        return Err(AofError::InvalidRecord(
            "unexpected trailing bytes in frame",
        ));
    }
    Ok(Record::Frame {
        frame,
        size,
        checked,
    })
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], AofError> {
    let (prefix, rest) = bytes
        .split_at_checked(count)
        .ok_or(AofError::InvalidRecord("truncated mutation payload"))?;
    *bytes = rest;
    Ok(prefix)
}

fn read_u16(bytes: &mut &[u8]) -> Result<usize, AofError> {
    Ok(u16::from_le_bytes(take(bytes, 2)?.try_into().unwrap()) as usize)
}

fn read_u32(bytes: &mut &[u8]) -> Result<u32, AofError> {
    Ok(u32::from_le_bytes(take(bytes, 4)?.try_into().unwrap()))
}

type FieldPair<'a> = (&'a [u8], &'a [u8]);

fn pairs_from_record(mut bytes: &[u8], batch: bool) -> Result<Vec<FieldPair<'_>>, AofError> {
    let count = if batch {
        read_u32(&mut bytes)? as usize
    } else {
        read_u16(&mut bytes)?
    };
    let minimum_pair = if batch { 6 } else { 4 };
    if count > bytes.len() / minimum_pair {
        return Err(AofError::InvalidRecord("invalid pair count"));
    }
    let mut pairs = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = read_u16(&mut bytes)?;
        let (key, len) = if batch {
            let len = read_u32(&mut bytes)? as usize;
            (take(&mut bytes, key_len)?, len)
        } else {
            let key = take(&mut bytes, key_len)?;
            (key, read_u16(&mut bytes)?)
        };
        pairs.push((key, take(&mut bytes, len)?));
    }
    if !bytes.is_empty() {
        return Err(AofError::InvalidRecord("unexpected trailing pair bytes"));
    }
    Ok(pairs)
}

/// Installs a named persisted value with its absolute expiration time.
pub(crate) fn restore_value(
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    hash: u64,
    key: &[u8],
    value_type: u8,
    value: &[u8],
    deadline: u32,
) -> Result<(), AofError> {
    let (class, size) = match value_type {
        VALUE_TYPE_STRING => (
            SlabClassType::for_size(value.len())
                .ok_or(AofError::InvalidRecord("invalid string size"))?,
            value.len(),
        ),
        VALUE_TYPE_HASH => {
            let pairs = pairs_from_record(value, false)?;
            let needed = HashSlotFrame::needed_bytes(None, &pairs);
            let class = kachedb_net::connection::hash_slab_class(needed)
                .ok_or(AofError::InvalidRecord("invalid hash size"))?;
            (class, class.slot_bytes())
        }
        _ => return Err(AofError::InvalidRecord("unknown value type")),
    };
    // Recovery owns the table. A same-class overwrite can reuse the indexed
    // slot, including when the live command changed only the deadline.
    let reusable = table
        .lookup_key_checked(hash, key, 0)
        .filter(|entry| SlabClassType::for_size(entry.value_len as usize) == Some(class))
        .map(|entry| entry.slab_block_id);
    let block = match reusable {
        Some(block) => block,
        None => pool.reserve(hash, class)?,
    };
    let result = (|| {
        // No workers can observe the slot until recovery succeeds.
        let ptr =
            unsafe { resolve_slot_ptr(block) }.ok_or(CoreError::InvalidBlockId { id: block.0 })?;
        if value_type == VALUE_TYPE_HASH {
            let slot = unsafe { std::slice::from_raw_parts_mut(ptr, size) };
            HashSlotFrame::build_from_pairs(slot, value)
                .map_err(|error| AofError::Apply(error.to_string()))?;
        } else {
            unsafe { std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, size) };
        }
        table
            .insert_typed_with_key(hash, key, block, size as u32, deadline, value_type)
            .map_err(|_| AofError::InvalidRecord("cannot install persisted key"))
    })();
    match result {
        Ok(Some(old)) if old != block => pool.deallocate(old)?,
        Ok(_) => {}
        Err(error) => {
            if reusable.is_none() {
                pool.deallocate(block)?;
            }
            return Err(error);
        }
    }
    Ok(())
}

fn run_command(
    command: Command<'_>,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
    now: u32,
) -> Result<Vec<u8>, AofError> {
    let mut response = Vec::new();
    Connection::execute_command_full(
        command,
        &mut response,
        table,
        pool,
        now,
        vectors,
        None,
        None,
    )
    .map_err(|error| AofError::Apply(error.to_string()))?;
    if response.starts_with(b"-") {
        return Err(AofError::Apply(
            String::from_utf8_lossy(&response).trim().to_owned(),
        ));
    }
    Ok(response)
}

fn apply_frame(
    frame: &AofFrame<'_>,
    table: &ShardedSwissTable,
    pool: &mut impl SlotAllocator,
    vectors: &VectorIndexRegistry,
    key_hash: &impl Fn(&[u8]) -> u64,
    now: u32,
) -> Result<(), AofError> {
    match frame.op {
        AofOp::Set => restore_value(
            table,
            pool,
            key_hash(frame.key),
            frame.key,
            VALUE_TYPE_STRING,
            frame.value,
            0,
        )?,
        AofOp::Put => {
            let mut value = frame.value;
            let value_type = take(&mut value, 1)?[0];
            let deadline = read_u32(&mut value)?;
            restore_value(
                table,
                pool,
                key_hash(frame.key),
                frame.key,
                value_type,
                value,
                deadline,
            )?;
        }
        AofOp::Del => {
            if !frame.value.is_empty() {
                return Err(AofError::InvalidRecord("DEL record has a payload"));
            }
            if let Some(entry) = table.remove_key(key_hash(frame.key), frame.key) {
                pool.deallocate(entry.slab_block_id)?;
            }
        }
        AofOp::Expire => {
            let deadline = std::str::from_utf8(frame.value)
                .ok()
                .and_then(|s| s.parse::<u32>().ok())
                .ok_or(AofError::InvalidRecord("invalid legacy expiry deadline"))?;
            table.update_key_ttl(key_hash(frame.key), frame.key, deadline, now);
        }
        AofOp::MSet => {
            if !frame.key.is_empty() {
                return Err(AofError::InvalidRecord("MSET record has an envelope key"));
            }
            let pairs = pairs_from_record(frame.value, true)?;
            replay_mset(table, pool, &pairs).map_err(|error| AofError::Apply(error.to_owned()))?;
        }
        AofOp::Command => {
            if !frame.key.is_empty() {
                return Err(AofError::InvalidRecord(
                    "command record has an envelope key",
                ));
            }
            let mut wire = frame.value;
            let expected_reply = read_u32(&mut wire)?;
            let expiry_count = read_u32(&mut wire)? as usize;
            let expiry_bytes = expiry_count
                .checked_mul(4)
                .ok_or(AofError::InvalidRecord("invalid vector expiry count"))?;
            let mut expirations = take(&mut wire, expiry_bytes)?;
            let (command, consumed) = parse_command_frame(wire)
                .ok()
                .flatten()
                .ok_or(AofError::InvalidRecord("invalid recorded command"))?;
            let mut command =
                command.map_err(|_| AofError::InvalidRecord("invalid recorded arguments"))?;
            if consumed != wire.len() || !replayable_command(&command) {
                return Err(AofError::InvalidRecord("unexpected recorded command"));
            }
            let verify = !matches!(command, Command::Del { .. });
            // Vector adds use zero as their clock origin when given absolute deadlines.
            let command_time = match &mut command {
                Command::VAdd { ttl_sec, .. } => {
                    *ttl_sec = Some(read_u32(&mut expirations)?);
                    0
                }
                Command::VAddBatch { items, .. } => {
                    for item in items {
                        item.ttl_sec = Some(read_u32(&mut expirations)?);
                    }
                    0
                }
                _ => now,
            };
            if !expirations.is_empty() {
                return Err(AofError::InvalidRecord(
                    "unexpected vector expiration deadlines",
                ));
            }
            let response = run_command(command, table, pool, vectors, command_time)?;
            if verify && crc32fast::hash(&response) != expected_reply {
                return Err(AofError::Apply(
                    "replayed command returned a different result".into(),
                ));
            }
        }
        AofOp::HSet => {
            let pairs = pairs_from_record(frame.value, false)?;
            if pairs.is_empty() {
                return Err(AofError::InvalidRecord("empty HSET record"));
            }
            run_command(
                Command::HSet {
                    key: frame.key,
                    pairs: pairs.into_iter().collect(),
                },
                table,
                pool,
                vectors,
                now,
            )?;
        }
        AofOp::HDel => {
            let mut remaining = frame.value;
            let count = read_u16(&mut remaining)?;
            if count == 0 || count > remaining.len() / 2 {
                return Err(AofError::InvalidRecord("invalid HDEL count"));
            }
            let mut fields = smallvec::SmallVec::new();
            for _ in 0..count {
                let length = read_u16(&mut remaining)?;
                fields.push(take(&mut remaining, length)?);
            }
            if !remaining.is_empty() {
                return Err(AofError::InvalidRecord("unexpected trailing HDEL fields"));
            }
            run_command(
                Command::HDel {
                    key: frame.key,
                    fields,
                },
                table,
                pool,
                vectors,
                now,
            )?;
        }
        AofOp::VIndexCreate => {
            let mut value = frame.value;
            let dim = read_u32(&mut value)? as usize;
            if !value.is_empty() || dim == 0 {
                return Err(AofError::InvalidRecord("invalid legacy vector index"));
            }
            run_command(
                Command::VIndexCreate {
                    name: frame.key,
                    dim,
                    m: None,
                    ef_construction: None,
                    ef_search: None,
                    metric: None,
                    quantization: None,
                },
                table,
                pool,
                vectors,
                now,
            )?;
        }
        AofOp::VIndexDrop => {
            if !frame.value.is_empty() {
                return Err(AofError::InvalidRecord("unexpected VINDEX DROP payload"));
            }
            run_command(
                Command::VIndexDrop { name: frame.key },
                table,
                pool,
                vectors,
                now,
            )?;
        }
        AofOp::VAdd => {
            let Some(separator) = frame.key.iter().position(|&byte| byte == 0) else {
                return Err(AofError::InvalidRecord("missing vector index separator"));
            };
            let mut value = frame.value;
            let dim = read_u32(&mut value)? as usize;
            let size = dim
                .checked_mul(4)
                .ok_or(AofError::InvalidRecord("invalid vector dimension"))?;
            let vector_bytes = take(&mut value, size)?;
            run_command(
                Command::VAdd {
                    index: &frame.key[..separator],
                    id: &frame.key[separator + 1..],
                    dim,
                    vector_bytes,
                    payload: (!value.is_empty()).then_some(value),
                    ttl_sec: None,
                    tag_mask: 0,
                    parent_key: None,
                },
                table,
                pool,
                vectors,
                now,
            )?;
        }
        AofOp::Generation => return Err(AofError::InvalidRecord("unexpected generation marker")),
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::aof::encode_frame;
    use kachedb_core::SlabPool;
    use std::io::Write;
    use std::sync::Arc;

    fn test_path(label: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("kachedb-{label}-{}-{nanos}", std::process::id()))
    }

    #[test]
    fn checked_tail_and_explicit_repair_preserve_the_right_bytes() {
        use kachedb_net::aof_encode::{CHECKED_HEADER_SIZE, encode_checked_frame};

        let path = test_path("aof-tail");
        let mut marker = vec![7; 16];
        marker.push(0);
        let generation = encode_checked_frame(AofOp::Generation, &[], &marker, 0);
        let mut batch = 1_u32.to_le_bytes().to_vec();
        batch.extend_from_slice(&1_u16.to_le_bytes());
        batch.extend_from_slice(&1_u32.to_le_bytes());
        batch.extend_from_slice(b"kv");
        let full = encode_checked_frame(AofOp::MSet, &[], &batch, 100);

        // Incomplete body with a verified length may be trimmed. It publishes no keys.
        let torn = [&generation[..], &full[..CHECKED_HEADER_SIZE + 5]].concat();
        std::fs::write(&path, &torn).unwrap();
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let state = replay_from(&path, 0, &table, &mut pool, &VectorIndexRegistry::new()).unwrap();
        assert!(state.incomplete_tail);
        assert_eq!(state.sequence, 0);
        assert!(table.is_empty());
        assert_eq!(std::fs::read(&path).unwrap(), torn);
        trim_incomplete_tail(&path, state).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), generation);

        // Partial headers and complete frames with corrupted data require offline repair.
        let mut corrupt = full.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        for tail in [&full[..CHECKED_HEADER_SIZE - 1], corrupt.as_slice()] {
            let damaged = [&generation[..], tail].concat();
            std::fs::write(&path, &damaged).unwrap();
            assert!(inspect(&path).is_err());
            assert_eq!(std::fs::read(&path).unwrap(), damaged);
            let backup = repair(&path).unwrap().unwrap();
            assert_eq!(std::fs::read(&backup).unwrap(), damaged);
            assert_eq!(std::fs::read(&path).unwrap(), generation);
            std::fs::remove_file(backup).unwrap();
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn legacy_hash_recreation_uses_the_inclusive_expiration_boundary() {
        let path = test_path("aof-hash-expiry");
        let hash_pair = |field| [1, 0, 1, 0, field, 1, 0, b'v'];
        std::fs::write(
            &path,
            [
                encode_frame(AofOp::HSet, b"hash", &hash_pair(b'a'), 99),
                encode_frame(AofOp::Expire, b"hash", b"101", 100),
                encode_frame(AofOp::HSet, b"hash", &hash_pair(b'b'), 101),
            ]
            .concat(),
        )
        .unwrap();
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let vectors = VectorIndexRegistry::new();
        assert_eq!(replay(&path, &table, &mut pool, &vectors).unwrap(), 3);
        let mut reply = Vec::new();
        Connection::execute_command_full(
            Command::HGetAll { key: b"hash" },
            &mut reply,
            &table,
            &mut pool,
            101,
            &vectors,
            None,
            None,
        )
        .unwrap();
        assert_eq!(reply, b"*2\r\n$1\r\nb\r\n$1\r\nv\r\n");
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn restoring_a_deadline_does_not_need_a_second_full_size_slot() {
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let key = b"large";
        let hash = hash_key(key);
        let value = vec![b'x'; 2 * 1024 * 1024];
        restore_value(&table, &mut pool, hash, key, VALUE_TYPE_STRING, &value, 100).unwrap();
        assert!(pool.reserve_non_evicting(SlabClassType::Tensor2MB).is_err());
        restore_value(&table, &mut pool, hash, key, VALUE_TYPE_STRING, &value, 0).unwrap();
        assert_eq!(table.get_key_ttl(hash, key, 100), -1);
    }

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
    #[test]
    fn aof_replay_keeps_colliding_and_empty_keys_independent() {
        let path = std::env::temp_dir().join(format!(
            "test_aof_collision_{}.kaof",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let first = [b'a'; 64];
        let mut second = first;
        second[63] = b'b';
        std::fs::write(
            &path,
            [
                encode_frame(AofOp::Set, &first, b"first", 100),
                encode_frame(AofOp::Set, &second, b"second", 101),
                encode_frame(AofOp::Set, b"", b"empty", 102),
                encode_frame(AofOp::Expire, &second, b"200", 103),
                encode_frame(AofOp::Del, &first, b"", 104),
            ]
            .concat(),
        )
        .unwrap();

        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 4 * 1024 * 1024).unwrap();
        let vectors = VectorIndexRegistry::new();
        let collision = 42;
        assert_eq!(
            replay_with_hash(&path, &table, &mut pool, &vectors, |_| collision).unwrap(),
            5
        );
        assert_eq!(table.len(), 2);
        assert!(table.lookup_key_checked(collision, &first, 0).is_none());
        assert_eq!(table.get_key_ttl(collision, &second, 105), 95);
        assert_eq!(table.get_key_ttl(collision, b"", 105), -1);
        for (key, value) in [(second.as_slice(), b"second".as_slice()), (b"", b"empty")] {
            let entry = table.lookup_key_checked(collision, key, 105).unwrap();
            let ptr = unsafe { resolve_slot_ptr(entry.slab_block_id) }.unwrap();
            let restored = unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
            assert_eq!(restored, value);
        }
        std::fs::remove_file(path).unwrap();
    }
}
