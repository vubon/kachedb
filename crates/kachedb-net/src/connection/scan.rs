//! Keyspace cursor iteration handler (`SCAN`).

use kachedb_hash::{
    CompiledPattern, DEFAULT_SCAN_COUNT, MAX_PATTERN_LEN, MAX_SCAN_COUNT, ShardedSwissTable,
    VALUE_TYPE_HASH, VALUE_TYPE_STRING,
};
use kachedb_proto_resp::{Command, encode_array_header, encode_bulk_string, encode_error};

use crate::error::NetError;

/// Executes the `SCAN` command over the sharded SwissTable.
pub fn handle_scan(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    table: &ShardedSwissTable,
    now_sec: u32,
) -> Result<bool, NetError> {
    match cmd {
        Command::Scan {
            cursor,
            pattern,
            count,
            type_filter,
        } => {
            if count == Some(0) {
                encode_error(write_buf, "ERR syntax error");
                return Ok(true);
            }
            if let Some(pat) = pattern {
                if pat.len() > MAX_PATTERN_LEN {
                    encode_error(write_buf, "ERR pattern exceeds maximum length of 256 bytes");
                    return Ok(true);
                }
            }

            let target_count = count.unwrap_or(DEFAULT_SCAN_COUNT).clamp(1, MAX_SCAN_COUNT);
            let compiled = pattern.map(CompiledPattern::new);
            let type_code = match type_filter {
                None => None,
                Some(t) if t.eq_ignore_ascii_case(b"string") => Some(VALUE_TYPE_STRING),
                Some(t) if t.eq_ignore_ascii_case(b"hash") => Some(VALUE_TYPE_HASH),
                Some(_) => Some(u8::MAX),
            };

            let mut keys_payload = Vec::with_capacity(target_count * 24);
            let mut key_count = 0usize;
            let next_cursor = table.scan_batch(
                cursor,
                compiled.as_ref(),
                target_count,
                type_code,
                now_sec,
                |key| {
                    encode_bulk_string(&mut keys_payload, key);
                    key_count += 1;
                },
            );

            encode_array_header(write_buf, 2);
            let cursor_str = next_cursor.to_string();
            encode_bulk_string(write_buf, cursor_str.as_bytes());
            encode_array_header(write_buf, key_count);
            write_buf.extend_from_slice(&keys_payload);
        }
        _ => unreachable!("handle_scan called with non-scan command"),
    }

    Ok(true)
}
