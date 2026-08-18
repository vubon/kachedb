//! `kachedb-net` — Connection buffer management, RESP protocol decoding, and command execution.

use std::io::{Read, Write};

use kachedb_core::{SlabClassType, SlabPool};
use kachedb_hash::{SwissTable, hash_key};
use kachedb_proto_resp::{
    Command, encode_array_header, encode_bulk_string, encode_error, encode_integer, encode_null,
    encode_simple_string, parse_frame,
};

use crate::error::NetError;

/// Default buffer capacity for incoming connection stream (64 KB).
const READ_BUF_SIZE: usize = 64 * 1024;
/// Initial write buffer capacity.
const WRITE_BUF_SIZE: usize = 64 * 1024;

/// Connection state machine managing the read buffer, parsing incoming commands,
/// executing them directly against the per-core `SwissTable` and `SlabPool`,
/// and staging responses in the write buffer.
pub struct Connection {
    /// Inbound TCP byte buffer.
    read_buf: Vec<u8>,
    /// Read cursor offset in `read_buf`.
    read_pos: usize,
    /// Number of valid bytes currently in `read_buf`.
    read_len: usize,
    /// Outbound response buffer.
    write_buf: Vec<u8>,
    /// Write cursor offset in `write_buf`.
    write_pos: usize,
    /// Coarse-grained cached epoch timestamp in seconds.
    pub current_sec: u32,
}

impl Connection {
    /// Creates a new connection handler.
    pub fn new() -> Self {
        let now_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as u32;

        Self {
            read_buf: vec![0u8; READ_BUF_SIZE],
            read_pos: 0,
            read_len: 0,
            write_buf: Vec::with_capacity(WRITE_BUF_SIZE),
            write_pos: 0,
            current_sec: now_sec,
        }
    }

    /// Updates the coarse-grained current second timestamp without a syscall.
    #[inline(always)]
    pub fn set_current_sec(&mut self, now_sec: u32) {
        self.current_sec = now_sec;
    }

    /// Reads available bytes from `stream` into the internal `read_buf`.
    /// Returns number of bytes read, or 0 if EOF (client disconnected).
    pub fn read_from_stream(&mut self, stream: &mut impl Read) -> Result<usize, NetError> {
        // Compact buffer if needed
        if self.read_pos > 0 {
            if self.read_pos < self.read_len {
                self.read_buf.copy_within(self.read_pos..self.read_len, 0);
                self.read_len -= self.read_pos;
            } else {
                self.read_len = 0;
            }
            self.read_pos = 0;
        }

        if self.read_len >= self.read_buf.len() {
            // Buffer is full — expand dynamically if needed for very large payloads
            self.read_buf.resize(self.read_buf.len() * 2, 0);
        }

        let n = stream.read(&mut self.read_buf[self.read_len..])?;
        if n == 0 {
            return Err(NetError::ConnectionClosed);
        }

        self.read_len += n;
        Ok(n)
    }

    /// Processes all complete RESP frames currently in `read_buf`.
    ///
    /// Executes decoded commands directly against `table` and `pool`.
    /// Returns `Ok(true)` if connection should stay open, or `Ok(false)` on `QUIT`.
    pub fn process_incoming(
        &mut self,
        table: &mut SwissTable,
        pool: &mut SlabPool,
    ) -> Result<bool, NetError> {
        loop {
            let slice = &self.read_buf[self.read_pos..self.read_len];
            if slice.is_empty() {
                break;
            }

            match parse_frame(slice)? {
                Some((frame, consumed)) => {
                    self.read_pos += consumed;
                    let cmd = Command::from_frame(frame)?;
                    let keep_alive = Self::execute_command_with_time(
                        cmd,
                        &mut self.write_buf,
                        table,
                        pool,
                        self.current_sec,
                    )?;
                    if !keep_alive {
                        return Ok(false);
                    }
                }
                None => break, // incomplete frame, wait for more data
            }
        }

        Ok(true)
    }

    /// Executes a single strongly-typed command against local memory structures (unconstrained time).
    #[inline]
    pub fn execute_command(
        cmd: Command<'_>,
        write_buf: &mut Vec<u8>,
        table: &mut SwissTable,
        pool: &mut SlabPool,
    ) -> Result<bool, NetError> {
        Self::execute_command_with_time(cmd, write_buf, table, pool, 0)
    }

    /// Executes a command with explicit `now_sec` for deterministic TTL evaluation.
    pub fn execute_command_with_time(
        cmd: Command<'_>,
        write_buf: &mut Vec<u8>,
        table: &mut SwissTable,
        pool: &mut SlabPool,
        now_sec: u32,
    ) -> Result<bool, NetError> {
        match cmd {
            Command::Ping { message } => match message {
                Some(msg) => encode_bulk_string(write_buf, msg),
                None => encode_simple_string(write_buf, "PONG"),
            },
            Command::Get { key } => {
                let h = hash_key(key);
                if let Some(entry) = table.lookup_checked(h, now_sec) {
                    if let Ok(ptr) = unsafe { pool.slot_ptr(entry.slab_block_id) } {
                        let val_slice = unsafe {
                            std::slice::from_raw_parts(ptr, entry.value_len as usize)
                        };
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
                        let block_id = pool.allocate(class)?;
                        let slot_ptr = unsafe { pool.slot_ptr(block_id)? };

                        // Copy raw payload into slab slot (cache-line aligned destination)
                        unsafe {
                            std::ptr::copy_nonoverlapping(
                                value.as_ptr(),
                                slot_ptr,
                                val_len,
                            );
                        }

                        let expire_at_secs = ttl_ms
                            .map(|ms| {
                                let secs = (ms / 1000).max(1) as u32;
                                if now_sec > 0 { now_sec + secs } else { secs }
                            })
                            .unwrap_or(0);

                        let h = hash_key(key);
                        let _ = table.insert_with_ttl(h, block_id, val_len as u32, expire_at_secs);
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
                        if let Ok(ptr) = unsafe { pool.slot_ptr(entry.slab_block_id) } {
                            let val_slice = unsafe {
                                std::slice::from_raw_parts(ptr, entry.value_len as usize)
                            };
                            encode_bulk_string(write_buf, val_slice);
                            continue;
                        }
                    }
                    encode_null(write_buf);
                }
            }
            Command::Del { keys } => {
                let mut deleted = 0i64;
                for key in keys {
                    let h = hash_key(key);
                    if let Some(entry) = table.remove(h) {
                        let _ = pool.deallocate(entry.slab_block_id);
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
            Command::CommandDoc => {
                encode_simple_string(write_buf, "OK");
            }
            Command::Quit => {
                encode_simple_string(write_buf, "OK");
                return Ok(false);
            }
            Command::Unknown { name } => {
                let name_str = std::str::from_utf8(name).unwrap_or("unknown");
                encode_error(
                    write_buf,
                    &format!("ERR unknown command '{name_str}'"),
                );
            }
        }

        Ok(true)
    }

    /// Flushes staged responses from `write_buf` out to `stream`.
    pub fn flush_to_stream(&mut self, stream: &mut impl Write) -> Result<usize, NetError> {
        let remaining = &self.write_buf[self.write_pos..];
        if remaining.is_empty() {
            return Ok(0);
        }

        let n = stream.write(remaining)?;
        self.write_pos += n;

        if self.write_pos >= self.write_buf.len() {
            self.write_buf.clear();
            self.write_pos = 0;
        }

        Ok(n)
    }

    /// Returns `true` if there are pending bytes to write.
    #[inline]
    pub fn has_pending_writes(&self) -> bool {
        self.write_pos < self.write_buf.len()
    }

    // ── io_uring-compatible API (Improvement 2) ───────────────────────────────

    /// Feeds raw bytes directly into the read buffer.
    ///
    /// Used by the io_uring engine (`engine_uring.rs`) where the kernel
    /// copies data directly into a pre-registered buffer; the engine then
    /// calls this to hand the received slice to the connection state machine.
    #[cfg(target_os = "linux")]
    pub fn feed_bytes(&mut self, data: &[u8]) {
        // Compact buffer if cursor has advanced
        if self.read_pos > 0 {
            if self.read_pos < self.read_len {
                self.read_buf.copy_within(self.read_pos..self.read_len, 0);
                self.read_len -= self.read_pos;
            } else {
                self.read_len = 0;
            }
            self.read_pos = 0;
        }

        // Grow buffer if needed
        let needed = self.read_len + data.len();
        if needed > self.read_buf.len() {
            self.read_buf.resize(needed.next_power_of_two(), 0);
        }

        self.read_buf[self.read_len..self.read_len + data.len()].copy_from_slice(data);
        self.read_len += data.len();
    }

    /// Processes all complete RESP frames currently buffered.
    ///
    /// Identical semantics to [`process_incoming`] but takes no `stream`
    /// parameter — designed for the io_uring path where I/O and processing
    /// are decoupled through the completion queue.
    ///
    /// Returns `Ok(true)` = keep connection open, `Ok(false)` = QUIT received.
    #[cfg(target_os = "linux")]
    pub fn process_pending(
        &mut self,
        table: &mut SwissTable,
        pool: &mut SlabPool,
    ) -> Result<bool, NetError> {
        self.process_incoming(table, pool)
    }

    /// Drains the write buffer and returns its contents as a `Vec<u8>`.
    ///
    /// Called by the io_uring engine after `process_pending()` to collect
    /// response bytes and submit a `Send` SQE to the ring.
    #[cfg(target_os = "linux")]
    pub fn take_write_buf(&mut self) -> Vec<u8> {
        let buf = self.write_buf[self.write_pos..].to_vec();
        self.write_buf.clear();
        self.write_pos = 0;
        buf
    }
}

impl Default for Connection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_ping_and_get_set_flow() {
        let mut conn = Connection::new();
        let mut table = SwissTable::with_capacity(128);
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

        // 1. PING
        Connection::execute_command(Command::Ping { message: None }, &mut conn.write_buf, &mut table, &mut pool).unwrap();
        assert_eq!(conn.write_buf, b"+PONG\r\n");
        conn.write_buf.clear();

        // 2. SET key1 "hello_world"
        Connection::execute_command(
            Command::Set {
                key: b"key1",
                value: b"hello_world",
                ttl_ms: None,
            },
            &mut conn.write_buf,
            &mut table,
            &mut pool,
        ).unwrap();
        assert_eq!(conn.write_buf, b"+OK\r\n");
        conn.write_buf.clear();

        // 3. GET key1
        Connection::execute_command(Command::Get { key: b"key1" }, &mut conn.write_buf, &mut table, &mut pool).unwrap();
        assert_eq!(conn.write_buf, b"$11\r\nhello_world\r\n");
        conn.write_buf.clear();

        // 4. EXISTS key1
        let mut keys = smallvec::SmallVec::new();
        keys.push(&b"key1"[..]);
        Connection::execute_command(Command::Exists { keys }, &mut conn.write_buf, &mut table, &mut pool).unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 5. DEL key1
        let mut del_keys = smallvec::SmallVec::new();
        del_keys.push(&b"key1"[..]);
        Connection::execute_command(Command::Del { keys: del_keys }, &mut conn.write_buf, &mut table, &mut pool).unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 6. GET key1 (now deleted -> null)
        Connection::execute_command(Command::Get { key: b"key1" }, &mut conn.write_buf, &mut table, &mut pool).unwrap();
        assert_eq!(conn.write_buf, b"$-1\r\n");
    }

    #[test]
    fn execute_set_with_ttl_and_expiry_flow() {
        let mut conn = Connection::new();
        let mut table = SwissTable::with_capacity(128);
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

        // 1. SET key_ttl "temp" EX 10 (at epoch 1000s -> expires at 1010s)
        Connection::execute_command_with_time(
            Command::Set {
                key: b"key_ttl",
                value: b"temp",
                ttl_ms: Some(10_000), // 10 seconds
            },
            &mut conn.write_buf,
            &mut table,
            &mut pool,
            1000,
        ).unwrap();
        assert_eq!(conn.write_buf, b"+OK\r\n");
        conn.write_buf.clear();

        // 2. GET at epoch 1005s -> active
        Connection::execute_command_with_time(
            Command::Get { key: b"key_ttl" },
            &mut conn.write_buf,
            &mut table,
            &mut pool,
            1005,
        ).unwrap();
        assert_eq!(conn.write_buf, b"$4\r\ntemp\r\n");
        conn.write_buf.clear();

        // 3. EXISTS at epoch 1005s -> 1
        let mut keys = smallvec::SmallVec::new();
        keys.push(&b"key_ttl"[..]);
        Connection::execute_command_with_time(
            Command::Exists { keys },
            &mut conn.write_buf,
            &mut table,
            &mut pool,
            1005,
        ).unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 4. GET at epoch 1011s -> expired -> $-1\r\n
        Connection::execute_command_with_time(
            Command::Get { key: b"key_ttl" },
            &mut conn.write_buf,
            &mut table,
            &mut pool,
            1011,
        ).unwrap();
        assert_eq!(conn.write_buf, b"$-1\r\n");
        conn.write_buf.clear();

        // 5. EXISTS at epoch 1011s -> 0
        let mut keys2 = smallvec::SmallVec::new();
        keys2.push(&b"key_ttl"[..]);
        Connection::execute_command_with_time(
            Command::Exists { keys: keys2 },
            &mut conn.write_buf,
            &mut table,
            &mut pool,
            1011,
        ).unwrap();
        assert_eq!(conn.write_buf, b":0\r\n");
    }
}
