//! `kachedb-net` — Connection buffer management, RESP protocol decoding, and command execution.

use std::io::{Read, Write};

use std::sync::LazyLock;

use kachedb_core::{SlabClassType, SlabPool, resolve_slot_ptr};
use kachedb_hash::{ShardedSwissTable, hash_key};
use kachedb_proto_resp::{
    Command, encode_array_header, encode_bulk_string, encode_error, encode_integer, encode_null,
    encode_simple_string, parse_command,
};
use kachedb_vector::VectorIndexRegistry;

use crate::error::NetError;

static DEFAULT_VECTORS: LazyLock<VectorIndexRegistry> = LazyLock::new(VectorIndexRegistry::new);

/// Default buffer capacity for incoming connection stream (64 KB).
const READ_BUF_SIZE: usize = 64 * 1024;
/// Initial write buffer capacity.
const WRITE_BUF_SIZE: usize = 64 * 1024;

/// Connection state machine managing the read buffer, parsing incoming commands,
/// executing them directly against the per-core `SlabPool` and `ShardedSwissTable`,
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

    /// Reads incoming bytes from `stream` into the internal ring buffer.
    /// Returns number of bytes read, or 0 on EOF.
    pub fn read_from<R: Read>(&mut self, stream: &mut R) -> Result<usize, NetError> {
        // Compact buffer only when empty or past halfway mark to avoid redundant memory copies
        if self.read_pos > 0 {
            if self.read_pos == self.read_len {
                self.read_pos = 0;
                self.read_len = 0;
            } else if self.read_pos >= self.read_buf.len() / 2 {
                self.read_buf.copy_within(self.read_pos..self.read_len, 0);
                self.read_len -= self.read_pos;
                self.read_pos = 0;
            }
        }

        // Grow buffer if full
        if self.read_len == self.read_buf.len() {
            self.read_buf.resize(self.read_buf.len() * 2, 0);
        }

        let n = match stream.read(&mut self.read_buf[self.read_len..]) {
            Ok(0) => return Ok(0), // EOF
            Ok(n) => n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(0),
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => return Ok(0),
            Err(e) => return Err(NetError::Io(e)),
        };

        self.read_len += n;
        Ok(n)
    }

    /// Parses and processes all complete frames in the read buffer with zero heap allocations.
    ///
    /// Executes decoded commands directly against `table` and `pool`.
    /// Returns `Ok(true)` if connection should stay open, or `Ok(false)` on `QUIT`.
    pub fn process_incoming(
        &mut self,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
    ) -> Result<bool, NetError> {
        self.process_incoming_with_vectors(table, pool, &DEFAULT_VECTORS)
    }

    /// Parses and processes all complete frames with explicit vector index registry.
    pub fn process_incoming_with_vectors(
        &mut self,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        vectors: &VectorIndexRegistry,
    ) -> Result<bool, NetError> {
        loop {
            let slice = &self.read_buf[self.read_pos..self.read_len];
            if slice.is_empty() {
                break;
            }

            match parse_command(slice)? {
                Some((cmd, consumed)) => {
                    self.read_pos += consumed;
                    let keep_alive = Self::execute_command_with_vectors(
                        cmd,
                        &mut self.write_buf,
                        table,
                        pool,
                        self.current_sec,
                        vectors,
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
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
    ) -> Result<bool, NetError> {
        Self::execute_command_with_vectors(cmd, write_buf, table, pool, 0, &DEFAULT_VECTORS)
    }

    /// Executes a command with explicit `now_sec` for deterministic TTL evaluation.
    pub fn execute_command_with_time(
        cmd: Command<'_>,
        write_buf: &mut Vec<u8>,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        now_sec: u32,
    ) -> Result<bool, NetError> {
        Self::execute_command_with_vectors(cmd, write_buf, table, pool, now_sec, &DEFAULT_VECTORS)
    }

    /// Executes a command with explicit `now_sec` and `VectorIndexRegistry`.
    pub fn execute_command_with_vectors(
        cmd: Command<'_>,
        write_buf: &mut Vec<u8>,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        now_sec: u32,
        vectors: &VectorIndexRegistry,
    ) -> Result<bool, NetError> {
        match cmd {
            Command::Ping { message } => match message {
                Some(msg) => encode_bulk_string(write_buf, msg),
                None => encode_simple_string(write_buf, "PONG"),
            },
            Command::Get { key } => {
                let h = hash_key(key);
                if let Some(entry) = table.lookup_checked(h, now_sec) {
                    if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
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
                        if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
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
            Command::VAdd {
                index,
                id,
                dim,
                vector_bytes,
                payload,
                ttl_sec,
            } => {
                if vector_bytes.len() != dim * 4 {
                    encode_error(
                        write_buf,
                        &format!(
                            "ERR vector byte length {} does not match dimension {} (expected {} bytes)",
                            vector_bytes.len(),
                            dim,
                            dim * 4
                        ),
                    );
                    return Ok(true);
                }

                let mut floats = Vec::with_capacity(dim);
                for chunk in vector_bytes.chunks_exact(4) {
                    floats.push(f32::from_ne_bytes(chunk.try_into().unwrap()));
                }

                let vec_idx = vectors.get_or_create(index);
                match vec_idx.insert(id, &floats, payload, ttl_sec, now_sec) {
                    Ok(()) => {
                        encode_integer(write_buf, 1);
                    }
                    Err(e) => {
                        encode_error(write_buf, &format!("ERR {e}"));
                    }
                }
            }
            Command::VSearch {
                index,
                query_bytes,
                top_k,
                threshold,
            } => {
                if query_bytes.len() % 4 != 0 {
                    encode_error(
                        write_buf,
                        "ERR query vector byte length must be a multiple of 4",
                    );
                    return Ok(true);
                }

                let dim = query_bytes.len() / 4;
                let mut query_floats = Vec::with_capacity(dim);
                for chunk in query_bytes.chunks_exact(4) {
                    query_floats.push(f32::from_ne_bytes(chunk.try_into().unwrap()));
                }

                if let Some(vec_idx) = vectors.get(index) {
                    match vec_idx.search(&query_floats, top_k, threshold, now_sec) {
                        Ok(results) => {
                            encode_array_header(write_buf, results.len());
                            for r in results {
                                encode_array_header(write_buf, 3);
                                encode_bulk_string(write_buf, &r.key);
                                let score_str = format!("{:.6}", r.similarity);
                                encode_bulk_string(write_buf, score_str.as_bytes());
                                if let Some(ref p) = r.payload {
                                    encode_bulk_string(write_buf, p);
                                } else {
                                    encode_null(write_buf);
                                }
                            }
                        }
                        Err(e) => {
                            encode_error(write_buf, &format!("ERR {e}"));
                        }
                    }
                } else {
                    encode_array_header(write_buf, 0);
                }
            }
            Command::VDel { index, id } => {
                if let Some(vec_idx) = vectors.get(index) {
                    let deleted = vec_idx.delete(id);
                    encode_integer(write_buf, if deleted { 1 } else { 0 });
                } else {
                    encode_integer(write_buf, 0);
                }
            }
            Command::VStats { index } => {
                if let Some(vec_idx) = vectors.get(index) {
                    let stats = vec_idx.stats(now_sec);
                    encode_array_header(write_buf, 8);
                    encode_bulk_string(write_buf, b"dimension");
                    encode_integer(write_buf, stats.dimension as i64);
                    encode_bulk_string(write_buf, b"total_vectors");
                    encode_integer(write_buf, stats.total_vectors as i64);
                    encode_bulk_string(write_buf, b"active_vectors");
                    encode_integer(write_buf, stats.active_vectors as i64);
                    encode_bulk_string(write_buf, b"memory_bytes");
                    encode_integer(write_buf, stats.memory_bytes as i64);
                } else {
                    encode_null(write_buf);
                }
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
                encode_error(write_buf, &format!("ERR unknown command '{name_str}'"));
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
        // Compact buffer only when empty or past halfway mark
        if self.read_pos > 0 {
            if self.read_pos == self.read_len {
                self.read_pos = 0;
                self.read_len = 0;
            } else if self.read_pos >= self.read_buf.len() / 2 {
                self.read_buf.copy_within(self.read_pos..self.read_len, 0);
                self.read_len -= self.read_pos;
                self.read_pos = 0;
            }
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
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
    ) -> Result<bool, NetError> {
        self.process_incoming(table, pool)
    }

    /// Drains the write buffer into `dest` without allocating a new Vec.
    #[cfg(target_os = "linux")]
    pub fn drain_write_buf_into(&mut self, dest: &mut Vec<u8>) {
        dest.clear();
        dest.extend_from_slice(&self.write_buf[self.write_pos..]);
        self.write_buf.clear();
        self.write_pos = 0;
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
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

        // 1. PING -> PONG
        Connection::execute_command(
            Command::Ping { message: None },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
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
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"+OK\r\n");
        conn.write_buf.clear();

        // 3. GET key1
        Connection::execute_command(
            Command::Get { key: b"key1" },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"$11\r\nhello_world\r\n");
        conn.write_buf.clear();

        // 4. EXISTS key1
        let mut keys = smallvec::SmallVec::new();
        keys.push(&b"key1"[..]);
        Connection::execute_command(
            Command::Exists { keys },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 5. DEL key1
        let mut del_keys = smallvec::SmallVec::new();
        del_keys.push(&b"key1"[..]);
        Connection::execute_command(
            Command::Del { keys: del_keys },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 6. GET key1 (now deleted -> null)
        Connection::execute_command(
            Command::Get { key: b"key1" },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"$-1\r\n");
    }

    #[test]
    fn execute_set_with_ttl_and_expiry_flow() {
        let mut conn = Connection::new();
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

        // 1. SET key_ttl "temp" EX 10 (at epoch 1000s -> expires at 1010s)
        Connection::execute_command_with_time(
            Command::Set {
                key: b"key_ttl",
                value: b"temp",
                ttl_ms: Some(10_000), // 10 seconds
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            1000,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"+OK\r\n");
        conn.write_buf.clear();

        // 2. GET at epoch 1005s -> active
        Connection::execute_command_with_time(
            Command::Get { key: b"key_ttl" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            1005,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"$4\r\ntemp\r\n");
        conn.write_buf.clear();

        // 3. EXISTS at epoch 1005s -> 1
        let mut keys = smallvec::SmallVec::new();
        keys.push(&b"key_ttl"[..]);
        Connection::execute_command_with_time(
            Command::Exists { keys },
            &mut conn.write_buf,
            &table,
            &mut pool,
            1005,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 4. GET at epoch 1011s -> expired -> $-1\r\n
        Connection::execute_command_with_time(
            Command::Get { key: b"key_ttl" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            1011,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"$-1\r\n");
        conn.write_buf.clear();

        // 5. EXISTS at epoch 1011s -> 0
        let mut keys2 = smallvec::SmallVec::new();
        keys2.push(&b"key_ttl"[..]);
        Connection::execute_command_with_time(
            Command::Exists { keys: keys2 },
            &mut conn.write_buf,
            &table,
            &mut pool,
            1011,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":0\r\n");
    }

    #[test]
    fn execute_vector_commands_flow() {
        let mut conn = Connection::new();
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
        let vectors = VectorIndexRegistry::new();

        // 1. VADD faq doc1 3 <floats> PAYLOAD "answer1"
        let v1 = [1.0f32, 0.0, 0.0];
        let mut v1_bytes = Vec::new();
        for f in &v1 {
            v1_bytes.extend_from_slice(&f.to_ne_bytes());
        }

        Connection::execute_command_with_vectors(
            Command::VAdd {
                index: b"faq",
                id: b"doc1",
                dim: 3,
                vector_bytes: &v1_bytes,
                payload: Some(b"answer1"),
                ttl_sec: None,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            0,
            &vectors,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 2. VSEARCH faq <v1_bytes> TOPK 1 THRESHOLD 0.8
        Connection::execute_command_with_vectors(
            Command::VSearch {
                index: b"faq",
                query_bytes: &v1_bytes,
                top_k: 1,
                threshold: 0.8,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            0,
            &vectors,
        )
        .unwrap();

        // Expected array with 1 item: [doc1, "1.000000", "answer1"]
        let resp_str = String::from_utf8_lossy(&conn.write_buf);
        assert!(resp_str.contains("*1\r\n*3\r\n$4\r\ndoc1\r\n"));
        assert!(resp_str.contains("answer1"));
        conn.write_buf.clear();

        // 3. VSTATS faq
        Connection::execute_command_with_vectors(
            Command::VStats { index: b"faq" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            0,
            &vectors,
        )
        .unwrap();
        let stats_resp = String::from_utf8_lossy(&conn.write_buf);
        assert!(stats_resp.contains("total_vectors"));
        conn.write_buf.clear();

        // 4. VDEL faq doc1
        Connection::execute_command_with_vectors(
            Command::VDel {
                index: b"faq",
                id: b"doc1",
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            0,
            &vectors,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
    }
}
