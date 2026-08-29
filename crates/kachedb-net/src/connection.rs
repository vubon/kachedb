//! `kachedb-net` — Connection buffer management, RESP protocol decoding, and command execution.

use std::io::{Read, Write};

use std::sync::LazyLock;

use kachedb_core::{HashedTimingWheel, SlabClassType, SlabPool, resolve_slot_ptr};
use kachedb_hash::{ShardedSwissTable, hash_key};
use kachedb_proto_resp::{
    ClientSubcommand, Command, encode_array_header, encode_bulk_string, encode_error,
    encode_integer, encode_null, encode_simple_string, parse_command,
};
use kachedb_vector::VectorIndexRegistry;

use crate::error::NetError;

pub(crate) static DEFAULT_VECTORS: LazyLock<VectorIndexRegistry> =
    LazyLock::new(VectorIndexRegistry::new);

/// Default buffer capacity for incoming connection stream (64 KB).
const READ_BUF_SIZE: usize = 64 * 1024;
/// Initial write buffer capacity.
const WRITE_BUF_SIZE: usize = 64 * 1024;

/// Client-specific connection metadata (e.g. protocol version and connection name).
#[derive(Debug, Clone)]
pub struct ClientState {
    pub name: Option<Vec<u8>>,
    pub client_id: u64,
    pub proto_version: u8,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            name: None,
            client_id: 1,
            proto_version: 2,
        }
    }
}

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
    pub write_buf: Vec<u8>,
    /// Write cursor offset in `write_buf`.
    pub write_pos: usize,
    /// Coarse-grained cached epoch timestamp in seconds.
    pub current_sec: u32,
    /// Client-specific connection state.
    pub client_state: ClientState,
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
            client_state: ClientState::default(),
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
        if self.read_pos > 0 {
            if self.read_pos == self.read_len {
                self.read_pos = 0;
                self.read_len = 0;
            } else {
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
            Ok(0) => return Err(NetError::ConnectionClosed), // True EOF from client
            Ok(n) => n,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => 0,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => 0,
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

    /// Parses and processes all complete frames with explicit vector index registry and timing wheel.
    #[inline(always)]
    pub fn process_incoming_with_wheel(
        &mut self,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        vectors: &VectorIndexRegistry,
        mut timing_wheel: Option<&mut HashedTimingWheel>,
    ) -> Result<bool, NetError> {
        loop {
            let slice = &self.read_buf[self.read_pos..self.read_len];
            if slice.is_empty() {
                break;
            }

            match parse_command(slice)? {
                Some((cmd, consumed)) => {
                    self.read_pos += consumed;
                    let keep_alive = Self::execute_command_full(
                        cmd,
                        &mut self.write_buf,
                        table,
                        pool,
                        self.current_sec,
                        vectors,
                        timing_wheel.as_deref_mut(),
                        Some(&mut self.client_state),
                    )?;
                    if !keep_alive {
                        return Ok(false);
                    }
                }
                None => break, // incomplete frame, wait for more data
            }
        }

        if self.read_pos == self.read_len {
            self.read_pos = 0;
            self.read_len = 0;
        }

        Ok(true)
    }

    /// Parses and processes all complete frames with explicit vector index registry.
    pub fn process_incoming_with_vectors(
        &mut self,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        vectors: &VectorIndexRegistry,
    ) -> Result<bool, NetError> {
        self.process_incoming_with_wheel(table, pool, vectors, None)
    }

    /// Executes a single strongly-typed command against local memory structures (unconstrained time).
    #[inline]
    pub fn execute_command(
        cmd: Command<'_>,
        write_buf: &mut Vec<u8>,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
    ) -> Result<bool, NetError> {
        Self::execute_command_full(cmd, write_buf, table, pool, 0, &DEFAULT_VECTORS, None, None)
    }

    /// Executes a command with explicit `now_sec` for deterministic TTL evaluation.
    pub fn execute_command_with_time(
        cmd: Command<'_>,
        write_buf: &mut Vec<u8>,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        now_sec: u32,
    ) -> Result<bool, NetError> {
        Self::execute_command_full(
            cmd,
            write_buf,
            table,
            pool,
            now_sec,
            &DEFAULT_VECTORS,
            None,
            None,
        )
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
        Self::execute_command_full(cmd, write_buf, table, pool, now_sec, vectors, None, None)
    }

    /// Executes a command with full engine context, including active TimingWheel scheduling and client state.
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub fn execute_command_full(
        cmd: Command<'_>,
        write_buf: &mut Vec<u8>,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        now_sec: u32,
        vectors: &VectorIndexRegistry,
        mut timing_wheel: Option<&mut HashedTimingWheel>,
        mut client_state: Option<&mut ClientState>,
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

                        // Schedule in TimingWheel if key has TTL
                        if expire_at_secs > 0 {
                            if let Some(ref mut wheel) = timing_wheel {
                                wheel.schedule(h, block_id, expire_at_secs);
                            }
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

                #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
                let floats: Vec<f32> = vector_bytes
                    .chunks_exact(4)
                    .map(|chunk| f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();

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

                #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
                let query_floats: Vec<f32> = query_bytes
                    .chunks_exact(4)
                    .map(|chunk| f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();

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
            Command::Expire { key, seconds } => {
                let h = hash_key(key);
                if seconds <= 0 {
                    if let Some(entry) = table.remove(h) {
                        let _ = pool.deallocate(entry.slab_block_id);
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
                        encode_integer(write_buf, 1);
                    } else {
                        encode_integer(write_buf, 0);
                    }
                } else {
                    let expire_at_secs = timestamp.max(0) as u32;
                    let ok = table.update_ttl(h, expire_at_secs, now_sec);
                    if ok {
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
                        encode_integer(write_buf, 1);
                    } else {
                        encode_integer(write_buf, 0);
                    }
                } else {
                    let expire_at_secs = ts_sec.max(0) as u32;
                    let ok = table.update_ttl(h, expire_at_secs, now_sec);
                    if ok {
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

                            let old_block_id =
                                table.insert_with_ttl(h, block_id, val_len as u32, 0);
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
            Command::Incr { key } => {
                Self::execute_incr_by(key, 1, write_buf, table, pool, now_sec)?;
            }
            Command::Decr { key } => {
                Self::execute_incr_by(key, -1, write_buf, table, pool, now_sec)?;
            }
            Command::IncrBy { key, delta } => {
                Self::execute_incr_by(key, delta, write_buf, table, pool, now_sec)?;
            }
            Command::DecrBy { key, delta } => {
                Self::execute_incr_by(key, -delta, write_buf, table, pool, now_sec)?;
            }
            Command::Append { key, value } => {
                let h = hash_key(key);
                let (combined_val, expire_at_secs) = if let Some(entry) =
                    table.lookup_checked(h, now_sec)
                {
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
            Command::Hello {
                protover,
                auth: _,
                setname,
            } => {
                let ver = protover.unwrap_or(2);
                if ver != 2 && ver != 3 {
                    encode_error(write_buf, "NOPROTO unsupported protocol version");
                    return Ok(true);
                }

                if let Some(ref mut state) = client_state {
                    state.proto_version = ver as u8;
                    if let Some(name) = setname {
                        state.name = Some(name.to_vec());
                    }
                }

                let client_id = client_state
                    .as_ref()
                    .map(|s| s.client_id as i64)
                    .unwrap_or(1);

                encode_array_header(write_buf, 14);
                encode_bulk_string(write_buf, b"server");
                encode_bulk_string(write_buf, b"kachedb");
                encode_bulk_string(write_buf, b"version");
                encode_bulk_string(write_buf, b"0.1.0");
                encode_bulk_string(write_buf, b"proto");
                encode_integer(write_buf, ver);
                encode_bulk_string(write_buf, b"id");
                encode_integer(write_buf, client_id);
                encode_bulk_string(write_buf, b"mode");
                encode_bulk_string(write_buf, b"standalone");
                encode_bulk_string(write_buf, b"role");
                encode_bulk_string(write_buf, b"master");
                encode_bulk_string(write_buf, b"modules");
                encode_array_header(write_buf, 0);
            }
            Command::Client { subcommand } => match subcommand {
                ClientSubcommand::SetName(name) => {
                    if let Some(ref mut state) = client_state {
                        state.name = Some(name.to_vec());
                    }
                    encode_simple_string(write_buf, "OK");
                }
                ClientSubcommand::GetName => {
                    if let Some(name) = client_state.as_ref().and_then(|s| s.name.as_ref()) {
                        encode_bulk_string(write_buf, name);
                    } else {
                        encode_null(write_buf);
                    }
                }
                ClientSubcommand::Id => {
                    let id = client_state
                        .as_ref()
                        .map(|s| s.client_id as i64)
                        .unwrap_or(1);
                    encode_integer(write_buf, id);
                }
                ClientSubcommand::List => {
                    let name_str = client_state
                        .as_ref()
                        .and_then(|s| s.name.as_ref())
                        .and_then(|n| std::str::from_utf8(n).ok())
                        .unwrap_or("");
                    let list_info = format!(
                        "id=1 addr=127.0.0.1:0 fd=0 name={name_str} age=1 idle=0 flags=N db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 argv-mem=0 obl=0 oll=0 omem=0 tot-mem=0 events=r cmd=client\n"
                    );
                    encode_bulk_string(write_buf, list_info.as_bytes());
                }
                ClientSubcommand::Unrecognized(sub) => {
                    let sub_str = std::str::from_utf8(sub).unwrap_or("unknown");
                    encode_error(
                        write_buf,
                        &format!("ERR unknown subcommand '{sub_str}' for 'CLIENT'"),
                    );
                }
            },
            Command::Info { section: _ } => {
                let info_text = format!(
                    "# Server\r\n\
                     kachedb_version:0.1.0\r\n\
                     os:{os}\r\n\
                     arch_bits:64\r\n\
                     process_id:{pid}\r\n\
                     tcp_port:6379\r\n\
                     uptime_in_seconds:{uptime}\r\n\
                     \r\n\
                     # Memory\r\n\
                     used_memory:{used_mem}\r\n\
                     used_memory_human:{used_mem_human:.2}M\r\n\
                     used_memory_peak:{used_mem}\r\n\
                     megaslabs_allocated:{megaslabs}\r\n\
                     slab_slots_active:{active_slots}\r\n\
                     fragmentation_ratio:1.00\r\n\
                     \r\n\
                     # Stats\r\n\
                     total_connections_received:1\r\n\
                     total_commands_processed:1\r\n\
                     instantaneous_ops_per_sec:0\r\n\
                     keyspace_hits:1\r\n\
                     keyspace_misses:0\r\n\
                     \r\n\
                     # Keyspace\r\n\
                     db0:keys={total_keys},expires=0,avg_ttl=0\r\n\
                     \r\n\
                     # VectorEngine\r\n\
                     active_indices:0\r\n\
                     total_vectors:0\r\n\
                     vector_memory_bytes:0\r\n\
                     simd_kernel:auto\r\n",
                    os = std::env::consts::OS,
                    pid = std::process::id(),
                    uptime = now_sec,
                    used_mem = pool.total_allocated_bytes(),
                    used_mem_human = pool.total_allocated_bytes() as f64 / (1024.0 * 1024.0),
                    megaslabs = pool.arena_count(),
                    active_slots = table.len(),
                    total_keys = table.len(),
                );
                encode_bulk_string(write_buf, info_text.as_bytes());
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

    /// Internal helper executing atomic arithmetic mutations (INCR, DECR, INCRBY, DECRBY).
    fn execute_incr_by(
        key: &[u8],
        delta: i64,
        write_buf: &mut Vec<u8>,
        table: &ShardedSwissTable,
        pool: &mut SlabPool,
        now_sec: u32,
    ) -> Result<bool, NetError> {
        let h = hash_key(key);
        let (new_val, expire_at_secs) = if let Some(entry) = table.lookup_checked(h, now_sec) {
            if let Some(ptr) = unsafe { resolve_slot_ptr(entry.slab_block_id) } {
                let val_slice =
                    unsafe { std::slice::from_raw_parts(ptr, entry.value_len as usize) };
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

                let old_block_id =
                    table.insert_with_ttl(h, block_id, val_len as u32, expire_at_secs);
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
    #[inline(always)]
    pub fn has_pending_writes(&self) -> bool {
        self.write_pos < self.write_buf.len()
    }

    /// Returns `true` if there are unparsed bytes in the read buffer.
    #[inline(always)]
    pub fn has_unprocessed_input(&self) -> bool {
        self.read_pos < self.read_len
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

    #[test]
    fn execute_expire_ttl_persist_flow() {
        let mut conn = Connection::new();
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();

        // 1. SET user "alice" (persistent)
        Connection::execute_command_with_time(
            Command::Set {
                key: b"user",
                value: b"alice",
                ttl_ms: None,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            100,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"+OK\r\n");
        conn.write_buf.clear();

        // 2. TTL user -> -1 (persistent)
        Connection::execute_command_with_time(
            Command::Ttl { key: b"user" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            100,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":-1\r\n");
        conn.write_buf.clear();

        // 3. EXPIRE user 50 -> :1
        Connection::execute_command_with_time(
            Command::Expire {
                key: b"user",
                seconds: 50,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            100,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 4. TTL user at epoch 120 -> 30s remaining
        Connection::execute_command_with_time(
            Command::Ttl { key: b"user" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            120,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":30\r\n");
        conn.write_buf.clear();

        // 5. PTTL user at epoch 120 -> 30000 ms remaining
        Connection::execute_command_with_time(
            Command::PTtl { key: b"user" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            120,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":30000\r\n");
        conn.write_buf.clear();

        // 6. PERSIST user -> :1
        Connection::execute_command_with_time(
            Command::Persist { key: b"user" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            120,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 7. TTL user -> -1 (persistent again)
        Connection::execute_command_with_time(
            Command::Ttl { key: b"user" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            120,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":-1\r\n");
        conn.write_buf.clear();

        // 8. EXPIRE with negative seconds -> immediate deletion (Redis 7 semantics)
        Connection::execute_command_with_time(
            Command::Expire {
                key: b"user",
                seconds: -5,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            120,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 9. TTL user -> -2 (missing)
        Connection::execute_command_with_time(
            Command::Ttl { key: b"user" },
            &mut conn.write_buf,
            &table,
            &mut pool,
            120,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":-2\r\n");
    }

    #[test]
    fn execute_extended_primitives_flow() {
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
        let mut conn = Connection::new();

        // 1. MSET k1 v1 k2 v2 k3 v3 -> +OK
        use smallvec::smallvec;
        Connection::execute_command(
            Command::MSet {
                pairs: smallvec![
                    (b"k1".as_slice(), b"v1".as_slice()),
                    (b"k2".as_slice(), b"v2".as_slice()),
                    (b"k3".as_slice(), b"v3".as_slice())
                ],
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"+OK\r\n");
        conn.write_buf.clear();

        // 2. STRLEN k1 -> :2
        Connection::execute_command(
            Command::Strlen { key: b"k1" },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":2\r\n");
        conn.write_buf.clear();

        // 3. APPEND k1 _append -> :9 ("v1_append")
        Connection::execute_command(
            Command::Append {
                key: b"k1",
                value: b"_append",
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":9\r\n");
        conn.write_buf.clear();

        // 4. GET k1 -> $9\r\nv1_append\r\n
        Connection::execute_command(
            Command::Get { key: b"k1" },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"$9\r\nv1_append\r\n");
        conn.write_buf.clear();

        // 5. INCR counter (new key) -> :1
        Connection::execute_command(
            Command::Incr { key: b"counter" },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":1\r\n");
        conn.write_buf.clear();

        // 6. INCRBY counter 10 -> :11
        Connection::execute_command(
            Command::IncrBy {
                key: b"counter",
                delta: 10,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":11\r\n");
        conn.write_buf.clear();

        // 7. DECR counter -> :10
        Connection::execute_command(
            Command::Decr { key: b"counter" },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":10\r\n");
        conn.write_buf.clear();

        // 8. DECRBY counter 5 -> :5
        Connection::execute_command(
            Command::DecrBy {
                key: b"counter",
                delta: 5,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(conn.write_buf, b":5\r\n");
        conn.write_buf.clear();

        // 9. INCR non-integer key (k1 is "v1_append") -> error
        Connection::execute_command(
            Command::Incr { key: b"k1" },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        assert_eq!(
            conn.write_buf,
            b"-ERR value is not an integer or out of range\r\n"
        );
        conn.write_buf.clear();
    }

    #[test]
    fn execute_hello_client_and_info_flow() {
        let table = ShardedSwissTable::new();
        let mut pool = SlabPool::new(0, 16 * 1024 * 1024).unwrap();
        let mut conn = Connection::new();

        // 1. HELLO 3 SETNAME my-python-app
        Connection::execute_command_full(
            Command::Hello {
                protover: Some(3),
                auth: None,
                setname: Some(b"my-python-app"),
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            100,
            &DEFAULT_VECTORS,
            None,
            Some(&mut conn.client_state),
        )
        .unwrap();
        assert!(conn.write_buf.starts_with(b"*14\r\n"));
        assert_eq!(conn.client_state.proto_version, 3);
        assert_eq!(
            conn.client_state.name.as_deref(),
            Some(b"my-python-app".as_slice())
        );
        conn.write_buf.clear();

        // 2. CLIENT GETNAME -> $13\r\nmy-python-app\r\n
        Connection::execute_command_full(
            Command::Client {
                subcommand: ClientSubcommand::GetName,
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            100,
            &DEFAULT_VECTORS,
            None,
            Some(&mut conn.client_state),
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"$13\r\nmy-python-app\r\n");
        conn.write_buf.clear();

        // 3. CLIENT SETNAME new-worker -> +OK\r\n
        Connection::execute_command_full(
            Command::Client {
                subcommand: ClientSubcommand::SetName(b"new-worker"),
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
            100,
            &DEFAULT_VECTORS,
            None,
            Some(&mut conn.client_state),
        )
        .unwrap();
        assert_eq!(conn.write_buf, b"+OK\r\n");
        assert_eq!(
            conn.client_state.name.as_deref(),
            Some(b"new-worker".as_slice())
        );
        conn.write_buf.clear();

        // 4. INFO server -> contains "# Server"
        Connection::execute_command(
            Command::Info {
                section: Some(b"server"),
            },
            &mut conn.write_buf,
            &table,
            &mut pool,
        )
        .unwrap();
        let resp_str = std::str::from_utf8(&conn.write_buf).unwrap();
        assert!(resp_str.contains("# Server"));
        assert!(resp_str.contains("kachedb_version:0.1.0"));
        assert!(resp_str.contains("# Memory"));
        conn.write_buf.clear();
    }
}
