//! `kachedb-net` — Connection buffer management, RESP protocol decoding, and command execution.

pub mod admin;
pub mod expiry;
pub mod hashes;
pub mod strings;
pub mod vectors;

#[cfg(test)]
mod tests;

use std::io::{Read, Write};
use std::sync::{Arc, LazyLock, RwLock};

use kachedb_core::{HashedTimingWheel, SlabPool};
use kachedb_hash::ShardedSwissTable;
use kachedb_proto_resp::{Command, encode_error, encode_simple_string, parse_command};
use kachedb_vector::VectorIndexRegistry;

use crate::error::NetError;

pub static DEFAULT_VECTORS: LazyLock<VectorIndexRegistry> = LazyLock::new(VectorIndexRegistry::new);

/// Default buffer capacity for incoming connection stream (64 KB).
const READ_BUF_SIZE: usize = 64 * 1024;
/// Initial write buffer capacity.
const WRITE_BUF_SIZE: usize = 64 * 1024;

/// Client-specific connection metadata (e.g. protocol version, connection name, auth status).
#[derive(Debug, Clone)]
pub struct ClientState {
    pub name: Option<Vec<u8>>,
    pub client_id: u64,
    pub proto_version: u8,
    pub authenticated: bool,
    pub requirepass: Option<Arc<Vec<u8>>>,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            name: None,
            client_id: 1,
            proto_version: 2,
            authenticated: true,
            requirepass: None,
        }
    }
}

impl ClientState {
    pub fn with_password(password: Option<Arc<Vec<u8>>>) -> Self {
        let authenticated = password.is_none();
        Self {
            name: None,
            client_id: 1,
            proto_version: 2,
            authenticated,
            requirepass: password,
        }
    }
}

static REQUIREPASS: RwLock<Option<Vec<u8>>> = RwLock::new(None);

/// Sets the server-wide authentication password.
pub fn set_requirepass(password: Option<String>) {
    let mut lock = REQUIREPASS.write().unwrap();
    *lock = password.map(|s| s.into_bytes());
}

/// Returns the server-wide authentication password if configured.
pub fn get_requirepass() -> Option<Vec<u8>> {
    REQUIREPASS.read().unwrap().clone()
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

        let server_password = get_requirepass().map(Arc::new);

        Self {
            read_buf: vec![0u8; READ_BUF_SIZE],
            read_pos: 0,
            read_len: 0,
            write_buf: Vec::with_capacity(WRITE_BUF_SIZE),
            write_pos: 0,
            current_sec: now_sec,
            client_state: ClientState::with_password(server_password),
        }
    }

    /// Sets the coarse-grained cached epoch timestamp.
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
        timing_wheel: Option<&mut HashedTimingWheel>,
        mut client_state: Option<&mut ClientState>,
    ) -> Result<bool, NetError> {
        let pass_configured = client_state.as_ref().and_then(|cs| cs.requirepass.clone());
        let is_authed = match (&pass_configured, client_state.as_ref()) {
            (Some(_), Some(cs)) => cs.authenticated,
            (Some(_), None) => false,
            (None, _) => true,
        };

        if !is_authed {
            match cmd {
                Command::Auth { password, .. } => {
                    if let Some(ref expected) = pass_configured {
                        if password == expected.as_slice() {
                            if let Some(ref mut cs) = client_state {
                                cs.authenticated = true;
                            }
                            encode_simple_string(write_buf, "OK");
                        } else {
                            encode_error(write_buf, "ERR invalid password");
                        }
                    } else {
                        encode_error(write_buf, "ERR Client sent AUTH, but no password is set");
                    }
                    return Ok(true);
                }
                Command::Quit => {
                    encode_simple_string(write_buf, "OK");
                    return Ok(false);
                }
                _ => {
                    encode_error(write_buf, "NOAUTH Authentication required.");
                    return Ok(true);
                }
            }
        }

        match cmd {
            // Administrative, server, and session commands
            Command::Ping { .. }
            | Command::Hello { .. }
            | Command::Client { .. }
            | Command::Info { .. }
            | Command::CommandDoc
            | Command::DBSize
            | Command::Type { .. }
            | Command::FlushDb
            | Command::FlushAll
            | Command::BgRewriteAof
            | Command::Quit
            | Command::Unknown { .. } => {
                admin::handle_admin(cmd, write_buf, table, pool, vectors, now_sec, client_state)
            }

            // String commands
            Command::Get { .. }
            | Command::Set { .. }
            | Command::MGet { .. }
            | Command::MSet { .. }
            | Command::Del { .. }
            | Command::Exists { .. }
            | Command::Incr { .. }
            | Command::Decr { .. }
            | Command::IncrBy { .. }
            | Command::DecrBy { .. }
            | Command::Append { .. }
            | Command::Strlen { .. } => {
                strings::handle_strings(cmd, write_buf, table, pool, now_sec, timing_wheel)
            }

            // Expiry and TTL commands
            Command::Expire { .. }
            | Command::PExpire { .. }
            | Command::ExpireAt { .. }
            | Command::PExpireAt { .. }
            | Command::Ttl { .. }
            | Command::PTtl { .. }
            | Command::Persist { .. } => {
                expiry::handle_expiry(cmd, write_buf, table, pool, now_sec, timing_wheel)
            }

            // Redis Hash primitives
            Command::HSet { .. }
            | Command::HGet { .. }
            | Command::HDel { .. }
            | Command::HExists { .. }
            | Command::HLen { .. }
            | Command::HGetAll { .. } => {
                hashes::handle_hashes(cmd, write_buf, table, pool, now_sec, timing_wheel)
            }

            // Vector index and similarity search commands
            Command::VAdd { .. }
            | Command::VSearch { .. }
            | Command::VAddBatch { .. }
            | Command::VSearchBatch { .. }
            | Command::VDel { .. }
            | Command::VStats { .. }
            | Command::VIndexCreate { .. }
            | Command::VIndexDrop { .. }
            | Command::VIndexInfo { .. } => {
                vectors::handle_vectors(cmd, write_buf, vectors, now_sec)
            }

            // Re-authentication on already authed connection
            Command::Auth { password, .. } => {
                if let Some(ref expected) = pass_configured {
                    if password == expected.as_slice() {
                        if let Some(ref mut cs) = client_state {
                            cs.authenticated = true;
                        }
                        encode_simple_string(write_buf, "OK");
                    } else {
                        encode_error(write_buf, "ERR invalid password");
                    }
                } else {
                    encode_error(write_buf, "ERR Client sent AUTH, but no password is set");
                }
                Ok(true)
            }
        }
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
