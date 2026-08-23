//! `kachedb-net` — Linux io_uring + SQPOLL asynchronous network engine.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │  Worker Thread (pinned to core N)                       │
//! │                                                         │
//! │  Completion loop → collect pending_sq: Vec<Entry>       │
//! │  After CQ drained → push all pending_sq to ring.sq     │
//! │  Submit (SQPOLL: no syscall once kernel thread running) │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! The **deferred submission pattern** is used throughout: all SQEs built
//! during CQE processing are accumulated in a local `Vec<Entry>` and pushed
//! to the submission queue only after the completion loop finishes. This avoids
//! holding a mutable borrow on `ring.completion()` and `ring.submission()`
//! simultaneously (which Rust's borrow checker would reject).
//!
//! # Platform
//!
//! Compiled **only on Linux** (`#[cfg(target_os = "linux")]`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::os::unix::io::{AsRawFd, RawFd};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use io_uring::{IoUring, opcode, squeue, types};

use kachedb_core::{HashedTimingWheel, SlabPool, pin_current_thread_to_core};
use kachedb_hash::ShardedSwissTable;

use crate::connection::Connection;
use crate::error::NetError;

// ── Constants ─────────────────────────────────────────────────────────────────

const RING_ENTRIES: u32 = 256;
const DEFAULT_POOL_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB/core
const RECV_BUF_SIZE: usize = 64 * 1024;
const MAX_CONNECTIONS: usize = 4096;

// ── User Data Encoding ─────────────────────────────────────────────────────────
// bits[63:32] = connection token (u32)
// bits[31:0]  = operation tag   (u32)

const OP_ACCEPT: u32 = 0;
const OP_RECV: u32 = 1;
const OP_SEND: u32 = 2;
const OP_CLOSE: u32 = 3;

#[inline(always)]
fn ud(token: u32, op: u32) -> u64 {
    ((token as u64) << 32) | (op as u64)
}

#[inline(always)]
fn decode_ud(u: u64) -> (u32, u32) {
    ((u >> 32) as u32, (u & 0xFFFF_FFFF) as u32)
}

// ── Connection State ──────────────────────────────────────────────────────────

struct ConnState {
    fd: RawFd,
    conn: Connection,
    recv_buf: Vec<u8>,
    pending_send: Vec<u8>,
    closing: bool,
}

impl ConnState {
    fn new(fd: RawFd) -> Self {
        Self {
            fd,
            conn: Connection::new(),
            recv_buf: vec![0u8; RECV_BUF_SIZE],
            pending_send: Vec::with_capacity(4096),
            closing: false,
        }
    }
}

// ── SQE builder helpers (purely safe, no ring borrow) ─────────────────────────

fn make_accept(
    listen_fd: RawFd,
    addr: *mut libc::sockaddr,
    addrlen: *mut libc::socklen_t,
) -> squeue::Entry {
    opcode::Accept::new(types::Fd(listen_fd), addr as *mut _, addrlen)
        .build()
        .user_data(ud(0, OP_ACCEPT))
}

fn make_recv(token: u32, fd: RawFd, buf_ptr: *mut u8, buf_len: usize) -> squeue::Entry {
    opcode::Recv::new(types::Fd(fd), buf_ptr, buf_len as u32)
        .build()
        .user_data(ud(token, OP_RECV))
}

fn make_send(token: u32, fd: RawFd, buf_ptr: *const u8, buf_len: usize) -> squeue::Entry {
    opcode::Send::new(types::Fd(fd), buf_ptr, buf_len as u32)
        .build()
        .user_data(ud(token, OP_SEND))
}

fn make_close(token: u32, fd: RawFd) -> squeue::Entry {
    opcode::Close::new(types::Fd(fd))
        .build()
        .user_data(ud(token, OP_CLOSE))
}

fn create_reuseport_listener(addr: SocketAddr) -> Result<std::net::TcpListener, std::io::Error> {
    use std::os::unix::io::FromRawFd;
    unsafe {
        let domain = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        let fd = libc::socket(domain, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let optval: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &optval as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );

        let mut storage: libc::sockaddr_storage = std::mem::zeroed();
        let socklen: libc::socklen_t = match addr {
            SocketAddr::V4(v4) => {
                let sin = &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in);
                sin.sin_family = libc::AF_INET as libc::sa_family_t;
                sin.sin_port = v4.port().to_be();
                sin.sin_addr = libc::in_addr {
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                };
                std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t
            }
            SocketAddr::V6(v6) => {
                let sin6 = &mut *(&mut storage as *mut _ as *mut libc::sockaddr_in6);
                sin6.sin6_family = libc::AF_INET6 as libc::sa_family_t;
                sin6.sin6_port = v6.port().to_be();
                sin6.sin6_addr = libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                };
                std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t
            }
        };

        if libc::bind(fd, &storage as *const _ as *const libc::sockaddr, socklen) < 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(err);
        }

        if libc::listen(fd, 1024) < 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(err);
        }

        Ok(std::net::TcpListener::from_raw_fd(fd))
    }
}

// ── UringWorkerThread ─────────────────────────────────────────────────────────

/// Per-core io_uring worker with shared global sharded hash index.
pub struct UringWorkerThread {
    pub core_id: u16,
    pub pool: SlabPool,
    pub table: Arc<ShardedSwissTable>,
    pub timing_wheel: HashedTimingWheel,
    pub bind_addr: SocketAddr,
}

impl UringWorkerThread {
    /// Creates a new `UringWorkerThread` with default pool capacity (64 MB).
    pub fn new(core_id: u16, bind_addr: SocketAddr) -> Result<Self, NetError> {
        Self::with_shared_table(
            core_id,
            bind_addr,
            DEFAULT_POOL_CAPACITY,
            Arc::new(ShardedSwissTable::new()),
        )
    }

    /// Creates a new `UringWorkerThread` with explicit memory pool capacity in bytes and shared table.
    pub fn with_shared_table(
        core_id: u16,
        bind_addr: SocketAddr,
        pool_bytes: usize,
        table: Arc<ShardedSwissTable>,
    ) -> Result<Self, NetError> {
        let pool = SlabPool::new(core_id, pool_bytes)?;
        let start_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as u32;
        let timing_wheel = HashedTimingWheel::new(start_sec);

        Ok(Self {
            core_id,
            pool,
            table,
            timing_wheel,
            bind_addr,
        })
    }

    pub fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), NetError> {
        let _ = pin_current_thread_to_core(self.core_id as usize);

        // ── io_uring init ────────────────────────────────────────────────────
        let mut ring = match IoUring::builder().setup_sqpoll(2000).build(RING_ENTRIES) {
            Ok(r) => {
                log::info!(
                    "Worker [{}]: io_uring SQPOLL enabled on {}",
                    self.core_id,
                    self.bind_addr
                );
                r
            }
            Err(e) => {
                log::warn!(
                    "Worker [{}]: SQPOLL unavailable ({e}), using standard io_uring",
                    self.core_id
                );
                IoUring::new(RING_ENTRIES).map_err(NetError::Io)?
            }
        };

        // ── Bind + listen with SO_REUSEPORT for multi-worker support ─────────
        let listener = create_reuseport_listener(self.bind_addr)?;
        let listen_fd = listener.as_raw_fd();

        // ── Accept address storage ───────────────────────────────────────────
        let mut accept_addr: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let mut accept_addrlen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        // ── Connection table ─────────────────────────────────────────────────
        let mut connections: HashMap<u32, ConnState> = HashMap::with_capacity(64);
        let mut next_token: u32 = 1;

        // Queue initial Accept SQE
        {
            let sqe = make_accept(
                listen_fd,
                &mut accept_addr as *mut _ as *mut libc::sockaddr,
                &mut accept_addrlen,
            );
            unsafe { ring.submission().push(&sqe).expect("SQ full on init") };
            ring.submit().map_err(NetError::Io)?;
        }

        // Deferred SQE accumulator — populated during CQ processing, pushed after
        let mut pending_sq: Vec<squeue::Entry> = Vec::with_capacity(32);
        let mut second_ticker: u64 = 0;

        // ── Main completion loop ─────────────────────────────────────────────
        loop {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            match ring.submit_and_wait(1) {
                Ok(_) => {}
                Err(ref e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                Err(e) => return Err(NetError::Io(e)),
            }

            // — CQ processing: build pending_sq but do NOT touch ring.submission() yet —
            {
                let mut cq = ring.completion();
                cq.sync();

                for cqe in cq.by_ref() {
                    let (token, op) = decode_ud(cqe.user_data());
                    let result = cqe.result();

                    match op {
                        OP_ACCEPT => {
                            if result >= 0 {
                                let client_fd = result as RawFd;
                                let conn_token = next_token;
                                next_token = next_token.wrapping_add(1).max(1);

                                if connections.len() < MAX_CONNECTIONS {
                                    unsafe {
                                        let flag: libc::c_int = 1;
                                        libc::setsockopt(
                                            client_fd,
                                            libc::IPPROTO_TCP,
                                            libc::TCP_NODELAY,
                                            &flag as *const _ as *const _,
                                            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                                        );
                                    }

                                    let state = ConnState::new(client_fd);
                                    let recv_ptr = state.recv_buf.as_ptr() as *mut u8;
                                    let recv_len = state.recv_buf.len();
                                    connections.insert(conn_token, state);
                                    pending_sq
                                        .push(make_recv(conn_token, client_fd, recv_ptr, recv_len));
                                } else {
                                    unsafe {
                                        libc::close(client_fd);
                                    }
                                }
                            }

                            // Re-arm Accept
                            accept_addr = unsafe { std::mem::zeroed() };
                            accept_addrlen =
                                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                            pending_sq.push(make_accept(
                                listen_fd,
                                &mut accept_addr as *mut _ as *mut libc::sockaddr,
                                &mut accept_addrlen,
                            ));
                        }

                        OP_RECV => {
                            if result <= 0 {
                                // Connection closed or error
                                if let Some(state) = connections.get(&token) {
                                    pending_sq.push(make_close(token, state.fd));
                                }
                                continue;
                            }

                            let n = result as usize;

                            if let Some(state) = connections.get_mut(&token) {
                                let data = state.recv_buf[..n].to_vec();
                                state.conn.feed_bytes(&data);

                                match state.conn.process_pending(&self.table, &mut self.pool) {
                                    Ok(keep_alive) => {
                                        if state.conn.has_pending_writes() {
                                            let response = state.conn.take_write_buf();
                                            state.pending_send = response;
                                            state.closing = !keep_alive;

                                            let send_ptr = state.pending_send.as_ptr();
                                            let send_len = state.pending_send.len();
                                            let fd = state.fd;
                                            pending_sq
                                                .push(make_send(token, fd, send_ptr, send_len));
                                        } else if !keep_alive {
                                            let fd = state.fd;
                                            pending_sq.push(make_close(token, fd));
                                        } else {
                                            let recv_ptr = state.recv_buf.as_ptr() as *mut u8;
                                            let recv_len = state.recv_buf.len();
                                            let fd = state.fd;
                                            pending_sq
                                                .push(make_recv(token, fd, recv_ptr, recv_len));
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "Worker [{}]: protocol error conn {token}: {e}",
                                            self.core_id
                                        );
                                        let fd = state.fd;
                                        pending_sq.push(make_close(token, fd));
                                    }
                                }
                            }
                        }

                        OP_SEND => {
                            if let Some(state) = connections.get(&token) {
                                if state.closing {
                                    let fd = state.fd;
                                    pending_sq.push(make_close(token, fd));
                                } else {
                                    let recv_ptr = state.recv_buf.as_ptr() as *mut u8;
                                    let recv_len = state.recv_buf.len();
                                    let fd = state.fd;
                                    pending_sq.push(make_recv(token, fd, recv_ptr, recv_len));
                                }
                            }
                        }

                        OP_CLOSE => {
                            connections.remove(&token);
                        }

                        _ => {}
                    }
                }
            } // CQ borrow released here

            // — Push all accumulated SQEs now that CQ borrow is released —
            if !pending_sq.is_empty() {
                let mut sq = ring.submission();
                sq.sync();
                for sqe in pending_sq.drain(..) {
                    // SAFETY: all pointers in sqe remain valid for the duration of this loop
                    unsafe {
                        let _ = sq.push(&sqe);
                    }
                }
            }

            // Idle tick: propagate arena activity timestamps and advance timing wheel (once per ~10k iterations)
            second_ticker += 1;
            if second_ticker % 10_000 == 0 {
                let now_sec = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as u32;
                self.pool.tick_second(now_sec);
                self.timing_wheel.advance_to(now_sec, &mut self.pool);
                for state in connections.values_mut() {
                    state.conn.set_current_sec(now_sec);
                }
            }
        }

        log::info!("Worker [{}]: io_uring shut down", self.core_id);
        Ok(())
    }
}
