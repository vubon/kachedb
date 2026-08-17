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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use io_uring::{opcode, squeue, types, IoUring};

use kachedb_core::{pin_current_thread_to_core, SlabPool};
use kachedb_hash::SwissTable;

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

fn make_accept(listen_fd: RawFd, addr: *mut libc::sockaddr, addrlen: *mut libc::socklen_t) -> squeue::Entry {
    unsafe {
        opcode::Accept::new(types::Fd(listen_fd), addr as *mut _, addrlen)
            .build()
            .user_data(ud(0, OP_ACCEPT))
    }
}

fn make_recv(token: u32, fd: RawFd, buf_ptr: *mut u8, buf_len: usize) -> squeue::Entry {
    unsafe {
        opcode::Recv::new(types::Fd(fd), buf_ptr, buf_len as u32)
            .build()
            .user_data(ud(token, OP_RECV))
    }
}

fn make_send(token: u32, fd: RawFd, buf_ptr: *const u8, buf_len: usize) -> squeue::Entry {
    unsafe {
        opcode::Send::new(types::Fd(fd), buf_ptr, buf_len as u32)
            .build()
            .user_data(ud(token, OP_SEND))
    }
}

fn make_close(token: u32, fd: RawFd) -> squeue::Entry {
    opcode::Close::new(types::Fd(fd))
        .build()
        .user_data(ud(token, OP_CLOSE))
}

// ── UringWorkerThread ─────────────────────────────────────────────────────────

/// Per-core io_uring worker.
pub struct UringWorkerThread {
    pub core_id: u16,
    pub pool: SlabPool,
    pub table: SwissTable,
    pub bind_addr: SocketAddr,
}

impl UringWorkerThread {
    pub fn new(core_id: u16, bind_addr: SocketAddr) -> Result<Self, NetError> {
        let pool = SlabPool::new(core_id, DEFAULT_POOL_CAPACITY)?;
        let table = SwissTable::with_capacity(65536);
        Ok(Self { core_id, pool, table, bind_addr })
    }

    pub fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), NetError> {
        let _ = pin_current_thread_to_core(self.core_id as usize);

        // ── io_uring init ────────────────────────────────────────────────────
        let mut ring = match IoUring::builder().setup_sqpoll(2000).build(RING_ENTRIES) {
            Ok(r) => {
                log::info!("Worker [{}]: io_uring SQPOLL enabled on {}", self.core_id, self.bind_addr);
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

        // ── Bind + listen ────────────────────────────────────────────────────
        let listener = std::net::TcpListener::bind(self.bind_addr)?;
        listener.set_nonblocking(true)?;
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
                                    pending_sq.push(make_recv(conn_token, client_fd, recv_ptr, recv_len));
                                } else {
                                    unsafe { libc::close(client_fd); }
                                }
                            }

                            // Re-arm Accept
                            accept_addr = unsafe { std::mem::zeroed() };
                            accept_addrlen = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
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

                                match state.conn.process_pending(&mut self.table, &mut self.pool) {
                                    Ok(keep_alive) => {
                                        if state.conn.has_pending_writes() {
                                            let response = state.conn.take_write_buf();
                                            state.pending_send = response;
                                            state.closing = !keep_alive;

                                            let send_ptr = state.pending_send.as_ptr();
                                            let send_len = state.pending_send.len();
                                            let fd = state.fd;
                                            pending_sq.push(make_send(token, fd, send_ptr, send_len));
                                        } else if !keep_alive {
                                            let fd = state.fd;
                                            pending_sq.push(make_close(token, fd));
                                        } else {
                                            let recv_ptr = state.recv_buf.as_ptr() as *mut u8;
                                            let recv_len = state.recv_buf.len();
                                            let fd = state.fd;
                                            pending_sq.push(make_recv(token, fd, recv_ptr, recv_len));
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!("Worker [{}]: protocol error conn {token}: {e}", self.core_id);
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
                    unsafe { let _ = sq.push(&sqe); }
                }
            }

            // Idle tick: propagate arena activity timestamps (once per ~10k iterations)
            second_ticker += 1;
            if second_ticker % 10_000 == 0 {
                let now_sec = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as u32;
                self.pool.tick_second(now_sec);
            }
        }

        log::info!("Worker [{}]: io_uring shut down", self.core_id);
        Ok(())
    }
}
