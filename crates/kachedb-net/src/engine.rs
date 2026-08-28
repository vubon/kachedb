//! `kachedb-net` — Per-core thread-per-core asynchronous networking engine.
//!
//! # Architecture
//!
//! - **Thread-per-core**: Each active CPU core runs an independent `WorkerThread`.
//! - **Thread Pinning**: Binds the worker to its specific physical CPU core using `kachedb_core::pin_current_thread_to_core`.
//! - **Local Storage**: Each worker owns a thread-local `SlabPool` and `SwissTable` with zero cross-thread lock overhead.
//! - **Connection Dispatch** (default): A single accept thread distributes connections round-robin via crossbeam channels.
//! - **Legacy SO_REUSEPORT**: Each worker creates its own listener (opt-in fallback).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crossbeam_channel::Receiver;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use kachedb_core::{HashedTimingWheel, SlabPool, pin_current_thread_to_core};
use kachedb_hash::ShardedSwissTable;

use crate::connection::Connection;
use crate::error::NetError;

const SERVER_TOKEN: Token = Token(0);
const EVENTS_CAPACITY: usize = 1024;
const DEFAULT_POOL_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB per core

/// Connection source for a `WorkerThread`.
enum ConnSource {
    /// Accept-dispatch mode: receive pre-accepted connections from a crossbeam channel.
    Channel(Receiver<TcpStream>),
    /// Legacy SO_REUSEPORT mode: each worker creates its own listener.
    Listener(SocketAddr),
}

/// Per-core worker thread running an independent `mio` event loop.
pub struct WorkerThread {
    pub core_id: u16,
    pub pool: SlabPool,
    pub table: Arc<ShardedSwissTable>,
    pub timing_wheel: HashedTimingWheel,
    conn_source: ConnSource,
}

fn create_reuseport_listener(addr: SocketAddr) -> Result<TcpListener, std::io::Error> {
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

        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }

        if libc::listen(fd, 1024) < 0 {
            let err = std::io::Error::last_os_error();
            libc::close(fd);
            return Err(err);
        }

        let std_listener = std::net::TcpListener::from_raw_fd(fd);
        Ok(TcpListener::from_std(std_listener))
    }
}

impl WorkerThread {
    /// Creates a new `WorkerThread` in **legacy SO_REUSEPORT mode** (each worker listens independently).
    pub fn new(core_id: u16, bind_addr: SocketAddr) -> Result<Self, NetError> {
        Self::with_shared_table(
            core_id,
            bind_addr,
            DEFAULT_POOL_CAPACITY,
            Arc::new(ShardedSwissTable::new()),
        )
    }

    /// Creates a new `WorkerThread` in **legacy SO_REUSEPORT mode** with explicit pool capacity and shared table.
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
            conn_source: ConnSource::Listener(bind_addr),
        })
    }

    /// Creates a new `WorkerThread` in **accept-dispatch mode** (receives connections from a channel).
    pub fn with_channel(
        core_id: u16,
        pool_bytes: usize,
        table: Arc<ShardedSwissTable>,
        receiver: Receiver<TcpStream>,
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
            conn_source: ConnSource::Channel(receiver),
        })
    }

    /// Runs the single-threaded event loop until `shutdown` is signaled.
    pub fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), NetError> {
        // Pin this thread to its assigned core
        let _ = pin_current_thread_to_core(self.core_id as usize);

        match &self.conn_source {
            ConnSource::Channel(_) => {
                log::info!(
                    "Worker [{core}]: pinned to core, accept-dispatch mode",
                    core = self.core_id
                );
                self.run_channel_mode(shutdown)
            }
            ConnSource::Listener(addr) => {
                let addr = *addr;
                log::info!(
                    "Worker [{core}]: pinned to core, SO_REUSEPORT mode on {addr}",
                    core = self.core_id
                );
                self.run_listener_mode(addr, shutdown)
            }
        }
    }

    /// Event loop for **accept-dispatch mode**: receives connections from a crossbeam channel.
    fn run_channel_mode(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), NetError> {
        let receiver = match &self.conn_source {
            ConnSource::Channel(rx) => rx.clone(),
            _ => unreachable!(),
        };

        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(EVENTS_CAPACITY);
        let mut connections: HashMap<Token, (TcpStream, Connection)> = HashMap::new();
        let mut next_token = 1usize;

        while !shutdown.load(Ordering::Relaxed) {
            // Drain all pending connections from the accept-dispatch channel
            while let Ok(mut stream) = receiver.try_recv() {
                let token = Token(next_token);
                next_token += 1;

                if let Err(e) = poll.registry().register(
                    &mut stream,
                    token,
                    Interest::READABLE | Interest::WRITABLE,
                ) {
                    log::error!("Failed to register dispatched connection token {token:?}: {e}");
                    continue;
                }

                log::debug!(
                    "Worker [{core}]: registered dispatched connection {token:?}",
                    core = self.core_id
                );
                connections.insert(token, (stream, Connection::new()));
            }

            match poll.poll(&mut events, Some(Duration::from_millis(10))) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(NetError::Io(e)),
            }

            self.process_events(&mut events, &mut connections, &mut poll)?;

            // Idle tick: advance timing wheel and pool arena timestamps
            let now_sec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as u32;
            self.pool.tick_second(now_sec);
            self.timing_wheel.advance_to(now_sec, &mut self.pool);
            for (_, conn) in connections.values_mut() {
                conn.set_current_sec(now_sec);
            }
        }

        log::info!(
            "Worker [{core}]: shut down successfully",
            core = self.core_id
        );
        Ok(())
    }

    /// Event loop for **legacy SO_REUSEPORT mode**: each worker creates its own listener.
    fn run_listener_mode(
        &mut self,
        bind_addr: SocketAddr,
        shutdown: Arc<AtomicBool>,
    ) -> Result<(), NetError> {
        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(EVENTS_CAPACITY);

        let mut listener = create_reuseport_listener(bind_addr)?;
        poll.registry()
            .register(&mut listener, SERVER_TOKEN, Interest::READABLE)?;

        let mut connections: HashMap<Token, (TcpStream, Connection)> = HashMap::new();
        let mut next_token = 1usize;

        while !shutdown.load(Ordering::Relaxed) {
            match poll.poll(&mut events, Some(Duration::from_millis(100))) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(NetError::Io(e)),
            }

            // Accept new connections from listener
            for event in events.iter() {
                if event.token() == SERVER_TOKEN {
                    loop {
                        match listener.accept() {
                            Ok((mut stream, peer_addr)) => {
                                log::debug!(
                                    "Worker [{core}]: accepted conn from {peer_addr}",
                                    core = self.core_id
                                );
                                let _ = stream.set_nodelay(true);
                                let token = Token(next_token);
                                next_token += 1;

                                if let Err(e) = poll.registry().register(
                                    &mut stream,
                                    token,
                                    Interest::READABLE | Interest::WRITABLE,
                                ) {
                                    log::error!(
                                        "Failed to register connection token {token:?}: {e}"
                                    );
                                    continue;
                                }

                                connections.insert(token, (stream, Connection::new()));
                            }
                            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                            Err(e) => {
                                log::error!(
                                    "Worker [{core}]: accept error: {e}",
                                    core = self.core_id
                                );
                                break;
                            }
                        }
                    }
                }
            }

            self.process_events(&mut events, &mut connections, &mut poll)?;

            // Idle tick: advance timing wheel and pool arena timestamps
            let now_sec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as u32;
            self.pool.tick_second(now_sec);
            self.timing_wheel.advance_to(now_sec, &mut self.pool);
            for (_, conn) in connections.values_mut() {
                conn.set_current_sec(now_sec);
            }
        }

        log::info!(
            "Worker [{core}]: shut down successfully",
            core = self.core_id
        );
        Ok(())
    }

    /// Process I/O events for active connections (shared between both modes).
    fn process_events(
        &mut self,
        events: &mut Events,
        connections: &mut HashMap<Token, (TcpStream, Connection)>,
        poll: &mut Poll,
    ) -> Result<(), NetError> {
        for event in events.iter() {
            let token = event.token();
            if token == SERVER_TOKEN {
                continue; // Handled by listener mode above
            }

            let mut should_remove = false;

            if let Some((stream, conn)) = connections.get_mut(&token) {
                if event.is_readable() {
                    loop {
                        match conn.read_from(stream) {
                            Ok(n) if n > 0 => {
                                match conn.process_incoming(&self.table, &mut self.pool) {
                                    Ok(keep_alive) => {
                                        if !keep_alive {
                                            should_remove = true;
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "Worker [{core}]: protocol error on {token:?}: {e}",
                                            core = self.core_id
                                        );
                                        should_remove = true;
                                        break;
                                    }
                                }
                            }
                            Ok(_) => break, // WouldBlock or buffer drained
                            Err(NetError::ConnectionClosed) => {
                                should_remove = true;
                                break;
                            }
                            Err(NetError::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                break;
                            }
                            Err(e) => {
                                log::warn!(
                                    "Worker [{core}]: read error on {token:?}: {e}",
                                    core = self.core_id
                                );
                                should_remove = true;
                                break;
                            }
                        }
                    }
                }

                if !should_remove && (conn.has_pending_writes() || event.is_writable()) {
                    while conn.has_pending_writes() {
                        match conn.flush_to_stream(stream) {
                            Ok(0) => break,
                            Ok(_) => {}
                            Err(NetError::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                break;
                            }
                            Err(e) => {
                                log::warn!(
                                    "Worker [{core}]: write error on {token:?}: {e}",
                                    core = self.core_id
                                );
                                should_remove = true;
                                break;
                            }
                        }
                    }
                }
            }

            if should_remove {
                if let Some((mut stream, _)) = connections.remove(&token) {
                    let _ = poll.registry().deregister(&mut stream);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream as StdTcpStream;
    use std::thread;

    #[test]
    fn worker_handles_redis_traffic_over_tcp() {
        let bind_addr: SocketAddr = "127.0.0.1:16379".parse().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_worker = shutdown.clone();

        let mut worker = WorkerThread::new(0, bind_addr).unwrap();

        let handle = thread::spawn(move || {
            worker.run(shutdown_worker).unwrap();
        });

        // Give the worker 50ms to bind and start listening
        thread::sleep(Duration::from_millis(50));

        let mut client = StdTcpStream::connect(bind_addr).expect("connect to worker");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        // 1. Send PING
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut buf = [0u8; 128];
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+PONG\r\n");

        // 2. Send SET
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nkachedb\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // 3. Send GET
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"$7\r\nkachedb\r\n");

        // 4. Send QUIT
        client.write_all(b"*1\r\n$4\r\nQUIT\r\n").unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // Shut down worker
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
