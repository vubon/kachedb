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

use crate::connection::{Connection, DEFAULT_VECTORS};
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
        let mut last_tick_sec = 0u32;
        let mut expired_entries = Vec::with_capacity(64);

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
                let mut conn = Connection::new();
                conn.set_current_sec(last_tick_sec);
                connections.insert(token, (stream, conn));
            }

            let poll_timeout = if connections.is_empty() {
                Some(Duration::from_millis(1))
            } else {
                Some(Duration::from_millis(5))
            };

            match poll.poll(&mut events, poll_timeout) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(NetError::Io(e)),
            }

            self.process_events(&mut events, &mut connections, &mut poll)?;

            // Idle tick: advance timing wheel and pool arena timestamps once per second
            let now_sec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as u32;

            if now_sec != last_tick_sec {
                last_tick_sec = now_sec;
                self.pool.tick_second(now_sec);

                expired_entries.clear();
                self.timing_wheel
                    .advance_expired_entries(now_sec, &mut expired_entries);
                for entry in &expired_entries {
                    if let Some(removed) = self
                        .table
                        .remove_if_matching(entry.key_hash, entry.slab_block_id)
                    {
                        let _ = self.pool.deallocate(removed.slab_block_id);
                    }
                }

                for (_, conn) in connections.values_mut() {
                    conn.set_current_sec(now_sec);
                }
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
        let mut last_tick_sec = 0u32;
        let mut expired_entries = Vec::with_capacity(64);

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

                                let mut conn = Connection::new();
                                conn.set_current_sec(last_tick_sec);
                                connections.insert(token, (stream, conn));
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

            // Idle tick: advance timing wheel and pool arena timestamps once per second
            let now_sec = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as u32;

            if now_sec != last_tick_sec {
                last_tick_sec = now_sec;
                self.pool.tick_second(now_sec);

                expired_entries.clear();
                self.timing_wheel
                    .advance_expired_entries(now_sec, &mut expired_entries);
                for entry in &expired_entries {
                    if let Some(removed) = self
                        .table
                        .remove_if_matching(entry.key_hash, entry.slab_block_id)
                    {
                        let _ = self.pool.deallocate(removed.slab_block_id);
                    }
                }

                for (_, conn) in connections.values_mut() {
                    conn.set_current_sec(now_sec);
                }
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
                    let read_res = conn.read_from(stream);
                    match read_res {
                        Ok(_) => {
                            if conn.has_unprocessed_input() {
                                match conn.process_incoming_with_wheel(
                                    &self.table,
                                    &mut self.pool,
                                    &DEFAULT_VECTORS,
                                    Some(&mut self.timing_wheel),
                                ) {
                                    Ok(keep_alive) => {
                                        if !keep_alive {
                                            should_remove = true;
                                        }
                                    }
                                    Err(e) => {
                                        log::warn!(
                                            "Worker [{core}]: protocol error on {token:?}: {e}",
                                            core = self.core_id
                                        );
                                        should_remove = true;
                                    }
                                }
                            }
                        }
                        Err(NetError::ConnectionClosed) => {
                            should_remove = true;
                        }
                        Err(NetError::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            if conn.has_unprocessed_input() {
                                let _ = conn.process_incoming_with_wheel(
                                    &self.table,
                                    &mut self.pool,
                                    &DEFAULT_VECTORS,
                                    Some(&mut self.timing_wheel),
                                );
                            }
                        }
                        Err(e) => {
                            log::warn!(
                                "Worker [{core}]: read error on {token:?}: {e}",
                                core = self.core_id
                            );
                            should_remove = true;
                        }
                    }
                }

                if conn.has_pending_writes() || event.is_writable() {
                    while conn.has_pending_writes() {
                        match conn.flush_to_stream(stream) {
                            Ok(0) => break,
                            Ok(_) => {}
                            Err(NetError::Io(ref e))
                                if e.kind() == std::io::ErrorKind::WouldBlock =>
                            {
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

    #[test]
    fn worker_handles_expire_ttl_persist_over_tcp() {
        let bind_addr: SocketAddr = "127.0.0.1:16380".parse().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_worker = shutdown.clone();

        let mut worker = WorkerThread::new(1, bind_addr).unwrap();

        let handle = thread::spawn(move || {
            worker.run(shutdown_worker).unwrap();
        });

        thread::sleep(Duration::from_millis(50));

        let mut client = StdTcpStream::connect(bind_addr).expect("connect to worker");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();

        let mut buf = [0u8; 128];

        // 1. SET session token123
        client
            .write_all(b"*3\r\n$3\r\nSET\r\n$7\r\nsession\r\n$8\r\ntoken123\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // 2. TTL session -> -1
        client
            .write_all(b"*2\r\n$3\r\nTTL\r\n$7\r\nsession\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":-1\r\n");

        // 3. EXPIRE session 300 -> :1
        client
            .write_all(b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nsession\r\n$3\r\n300\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":1\r\n");

        // 4. TTL session -> should be positive
        client
            .write_all(b"*2\r\n$3\r\nTTL\r\n$7\r\nsession\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert!(n > 0 && buf[0] == b':');

        // 5. PERSIST session -> :1
        client
            .write_all(b"*2\r\n$7\r\nPERSIST\r\n$7\r\nsession\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":1\r\n");

        // 6. TTL session -> -1
        client
            .write_all(b"*2\r\n$3\r\nTTL\r\n$7\r\nsession\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":-1\r\n");

        // Shut down worker
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn worker_actively_reclaims_expired_keys_via_timing_wheel() {
        let bind_addr: SocketAddr = "127.0.0.1:16382".parse().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_worker = shutdown.clone();

        let table = Arc::new(ShardedSwissTable::new());
        let table_clone = Arc::clone(&table);

        let mut worker =
            WorkerThread::with_shared_table(2, bind_addr, DEFAULT_POOL_CAPACITY, table_clone)
                .unwrap();

        let handle = thread::spawn(move || {
            worker.run(shutdown_worker).unwrap();
        });

        thread::sleep(Duration::from_millis(50));

        let mut client = StdTcpStream::connect(bind_addr).expect("connect to worker");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let mut buf = [0u8; 128];

        // 1. SET temp_key ephemeral EX 1
        client
            .write_all(
                b"*5\r\n$3\r\nSET\r\n$8\r\ntemp_key\r\n$9\r\nephemeral\r\n$2\r\nEX\r\n$1\r\n1\r\n",
            )
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // Verify table has 1 active key
        assert_eq!(table.len(), 1);

        // 2. Wait 1.3 seconds for TimingWheel event loop tick to actively reclaim
        thread::sleep(Duration::from_millis(1300));

        // 3. Verify key was removed by TimingWheel background tick without passive lookup
        assert_eq!(table.len(), 0);

        // 4. GET temp_key -> returns null ($-1\r\n)
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$8\r\ntemp_key\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"$-1\r\n");

        // Shut down worker
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn worker_handles_extended_primitives_over_tcp() {
        let bind_addr: SocketAddr = "127.0.0.1:16383".parse().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_worker = shutdown.clone();

        let table = Arc::new(ShardedSwissTable::new());
        let table_clone = Arc::clone(&table);

        let mut worker =
            WorkerThread::with_shared_table(3, bind_addr, DEFAULT_POOL_CAPACITY, table_clone)
                .unwrap();

        let handle = thread::spawn(move || {
            worker.run(shutdown_worker).unwrap();
        });

        thread::sleep(Duration::from_millis(50));

        let mut client = StdTcpStream::connect(bind_addr).expect("connect to worker");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let mut buf = [0u8; 128];

        // 1. MSET user:1 Alice user:2 Bob -> +OK
        client
            .write_all(
                b"*5\r\n$4\r\nMSET\r\n$6\r\nuser:1\r\n$5\r\nAlice\r\n$6\r\nuser:2\r\n$3\r\nBob\r\n",
            )
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // 2. STRLEN user:1 -> :5
        client
            .write_all(b"*2\r\n$6\r\nSTRLEN\r\n$6\r\nuser:1\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":5\r\n");

        // 3. APPEND user:1 _Smith -> :11
        client
            .write_all(b"*3\r\n$6\r\nAPPEND\r\n$6\r\nuser:1\r\n$6\r\n_Smith\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":11\r\n");

        // 4. GET user:1 -> $11\r\nAlice_Smith\r\n
        client
            .write_all(b"*2\r\n$3\r\nGET\r\n$6\r\nuser:1\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"$11\r\nAlice_Smith\r\n");

        // 5. INCR hit_count -> :1
        client
            .write_all(b"*2\r\n$4\r\nINCR\r\n$9\r\nhit_count\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":1\r\n");

        // 6. INCRBY hit_count 99 -> :100
        client
            .write_all(b"*3\r\n$6\r\nINCRBY\r\n$9\r\nhit_count\r\n$2\r\n99\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":100\r\n");

        // 7. DECR hit_count -> :99
        client
            .write_all(b"*2\r\n$4\r\nDECR\r\n$9\r\nhit_count\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":99\r\n");

        // 8. DECRBY hit_count 50 -> :49
        client
            .write_all(b"*3\r\n$6\r\nDECRBY\r\n$9\r\nhit_count\r\n$2\r\n50\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b":49\r\n");

        // Shut down worker
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }

    #[test]
    fn worker_handles_hello_client_and_info_over_tcp() {
        let bind_addr: SocketAddr = "127.0.0.1:16384".parse().unwrap();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_worker = shutdown.clone();

        let table = Arc::new(ShardedSwissTable::new());
        let table_clone = Arc::clone(&table);

        let mut worker =
            WorkerThread::with_shared_table(4, bind_addr, DEFAULT_POOL_CAPACITY, table_clone)
                .unwrap();

        let handle = thread::spawn(move || {
            worker.run(shutdown_worker).unwrap();
        });

        thread::sleep(Duration::from_millis(50));

        let mut client = StdTcpStream::connect(bind_addr).expect("connect to worker");
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();

        let mut buf = [0u8; 1024];

        // 1. HELLO 3 SETNAME my-driver
        client
            .write_all(b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$9\r\nmy-driver\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        let resp = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(resp.starts_with("*14\r\n"));
        assert!(resp.contains("kachedb"));
        assert!(resp.contains("standalone"));

        // 2. CLIENT GETNAME -> $9\r\nmy-driver\r\n
        client
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"$9\r\nmy-driver\r\n");

        // 3. CLIENT SETNAME new-worker -> +OK\r\n
        client
            .write_all(b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$10\r\nnew-worker\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // 4. CLIENT GETNAME -> $10\r\nnew-worker\r\n
        client
            .write_all(b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n")
            .unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"$10\r\nnew-worker\r\n");

        // 5. INFO -> contains "# Server" and "kachedb_version:0.1.0"
        client.write_all(b"*1\r\n$4\r\nINFO\r\n").unwrap();
        let n = client.read(&mut buf).unwrap();
        let info_resp = std::str::from_utf8(&buf[..n]).unwrap();
        assert!(info_resp.contains("# Server"));
        assert!(info_resp.contains("kachedb_version:0.1.0"));
        assert!(info_resp.contains("# Memory"));

        // Shut down worker
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap();
    }
}
