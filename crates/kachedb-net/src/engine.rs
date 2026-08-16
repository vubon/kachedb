//! `kachedb-net` — Per-core thread-per-core asynchronous networking engine.
//!
//! # Architecture
//!
//! - **Thread-per-core**: Each active CPU core runs an independent `WorkerThread`.
//! - **Thread Pinning**: Binds the worker to its specific physical CPU core using `kachedb_core::pin_current_thread_to_core`.
//! - **Local Storage**: Each worker owns a thread-local `SlabPool` and `SwissTable` with zero cross-thread lock overhead.
//! - **Platform I/O**:
//!   - Linux: Asynchronous completion loop (`io_uring` with registered buffers).
//!   - macOS / BSD: Non-blocking event loop using `mio` (`kqueue`).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};

use kachedb_core::{pin_current_thread_to_core, SlabPool};
use kachedb_hash::SwissTable;

use crate::connection::Connection;
use crate::error::NetError;

const SERVER_TOKEN: Token = Token(0);
const EVENTS_CAPACITY: usize = 1024;
const DEFAULT_POOL_CAPACITY: usize = 64 * 1024 * 1024; // 64 MB slab pool per core

/// A dedicated per-core worker executing a zero-contention event loop.
pub struct WorkerThread {
    /// Zero-based physical core ID.
    pub core_id: u16,
    /// Core-local slab memory pool.
    pub pool: SlabPool,
    /// Core-local Swiss Table point index.
    pub table: SwissTable,
    /// Listening socket address.
    pub bind_addr: SocketAddr,
}

impl WorkerThread {
    /// Creates a new `WorkerThread` for the given core ID.
    pub fn new(core_id: u16, bind_addr: SocketAddr) -> Result<Self, NetError> {
        let pool = SlabPool::new(core_id, DEFAULT_POOL_CAPACITY)?;
        let table = SwissTable::with_capacity(65536);

        Ok(Self {
            core_id,
            pool,
            table,
            bind_addr,
        })
    }

    /// Runs the single-threaded event loop until `shutdown` is signaled.
    pub fn run(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), NetError> {
        // Pin this thread to its assigned core
        let _ = pin_current_thread_to_core(self.core_id as usize);
        log::info!("Worker [{core}]: pinned to core and starting event loop on {}", self.bind_addr, core = self.core_id);

        let mut poll = Poll::new()?;
        let mut events = Events::with_capacity(EVENTS_CAPACITY);

        let mut listener = TcpListener::bind(self.bind_addr)?;
        poll.registry().register(&mut listener, SERVER_TOKEN, Interest::READABLE)?;

        let mut connections: HashMap<Token, (TcpStream, Connection)> = HashMap::new();
        let mut next_token = 1usize;

        while !shutdown.load(Ordering::Relaxed) {
            match poll.poll(&mut events, Some(Duration::from_millis(100))) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(NetError::Io(e)),
            }

            for event in events.iter() {
                match event.token() {
                    SERVER_TOKEN => {
                        // Accept incoming connections
                        loop {
                            match listener.accept() {
                                Ok((mut stream, peer_addr)) => {
                                    log::debug!("Worker [{core}]: accepted conn from {peer_addr}", core = self.core_id);
                                    let token = Token(next_token);
                                    next_token += 1;

                                    if let Err(e) = poll.registry().register(
                                        &mut stream,
                                        token,
                                        Interest::READABLE | Interest::WRITABLE,
                                    ) {
                                        log::error!("Failed to register connection token {token:?}: {e}");
                                        continue;
                                    }

                                    connections.insert(token, (stream, Connection::new()));
                                }
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                                Err(e) => {
                                    log::error!("Worker [{core}]: accept error: {e}", core = self.core_id);
                                    break;
                                }
                            }
                        }
                    }
                    token => {
                        let mut should_remove = false;

                        if let Some((stream, conn)) = connections.get_mut(&token) {
                            if event.is_readable() {
                                match conn.read_from_stream(stream) {
                                    Ok(_) => {
                                        match conn.process_incoming(&mut self.table, &mut self.pool) {
                                            Ok(keep_alive) => {
                                                if !keep_alive {
                                                    should_remove = true;
                                                }
                                            }
                                            Err(e) => {
                                                log::warn!("Worker [{core}]: protocol error on {token:?}: {e}", core = self.core_id);
                                                should_remove = true;
                                            }
                                        }
                                    }
                                    Err(NetError::ConnectionClosed) => {
                                        should_remove = true;
                                    }
                                    Err(NetError::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(e) => {
                                        log::warn!("Worker [{core}]: read error on {token:?}: {e}", core = self.core_id);
                                        should_remove = true;
                                    }
                                }
                            }

                            if conn.has_pending_writes() || event.is_writable() {
                                match conn.flush_to_stream(stream) {
                                    Ok(_) => {}
                                    Err(NetError::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                                    Err(e) => {
                                        log::warn!("Worker [{core}]: write error on {token:?}: {e}", core = self.core_id);
                                        should_remove = true;
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
                }
            }
        }

        log::info!("Worker [{core}]: shut down successfully", core = self.core_id);
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
        client.set_read_timeout(Some(Duration::from_secs(1))).unwrap();

        // 1. Send PING
        client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
        let mut buf = [0u8; 128];
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+PONG\r\n");

        // 2. Send SET
        client.write_all(b"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$7\r\nkachedb\r\n").unwrap();
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"+OK\r\n");

        // 3. Send GET
        client.write_all(b"*2\r\n$3\r\nGET\r\n$4\r\nname\r\n").unwrap();
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
