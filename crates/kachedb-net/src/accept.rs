//! `kachedb-net` — Accept-and-Dispatch connection distributor.
//!
//! # Architecture
//!
//! A single **accept thread** (floating, no CPU pinning) binds one `TcpListener`
//! and round-robin distributes accepted `TcpStream` connections to per-worker
//! bounded crossbeam channels.
//!
//! This eliminates the `SO_REUSEPORT` localhost connection skew problem where
//! the Linux kernel's 4-tuple hash unevenly assigns connections to workers.
//!
//! The accept thread is extremely lightweight (~1% CPU) and the OS scheduler
//! naturally sneaks it into idle gaps between worker `epoll_wait` timeouts.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_channel::Sender;
use mio::net::TcpStream;

/// Channel capacity per worker — bounds backpressure.
/// 256 is generous for connection establishment bursts.
const CHANNEL_CAPACITY: usize = 256;

/// Creates per-worker bounded channels for connection dispatch.
///
/// Returns `(senders, receivers)` — one pair per worker.
pub fn create_dispatch_channels(
    num_workers: usize,
) -> (
    Vec<Sender<TcpStream>>,
    Vec<crossbeam_channel::Receiver<TcpStream>>,
) {
    let mut senders = Vec::with_capacity(num_workers);
    let mut receivers = Vec::with_capacity(num_workers);
    for _ in 0..num_workers {
        let (tx, rx) = crossbeam_channel::bounded(CHANNEL_CAPACITY);
        senders.push(tx);
        receivers.push(rx);
    }
    (senders, receivers)
}

/// Lightweight accept-and-dispatch thread that distributes connections
/// to worker threads via round-robin crossbeam channels.
pub struct AcceptDispatcher {
    bind_addr: SocketAddr,
    senders: Vec<Sender<TcpStream>>,
    shutdown: Arc<AtomicBool>,
}

impl AcceptDispatcher {
    /// Creates a new `AcceptDispatcher`.
    pub fn new(
        bind_addr: SocketAddr,
        senders: Vec<Sender<TcpStream>>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self {
            bind_addr,
            senders,
            shutdown,
        }
    }

    /// Runs the accept loop, distributing connections round-robin.
    ///
    /// This method blocks until `shutdown` is set to `true`.
    /// Should be spawned in a dedicated thread (floating, no CPU pinning).
    pub fn run(&self) -> Result<(), std::io::Error> {
        use std::os::unix::io::FromRawFd;

        let listener = unsafe {
            let domain = match self.bind_addr {
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

            let mut storage: libc::sockaddr_storage = std::mem::zeroed();
            let socklen: libc::socklen_t = match self.bind_addr {
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

            // Set non-blocking for polling
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
            mio::net::TcpListener::from_std(std_listener)
        };

        // Use mio to poll the listener for accept readiness
        let mut poll = mio::Poll::new()?;
        let mut events = mio::Events::with_capacity(64);
        let token = mio::Token(0);
        let mut listener = listener;

        poll.registry()
            .register(&mut listener, token, mio::Interest::READABLE)?;

        let mut next_worker: usize = 0;
        let num_workers = self.senders.len();

        log::info!(
            "AcceptDispatcher: listening on {} (round-robin → {} workers)",
            self.bind_addr,
            num_workers
        );

        while !self.shutdown.load(Ordering::Relaxed) {
            match poll.poll(&mut events, Some(std::time::Duration::from_millis(100))) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }

            for _event in events.iter() {
                // Accept all pending connections
                loop {
                    match listener.accept() {
                        Ok((stream, peer_addr)) => {
                            // Set TCP_NODELAY before dispatching
                            let _ = stream.set_nodelay(true);

                            log::debug!(
                                "AcceptDispatcher: accepted {} → worker[{next_worker}]",
                                peer_addr
                            );

                            // Round-robin dispatch to workers
                            if self.senders[next_worker].try_send(stream).is_err() {
                                log::warn!(
                                    "AcceptDispatcher: worker[{next_worker}] channel full, dropping connection from {}",
                                    peer_addr
                                );
                            }

                            next_worker = (next_worker + 1) % num_workers;
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                        Err(e) => {
                            log::error!("AcceptDispatcher: accept error: {e}");
                            break;
                        }
                    }
                }
            }
        }

        log::info!("AcceptDispatcher: shut down");
        Ok(())
    }
}
