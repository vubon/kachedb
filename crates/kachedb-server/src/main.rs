//! `kachedb-server` — Multi-core daemon entry point.

mod config;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use config::ServerConfig;
use kachedb_net::{AcceptDispatcher, create_dispatch_channels};
use kachedb_shm::ShmChannel;

fn main() {
    // Basic terminal logging initialization
    let _ = simple_logger();

    let config = ServerConfig::parse_args();

    println!(
        r#"
  _  __           _          _____  ____  
 | |/ /          | |        |  __ \|  _ \ 
 | ' / __ _  ___| |__   ___| |  | | |_) |
 |  < / _` |/ __| '_ \ / _ \ |  | |  _ < 
 | . \ (_| | (__| | | |  __/ |__| | |_) |
 |_|\_\__,_|\___|_| |_|\___|_____/|____/ 
"#
    );

    println!("⚡ KacheDB Daemon v0.1.0 starting...");
    println!("   └─ TCP Listener:       {}", config.bind_addr);
    println!(
        "   └─ Worker Threads:     {} cores (Thread-Per-Core topology)",
        config.num_workers
    );
    println!(
        "   └─ Memory Pool:        {} MB/core (Total: {} MB)",
        config.pool_mb_per_core,
        config.num_workers * config.pool_mb_per_core
    );
    println!("   └─ Connection Dispatch: Accept-Dispatch (round-robin crossbeam channels)");
    println!(
        "   └─ POSIX SHM IPC:      {}",
        if config.shm_enabled {
            "Enabled (/dev/shm)"
        } else {
            "Disabled"
        }
    );
    #[cfg(target_os = "linux")]
    println!("   └─ I/O Engine:         epoll + TCP_NODELAY (Linux)");
    #[cfg(not(target_os = "linux"))]
    println!("   └─ I/O Engine:         mio/kqueue (macOS/BSD)");
    println!();

    let shutdown = Arc::new(AtomicBool::new(false));
    let shared_table = Arc::new(kachedb_hash::ShardedSwissTable::new());

    // Handle SIGINT (Ctrl+C) and SIGTERM gracefully
    register_shutdown_handler(shutdown.clone());

    // Clean up any orphaned POSIX SHM regions from previous unclean shutdowns
    if config.shm_enabled {
        cleanup_stale_shm(config.num_workers);
    }

    // Create per-worker crossbeam dispatch channels
    let (senders, receivers) = create_dispatch_channels(config.num_workers);

    let mut handles = Vec::with_capacity(config.num_workers + 1);

    // Spawn worker threads (accept-dispatch mode)
    for (core_id, receiver) in receivers.into_iter().enumerate() {
        let shutdown_worker = shutdown.clone();
        let shm_enabled = config.shm_enabled;
        let worker_table = shared_table.clone();
        let pool_bytes = config.pool_mb_per_core * 1024 * 1024;

        let handle = thread::Builder::new()
            .name(format!("kachedb-worker-{}", core_id))
            .spawn(move || {
                // Initialize core-local POSIX SHM channel if enabled
                let _shm_channel = if shm_enabled {
                    let shm_name = format!("kachedb_{core_id}");
                    match ShmChannel::create(&shm_name, 1024) {
                        Ok(ch) => {
                            log::info!("Worker [{core_id}]: created POSIX SHM region '{shm_name}'");
                            Some(ch)
                        }
                        Err(e) => {
                            log::warn!(
                                "Worker [{core_id}]: failed to create SHM region '{shm_name}': {e}"
                            );
                            None
                        }
                    }
                } else {
                    None
                };

                let mut worker = match kachedb_net::WorkerThread::with_channel(
                    core_id as u16,
                    pool_bytes,
                    worker_table,
                    receiver,
                ) {
                    Ok(w) => w,
                    Err(e) => {
                        log::error!("Worker [{core_id}]: failed to initialize: {e}");
                        return;
                    }
                };
                if let Err(e) = worker.run(shutdown_worker) {
                    log::error!("Worker [{core_id}]: event loop terminated with error: {e}");
                }
            })
            .expect("failed to spawn worker thread");

        handles.push(handle);
    }

    // Spawn the accept-dispatcher thread (floating, no CPU pinning)
    let accept_shutdown = shutdown.clone();
    let accept_handle = thread::Builder::new()
        .name("kachedb-accept".to_string())
        .spawn(move || {
            let dispatcher = AcceptDispatcher::new(config.bind_addr, senders, accept_shutdown);
            if let Err(e) = dispatcher.run() {
                log::error!("AcceptDispatcher: terminated with error: {e}");
            }
        })
        .expect("failed to spawn accept-dispatcher thread");

    handles.push(accept_handle);

    println!("🚀 KacheDB is ready to accept Redis & LLM tensor connections!");

    // Wait for all worker threads to exit
    for handle in handles {
        let _ = handle.join();
    }

    if config.shm_enabled {
        cleanup_stale_shm(config.num_workers);
    }

    println!("🛑 KacheDB server stopped gracefully.");
}

struct SimpleLogger;

impl log::Log for SimpleLogger {
    fn enabled(&self, _metadata: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if self.enabled(record.metadata()) {
            eprintln!("[{}] {}", record.level(), record.args());
        }
    }

    fn flush(&self) {}
}

static LOGGER: SimpleLogger = SimpleLogger;

fn simple_logger() -> Result<(), ()> {
    let _ = log::set_logger(&LOGGER);
    log::set_max_level(log::LevelFilter::Info);
    Ok(())
}

static GLOBAL_SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" fn sig_handler(_sig: libc::c_int) {
    GLOBAL_SHUTDOWN.store(true, Ordering::SeqCst);
}

fn install_signal_handlers() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = sig_handler as *const () as usize;
        sa.sa_flags = libc::SA_RESTART;
        libc::sigemptyset(&mut sa.sa_mask);

        libc::sigaction(libc::SIGINT, &sa, std::ptr::null_mut());
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());
    }
}

fn register_shutdown_handler(shutdown: Arc<AtomicBool>) {
    install_signal_handlers();
    let s = shutdown.clone();
    thread::Builder::new()
        .name("kachedb-signal-watcher".to_string())
        .spawn(move || {
            while !s.load(Ordering::Relaxed) {
                if GLOBAL_SHUTDOWN.load(Ordering::Relaxed) {
                    log::info!(
                        "Received shutdown signal (SIGINT/SIGTERM). Initiating graceful shutdown..."
                    );
                    s.store(true, Ordering::SeqCst);
                    break;
                }
                thread::sleep(std::time::Duration::from_millis(50));
            }
        })
        .expect("failed to spawn signal watcher thread");
}

fn cleanup_stale_shm(num_workers: usize) {
    for core_id in 0..num_workers {
        let shm_name = format!("kachedb_{core_id}");
        cfg_if::cfg_if! {
            if #[cfg(target_os = "linux")] {
                let path = format!("/dev/shm/{shm_name}");
                let _ = std::fs::remove_file(&path);
            } else {
                use std::ffi::CString;
                let full_name = format!("/{shm_name}");
                if let Ok(c) = CString::new(full_name) {
                    unsafe { libc::shm_unlink(c.as_ptr()) };
                }
            }
        }
    }
}
