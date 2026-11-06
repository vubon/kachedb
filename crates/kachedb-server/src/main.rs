//! `kachedb-server` — Multi-core daemon entry point.

mod aof;
mod aof_recovery;
mod aof_rewrite;
mod config;
pub mod crypto;
mod snapshot;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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

    println!(
        "⚡ KacheDB Daemon v{} (Production Stable) starting...",
        env!("CARGO_PKG_VERSION")
    );
    if let Some(ref path) = config.config_path {
        println!("   └─ Config File:        {}", path.display());
    }
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
    println!("   └─ Max Clients:        {}", config.maxclients);
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

    // Configure Max Clients limit
    kachedb_net::set_max_clients(config.maxclients);

    // Configure AUTH requirepass
    if let Some(ref pass) = config.requirepass {
        kachedb_net::set_requirepass(Some(pass.clone()));
    }

    // Configure TLS 1.3
    let tls_config = match (&config.tls_cert_path, &config.tls_key_path) {
        (Some(cert_path), Some(key_path)) => {
            match kachedb_net::load_server_config(
                cert_path,
                key_path,
                config.tls_ca_path.as_deref(),
            ) {
                Ok(cfg) => {
                    log::info!("TLS 1.3 server encryption active");
                    Some(cfg)
                }
                Err(e) => {
                    log::error!("Failed to initialize TLS: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ => None,
    };

    println!(
        "   └─ AOF Persistence:    {}",
        if config.aof_enabled {
            format!(
                "Enabled ({:?}, policy: {:?})",
                config.aof_path, config.appendfsync
            )
        } else {
            "Disabled".to_string()
        }
    );
    println!(
        "   └─ Snapshot Engine:    {}",
        if config.snapshot_enabled {
            format!(
                "Enabled ({:?}, interval: {}s)",
                config.snapshot_path, config.snapshot_interval_secs
            )
        } else {
            "Disabled".to_string()
        }
    );
    println!(
        "   └─ Snapshot Encryption:{}",
        if config.snapshot_encryption_enabled {
            format!("Enabled ({})", config.snapshot_encryption_cipher.as_str())
        } else {
            "Disabled (Plaintext)".to_string()
        }
    );
    println!(
        "   └─ TLS 1.3 / mTLS:     {}",
        if tls_config.is_some() {
            "Enabled"
        } else {
            "Disabled"
        }
    );
    println!(
        "   └─ AUTH Security:      {}",
        if config.requirepass.is_some() {
            "Protected (requirepass configured)"
        } else {
            "Open (no password)"
        }
    );
    println!();

    let shutdown = Arc::new(AtomicBool::new(false));
    let shared_table = Arc::new(kachedb_hash::ShardedSwissTable::new());

    // Handle SIGINT (Ctrl+C) and SIGTERM gracefully
    register_shutdown_handler(shutdown.clone());

    // Clean up any orphaned POSIX SHM regions from previous unclean shutdowns
    if config.shm_enabled {
        cleanup_stale_shm(config.num_workers);
    }

    let enc_config = config.snapshot_encryption_config();

    // 1. If snapshot exists, hydrate in-memory state from snapshot binary first
    let mut initial_pool =
        kachedb_core::SlabPool::new(0, config.pool_mb_per_core * 1024 * 1024).ok();
    if config.snapshot_enabled
        && config.snapshot_path.exists()
        && let Some(ref mut pool) = initial_pool
    {
        match snapshot::load_snapshot(
            &config.snapshot_path,
            &shared_table,
            pool,
            &kachedb_net::DEFAULT_VECTORS,
            Some(&enc_config),
        ) {
            Ok(Some(stats)) => println!(
                "💾 Hydrated snapshot successfully: {} vectors across {} indexes, {} KV keys from {:?}",
                stats.total_vectors,
                stats.vector_indexes,
                stats.total_kv_entries,
                config.snapshot_path
            ),
            Ok(None) => {}
            Err(e) => eprintln!("⚠️ Snapshot hydration error: {e}"),
        }
    }

    // 2. If AOF is enabled, replay delta mutations to restore in-memory state
    if config.aof_enabled {
        let aof_path = &config.aof_path;
        if aof_path.exists()
            && let Some(ref mut pool) = initial_pool
        {
            match aof_recovery::replay(aof_path, &shared_table, pool, &kachedb_net::DEFAULT_VECTORS)
            {
                Ok(count) => println!(
                    "📦 Replayed {count} AOF frames successfully from {:?}",
                    config.aof_path
                ),
                Err(e) => eprintln!("⚠️ AOF recovery error: {e}"),
            }
        }
    }

    // Launch background AOF writer thread
    let aof_writer = if config.aof_enabled {
        match aof::AofWriter::start(&config.aof_path, config.appendfsync) {
            Ok(writer) => {
                kachedb_net::set_aof_channel(writer.channel());
                Some(writer)
            }
            Err(e) => {
                eprintln!("⚠️ Failed to initialize AOF writer: {e}");
                None
            }
        }
    } else {
        None
    };

    // Launch background snapshot worker thread
    let snapshot_worker = if config.snapshot_enabled {
        Some(snapshot::SnapshotWorker::start(
            config.snapshot_path.clone(),
            shared_table.clone(),
            &kachedb_net::DEFAULT_VECTORS,
            config.snapshot_interval_secs,
            shutdown.clone(),
            Some(enc_config.clone()),
        ))
    } else {
        None
    };

    // Create per-worker crossbeam dispatch channels
    let (senders, receivers) = create_dispatch_channels(config.num_workers);

    // Shutdown barrier keeps worker SlabPools alive during final snapshot persistence
    let shutdown_barrier = Arc::new(kachedb_net::ShutdownBarrier::new());
    let mut worker_handles = Vec::with_capacity(config.num_workers);

    // Spawn worker threads (accept-dispatch mode)
    for (core_id, receiver) in receivers.into_iter().enumerate() {
        let shutdown_worker = shutdown.clone();
        let barrier = shutdown_barrier.clone();
        let shm_enabled = config.shm_enabled;
        let worker_table = shared_table.clone();
        let pool_bytes = config.pool_mb_per_core * 1024 * 1024;
        let worker_pool = if core_id == 0 {
            initial_pool.take()
        } else {
            None
        };
        let worker_tls = tls_config.clone();

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

                let mut worker = if let Some(pool) = worker_pool {
                    kachedb_net::WorkerThread::with_channel_and_pool(
                        core_id as u16,
                        pool,
                        worker_table,
                        receiver,
                    )
                    .with_tls(worker_tls)
                } else {
                    match kachedb_net::WorkerThread::with_channel(
                        core_id as u16,
                        pool_bytes,
                        worker_table,
                        receiver,
                    ) {
                        Ok(w) => w.with_tls(worker_tls),
                        Err(e) => {
                            log::error!("Worker [{core_id}]: failed to initialize: {e}");
                            return;
                        }
                    }
                };
                if let Err(e) = worker.run_with_barrier(shutdown_worker, Some(barrier)) {
                    log::error!("Worker [{core_id}]: event loop terminated with error: {e}");
                }
            })
            .expect("failed to spawn worker thread");

        worker_handles.push(handle);
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

    println!("🚀 KacheDB is ready to accept Redis & LLM tensor connections!");

    // Shutdown orchestration:
    // 1. Wait for accept-dispatcher to exit (no new incoming connections)
    let _ = accept_handle.join();
    log::info!("AcceptDispatcher: shut down, no new connections accepted");

    // 2. Stop the background snapshot worker thread so no async save collides
    if let Some(mut sw) = snapshot_worker {
        sw.stop();
    }

    // 3. Wait for all worker threads to stop processing client traffic
    // (Workers pause at shutdown_barrier, keeping their SlabPools and arenas fully alive)
    if !shutdown_barrier.wait_all_stopped(config.num_workers, Duration::from_secs(5)) {
        log::warn!("Timed out waiting for all workers to pause event loop");
    } else {
        log::info!(
            "All {} workers paused event loop, memory pools intact",
            config.num_workers
        );
    }

    // 4. Perform final shutdown snapshot while all memory pools and slab slots are guaranteed intact!
    if config.snapshot_enabled {
        log::info!("Snapshot worker: performing final shutdown snapshot...");
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as u32;
        match snapshot::save_snapshot(
            &config.snapshot_path,
            &shared_table,
            &kachedb_net::DEFAULT_VECTORS,
            now_sec,
            Some(&enc_config),
        ) {
            Ok(stats) => {
                log::info!(
                    "💾 Final shutdown snapshot saved successfully: {} vectors across {} indexes, {} KV keys ({} bytes) written to {:?}",
                    stats.total_vectors,
                    stats.vector_indexes,
                    stats.total_kv_entries,
                    stats.bytes_written,
                    config.snapshot_path
                );
            }
            Err(e) => {
                log::error!("Final shutdown snapshot failed: {e}");
            }
        }
    }

    // 5. Release workers from barrier so they can cleanly drop their SlabPools
    shutdown_barrier.release();

    // 6. Join all worker thread handles
    for handle in worker_handles {
        let _ = handle.join();
    }

    // 7. Flush and stop AOF writer if enabled
    if let Some(mut aof) = aof_writer {
        aof.stop();
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
