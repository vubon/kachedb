//! `kachedb-server` — Multi-core daemon entry point.

mod aof;
mod aof_recovery;
mod aof_rewrite;
mod config;
pub mod crypto;
mod recovery_pool;
mod snapshot;
mod storage;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use config::ServerConfig;
use kachedb_net::{AcceptDispatcher, create_dispatch_channels};
use kachedb_shm::ShmChannel;

fn main() {
    // Process-wide fail-stop: unexpected worker thread panics abort immediately to prevent dangling slab memory
    let orig_panic_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        eprintln!(
            "FATAL: KacheDB thread panic detected: {panic_info}. Aborting process immediately to prevent slab corruption."
        );
        orig_panic_hook(panic_info);
        std::process::abort();
    }));

    // Basic terminal logging initialization
    let _ = simple_logger();

    let config = ServerConfig::parse_args();
    if config.aof_repair {
        match aof_recovery::repair(&config.aof_path) {
            Ok(Some(backup)) => println!(
                "Repaired {:?}. Original saved to {:?}",
                config.aof_path, backup
            ),
            Ok(None) => println!("AOF {:?} has no framing damage", config.aof_path),
            Err(error) => {
                eprintln!("AOF repair failed for {:?}: {error}", config.aof_path);
                std::process::exit(1);
            }
        }
        return;
    }

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

    let (recovery_pools, aof_writer) = match storage::start(
        &config,
        &shared_table,
        &kachedb_net::DEFAULT_VECTORS,
        &enc_config,
    ) {
        Ok(storage) => storage,
        Err(error) => {
            eprintln!(
                "Persistence startup failed (snapshot {:?}, AOF {:?}): {error}",
                config.snapshot_path, config.aof_path,
            );
            std::process::exit(1);
        }
    };

    // Capture restored deadlines before any worker can change the table.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let mut recovered_wheel =
        kachedb_core::HashedTimingWheel::new(u32::try_from(now).unwrap_or(u32::MAX));
    shared_table.schedule_restored_expirations(&mut recovered_wheel);
    let mut recovered_wheel = Some(recovered_wheel);

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
    for (core_id, (receiver, pool)) in receivers.into_iter().zip(recovery_pools.0).enumerate() {
        let shutdown_worker = shutdown.clone();
        let barrier = shutdown_barrier.clone();
        let shm_enabled = config.shm_enabled;
        let worker_table = shared_table.clone();
        let worker_tls = tls_config.clone();
        let worker_wheel = if core_id == 0 {
            recovered_wheel.take()
        } else {
            None
        };

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

                let mut worker = kachedb_net::WorkerThread::with_channel_and_pool(
                    core_id as u16,
                    pool,
                    worker_table,
                    receiver,
                )
                .with_tls(worker_tls);
                if let Some(wheel) = worker_wheel {
                    worker.timing_wheel = wheel;
                }
                if let Err(e) = worker.run_with_barrier(shutdown_worker.clone(), Some(barrier)) {
                    log::error!("Worker [{core_id}]: event loop terminated with error: {e}");
                    eprintln!("FATAL: Worker [{core_id}] event loop fatal error: {e}. Aborting process.");
                    std::process::abort();
                }

                if !shutdown_worker.load(Ordering::Relaxed) {
                    eprintln!("FATAL: Worker [{core_id}] exited prematurely without shutdown signal. Aborting process.");
                    std::process::abort();
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
            let dispatcher =
                AcceptDispatcher::new(config.bind_addr, senders, accept_shutdown.clone());
            if let Err(e) = dispatcher.run() {
                log::error!("AcceptDispatcher: terminated with error: {e}");
                if !accept_shutdown.load(Ordering::Relaxed) {
                    eprintln!(
                        "FATAL: AcceptDispatcher terminated unexpectedly: {e}. Aborting process."
                    );
                    std::process::abort();
                }
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
    let workers_paused =
        shutdown_barrier.wait_all_stopped(config.num_workers, Duration::from_secs(5));
    if !workers_paused {
        log::error!("Skipping the final snapshot because workers did not pause before the timeout");
    } else {
        log::info!(
            "All {} workers paused event loop, memory pools intact",
            config.num_workers
        );
    }

    // 4. Perform final shutdown snapshot while all memory pools and slab slots are guaranteed intact!
    if config.snapshot_enabled && workers_paused {
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
    let aof_result = aof_writer.map(|mut aof| aof.stop()).transpose();

    if config.shm_enabled {
        cleanup_stale_shm(config.num_workers);
    }

    if let Err(error) = aof_result {
        eprintln!("AOF shutdown failed: {error}");
        std::process::exit(1);
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
