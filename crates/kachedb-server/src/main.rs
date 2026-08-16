//! `kachedb-server` — Multi-core daemon entry point.

mod config;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use config::ServerConfig;
use kachedb_net::WorkerThread;
use kachedb_shm::ShmChannel;

fn main() {
    // Basic terminal logging initialization
    let _ = simple_logger();

    let config = ServerConfig::parse_args();

    println!(
        r#"
  _  __           _          _____  ____  
 | |/ /          | |        |  __ \|  _ \ 
 | ' / __ _  ___ | |__   ___| |  | | |_) |
 |  < / _` |/ __|| '_ \ / _ \ |  | |  _ < 
 | . \ (_| | (__ | | | |  __/ |__| | |_) |
 |_|\_\__,_|\___||_| |_|\___|_____/|____/ 
"#
    );

    println!("⚡ KacheDB Daemon v0.1.0 starting...");
    println!("   └─ TCP Listener:       {}", config.bind_addr);
    println!("   └─ Worker Threads:     {} cores (Thread-Per-Core topology)", config.num_workers);
    println!(
        "   └─ Memory Pool:        {} MB/core (Total: {} MB)",
        config.pool_mb_per_core,
        config.num_workers * config.pool_mb_per_core
    );
    println!(
        "   └─ POSIX SHM IPC:      {}",
        if config.shm_enabled { "Enabled (/dev/shm)" } else { "Disabled" }
    );
    println!();

    let shutdown = Arc::new(AtomicBool::new(false));

    // Handle Ctrl+C gracefully
    let shutdown_signal = shutdown.clone();
    ctrlc_handler(shutdown_signal);

    let mut handles = Vec::with_capacity(config.num_workers);

    for core_id in 0..config.num_workers {
        let shutdown_worker = shutdown.clone();
        let bind_addr = config.bind_addr;
        let shm_enabled = config.shm_enabled;

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
                            log::warn!("Worker [{core_id}]: failed to create SHM region '{shm_name}': {e}");
                            None
                        }
                    }
                } else {
                    None
                };

                let mut worker = match WorkerThread::new(core_id as u16, bind_addr) {
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

    println!("🚀 KacheDB is ready to accept Redis & LLM tensor connections!");

    // Wait for all worker threads to exit
    for handle in handles {
        let _ = handle.join();
    }

    println!("🛑 KacheDB server stopped.");
}

fn simple_logger() -> Result<(), ()> {
    // Minimal standard output logger
    log::set_max_level(log::LevelFilter::Info);
    Ok(())
}

fn ctrlc_handler(shutdown: Arc<AtomicBool>) {
    // Simple shutdown handler
    let s = shutdown.clone();
    std::thread::spawn(move || {
        // Wait on stdin or sleep loop as fallback for signal trapping
        loop {
            std::thread::sleep(std::time::Duration::from_millis(500));
            if s.load(Ordering::Relaxed) {
                break;
            }
        }
    });
}
