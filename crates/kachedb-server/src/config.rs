//! `kachedb-server` — Server configuration and runtime options.

use std::net::SocketAddr;

/// Server configuration options.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Listening TCP socket address.
    pub bind_addr: SocketAddr,
    /// Number of worker threads (default: available physical/logical CPU cores).
    pub num_workers: usize,
    /// Slab pool memory capacity per worker core in megabytes.
    pub pool_mb_per_core: usize,
    /// Enable POSIX shared memory (`/dev/shm`) IPC channels for LLM tensor streaming.
    pub shm_enabled: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let default_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        Self {
            bind_addr: "127.0.0.1:6379".parse().unwrap(),
            num_workers: default_cores,
            pool_mb_per_core: 64,
            shm_enabled: true,
        }
    }
}

impl ServerConfig {
    /// Parses configuration from command line arguments.
    pub fn parse_args() -> Self {
        let mut config = Self::default();
        let args: Vec<String> = std::env::args().collect();

        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--bind" | "-b" if i + 1 < args.len() => {
                    if let Ok(addr) = args[i + 1].parse() {
                        config.bind_addr = addr;
                    }
                    i += 2;
                }
                "--port" | "-p" if i + 1 < args.len() => {
                    if let Ok(port) = args[i + 1].parse::<u16>() {
                        config.bind_addr.set_port(port);
                    }
                    i += 2;
                }
                "--workers" | "-w" if i + 1 < args.len() => {
                    if let Ok(w) = args[i + 1].parse() {
                        config.num_workers = w;
                    }
                    i += 2;
                }
                "--pool-mb" if i + 1 < args.len() => {
                    if let Ok(mb) = args[i + 1].parse() {
                        config.pool_mb_per_core = mb;
                    }
                    i += 2;
                }
                "--no-shm" => {
                    config.shm_enabled = false;
                    i += 1;
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => {
                    i += 1;
                }
            }
        }

        config
    }
}

fn print_help() {
    println!(
        r#"
  _  __           _          _____  ____  
 | |/ /          | |        |  __ \|  _ \ 
 | ' / __ _  ___ | |__   ___| |  | | |_) |
 |  < / _` |/ __|| '_ \ / _ \ |  | |  _ < 
 | . \ (_| | (__ | | | |  __/ |__| | |_) |
 |_|\_\__,_|\___||_| |_|\___|_____/|____/ 

 KacheDB: The Zero-Copy Redis-Compatible & LLM KV-Cache Storage Engine

 USAGE:
     kachedb-server [OPTIONS]

 OPTIONS:
     -b, --bind <ADDR>       Bind address (default: 127.0.0.1:6379)
     -p, --port <PORT>       TCP listening port (default: 6379)
     -w, --workers <N>       Number of worker threads (default: CPU core count)
         --pool-mb <MB>      Slab memory pool capacity per core in MB (default: 64)
         --no-shm            Disable POSIX shared memory (/dev/shm) IPC
     -h, --help              Print this help information
"#
    );
}
