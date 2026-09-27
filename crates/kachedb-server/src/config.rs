//! `kachedb-server` — Server configuration and runtime options.

use std::net::SocketAddr;
use std::path::PathBuf;

use crate::aof::AppendFsync;

/// Server configuration options.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Optional path to loaded configuration file.
    pub config_path: Option<PathBuf>,
    /// Listening TCP socket address.
    pub bind_addr: SocketAddr,
    /// Number of worker threads (default: available physical/logical CPU cores).
    pub num_workers: usize,
    /// Slab pool memory capacity per worker core in megabytes.
    pub pool_mb_per_core: usize,
    /// Maximum simultaneous client connections (default: 10,000).
    pub maxclients: usize,
    /// Enable POSIX shared memory (`/dev/shm`) IPC channels for LLM tensor streaming.
    pub shm_enabled: bool,
    /// Enable Append-Only File (AOF) persistence.
    pub aof_enabled: bool,
    /// Path to the `.kaof` log file.
    pub aof_path: PathBuf,
    /// AOF fsync policy (always, everysec, no).
    pub appendfsync: AppendFsync,
    /// Path to TLS server certificate PEM file.
    pub tls_cert_path: Option<PathBuf>,
    /// Path to TLS private key PEM file.
    pub tls_key_path: Option<PathBuf>,
    /// Optional path to TLS CA certificate PEM file for client mTLS.
    pub tls_ca_path: Option<PathBuf>,
    /// Optional password required to authenticate via AUTH command.
    pub requirepass: Option<String>,
    /// Enable periodic background snapshotting (dump.kdb).
    pub snapshot_enabled: bool,
    /// Path to the binary snapshot file.
    pub snapshot_path: PathBuf,
    /// Snapshot interval in seconds (default: 300).
    pub snapshot_interval_secs: u64,
    /// Enable snapshot encryption-at-rest.
    pub snapshot_encryption_enabled: bool,
    /// Cipher suite for snapshot encryption (aes-256-gcm or chacha20-poly1305).
    pub snapshot_encryption_cipher: crate::crypto::CipherSuite,
    /// Plaintext passphrase or 64-char hex key for snapshot encryption.
    pub snapshot_encryption_key: Option<String>,
    /// Path to a 32-byte raw binary key or key file for snapshot encryption.
    pub snapshot_encryption_key_path: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        let default_cores = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        Self {
            config_path: None,
            bind_addr: "127.0.0.1:6379".parse().unwrap(),
            num_workers: default_cores,
            pool_mb_per_core: 64,
            maxclients: 10_000,
            shm_enabled: true,
            aof_enabled: false,
            aof_path: PathBuf::from("kachedb.aof"),
            appendfsync: AppendFsync::EverySec,
            tls_cert_path: None,
            tls_key_path: None,
            tls_ca_path: None,
            requirepass: None,
            snapshot_enabled: true,
            snapshot_path: PathBuf::from("dump.kdb"),
            snapshot_interval_secs: 300,
            snapshot_encryption_enabled: false,
            snapshot_encryption_cipher: crate::crypto::CipherSuite::Aes256Gcm,
            snapshot_encryption_key: None,
            snapshot_encryption_key_path: None,
        }
    }
}

impl ServerConfig {
    /// Returns the active `SnapshotEncryptionConfig` from these server settings.
    pub fn snapshot_encryption_config(&self) -> crate::snapshot::SnapshotEncryptionConfig {
        crate::snapshot::SnapshotEncryptionConfig {
            enabled: self.snapshot_encryption_enabled,
            cipher: self.snapshot_encryption_cipher,
            key: self.snapshot_encryption_key.clone(),
            key_file: self.snapshot_encryption_key_path.clone(),
        }
    }

    /// Loads directives from a configuration file into this `ServerConfig`.
    pub fn load_file(&mut self, path: &std::path::Path) -> Result<(), String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file '{:?}': {e}", path))?;

        self.config_path = Some(path.to_path_buf());
        self.load_str(&content)
    }

    /// Parses configuration directives from a string.
    pub fn load_str(&mut self, content: &str) -> Result<(), String> {
        for (line_num, line) in content.lines().enumerate() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
                continue;
            }

            // Split into directive and value (by '=' or whitespace)
            let parts: Vec<&str> = if let Some((k, v)) = trimmed.split_once('=') {
                vec![k.trim(), v.trim()]
            } else {
                trimmed.split_whitespace().collect()
            };

            if parts.is_empty() {
                continue;
            }

            let key = parts[0].to_lowercase();
            let value = if parts.len() > 1 {
                // If value is quoted, strip quotes
                let raw_val = parts[1..].join(" ");
                let val_trimmed = raw_val.trim();
                if (val_trimmed.starts_with('"') && val_trimmed.ends_with('"'))
                    || (val_trimmed.starts_with('\'') && val_trimmed.ends_with('\''))
                {
                    val_trimmed[1..val_trimmed.len() - 1].to_string()
                } else {
                    val_trimmed.to_string()
                }
            } else {
                String::new()
            };

            match key.as_str() {
                "bind" => {
                    if let Ok(ip) = value.parse::<std::net::IpAddr>() {
                        self.bind_addr.set_ip(ip);
                    } else if let Ok(addr) = value.parse::<SocketAddr>() {
                        self.bind_addr = addr;
                    } else {
                        return Err(format!(
                            "Invalid bind address '{value}' on line {}",
                            line_num + 1
                        ));
                    }
                }
                "port" => {
                    if let Ok(port) = value.parse::<u16>() {
                        self.bind_addr.set_port(port);
                    } else {
                        return Err(format!("Invalid port '{value}' on line {}", line_num + 1));
                    }
                }
                "workers" => {
                    if let Ok(w) = value.parse::<usize>() {
                        self.num_workers = if w == 0 {
                            std::thread::available_parallelism()
                                .map(|p| p.get())
                                .unwrap_or(1)
                        } else {
                            w
                        };
                    } else {
                        return Err(format!(
                            "Invalid workers '{value}' on line {}",
                            line_num + 1
                        ));
                    }
                }
                "pool-mb" | "pool_mb" | "pool_mb_per_core" => {
                    if let Ok(mb) = value.parse::<usize>() {
                        self.pool_mb_per_core = mb;
                    } else {
                        return Err(format!(
                            "Invalid pool-mb '{value}' on line {}",
                            line_num + 1
                        ));
                    }
                }
                "maxclients" => {
                    if let Ok(mc) = value.parse::<usize>() {
                        self.maxclients = mc;
                    } else {
                        return Err(format!(
                            "Invalid maxclients '{value}' on line {}",
                            line_num + 1
                        ));
                    }
                }
                "shm" => {
                    self.shm_enabled =
                        matches!(value.to_lowercase().as_str(), "yes" | "true" | "1");
                }
                "appendonly" | "aof" => {
                    self.aof_enabled =
                        matches!(value.to_lowercase().as_str(), "yes" | "true" | "1");
                }
                "appendfilename" | "aof-file" | "aof_file" => {
                    self.aof_path = PathBuf::from(&value);
                }
                "appendfsync" => {
                    if let Some(p) = AppendFsync::parse(&value) {
                        self.appendfsync = p;
                    } else {
                        return Err(format!(
                            "Invalid appendfsync '{value}' on line {}",
                            line_num + 1
                        ));
                    }
                }
                "tls-cert" | "tls_cert" => {
                    self.tls_cert_path = Some(PathBuf::from(&value));
                }
                "tls-key" | "tls_key" => {
                    self.tls_key_path = Some(PathBuf::from(&value));
                }
                "tls-ca" | "tls_ca" => {
                    self.tls_ca_path = Some(PathBuf::from(&value));
                }
                "requirepass" => {
                    if !value.is_empty() {
                        self.requirepass = Some(value);
                    }
                }
                "snapshot" | "snapshot-enabled" | "snapshot_enabled" => {
                    self.snapshot_enabled =
                        matches!(value.to_lowercase().as_str(), "yes" | "true" | "1");
                }
                "dbfilename" | "snapshot-file" | "snapshot_file" | "snapshot-path" => {
                    self.snapshot_path = PathBuf::from(&value);
                }
                "snapshot-interval" | "snapshot_interval" | "save-interval" => {
                    if let Ok(interval) = value.parse::<u64>() {
                        self.snapshot_interval_secs = interval;
                    } else {
                        return Err(format!(
                            "Invalid snapshot-interval '{value}' on line {}",
                            line_num + 1
                        ));
                    }
                }
                "snapshot-encryption" | "snapshot_encryption" => {
                    self.snapshot_encryption_enabled =
                        matches!(value.to_lowercase().as_str(), "yes" | "true" | "1");
                }
                "snapshot-encryption-cipher" | "snapshot_encryption_cipher" | "snapshot-cipher" => {
                    if let Ok(c) = crate::crypto::CipherSuite::from_str_name(&value) {
                        self.snapshot_encryption_cipher = c;
                    } else {
                        return Err(format!(
                            "Invalid snapshot cipher '{value}' on line {}",
                            line_num + 1
                        ));
                    }
                }
                "snapshot-encryption-key" | "snapshot_encryption_key" | "snapshot-key" => {
                    if !value.is_empty() {
                        self.snapshot_encryption_key = Some(value);
                    }
                }
                "snapshot-encryption-key-file"
                | "snapshot_encryption_key_file"
                | "snapshot-key-file"
                | "snapshot-key-path" => {
                    if !value.is_empty() {
                        self.snapshot_encryption_key_path = Some(PathBuf::from(&value));
                    }
                }
                other => {
                    log::warn!(
                        "Unknown config directive '{other}' on line {}",
                        line_num + 1
                    );
                }
            }
        }

        Ok(())
    }

    /// Parses configuration from command line arguments, loading any specified config file first.
    pub fn parse_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let mut config = Self::default();

        // Pass 1: check for config file flag (-c / --config)
        let mut i = 1;
        while i < args.len() {
            if (args[i] == "--config" || args[i] == "-c") && i + 1 < args.len() {
                let path = PathBuf::from(&args[i + 1]);
                if let Err(e) = config.load_file(&path) {
                    eprintln!("⚠️ Configuration file error: {e}");
                    std::process::exit(1);
                }
                break;
            }
            i += 1;
        }

        // Pass 2: parse CLI flags (which take precedence over config file values)
        let mut i = 1;
        while i < args.len() {
            match args[i].as_str() {
                "--config" | "-c" if i + 1 < args.len() => {
                    i += 2;
                }
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
                    if let Ok(w) = args[i + 1].parse::<usize>() {
                        config.num_workers = if w == 0 {
                            std::thread::available_parallelism()
                                .map(|p| p.get())
                                .unwrap_or(1)
                        } else {
                            w
                        };
                    }
                    i += 2;
                }
                "--pool-mb" if i + 1 < args.len() => {
                    if let Ok(mb) = args[i + 1].parse() {
                        config.pool_mb_per_core = mb;
                    }
                    i += 2;
                }
                "--maxclients" if i + 1 < args.len() => {
                    if let Ok(mc) = args[i + 1].parse() {
                        config.maxclients = mc;
                    }
                    i += 2;
                }
                "--no-shm" => {
                    config.shm_enabled = false;
                    i += 1;
                }
                "--aof" => {
                    config.aof_enabled = true;
                    i += 1;
                }
                "--aof-file" if i + 1 < args.len() => {
                    config.aof_path = PathBuf::from(&args[i + 1]);
                    i += 2;
                }
                "--appendfsync" if i + 1 < args.len() => {
                    if let Some(p) = AppendFsync::parse(&args[i + 1]) {
                        config.appendfsync = p;
                    }
                    i += 2;
                }
                "--tls-cert" if i + 1 < args.len() => {
                    config.tls_cert_path = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                }
                "--tls-key" if i + 1 < args.len() => {
                    config.tls_key_path = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                }
                "--tls-ca" if i + 1 < args.len() => {
                    config.tls_ca_path = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
                }
                "--requirepass" if i + 1 < args.len() => {
                    config.requirepass = Some(args[i + 1].clone());
                    i += 2;
                }
                "--no-snapshot" => {
                    config.snapshot_enabled = false;
                    i += 1;
                }
                "--snapshot" => {
                    config.snapshot_enabled = true;
                    i += 1;
                }
                "--dbfilename" | "--snapshot-file" if i + 1 < args.len() => {
                    config.snapshot_path = PathBuf::from(&args[i + 1]);
                    i += 2;
                }
                "--snapshot-interval" if i + 1 < args.len() => {
                    if let Ok(sec) = args[i + 1].parse() {
                        config.snapshot_interval_secs = sec;
                    }
                    i += 2;
                }
                "--snapshot-encryption" => {
                    config.snapshot_encryption_enabled = true;
                    i += 1;
                }
                "--no-snapshot-encryption" => {
                    config.snapshot_encryption_enabled = false;
                    i += 1;
                }
                "--snapshot-cipher" if i + 1 < args.len() => {
                    if let Ok(c) = crate::crypto::CipherSuite::from_str_name(&args[i + 1]) {
                        config.snapshot_encryption_cipher = c;
                    }
                    i += 2;
                }
                "--snapshot-key" if i + 1 < args.len() => {
                    config.snapshot_encryption_key = Some(args[i + 1].clone());
                    i += 2;
                }
                "--snapshot-key-file" if i + 1 < args.len() => {
                    config.snapshot_encryption_key_path = Some(PathBuf::from(&args[i + 1]));
                    i += 2;
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
     -c, --config <PATH>           Path to kachedb.conf configuration file
     -b, --bind <ADDR>             Bind address (default: 127.0.0.1:6379)
     -p, --port <PORT>             TCP listening port (default: 6379)
     -w, --workers <N>             Number of worker threads (default: CPU core count)
         --pool-mb <MB>            Slab memory pool capacity per core in MB (default: 4)
         --maxclients <N>          Maximum simultaneous client connections (default: 10000)
         --no-shm                  Disable POSIX shared memory (/dev/shm) IPC
         --aof                     Enable Append-Only File (AOF) persistence
         --aof-file <PATH>         Path to AOF log file (default: kachedb.aof)
         --appendfsync <POLICY>    AOF fsync policy: always, everysec, no (default: everysec)
         --snapshot-encryption     Enable snapshot encryption-at-rest
         --no-snapshot-encryption  Disable snapshot encryption-at-rest
         --snapshot-cipher <CIPH>  Cipher suite: aes-256-gcm or chacha20-poly1305
         --snapshot-key <KEY>      Passphrase or 64-char hex key for snapshot encryption
         --snapshot-key-file <PTH> Path to 32-byte key file for snapshot encryption
         --tls-cert <PATH>         Path to TLS certificate PEM file
         --tls-key <PATH>          Path to TLS private key PEM file
         --tls-ca <PATH>           Path to TLS CA PEM file (enables mTLS)
         --requirepass <PASS>      Require authentication password via AUTH
     -h, --help                    Print this help information
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.bind_addr.port(), 6379);
        assert_eq!(cfg.maxclients, 10_000);
        assert_eq!(cfg.pool_mb_per_core, 64);
        assert!(cfg.shm_enabled);
        assert!(!cfg.aof_enabled);
    }

    #[test]
    fn test_load_file_parsing() {
        let sample_conf = r#"
# KacheDB Sample Config
port 6380
bind 0.0.0.0
workers 2
pool-mb 80
maxclients 5000
shm no
appendonly yes
appendfilename custom.aof
appendfsync always
requirepass "supersecret"
"#;

        let mut cfg = ServerConfig::default();
        cfg.load_str(sample_conf).unwrap();

        assert_eq!(cfg.bind_addr.port(), 6380);
        assert_eq!(cfg.bind_addr.ip().to_string(), "0.0.0.0");
        assert_eq!(cfg.num_workers, 2);
        assert_eq!(cfg.pool_mb_per_core, 80);
        assert_eq!(cfg.maxclients, 5000);
        assert!(!cfg.shm_enabled);
        assert!(cfg.aof_enabled);
        assert_eq!(cfg.aof_path, PathBuf::from("custom.aof"));
        assert_eq!(cfg.appendfsync, AppendFsync::Always);
        assert_eq!(cfg.requirepass.as_deref(), Some("supersecret"));

        // Also test file I/O
        let temp_path = std::env::temp_dir().join("kachedb_test_config.conf");
        std::fs::write(&temp_path, sample_conf).unwrap();
        let mut file_cfg = ServerConfig::default();
        file_cfg.load_file(&temp_path).unwrap();
        let _ = std::fs::remove_file(&temp_path);
        assert_eq!(file_cfg.bind_addr.port(), 6380);
        assert_eq!(file_cfg.maxclients, 5000);
    }

    #[test]
    fn test_canonical_kachedb_conf() {
        let root_conf = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../kachedb.conf");
        if root_conf.exists() {
            let mut cfg = ServerConfig::default();
            cfg.load_file(&root_conf)
                .expect("canonical kachedb.conf must parse successfully");
            assert_eq!(cfg.bind_addr.port(), 6379);
            assert_eq!(cfg.maxclients, 10000);
            assert!(cfg.shm_enabled);
            assert!(!cfg.aof_enabled);
        }
    }

    #[test]
    fn test_snapshot_encryption_config_parsing() {
        let conf = r#"
snapshot-encryption yes
snapshot-encryption-cipher chacha20-poly1305
snapshot-encryption-key "super_secret_test_pass"
snapshot-encryption-key-file /tmp/kachedb.key
"#;
        let mut cfg = ServerConfig::default();
        cfg.load_str(conf).unwrap();
        assert!(cfg.snapshot_encryption_enabled);
        assert_eq!(
            cfg.snapshot_encryption_cipher,
            crate::crypto::CipherSuite::ChaCha20Poly1305
        );
        assert_eq!(
            cfg.snapshot_encryption_key.as_deref(),
            Some("super_secret_test_pass")
        );
        assert_eq!(
            cfg.snapshot_encryption_key_path.as_deref(),
            Some(std::path::Path::new("/tmp/kachedb.key"))
        );

        let enc_cfg = cfg.snapshot_encryption_config();
        assert!(enc_cfg.enabled);
        assert_eq!(enc_cfg.cipher, crate::crypto::CipherSuite::ChaCha20Poly1305);
    }
}
