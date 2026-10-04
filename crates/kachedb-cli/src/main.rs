//! `kachedb-cli` — Interactive CLI client, live benchmarking, and diagnostic utility for KacheDB.

mod bench;
mod repl;
mod snapshot;

use bench::run_benchmark;
use repl::run_repl;
use snapshot::inspect_snapshot;

#[derive(Debug, PartialEq, Eq)]
pub enum Subcommand {
    Repl {
        host: String,
        port: u16,
    },
    Bench {
        host: String,
        port: u16,
        requests: usize,
    },
    SnapshotInfo {
        path: String,
    },
}

pub fn parse_args(args: &[String]) -> Result<Subcommand, String> {
    if args.len() <= 1 {
        return Ok(Subcommand::Repl {
            host: "127.0.0.1".to_string(),
            port: 6379,
        });
    }

    // Direct subcommand shortcut: `kachedb-cli snapshot-info <file>`
    if args[1] == "snapshot-info" || args[1] == "--snapshot-info" {
        if args.len() > 2 {
            return Ok(Subcommand::SnapshotInfo {
                path: args[2].clone(),
            });
        } else {
            return Err("Usage: kachedb-cli snapshot-info <snapshot_file.kdb>".to_string());
        }
    }

    let mut host = "127.0.0.1".to_string();
    let mut port = 6379u16;
    let mut bench_mode = false;
    let mut bench_requests = 10_000usize;
    let mut snapshot_path: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "snapshot-info" | "--snapshot-info" => {
                if i + 1 < args.len() {
                    snapshot_path = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    return Err("Usage: kachedb-cli snapshot-info <snapshot_file.kdb>".to_string());
                }
            }
            "-h" | "--host" => {
                if i + 1 < args.len() {
                    host = args[i + 1].clone();
                    i += 2;
                } else {
                    return Err("Missing argument for -h/--host".to_string());
                }
            }
            "-p" | "--port" => {
                if i + 1 < args.len() {
                    port = args[i + 1]
                        .parse()
                        .map_err(|_| format!("Invalid port number: {}", args[i + 1]))?;
                    i += 2;
                } else {
                    return Err("Missing argument for -p/--port".to_string());
                }
            }
            "-b" | "--bench" => {
                bench_mode = true;
                i += 1;
            }
            "-n" => {
                if i + 1 < args.len() {
                    bench_requests = args[i + 1]
                        .parse()
                        .map_err(|_| format!("Invalid request count: {}", args[i + 1]))?;
                    i += 2;
                } else {
                    return Err("Missing argument for -n".to_string());
                }
            }
            "--help" | "-help" => {
                print_help();
                std::process::exit(0);
            }
            unknown => {
                return Err(format!(
                    "Unknown argument: {}\nRun with --help for usage.",
                    unknown
                ));
            }
        }
    }

    if let Some(path) = snapshot_path {
        Ok(Subcommand::SnapshotInfo { path })
    } else if bench_mode {
        Ok(Subcommand::Bench {
            host,
            port,
            requests: bench_requests,
        })
    } else {
        Ok(Subcommand::Repl { host, port })
    }
}

pub fn print_help() {
    println!(
        r#"
  KacheDB CLI - Interactive Client, Live Benchmark & Diagnostic Tool

  USAGE:
      kachedb-cli [OPTIONS]
      kachedb-cli snapshot-info <FILE.kdb>

  COMMANDS:
      snapshot-info <FILE>   Inspect header, cipher suite, encryption status, and CRC32 of snapshot (v2, v3, v4)

  OPTIONS:
      -h, --host <HOST>      Server hostname (default: 127.0.0.1)
      -p, --port <PORT>      Server port (default: 6379)
      -b, --bench            Run live throughput benchmark
      -n <NUM>               Number of requests for benchmark (default: 10,000)
          --help             Print this help message
"#
    );
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    match parse_args(&args) {
        Ok(Subcommand::Repl { host, port }) => {
            let addr = format!("{}:{}", host, port);
            run_repl(&addr);
        }
        Ok(Subcommand::Bench {
            host,
            port,
            requests,
        }) => {
            let addr = format!("{}:{}", host, port);
            run_benchmark(&addr, requests);
        }
        Ok(Subcommand::SnapshotInfo { path }) => {
            inspect_snapshot(&path);
        }
        Err(err) => {
            eprintln!("❌ {}", err);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_args_defaults() {
        let args = vec!["kachedb-cli".to_string()];
        let cmd = parse_args(&args).expect("should parse defaults");
        assert_eq!(
            cmd,
            Subcommand::Repl {
                host: "127.0.0.1".to_string(),
                port: 6379,
            }
        );
    }

    #[test]
    fn test_parse_args_custom_host_port() {
        let args = vec![
            "kachedb-cli".to_string(),
            "-h".to_string(),
            "10.0.0.5".to_string(),
            "-p".to_string(),
            "6381".to_string(),
        ];
        let cmd = parse_args(&args).expect("should parse host/port");
        assert_eq!(
            cmd,
            Subcommand::Repl {
                host: "10.0.0.5".to_string(),
                port: 6381,
            }
        );
    }

    #[test]
    fn test_parse_args_bench() {
        let args = vec![
            "kachedb-cli".to_string(),
            "--bench".to_string(),
            "-n".to_string(),
            "5000".to_string(),
        ];
        let cmd = parse_args(&args).expect("should parse bench mode");
        assert_eq!(
            cmd,
            Subcommand::Bench {
                host: "127.0.0.1".to_string(),
                port: 6379,
                requests: 5000,
            }
        );
    }

    #[test]
    fn test_parse_args_snapshot_info_subcommand() {
        let args = vec![
            "kachedb-cli".to_string(),
            "snapshot-info".to_string(),
            "backup.kdb".to_string(),
        ];
        let cmd = parse_args(&args).expect("should parse snapshot-info");
        assert_eq!(
            cmd,
            Subcommand::SnapshotInfo {
                path: "backup.kdb".to_string(),
            }
        );
    }

    #[test]
    fn test_parse_args_invalid_port() {
        let args = vec![
            "kachedb-cli".to_string(),
            "-p".to_string(),
            "not_a_port".to_string(),
        ];
        assert!(parse_args(&args).is_err());
    }
}
