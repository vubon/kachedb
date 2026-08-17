//! `kachedb-bench` — Multi-connection pipelined load generator for KacheDB.
//!
//! Saturates the KacheDB server with concurrent pipelined TCP connections,
//! measuring real end-to-end throughput and P50/P99/P999 latency percentiles.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kachedb_proto_resp::{encode_array_header, encode_bulk_string};

// ── CLI Configuration ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct BenchConfig {
    host: String,
    port: u16,
    requests: usize,
    clients: usize,
    pipeline: usize,
    command: BenchCommand,
    key_size: usize,
    value_size: usize,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BenchCommand {
    Ping,
    Set,
    Get,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".into(),
            port: 6379,
            requests: 100_000,
            clients: 50,
            pipeline: 16,
            command: BenchCommand::Ping,
            key_size: 16,
            value_size: 64,
        }
    }
}

fn parse_args() -> BenchConfig {
    let args: Vec<String> = std::env::args().collect();
    let mut cfg = BenchConfig::default();
    let mut i = 1;

    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--host" if i + 1 < args.len() => { cfg.host = args[i + 1].clone(); i += 2; }
            "-p" | "--port" if i + 1 < args.len() => { cfg.port = args[i + 1].parse().unwrap_or(6379); i += 2; }
            "-n" | "--requests" if i + 1 < args.len() => { cfg.requests = args[i + 1].parse().unwrap_or(100_000); i += 2; }
            "-c" | "--clients" if i + 1 < args.len() => { cfg.clients = args[i + 1].parse().unwrap_or(50); i += 2; }
            "--pipeline" if i + 1 < args.len() => { cfg.pipeline = args[i + 1].parse().unwrap_or(16); i += 2; }
            "--key-size" if i + 1 < args.len() => { cfg.key_size = args[i + 1].parse().unwrap_or(16); i += 2; }
            "--value-size" if i + 1 < args.len() => { cfg.value_size = args[i + 1].parse().unwrap_or(64); i += 2; }
            "--command" if i + 1 < args.len() => {
                cfg.command = match args[i + 1].to_uppercase().as_str() {
                    "SET" => BenchCommand::Set,
                    "GET" => BenchCommand::Get,
                    _ => BenchCommand::Ping,
                };
                i += 2;
            }
            "--help" => { print_help(); std::process::exit(0); }
            _ => { i += 1; }
        }
    }
    cfg
}

fn print_help() {
    println!(r#"
  KacheDB Bench — Multi-Connection Pipelined Load Generator

  USAGE:
      kachedb-bench [OPTIONS]

  OPTIONS:
      -h, --host <HOST>         Server hostname (default: 127.0.0.1)
      -p, --port <PORT>         Server port (default: 6379)
      -n, --requests <N>        Total number of requests (default: 100,000)
      -c, --clients <N>         Number of concurrent TCP connections (default: 50)
          --pipeline <N>        In-flight requests per connection (default: 16)
          --command <CMD>       Command: PING | SET | GET (default: PING)
          --key-size <N>        Key length in bytes (default: 16)
          --value-size <N>      Value length in bytes for SET (default: 64)
          --help                Print this help message
"#);
}

// ── Latency Histogram ─────────────────────────────────────────────────────────

/// Fixed-resolution histogram with 1 µs buckets up to 100 ms.
struct Histogram {
    buckets: Vec<u64>,
    bucket_width_us: u64,
    overflow: u64,
    total: u64,
}

impl Histogram {
    fn new() -> Self {
        let bucket_width_us = 1;
        let num_buckets = 100_000; // 0–100ms in 1 µs steps
        Self {
            buckets: vec![0u64; num_buckets],
            bucket_width_us,
            overflow: 0,
            total: 0,
        }
    }

    fn record(&mut self, duration: Duration) {
        let us = duration.as_micros() as u64;
        let idx = (us / self.bucket_width_us) as usize;
        self.total += 1;
        if idx < self.buckets.len() {
            self.buckets[idx] += 1;
        } else {
            self.overflow += 1;
        }
    }

    fn percentile(&self, pct: f64) -> u64 {
        let target = ((self.total as f64) * pct / 100.0).ceil() as u64;
        let mut cumulative = 0u64;
        for (i, &count) in self.buckets.iter().enumerate() {
            cumulative += count;
            if cumulative >= target {
                return i as u64 * self.bucket_width_us;
            }
        }
        u64::MAX
    }
}

// ── RESP Frame Builder ─────────────────────────────────────────────────────────

fn build_ping_frame() -> Vec<u8> {
    let mut buf = Vec::with_capacity(32);
    encode_array_header(&mut buf, 1);
    encode_bulk_string(&mut buf, b"PING");
    buf
}

fn build_set_frame(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64 + key.len() + value.len());
    encode_array_header(&mut buf, 3);
    encode_bulk_string(&mut buf, b"SET");
    encode_bulk_string(&mut buf, key);
    encode_bulk_string(&mut buf, value);
    buf
}

fn build_get_frame(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(32 + key.len());
    encode_array_header(&mut buf, 2);
    encode_bulk_string(&mut buf, b"GET");
    encode_bulk_string(&mut buf, key);
    buf
}

// ── Worker Thread ─────────────────────────────────────────────────────────────

fn run_worker(
    client_id: usize,
    cfg: Arc<BenchConfig>,
    requests_per_client: usize,
    done: Arc<AtomicBool>,
    total_completed: Arc<AtomicU64>,
) -> Histogram {
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Client {client_id}: failed to connect to {addr}: {e}");
            done.store(true, Ordering::Relaxed);
            return Histogram::new();
        }
    };

    stream.set_nodelay(true).ok();
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();

    let key_base = format!("bench:key:{client_id}:");
    let value: Vec<u8> = vec![b'x'; cfg.value_size];
    let mut hist = Histogram::new();
    let mut read_buf = vec![0u8; 256 * 1024];
    let mut completed = 0usize;

    while completed < requests_per_client {
        let batch_size = cfg.pipeline.min(requests_per_client - completed);

        // Build and send pipeline batch
        let send_start = Instant::now();
        let mut send_buf = Vec::with_capacity(batch_size * 64);
        for j in 0..batch_size {
            let key = format!("{}{}", key_base, (completed + j) % 10_000);
            let frame = match cfg.command {
                BenchCommand::Ping => build_ping_frame(),
                BenchCommand::Set => build_set_frame(key.as_bytes(), &value),
                BenchCommand::Get => build_get_frame(key.as_bytes()),
            };
            send_buf.extend_from_slice(&frame);
        }

        if stream.write_all(&send_buf).is_err() {
            break;
        }

        // Drain responses for the batch
        let mut responses_received = 0;
        let mut carry = Vec::new();

        while responses_received < batch_size {
            let n = match stream.read(&mut read_buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };

            carry.extend_from_slice(&read_buf[..n]);
            // Count RESP responses by counting leading type markers
            let mut pos = 0;
            while pos < carry.len() && responses_received < batch_size {
                match carry[pos] {
                    b'+' | b'-' | b':' => {
                        if let Some(end) = find_crlf(&carry, pos) {
                            pos = end + 2;
                            responses_received += 1;
                        } else {
                            break;
                        }
                    }
                    b'$' => {
                        if let Some(end) = find_crlf(&carry, pos) {
                            let len_str = std::str::from_utf8(&carry[pos + 1..end]).unwrap_or("0");
                            let len: isize = len_str.parse().unwrap_or(-1);
                            let next = end + 2 + len.max(0) as usize + 2;
                            if next <= carry.len() {
                                pos = next;
                                responses_received += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    _ => { pos += 1; }
                }
            }
            carry.drain(..pos);
        }

        let elapsed = send_start.elapsed();
        // Amortise latency across the batch
        let per_req = elapsed / batch_size.max(1) as u32;
        for _ in 0..responses_received {
            hist.record(per_req);
        }

        completed += responses_received;
        total_completed.fetch_add(responses_received as u64, Ordering::Relaxed);
    }

    hist
}

fn find_crlf(buf: &[u8], from: usize) -> Option<usize> {
    buf[from..].windows(2).position(|w| w == b"\r\n").map(|p| from + p)
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let cfg = Arc::new(parse_args());
    let addr = format!("{}:{}", cfg.host, cfg.port);

    println!();
    println!("🔥 KacheDB Live Throughput Benchmark");
    println!("   └─ Target:             {}", addr);
    println!("   └─ Command:            {:?}", cfg.command);
    println!("   └─ Total Requests:     {:>12}", cfg.requests);
    println!("   └─ Concurrent Clients: {:>12}", cfg.clients);
    println!("   └─ Pipeline Depth:     {:>12}", cfg.pipeline);
    println!("   └─ Key Size:           {:>9} bytes", cfg.key_size);
    if cfg.command == BenchCommand::Set {
        println!("   └─ Value Size:         {:>9} bytes", cfg.value_size);
    }
    println!();

    // Verify connectivity
    match TcpStream::connect(&addr) {
        Ok(_) => println!("✅ Server reachable at {}", addr),
        Err(e) => {
            eprintln!("❌ Cannot connect to {}: {}", addr, e);
            std::process::exit(1);
        }
    }

    let requests_per_client = (cfg.requests + cfg.clients - 1) / cfg.clients;
    let done = Arc::new(AtomicBool::new(false));
    let total_completed = Arc::new(AtomicU64::new(0));

    let wall_start = Instant::now();

    // Spawn one thread per client connection
    let handles: Vec<_> = (0..cfg.clients)
        .map(|client_id| {
            let cfg = cfg.clone();
            let done = done.clone();
            let total_completed = total_completed.clone();

            std::thread::Builder::new()
                .name(format!("bench-client-{client_id}"))
                .spawn(move || {
                    run_worker(client_id, cfg, requests_per_client, done, total_completed)
                })
                .expect("failed to spawn client thread")
        })
        .collect();

    // Collect histograms from all threads
    let mut merged = Histogram::new();
    for handle in handles {
        if let Ok(h) = handle.join() {
            for (i, &count) in h.buckets.iter().enumerate() {
                merged.buckets[i] += count;
            }
            merged.overflow += h.overflow;
            merged.total += h.total;
        }
    }

    let wall_elapsed = wall_start.elapsed();
    let actual_completed = total_completed.load(Ordering::Relaxed);
    let qps = actual_completed as f64 / wall_elapsed.as_secs_f64();
    let p50 = merged.percentile(50.0);
    let p99 = merged.percentile(99.0);
    let p999 = merged.percentile(99.9);
    let avg_us = if merged.total > 0 {
        merged.buckets.iter().enumerate()
            .map(|(i, &c)| i as f64 * c as f64)
            .sum::<f64>() / merged.total as f64
    } else {
        0.0
    };

    println!();
    println!("📊 Benchmark Results");
    println!("   ─────────────────────────────────────────────────");
    println!("   └─ Total Requests Sent:  {:>12}", actual_completed);
    println!("   └─ Total Wall Time:      {:>12.3?}", wall_elapsed);
    println!("   └─ Throughput (QPS):     {:>12.0} req/sec", qps);
    println!("   └─ Avg Latency:          {:>12.2} µs / req", avg_us);
    println!("   └─ P50 Latency:          {:>12} µs", p50);
    println!("   └─ P99 Latency:          {:>12} µs", p99);
    println!("   └─ P999 Latency:         {:>12} µs", p999);
    println!("   ─────────────────────────────────────────────────");
    println!();
}
