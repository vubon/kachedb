//! `kachedb-cli` — Interactive CLI client and live benchmarking utility for KacheDB.

use std::io::{BufRead, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use kachedb_proto_resp::{
    encode_array_header, encode_bulk_string, parse_frame, Frame,
};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut host = "127.0.0.1".to_string();
    let mut port = 6379u16;
    let mut bench_mode = false;
    let mut bench_requests = 10_000usize;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-h" | "--host" if i + 1 < args.len() => {
                host = args[i + 1].clone();
                i += 2;
            }
            "-p" | "--port" if i + 1 < args.len() => {
                if let Ok(p) = args[i + 1].parse() {
                    port = p;
                }
                i += 2;
            }
            "--bench" | "-b" => {
                bench_mode = true;
                i += 1;
            }
            "-n" if i + 1 < args.len() => {
                if let Ok(n) = args[i + 1].parse() {
                    bench_requests = n;
                }
                i += 2;
            }
            "--help" => {
                print_help();
                return;
            }
            _ => {
                i += 1;
            }
        }
    }

    let addr = format!("{}:{}", host, port);

    if bench_mode {
        run_benchmark(&addr, bench_requests);
    } else {
        run_repl(&addr);
    }
}

fn print_help() {
    println!(
        r#"
  KacheDB CLI - Interactive Client & Live Benchmark Tool

  USAGE:
      kachedb-cli [OPTIONS]

  OPTIONS:
      -h, --host <HOST>      Server hostname (default: 127.0.0.1)
      -p, --port <PORT>      Server port (default: 6379)
      -b, --bench            Run live throughput benchmark
      -n <NUM>               Number of requests for benchmark (default: 10,000)
          --help             Print this help message
"#
    );
}

fn run_benchmark(addr: &str, requests: usize) {
    println!("🔥 Connecting to {} for live benchmark ({} requests)...", addr, requests);

    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Failed to connect to KacheDB at {}: {}", addr, e);
            std::process::exit(1);
        }
    };

    let start = Instant::now();
    let mut read_buf = vec![0u8; 4096];

    // Pipeline PING commands
    let mut req_buf = Vec::with_capacity(32);
    encode_array_header(&mut req_buf, 1);
    encode_bulk_string(&mut req_buf, b"PING");

    for _ in 0..requests {
        stream.write_all(&req_buf).unwrap();
        let n = stream.read(&mut read_buf).unwrap();
        if n == 0 {
            eprintln!("Connection closed by server.");
            break;
        }
    }

    let duration = start.elapsed();
    let qps = (requests as f64) / duration.as_secs_f64();
    let avg_lat_us = (duration.as_micros() as f64) / (requests as f64);

    println!("\n📊 Benchmark Results:");
    println!("   └─ Total Requests:   {}", requests);
    println!("   └─ Total Time:       {:.3?}", duration);
    println!("   └─ Throughput (QPS): {:.2} req/sec", qps);
    println!("   └─ Avg Ping Latency: {:.2} µs / req", avg_lat_us);
}

fn run_repl(addr: &str) {
    println!("Connecting to KacheDB at {}...", addr);
    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Failed to connect to KacheDB at {}: {}", addr, e);
            eprintln!("(Make sure `cargo run -p kachedb-server` is running)");
            return;
        }
    };

    println!("Connected to KacheDB. Type commands (e.g. PING, SET foo bar, GET foo, QUIT) or Ctrl+C to exit.\n");

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut read_buf = vec![0u8; 64 * 1024];

    loop {
        print!("{}> ", addr);
        stdout.flush().unwrap();

        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap() == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let parts: Vec<&str> = trimmed.split_whitespace().collect();

        // Encode as RESP Array
        let mut req_buf = Vec::new();
        encode_array_header(&mut req_buf, parts.len());
        for part in parts {
            encode_bulk_string(&mut req_buf, part.as_bytes());
        }

        if let Err(e) = stream.write_all(&req_buf) {
            eprintln!("Error sending command: {}", e);
            break;
        }

        match stream.read(&mut read_buf) {
            Ok(0) => {
                println!("Connection closed by server.");
                break;
            }
            Ok(n) => {
                match parse_frame(&read_buf[..n]) {
                    Ok(Some((frame, _))) => {
                        print_frame(&frame);
                    }
                    Ok(None) => {
                        println!("(Incomplete response)");
                    }
                    Err(e) => {
                        println!("(Protocol error: {})", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error reading response: {}", e);
                break;
            }
        }
    }
}

fn print_frame(frame: &Frame) {
    match frame {
        Frame::SimpleString(s) => {
            println!("{}", String::from_utf8_lossy(s));
        }
        Frame::Error(e) => {
            println!("(error) {}", String::from_utf8_lossy(e));
        }
        Frame::Integer(i) => {
            println!("(integer) {}", i);
        }
        Frame::BulkString(b) => {
            println!("\"{}\"", String::from_utf8_lossy(b));
        }
        Frame::Null => {
            println!("(nil)");
        }
        Frame::Array(arr) => {
            for (idx, elem) in arr.iter().enumerate() {
                print!("{}) ", idx + 1);
                print_frame(elem);
            }
        }
    }
}
