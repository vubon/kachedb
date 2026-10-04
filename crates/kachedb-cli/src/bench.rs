//! Live benchmark utility for KacheDB.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use kachedb_proto_resp::{encode_array_header, encode_bulk_string};

pub fn run_benchmark(addr: &str, requests: usize) {
    println!(
        "🔥 Connecting to {} for live benchmark ({} requests)...",
        addr, requests
    );

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
