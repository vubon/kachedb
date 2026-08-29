//! `kachedb-cli` — Interactive CLI client and live benchmarking utility for KacheDB.

use std::io::{BufRead, Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use kachedb_proto_resp::{Frame, encode_array_header, encode_bulk_string, parse_frame};

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

fn run_repl(addr: &str) {
    println!(
        r#"
  _  __           _          _____  ____   _____ _      _____ 
 | |/ /          | |        |  __ \|  _ \ / ____| |    |_   _|
 | ' / __ _  ___| |__   ___| |  | | |_) | |    | |      | |  
 |  < / _` |/ __| '_ \ / _ \ |  | |  _ <| |    | |      | |  
 | . \ (_| | (__| | | |  __/ |__| | |_) | |____| |____ _| |_ 
 |_|\_\__,_|\___|_| |_|\___|_____/|____/ \_____|______|_____|
"#
    );
    println!("Connecting to KacheDB at {}...", addr);
    let mut stream = match TcpStream::connect(addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("❌ Failed to connect to KacheDB at {}: {}", addr, e);
            eprintln!("(Make sure `cargo run -p kachedb-server` is running)");
            return;
        }
    };

    println!(
        "⚡ Connected to KacheDB. Type commands (e.g. SET, GET, EXPIRE, MSET, INCR, INFO, VADD, VSEARCH) or 'help' / 'quit'.\n"
    );

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut read_buf = vec![0u8; 64 * 1024];

    loop {
        print!("{}> ", addr);
        let _ = stdout.flush();

        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if trimmed.eq_ignore_ascii_case("clear") {
            print!("\x1B[2J\x1B[1;1H");
            let _ = stdout.flush();
            continue;
        }

        if trimmed.eq_ignore_ascii_case("help") {
            print_cli_help();
            continue;
        }

        if trimmed.eq_ignore_ascii_case("quit") || trimmed.eq_ignore_ascii_case("exit") {
            println!("Bye!");
            break;
        }

        let parts = tokenize_command(trimmed);
        if parts.is_empty() {
            continue;
        }

        // Encode as RESP Array
        let mut req_buf = Vec::new();
        encode_array_header(&mut req_buf, parts.len());
        for part in &parts {
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
            Ok(n) => match parse_frame(&read_buf[..n]) {
                Ok(Some((frame, _))) => {
                    print_frame(&frame, 0);
                }
                Ok(None) => {
                    println!("(Incomplete response)");
                }
                Err(e) => {
                    println!("(Protocol error: {})", e);
                }
            },
            Err(e) => {
                eprintln!("Error reading response: {}", e);
                break;
            }
        }
    }
}

fn tokenize_command(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\'' if !in_double_quote => {
                in_single_quote = !in_single_quote;
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
            }
            '\\' if in_double_quote => {
                if let Some(next_ch) = chars.next() {
                    match next_ch {
                        'n' => current.push('\n'),
                        'r' => current.push('\r'),
                        't' => current.push('\t'),
                        '\\' => current.push('\\'),
                        '"' => current.push('"'),
                        _ => {
                            current.push('\\');
                            current.push(next_ch);
                        }
                    }
                }
            }
            c if c.is_whitespace() && !in_single_quote && !in_double_quote => {
                if !current.is_empty() {
                    tokens.push(std::mem::take(&mut current));
                }
            }
            c => {
                current.push(c);
            }
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn print_cli_help() {
    println!(
        r#"
Available Commands in KacheDB:
  • Key-Value:
      SET <key> <val> [EX <sec>]  - Set key to value with optional TTL in seconds
      GET <key>                   - Retrieve value of key
      MSET <k1> <v1> [<k2> <v2>]  - Set multiple keys simultaneously
      MGET <k1> [<k2> ...]        - Retrieve multiple keys simultaneously
      DEL <key> [<key> ...]       - Delete key(s)
      INCR <key> / DECR <key>     - Increment / Decrement integer value by 1
      INCRBY <key> <delta>        - Increment integer value by delta
      APPEND <key> <val>          - Append string to key
      STRLEN <key>                - Return byte length of string value
  • Expiration & TTL:
      EXPIRE <key> <sec>          - Set expiration in seconds from now
      TTL <key> / PTTL <key>      - Query remaining time-to-live
      PERSIST <key>               - Remove expiration from key
  • Server & Observability:
      HELLO [2|3]                 - Handshake and switch RESP protocol version
      INFO [section]              - Return server runtime, memory, and stats
      CLIENT SETNAME <name>       - Assign connection name
      CLIENT GETNAME              - Get connection name
      PING [msg]                  - Ping server
  • Vector Engine:
      VADD <idx> <key> <dim> <f32...>  - Insert embedding vector
      VSEARCH <idx> <top_k> <f32...>   - Cosine similarity search
"#
    );
}

fn print_frame(frame: &Frame, depth: usize) {
    let indent = "  ".repeat(depth);
    match frame {
        Frame::SimpleString(s) => {
            println!("{}{}", indent, String::from_utf8_lossy(s));
        }
        Frame::Error(e) => {
            println!("{}(error) {}", indent, String::from_utf8_lossy(e));
        }
        Frame::Integer(i) => {
            println!("{}(integer) {}", indent, i);
        }
        Frame::BulkString(b) => {
            let s = String::from_utf8_lossy(b);
            if s.contains('\n') {
                for line in s.lines() {
                    println!("{}{}", indent, line);
                }
            } else {
                println!("{}\"{}\"", indent, s);
            }
        }
        Frame::Null => {
            println!("{}(nil)", indent);
        }
        Frame::Array(arr) => {
            if arr.is_empty() {
                println!("{}(empty list or set)", indent);
            } else {
                for (idx, elem) in arr.iter().enumerate() {
                    print!("{}{}) ", indent, idx + 1);
                    if matches!(&**elem, Frame::Array(_)) {
                        println!();
                    }
                    print_frame(elem, depth + 1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_plain_and_quoted_arguments() {
        let tokens = tokenize_command("SET foo bar");
        assert_eq!(tokens, vec!["SET", "foo", "bar"]);

        let tokens = tokenize_command("SET \"user session\" 'logged in value'");
        assert_eq!(tokens, vec!["SET", "user session", "logged in value"]);

        let tokens = tokenize_command("MSET k1 \"val 1\" k2 \"val 2\"");
        assert_eq!(tokens, vec!["MSET", "k1", "val 1", "k2", "val 2"]);

        let tokens = tokenize_command("VADD index_a key1 4 0.1 0.2 0.3 0.4");
        assert_eq!(
            tokens,
            vec!["VADD", "index_a", "key1", "4", "0.1", "0.2", "0.3", "0.4"]
        );
    }
}
