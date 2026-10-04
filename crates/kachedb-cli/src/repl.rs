//! Interactive REPL client and terminal response formatter for KacheDB.

use std::io::{BufRead, Read, Write};
use std::net::TcpStream;

use kachedb_proto_resp::{Frame, encode_array_header, encode_bulk_string, parse_frame};

pub fn run_repl(addr: &str) {
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
        "⚡ Connected to KacheDB. Type commands (e.g. SET, GET, HSET, HGET, EXPIRE, MSET, INCR, INFO, VADD, VSEARCH) or 'help' / 'quit'.\n"
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

pub fn run_one_shot(addr: &str, command: &[String]) -> Result<(), String> {
    if command.is_empty() {
        return Ok(());
    }

    let mut stream = TcpStream::connect(addr)
        .map_err(|e| format!("Failed to connect to KacheDB at {addr}: {e}"))?;

    let mut req_buf = Vec::new();
    encode_array_header(&mut req_buf, command.len());
    for part in command {
        encode_bulk_string(&mut req_buf, part.as_bytes());
    }

    stream
        .write_all(&req_buf)
        .map_err(|e| format!("Error sending command: {e}"))?;

    let mut read_buf = vec![0u8; 64 * 1024];
    let n = stream
        .read(&mut read_buf)
        .map_err(|e| format!("Error reading response: {e}"))?;

    if n == 0 {
        return Err("Connection closed by server.".to_string());
    }

    match parse_frame(&read_buf[..n]) {
        Ok(Some((frame, _))) => {
            print_frame(&frame, 0);
            if matches!(frame, Frame::Error(_)) {
                std::process::exit(1);
            }
            Ok(())
        }
        Ok(None) => Err("(Incomplete response from server)".to_string()),
        Err(e) => Err(format!("(Protocol error: {e})")),
    }
}

pub fn tokenize_command(input: &str) -> Vec<String> {
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

pub fn print_cli_help() {
    println!(
        r#"
Available Commands in KacheDB:
  • Key-Value:
      SET <key> <val> [EX <sec>]  - Set key to value with optional TTL in seconds
      GET <key>                   - Retrieve value of key
      MSET <k1> <v1> [<k2> <v2>]  - Set multiple keys simultaneously
      MGET <k1> [<k2> ...]        - Retrieve multiple keys simultaneously
      DEL <key> [<key> ...]       - Delete key(s)
      EXISTS <key> [<key> ...]    - Check if key(s) exist
      INCR <key> / DECR <key>     - Increment / Decrement integer value by 1
      INCRBY <key> <delta>        - Increment integer value by delta
      DECRBY <key> <delta>        - Decrement integer value by delta
      APPEND <key> <val>          - Append string to key
      STRLEN <key>                - Return byte length of string value
  • Redis Hashes:
      HSET <key> <f> <v> [<f> <v>] - Set field-value pair(s) in hash
      HGET <key> <field>          - Retrieve value of hash field
      HDEL <key> <f> [<f> ...]    - Delete field(s) from hash
      HEXISTS <key> <field>       - Check if field exists in hash
      HLEN <key>                  - Return number of fields in hash
      HGETALL <key>               - Return all fields and values in hash
  • Expiration & TTL:
      EXPIRE <key> <sec>          - Set expiration in seconds from now
      PEXPIRE <key> <ms>          - Set expiration in milliseconds from now
      EXPIREAT <key> <ts>         - Set expiration timestamp (Unix seconds)
      PEXPIREAT <key> <ts_ms>     - Set expiration timestamp (Unix milliseconds)
      TTL <key>                   - Query remaining time-to-live in seconds
      PTTL <key>                  - Query remaining time-to-live in milliseconds
      PERSIST <key>               - Remove expiration from key
  • Database & Keyspace:
      DBSIZE                      - Return total number of active keys
      TYPE <key>                  - Return data type of key (string, hash)
      FLUSHDB / FLUSHALL          - Clear database keys and reclaim memory
  • Server & Observability:
      HELLO [2|3]                 - Handshake and switch RESP protocol version
      AUTH <password>             - Authenticate connection
      INFO [section]              - Return server runtime, memory, and stats
      CLIENT SETNAME <name>       - Assign connection name
      CLIENT GETNAME              - Get connection name
      BGREWRITEAOF                - Trigger background AOF rewrite
      PING [msg]                  - Ping server
      QUIT                        - Close connection
  • Vector Engine:
      VADD <idx> <key> <dim> <f32...>       - Insert embedding vector
      VSEARCH <idx> <top_k> <f32...>        - Cosine similarity search
      VADDBATCH <idx> <dim> <n> <data...>   - Batch insert vectors
      VSearchBatch <idx> <top_k> <queries>  - Batch search vectors
      VDEL <idx> <key>                      - Delete vector by key
      VSTATS [idx]                          - Return vector index stats
      VINDEX CREATE <idx> <dim> <metric>    - Create named HNSW index
      VINDEX DROP <idx>                     - Drop named vector index
      VINDEX INFO <idx>                     - Detailed index configuration & stats
"#
    );
}

pub fn print_frame(frame: &Frame, depth: usize) {
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

    #[test]
    fn test_tokenize_hash_and_vector_index_commands() {
        let tokens =
            tokenize_command("HSET user:1 name \"John Doe\" email john@example.com age 30");
        assert_eq!(
            tokens,
            vec![
                "HSET",
                "user:1",
                "name",
                "John Doe",
                "email",
                "john@example.com",
                "age",
                "30"
            ]
        );

        let tokens = tokenize_command("VINDEX CREATE doc_index 128 COSINE");
        assert_eq!(
            tokens,
            vec!["VINDEX", "CREATE", "doc_index", "128", "COSINE"]
        );
    }
}
