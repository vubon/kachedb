# 💻 `kachedb-cli` User Guide

`kachedb-cli` is the native command-line interface and benchmarking utility for **KacheDB**. It provides an interactive REPL with formatted RESP rendering, auto-reconnect capabilities, and a high-performance live throughput benchmarking harness.

---

## 🛠️ Usage & Command-Line Flags

```bash
kachedb-cli [OPTIONS]
```

### Options Reference

| Flag | Long Flag | Description | Default |
| :--- | :--- | :--- | :--- |
| `-h` | `--host <HOST>` | Target KacheDB server hostname or IP address | `127.0.0.1` |
| `-p` | `--port <PORT>` | Target KacheDB server TCP port | `6379` |
| `-b` | `--bench` | Execute a high-speed throughput benchmark | `false` |
| `-n` | `-n <NUM>` | Total number of requests in benchmark mode | `10,000` |
| | `--help` | Display command help and usage flags | |

---

## 💬 Interactive REPL Mode

Starting `kachedb-cli` without `--bench` enters interactive REPL mode:

```bash
./target/release/kachedb-cli -h 127.0.0.1 -p 6379
```

### Banner & Prompt
Upon connection, `kachedb-cli` displays the ASCII logo and prompt:

```text
  _  __           _          _____  ____   _____ _      _____ 
 | |/ /          | |        |  __ \|  _ \ / ____| |    |_   _|
 | ' / __ _  ___| |__   ___| |  | | |_) | |    | |      | |  
 |  < / _` |/ __| '_ \ / _ \ |  | |  _ <| |    | |      | |  
 | . \ (_| | (__| | | |  __/ |__| | |_) | |____| |____ _| |_ 
 |_|\_\__,_|\___|_| |_|\___|_____/|____/ \_____|______|_____|

Connecting to KacheDB at 127.0.0.1:6379...
⚡ Connected to KacheDB. Type commands or 'help' / 'quit'.

127.0.0.1:6379> 
```

---

## ⚡ REPL Commands & Examples

### 1. Key-Value & Strings
```text
127.0.0.1:6379> SET user:1 "Alice Smith"
OK

127.0.0.1:6379> GET user:1
"Alice Smith"

127.0.0.1:6379> APPEND user:1 " (Admin)"
(integer) 19

127.0.0.1:6379> GET user:1
"Alice Smith (Admin)"

127.0.0.1:6379> STRLEN user:1
(integer) 19
```

### 2. Atomic Counters
```text
127.0.0.1:6379> INCR visits
(integer) 1

127.0.0.1:6379> INCRBY visits 10
(integer) 11

127.0.0.1:6379> DECR visits
(integer) 10

127.0.0.1:6379> DECRBY visits 5
(integer) 5
```

### 3. Expiration & TTL Management
```text
127.0.0.1:6379> SET session:temp "xyz" EX 60
OK

127.0.0.1:6379> TTL session:temp
(integer) 58

127.0.0.1:6379> PERSIST session:temp
(integer) 1

127.0.0.1:6379> TTL session:temp
(integer) -1
```

### 4. Vector Ingestion & Nearest Neighbor Search
```text
# Store a 4-dimensional vector in index 'docs' with ID 'doc:1'
127.0.0.1:6379> VADD docs doc:1 4 "\x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" PAYLOAD "Introduction to KacheDB" EX 3600
OK

# Search index 'docs' for closest vector matches
127.0.0.1:6379> VSEARCH docs "\x00\x00\x80?\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" TOPK 1 THRESHOLD 0.8
1) 1) "doc:1"
   2) "1.000000"
   3) "Introduction to KacheDB"

# Check vector index statistics
127.0.0.1:6379> VSTATS docs
1) "dimension"
2) (integer) 4
3) "total_vectors"
4) (integer) 1
5) "active_vectors"
6) (integer) 1
6) "memory_bytes"
8) (integer) 64
```

### 5. Introspection & Diagnostics
```text
127.0.0.1:6379> INFO
# Server
kachedb_version:0.1.0
os:macos
arch_bits:64
process_id:48123
tcp_port:6379
uptime_in_seconds:342

# Memory
used_memory:134217728
used_memory_human:128.00M
used_memory_peak:134217728
megaslabs_allocated:64
slab_slots_active:1024
fragmentation_ratio:1.00

# Stats
total_connections_received:42
total_commands_processed:15234
instantaneous_ops_per_sec:0
keyspace_hits:14200
keyspace_misses:1034
```

### 6. Built-in REPL Helper Commands
* `help`: Displays a quick command reference card.
* `clear`: Clears the terminal screen.
* `quit` / `exit`: Closes the connection and exits the CLI.

---

## 🔥 Live Benchmark Mode (`--bench`)

You can measure raw network round-trip throughput and latency using the built-in benchmark harness:

```bash
# Run a 100,000-request pipelined benchmark against localhost
./target/release/kachedb-cli -p 6379 --bench -n 100000
```

### Example Benchmark Output
```text
🔥 Connecting to 127.0.0.1:6379 for live benchmark (100,000 requests)...
⚡ Pipelining 100,000 PING commands...

══════════════════════════════════════════════════════════════
  KacheDB Live Benchmark Report
══════════════════════════════════════════════════════════════
  Requests:          100,000
  Total Elapsed:     34.28 ms
  Throughput:        2,916,742 ops/sec
  Average Latency:   342.8 ns / op
══════════════════════════════════════════════════════════════
```
