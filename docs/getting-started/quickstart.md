# 🚀 Quickstart Guide

Get up and running with **KacheDB** in less than 60 seconds.

---

## 📦 Installation & Deployment Options

### Option 1: Run with Pre-Built Docker Image (Recommended for Cloud/Production)

KacheDB publishes official multi-architecture container images (`linux/amd64` and `linux/arm64`) to the GitHub Container Registry:

```bash
# Run with host IPC for zero-copy POSIX Shared Memory support
docker run --privileged --ipc host -p 6379:6379 -d --name kachedb ghcr.io/vubon/kachedb:latest
```

Verify the container is running:
```bash
docker logs kachedb
```

---

### Option 2: Build & Run from Source with Cargo

#### Prerequisites
* **Rust Toolchain**: 2024 edition (`rustc >= 1.85.0`)
* **OS**: Linux (kernel 5.10+ recommended for `io_uring`) or macOS (Apple Silicon / Intel with `kqueue`)

```bash
# 1. Clone the repository
git clone https://github.com/vubon/kachedb.git
cd kachedb

# 2. Compile release binaries across all workspace crates
cargo build --release --workspace

# 3. Start the KacheDB multi-core daemon on default port 6379 with 4 worker threads
./target/release/kachedb-server -p 6379 -w 4
```

---

### Option 3: Run with Docker Compose

```bash
git clone https://github.com/vubon/kachedb.git
cd kachedb
docker compose -f docker/docker-compose.yml up -d --build
```

---

## ⚡ Connecting to KacheDB

### 1. Using the Interactive `kachedb-cli`

KacheDB includes a built-in terminal CLI with colorized output and syntax assistance:

```bash
# Start interactive REPL connected to localhost:6379
./target/release/kachedb-cli -p 6379
```

Try some basic commands:
```text
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET user:100 "alice" EX 60
OK

127.0.0.1:6379> GET user:100
"alice"

127.0.0.1:6379> TTL user:100
(integer) 58

127.0.0.1:6379> INCR counter
(integer) 1

127.0.0.1:6379> INFO
# Server
kachedb_version:0.1.0
os:macos
arch_bits:64
...
```

---

### 2. Using Standard `redis-cli`

Because KacheDB implements the standard Redis RESP2/RESP3 wire protocol, you can use any existing `redis-cli` tool:

```bash
# Connect using standard redis-cli
redis-cli -p 6379

127.0.0.1:6379> MSET key1 "val1" key2 "val2"
OK

127.0.0.1:6379> MGET key1 key2 missing_key
1) "val1"
2) "val2"
3) (nil)
```

---

### 3. Using the Official Python SDK (`kachedb-py`)

Install the client SDK:
```bash
pip install kachedb
```

Synchronous usage:
```python
from kachedb import KacheClient

with KacheClient(host="127.0.0.1", port=6379) as client:
    client.set("greeting", "Hello from Python", ex=300)
    val = client.get("greeting")
    print(val)  # b'Hello from Python'
```

Asynchronous usage (`asyncio`):
```python
import asyncio
from kachedb import AsyncKacheClient

async def main():
    async with AsyncKacheClient(host="127.0.0.1", port=6379) as client:
        await client.set("session:abc", "active_data", ex=120)
        res = await client.get("session:abc")
        print(res)

asyncio.run(main())
```

---

## 🎯 Next Steps

* Explore all supported operations in the [**Core Key-Value Command Reference**](../commands/core-kv.md).
* Learn about active memory reclamation in [**TTL & Key Lifecycle Guide**](../commands/ttl-lifecycle.md).
* Perform nearest-neighbor vector queries in the [**SIMD Vector Search Guide**](../commands/vector-search.md).
* Offload LLM prompt prefill compute in the [**vLLM Integration Guide**](../guides/vllm-integration.md).
