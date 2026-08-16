# KacheDB

<p align="center">
  <img src="https://raw.githubusercontent.com/vubon/kachedb/main/docs/assets/banner.png" alt="KacheDB Banner" width="600" onerror="this.style.display='none'"/>
</p>

<p align="center">
  <strong>The High-Performance, Zero-Copy In-Memory Engine for Redis-Compatible Caching & LLM KV-Cache Offloading</strong>
</p>

<p align="center">
  <a href="#benchmarks"><img src="https://img.shields.io/badge/benchmark-sub--4ns%20alloc-brightgreen.svg" alt="Benchmark"/></a>
  <a href="#benchmarks"><img src="https://img.shields.io/badge/throughput-10.2M%20QPS%2Fcore-blue.svg" alt="Throughput"/></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-Apache--2.0%20%2F%20MIT-blue.svg" alt="License"/></a>
  <a href="#tests"><img src="https://img.shields.io/badge/tests-68%20passed%2C%200%20failed-success.svg" alt="Tests"/></a>
</p>

---

## ⚡ Overview

**KacheDB** is a next-generation in-memory storage engine written in Rust. It unifies two critical workloads into a single, zero-copy architecture:

1. **Microsecond App Cache**: Redis & Valkey wire-compatible key-value cache (RESP2 / RESP3 protocol) powered by a SIMD-accelerated Swiss Table and an explicit Megaslab memory allocator.
2. **LLM KV-Cache Offloader**: Hierarchical `&[u32]` token prefix tree and zero-copy POSIX Shared Memory (`/dev/shm`) ring buffer transport for vLLM, SGLang, and PyTorch inference engines.

---

## 📐 System Architecture

```text
+-------------------------------------------------------------------------------+
|                                CLIENT LAYER                                   |
|   [RESP3 Wire Protocol (Redis/Valkey Compat)]     [Tensor-IPC / Python SDK]   |
+-------------------------------------------------------------------------------+
                                       |
+-------------------------------------------------------------------------------+
|                               INDEXING LAYER                                  |
|   1. SIMD Swiss Hash Table (O(1) Point Queries for App Keys, S3-FIFO)         |
|   2. Token Radix Prefix Tree (&[u32] Longest Prefix Match for LLM KV-Cache)   |
+-------------------------------------------------------------------------------+
                                       |
+-------------------------------------------------------------------------------+
|                          CORE SLAB & ARENA ENGINE                             |
|   - 2 MB Megaslab Page Frames (64-byte Cache-Line Aligned Slots)              |
|   - Zero Runtime malloc/free Allocation Jitter (Bump-pointer + Free-list)     |
|   - Thread-per-Core Isolated Memory Pools (Shared-Nothing Topology)           |
+-------------------------------------------------------------------------------+
                                       |
+-------------------------------------------------------------------------------+
|                        STORAGE & ZERO-COPY TRANSPORT                          |
|   - POSIX Shared Memory (/dev/shm) Lock-Free SPSC Ring Buffer IPC             |
|   - Asynchronous TCP Event Loop (Linux io_uring / macOS kqueue)               |
+-------------------------------------------------------------------------------+
```

---

## 🏎️ Benchmark Performance

All micro-benchmarks evaluated with [Criterion.rs](https://github.com/bheisler/criterion.rs) in release mode (`opt-level = 3`):

| Subsystem | Operation | Measured Performance | Target / Context |
| :--- | :--- | ---: | :--- |
| **`kachedb-core`** | Megaslab Slot Allocation (`AppSmall` 128 B) | **3.93 ns** | < 20 ns target (5.1× faster) |
| **`kachedb-core`** | Multi-Arena Pool Allocate + Free (`Tensor64KB`) | **7.06 ns** | < 20 ns target (2.8× faster) |
| **`kachedb-hash`** | Swiss Table Point Query Hit (1M entries) | **3.15 ns** | L1 cache-speed lookup |
| **`kachedb-radix`** | 1,024-token Prompt Prefix Match (64 blocks) | **2.51 µs** | **~10,000× speedup** vs GPU prefill |
| **`kachedb-radix`** | Bottom-up LRU Leaf Eviction | **519 ns** | Sub-microsecond memory reclaim |
| **`kachedb-shm`** | POSIX Shared Memory IPC Streaming | **65.4 ns / msg** | **15.28 Million msgs/sec** across cores |
| **`kachedb-proto-resp`** | Zero-Alloc RESP `GET` Parsing & Decoding | **69.54 ns** | Zero heap allocations on borrowed slice |
| **`kachedb-net`** | End-to-End `GET` In-Memory Pipeline | **97.63 ns** | **~10.24 Million requests/sec / core** |

---

## 📦 Workspace Crates

```text
kachedb/
├── crates/
│   ├── kachedb-core/             # 64-byte aligned Megaslab memory allocator & SlabPool
│   ├── kachedb-hash/             # SIMD Swiss Table hash index with S3-FIFO access bit
│   ├── kachedb-radix/            # &[u32] token prefix tree with bottom-up LRU eviction
│   ├── kachedb-proto-tensor/     # 64-byte TensorBlockDescriptor & PagedAttention layouts
│   ├── kachedb-shm/              # Zero-copy POSIX /dev/shm SPSC ring buffer IPC
│   ├── kachedb-proto-resp/       # Zero-allocation streaming RESP2/RESP3 wire parser
│   ├── kachedb-net/              # Thread-per-core async TCP engine (io_uring / mio)
│   ├── kachedb-server/           # Multi-core daemon runtime executable
│   └── kachedb-cli/              # Interactive CLI admin & live benchmark tool
├── bindings/
│   └── python/                   # Zero-copy Python client & PyTorch/vLLM bindings
├── benchmarks/                   # Phase benchmark reports & evaluations
└── docker/                       # Standalone reproducible Linux container environment
```

---

## 🚀 Quick Start

### 1. Prerequisites

- **Rust**: 1.80+ (`rustup default stable`)
- **Python**: 3.9+ (for Python SDK & PyTorch tensor bindings)

### 2. Build the Workspace

```bash
git clone https://github.com/vubon/kachedb.git
cd kachedb

# Build release binaries
cargo build --workspace --release
```

### 3. Run the Server Daemon

```bash
# Launch multi-worker daemon on port 6379 across all available CPU cores
./target/release/kachedb-server --port 6379 --workers 8 --pool-mb 64
```

### 4. Interactive CLI & Live Benchmarking

```bash
# Connect to interactive REPL
./target/release/kachedb-cli --port 6379

127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET user:100 "alice"
OK
127.0.0.1:6379> GET user:100
"alice"

# Run a live 10,000-request throughput benchmark
./target/release/kachedb-cli --port 6379 --bench -n 10000
```

### 5. Standard Redis Client Compatibility

Because KacheDB implements the standard RESP wire protocol, you can use any Redis client:

```bash
redis-cli -p 6379 PING
# Returns: PONG
```

---

## 🐍 Python Zero-Copy Client & PyTorch Integration

```python
from kachedb import KacheClient

# Connect to KacheDB daemon over TCP
with KacheClient(host="127.0.0.1", port=6379) as client:
    # Standard Redis caching
    client.set("session:user_1", "active_payload", ex=3600)
    print(client.get("session:user_1"))

    # Zero-copy KV-cache tensor extraction from /dev/shm
    tensor = client.read_tensor_zero_copy(core_id=0, byte_offset=0)
    print("Zero-copy Tensor Shape:", tensor.shape)
```

---

## 🧪 Running Tests & Benchmarks

```bash
# Run all 68 unit & doc tests across all crates
cargo test --workspace

# Run Criterion micro-benchmarks
cargo bench --workspace
```

---

## 📄 License

Dual-licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
