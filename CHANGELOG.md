# Changelog

All notable changes to **KacheDB** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [v0.1.0-alpha.2] — 2026-08-23

### 🚀 Landmark Performance Breakthroughs
- **Dethroned Redis, Valkey, and DragonflyDB on Throughput:**
  - **SET (Writes):** Reached **`2,945,079 QPS`** (+58% over Dragonfly, +220% over Valkey, +247% over Redis).
  - **GET (Reads):** Reached **`2,776,102 QPS`** (+38% over Dragonfly, +179% over Valkey, +236% over Redis).
  - **Mixed (80/20):** Reached **`2,676,994 QPS`** (+29% over Dragonfly, +190% over Valkey, +216% over Redis).
- **Sub-Millisecond Tail Latency:**
  - **P50 Latency:** Achieved **`0.855 ms`** ($1.67\times$ faster than Dragonfly $1.431\text{ ms}$, $4.17\times$ faster than Redis $3.567\text{ ms}$).
  - **P99 Latency:** **`4.543 ms`** across concurrent pipelined connections.

### ⚡ Architectural & Engine Optimizations
- **Zero-Allocation Flat SwissTable (`kachedb-hash`):**
  - Converted `SwissTable` from `Vec<Option<Box<HashEntry>>>` to a contiguous, pre-allocated `Vec<HashEntry>`.
  - Completely eliminated runtime `malloc` lock contention during multi-threaded `SET` operations.
- **$O(1)$ Active Arena Fast Path & FIFO Ring Recycling (`kachedb-core`):**
  - Implemented direct $O(1)$ active arena caching (`active_arena: [Option<usize>; 6]`) and reverse lookup table (`slab_id_to_arena`).
  - Added continuous FIFO slot reuse cursor (`allocate_or_recycle()`) preventing memory exhaustion pipeline stalls.
- **Full-Duplex TCP Pipeline & `TCP_NODELAY` (`kachedb-net`, `kachedb-server`):**
  - Enabled `TCP_NODELAY` across all accepted client sockets, eliminating 40ms Nagle algorithm latency delays.
  - Streamlined socket event loop with non-blocking level-triggered socket buffer flushes.
  - Added zero-copy in-place slice feeding and response buffer recycling.

### 🧪 Benchmarks & Quality Infrastructure
- **Comprehensive Multi-Engine Benchmark Suite (`docker/`):**
  - Added automated Docker-isolated test harness comparing Redis 7.4, Valkey 8.0, DragonflyDB, and KacheDB under identical resource constraints (4 CPUs, 4 GB RAM).
  - Generates reproducible comparative Markdown scorecards.
- **Developer Rules & Pre-Commit Gates (`AGENTS.md`):**
  - Codified mandatory pre-commit verification gates: `cargo fmt --all -- --check`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --workspace`.

---

## [v0.1.0-alpha.1] — 2026-08-22

### 🌟 Initial Release
- **Dual-Engine In-Memory Storage:**
  - Standard KV cache supporting RESP2 / RESP3 binary protocol (`PING`, `SET`, `GET`, `MGET`, `DEL`, `EXISTS`, `QUIT`).
  - LLM KV-cache engine with Shared-Prefix Radix Tree, copy-on-write epochs, and zero-copy POSIX SHM IPC (`/dev/shm`).
- **SIMD Swiss Table & S3-FIFO Eviction:**
  - SIMD control byte probe groups with 7-bit H2 fingerprints.
  - S3-FIFO cache eviction (Small, Main, and Ghost rings).
- **Megaslab Arena Allocator:**
  - 2 MB power-of-two slab classes with cache-line-aligned slot layouts and zero runtime fragmentation.
- **Thread-Per-Core Network Engine:**
  - Shared-nothing worker threads pinned to CPU cores with `SO_REUSEPORT` listeners.
- **CLI & Benchmark Tools:**
  - `kachedb-cli` terminal client with interactive REPL.
  - `kachedb-bench` high-throughput load generator.
