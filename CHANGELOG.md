# Changelog

All notable changes to **KacheDB** will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.1.0] — 2026-09-08 (Official Production Stable Release)

### 🚀 Production Stabilization & Single-Node Core Lock
- **Canonical Configuration File & CLI Flags (`kachedb.conf`):**
  - Introduced canonical configuration template `kachedb.conf` documenting production defaults for network, memory pools, persistence, and TLS.
  - Implemented `-c` / `--config <path>` flag support in `kachedb-server` with clean CLI override precedence.
- **Connection Limiting & Quota Management (`maxclients`):**
  - Added global atomic client tracking and limit enforcement (`maxclients`, default: 10,000).
  - Automatically rejects over-capacity connections with `-ERR max number of clients reached\r\n` and immediate graceful disconnect.
- **Extended Redis Command Compatibility (`DBSIZE`, `TYPE`, `FLUSHDB`, `FLUSHALL`):**
  - Added `DBSIZE`: returns exact count of active keys across all 256 shards in $O(1)$.
  - Added `TYPE <key>`: returns `+string\r\n` or `+none\r\n` matching Redis specifications.
  - Added `FLUSHDB` and `FLUSHALL`: clears in-memory tables and instantly recycles slab blocks back into core memory pools without memory leaks.
- **Hyper-Optimized Distroless Docker Image (37.5 MB disk / ~12 MB download):**
  - Transitioned runtime image to `gcr.io/distroless/cc-debian12:nonroot` with automatic binary debug symbol stripping (`strip`).
  - Core KacheDB binaries total only **3.2 MB**; full uncompressed runtime slashed from 108 MB down to **37.5 MB** ($2.9\times$ reduction, ~12 MB compressed transfer).
- **Production Systemd Service Unit (`docker/kachedb.service`):**
  - Added hardened systemd unit file with security sandboxing (`ProtectSystem=strict`, `ProtectHome=true`, `LimitNOFILE=65535`).
- **mdBook Official Documentation Site:**
  - Configured mdBook documentation build with multi-version dropdown switcher (`v0.1.0 (Stable)` and `latest`) deployed to `https://vubon.github.io/kachedb/`.
  - Added automated GitHub Actions documentation deployment workflow (`.github/workflows/docs.yml`).

---

## [v0.1.0-beta.3] — 2026-09-06

### 🏆 Milestone Benchmark Victory: Lowest Memory Footprint & 3.56M QPS
- **Uncontested #1 Across Memory and Speed (`docs/benchmarks/benchmark_comparison_results.md`):**
  - **Lowest Peak RSS:** **`871.4 MiB`** — Lower memory footprint than Redis (`1,001 MiB`), Valkey (`932 MiB`), and Dragonfly (`1.05 GiB`).
  - **Peak GET (Reads):** **`3,563,603 QPS`** ($3.69\times$ Redis, $3.45\times$ Valkey, $1.55\times$ Dragonfly).
  - **Peak SET (Writes):** **`3,217,341 QPS`** ($3.48\times$ Redis, $3.25\times$ Valkey, $1.57\times$ Dragonfly).
  - **Mixed 80/20 QPS:** **`3,524,141 QPS`** ($3.75\times$ Redis, $3.65\times$ Valkey, $1.75\times$ Dragonfly).
  - **P50 Tail Latency:** **`0.703 ms`** ($4.62\times$ faster than Redis $3.25\text{ ms}$, $1.86\times$ faster than Dragonfly $1.31\text{ ms}$).
  - **P99 Tail Latency:** **`3.535 ms`** (Lowest tail latency of all tested engines).

### 🧹 Megaslab Adaptive Compaction & Memory Optimization (`crates/kachedb-core`)
- **Continuous Megaslab Page Deflation:**
  - Implemented `madvise(MADV_DONTNEED)` on free slab pages in the megaslab allocator to immediately yield physical pages back to the operating system kernel without unmapping virtual address space.
  - Background compaction routine reclaims unused slab frames and defragments active slots.
- **Hot-Path SET Optimization & Multi-Arena Recycling:**
  - Removed hot-path `SystemTime::now()` syscalls from pool allocation paths.
  - Implemented cross-arena round-robin recycling (`recycle_slot()`) avoiding pipeline stalls under stringent memory constraints.
  - Eliminated mutex locking on hot mutation paths with atomic fast-path flags.

### 📐 HNSW Vector Indexing & 8-Bit Scalar Quantization (`crates/kachedb-vector`)
- **Hierarchical Navigable Small World (HNSW) Index:**
  - Implemented multi-layer HNSW graph exploration supporting sub-millisecond approximate nearest neighbor (ANN) search over massive vector datasets.
  - Configurable graph connectivity parameters `M` and `ef_construction` via `VINDEX CREATE <index> TYPE HNSW`.
- **SQ8 Scalar Quantization:**
  - Added 8-bit scalar quantization compressing 32-bit floating-point embeddings by $4\times$ (75% memory footprint reduction) with minimal recall degradation.
  - Hardware-accelerated integer dot products leveraging AVX2 and NEON SIMD instructions.

### 💾 Append-Only File (AOF) Persistence Engine (`crates/kachedb-core` & `crates/kachedb-net`)
- **Streaming Binary Journaling (`.kaof`):**
  - Continuous binary append log recording all state mutations (`SET`, `DEL`, `EXPIRE`, `VADD`, `VDEL`).
  - Framing with 32-bit CRC32 integrity checksums per transaction record.
  - Configurable sync policies: `always`, `everysec`, and `no`.
- **Startup Crash Recovery & Replay:**
  - High-throughput parallel log replayer parsing `.kaof` records at server startup to reconstruct state.
- **Online Zero-Downtime Compaction (`BGREWRITEAOF`):**
  - Non-blocking background compaction snapshotting current in-memory keyspace into a fresh minimal journal file.

### 🔒 TLS 1.3 Encryption & Password Authentication Hardening (`crates/kachedb-net`)
- **Native TLS 1.3 Wire Security:**
  - Direct TLS termination powered by `rustls 0.23` with ALPN `resp/2` negotiation.
  - Optional Mutual TLS (mTLS) client certificate verification (`--ssl-cert`, `--ssl-key`, `--ssl-ca`).
- **Timing-Safe Password Authentication:**
  - Constant-time password hashing and verification resisting side-channel timing attacks via `AUTH <password>`.

---

## [v0.1.0-beta.2] — 2026-09-02

### ⚡ Batch Vector Operations & AI Ecosystem Integration
- **Zero-Copy Bulk Vector Operations (`crates/kachedb-proto-resp` & `crates/kachedb-net`):**
  - Added `VADD_BATCH` for bulk ingestion of embedding vectors and JSON payloads in a single round-trip.
  - Added `VSEARCH_BATCH` for parallel multi-query similarity searches across shared index graphs.
- **Benchmarking & Tooling Enhancements (`crates/kachedb-bench`):**
  - Added `-P/--pipeline` and `-t/--command` CLI flags.
  - Pre-allocated zero-heap RESP frame builder for saturating multi-gigabit pipelines.
- **OpenAI Reverse Proxy Integration Guide:**
  - Added comprehensive integration guide and reference architecture for OpenAI semantic caching proxy.

---

## [v0.1.0-beta.1] — 2026-08-30

### ⏱️ Full Redis TTL Lifecycle & Active Timing Wheel Expiry Engine
- **RESP Key Expiration & TTL Lifecycle Primitives (`crates/kachedb-proto-resp` & `crates/kachedb-hash`):**
  - Implemented standard RESP2/RESP3 commands: `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, and `PERSIST`.
  - In-place TTL metadata updates in 64-byte `TableEntry` with zero heap reallocations and no Megaslab slot moves.
- **Active Background Timing Wheel Memory Reclamation (`crates/kachedb-core` & `crates/kachedb-net`):**
  - Integrated the hierarchical $\mathcal{O}(1)$ 3,600-bucket `HashedTimingWheel` directly into the worker event loop tick.
  - Active 1-second interval eviction of expired keys with atomic removal from the `ShardedSwissTable` and immediate slot deallocation back to the `SlabPool` free-list.
- **Extended Redis Commands (`crates/kachedb-proto-resp` & `crates/kachedb-net`):**
  - Added zero-allocation implementations of `MSET`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, and `STRLEN`.
- **Observability & Modern Client Introspection:**
  - Implemented `INFO` (`server`, `memory`, `stats`, `keyspace`, `vectorengine`), `COMMAND` / `COMMAND DOCS`, `HELLO 2/3`, and `CLIENT SETNAME/GETNAME/ID/LIST`.
- **Multi-Architecture Container Release:**
  - Configured multi-arch `linux/amd64` and `linux/arm64` container builds publishing to GitHub Container Registry (`ghcr.io/vubon/kachedb`).

---

## [v0.1.0-alpha.4] — 2026-08-28

### 🧠 In-Memory SIMD Vector Search & Semantic Cache Engine
- **Hardware-Accelerated SIMD Vector Math Kernel (`crates/kachedb-vector`):**
  - **ARM NEON Kernel (`aarch64`):** 128-bit `vfmaq_f32` with 4-way loop unrolling (16 floats per iteration) delivering $< 120\text{ ns}$ dot products.
  - **x86_64 AVX2 / FMA Kernel:** 256-bit `_mm256_fmadd_ps` with 4-way loop unrolling (32 floats per iteration) delivering $> 40\text{ GB/s}$ throughput.
  - **Runtime CPUID Probing:** Dynamic feature detection on x86_64 (`is_x86_feature_detected!`) with auto-vectorized portable scalar fallback.
  - **Math Routines:** $L_2$ vector pre-normalization (`l2_normalize`), normalized Cosine Similarity (`cosine_similarity_normalized`), and Euclidean distance.
- **In-Memory `VectorIndex` & `VectorIndexRegistry`:**
  - Contiguous float storage arena with 64-byte alignment for maximum CPU L1/L2 cache locality.
  - Multi-tenant named index isolation (`VectorIndexRegistry`).
  - Active TTL expiration and lazy background reclaiming for temporary session vectors.
  - Thread-safe concurrency with reader-writer locks (`parking_lot::RwLock`).
- **RESP Vector Command Suite (`crates/kachedb-proto-resp` & `crates/kachedb-net`):**
  - Added zero-allocation parsing and execution for:
    - `VADD <index> <id> <dim> <vector_bytes> [PAYLOAD <payload>] [EX <seconds>]`
    - `VSEARCH <index> <query_bytes> [TOPK <k>] [THRESHOLD <min_sim>]`
    - `VDEL <index> <id>`
    - `VSTATS <index>`
- **Benchmarks & Test Suite:** Added Criterion micro-benchmarks (`benches/simd_vector.rs`) and unit tests across math kernels, index registry, and RESP wire execution (109/109 workspace tests passing).

---

## [v0.1.0-alpha.3] — 2026-08-24

### 🏆 Scorecard Clean Sweep (Clean #1 Across All Metrics)
- **GET (Reads):** Reached **`2,912,997 QPS`** (+64.1% over Dragonfly 1.78M, +205% over Valkey 953k, +264% over Redis 800k).
- **Mixed (80/20):** Reached **`2,127,839 QPS`** (+31.0% over Dragonfly 1.62M, +135% over Valkey 902k, +147% over Redis 859k).
- **SET (Writes):** Maintained **`2,896,299 QPS`** (+48% over Dragonfly 1.96M, +241% over Valkey 849k, +262% over Redis 800k).
- **P50 Tail Latency:** **`0.911 ms`** ($1.58\times$ lower than Dragonfly 1.44 ms, $3.44\times$ lower than Valkey 3.14 ms, $3.76\times$ lower than Redis 3.42 ms).
- **P99 Tail Latency:** **`4.319 ms`** ($1.65\times$ lower than Dragonfly 7.14 ms, $1.73\times$ lower than Valkey 7.49 ms).

### ⚡ Architectural & Memory Scaling Breakthroughs
- **Connection-Aware Accept-Dispatch Architecture (`kachedb-net`):**
  - Eliminated Linux `SO_REUSEPORT` 4-tuple localhost connection skew by introducing a dedicated `AcceptDispatcher` thread.
  - Round-robin distributes client sockets to worker threads via bounded `crossbeam-channel` rings.
  - Saturated all 4 CPU cores evenly at 90.5%+ utilization across all workloads.
- **Dynamic 16 MB Swiss Table with Auto-Shrinking (`kachedb-hash`):**
  - Reduced initial `ShardedSwissTable` memory from **1.07 GiB** down to **16 MB** (1,024 slots per shard).
  - **Auto-Grow:** Independent micro-shards double capacity when load factor exceeds **87.5%**.
  - **Auto-Shrink:** Independent micro-shards halve capacity when load factor drops below **12.5%** down to minimum floor, actively releasing memory back to the OS.
- **Lazy Megaslab Arena Allocation (`kachedb-core`):**
  - Removed pre-warmed slab allocations from `SlabPool::new()` — arenas grow lazily on first allocation per class.
  - Peak RAM reduced by **61%** (from 3.85 GiB $\rightarrow$ **1.507 GiB**).
- **Configurable Memory Sizing (`kachedb-server`, `docker/`):**
  - Configured `--pool-mb 256` default baseline matching Redis and Dragonfly 1.0 GB memory footprints.

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
