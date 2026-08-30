# KacheDB Documentation

<p align="center">
  <img src="./assets/logo.svg" alt="KacheDB Logo" width="160"/>
</p>

<p align="center">
  <strong>The High-Performance, Zero-Copy In-Memory Engine for Redis-Compatible Caching & LLM KV-Cache Offloading</strong>
</p>

---

## 📖 Welcome to the KacheDB Documentation

**KacheDB** is an open-source, next-generation in-memory storage engine engineered from first principles in Rust. It bridges the gap between traditional microsecond application caching (**Redis / Valkey** workloads) and multi-gigabyte tensor state offloading for Large Language Model (LLM) inference engines (**vLLM, SGLang, PyTorch**).

---

## 🗺️ Documentation Sitemap

### 🚀 Getting Started
* [**Quickstart Guide**](./getting-started/quickstart.md): Build from source, run via Cargo or Docker, and query via CLI.
* [**kachedb-cli User Guide**](./getting-started/kachedb-cli.md): Interactive terminal REPL and built-in throughput benchmark harness.
* [**Server Configuration & Tuning**](./getting-started/configuration.md): Worker threads, CPU pinning, memory pool sizing, and kernel bypass settings.

---

### ⚡ Command Reference
* [**Core Key-Value Commands**](./commands/core-kv.md): `GET`, `SET`, `MGET`, `MSET`, `DEL`, `EXISTS`, `INCR`, `DECR`, `APPEND`, `STRLEN`, `PING`.
* [**TTL & Key Lifecycle Commands**](./commands/ttl-lifecycle.md): `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, and the background Timing Wheel.
* [**SIMD Semantic Vector Commands**](./commands/vector-search.md): `VADD`, `VSEARCH`, `VDEL`, `VSTATS`, ARM NEON / AVX2 kernels, and similarity thresholds.
* [**Server Observability & Introspection**](./commands/server-introspection.md): `INFO`, `COMMAND DOCS`, `HELLO 2/3`, `CLIENT SETNAME/GETNAME/ID/LIST`, `QUIT`.

---

### 🏛️ System Architecture
* [**System Architecture Overview**](./architecture/overview.md): High-level system design, thread-per-core model, and the physical memory hierarchy.
* [**2 MB Megaslab Memory Engine**](./architecture/memory-engine.md): Slotted slab bump allocator, 64-byte cache-line alignment, and S3-FIFO quota manager.
* [**Token Radix Prefix Tree**](./architecture/radix-prefix-tree.md): Hierarchical `&[u32]` token prefix tree, sub-microsecond prefill lookup, and Epoch RCU concurrency.
* [**Zero-Copy Shared Memory IPC**](./architecture/zero-copy-ipc.md): POSIX `/dev/shm` lock-free ring buffers and PCIe line-rate tensor sharing.

---

### 🤖 Production Integration Guides
* [**vLLM Integration Guide**](./guides/vllm-integration.md): Drop-in PagedAttention KV connector for vLLM inference servers.
* [**SGLang Integration Guide**](./guides/sglang-integration.md): RadixAttention tree-branching prefill offloading with SGLang.
* [**Semantic Caching Guide**](./guides/semantic-caching.md): High-throughput sync and async prompt caching with `kachedb-py` and FastEmbed/HuggingFace.

---

### 📊 Performance & Benchmarks
* [**Consolidated Master Benchmark Report**](./benchmark_report.md): Criterion micro-benchmarks, latency percentiles, and hardware comparisons against Redis 7.4, Valkey 8.0, and DragonflyDB.
* [**Benchmark Artifacts & Logs**](./benchmarks/): Raw test outputs, multi-phase scaling reports, and reproducibility scripts.
