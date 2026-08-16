# KacheDB

> **Ultra-Fast, Zero-Copy Dual-Engine In-Memory Database for Microsecond Application Caching and High-Throughput LLM KV-Cache Offloading.**

[![License: Apache-2.0 / MIT](https://img.shields.io/badge/license-Apache--2.0%2FMIT-blue.svg)](LICENSE)
[![Language: Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)

---

## Architecture Overview

KacheDB is a shared-nothing, thread-per-core in-memory storage engine that bridges two worlds:

| Workload | Engine | Latency Target |
|---|---|---|
| App Cache (Redis-compatible) | SIMD Swiss-table + S3-FIFO eviction | < 1 µs P99 |
| LLM KV-Cache Offloading | `&[u32]` Radix Prefix Tree + PagedAttention Blocks | 5x–15x TTFT reduction |

## Workspace Crates

| Crate | Role |
|---|---|
| `kachedb-core` | 64-byte aligned Megaslab arena allocator & memory manager |
| `kachedb-hash` | SIMD-accelerated Swiss Table O(1) hash index |
| `kachedb-radix` | `&[u32]` token prefix tree with bottom-up LRU eviction |
| `kachedb-shm` | Zero-copy POSIX `/dev/shm` SPSC ring buffer IPC |
| `kachedb-net` | Linux `io_uring` async TCP engine with registered buffers |
| `kachedb-proto-resp` | Zero-alloc RESP2/RESP3 streaming parser |
| `kachedb-proto-tensor` | Tensor block descriptors & PagedAttention layout |
| `kachedb-server` | Unified daemon runtime |
| `kachedb-cli` | Interactive admin CLI |

## Getting Started

```bash
# Build all crates
cargo build --workspace

# Run all tests
cargo test --workspace

# Run micro-benchmarks (Criterion)
cargo bench --workspace
```

## Roadmap

- **Phase 0:** `kachedb-core` slab allocator + `kachedb-hash` Swiss Table
- **Phase 1:** `kachedb-radix` + `kachedb-shm` zero-copy IPC
- **Phase 2:** `kachedb-net` io_uring TCP engine + RESP3 parser
- **Phase 3:** vLLM/SGLang integration + public launch

## License

Dual-licensed under [Apache-2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT).
