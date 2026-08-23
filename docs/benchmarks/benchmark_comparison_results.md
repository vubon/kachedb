# 🏎️ In-Memory Storage Engine Benchmark Scorecard

**Benchmark Date:** $(date -u +"%Y-%m-%d %H:%M:%S UTC")  
**Environment:** Docker Linux (Isolated 4 CPUs, 4 GB RAM per container)  
**Load Generator:** `memtier_benchmark` (50 clients, 4 threads, 16 pipeline, 64-byte value)

---

## 📊 Comparative Performance Results

| Storage Engine | SET (Writes/sec) | GET (Reads/sec) | Mixed 80/20 (QPS) | Latency P50 (ms) | Latency P99 (ms) | Peak RAM (RSS) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **REDIS** | 910792.35 | 954077.03 | 929137.69 | 3.34987 | 3.23100 | 1001MiB / 4GiB |
| **VALKEY** | 986162.37 | 1036892.48 | 964806.51 | 3.08194 | 3.07100 | 931.9MiB / 4GiB |
| **DRAGONFLY** | 1965010.82 | 2203727.56 | 2281983.57 | 1.44518 | 1.31100 | 1.054GiB / 4GiB |
| **KACHEDB** | 0.00 | 1521147.60 | 0.00 | 2.09627 | 1.99900 | 336.2MiB / 4GiB |

---

## 🔬 Key Architectural Differences

1. **KacheDB (Rust):** Thread-per-core topology with SIMD Swiss Table, S3-FIFO eviction, and 2 MB Megaslab bump allocator (zero runtime heap jitter).
2. **DragonflyDB (C++):** Multi-threaded fiber pool per core with `dashtable` segment locking.
3. **Redis 7.4 (C):** Single-threaded execution core with multi-threaded socket I/O (`io-threads 4`) and `jemalloc`.
4. **Valkey 8.0 (C):** Open-source Linux Foundation engine based on Redis with multi-threaded I/O.
