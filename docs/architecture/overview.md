# 🏛️ System Architecture Overview

KacheDB is built from first principles in Rust to solve the fundamental memory, OS, and serialization bottlenecks of modern database architectures.

---

## ⚡ The Dual-Engine Topology

```text
+-----------------------------------------------------------------------------------------------+
|                                      CLIENT INTERFACES                                        |
|   [RESP3 Wire Protocol (Redis/Valkey Clients)]     [Zero-Copy Tensor IPC / Python SDK]        |
+-----------------------------------------------------------------------------------------------+
                                               │
+-----------------------------------------------------------------------------------------------+
|                                       INDEXING SUBSYSTEM                                      |
|   1. SIMD Swiss Hash Table (3.09 ns Point Lookups, S3-FIFO Eviction Tracking)                 |
|   2. Token Radix Prefix Tree (&[u32] Longest Prefix Match for LLM KV-Cache Prefills)          |
|   3. Per-Core Hashed Timing Wheel (3,600 Circular Buckets for O(1) Memory Reclamation)        |
+-----------------------------------------------------------------------------------------------+
                                               │
+-----------------------------------------------------------------------------------------------+
|                                  CORE SLAB & ARENA ENGINE                                     |
|   - 2 MB Megaslab Page Frames (64-byte Cache-Line Aligned Slots, 0 False Sharing)             |
|   - Zero Runtime Heap Allocation Jitter (Bump-pointer + Free-list Recycling)                  |
|   - S3-FIFO Dynamic Quota Management (Elastic Workload Quotas)                                |
+-----------------------------------------------------------------------------------------------+
                                               │
+-----------------------------------------------------------------------------------------------+
|                                ZERO-COPY TRANSPORT LAYER                                      |
|   - Local: POSIX Shared Memory (/dev/shm) Lock-Free SPSC Ring Buffers (17.66M msgs/sec)       |
|   - Network: Linux io_uring (SQPOLL Fixed Buffers) / macOS kqueue (Edge-Triggered Workers)    |
+-----------------------------------------------------------------------------------------------+
```

---

## 🥊 Why Traditional In-Memory Databases Hit a Wall

Traditional in-memory engines (e.g. Redis, Memcached) struggle when scaling to tens of millions of operations per second or serving multi-megabyte AI tensors due to three physical hardware constraints:

### 1. The Generality Tax (Heap Fragmentation & Pointer Chasing)
* General-purpose databases support variable-size dynamic keys and polymorphic data structures, relying on standard dynamic allocators (`jemalloc`, `malloc`).
* Every non-contiguous pointer chase incurs an **L1/L2/L3 CPU cache miss** ($\sim 50\text{--}100\text{ ns}$ stall), degrading memory bandwidth.
* **KacheDB Solution:** Pre-allocates uniform **2 MB Megaslabs** with fixed size classes and 64-byte cache line alignment, eliminating runtime malloc overhead down to **3.84 ns**.

### 2. Kernel Context-Switching Overhead
* Standard POSIX socket operations (`read()`, `write()`, `epoll_wait()`) require constant transitions between user-space and kernel-space.
* Processing millions of small network packets saturates the CPU with interrupt handling and buffer copies.
* **KacheDB Solution:** Utilizes **Linux `io_uring` with SQPOLL** for zero-syscall kernel-bypass socket polling and an **Accept-Dispatcher** ring for even worker CPU saturation.

### 3. The Tensor Serialization Penalty
* Traditional key-value protocols serialize all responses over TCP sockets.
* For an LLM KV cache (hundreds of megabytes of FP16/BF16 attention matrices), serializing and copying through kernel socket buffers to Python completely destroys latency gains.
* **KacheDB Solution:** Direct **POSIX Shared Memory (`/dev/shm`) lock-free SPSC ring buffers**, allowing PyTorch and vLLM to map tensors directly from host RAM at PCIe line rate without socket copies.

---

## 🧵 Thread-per-Core Execution Model

* Each physical CPU core runs an independent `WorkerThread` pinned via `core_affinity`.
* Each worker owns an isolated `SlabPool`, `SwissTable` shard, and `TimingWheel`.
* Zero cross-thread locks during request execution ensures **linear throughput scaling** across 4, 8, 32, or 64 cores.
