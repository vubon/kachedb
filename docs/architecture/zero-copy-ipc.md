# 🚀 Zero-Copy Shared Memory IPC

The `kachedb-shm` subsystem implements high-throughput, zero-copy inter-process communication (IPC) between the KacheDB daemon and Python / PyTorch / CUDA inference engines via POSIX Shared Memory (`/dev/shm`).

---

## ⚡ The Socket Serialization Bottleneck

Transferring large attention tensors (50 MB – 2 GB) over standard TCP loopback sockets suffers from severe throughput degradation:
1. Python creates a socket payload $\rightarrow$ serializes tensor buffers.
2. Kernel performs socket `send()` / context switch into kernel space $\rightarrow$ copies into socket ring buffers.
3. Daemon `recv()` / context switch into user space $\rightarrow$ deserializes data into memory.
4. Total latency penalty: **$15\text{--}40\text{ ms}$**, completely negating prefill savings.

---

## 🏛️ KacheDB Zero-Copy Architecture

```text
┌────────────────────────┐              ┌────────────────────────┐
│   KacheDB Rust Daemon  │              │  vLLM / PyTorch Worker │
│ ┌────────────────────┐ │              │ ┌────────────────────┐ │
│ │  Megaslab Memory   │ │              │ │   torch.Tensor     │ │
│ │ (Direct Slot Ptr)  │ │              │ │  (Zero-Copy View)  │ │
│ └─────────┬──────────┘ │              │ └──────────┬─────────┘ │
└───────────┼────────────┘              └────────────┼───────────┘
            │                                        │
            ▼                                        ▼
┌────────────────────────────────────────────────────────────────┐
│           POSIX Shared Memory Region (/dev/shm/kachedb_0)       │
│ ┌────────────────────────────────────────────────────────────┐ │
│ │ 192-byte Lock-Free SPSC Ring Header (3 Isolated 64B Lines) │ │
│ ├────────────────────────────────────────────────────────────┤ │
│ │ Cache-Line Aligned Megaslab Page Frames (Multi-GB Storage) │ │
│ └────────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
```

### 1. Lock-Free SPSC Ring Buffer
* Operates a Single-Producer Single-Consumer queue transferring `TensorBlockDescriptor` metadata frames.
* **Throughput:** Delivers **17.66 Million messages/sec** (56.6 ns per slot) across process boundaries.

### 2. Cache-Line Isolation (0 False Sharing)
* The 192-byte ring header is partitioned into three isolated 64-byte cache lines:
  - Cache Line 0: Read cursor & consumer state
  - Cache Line 1: Write cursor & producer state
  - Cache Line 2: Ring capacity & flags
* Multi-core reader and writer threads never trigger CPU L1 cache invalidation races.

### 3. Adaptive Spin-Then-Park Strategy
* Fast-path transfers use busy-spinning for $< 50\text{ ns}$ handoffs.
* Falls back to OS thread parking if the queue remains empty or full for $> 100\ \mu\text{s}$, conserving CPU cycles during idle periods.
