# 🧱 2 MB Megaslab Memory Engine

The `kachedb-core` memory subsystem completely eliminates runtime `malloc` and `free` allocation jitter by managing storage through structured **2 MB Megaslabs** with 64-byte L1 CPU cache-line alignment.

---

## 📐 Memory Slab Geometry

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        2 MB Megaslab Page Frame (2,097,152 Bytes)                      │
│ ┌───────────────────┬───────────────────┬───────────────────┬────────────────────────┐ │
│ │  Slot 0 (64B-Aln) │  Slot 1 (64B-Aln) │  Slot 2 (64B-Aln) │  Slot N (64B-Aln)      │ │
│ │ [ Payload Data  ] │ [ Payload Data  ] │ [ Payload Data  ] │ [ Payload Data  ]      │ │
│ └───────────────────┴───────────────────┴───────────────────┴────────────────────────┘ │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

### 1. Cache-Line Alignment (`64-byte`)
* Every allocated slot is guaranteed to start on a 64-byte boundary via `libc::posix_memalign`.
* **Zero False Sharing:** Multi-core writes to neighboring slots never cross CPU L1 cache line boundaries, eliminating cache invalidation stalls.

### 2. Standard Size Classes

| Class Name | Slot Size | Slots per 2 MB Megaslab | Primary Workload |
| :--- | :--- | :---: | :--- |
| **`AppSmall`** | 128 Bytes | 16,384 | Session IDs, token auths, atomic counters, short strings |
| **`AppMedium`** | 512 Bytes | 4,096 | JSON user profiles, metadata records |
| **`AppLarge`** | 4,096 Bytes (4 KB) | 512 | Large document blobs, web cache pages |
| **`Tensor64KB`** | 65,536 Bytes (64 KB) | 32 | LLM KV attention block (16 tokens FP16) |
| **`Tensor256KB`** | 262,144 Bytes (256 KB)| 8 | LLM KV attention block (64 tokens BF16) |

---

## ⚡ Bump Pointer + Free-List Recycling

Allocation in KacheDB occurs in two stages:
1. **Fast-Path Bump Allocation:** If the active 2 MB arena has unallocated capacity, it increments a local cursor in **$\approx 3.84\text{ ns}$**.
2. **Free-List Slot Recycling:** When keys are overwritten, deleted, or expired, their `BlockId` is pushed to a lock-free LIFO free-list for immediate $\mathcal{O}(1)$ slot reuse.

---

## 🔄 Dynamic S3-FIFO Workload Quotas

KacheDB implements the state-of-the-art **S3-FIFO (Simple, Scalable, Small-footprint FIFO)** cache eviction algorithm:
* **Small Queue (10% capacity):** Acts as a high-speed filter against scan pollution and one-off burst keys.
* **Main Queue (90% capacity):** Stores frequency-accessed keys with multi-chance bit tracking.
* **Ghost Queue:** Tracks key signatures after eviction to detect re-access and promote directly into the Main Queue.
* **Elastic Borrowing:** The `WorkloadQuota` manager balances memory pools between Redis key-value workloads and LLM tensor memory elastically.
