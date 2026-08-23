# RFC 0001: Time-To-Live (TTL) & Memory Expiration Architecture

**RFC Number:** `0001`  
**Title:** Time-To-Live (TTL) & Memory Expiration Architecture  
**Author(s):** Vubon Roy & KacheDB Core Team  
**Status:** Approved & Implemented  
**Target Module(s):** `kachedb-core` / `kachedb-hash` / `kachedb-net`  

---

## 1. Summary

This RFC specifies the dual-engine expiration subsystem for KacheDB, supporting second-level (`EX`) and millisecond-level (`PX`) TTL parameters for `SET` commands without degrading the sub-4ns single-core lookup pipeline.

---

## 2. Motivation

A high-performance in-memory cache requires deterministic expiration semantics for two primary operational profiles:
1. **Application Caching:** Ephemeral keys (e.g. auth tokens, rate-limit counters, API response fragments) require precise second- or millisecond-level TTL enforcement without degrading the sub-10ns single-core lookup pipeline.
2. **LLM KV-Cache Sessions:** Multi-turn inference sessions (e.g. chat histories, agent workflows) left idle by clients must expire automatically, releasing multi-megabyte tensor slabs from RAM back to the memory pool without blocking active GPU inference jobs.

---

## 3. Evaluation of Approaches

| Strategy | Write Cost | Read Hot-Path Cost | Background Scan Cost | Memory Contention | Verdict |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Global Min-Heap** | $\mathcal{O}(\log N)$ | Zero | $\mathcal{O}(1)$ top check | **High** (Global locks / CAS) | ❌ Rejected |
| **Probabilistic Sampling** | $\mathcal{O}(1)$ | Zero | $\mathcal{O}(K)$ random sample | **Medium** (CPU probing) | ❌ Rejected |
| **Lazy Expiration Only** | $\mathcal{O}(1)$ | $<0.5\text{ ns}$ | None | **Zero** | ❌ Rejected (Memory leaks) |
| **Dual Engine: Lazy + Timing Wheel** | **$\mathcal{O}(1)$** | **$<0.5\text{ ns}$** | **$\mathcal{O}(1)$ per bucket** | **Zero (Thread-isolated)** | ✅ **Selected for KacheDB** |

---

## 4. Detailed Design

### 4.1 Memory Layout & 64-Byte Cache-Line Invariant
`expire_at_secs: u32` is packed directly into `HashEntry` padding (47B $\rightarrow$ 43B):

```rust
#[repr(C, align(64))]
pub struct HashEntry {
    pub key_hash: u64,
    pub slab_block_id: SlabBlockId,
    pub value_len: u32,
    pub expire_at_secs: u32,       // 0 = persistent / no TTL
    pub access_flags: AtomicU8,     // S3-FIFO frequency bit
    _pad: [u8; 43],                 // 64 bytes total
}

const _: () = assert!(std::mem::size_of::<HashEntry>() == 64);
```

### 4.2 Per-Core Hashed Timing Wheel (`kachedb-core`)
- 3,600 one-second circular buckets (`WHEEL_BUCKETS = 3600`) representing a 1-hour circular resolution ring.
- `schedule(slot_id, expire_at_sec)` places handles in $\mathcal{O}(1)$ time.
- `advance_to(now_sec, &mut slab_pool)` batch-reclaims expired slots during idle event-loop ticks.

---

## 5. Verification & Benchmark Impact

- `swiss_table::lookup hit`: **3.09 ns** (0.0 ns regression vs pre-TTL 3.07 ns).
- `swiss_table::insert 1M keys`: **46.6 ms** (−24.8% vs initial baseline).
- Live Redis `SET ... EX 2` integration verified on macOS (`mio`) and Linux (`io_uring`).
