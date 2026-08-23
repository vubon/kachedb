# Contributing to KacheDB

First off, thank you for considering contributing to **KacheDB**! 🎉

KacheDB is a high-performance in-memory caching engine designed for microsecond app caching and zero-copy LLM KV-cache offloading. We hold high engineering standards to maintain sub-nanosecond lookups and multi-million QPS throughput.

---

## 🧭 Code of Conduct

All contributors and maintainers are expected to follow our [Code of Conduct](CODE_OF_CONDUCT.md).

---

## 🛠️ Development Setup & Prerequisites

### Prerequisites
- **Rust 1.85+** (with `cargo`, `rustfmt`, `clippy`)
- **Git**
- *(Optional for Linux `io_uring`)*: Linux kernel 5.19+ or Docker with `--privileged` support.

### One-Command Quickstart
```bash
# Clone the repository
git clone https://github.com/vubon/kachedb.git
cd kachedb

# Build all workspace crates
cargo build --workspace

# Run all unit and integration tests (88+ tests)
cargo test --workspace
```

---

## 📐 Systems Engineering Standards

To maintain textbook-quality code and zero performance regressions, we adhere to strict systems engineering invariants:

### 1. Zero Runtime `malloc`/`free` on Hot Paths
- Hot-path requests (`GET`, `SET`, `PING`, prefix queries) must **never** allocate dynamically from the OS heap.
- All allocations are recycled via pre-allocated 2 MB `MegaslabArena` slots and free-lists in `kachedb-core`.

### 2. Strict 64-Byte Cache-Line Alignment
- Every hot struct (`HashEntry`, `AppCacheItemHeader`, `MegaslabHeader`, `TensorBlockDescriptor`) must be cache-line aligned:
  ```rust
  #[repr(C, align(64))]
  pub struct HashEntry { ... }
  ```
- Always include a compile-time static assertion verifying struct size:
  ```rust
  const _: () = assert!(std::mem::size_of::<HashEntry>() == 64);
  ```

### 3. Mandatory `// SAFETY:` Invariant Comments
Every `unsafe` block must be immediately preceded by a `// SAFETY:` comment explaining why pointer offsets, dereferences, or transmutations are guaranteed sound:
```rust
// SAFETY: slot_idx is strictly bounded by SLOTS_PER_MEGASLAB (< 16,384)
// and ptr is verified within the mapped 2 MB OS virtual address range.
unsafe {
    std::ptr::copy_nonoverlapping(src, dst, len);
}
```

### 4. Zero-Syscall Clock Policy
- Avoid calling `SystemTime::now()` on hot request lookup paths (~50 ns latency penalty).
- Cache and advance timestamps on the event-loop idle tick (`pool.tick_second(now_sec)`).

---

## 🧪 Pre-Commit Verification Gate

Before submitting a pull request, run the local verification suite:

```bash
# 1. Format check
cargo fmt --all -- --check

# 2. Clippy linter with strict warning enforcement
cargo clippy --workspace --all-targets -- -D warnings

# 3. Test suite
cargo test --workspace

# 4. Performance regression check (if modifying hot paths)
cargo bench --workspace
```

---

## 🚀 Pull Request Lifecycle

1. **Fork and Branch:** Create a branch from `main` (e.g. `feat/rdma-transport` or `fix/tombstone-reclaim`).
2. **Commit Conventions:** Write clear, concise commit messages (e.g. `feat(hash): add compact_one_group helper`).
3. **Open PR:** Fill out the [Pull Request Template](.github/PULL_REQUEST_TEMPLATE.md) and include any benchmark numbers.
4. **Code Review:** Core maintainers will review your PR within 48 hours.

---

## 📐 Request For Comments (RFC) Process

For major architectural proposals (e.g., adding a new network engine, changing the memory allocator, introducing new eviction algorithms):
1. Copy `docs/rfcs/0000-template.md` to `docs/rfcs/000X-my-proposal.md`.
2. Detail motivation, design, trade-offs, and alternatives.
3. Open an RFC pull request for community feedback.
