//! `kachedb-core` — Core memory subsystem for KacheDB.
//!
//! This crate provides the foundational memory primitives used by all other
//! KacheDB crates:
//!
//! - **[`slab`]** — Size class definitions (`SlabClassType`) and constants
//!   (`CACHE_LINE_BYTES`, `MEGASLAB_BYTES`).
//! - **[`arena`]** — `MegaslabArena`: single 2 MB OS page subdivided into
//!   fixed-size cache-line-aligned slots.
//! - **[`pool`]** — `SlabPool`: per-core arena manager with soft quota
//!   enforcement between App Cache and LLM Tensor workloads.
//! - **[`affinity`]** — Thread-to-core pinning with compile-time platform
//!   dispatch (Linux: `sched_setaffinity`, macOS: no-op).
//! - **[`error`]** — `CoreError`: unified error type for the memory subsystem.
//!
//! # Design Invariants
//!
//! 1. **No runtime `malloc`/`free` during request execution.** All memory is
//!    pre-allocated in 2 MB Megaslabs at startup and recycled via free-lists.
//! 2. **Cache-line isolation.** Every hot struct (`MegaslabHeader`, slot
//!    boundaries) is 64-byte aligned to prevent false sharing.
//! 3. **Single-owner per core.** `SlabPool` and `MegaslabArena` are `Send`
//!    but not `Sync`; cross-core access is mediated by SPSC channels in
//!    `kachedb-net`.

pub mod affinity;
pub mod arena;
pub mod error;
pub mod pool;
pub mod quota;
pub mod slab;

// ─── Convenience re-exports ───────────────────────────────────────────────────

pub use affinity::pin_current_thread_to_core;
pub use arena::{MegaslabArena, SlabBlockId};
pub use error::CoreError;
pub use pool::SlabPool;
pub use slab::{
    CACHE_LINE_BYTES, MEGASLAB_BYTES, MEGASLAB_HEADER_BYTES, MEGASLAB_PAYLOAD_BYTES,
    SlabClass, SlabClassType,
};
