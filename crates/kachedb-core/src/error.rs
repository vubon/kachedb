//! `kachedb-core` — Error types for the KacheDB core allocator.

use thiserror::Error;

/// Errors returned by the KacheDB core memory subsystem.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum CoreError {
    /// The requested slab class is exhausted; no free slots remain.
    #[error("slab pool exhausted for class {class:?}")]
    PoolExhausted { class: crate::slab::SlabClassType },

    /// The provided `SlabBlockId` does not belong to this pool.
    #[error("invalid slab block id: {id}")]
    InvalidBlockId { id: u32 },

    /// Memory allocation via the OS failed.
    #[error("OS memory allocation failed: {reason}")]
    OsAllocFailed { reason: String },

    /// Thread-to-core pinning failed (Linux only).
    #[error("failed to pin thread to core {core_id}: {reason}")]
    AffinityFailed { core_id: usize, reason: String },
}
