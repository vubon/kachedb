//! `kachedb-shm` — Error types for the shared memory IPC subsystem.

use thiserror::Error;

/// Errors returned by the `kachedb-shm` subsystem.
#[derive(Debug, Error)]
pub enum ShmError {
    /// POSIX `shm_open` or `/dev/shm` file creation failed.
    #[error("failed to open shared memory region '{name}': {reason}")]
    OpenFailed { name: String, reason: String },

    /// `ftruncate` / `set_len` to the requested size failed.
    #[error("failed to resize shared memory '{name}' to {size} bytes: {reason}")]
    ResizeFailed {
        name: String,
        size: usize,
        reason: String,
    },

    /// `mmap` mapping failed.
    #[error("mmap failed for '{name}': {reason}")]
    MmapFailed { name: String, reason: String },

    /// The ring buffer capacity was zero or not a power of two.
    #[error("invalid ring capacity {capacity}: must be a non-zero power of two")]
    InvalidCapacity { capacity: u32 },

    /// The ring buffer is full — push would overwrite unread data.
    #[error("ring buffer is full (capacity={capacity})")]
    RingFull { capacity: u32 },

    /// The ring buffer is empty — no data to pop.
    #[error("ring buffer is empty")]
    RingEmpty,
}
