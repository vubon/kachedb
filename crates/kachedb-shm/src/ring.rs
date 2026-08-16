//! `kachedb-shm` — SPSC ring buffer header residing in shared memory.
//!
//! The `ShmRingHeader` is placed at the very start of the `/dev/shm` region.
//! It contains three isolated cache lines:
//!
//! ```text
//! Cache line 0 (Producer-owned): head  (u32) + 60 B padding
//! Cache line 1 (Consumer-owned): tail  (u32) + consumer_state (u32) + 56 B padding
//! Cache line 2 (Static):         capacity (u32) + eventfd_fd (i32) + 56 B padding
//! ```
//!
//! Separating `head` and `tail` across different cache lines prevents
//! **false sharing** — the producer and consumer do not invalidate each
//! other's L1 cache lines on every update.

use std::sync::atomic::{AtomicI32, AtomicU32, Ordering};

use kachedb_core::CACHE_LINE_BYTES;

// ─── ConsumerState ────────────────────────────────────────────────────────────

/// Synchronization state of the ring buffer consumer.
///
/// The producer reads this before deciding whether to issue a syscall wakeup.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerState {
    /// Consumer is in the active busy-spin loop. No syscall needed on push.
    ActiveSpin = 0,
    /// Consumer is parked in an OS sleep (`futex` / `Condvar`).
    /// Producer must issue a wakeup when pushing new data.
    Parked = 1,
}

// ─── ShmRingHeader ────────────────────────────────────────────────────────────

/// Three-cache-line header for the lock-free SPSC ring buffer in shared memory.
///
/// # Invariants
///
/// - `head` is written only by the **producer** (KacheDB daemon).
/// - `tail` and `consumer_state` are written only by the **consumer** (Python worker).
/// - `capacity` and `eventfd_fd` are written once at initialisation and then read-only.
///
/// # Size
///
/// `3 × 64 = 192 bytes`. Must not cross a page boundary in `/dev/shm`.
#[repr(C, align(64))]
pub struct ShmRingHeader {
    // ── Cache line 0: Producer cache line ────────────────────────────────────
    /// Producer write index (monotonically increasing; wrap with `% capacity`).
    pub head: AtomicU32,
    _pad0: [u8; CACHE_LINE_BYTES - 4],

    // ── Cache line 1: Consumer cache line ─────────────────────────────────────
    /// Consumer read index.
    pub tail: AtomicU32,
    /// Current consumer synchronization state (`ConsumerState` discriminant).
    pub consumer_state: AtomicU32,
    _pad1: [u8; CACHE_LINE_BYTES - 8],

    // ── Cache line 2: Static geometry ─────────────────────────────────────────
    /// Maximum number of slots in the ring (must be a power of two).
    pub capacity: u32,
    /// Linux `eventfd` file descriptor for consumer wakeup.
    /// Set to -1 on macOS (fallback uses `Condvar`).
    pub eventfd_fd: AtomicI32,
    _pad2: [u8; CACHE_LINE_BYTES - 8],
}

const _: () = assert!(
    std::mem::size_of::<ShmRingHeader>() == 3 * CACHE_LINE_BYTES,
    "ShmRingHeader must be exactly 192 bytes (3 cache lines)"
);

impl ShmRingHeader {
    /// Initialises the header in-place at a raw pointer (for shared memory mapping).
    ///
    /// # Safety
    ///
    /// `ptr` must point to at least `size_of::<ShmRingHeader>()` bytes of
    /// writable, correctly aligned shared memory.
    pub unsafe fn init_at(ptr: *mut Self, capacity: u32, eventfd_fd: i32) {
        unsafe {
            (*ptr).head = AtomicU32::new(0);
            (*ptr).tail = AtomicU32::new(0);
            (*ptr).consumer_state = AtomicU32::new(ConsumerState::ActiveSpin as u32);
            (*ptr).capacity = capacity;
            (*ptr).eventfd_fd = AtomicI32::new(eventfd_fd);
            // Zero the padding arrays.
            std::ptr::write_bytes(
                (*ptr)._pad0.as_mut_ptr(),
                0,
                CACHE_LINE_BYTES - 4,
            );
            std::ptr::write_bytes(
                (*ptr)._pad1.as_mut_ptr(),
                0,
                CACHE_LINE_BYTES - 8,
            );
            std::ptr::write_bytes(
                (*ptr)._pad2.as_mut_ptr(),
                0,
                CACHE_LINE_BYTES - 8,
            );
        }
    }

    /// Returns the current consumer synchronization state.
    #[inline(always)]
    pub fn consumer_state(&self) -> ConsumerState {
        match self.consumer_state.load(Ordering::Acquire) {
            1 => ConsumerState::Parked,
            _ => ConsumerState::ActiveSpin,
        }
    }

    /// Sets the consumer state (called by the consumer thread only).
    #[inline(always)]
    pub fn set_consumer_state(&self, state: ConsumerState) {
        self.consumer_state
            .store(state as u32, Ordering::Release);
    }

    /// Returns `true` if there is at least one slot available for writing.
    #[inline]
    pub fn has_space(&self) -> bool {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        (h.wrapping_sub(t)) < self.capacity
    }

    /// Returns `true` if there is at least one slot available for reading.
    #[inline]
    pub fn has_data(&self) -> bool {
        let h = self.head.load(Ordering::Acquire);
        let t = self.tail.load(Ordering::Acquire);
        h != t
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn ring_header_size_is_192_bytes() {
        assert_eq!(mem::size_of::<ShmRingHeader>(), 192);
    }

    #[test]
    fn ring_header_alignment_is_64_bytes() {
        assert_eq!(mem::align_of::<ShmRingHeader>(), 64);
    }
}
