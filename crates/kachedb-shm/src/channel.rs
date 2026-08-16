//! `kachedb-shm` — High-level SPSC channel with adaptive spin-then-park.
//!
//! `ShmChannel` ties together:
//! - A [`ShmRegion`] (the physical shared memory page)
//! - A [`ShmRingHeader`] (SPSC control state at the base of the region)
//! - A data slot array (immediately after the header in the same region)
//!
//! # Synchronization Model (RFC 4 §1)
//!
//! ```text
//! Producer pushes ──────────────────────────────────────────────────────────┐
//!                                                                            │
//!                    ┌──────────────────────────────┐                        │
//!                    │  ShmRingHeader               │                        │
//!                    │  head (producer-owned CL)    │<── producer writes     │
//!                    │  tail (consumer-owned CL)    │<── consumer reads      │
//!                    │  consumer_state              │                        │
//!                    └──────────────────────────────┘                        │
//!                                                                            │
//! Consumer state machine:                                                    │
//!   1. ActiveSpin → poll head/tail atomics in tight loop (< 200 ns)         │
//!   2. After spin_limit → yield_now() (Exponential backoff)                 │
//!   3. After yield_limit → park (Condvar::wait / futex WAIT)               │
//!   4. Producer sees Parked → signal wakeup                                 │
//! ```

use std::mem::size_of;
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex};

use kachedb_core::SlabBlockId;
use kachedb_proto_tensor::TensorBlockDescriptor;

use crate::{
    error::ShmError,
    region::ShmRegion,
    ring::{ConsumerState, ShmRingHeader},
};

/// Number of busy-spin iterations before moving to the backoff phase.
const SPIN_LIMIT: u32 = 2048;
/// Number of `thread::yield_now()` calls before parking.
const YIELD_LIMIT: u32 = 16;

// ─── IpcSlot ──────────────────────────────────────────────────────────────────

/// One slot in the SPSC ring buffer.
///
/// Carries the lightweight control message between the KacheDB daemon (producer)
/// and the Python inference worker (consumer). The actual tensor payload is
/// read directly from the slab block in shared memory — this slot only carries
/// the metadata descriptor and block address.
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct IpcSlot {
    /// Descriptor describing the tensor shape, dtype, and payload location.
    pub descriptor: TensorBlockDescriptor,
    /// Opaque slab block identifier for the caller to resolve the slab address.
    pub slab_block_id: SlabBlockId,
    /// Sequence number for ordering validation.
    pub seq: u64,
    _pad: [u8; 56],
}

impl IpcSlot {
    /// Creates a new `IpcSlot`.
    pub fn new(descriptor: TensorBlockDescriptor, slab_block_id: SlabBlockId, seq: u64) -> Self {
        Self {
            descriptor,
            slab_block_id,
            seq,
            _pad: [0u8; 56],
        }
    }
}

// ─── ShmChannel ───────────────────────────────────────────────────────────────

/// Zero-copy IPC channel over POSIX shared memory.
///
/// Encapsulates one named SHM region divided as:
///
/// ```text
/// ┌──────────────────────────┐ offset 0
/// │  ShmRingHeader (192 B)   │
/// ├──────────────────────────┤ offset 192 (next 64-byte boundary)
/// │  IpcSlot[0]  (128 B)     │
/// │  IpcSlot[1]  (128 B)     │
/// │  ...                     │
/// │  IpcSlot[N-1] (128 B)    │
/// └──────────────────────────┘
/// ```
///
/// # Usage
///
/// **Daemon (producer):**
/// ```rust,ignore
/// let mut ch = ShmChannel::create("kachedb_0", 256)?;
/// ch.push(slot)?;
/// ```
///
/// **Python worker (consumer) via PyO3:**
/// ```python
/// # Attach to the same name:
/// # shm = mmap.mmap(-1, size, "kachedb_0")
/// # Read ShmRingHeader and IpcSlot array directly.
/// ```
pub struct ShmChannel {
    region: ShmRegion,
    capacity: u32,
    /// Condvar used on macOS (and as fallback on Linux) for consumer parking.
    park: Mutex<bool>,
    park_cv: Condvar,
}

impl ShmChannel {
    /// Minimum required byte size for one channel of `capacity` slots.
    pub fn required_bytes(capacity: u32) -> usize {
        // Header (192 B, padded to next 64-byte boundary) + slot array.
        let header_padded = next_align(size_of::<ShmRingHeader>(), 64);
        header_padded + (capacity as usize) * size_of::<IpcSlot>()
    }

    /// Creates a new shared memory channel (owner mode).
    ///
    /// The SHM region is created and the `ShmRingHeader` is initialised.
    /// `capacity` must be a non-zero power of two.
    pub fn create(name: &str, capacity: u32) -> Result<Self, ShmError> {
        if capacity == 0 || !capacity.is_power_of_two() {
            return Err(ShmError::InvalidCapacity { capacity });
        }

        let size = Self::required_bytes(capacity);
        let region = ShmRegion::open_or_create(name, size, true)?;

        // Initialise the header in-place.
        unsafe {
            let header_ptr = region.as_typed_ptr::<ShmRingHeader>();
            ShmRingHeader::init_at(header_ptr, capacity, -1);
        }

        log::debug!("ShmChannel::create: name={name} capacity={capacity} size={size}");

        Ok(Self {
            region,
            capacity,
            park: Mutex::new(false),
            park_cv: Condvar::new(),
        })
    }

    /// Attaches to an existing shared memory channel (non-owner mode).
    pub fn attach(name: &str, capacity: u32) -> Result<Self, ShmError> {
        let size = Self::required_bytes(capacity);
        let region = ShmRegion::open_or_create(name, size, false)?;

        Ok(Self {
            region,
            capacity,
            park: Mutex::new(false),
            park_cv: Condvar::new(),
        })
    }

    // ── Producer API ──────────────────────────────────────────────────────────

    /// Pushes one `IpcSlot` into the ring buffer.
    ///
    /// If the consumer is [`Parked`](ConsumerState::Parked), signals the
    /// `Condvar` to wake it up. If the consumer is spinning, no syscall is
    /// issued.
    ///
    /// # Errors
    ///
    /// Returns [`ShmError::RingFull`] if the ring has no space.
    pub fn push(&self, slot: IpcSlot) -> Result<(), ShmError> {
        let header = self.header();

        if !header.has_space() {
            return Err(ShmError::RingFull { capacity: self.capacity });
        }

        let head = header.head.load(Ordering::Relaxed);
        let idx = (head % self.capacity) as usize;

        // Write the slot into the ring data array.
        unsafe {
            let slot_ptr = self.slot_ptr(idx);
            slot_ptr.write(slot);
        }

        // Release store: the consumer will see the slot data before the head update.
        header.head.fetch_add(1, Ordering::Release);

        // Wake the consumer only if it has parked to save the syscall on the hot path.
        if header.consumer_state() == ConsumerState::Parked {
            let mut guard = self.park.lock().unwrap();
            *guard = true;
            self.park_cv.notify_one();
        }

        Ok(())
    }

    // ── Consumer API ──────────────────────────────────────────────────────────

    /// Pops one `IpcSlot` from the ring buffer (non-blocking).
    ///
    /// Returns [`ShmError::RingEmpty`] if no data is available.
    pub fn try_pop(&self) -> Result<IpcSlot, ShmError> {
        let header = self.header();

        if !header.has_data() {
            return Err(ShmError::RingEmpty);
        }

        let tail = header.tail.load(Ordering::Relaxed);
        let idx = (tail % self.capacity) as usize;

        // Acquire load: see all writes from the producer before the head update.
        let slot = unsafe { self.slot_ptr(idx).read() };
        header.tail.fetch_add(1, Ordering::Release);

        Ok(slot)
    }

    /// Pops one `IpcSlot`, blocking with adaptive spin-then-park.
    ///
    /// **Phase 1 — Active spin:** Polls atomics with `spin_loop()` hint.
    /// **Phase 2 — Backoff:** `thread::yield_now()` to release the core briefly.
    /// **Phase 3 — Park:** Waits on `Condvar` until the producer signals.
    pub fn pop_blocking(&self) -> IpcSlot {
        let header = self.header();
        let mut spin_count = 0u32;
        let mut yield_count = 0u32;

        loop {
            if let Ok(slot) = self.try_pop() {
                // Ensure consumer is marked as spinning (reset from parked state).
                header.set_consumer_state(ConsumerState::ActiveSpin);
                return slot;
            }

            if spin_count < SPIN_LIMIT {
                std::hint::spin_loop();
                spin_count += 1;
            } else if yield_count < YIELD_LIMIT {
                std::thread::yield_now();
                yield_count += 1;
            } else {
                // Park: mark state so producer issues wakeup.
                header.set_consumer_state(ConsumerState::Parked);
                let guard = self.park.lock().unwrap();
                let _guard = self
                    .park_cv
                    .wait_while(guard, |notified| !*notified)
                    .unwrap();
                // Reset for the next spin cycle.
                spin_count = 0;
                yield_count = 0;
            }
        }
    }

    // ── Introspection ─────────────────────────────────────────────────────────

    /// Returns the number of unconsumed slots currently in the ring.
    pub fn pending(&self) -> u32 {
        let h = self.header();
        h.head
            .load(Ordering::Acquire)
            .wrapping_sub(h.tail.load(Ordering::Acquire))
    }

    /// Returns `true` if the ring buffer is empty.
    pub fn is_empty(&self) -> bool {
        !self.header().has_data()
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn header(&self) -> &ShmRingHeader {
        unsafe { &*(self.region.as_ptr() as *const ShmRingHeader) }
    }

    unsafe fn slot_ptr(&self, idx: usize) -> *mut IpcSlot {
        let header_size = next_align(size_of::<ShmRingHeader>(), 64);
        unsafe {
            (self.region.as_ptr().add(header_size) as *mut IpcSlot).add(idx)
        }
    }
}

/// Rounds `value` up to the next multiple of `align`.
#[inline(always)]
const fn next_align(value: usize, align: usize) -> usize {
    (value + align - 1) & !(align - 1)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use kachedb_proto_tensor::{TensorBlockDescriptor, TensorDType};

    fn make_slot(seq: u64) -> IpcSlot {
        IpcSlot {
            descriptor: TensorBlockDescriptor::new(0, 32, 16, 8, 128, TensorDType::BF16, seq),
            slab_block_id: SlabBlockId(seq as u32),
            seq,
            _pad: [0u8; 56],
        }
    }

    #[test]
    fn create_push_pop() {
        let name = format!("kachedb_ch_{}", std::process::id());
        let ch = ShmChannel::create(&name, 16).unwrap();

        let slot = make_slot(42);
        ch.push(slot).unwrap();

        let received = ch.try_pop().unwrap();
        assert_eq!(received.seq, 42);
        assert_eq!(received.slab_block_id, SlabBlockId(42));
    }

    #[test]
    fn ring_reports_empty_after_drain() {
        let name = format!("kachedb_empty_{}", std::process::id());
        let ch = ShmChannel::create(&name, 8).unwrap();
        assert!(ch.is_empty());
        ch.push(make_slot(1)).unwrap();
        assert!(!ch.is_empty());
        ch.try_pop().unwrap();
        assert!(ch.is_empty());
    }

    #[test]
    fn ring_full_returns_error() {
        let name = format!("kachedb_full_{}", std::process::id());
        let ch = ShmChannel::create(&name, 4).unwrap();
        for i in 0..4 {
            ch.push(make_slot(i)).unwrap();
        }
        let result = ch.push(make_slot(99));
        assert!(matches!(result, Err(ShmError::RingFull { .. })));
    }

    #[test]
    fn pending_count_is_correct() {
        let name = format!("kachedb_pending_{}", std::process::id());
        let ch = ShmChannel::create(&name, 8).unwrap();
        ch.push(make_slot(1)).unwrap();
        ch.push(make_slot(2)).unwrap();
        assert_eq!(ch.pending(), 2);
        ch.try_pop().unwrap();
        assert_eq!(ch.pending(), 1);
    }

    #[test]
    fn required_bytes_nonzero() {
        assert!(ShmChannel::required_bytes(16) > 192);
    }
}
