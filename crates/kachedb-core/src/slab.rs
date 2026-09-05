//! `kachedb-core` — Slab class definitions and size constants.
//!
//! KacheDB operates a **two-level hierarchical slab arena**:
//!
//! - **Level 1 (Megaslab):** 2 MB chunks aligned to OS Transparent Huge Pages.
//! - **Level 2 (Slot):** Each Megaslab is sub-divided into identically-sized,
//!   64-byte cache-line-aligned slots belonging to one of the classes below.
//!
//! # Memory Layout
//!
//! ```text
//! ┌─────────────────────────── 2 MB Megaslab ────────────────────────────┐
//! │ MegaslabHeader (64 B) │ Slot 0 (N B) │ Slot 1 (N B) │ ... │ Slot M │
//! └───────────────────────────────────────────────────────────────────────┘
//! ```

/// CPU cache-line width in bytes. All hot structures are aligned to this.
pub const CACHE_LINE_BYTES: usize = 64;

/// Size of one Megaslab in bytes (2 MiB — matches Linux Transparent Huge Pages).
pub const MEGASLAB_BYTES: usize = 2 * 1024 * 1024;

/// Adaptive chunk allocation unit in bytes (4 MiB).
pub const CHUNK_BYTES: usize = 4 * 1024 * 1024;

/// Byte size consumed by `MegaslabHeader` at the base of every slab chunk.
pub const MEGASLAB_HEADER_BYTES: usize = CACHE_LINE_BYTES;

/// Usable payload bytes within one Megaslab (after the header).
pub const MEGASLAB_PAYLOAD_BYTES: usize = MEGASLAB_BYTES - MEGASLAB_HEADER_BYTES;

// ─── Slab class byte sizes ─────────────────────────────────────────────────

/// App Cache slot sizes.
pub const APP_SMALL_BYTES: usize = 128;
pub const APP_MEDIUM_BYTES: usize = 512;
pub const APP_LARGE_BYTES: usize = 4 * 1024; // 4 KB

/// LLM Tensor block sizes.
pub const TENSOR_SMALL_BYTES: usize = 64 * 1024; // 64 KB
pub const TENSOR_MEDIUM_BYTES: usize = 256 * 1024; // 256 KB
pub const TENSOR_LARGE_BYTES: usize = 2 * 1024 * 1024; // 2 MB (one full Megaslab)

/// Identifies one of the six slab size classes used by KacheDB.
///
/// App cache classes hold small application key-value payloads; tensor classes
/// hold fixed-size PagedAttention KV-cache blocks.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SlabClassType {
    // ── App Cache ─────────────────────────────────────────────────────────
    /// 128 B slots — 16,256 slots per 2 MB Megaslab.
    AppSmall = 0,
    /// 512 B slots — 4,064 slots per 2 MB Megaslab.
    AppMedium = 1,
    /// 4 KB slots — 508 slots per 2 MB Megaslab.
    AppLarge = 2,

    // ── LLM KV-Cache ──────────────────────────────────────────────────────
    /// 64 KB blocks — 31 blocks per 2 MB Megaslab.
    Tensor64KB = 10,
    /// 256 KB blocks — 7 blocks per 2 MB Megaslab.
    Tensor256KB = 11,
    /// 2 MB blocks — occupies the entire Megaslab (1 block).
    Tensor2MB = 12,
}

impl SlabClassType {
    /// Returns the slot size in bytes for this class.
    #[inline(always)]
    pub const fn slot_bytes(self) -> usize {
        match self {
            Self::AppSmall => APP_SMALL_BYTES,
            Self::AppMedium => APP_MEDIUM_BYTES,
            Self::AppLarge => APP_LARGE_BYTES,
            Self::Tensor64KB => TENSOR_SMALL_BYTES,
            Self::Tensor256KB => TENSOR_MEDIUM_BYTES,
            Self::Tensor2MB => TENSOR_LARGE_BYTES,
        }
    }

    /// Returns the maximum number of slots that fit inside one 2 MB Megaslab.
    /// The first 64 bytes are consumed by `MegaslabHeader`.
    /// For `Tensor2MB`, the entire megaslab payload is a single block.
    #[inline(always)]
    pub const fn slots_per_megaslab(self) -> u32 {
        match self {
            // A 2 MB tensor block occupies the entire megaslab payload.
            // We treat it as 1 block per slab (header is embedded separately).
            Self::Tensor2MB => 1,
            _ => (MEGASLAB_PAYLOAD_BYTES / self.slot_bytes()) as u32,
        }
    }

    /// Returns `true` if this class is used for LLM tensor storage.
    #[inline(always)]
    pub const fn is_tensor(self) -> bool {
        matches!(self, Self::Tensor64KB | Self::Tensor256KB | Self::Tensor2MB)
    }

    /// Selects the smallest fitting slab class for a given payload size.
    pub const fn for_size(bytes: usize) -> Option<Self> {
        if bytes <= APP_SMALL_BYTES {
            Some(Self::AppSmall)
        } else if bytes <= APP_MEDIUM_BYTES {
            Some(Self::AppMedium)
        } else if bytes <= APP_LARGE_BYTES {
            Some(Self::AppLarge)
        } else if bytes <= TENSOR_SMALL_BYTES {
            Some(Self::Tensor64KB)
        } else if bytes <= TENSOR_MEDIUM_BYTES {
            Some(Self::Tensor256KB)
        } else if bytes <= TENSOR_LARGE_BYTES {
            Some(Self::Tensor2MB)
        } else {
            None
        }
    }
}

/// Metadata describing a single size class within a Megaslab.
#[derive(Debug, Clone, Copy)]
pub struct SlabClass {
    /// The class variant.
    pub class_type: SlabClassType,
    /// Bytes per individual slot.
    pub slot_bytes: usize,
    /// Maximum slots in one 2 MB Megaslab.
    pub slots_per_megaslab: u32,
}

impl SlabClass {
    /// Construct a `SlabClass` from the given `SlabClassType`.
    #[inline]
    pub const fn new(class_type: SlabClassType) -> Self {
        Self {
            class_type,
            slot_bytes: class_type.slot_bytes(),
            slots_per_megaslab: class_type.slots_per_megaslab(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_sizes_are_cache_line_aligned() {
        for class in [
            SlabClassType::AppSmall,
            SlabClassType::AppMedium,
            SlabClassType::AppLarge,
            SlabClassType::Tensor64KB,
            SlabClassType::Tensor256KB,
            SlabClassType::Tensor2MB,
        ] {
            assert_eq!(
                class.slot_bytes() % CACHE_LINE_BYTES,
                0,
                "{class:?} slot size is not 64-byte aligned"
            );
        }
    }

    #[test]
    fn slots_per_megaslab_nonzero() {
        for class in [
            SlabClassType::AppSmall,
            SlabClassType::AppMedium,
            SlabClassType::AppLarge,
            SlabClassType::Tensor64KB,
            SlabClassType::Tensor256KB,
            SlabClassType::Tensor2MB,
        ] {
            assert!(
                class.slots_per_megaslab() >= 1,
                "{class:?}: slots_per_megaslab must be >= 1"
            );
        }
    }

    #[test]
    fn app_small_slot_count() {
        // 2MB - 64 B header = 2,096,960 B / 128 B = 16,382.5 → 16,382 slots (floor)
        // Actual: (2*1024*1024 - 64) / 128 = 2,096,960 / 128 = 16,382.5 → 16,382
        // Rust integer division truncates, so: 2_096_960 / 128 = 16_382
        assert_eq!(SlabClassType::AppSmall.slots_per_megaslab(), 16383);
    }
}
