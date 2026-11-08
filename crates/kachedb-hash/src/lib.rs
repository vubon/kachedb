//! `kachedb-hash` — SIMD-accelerated Swiss Table O(1) hash index for KacheDB.
//!
//! This crate implements the **Hash Engine** from the KacheDB architecture blueprint:
//!
//! > _"Implements a SIMD-accelerated Swiss Table hash map for microsecond O(1)
//! > GET/SET operations."_
//!
//! # Modules
//!
//! - **[`entry`]** — `HashEntry`: 64-byte aligned slot with S3-FIFO access bit.
//! - **[`table`]** — `SwissTable`: open-addressed hash map with H2 fingerprint
//!   probing, auto-resize at 87.5% load, and tombstone deletion.
//!
//! # Quick Start
//!
//! ```rust
//! use kachedb_hash::{SwissTable, hash_key};
//! use kachedb_core::SlabBlockId;
//!
//! let mut table = SwissTable::with_capacity(1024);
//! let hash = hash_key(b"user:session:42");
//! table.insert(hash, SlabBlockId(7), 512).unwrap();
//!
//! if let Some(entry) = table.lookup(hash) {
//!     println!("slab_block_id = {:?}, value_len = {}", entry.slab_block_id, entry.value_len);
//! }
//! ```

pub mod entry;
pub mod glob;
pub mod hash_frame;
pub mod sharded;
pub mod table;

pub use entry::{
    ACCESS_BIT_ACCESSED, HashEntry, INLINE_KEY_CAPACITY, TableEntry, VALUE_TYPE_HASH,
    VALUE_TYPE_STRING,
};
pub use glob::{CompiledPattern, MAX_PATTERN_LEN, glob_match};
pub use hash_frame::{FrameError, HashSlotFrame, hash_field};
pub use sharded::{DEFAULT_SCAN_COUNT, MAX_SCAN_COUNT, NUM_SHARDS, ShardedSwissTable};
pub use table::{SwissTable, hash_key, next_bit_reversed_cursor};
