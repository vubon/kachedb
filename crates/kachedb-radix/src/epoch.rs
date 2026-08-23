//! `kachedb-radix` — Epoch-based RCU wrapper for concurrent multi-worker radix tree access.
//!
//! # Why Epoch-Based RCU?
//!
//! The base [`RadixTree`] is single-threaded per core. For multi-worker LLM
//! inference where dozens of decoding threads must simultaneously resolve prompt
//! prefixes against a shared KV-cache index, [`EpochTree`] provides:
//!
//! - **Zero-cost reads**: Readers execute a single atomic pointer load (~1 ns).
//! - **No reader blocking**: Writers swap the tree snapshot atomically;
//!   readers in progress against the old snapshot finish without interruption.
//! - **Automatic memory reclamation**: The old `Arc<RadixTree>` is freed
//!   automatically once all concurrent readers release their guards.
//!
//! # Usage Pattern
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use kachedb_radix::epoch::EpochTree;
//!
//! // Shared across all worker threads
//! let epoch_tree = EpochTree::new();
//!
//! // Reader thread (zero-cost)
//! let guard = epoch_tree.read();
//! let result = guard.lookup(&tokens)?;
//! drop(guard); // Arc refcount decremented; old tree freed if last reader
//!
//! // Writer thread (Core 0 — after building updated snapshot)
//! let mut new_tree = epoch_tree.read().as_ref().clone();
//! new_tree.insert(&new_tokens, slab_block_id)?;
//! epoch_tree.install_new_version(new_tree);
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;

use crate::tree::RadixTree;

// ─── EpochTree ────────────────────────────────────────────────────────────────

/// A globally shared, epoch-versioned radix tree supporting zero-cost concurrent reads.
///
/// Multiple reader threads can call [`EpochTree::read()`] simultaneously with
/// no locking or CAS overhead on the hot path. A single designated writer thread
/// (typically Core 0 in `kachedb-server`) calls [`EpochTree::install_new_version()`]
/// to atomically replace the tree snapshot.
///
/// # Memory Safety
///
/// Old tree versions are freed automatically via `Arc` reference counting once
/// all reader guards holding a reference to that version have been dropped.
/// This is the fundamental RCU safety guarantee — no reader ever accesses freed memory.
pub struct EpochTree {
    /// Current epoch — incremented monotonically on every structural write.
    global_epoch: AtomicU64,
    /// The shared radix tree root, protected by epoch-aware RCU.
    /// `ArcSwap` provides lock-free atomic pointer load for readers.
    inner: ArcSwap<RadixTree>,
}

impl EpochTree {
    /// Creates a new `EpochTree` backed by an empty `RadixTree`.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            global_epoch: AtomicU64::new(0),
            inner: ArcSwap::from_pointee(RadixTree::new()),
        })
    }

    /// Creates a new `EpochTree` initialised with an existing tree.
    pub fn from_tree(tree: RadixTree) -> Arc<Self> {
        Arc::new(Self {
            global_epoch: AtomicU64::new(0),
            inner: ArcSwap::from_pointee(tree),
        })
    }

    /// Acquires a zero-cost read guard to the current tree snapshot.
    ///
    /// On the hot path, this is equivalent to a single atomic pointer load
    /// (~1 ns). No mutex, spinlock, or CAS loop is involved.
    ///
    /// The returned guard holds an `Arc` reference to the snapshot; the old
    /// tree is freed automatically once all guards referencing it are dropped.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let guard = epoch_tree.read();
    /// let result = guard.lookup(&tokens)?;
    /// // guard dropped here — Arc refcount decremented
    /// ```
    #[inline]
    pub fn read(&self) -> arc_swap::Guard<Arc<RadixTree>> {
        self.inner.load()
    }

    /// Atomically installs a new version of the radix tree.
    ///
    /// All new readers immediately see the new version. Readers currently
    /// holding a guard to the old snapshot finish their lookup uninterrupted —
    /// the old `Arc<RadixTree>` is freed only after the last such guard drops.
    ///
    /// This must only be called from the designated writer thread (Core 0).
    ///
    /// # Epoch Semantics
    ///
    /// The global epoch counter is incremented atomically on every install.
    /// Callers can use [`EpochTree::current_epoch()`] to detect if a snapshot
    /// has changed between two reads.
    pub fn install_new_version(&self, new_tree: RadixTree) {
        let prev_epoch = self.global_epoch.fetch_add(1, Ordering::AcqRel);
        self.inner.store(Arc::new(new_tree));
        log::debug!(
            "EpochTree: installed epoch {} → {}",
            prev_epoch,
            prev_epoch + 1
        );
    }

    /// Returns the current epoch counter value.
    ///
    /// Monotonically increases with each [`install_new_version()`] call.
    #[inline]
    pub fn current_epoch(&self) -> u64 {
        self.global_epoch.load(Ordering::Relaxed)
    }

    /// Returns the number of live nodes in the current snapshot.
    #[inline]
    pub fn node_count(&self) -> usize {
        self.inner.load().node_count()
    }
}

impl Default for EpochTree {
    fn default() -> Self {
        Self {
            global_epoch: AtomicU64::new(0),
            inner: ArcSwap::from_pointee(RadixTree::new()),
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    #[test]
    fn epoch_tree_starts_at_epoch_zero() {
        let tree = EpochTree::new();
        assert_eq!(tree.current_epoch(), 0);
    }

    #[test]
    fn install_increments_epoch() {
        let tree = EpochTree::new();
        tree.install_new_version(RadixTree::new());
        assert_eq!(tree.current_epoch(), 1);
        tree.install_new_version(RadixTree::new());
        assert_eq!(tree.current_epoch(), 2);
    }

    #[test]
    fn read_returns_current_snapshot() {
        let epoch_tree = EpochTree::new();
        let guard = epoch_tree.read();
        assert_eq!(guard.node_count(), 1); // root node
        drop(guard);
    }

    #[test]
    fn concurrent_readers_and_writer() {
        use std::sync::Arc;
        use std::thread;

        let epoch_tree = EpochTree::new();
        let shared = Arc::clone(&epoch_tree);

        // Spawn 8 reader threads
        let readers: Vec<_> = (0..8)
            .map(|_| {
                let tree = Arc::clone(&shared);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        let guard = tree.read();
                        let _ = guard.node_count();
                        drop(guard);
                    }
                })
            })
            .collect();

        // Writer installs 10 new versions while readers are running
        for _ in 0..10 {
            epoch_tree.install_new_version(RadixTree::new());
            std::thread::yield_now();
        }

        for r in readers {
            r.join().expect("reader thread panicked");
        }

        assert!(epoch_tree.current_epoch() >= 10);
    }

    #[test]
    fn reader_sees_old_snapshot_during_install() {
        let epoch_tree = EpochTree::new();

        // Take a guard (old snapshot)
        let guard = epoch_tree.read();
        let old_epoch = epoch_tree.current_epoch();

        // Writer installs a new version
        epoch_tree.install_new_version(RadixTree::new());

        // Guard still valid — no use-after-free
        let _ = guard.node_count();
        drop(guard);

        // New reads see the new epoch
        assert_eq!(epoch_tree.current_epoch(), old_epoch + 1);
    }

    #[test]
    fn insert_visible_after_install() {
        use kachedb_core::SlabBlockId;

        let epoch_tree = EpochTree::new();
        let tokens: SmallVec<[u32; 16]> = (0u32..16).collect();

        // Writer: clone, insert, install
        let mut new_tree = epoch_tree.read().as_ref().clone();
        new_tree
            .insert(&tokens, &[SlabBlockId(42)])
            .expect("insert should succeed");
        epoch_tree.install_new_version(new_tree);

        // Reader: new snapshot has the inserted node
        let guard = epoch_tree.read();
        let result = guard.lookup(&tokens).expect("lookup should succeed");
        assert_eq!(result.matched_tokens, 16);
        assert!(!result.slab_block_ids.is_empty());
    }
}
