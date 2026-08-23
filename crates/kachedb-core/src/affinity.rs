//! `kachedb-core` — CPU core affinity and thread pinning.
//!
//! On Linux production systems, worker threads are pinned 1:1 to physical
//! CPU cores using `core_affinity::set_for_current()`. This eliminates CPU
//! migration jitter and preserves L1/L2 cache warmth across request batches.
//!
//! On macOS (development builds), hard core pinning is unavailable. The
//! function becomes a no-op, and the OS QoS scheduler manages scheduling.
//! All production performance targets apply to Linux only.

use crate::error::CoreError;

/// Attempts to pin the calling thread to the physical CPU core `core_id`.
///
/// # Linux (Production)
///
/// Calls `core_affinity::set_for_current()` which wraps `sched_setaffinity(2)`.
/// Returns an error if the specified core ID is invalid or the syscall fails.
///
/// # macOS / Other (Development)
///
/// No-op: logs a debug message and returns `Ok(())`. Core affinity is not
/// enforced; standard OS scheduling applies.
///
/// # Errors
///
/// Returns [`CoreError::AffinityFailed`] on Linux if pinning fails.
pub fn pin_current_thread_to_core(core_id: usize) -> Result<(), CoreError> {
    cfg_if::cfg_if! {
        if #[cfg(target_os = "linux")] {
            use core_affinity::CoreId;
            if core_affinity::set_for_current(CoreId { id: core_id }) {
                log::debug!("Thread pinned to physical core {core_id}");
                Ok(())
            } else {
                Err(CoreError::AffinityFailed {
                    core_id,
                    reason: "core_affinity::set_for_current() returned false".into(),
                })
            }
        } else {
            // macOS / BSD: hard core pinning is unavailable.
            log::debug!(
                "Thread pinning is a no-op on this platform (core_id={core_id}). \
                 OS QoS policy applies."
            );
            Ok(())
        }
    }
}

/// Returns the number of logical CPUs available to the process.
///
/// Used by the server runtime to determine the worker thread count.
pub fn available_parallelism() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_to_core_zero_succeeds() {
        // On macOS this is always Ok(()); on Linux it should succeed for core 0.
        let result = pin_current_thread_to_core(0);
        assert!(
            result.is_ok(),
            "pinning core 0 should not error: {result:?}"
        );
    }

    #[test]
    fn available_parallelism_nonzero() {
        assert!(available_parallelism() >= 1);
    }
}
