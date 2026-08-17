//! `kachedb-core` — Per-workload quota budget tracker (Improvement 4).
//!
//! Tracks the megaslab budget for App Cache and Tensor Cache workloads,
//! enforcing a soft target ratio with an elastic borrowing ceiling.

/// Tracks the megaslab budget for one workload (App Cache or Tensor Cache).
///
/// # Quota Model
///
/// ```text
/// ┌── Target (soft)  ──►  20% / 80% default split
/// └── Ceiling (hard) ──►  50% / 95% elastic limit (can borrow from unassigned pool)
/// ```
#[derive(Debug, Clone)]
pub struct WorkloadQuota {
    /// Target (soft) megaslab count for this workload.
    pub target: usize,
    /// Hard ceiling — maximum megaslabs this workload may claim.
    pub ceiling: usize,
    /// Currently claimed megaslab count.
    pub claimed: usize,
}

impl WorkloadQuota {
    /// Creates a `WorkloadQuota` from total megaslab count and ratio parameters.
    pub fn new(total_megaslabs: usize, target_ratio: f64, ceiling_ratio: f64) -> Self {
        Self {
            target: ((total_megaslabs as f64) * target_ratio).floor() as usize,
            ceiling: ((total_megaslabs as f64) * ceiling_ratio).floor() as usize,
            claimed: 0,
        }
    }

    /// Returns `true` if this workload has remaining elastic headroom below its ceiling.
    #[inline(always)]
    pub fn can_borrow(&self) -> bool {
        self.claimed < self.ceiling
    }

    /// Returns the number of megaslabs claimed above the soft target.
    /// Used by S3-FIFO reclamation to identify over-allocated workloads.
    #[inline(always)]
    pub fn surplus_above_target(&self) -> usize {
        self.claimed.saturating_sub(self.target)
    }

    /// Atomically claims one megaslab from this workload's quota.
    #[inline(always)]
    pub fn claim_one(&mut self) {
        self.claimed += 1;
    }

    /// Releases one megaslab back from this workload's quota.
    #[inline(always)]
    pub fn release_one(&mut self) {
        self.claimed = self.claimed.saturating_sub(1);
    }
}

/// Snapshot of current quota utilisation for both workloads.
/// Returned by `SlabPool::quota_snapshot()` for monitoring and server INFO.
#[derive(Debug, Clone, Copy)]
pub struct QuotaSnapshot {
    pub app_claimed: usize,
    pub app_target: usize,
    pub app_ceiling: usize,
    pub tensor_claimed: usize,
    pub tensor_target: usize,
    pub tensor_ceiling: usize,
    pub total_megaslabs: usize,
    /// Megaslabs not yet claimed by either workload.
    pub unassigned: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quota_new_calculates_correct_ratios() {
        let q = WorkloadQuota::new(100, 0.20, 0.50);
        assert_eq!(q.target, 20);
        assert_eq!(q.ceiling, 50);
        assert_eq!(q.claimed, 0);
    }

    #[test]
    fn can_borrow_respects_ceiling() {
        let mut q = WorkloadQuota::new(100, 0.20, 0.50);
        for _ in 0..50 {
            assert!(q.can_borrow());
            q.claim_one();
        }
        assert!(!q.can_borrow()); // at ceiling
    }

    #[test]
    fn surplus_above_target() {
        let mut q = WorkloadQuota::new(100, 0.20, 0.50);
        for _ in 0..30 {
            q.claim_one();
        }
        assert_eq!(q.surplus_above_target(), 10); // 30 - 20 = 10 above target
    }
}
