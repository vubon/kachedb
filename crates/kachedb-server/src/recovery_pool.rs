//! Startup allocations use the same pools that will serve traffic.

use kachedb_core::{CoreError, SlabBlockId, SlabClassType, SlabPool, pool::SlotAllocator};
use kachedb_hash::NUM_SHARDS;

pub struct RecoveryPools(pub Vec<SlabPool>);

impl RecoveryPools {
    pub fn new(workers: usize, bytes_per_worker: usize) -> Result<Self, CoreError> {
        let pools = (0..workers)
            .map(|core| SlabPool::new(core as u16, bytes_per_worker))
            .collect::<Result<_, _>>()?;
        Ok(Self(pools))
    }
}

impl SlotAllocator for RecoveryPools {
    fn reserve(&mut self, hash: u64, class: SlabClassType) -> Result<SlabBlockId, CoreError> {
        let count = self.0.len();
        if count == 0 {
            return Err(CoreError::PoolExhausted { class });
        }
        let preferred = (((hash >> 32) as usize) & (NUM_SHARDS - 1)) % count;
        for offset in 0..count {
            match self.0[(preferred + offset) % count].reserve_non_evicting(class) {
                Ok(block) => return Ok(block),
                Err(CoreError::PoolExhausted { .. }) => {}
                Err(error) => return Err(error),
            }
        }
        // Historical writes can leave empty arenas assigned to other classes.
        for offset in 0..count {
            let pool = &mut self.0[(preferred + offset) % count];
            if pool.reclaim_empty_arenas() > 0 {
                match pool.reserve_non_evicting(class) {
                    Ok(block) => return Ok(block),
                    Err(CoreError::PoolExhausted { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Err(CoreError::PoolExhausted { class })
    }

    fn deallocate(&mut self, block: SlabBlockId) -> Result<(), CoreError> {
        let core = kachedb_core::registry::resolve_owning_core(block)
            .ok_or(CoreError::InvalidBlockId { id: block.0 })?;
        self.0
            .get_mut(usize::from(core))
            .ok_or(CoreError::InvalidBlockId { id: block.0 })?
            .deallocate(block)
    }

    fn total_allocated_bytes(&self) -> usize {
        self.0.iter().map(SlabPool::total_allocated_bytes).sum()
    }

    fn arena_count(&self) -> usize {
        self.0.iter().map(SlabPool::arena_count).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skewed_keys_fall_back_to_other_worker_pools() {
        let mut pools = RecoveryPools::new(2, 6 * 1024 * 1024).unwrap();
        let mut owners = Vec::new();
        for _ in 0..3 {
            let block = pools.reserve(0, SlabClassType::Tensor2MB).unwrap();
            owners.push(kachedb_core::registry::resolve_owning_core(block).unwrap());
        }
        assert_eq!(owners, [0, 0, 1]);
    }
}
