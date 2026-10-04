//! `kachedb-hash` — In-Slot Slotted Frame with Strict O(1) Open-Addressing Directory.
//!
//! Stores Redis Hash field-value pairs contiguously inside a single pre-allocated
//! Megaslab slot (128B, 512B, or 4096B).
//!
//! # Layout
//!
//! ```text
//!  slot (cap = 128 | 512 | 4096 bytes)
//! ┌────────────┬───────────────────────────┬───────────────┬───────────────────────────┐
//! │ Header (8) │ Payload → grows upward    │   free gap    │ ← Open-Addressing Buckets │
//! │            │ f1 v1 f2 v2 ... (+ dead)  │               │   (Power-of-2 K buckets)  │
//! └────────────┴───────────────────────────┴───────────────┴───────────────────────────┘
//! Offset 0     Offset 8                    Offset P        Offset (cap - 10*K)     Offset cap
//! ```

use smallvec::SmallVec;
use thiserror::Error;

use crate::table::hash_key;

pub const HEADER_SIZE: usize = 8;
pub const BUCKET_SIZE: usize = 10;
pub const EMPTY_FIELD_LEN: u16 = 0x0000;
pub const TOMBSTONE_FIELD_LEN: u16 = 0xFFFF;
pub const MAX_SLOT_CAPACITY: usize = 4096;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Error)]
pub enum FrameError {
    #[error("hash frame is full and cannot fit the requested entries")]
    FrameFull,
    #[error("corrupt or invalid hash frame layout")]
    CorruptFrame,
}

/// Helper to compute 32-bit field hash from 64-bit AHash.
#[inline(always)]
pub fn hash_field(field: &[u8]) -> u32 {
    let h = hash_key(field);
    (h ^ (h >> 32)) as u32
}

/// Decoded directory bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bucket {
    pub hash32: u32,
    pub offset: u16,
    pub field_len: u16,
    pub val_len: u16,
}

/// Pure in-memory slotted-frame operations with strict O(1) Open-Addressing Directory.
pub struct HashSlotFrame;

impl HashSlotFrame {
    /// Determines the initial bucket count K (power of 2) for a given slot capacity.
    #[inline(always)]
    pub fn default_k_for_capacity(cap: usize) -> usize {
        if cap <= 128 {
            8
        } else if cap <= 512 {
            16
        } else {
            32
        }
    }

    /// Initializes an empty `HashSlotFrame` in `slot`.
    pub fn init(slot: &mut [u8]) -> Result<(), FrameError> {
        let cap = slot.len();
        if !(64..=MAX_SLOT_CAPACITY).contains(&cap) {
            return Err(FrameError::CorruptFrame);
        }
        let k = Self::default_k_for_capacity(cap);
        let mask = (k - 1) as u16;

        // Header: live_count=0, bucket_mask=mask, payload_end=8, dead_bytes=0
        slot[0..2].copy_from_slice(&0u16.to_le_bytes());
        slot[2..4].copy_from_slice(&mask.to_le_bytes());
        slot[4..6].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
        slot[6..8].copy_from_slice(&0u16.to_le_bytes());

        // Zero out the directory buckets at the tail
        let dir_bytes = k * BUCKET_SIZE;
        if HEADER_SIZE + dir_bytes > cap {
            return Err(FrameError::FrameFull);
        }
        let dir_start = cap - dir_bytes;
        slot[dir_start..cap].fill(0);

        Ok(())
    }

    /// Reads header fields: `(live_count, bucket_mask, payload_end, dead_bytes)`.
    #[inline(always)]
    pub fn read_header(slot: &[u8]) -> Result<(u16, u16, u16, u16), FrameError> {
        if slot.len() < HEADER_SIZE {
            return Err(FrameError::CorruptFrame);
        }
        let live_count = u16::from_le_bytes(slot[0..2].try_into().unwrap());
        let bucket_mask = u16::from_le_bytes(slot[2..4].try_into().unwrap());
        let payload_end = u16::from_le_bytes(slot[4..6].try_into().unwrap());
        let dead_bytes = u16::from_le_bytes(slot[6..8].try_into().unwrap());

        let k = (bucket_mask as usize) + 1;
        if !k.is_power_of_two() || !(4..=256).contains(&k) {
            return Err(FrameError::CorruptFrame);
        }
        let dir_start = slot.len().saturating_sub(k * BUCKET_SIZE);
        if (payload_end as usize) < HEADER_SIZE || (payload_end as usize) > dir_start {
            return Err(FrameError::CorruptFrame);
        }

        Ok((live_count, bucket_mask, payload_end, dead_bytes))
    }

    /// Writes header fields back to slot.
    #[inline(always)]
    fn write_header(
        slot: &mut [u8],
        live_count: u16,
        bucket_mask: u16,
        payload_end: u16,
        dead_bytes: u16,
    ) {
        slot[0..2].copy_from_slice(&live_count.to_le_bytes());
        slot[2..4].copy_from_slice(&bucket_mask.to_le_bytes());
        slot[4..6].copy_from_slice(&payload_end.to_le_bytes());
        slot[6..8].copy_from_slice(&dead_bytes.to_le_bytes());
    }

    /// Reads a bucket by its index `0..K`.
    #[inline(always)]
    pub fn read_bucket(slot: &[u8], idx: usize) -> Bucket {
        let cap = slot.len();
        let b_off = cap - BUCKET_SIZE * (idx + 1);
        let hash32 = u32::from_le_bytes(slot[b_off..b_off + 4].try_into().unwrap());
        let offset = u16::from_le_bytes(slot[b_off + 4..b_off + 6].try_into().unwrap());
        let field_len = u16::from_le_bytes(slot[b_off + 6..b_off + 8].try_into().unwrap());
        let val_len = u16::from_le_bytes(slot[b_off + 8..b_off + 10].try_into().unwrap());
        Bucket {
            hash32,
            offset,
            field_len,
            val_len,
        }
    }

    /// Writes a bucket by its index `0..K`.
    #[inline(always)]
    pub fn write_bucket(slot: &mut [u8], idx: usize, b: &Bucket) {
        let cap = slot.len();
        let b_off = cap - BUCKET_SIZE * (idx + 1);
        slot[b_off..b_off + 4].copy_from_slice(&b.hash32.to_le_bytes());
        slot[b_off + 4..b_off + 6].copy_from_slice(&b.offset.to_le_bytes());
        slot[b_off + 6..b_off + 8].copy_from_slice(&b.field_len.to_le_bytes());
        slot[b_off + 8..b_off + 10].copy_from_slice(&b.val_len.to_le_bytes());
    }

    /// Number of active fields in the frame.
    #[inline(always)]
    pub fn live_count(slot: &[u8]) -> u16 {
        Self::read_header(slot).map(|(lc, _, _, _)| lc).unwrap_or(0)
    }

    /// Strict O(1) lookup of a field value.
    pub fn get<'s>(slot: &'s [u8], field: &[u8]) -> Option<&'s [u8]> {
        let (live_count, bucket_mask, _, _) = Self::read_header(slot).ok()?;
        if live_count == 0 {
            return None;
        }

        let hash32 = hash_field(field);
        let mask = bucket_mask as usize;
        let k = mask + 1;
        let mut idx = (hash32 as usize) & mask;

        for _ in 0..k {
            let b = Self::read_bucket(slot, idx);
            if b.field_len == EMPTY_FIELD_LEN {
                return None; // Strict O(1) termination!
            }
            if b.field_len != TOMBSTONE_FIELD_LEN && b.hash32 == hash32 {
                let f_off = b.offset as usize;
                let f_len = b.field_len as usize;
                if f_off + f_len <= slot.len() && &slot[f_off..f_off + f_len] == field {
                    let v_off = f_off + f_len;
                    let v_len = b.val_len as usize;
                    if v_off + v_len <= slot.len() {
                        return Some(&slot[v_off..v_off + v_len]);
                    }
                }
            }
            idx = (idx + 1) & mask;
        }

        None
    }

    /// Returns `true` if field exists in the frame.
    #[inline(always)]
    pub fn exists(slot: &[u8], field: &[u8]) -> bool {
        Self::get(slot, field).is_some()
    }

    /// Marks field as deleted (tombstone).
    /// Returns `true` if field was present and removed, `false` otherwise.
    pub fn delete(slot: &mut [u8], field: &[u8]) -> bool {
        let Ok((mut live_count, bucket_mask, payload_end, mut dead_bytes)) =
            Self::read_header(slot)
        else {
            return false;
        };
        if live_count == 0 {
            return false;
        }

        let hash32 = hash_field(field);
        let mask = bucket_mask as usize;
        let k = mask + 1;
        let mut idx = (hash32 as usize) & mask;

        for _ in 0..k {
            let b = Self::read_bucket(slot, idx);
            if b.field_len == EMPTY_FIELD_LEN {
                return false;
            }
            if b.field_len != TOMBSTONE_FIELD_LEN && b.hash32 == hash32 {
                let f_off = b.offset as usize;
                let f_len = b.field_len as usize;
                if f_off + f_len <= slot.len() && &slot[f_off..f_off + f_len] == field {
                    // Mark as tombstone
                    Self::write_bucket(
                        slot,
                        idx,
                        &Bucket {
                            hash32: 0,
                            offset: 0,
                            field_len: TOMBSTONE_FIELD_LEN,
                            val_len: 0,
                        },
                    );
                    live_count = live_count.saturating_sub(1);
                    dead_bytes = dead_bytes.saturating_add((f_len + b.val_len as usize) as u16);
                    Self::write_header(slot, live_count, bucket_mask, payload_end, dead_bytes);
                    return true;
                }
            }
            idx = (idx + 1) & mask;
        }

        false
    }

    /// Inserts or updates a field-value pair.
    ///
    /// Returns `Ok(true)` if field was newly created, `Ok(false)` if updated in place.
    pub fn upsert(slot: &mut [u8], field: &[u8], value: &[u8]) -> Result<bool, FrameError> {
        let (mut live_count, bucket_mask, mut payload_end, mut dead_bytes) =
            Self::read_header(slot)?;
        let hash32 = hash_field(field);
        let k = (bucket_mask as usize) + 1;

        // Step 1: Probe to see if field already exists
        let mut target_bucket_idx = None;
        let mut first_available_bucket_idx = None;
        let mut tombstone_count = 0usize;

        let mask = bucket_mask as usize;
        let mut idx = (hash32 as usize) & mask;
        for _ in 0..k {
            let b = Self::read_bucket(slot, idx);
            if b.field_len == EMPTY_FIELD_LEN {
                if first_available_bucket_idx.is_none() {
                    first_available_bucket_idx = Some(idx);
                }
                break; // stop on first empty
            } else if b.field_len == TOMBSTONE_FIELD_LEN {
                tombstone_count += 1;
                if first_available_bucket_idx.is_none() {
                    first_available_bucket_idx = Some(idx);
                }
            } else if b.hash32 == hash32 {
                let f_off = b.offset as usize;
                let f_len = b.field_len as usize;
                if f_off + f_len <= slot.len() && &slot[f_off..f_off + f_len] == field {
                    target_bucket_idx = Some((idx, b));
                    break;
                }
            }
            idx = (idx + 1) & mask;
        }

        // Case A: Field already exists -> Update path
        if let Some((b_idx, old_b)) = target_bucket_idx {
            let old_val_len = old_b.val_len as usize;
            let new_val_len = value.len();

            if new_val_len <= old_val_len {
                // In-place overwrite without changing payload_end
                let val_off = old_b.offset as usize + old_b.field_len as usize;
                slot[val_off..val_off + new_val_len].copy_from_slice(value);
                let slack = (old_val_len - new_val_len) as u16;
                dead_bytes = dead_bytes.saturating_add(slack);

                Self::write_bucket(
                    slot,
                    b_idx,
                    &Bucket {
                        hash32,
                        offset: old_b.offset,
                        field_len: old_b.field_len,
                        val_len: new_val_len as u16,
                    },
                );
                Self::write_header(slot, live_count, bucket_mask, payload_end, dead_bytes);
                return Ok(false);
            }

            // Larger value -> append new field+value pair to gap, mark old bytes dead
            let needed = field.len() + value.len();
            let dir_start = slot.len() - k * BUCKET_SIZE;
            if (payload_end as usize) + needed > dir_start {
                if dead_bytes > 0 {
                    Self::compact(slot)?;
                    return Self::upsert(slot, field, value);
                }
                return Err(FrameError::FrameFull);
            }

            let new_offset = payload_end as usize;
            slot[new_offset..new_offset + field.len()].copy_from_slice(field);
            let new_val_off = new_offset + field.len();
            slot[new_val_off..new_val_off + value.len()].copy_from_slice(value);

            dead_bytes = dead_bytes.saturating_add(old_b.field_len + old_b.val_len);
            payload_end += needed as u16;

            Self::write_bucket(
                slot,
                b_idx,
                &Bucket {
                    hash32,
                    offset: new_offset as u16,
                    field_len: field.len() as u16,
                    val_len: value.len() as u16,
                },
            );
            Self::write_header(slot, live_count, bucket_mask, payload_end, dead_bytes);
            return Ok(false);
        }

        // Case B: New field insertion
        // Check load factor: if (live_count + 1 + tombstones) * 10 >= k * 7, we need more buckets or compaction
        let total_occupied = (live_count as usize) + 1 + tombstone_count;
        if total_occupied * 10 >= k * 7 {
            if dead_bytes > 0 || tombstone_count > 0 {
                Self::compact(slot)?;
                return Self::upsert(slot, field, value);
            }
            // Try doubling K if space allows
            let next_k = k * 2;
            let needed_dir_growth = (next_k - k) * BUCKET_SIZE;
            let dir_start = slot.len() - k * BUCKET_SIZE;
            let needed_payload = field.len() + value.len();

            if dir_start >= (payload_end as usize) + needed_payload + needed_dir_growth {
                Self::grow_directory(slot, next_k)?;
                return Self::upsert(slot, field, value);
            } else {
                return Err(FrameError::FrameFull);
            }
        }

        let needed = field.len() + value.len();
        let dir_start = slot.len() - k * BUCKET_SIZE;
        if (payload_end as usize) + needed > dir_start {
            if dead_bytes > 0 {
                Self::compact(slot)?;
                return Self::upsert(slot, field, value);
            }
            return Err(FrameError::FrameFull);
        }

        let b_idx = first_available_bucket_idx.ok_or(FrameError::FrameFull)?;
        let new_offset = payload_end as usize;
        slot[new_offset..new_offset + field.len()].copy_from_slice(field);
        let val_off = new_offset + field.len();
        slot[val_off..val_off + value.len()].copy_from_slice(value);

        payload_end += needed as u16;
        live_count += 1;

        Self::write_bucket(
            slot,
            b_idx,
            &Bucket {
                hash32,
                offset: new_offset as u16,
                field_len: field.len() as u16,
                val_len: value.len() as u16,
            },
        );
        Self::write_header(slot, live_count, bucket_mask, payload_end, dead_bytes);
        Ok(true)
    }

    /// Re-allocates buckets with a larger K (power of 2) and rehashes in place.
    fn grow_directory(slot: &mut [u8], next_k: usize) -> Result<(), FrameError> {
        let (live_count, bucket_mask, payload_end, dead_bytes) = Self::read_header(slot)?;
        let old_k = (bucket_mask as usize) + 1;

        // Gather all live buckets
        let mut live_buckets: SmallVec<[Bucket; 64]> = SmallVec::new();
        for i in 0..old_k {
            let b = Self::read_bucket(slot, i);
            if b.field_len != EMPTY_FIELD_LEN && b.field_len != TOMBSTONE_FIELD_LEN {
                live_buckets.push(b);
            }
        }

        let next_mask = (next_k - 1) as u16;
        let next_dir_bytes = next_k * BUCKET_SIZE;
        let cap = slot.len();
        if (payload_end as usize) + next_dir_bytes > cap {
            return Err(FrameError::FrameFull);
        }

        // Clear the new directory area
        let dir_start = cap - next_dir_bytes;
        slot[dir_start..cap].fill(0);

        // Update header with new bucket_mask
        Self::write_header(slot, live_count, next_mask, payload_end, dead_bytes);

        // Rehash all live buckets
        for b in live_buckets {
            let mask = next_mask as usize;
            let mut idx = (b.hash32 as usize) & mask;
            for _ in 0..next_k {
                let existing = Self::read_bucket(slot, idx);
                if existing.field_len == EMPTY_FIELD_LEN {
                    Self::write_bucket(slot, idx, &b);
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }

        Ok(())
    }

    /// Compaction: removes all tombstones, eliminates dead payload gaps, and rehashes live entries.
    pub fn compact(slot: &mut [u8]) -> Result<(), FrameError> {
        let (live_count, bucket_mask, _payload_end, _dead_bytes) = Self::read_header(slot)?;
        if live_count == 0 {
            return Self::init(slot);
        }

        let k = (bucket_mask as usize) + 1;
        // Temporary copy of live pairs
        #[allow(clippy::type_complexity)]
        let mut pairs: SmallVec<[(SmallVec<[u8; 32]>, SmallVec<[u8; 64]>); 32]> = SmallVec::new();

        for i in 0..k {
            let b = Self::read_bucket(slot, i);
            if b.field_len != EMPTY_FIELD_LEN && b.field_len != TOMBSTONE_FIELD_LEN {
                let f_off = b.offset as usize;
                let f_len = b.field_len as usize;
                let v_off = f_off + f_len;
                let v_len = b.val_len as usize;

                if v_off + v_len <= slot.len() {
                    let mut field = SmallVec::new();
                    field.extend_from_slice(&slot[f_off..f_off + f_len]);
                    let mut val = SmallVec::new();
                    val.extend_from_slice(&slot[v_off..v_off + v_len]);
                    pairs.push((field, val));
                }
            }
        }

        // Reset frame cleanly
        Self::init(slot)?;
        for (field, val) in pairs {
            let _ = Self::upsert(slot, &field, &val);
        }

        Ok(())
    }

    /// Sizing helper: calculates the minimum bytes needed for an initial or updated hash.
    pub fn needed_bytes(slot: Option<&[u8]>, pairs: &[(&[u8], &[u8])]) -> usize {
        let mut total_payload = 0usize;
        let mut live_set: SmallVec<[&[u8]; 32]> = SmallVec::new();

        // If slot exists, account for existing pairs
        if let Some(s) = slot
            && let Ok((live_count, bucket_mask, _, _)) = Self::read_header(s)
            && live_count > 0
        {
            let k = (bucket_mask as usize) + 1;
            for i in 0..k {
                let b = Self::read_bucket(s, i);
                if b.field_len != EMPTY_FIELD_LEN && b.field_len != TOMBSTONE_FIELD_LEN {
                    let f_off = b.offset as usize;
                    let f_len = b.field_len as usize;
                    if f_off + f_len <= s.len() {
                        let f = &s[f_off..f_off + f_len];
                        // If the field is being updated in `pairs`, don't double count it
                        if !pairs.iter().any(|(pf, _)| *pf == f) {
                            total_payload += f_len + (b.val_len as usize);
                            live_set.push(f);
                        }
                    }
                }
            }
        }

        // Add new pairs
        for (f, v) in pairs {
            total_payload += f.len() + v.len();
            if !live_set.contains(f) {
                live_set.push(f);
            }
        }

        // Determine bucket count K needed for live_set.len() with load factor <= 0.70
        let target_entries = live_set.len().max(1);
        let mut k = 8;
        while target_entries * 10 >= k * 7 && k <= 256 {
            k *= 2;
        }

        HEADER_SIZE + total_payload + (k * BUCKET_SIZE)
    }

    /// Iterates over all active (field, value) pairs.
    pub fn iter_pairs<'a>(slot: &'a [u8]) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + 'a {
        let (live_count, bucket_mask, _, _) = Self::read_header(slot).unwrap_or((0, 0, 0, 0));
        let k = if live_count > 0 {
            (bucket_mask as usize) + 1
        } else {
            0
        };

        (0..k).filter_map(move |i| {
            let b = Self::read_bucket(slot, i);
            if b.field_len != EMPTY_FIELD_LEN && b.field_len != TOMBSTONE_FIELD_LEN {
                let f_off = b.offset as usize;
                let f_len = b.field_len as usize;
                let v_off = f_off + f_len;
                let v_len = b.val_len as usize;
                if v_off + v_len <= slot.len() {
                    let field = &slot[f_off..f_off + f_len];
                    let val = &slot[v_off..v_off + v_len];
                    return Some((field, val));
                }
            }
            None
        })
    }

    /// Serializes active fields to logical pair list for AOF and Snapshot:
    /// `[count: u16] { [field_len: u16][field][val_len: u16][val] } × count`
    pub fn encode_pairs(slot: &[u8], out: &mut Vec<u8>) {
        let live = Self::live_count(slot);
        out.extend_from_slice(&live.to_le_bytes());

        for (field, val) in Self::iter_pairs(slot) {
            out.extend_from_slice(&(field.len() as u16).to_le_bytes());
            out.extend_from_slice(field);
            out.extend_from_slice(&(val.len() as u16).to_le_bytes());
            out.extend_from_slice(val);
        }
    }

    /// Reconstructs a `HashSlotFrame` from a logical pair list.
    pub fn build_from_pairs(slot: &mut [u8], encoded: &[u8]) -> Result<(), FrameError> {
        if encoded.len() < 2 {
            return Err(FrameError::CorruptFrame);
        }
        let count = u16::from_le_bytes(encoded[0..2].try_into().unwrap()) as usize;
        let mut offset = 2;

        Self::init(slot)?;

        for _ in 0..count {
            if offset + 2 > encoded.len() {
                return Err(FrameError::CorruptFrame);
            }
            let f_len =
                u16::from_le_bytes(encoded[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if offset + f_len > encoded.len() {
                return Err(FrameError::CorruptFrame);
            }
            let field = &encoded[offset..offset + f_len];
            offset += f_len;

            if offset + 2 > encoded.len() {
                return Err(FrameError::CorruptFrame);
            }
            let v_len =
                u16::from_le_bytes(encoded[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if offset + v_len > encoded.len() {
                return Err(FrameError::CorruptFrame);
            }
            let val = &encoded[offset..offset + v_len];
            offset += v_len;

            Self::upsert(slot, field, val)?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_slot_frame_basic_crud() {
        let mut slot = vec![0u8; 512];
        HashSlotFrame::init(&mut slot).unwrap();

        assert_eq!(HashSlotFrame::live_count(&slot), 0);
        assert!(!HashSlotFrame::exists(&slot, b"name"));
        assert_eq!(HashSlotFrame::get(&slot, b"name"), None);

        // Insert name -> "Alice"
        let is_new = HashSlotFrame::upsert(&mut slot, b"name", b"Alice").unwrap();
        assert!(is_new);
        assert_eq!(HashSlotFrame::live_count(&slot), 1);
        assert!(HashSlotFrame::exists(&slot, b"name"));
        assert_eq!(
            HashSlotFrame::get(&slot, b"name"),
            Some(b"Alice".as_slice())
        );

        // Insert role -> "engineer"
        let is_new = HashSlotFrame::upsert(&mut slot, b"role", b"engineer").unwrap();
        assert!(is_new);
        assert_eq!(HashSlotFrame::live_count(&slot), 2);
        assert_eq!(
            HashSlotFrame::get(&slot, b"role"),
            Some(b"engineer".as_slice())
        );

        // Update in-place name -> "Bob" (smaller)
        let is_new = HashSlotFrame::upsert(&mut slot, b"name", b"Bob").unwrap();
        assert!(!is_new);
        assert_eq!(HashSlotFrame::live_count(&slot), 2);
        assert_eq!(HashSlotFrame::get(&slot, b"name"), Some(b"Bob".as_slice()));

        // Update name -> "Alexander" (larger)
        let is_new = HashSlotFrame::upsert(&mut slot, b"name", b"Alexander").unwrap();
        assert!(!is_new);
        assert_eq!(HashSlotFrame::live_count(&slot), 2);
        assert_eq!(
            HashSlotFrame::get(&slot, b"name"),
            Some(b"Alexander".as_slice())
        );

        // Delete role
        let deleted = HashSlotFrame::delete(&mut slot, b"role");
        assert!(deleted);
        assert_eq!(HashSlotFrame::live_count(&slot), 1);
        assert_eq!(HashSlotFrame::get(&slot, b"role"), None);

        // Delete non-existent
        assert!(!HashSlotFrame::delete(&mut slot, b"non_existent"));
    }

    #[test]
    fn test_encode_and_build_pairs() {
        let mut slot = vec![0u8; 512];
        HashSlotFrame::init(&mut slot).unwrap();

        HashSlotFrame::upsert(&mut slot, b"f1", b"v1").unwrap();
        HashSlotFrame::upsert(&mut slot, b"f2", b"v2").unwrap();
        HashSlotFrame::upsert(&mut slot, b"f3", b"v3").unwrap();

        let mut encoded = Vec::new();
        HashSlotFrame::encode_pairs(&slot, &mut encoded);

        let mut restored = vec![0u8; 512];
        HashSlotFrame::build_from_pairs(&mut restored, &encoded).unwrap();

        assert_eq!(HashSlotFrame::live_count(&restored), 3);
        assert_eq!(HashSlotFrame::get(&restored, b"f1"), Some(b"v1".as_slice()));
        assert_eq!(HashSlotFrame::get(&restored, b"f2"), Some(b"v2".as_slice()));
        assert_eq!(HashSlotFrame::get(&restored, b"f3"), Some(b"v3".as_slice()));
    }

    #[test]
    fn test_hash_frame_compaction() {
        let mut slot = vec![0u8; 128];
        HashSlotFrame::init(&mut slot).unwrap();

        HashSlotFrame::upsert(&mut slot, b"k1", b"v1").unwrap();
        HashSlotFrame::upsert(&mut slot, b"k2", b"v2").unwrap();
        HashSlotFrame::delete(&mut slot, b"k1");

        // Verify dead bytes are cleared on compaction
        HashSlotFrame::compact(&mut slot).unwrap();
        assert_eq!(HashSlotFrame::live_count(&slot), 1);
        assert_eq!(HashSlotFrame::get(&slot, b"k2"), Some(b"v2".as_slice()));
        let (_, _, _, dead_bytes) = HashSlotFrame::read_header(&slot).unwrap();
        assert_eq!(dead_bytes, 0);
    }

    #[test]
    fn test_hash_frame_against_hashmap_model() {
        use std::collections::HashMap;

        let mut slot = vec![0u8; 4096];
        HashSlotFrame::init(&mut slot).unwrap();
        let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        // Sequential operations: insert 50 pairs
        for i in 0..50 {
            let key = format!("field_{:03}", i).into_bytes();
            let val = format!("value_{:04}", i * 17).into_bytes();
            HashSlotFrame::upsert(&mut slot, &key, &val).unwrap();
            model.insert(key, val);
        }

        assert_eq!(HashSlotFrame::live_count(&slot) as usize, model.len());

        // Validate all match
        for (k, v) in &model {
            assert_eq!(HashSlotFrame::get(&slot, k), Some(v.as_slice()));
        }

        // Update 25 pairs
        for i in (0..50).step_by(2) {
            let key = format!("field_{:03}", i).into_bytes();
            let new_val = format!("updated_longer_val_{}", i).into_bytes();
            HashSlotFrame::upsert(&mut slot, &key, &new_val).unwrap();
            model.insert(key, new_val);
        }

        // Delete 15 pairs
        for i in 10..25 {
            let key = format!("field_{:03}", i).into_bytes();
            HashSlotFrame::delete(&mut slot, &key);
            model.remove(&key);
        }

        assert_eq!(HashSlotFrame::live_count(&slot) as usize, model.len());

        for (k, v) in &model {
            assert_eq!(HashSlotFrame::get(&slot, k), Some(v.as_slice()));
        }

        // Ensure deleted are gone
        for i in 10..25 {
            let key = format!("field_{:03}", i).into_bytes();
            assert_eq!(HashSlotFrame::get(&slot, &key), None);
        }
    }

    #[test]
    fn test_hash_frame_fuzz_arbitrary_bytes_no_panic() {
        // Pseudo-random deterministic bytes generator
        let mut state = 0x12345678u64;
        let mut next_u8 = || {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            (state >> 33) as u8
        };

        for size in [0, 1, 7, 8, 15, 63, 64, 127, 128, 511, 512, 1024, 4096] {
            for _ in 0..20 {
                let mut junk = vec![0u8; size];
                for b in &mut junk {
                    *b = next_u8();
                }

                // None of these methods should ever panic or crash when given arbitrary bytes
                let _ = HashSlotFrame::read_header(&junk);
                let _ = HashSlotFrame::live_count(&junk);
                let _ = HashSlotFrame::get(&junk, b"field");
                let _ = HashSlotFrame::exists(&junk, b"field");
                let _ = HashSlotFrame::delete(&mut junk, b"field");
                let _ = HashSlotFrame::upsert(&mut junk, b"field", b"val");
                let _ = HashSlotFrame::compact(&mut junk);

                let mut out = Vec::new();
                HashSlotFrame::encode_pairs(&junk, &mut out);
                let _ = HashSlotFrame::build_from_pairs(&mut junk, &out);
            }
        }
    }
}
