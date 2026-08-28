//! `kachedb-vector` — In-memory vector index and multi-index registry.
//!
//! Stores normalized float vectors in contiguous memory for high-cache-locality SIMD scanning,
//! supporting TTL expiration, top-k similarity searches, and atomic updates.

use ahash::AHashMap;
use parking_lot::RwLock;
use std::sync::Arc;

use crate::error::VectorError;
use crate::simd::{cosine_similarity_normalized, l2_normalize};

/// A stored vector record in the index.
#[derive(Debug, Clone)]
pub struct VectorEntry {
    /// Unique identifier / prompt key.
    pub key: Vec<u8>,
    /// Unit-normalized $L_2$ embedding vector.
    pub vector: Vec<f32>,
    /// Associated metadata or cached LLM completion payload.
    pub payload: Option<Vec<u8>>,
    /// Absolute expiration timestamp in Unix seconds (0 = persistent / no expiry).
    pub expire_at_secs: u32,
}

/// A matched result from a vector similarity search.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchResult {
    /// Matched record identifier.
    pub key: Vec<u8>,
    /// Cosine similarity score in range `[-1.0, 1.0]`.
    pub similarity: f32,
    /// Associated metadata or LLM response payload.
    pub payload: Option<Vec<u8>>,
}

/// Statistics for a specific vector index.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct VectorIndexStats {
    pub name: String,
    pub dimension: usize,
    pub total_vectors: usize,
    pub active_vectors: usize,
    pub memory_bytes: usize,
}

/// In-memory contiguous vector index with SIMD search acceleration.
pub struct VectorIndex {
    pub name: String,
    dimension: RwLock<Option<usize>>,
    entries: RwLock<Vec<Option<VectorEntry>>>,
    key_map: RwLock<AHashMap<Vec<u8>, usize>>,
}

impl VectorIndex {
    /// Creates a new named vector index.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            dimension: RwLock::new(None),
            entries: RwLock::new(Vec::new()),
            key_map: RwLock::new(AHashMap::new()),
        }
    }

    /// Creates a new named vector index with an explicit pre-configured dimension.
    pub fn with_dimension(name: impl Into<String>, dim: usize) -> Self {
        Self {
            name: name.into(),
            dimension: RwLock::new(Some(dim)),
            entries: RwLock::new(Vec::new()),
            key_map: RwLock::new(AHashMap::new()),
        }
    }

    /// Inserts or updates a vector entry.
    ///
    /// The incoming vector is automatically normalized to unit length ($L_2 = 1.0$)
    /// so subsequent searches can use direct SIMD dot products.
    pub fn insert(
        &self,
        key: &[u8],
        vector: &[f32],
        payload: Option<&[u8]>,
        ttl_sec: Option<u32>,
        now_sec: u32,
    ) -> Result<(), VectorError> {
        if key.is_empty() {
            return Err(VectorError::EmptyKey);
        }

        let dim = vector.len();
        if dim == 0 {
            return Err(VectorError::DimensionMismatch {
                expected: 1,
                actual: 0,
            });
        }

        // Validate or set dimension
        {
            let mut dim_guard = self.dimension.write();
            match *dim_guard {
                Some(expected) => {
                    if expected != dim {
                        return Err(VectorError::DimensionMismatch {
                            expected,
                            actual: dim,
                        });
                    }
                }
                None => {
                    *dim_guard = Some(dim);
                }
            }
        }

        // Prepare normalized vector copy
        let mut normalized_vec = vector.to_vec();
        l2_normalize(&mut normalized_vec);

        let expire_at_secs = ttl_sec
            .map(|s| if now_sec > 0 { now_sec + s } else { s })
            .unwrap_or(0);

        let new_entry = VectorEntry {
            key: key.to_vec(),
            vector: normalized_vec,
            payload: payload.map(|p| p.to_vec()),
            expire_at_secs,
        };

        let mut key_map = self.key_map.write();
        let mut entries = self.entries.write();

        if let Some(&idx) = key_map.get(key) {
            // Update existing entry
            entries[idx] = Some(new_entry);
        } else {
            // Try to find a dead/tombstoned slot first
            let mut inserted_idx = None;
            for (i, slot) in entries.iter_mut().enumerate() {
                if slot.is_none() {
                    *slot = Some(new_entry.clone());
                    inserted_idx = Some(i);
                    break;
                }
            }

            let idx = match inserted_idx {
                Some(i) => i,
                None => {
                    entries.push(Some(new_entry));
                    entries.len() - 1
                }
            };
            key_map.insert(key.to_vec(), idx);
        }

        Ok(())
    }

    /// Performs a top-k SIMD vector similarity search across all active unexpired vectors.
    pub fn search(
        &self,
        query_vector: &[f32],
        top_k: usize,
        threshold: f32,
        now_sec: u32,
    ) -> Result<Vec<VectorSearchResult>, VectorError> {
        if top_k == 0 {
            return Err(VectorError::InvalidTopK(top_k));
        }
        if !(-1.0..=1.0).contains(&threshold) {
            return Err(VectorError::InvalidThreshold(threshold));
        }

        let dim = query_vector.len();
        {
            let dim_guard = self.dimension.read();
            if let Some(expected) = *dim_guard {
                if expected != dim {
                    return Err(VectorError::DimensionMismatch {
                        expected,
                        actual: dim,
                    });
                }
            } else {
                // Empty index
                return Ok(Vec::new());
            }
        }

        // Normalize query vector for exact cosine similarity via dot product
        let mut norm_query = query_vector.to_vec();
        l2_normalize(&mut norm_query);

        let entries = self.entries.read();
        let mut scored: Vec<VectorSearchResult> = Vec::new();

        for slot in entries.iter().flatten() {
            // Check TTL expiration
            if slot.expire_at_secs > 0 && now_sec > 0 && now_sec >= slot.expire_at_secs {
                continue;
            }

            // SIMD Dot Product on normalized vectors
            let sim = cosine_similarity_normalized(&norm_query, &slot.vector);

            if sim >= threshold {
                scored.push(VectorSearchResult {
                    key: slot.key.clone(),
                    similarity: sim,
                    payload: slot.payload.clone(),
                });
            }
        }

        // Sort descending by similarity score
        scored.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if scored.len() > top_k {
            scored.truncate(top_k);
        }

        Ok(scored)
    }

    /// Deletes an entry by key. Returns true if key was found and removed.
    pub fn delete(&self, key: &[u8]) -> bool {
        let mut key_map = self.key_map.write();
        let mut entries = self.entries.write();

        #[allow(clippy::collapsible_if)]
        if let Some(idx) = key_map.remove(key) {
            if idx < entries.len() {
                entries[idx] = None;
                return true;
            }
        }
        false
    }

    /// Returns index statistics.
    pub fn stats(&self, now_sec: u32) -> VectorIndexStats {
        let entries = self.entries.read();
        let dim = self.dimension.read().unwrap_or(0);

        let mut active = 0;
        let mut mem = 0;

        for slot in entries.iter().flatten() {
            if slot.expire_at_secs == 0 || now_sec == 0 || now_sec < slot.expire_at_secs {
                active += 1;
                mem += slot.key.len()
                    + (slot.vector.len() * 4)
                    + slot.payload.as_ref().map_or(0, |p| p.len())
                    + 32;
            }
        }

        VectorIndexStats {
            name: self.name.clone(),
            dimension: dim,
            total_vectors: entries.len(),
            active_vectors: active,
            memory_bytes: mem,
        }
    }
}

/// Thread-safe global registry of named vector indexes.
#[derive(Default)]
pub struct VectorIndexRegistry {
    indexes: RwLock<AHashMap<Vec<u8>, Arc<VectorIndex>>>,
}

impl VectorIndexRegistry {
    /// Creates a new empty vector index registry.
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(AHashMap::new()),
        }
    }

    /// Retrieves an existing index or creates a new one with the given name.
    pub fn get_or_create(&self, name: &[u8]) -> Arc<VectorIndex> {
        let read_guard = self.indexes.read();
        if let Some(idx) = read_guard.get(name) {
            return Arc::clone(idx);
        }
        drop(read_guard);

        let mut write_guard = self.indexes.write();
        write_guard
            .entry(name.to_vec())
            .or_insert_with(|| {
                let name_str = String::from_utf8_lossy(name).to_string();
                Arc::new(VectorIndex::new(name_str))
            })
            .clone()
    }

    /// Retrieves an existing index by name if present.
    pub fn get(&self, name: &[u8]) -> Option<Arc<VectorIndex>> {
        self.indexes.read().get(name).cloned()
    }

    /// Deletes a named index.
    pub fn delete_index(&self, name: &[u8]) -> bool {
        self.indexes.write().remove(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_index_basic_crud() {
        let index = VectorIndex::new("test_faq");
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];

        // Insert
        index.insert(b"q1", &v1, Some(b"ans1"), None, 0).unwrap();
        index.insert(b"q2", &v2, Some(b"ans2"), None, 0).unwrap();

        // Search exact match for v1
        let results = index.search(&v1, 5, 0.8, 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"q1");
        assert!((results[0].similarity - 1.0).abs() < 1e-5);
        assert_eq!(results[0].payload.as_deref(), Some(&b"ans1"[..]));

        // Search with orthogonal query
        let v_ortho = vec![0.0, 0.0, 1.0];
        let results2 = index.search(&v_ortho, 5, 0.8, 0).unwrap();
        assert_eq!(results2.len(), 0);

        // Delete
        assert!(index.delete(b"q1"));
        assert!(!index.delete(b"non_existent"));

        let results3 = index.search(&v1, 5, 0.8, 0).unwrap();
        assert_eq!(results3.len(), 0);
    }

    #[test]
    fn test_vector_index_ttl_expiry() {
        let index = VectorIndex::new("test_ttl");
        let v1 = vec![1.0, 0.0];

        // Insert with 10s TTL starting at t=100 (expires at t=110)
        index
            .insert(b"q1", &v1, Some(b"ans1"), Some(10), 100)
            .unwrap();

        // At t=105, still active
        let results = index.search(&v1, 1, 0.5, 105).unwrap();
        assert_eq!(results.len(), 1);

        // At t=115, expired!
        let results_expired = index.search(&v1, 1, 0.5, 115).unwrap();
        assert_eq!(results_expired.len(), 0);
    }

    #[test]
    fn test_registry_get_or_create() {
        let registry = VectorIndexRegistry::new();
        let idx1 = registry.get_or_create(b"idx1");
        let idx2 = registry.get_or_create(b"idx1");
        assert_eq!(idx1.name, idx2.name);
    }
}
