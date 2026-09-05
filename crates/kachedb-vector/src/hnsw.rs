//! `kachedb-vector` — Hierarchical Navigable Small World (HNSW) multi-layer proximity graphs.
//!
//! Provides logarithmic $O(\log N)$ approximate nearest neighbor search for high-dimensional vector spaces,
//! configurable distance metrics (Cosine, Euclidean, Dot Product), and optional 4x SQ8 scalar quantization.

#![allow(clippy::collapsible_if)]

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use ahash::{AHashMap, AHashSet};
use parking_lot::RwLock;

use crate::error::VectorError;
use crate::index::{VectorIndexStats, VectorSearchResult};
use crate::quantizer::{QuantizationMode, Sq8Quantizer};
use crate::simd::{cosine_similarity_normalized, dot_product, l2_distance_squared, l2_normalize};

/// Distance metric used for vector similarity in HNSW.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum VectorMetric {
    #[default]
    Cosine,
    L2,
    DotProduct,
}

impl VectorMetric {
    pub fn parse(s: &str) -> Option<Self> {
        if s.eq_ignore_ascii_case("COSINE") {
            Some(Self::Cosine)
        } else if s.eq_ignore_ascii_case("L2") || s.eq_ignore_ascii_case("EUCLIDEAN") {
            Some(Self::L2)
        } else if s.eq_ignore_ascii_case("IP")
            || s.eq_ignore_ascii_case("DOT")
            || s.eq_ignore_ascii_case("DOTPRODUCT")
        {
            Some(Self::DotProduct)
        } else {
            None
        }
    }
}

/// A node in the HNSW multi-layer graph.
#[derive(Debug, Clone)]
pub struct HnswNode {
    pub id: u32,
    pub key: Vec<u8>,
    pub vector_fp32: Option<Vec<f32>>,
    pub vector_sq8: Option<(Vec<u8>, f32, f32)>, // (quantized_bytes, min, max)
    pub payload: Option<Vec<u8>>,
    pub expire_at_secs: u32,
    pub max_layer: usize,
}

#[derive(Clone, Copy)]
struct Candidate {
    node_id: u32,
    distance: f32,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.node_id == other.node_id && self.distance == other.distance
    }
}

impl Eq for Candidate {}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .partial_cmp(&other.distance)
            .unwrap_or(Ordering::Equal)
    }
}

struct MinCandidate(Candidate);

impl PartialEq for MinCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for MinCandidate {}

impl PartialOrd for MinCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse for min-heap
        other.0.cmp(&self.0)
    }
}

struct HnswInner {
    entry_point: Option<(u32, usize)>, // (node_id, top_layer)
    nodes: Vec<Option<HnswNode>>,
    key_map: AHashMap<Vec<u8>, u32>,
    layers: Vec<AHashMap<u32, Vec<u32>>>, // layer -> (node_id -> neighbors)
    rng_state: u64,
}

impl HnswInner {
    fn random_level(&mut self, ml: f64) -> usize {
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;
        let r = ((self.rng_state as f64) / (u64::MAX as f64)).max(1e-15);
        let lvl = (-r.ln() * ml).floor() as usize;
        lvl.min(16)
    }
}

/// Hierarchical Navigable Small World index for vector proximity search.
pub struct HnswIndex {
    pub name: String,
    pub dim: usize,
    pub m: usize,
    pub m0: usize,
    pub ef_construction: usize,
    pub ef_search: usize,
    pub metric: VectorMetric,
    pub quantization: QuantizationMode,
    ml: f64,
    inner: RwLock<HnswInner>,
}

impl HnswIndex {
    /// Creates a new HNSW index with default parameters:
    /// M=16, EF_CONSTRUCTION=200, EF_SEARCH=50, Cosine metric, no quantization.
    pub fn new(name: impl Into<String>, dim: usize) -> Self {
        Self::with_params(
            name,
            dim,
            16,
            200,
            50,
            VectorMetric::Cosine,
            QuantizationMode::None,
        )
    }

    /// Creates a new HNSW index with explicit parameters.
    pub fn with_params(
        name: impl Into<String>,
        dim: usize,
        m: usize,
        ef_construction: usize,
        ef_search: usize,
        metric: VectorMetric,
        quantization: QuantizationMode,
    ) -> Self {
        let m = m.max(2);
        let m0 = 2 * m;
        let ml = 1.0 / (m as f64).ln();

        let inner = HnswInner {
            entry_point: None,
            nodes: Vec::new(),
            key_map: AHashMap::new(),
            layers: Vec::new(),
            rng_state: 0x9E3779B97F4A7C15,
        };

        Self {
            name: name.into(),
            dim,
            m,
            m0,
            ef_construction,
            ef_search,
            metric,
            quantization,
            ml,
            inner: RwLock::new(inner),
        }
    }

    /// Evaluates distance between query vector and a stored node (smaller = closer).
    fn node_distance_nodes(&self, nodes: &[Option<HnswNode>], node_id: u32, query: &[f32]) -> f32 {
        let node = match nodes.get(node_id as usize).and_then(|n| n.as_ref()) {
            Some(n) => n,
            None => return f32::INFINITY,
        };

        match self.quantization {
            QuantizationMode::SQ8 => {
                if let Some((ref bytes, min, max)) = node.vector_sq8 {
                    match self.metric {
                        VectorMetric::Cosine => {
                            let sim =
                                Sq8Quantizer::asymmetric_cosine_similarity(query, bytes, min, max);
                            1.0 - sim
                        }
                        VectorMetric::L2 => {
                            Sq8Quantizer::asymmetric_l2_squared(query, bytes, min, max)
                        }
                        VectorMetric::DotProduct => {
                            -Sq8Quantizer::asymmetric_dot_product(query, bytes, min, max)
                        }
                    }
                } else if let Some(ref fp32) = node.vector_fp32 {
                    self.raw_distance(query, fp32)
                } else {
                    f32::INFINITY
                }
            }
            QuantizationMode::None => {
                if let Some(ref fp32) = node.vector_fp32 {
                    self.raw_distance(query, fp32)
                } else {
                    f32::INFINITY
                }
            }
        }
    }

    #[inline]
    fn node_distance(&self, inner: &HnswInner, node_id: u32, query: &[f32]) -> f32 {
        self.node_distance_nodes(&inner.nodes, node_id, query)
    }

    fn raw_distance(&self, query: &[f32], target: &[f32]) -> f32 {
        match self.metric {
            VectorMetric::Cosine => 1.0 - cosine_similarity_normalized(query, target),
            VectorMetric::L2 => l2_distance_squared(query, target),
            VectorMetric::DotProduct => -dot_product(query, target),
        }
    }

    /// Inserts a vector into the HNSW graph.
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
        if vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                actual: vector.len(),
            });
        }

        let mut processed_vec = vector.to_vec();
        if self.metric == VectorMetric::Cosine {
            l2_normalize(&mut processed_vec);
        }

        let expire_at_secs = ttl_sec
            .map(|s| if now_sec > 0 { now_sec + s } else { s })
            .unwrap_or(0);

        let (vector_fp32, vector_sq8) = match self.quantization {
            QuantizationMode::None => (Some(processed_vec.clone()), None),
            QuantizationMode::SQ8 => {
                let (bytes, min, max) = Sq8Quantizer::encode(&processed_vec);
                (None, Some((bytes, min, max)))
            }
        };

        let mut inner = self.inner.write();

        // Update if key already exists
        if let Some(&existing_id) = inner.key_map.get(key) {
            if let Some(node) = inner.nodes.get_mut(existing_id as usize) {
                if let Some(n) = node.as_mut() {
                    n.vector_fp32 = vector_fp32;
                    n.vector_sq8 = vector_sq8;
                    n.payload = payload.map(|p| p.to_vec());
                    n.expire_at_secs = expire_at_secs;
                    return Ok(());
                }
            }
        }

        let node_layer = inner.random_level(self.ml);
        let node_id = inner.nodes.len() as u32;

        let node = HnswNode {
            id: node_id,
            key: key.to_vec(),
            vector_fp32,
            vector_sq8,
            payload: payload.map(|p| p.to_vec()),
            expire_at_secs,
            max_layer: node_layer,
        };

        inner.nodes.push(Some(node));
        inner.key_map.insert(key.to_vec(), node_id);

        while inner.layers.len() <= node_layer {
            inner.layers.push(AHashMap::new());
        }

        let entry_point = inner.entry_point;

        if entry_point.is_none() {
            inner.entry_point = Some((node_id, node_layer));
            for l in 0..=node_layer {
                inner.layers[l].insert(node_id, Vec::new());
            }
            return Ok(());
        }

        let (mut curr_ep, ep_layer) = entry_point.unwrap();
        let mut curr_dist = self.node_distance(&inner, curr_ep, &processed_vec);

        // Phase 1: greedy zoom-in from ep_layer down to node_layer + 1
        let mut l = ep_layer;
        while l > node_layer {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(neighbors) = inner.layers[l].get(&curr_ep).cloned() {
                    for nbr in neighbors {
                        let d = self.node_distance(&inner, nbr, &processed_vec);
                        if d < curr_dist {
                            curr_dist = d;
                            curr_ep = nbr;
                            changed = true;
                        }
                    }
                }
            }
            l -= 1;
        }

        // Phase 2: beam search and connection from min(ep_layer, node_layer) down to 0
        let mut top_l = ep_layer.min(node_layer);
        loop {
            let m_max = if top_l == 0 { self.m0 } else { self.m };
            let candidates =
                self.search_layer(&inner, &processed_vec, curr_ep, self.ef_construction, top_l);

            // Select m_max closest neighbors
            let selected_neighbors: Vec<u32> = candidates
                .iter()
                .filter(|c| c.node_id != node_id)
                .take(m_max)
                .map(|c| c.node_id)
                .collect();

            // Connect node_id to selected neighbors
            inner.layers[top_l].insert(node_id, selected_neighbors.clone());

            // Add back-links from selected neighbors to node_id
            let HnswInner {
                ref nodes,
                ref mut layers,
                ..
            } = *inner;
            let layer_map = &mut layers[top_l];
            for &nbr in &selected_neighbors {
                let nbr_list = layer_map.entry(nbr).or_default();
                if !nbr_list.contains(&node_id) {
                    nbr_list.push(node_id);
                    if nbr_list.len() > m_max {
                        // Shrink to m_max closest
                        let nbr_vec = match self.get_node_vector_nodes(nodes, nbr) {
                            Some(v) => v,
                            None => continue,
                        };
                        nbr_list.sort_by(|&a, &b| {
                            let da = self.node_distance_nodes(nodes, a, &nbr_vec);
                            let db = self.node_distance_nodes(nodes, b, &nbr_vec);
                            da.partial_cmp(&db).unwrap_or(Ordering::Equal)
                        });
                        nbr_list.truncate(m_max);
                    }
                }
            }

            if let Some(first) = candidates.first() {
                curr_ep = first.node_id;
            }

            if top_l == 0 {
                break;
            }
            top_l -= 1;
        }

        // Initialize higher levels if node_layer > ep_layer
        for l in (ep_layer + 1)..=node_layer {
            inner.layers[l].insert(node_id, Vec::new());
        }

        if node_layer > ep_layer {
            inner.entry_point = Some((node_id, node_layer));
        }

        Ok(())
    }

    fn get_node_vector_nodes(&self, nodes: &[Option<HnswNode>], node_id: u32) -> Option<Vec<f32>> {
        let node = nodes.get(node_id as usize)?.as_ref()?;
        if let Some(ref fp32) = node.vector_fp32 {
            Some(fp32.clone())
        } else if let Some((ref bytes, min, max)) = node.vector_sq8 {
            Some(Sq8Quantizer::decode(bytes, min, max))
        } else {
            None
        }
    }

    fn search_layer(
        &self,
        inner: &HnswInner,
        query: &[f32],
        ep: u32,
        ef: usize,
        layer: usize,
    ) -> Vec<Candidate> {
        let mut visited = AHashSet::new();
        let mut candidates = BinaryHeap::new(); // Min-heap (closest candidate popped first)
        let mut w = BinaryHeap::new(); // Max-heap (furthest popped when > ef)

        let initial_dist = self.node_distance(inner, ep, query);
        let ep_candidate = Candidate {
            node_id: ep,
            distance: initial_dist,
        };

        visited.insert(ep);
        candidates.push(MinCandidate(ep_candidate));
        w.push(ep_candidate);

        while let Some(MinCandidate(c)) = candidates.pop() {
            if let Some(furthest) = w.peek() {
                if c.distance > furthest.distance {
                    break;
                }
            }

            if let Some(neighbors) = inner.layers.get(layer).and_then(|l| l.get(&c.node_id)) {
                for &nbr in neighbors {
                    if visited.insert(nbr) {
                        let d = self.node_distance(inner, nbr, query);
                        let furthest_dist = w.peek().map(|f| f.distance).unwrap_or(f32::INFINITY);
                        if d < furthest_dist || w.len() < ef {
                            let cand = Candidate {
                                node_id: nbr,
                                distance: d,
                            };
                            candidates.push(MinCandidate(cand));
                            w.push(cand);
                            if w.len() > ef {
                                w.pop();
                            }
                        }
                    }
                }
            }
        }

        let mut results: Vec<Candidate> = w.into_vec();
        results.sort();
        results
    }

    /// Performs approximate nearest neighbor search on the HNSW graph.
    pub fn search(
        &self,
        query_vector: &[f32],
        top_k: usize,
        ef_search: Option<usize>,
        threshold: f32,
        now_sec: u32,
    ) -> Result<Vec<VectorSearchResult>, VectorError> {
        if top_k == 0 {
            return Err(VectorError::InvalidTopK(top_k));
        }
        if !(-1.0..=1.0).contains(&threshold) {
            return Err(VectorError::InvalidThreshold(threshold));
        }
        if query_vector.len() != self.dim {
            return Err(VectorError::DimensionMismatch {
                expected: self.dim,
                actual: query_vector.len(),
            });
        }

        let mut processed_query = query_vector.to_vec();
        if self.metric == VectorMetric::Cosine {
            l2_normalize(&mut processed_query);
        }

        let inner = self.inner.read();
        let (mut curr_ep, ep_layer) = match inner.entry_point {
            Some(ep) => ep,
            None => return Ok(Vec::new()),
        };

        let mut curr_dist = self.node_distance(&inner, curr_ep, &processed_query);

        // Phase 1: greedy search through upper layers
        let mut l = ep_layer;
        while l > 0 {
            let mut changed = true;
            while changed {
                changed = false;
                if let Some(neighbors) = inner.layers[l].get(&curr_ep) {
                    for &nbr in neighbors {
                        let d = self.node_distance(&inner, nbr, &processed_query);
                        if d < curr_dist {
                            curr_dist = d;
                            curr_ep = nbr;
                            changed = true;
                        }
                    }
                }
            }
            l -= 1;
        }

        // Phase 2: beam search at layer 0
        let ef = ef_search.unwrap_or(self.ef_search).max(top_k);
        let candidates = self.search_layer(&inner, &processed_query, curr_ep, ef, 0);

        let mut results = Vec::new();
        for cand in candidates {
            if let Some(Some(node)) = inner.nodes.get(cand.node_id as usize) {
                // Check TTL expiry
                if node.expire_at_secs > 0 && now_sec > 0 && now_sec >= node.expire_at_secs {
                    continue;
                }

                let similarity = match self.metric {
                    VectorMetric::Cosine => 1.0 - cand.distance,
                    VectorMetric::DotProduct => -cand.distance,
                    VectorMetric::L2 => 1.0 / (1.0 + cand.distance.sqrt()),
                };

                if similarity >= threshold {
                    results.push(VectorSearchResult {
                        key: node.key.clone(),
                        similarity,
                        payload: node.payload.clone(),
                    });
                }
            }
        }

        results.sort_by(|a, b| {
            b.similarity
                .partial_cmp(&a.similarity)
                .unwrap_or(Ordering::Equal)
        });

        if results.len() > top_k {
            results.truncate(top_k);
        }

        Ok(results)
    }

    /// Removes a vector by key (lazy tombstone).
    pub fn delete(&self, key: &[u8]) -> bool {
        let mut inner = self.inner.write();
        if let Some(node_id) = inner.key_map.remove(key) {
            if let Some(node) = inner.nodes.get_mut(node_id as usize) {
                *node = None;
                return true;
            }
        }
        false
    }

    /// Returns index statistics.
    pub fn stats(&self, now_sec: u32) -> VectorIndexStats {
        let inner = self.inner.read();
        let mut active = 0;
        let mut mem = 0;

        for node in inner.nodes.iter().flatten() {
            if node.expire_at_secs == 0 || now_sec == 0 || now_sec < node.expire_at_secs {
                active += 1;
                mem += node.key.len() + node.payload.as_ref().map_or(0, |p| p.len()) + 48;
                if let Some(ref v) = node.vector_fp32 {
                    mem += v.len() * 4;
                }
                if let Some((ref b, _, _)) = node.vector_sq8 {
                    mem += b.len() + 8;
                }
            }
        }

        for layer in &inner.layers {
            for (_, neighbors) in layer {
                mem += 24 + neighbors.len() * 4;
            }
        }

        VectorIndexStats {
            name: self.name.clone(),
            dimension: self.dim,
            total_vectors: inner.nodes.len(),
            active_vectors: active,
            memory_bytes: mem,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hnsw_basic_insert_and_search() {
        let index = HnswIndex::new("test_hnsw", 3);
        let v1 = vec![1.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0];
        let v3 = vec![0.0, 0.0, 1.0];

        index.insert(b"x", &v1, Some(b"x_data"), None, 0).unwrap();
        index.insert(b"y", &v2, Some(b"y_data"), None, 0).unwrap();
        index.insert(b"z", &v3, Some(b"z_data"), None, 0).unwrap();

        let query = vec![0.9, 0.1, 0.0];
        let results = index.search(&query, 2, None, 0.5, 0).unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].key, b"x");
        assert_eq!(results[0].payload.as_deref(), Some(&b"x_data"[..]));
        assert!(results[0].similarity > 0.9);
    }

    #[test]
    fn test_hnsw_sq8_quantization_search() {
        let index = HnswIndex::with_params(
            "test_sq8_hnsw",
            4,
            8,
            50,
            20,
            VectorMetric::Cosine,
            QuantizationMode::SQ8,
        );

        let v1 = vec![1.0, 0.0, 0.0, 0.0];
        let v2 = vec![0.0, 1.0, 0.0, 0.0];

        index.insert(b"k1", &v1, None, None, 0).unwrap();
        index.insert(b"k2", &v2, None, None, 0).unwrap();

        let query = vec![0.95, 0.05, 0.0, 0.0];
        let results = index.search(&query, 1, None, 0.8, 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].key, b"k1");
    }

    #[test]
    fn test_hnsw_delete_and_expiry() {
        let index = HnswIndex::new("test_del_exp", 2);
        let v1 = vec![1.0, 0.0];

        index
            .insert(b"q1", &v1, Some(b"ans"), Some(10), 100)
            .unwrap();

        let res = index.search(&v1, 1, None, 0.5, 105).unwrap();
        assert_eq!(res.len(), 1);

        // Expired at t=115
        let res_exp = index.search(&v1, 1, None, 0.5, 115).unwrap();
        assert_eq!(res_exp.len(), 0);

        // Re-insert without expiry and delete
        index.insert(b"q1", &v1, None, None, 0).unwrap();
        assert!(index.delete(b"q1"));
        let res_del = index.search(&v1, 1, None, 0.5, 0).unwrap();
        assert_eq!(res_del.len(), 0);
    }
}
