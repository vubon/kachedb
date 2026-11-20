//! Snapshot encoding for HNSW parameters, stored vectors and graph topology.

use std::collections::HashSet;

use kachedb_vector::hnsw::{HnswGraph, HnswNode};
use kachedb_vector::{HnswIndex, QuantizationMode, VectorIndexRegistry, VectorMetric};

use super::{PayloadWriter, SnapshotError};

pub(super) fn write_index(
    writer: &mut PayloadWriter<'_>,
    name: &[u8],
    index: &HnswIndex,
    graph: &HnswGraph,
) -> Result<usize, SnapshotError> {
    write_key(writer, name)?;
    write_count(writer, index.dim)?;
    for param in [index.m, index.ef_construction, index.ef_search] {
        writer.write_bytes(&(param as u64).to_le_bytes())?;
    }
    let metric = match index.metric {
        VectorMetric::Cosine => 0,
        VectorMetric::L2 => 1,
        VectorMetric::DotProduct => 2,
    };
    let quantization = match index.quantization {
        QuantizationMode::None => 0,
        QuantizationMode::SQ8 => 1,
    };
    writer.write_bytes(&[metric, quantization])?;

    writer.write_bytes(&graph.rng_state.to_le_bytes())?;
    writer.write_bytes(&[u8::from(graph.entry_point.is_some())])?;
    if let Some((id, level)) = graph.entry_point {
        writer.write_bytes(&id.to_le_bytes())?;
        writer.write_bytes(&[level as u8])?;
    }

    write_count(writer, graph.nodes.len())?;
    for node in &graph.nodes {
        writer.write_bytes(&[u8::from(node.is_some())])?;
        let Some(node) = node else { continue };
        write_key(writer, &node.key)?;
        writer.write_bytes(&node.expire_at_secs.to_le_bytes())?;
        writer.write_bytes(&[node.max_layer as u8])?;
        writer.write_bytes(&[u8::from(node.payload.is_some())])?;
        if let Some(payload) = &node.payload {
            write_count(writer, payload.len())?;
            writer.write_bytes(payload)?;
        }
        if let Some((bytes, min, max)) = &node.vector_sq8 {
            writer.write_bytes(&min.to_le_bytes())?;
            writer.write_bytes(&max.to_le_bytes())?;
            writer.write_bytes(bytes)?;
        } else if let Some(vector) = &node.vector_fp32 {
            for value in vector {
                writer.write_bytes(&value.to_le_bytes())?;
            }
        } else {
            return Err(SnapshotError::Invalid("missing HNSW vector"));
        }
    }
    writer.write_bytes(&[graph.layers.len() as u8])?;
    for layer in &graph.layers {
        write_count(writer, layer.len())?;
        for (id, neighbors) in layer {
            writer.write_bytes(&id.to_le_bytes())?;
            write_count(writer, neighbors.len())?;
            for neighbor in neighbors {
                writer.write_bytes(&neighbor.to_le_bytes())?;
            }
        }
    }
    Ok(graph.nodes.iter().flatten().count())
}

fn write_count(writer: &mut PayloadWriter<'_>, count: usize) -> Result<(), SnapshotError> {
    let count =
        u32::try_from(count).map_err(|_| SnapshotError::Invalid("HNSW size out of range"))?;
    writer.write_bytes(&count.to_le_bytes())
}

fn write_key(writer: &mut PayloadWriter<'_>, key: &[u8]) -> Result<(), SnapshotError> {
    let len = u16::try_from(key.len()).map_err(|_| SnapshotError::Invalid("HNSW key too long"))?;
    writer.write_bytes(&len.to_le_bytes())?;
    writer.write_bytes(key)
}

pub(super) fn read_index<'a>(
    bytes: &'a [u8],
    vectors: &VectorIndexRegistry,
) -> Result<(&'a [u8], usize), SnapshotError> {
    let mut cursor = Cursor(bytes);
    let name = cursor.key()?;
    let dim = cursor.u32()? as usize;
    let m = cursor.param()?;
    let ef_construction = cursor.param()?;
    let ef_search = cursor.param()?;
    if dim == 0 || m < 2 || m.checked_mul(2).is_none() {
        return Err(SnapshotError::Invalid("invalid HNSW parameters"));
    }
    let metric = match cursor.byte()? {
        0 => VectorMetric::Cosine,
        1 => VectorMetric::L2,
        2 => VectorMetric::DotProduct,
        _ => return Err(SnapshotError::Invalid("unknown HNSW metric")),
    };
    let quantization = match cursor.byte()? {
        0 => QuantizationMode::None,
        1 => QuantizationMode::SQ8,
        _ => return Err(SnapshotError::Invalid("unknown HNSW quantization")),
    };
    let rng_state = u64::from_le_bytes(cursor.array()?);
    let entry_point = if cursor.present()? {
        Some((cursor.u32()?, usize::from(cursor.byte()?)))
    } else {
        None
    };

    let node_count = cursor.count(1)?;
    let mut nodes = Vec::new();
    nodes
        .try_reserve_exact(node_count)
        .map_err(|_| SnapshotError::Invalid("cannot reserve HNSW nodes"))?;
    for id in 0..node_count {
        let node = if cursor.present()? {
            Some(read_node(&mut cursor, id as u32, dim, quantization)?)
        } else {
            None
        };
        nodes.push(node);
    }
    let layers_count = usize::from(cursor.byte()?);
    if layers_count > 17
        || (node_count != 0 && entry_point.is_none())
        || entry_point.is_some_and(|(id, level)| id as usize >= node_count || level >= layers_count)
        || nodes
            .iter()
            .flatten()
            .any(|node| node.max_layer >= layers_count)
    {
        return Err(SnapshotError::Invalid("invalid HNSW graph levels"));
    }

    let mut layers = Vec::with_capacity(layers_count);
    for _ in 0..layers_count {
        let count = cursor.count(8)?;
        if count > node_count {
            return Err(SnapshotError::Invalid("too many HNSW edges"));
        }
        let mut layer = Vec::with_capacity(count);
        let mut ids = HashSet::with_capacity(count);
        for _ in 0..count {
            let id = cursor.node_id(node_count)?;
            if !ids.insert(id) {
                return Err(SnapshotError::Invalid("duplicate HNSW node in layer"));
            }
            let neighbors = cursor.count(4)?;
            let neighbors = (0..neighbors)
                .map(|_| cursor.node_id(node_count))
                .collect::<Result<Vec<_>, _>>()?;
            layer.push((id, neighbors));
        }
        layers.push(layer);
    }

    if vectors.has_hnsw(name) {
        return Err(SnapshotError::Invalid("duplicate HNSW index name"));
    }
    let index = vectors.create_hnsw(
        name,
        dim,
        m,
        ef_construction,
        ef_search,
        metric,
        quantization,
    );
    let restored = nodes.iter().flatten().count();
    if !index.restore_graph(HnswGraph {
        entry_point,
        nodes,
        layers,
        rng_state,
    }) {
        return Err(SnapshotError::Invalid("duplicate HNSW vector key"));
    }
    Ok((cursor.0, restored))
}

fn read_node(
    cursor: &mut Cursor<'_>,
    id: u32,
    dim: usize,
    quantization: QuantizationMode,
) -> Result<HnswNode, SnapshotError> {
    let key = cursor.key()?;
    if key.is_empty() {
        return Err(SnapshotError::Invalid("empty HNSW vector key"));
    }
    let expire_at_secs = cursor.u32()?;
    let max_layer = usize::from(cursor.byte()?);
    let payload = if cursor.present()? {
        let size = cursor.u32()? as usize;
        Some(cursor.take(size)?.to_vec())
    } else {
        None
    };
    let (vector_fp32, vector_sq8) = match quantization {
        QuantizationMode::None => {
            let len = dim
                .checked_mul(4)
                .ok_or(SnapshotError::Invalid("HNSW dimension out of range"))?;
            let vector = cursor
                .take(len)?
                .as_chunks::<4>()
                .0
                .iter()
                .copied()
                .map(f32::from_le_bytes)
                .collect();
            (Some(vector), None)
        }
        QuantizationMode::SQ8 => {
            let min = f32::from_le_bytes(cursor.array()?);
            let max = f32::from_le_bytes(cursor.array()?);
            let bytes = cursor.take(dim)?.to_vec();
            (None, Some((bytes, min, max)))
        }
    };
    Ok(HnswNode {
        id,
        key: key.to_vec(),
        vector_fp32,
        vector_sq8,
        payload,
        expire_at_secs,
        max_layer,
    })
}

struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8], SnapshotError> {
        let (bytes, remainder) = self
            .0
            .split_at_checked(size)
            .ok_or(SnapshotError::UnexpectedEof)?;
        self.0 = remainder;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], SnapshotError> {
        Ok(self.take(N)?.try_into().unwrap())
    }

    fn byte(&mut self) -> Result<u8, SnapshotError> {
        Ok(self.take(1)?[0])
    }

    fn present(&mut self) -> Result<bool, SnapshotError> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(SnapshotError::Invalid("invalid HNSW presence flag")),
        }
    }

    fn u32(&mut self) -> Result<u32, SnapshotError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn key(&mut self) -> Result<&'a [u8], SnapshotError> {
        let len = usize::from(u16::from_le_bytes(self.array()?));
        self.take(len)
    }

    fn param(&mut self) -> Result<usize, SnapshotError> {
        usize::try_from(u64::from_le_bytes(self.array()?))
            .map_err(|_| SnapshotError::Invalid("HNSW parameter out of range"))
    }

    fn count(&mut self, min_size: usize) -> Result<usize, SnapshotError> {
        let count = self.u32()? as usize;
        if count > self.0.len() / min_size {
            return Err(SnapshotError::UnexpectedEof);
        }
        Ok(count)
    }

    fn node_id(&mut self, count: usize) -> Result<u32, SnapshotError> {
        let id = self.u32()?;
        if id as usize >= count {
            return Err(SnapshotError::Invalid("invalid HNSW node ID"));
        }
        Ok(id)
    }
}
