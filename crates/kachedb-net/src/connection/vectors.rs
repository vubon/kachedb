//! Vector & HNSW command execution handlers (`VADD`, `VSEARCH`, `VDEL`, `VSTATS`, `VINDEX...`).

use kachedb_proto_resp::{
    Command, encode_array_header, encode_bulk_string, encode_error, encode_integer, encode_null,
    encode_simple_string,
};
use kachedb_vector::VectorIndexRegistry;

use crate::error::NetError;

/// Executes vector index and similarity search commands.
pub fn handle_vectors(
    cmd: Command<'_>,
    write_buf: &mut Vec<u8>,
    vectors: &VectorIndexRegistry,
    now_sec: u32,
) -> Result<bool, NetError> {
    match cmd {
        Command::VAdd {
            index,
            id,
            dim,
            vector_bytes,
            payload,
            ttl_sec,
            tag_mask,
            parent_key,
        } => {
            if dim.checked_mul(4) != Some(vector_bytes.len()) {
                encode_error(
                    write_buf,
                    &format!(
                        "ERR vector byte length {} does not match dimension {}",
                        vector_bytes.len(),
                        dim,
                    ),
                );
                return Ok(true);
            }
            if id.is_empty() || dim == 0 {
                encode_error(write_buf, "ERR vector ID and dimension must be nonempty");
                return Ok(true);
            }

            #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
            let floats: Vec<f32> = vector_bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect();

            let inserted = if let Some(hnsw) = vectors.get_hnsw(index) {
                hnsw.insert(id, &floats, payload, ttl_sec, now_sec)
            } else {
                vectors
                    .get_or_create(index)
                    .insert(id, &floats, payload, ttl_sec, now_sec, tag_mask, parent_key)
            };
            match inserted {
                Ok(()) => encode_integer(write_buf, 1),
                Err(error) => encode_error(write_buf, &format!("ERR {error}")),
            }
        }
        Command::VSearch {
            index,
            query_bytes,
            top_k,
            threshold,
            filter_mask,
        } => {
            if query_bytes.len() % 4 != 0 {
                encode_error(
                    write_buf,
                    "ERR query vector byte length must be a multiple of 4",
                );
                return Ok(true);
            }

            #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
            let query_floats: Vec<f32> = query_bytes
                .chunks_exact(4)
                .map(|chunk| f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                .collect();

            if let Some(hnsw) = vectors.get_hnsw(index) {
                match hnsw.search(&query_floats, top_k, None, threshold, now_sec) {
                    Ok(results) => {
                        encode_array_header(write_buf, results.len());
                        for r in results {
                            encode_array_header(write_buf, 3);
                            encode_bulk_string(write_buf, &r.key);
                            let score_str = format!("{:.6}", r.similarity);
                            encode_bulk_string(write_buf, score_str.as_bytes());
                            if let Some(ref p) = r.payload {
                                encode_bulk_string(write_buf, p);
                            } else {
                                encode_null(write_buf);
                            }
                        }
                    }
                    Err(e) => {
                        encode_error(write_buf, &format!("ERR {e}"));
                    }
                }
            } else if let Some(vec_idx) = vectors.get(index) {
                match vec_idx.search(&query_floats, top_k, threshold, now_sec, filter_mask) {
                    Ok(results) => {
                        encode_array_header(write_buf, results.len());
                        for r in results {
                            if let Some(ref pk) = r.parent_key {
                                encode_array_header(write_buf, 4);
                                encode_bulk_string(write_buf, &r.key);
                                let score_str = format!("{:.6}", r.similarity);
                                encode_bulk_string(write_buf, score_str.as_bytes());
                                if let Some(ref p) = r.payload {
                                    encode_bulk_string(write_buf, p);
                                } else {
                                    encode_null(write_buf);
                                }
                                encode_bulk_string(write_buf, pk);
                            } else {
                                encode_array_header(write_buf, 3);
                                encode_bulk_string(write_buf, &r.key);
                                let score_str = format!("{:.6}", r.similarity);
                                encode_bulk_string(write_buf, score_str.as_bytes());
                                if let Some(ref p) = r.payload {
                                    encode_bulk_string(write_buf, p);
                                } else {
                                    encode_null(write_buf);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        encode_error(write_buf, &format!("ERR {e}"));
                    }
                }
            } else {
                encode_array_header(write_buf, 0);
            }
        }
        Command::VAddBatch { index, items } => {
            let mut inserted_count = 0i64;
            if let Some(hnsw) = vectors.get_hnsw(index) {
                for item in items {
                    if item.vector_bytes.len() % 4 == 0 {
                        #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
                        let floats: Vec<f32> = item
                            .vector_bytes
                            .chunks_exact(4)
                            .map(|chunk| {
                                f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
                            })
                            .collect();

                        if hnsw
                            .insert(item.id, &floats, item.payload, item.ttl_sec, now_sec)
                            .is_ok()
                        {
                            inserted_count += 1;
                        }
                    }
                }
            } else {
                let vec_idx = vectors.get_or_create(index);
                for item in items {
                    if item.vector_bytes.len() % 4 == 0 {
                        #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
                        let floats: Vec<f32> = item
                            .vector_bytes
                            .chunks_exact(4)
                            .map(|chunk| {
                                f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])
                            })
                            .collect();

                        if vec_idx
                            .insert(
                                item.id,
                                &floats,
                                item.payload,
                                item.ttl_sec,
                                now_sec,
                                0,
                                None,
                            )
                            .is_ok()
                        {
                            inserted_count += 1;
                        }
                    }
                }
            }
            encode_integer(write_buf, inserted_count);
        }
        Command::VSearchBatch {
            index,
            queries,
            top_k,
            threshold,
        } => {
            encode_array_header(write_buf, queries.len());
            if let Some(hnsw) = vectors.get_hnsw(index) {
                for q_bytes in queries {
                    if q_bytes.len() % 4 != 0 {
                        encode_array_header(write_buf, 0);
                        continue;
                    }
                    #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
                    let query_floats: Vec<f32> = q_bytes
                        .chunks_exact(4)
                        .map(|chunk| f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                        .collect();

                    match hnsw.search(&query_floats, top_k, None, threshold, now_sec) {
                        Ok(results) => {
                            encode_array_header(write_buf, results.len());
                            for r in results {
                                encode_array_header(write_buf, 3);
                                encode_bulk_string(write_buf, &r.key);
                                let score_str = format!("{:.6}", r.similarity);
                                encode_bulk_string(write_buf, score_str.as_bytes());
                                if let Some(ref p) = r.payload {
                                    encode_bulk_string(write_buf, p);
                                } else {
                                    encode_null(write_buf);
                                }
                            }
                        }
                        Err(_) => {
                            encode_array_header(write_buf, 0);
                        }
                    }
                }
            } else if let Some(vec_idx) = vectors.get(index) {
                for q_bytes in queries {
                    if q_bytes.len() % 4 != 0 {
                        encode_array_header(write_buf, 0);
                        continue;
                    }
                    #[allow(unknown_lints, clippy::chunks_exact_to_as_chunks)]
                    let query_floats: Vec<f32> = q_bytes
                        .chunks_exact(4)
                        .map(|chunk| f32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                        .collect();

                    match vec_idx.search(&query_floats, top_k, threshold, now_sec, 0) {
                        Ok(results) => {
                            encode_array_header(write_buf, results.len());
                            for r in results {
                                encode_array_header(write_buf, 3);
                                encode_bulk_string(write_buf, &r.key);
                                let score_str = format!("{:.6}", r.similarity);
                                encode_bulk_string(write_buf, score_str.as_bytes());
                                if let Some(ref p) = r.payload {
                                    encode_bulk_string(write_buf, p);
                                } else {
                                    encode_null(write_buf);
                                }
                            }
                        }
                        Err(_) => {
                            encode_array_header(write_buf, 0);
                        }
                    }
                }
            } else {
                for _ in 0..queries.len() {
                    encode_array_header(write_buf, 0);
                }
            }
        }
        Command::VDel { index, id } => {
            let deleted = if let Some(hnsw) = vectors.get_hnsw(index) {
                hnsw.delete(id)
            } else if let Some(vec_idx) = vectors.get(index) {
                vec_idx.delete(id)
            } else {
                false
            };
            encode_integer(write_buf, if deleted { 1 } else { 0 });
        }
        Command::VStats { index } => {
            if let Some(hnsw) = vectors.get_hnsw(index) {
                let stats = hnsw.stats(now_sec);
                encode_array_header(write_buf, 8);
                encode_bulk_string(write_buf, b"dimension");
                encode_integer(write_buf, stats.dimension as i64);
                encode_bulk_string(write_buf, b"total_vectors");
                encode_integer(write_buf, stats.total_vectors as i64);
                encode_bulk_string(write_buf, b"active_vectors");
                encode_integer(write_buf, stats.active_vectors as i64);
                encode_bulk_string(write_buf, b"memory_bytes");
                encode_integer(write_buf, stats.memory_bytes as i64);
            } else if let Some(vec_idx) = vectors.get(index) {
                let stats = vec_idx.stats(now_sec);
                encode_array_header(write_buf, 8);
                encode_bulk_string(write_buf, b"dimension");
                encode_integer(write_buf, stats.dimension as i64);
                encode_bulk_string(write_buf, b"total_vectors");
                encode_integer(write_buf, stats.total_vectors as i64);
                encode_bulk_string(write_buf, b"active_vectors");
                encode_integer(write_buf, stats.active_vectors as i64);
                encode_bulk_string(write_buf, b"memory_bytes");
                encode_integer(write_buf, stats.memory_bytes as i64);
            } else {
                encode_null(write_buf);
            }
        }
        Command::VIndexCreate {
            name,
            dim,
            m,
            ef_construction,
            ef_search,
            metric,
            quantization,
        } => {
            if dim == 0
                || u32::try_from(dim).is_err()
                || m.is_some_and(|m| m.checked_mul(2).is_none())
            {
                encode_error(write_buf, "ERR invalid HNSW dimension or M parameter");
                return Ok(true);
            }
            let metric_val = metric
                .and_then(|m| std::str::from_utf8(m).ok())
                .and_then(kachedb_vector::VectorMetric::parse)
                .unwrap_or_default();
            let quant_val = quantization
                .and_then(|q| std::str::from_utf8(q).ok())
                .and_then(kachedb_vector::QuantizationMode::parse)
                .unwrap_or_default();
            let m_val = m.unwrap_or(16);
            let ef_c = ef_construction.unwrap_or(200);
            let ef_s = ef_search.unwrap_or(50);

            vectors.create_hnsw(name, dim, m_val, ef_c, ef_s, metric_val, quant_val);
            encode_simple_string(write_buf, "OK");
        }
        Command::VIndexDrop { name } => {
            let dropped = vectors.drop_hnsw(name) || vectors.delete_index(name);
            encode_integer(write_buf, if dropped { 1 } else { 0 });
        }
        Command::VIndexInfo { name } => {
            if let Some(hnsw) = vectors.get_hnsw(name) {
                let stats = hnsw.stats(now_sec);
                encode_array_header(write_buf, 12);
                encode_bulk_string(write_buf, b"name");
                encode_bulk_string(write_buf, stats.name.as_bytes());
                encode_bulk_string(write_buf, b"type");
                encode_bulk_string(write_buf, b"hnsw");
                encode_bulk_string(write_buf, b"dimension");
                encode_integer(write_buf, stats.dimension as i64);
                encode_bulk_string(write_buf, b"total_vectors");
                encode_integer(write_buf, stats.total_vectors as i64);
                encode_bulk_string(write_buf, b"active_vectors");
                encode_integer(write_buf, stats.active_vectors as i64);
                encode_bulk_string(write_buf, b"memory_bytes");
                encode_integer(write_buf, stats.memory_bytes as i64);
            } else if let Some(flat) = vectors.get(name) {
                let stats = flat.stats(now_sec);
                encode_array_header(write_buf, 12);
                encode_bulk_string(write_buf, b"name");
                encode_bulk_string(write_buf, stats.name.as_bytes());
                encode_bulk_string(write_buf, b"type");
                encode_bulk_string(write_buf, b"flat");
                encode_bulk_string(write_buf, b"dimension");
                encode_integer(write_buf, stats.dimension as i64);
                encode_bulk_string(write_buf, b"total_vectors");
                encode_integer(write_buf, stats.total_vectors as i64);
                encode_bulk_string(write_buf, b"active_vectors");
                encode_integer(write_buf, stats.active_vectors as i64);
                encode_bulk_string(write_buf, b"memory_bytes");
                encode_integer(write_buf, stats.memory_bytes as i64);
            } else {
                encode_error(write_buf, "ERR no such index");
            }
        }
        _ => unreachable!("handle_vectors called with non-vector command"),
    }

    Ok(true)
}
