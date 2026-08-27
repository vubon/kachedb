//! Criterion micro-benchmarks for `kachedb-vector` SIMD operations and vector index search.
//!
//! Measures:
//! - Scalar vs SIMD dot product (384d, 768d, 1536d)
//! - Cosine similarity calculation latency
//! - VectorIndex top-k search throughput (10,000 vectors)
//!
//! Run with:
//! ```bash
//! cargo bench -p kachedb-vector
//! ```

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kachedb_vector::{VectorIndex, dot_product, dot_product_scalar, l2_normalize};

fn generate_vector(dim: usize, seed: f32) -> Vec<f32> {
    let mut v: Vec<f32> = (0..dim).map(|i| (i as f32 * seed).sin()).collect();
    l2_normalize(&mut v);
    v
}

fn bench_dot_product_384d(c: &mut Criterion) {
    let a = generate_vector(384, 0.1);
    let b = generate_vector(384, 0.2);

    let mut group = c.benchmark_group("dot_product_384d");
    group.bench_function("simd", |bench| {
        bench.iter(|| black_box(dot_product(black_box(&a), black_box(&b))));
    });
    group.bench_function("scalar", |bench| {
        bench.iter(|| black_box(dot_product_scalar(black_box(&a), black_box(&b))));
    });
    group.finish();
}

fn bench_dot_product_768d(c: &mut Criterion) {
    let a = generate_vector(768, 0.1);
    let b = generate_vector(768, 0.2);

    let mut group = c.benchmark_group("dot_product_768d");
    group.bench_function("simd", |bench| {
        bench.iter(|| black_box(dot_product(black_box(&a), black_box(&b))));
    });
    group.bench_function("scalar", |bench| {
        bench.iter(|| black_box(dot_product_scalar(black_box(&a), black_box(&b))));
    });
    group.finish();
}

fn bench_dot_product_1536d(c: &mut Criterion) {
    let a = generate_vector(1536, 0.1);
    let b = generate_vector(1536, 0.2);

    let mut group = c.benchmark_group("dot_product_1536d");
    group.bench_function("simd", |bench| {
        bench.iter(|| black_box(dot_product(black_box(&a), black_box(&b))));
    });
    group.bench_function("scalar", |bench| {
        bench.iter(|| black_box(dot_product_scalar(black_box(&a), black_box(&b))));
    });
    group.finish();
}

fn bench_vector_index_search_10k(c: &mut Criterion) {
    let index = VectorIndex::with_dimension("bench_index", 384);
    for i in 0..10_000 {
        let v = generate_vector(384, i as f32 + 1.0);
        let key = format!("doc_{}", i);
        let payload = format!("Payload for item {}", i);
        index
            .insert(key.as_bytes(), &v, Some(payload.as_bytes()), None, 0)
            .unwrap();
    }

    let query = generate_vector(384, 42.0);

    c.bench_function("vector_index::search_10k_top5_384d", |bench| {
        bench.iter(|| {
            black_box(index.search(black_box(&query), 5, 0.5, 0).unwrap());
        });
    });
}

criterion_group!(
    benches,
    bench_dot_product_384d,
    bench_dot_product_768d,
    bench_dot_product_1536d,
    bench_vector_index_search_10k
);
criterion_main!(benches);
