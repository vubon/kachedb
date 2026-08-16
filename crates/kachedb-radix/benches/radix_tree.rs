//! Criterion micro-benchmarks for `kachedb-radix` prefix tree.
//!
//! Measures:
//! - Insert throughput for long token sequences (1024 tokens = 64 blocks)
//! - Multi-branch tree insertion (shared system prompt + divergent chats)
//! - Lookup latency for various prompt lengths (128, 1024, 4096 tokens)
//! - Lookup miss latency
//! - Bottom-up LRU leaf eviction latency
//!
//! Run with:
//! ```bash
//! cargo bench -p kachedb-radix
//! ```

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use kachedb_core::SlabBlockId;
use kachedb_radix::RadixTree;

fn generate_tokens(start: u32, len: usize) -> Vec<u32> {
    (start..start + len as u32).collect()
}

fn generate_slab_ids(count: usize) -> Vec<SlabBlockId> {
    (0..count as u32).map(SlabBlockId).collect()
}

// ─── Insert Benchmarks ────────────────────────────────────────────────────────

fn bench_insert_1024_tokens(c: &mut Criterion) {
    let tokens = generate_tokens(1, 1024);
    let slabs = generate_slab_ids(64); // 1024 / 16 = 64 blocks

    c.bench_function("radix::insert 1024 tokens (64 blocks)", |b| {
        b.iter_batched(
            RadixTree::new,
            |mut tree| {
                tree.insert(black_box(&tokens), black_box(&slabs)).unwrap();
                tree
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_insert_branching_conversations(c: &mut Criterion) {
    // 512-token shared system prompt + 10 distinct 128-token user turns
    let system_prompt = generate_tokens(1, 512);
    let user_turns: Vec<Vec<u32>> = (0..10)
        .map(|i| {
            let mut seq = system_prompt.clone();
            seq.extend(generate_tokens(10_000 + i * 1000, 128));
            seq
        })
        .collect();
    let slabs = generate_slab_ids(40);

    c.bench_function("radix::insert 10 branching dialogues (shared prefix)", |b| {
        b.iter_batched(
            RadixTree::new,
            |mut tree| {
                for seq in &user_turns {
                    tree.insert(black_box(seq), black_box(&slabs)).unwrap();
                }
                tree
            },
            BatchSize::SmallInput,
        );
    });
}

// ─── Lookup Benchmarks ────────────────────────────────────────────────────────

fn bench_lookup_prefix_128_tokens(c: &mut Criterion) {
    let tokens = generate_tokens(1, 128);
    let slabs = generate_slab_ids(8);
    let mut tree = RadixTree::new();
    tree.insert(&tokens, &slabs).unwrap();

    c.bench_function("radix::lookup hit 128 tokens (8 blocks)", |b| {
        b.iter(|| {
            let res = tree.lookup(black_box(&tokens)).unwrap();
            tree.unpin(black_box(&res.slab_block_ids));
        });
    });
}

fn bench_lookup_prefix_1024_tokens(c: &mut Criterion) {
    let tokens = generate_tokens(1, 1024);
    let slabs = generate_slab_ids(64);
    let mut tree = RadixTree::new();
    tree.insert(&tokens, &slabs).unwrap();

    c.bench_function("radix::lookup hit 1024 tokens (64 blocks)", |b| {
        b.iter(|| {
            let res = tree.lookup(black_box(&tokens)).unwrap();
            tree.unpin(black_box(&res.slab_block_ids));
        });
    });
}

fn bench_lookup_prefix_4096_tokens(c: &mut Criterion) {
    let tokens = generate_tokens(1, 4096);
    let slabs = generate_slab_ids(256);
    let mut tree = RadixTree::new();
    tree.insert(&tokens, &slabs).unwrap();

    c.bench_function("radix::lookup hit 4096 tokens (256 blocks)", |b| {
        b.iter(|| {
            let res = tree.lookup(black_box(&tokens)).unwrap();
            tree.unpin(black_box(&res.slab_block_ids));
        });
    });
}

fn bench_lookup_miss(c: &mut Criterion) {
    let tokens = generate_tokens(1, 1024);
    let slabs = generate_slab_ids(64);
    let mut tree = RadixTree::new();
    tree.insert(&tokens, &slabs).unwrap();

    let miss_tokens = generate_tokens(999_999, 128);

    c.bench_function("radix::lookup miss", |b| {
        b.iter(|| {
            let res = tree.lookup(black_box(&miss_tokens)).unwrap();
            black_box(res);
        });
    });
}

// ─── Eviction Benchmarks ──────────────────────────────────────────────────────

fn bench_evict_lru_leaf(c: &mut Criterion) {
    c.bench_function("radix::evict_lru single leaf", |b| {
        b.iter_batched(
            || {
                let mut tree = RadixTree::new();
                for i in 0..50 {
                    let seq = generate_tokens(i * 100, 32);
                    let slabs = generate_slab_ids(2);
                    tree.insert(&seq, &slabs).unwrap();
                }
                tree
            },
            |mut tree| {
                tree.evict_lru().unwrap();
                tree
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(
    radix_benches,
    bench_insert_1024_tokens,
    bench_insert_branching_conversations,
    bench_lookup_prefix_128_tokens,
    bench_lookup_prefix_1024_tokens,
    bench_lookup_prefix_4096_tokens,
    bench_lookup_miss,
    bench_evict_lru_leaf,
);

criterion_main!(radix_benches);
