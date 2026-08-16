//! Criterion micro-benchmarks for `kachedb-hash` Swiss Table.
//!
//! Measures:
//! - Insert throughput (1M sequential keys)
//! - Lookup hit latency (warm cache, existing key)
//! - Lookup miss latency (cold probe, absent key)
//!
//! Run with:
//! ```bash
//! cargo bench -p kachedb-hash
//! ```

use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use kachedb_core::SlabBlockId;
use kachedb_hash::{SwissTable, hash_key};

// ─── Helpers ──────────────────────────────────────────────────────────────────

fn make_id(n: u32) -> SlabBlockId {
    SlabBlockId(n)
}

fn build_table(n: usize) -> SwissTable {
    let mut t = SwissTable::with_capacity(n * 2);
    for i in 0..n as u64 {
        t.insert(hash_key(&i.to_le_bytes()), make_id(i as u32), 128).unwrap();
    }
    t
}

// ─── Benchmarks ───────────────────────────────────────────────────────────────

fn bench_insert_1m_sequential(c: &mut Criterion) {
    c.bench_function("swiss_table::insert 1M sequential keys", |b| {
        b.iter_batched(
            || SwissTable::with_capacity(1 << 21), // 2M slots pre-allocated
            |mut t| {
                for i in 0u64..1_000_000 {
                    t.insert(black_box(hash_key(&i.to_le_bytes())), make_id(i as u32), 128)
                        .unwrap();
                }
                t
            },
            BatchSize::LargeInput,
        );
    });
}

fn bench_lookup_hit(c: &mut Criterion) {
    let table = build_table(1_000_000);
    let probe_hash = hash_key(&42u64.to_le_bytes());
    c.bench_function("swiss_table::lookup hit (warm cache)", |b| {
        b.iter(|| {
            black_box(table.lookup(black_box(probe_hash)))
        });
    });
}

fn bench_lookup_miss(c: &mut Criterion) {
    let table = build_table(1_000_000);
    // Key 9_999_999 was never inserted.
    let miss_hash = hash_key(&9_999_999u64.to_le_bytes());
    c.bench_function("swiss_table::lookup miss", |b| {
        b.iter(|| {
            black_box(table.lookup(black_box(miss_hash)))
        });
    });
}

fn bench_remove_and_reinsert(c: &mut Criterion) {
    c.bench_function("swiss_table::remove + reinsert (tombstone cycle)", |b| {
        b.iter_batched(
            || {
                let mut t = SwissTable::with_capacity(128);
                let h = hash_key(b"cycle-key");
                t.insert(h, make_id(1), 64).unwrap();
                (t, h)
            },
            |(mut t, h)| {
                t.remove(black_box(h));
                t.insert(black_box(h), make_id(2), 64).unwrap();
                t
            },
            BatchSize::SmallInput,
        );
    });
}

// ─── Groups ───────────────────────────────────────────────────────────────────

criterion_group!(
    swiss_table_benches,
    bench_insert_1m_sequential,
    bench_lookup_hit,
    bench_lookup_miss,
    bench_remove_and_reinsert,
);

criterion_main!(swiss_table_benches);
