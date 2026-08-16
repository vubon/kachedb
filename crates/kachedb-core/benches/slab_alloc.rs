//! Criterion micro-benchmarks for `kachedb-core` slab allocation.
//!
//! Target: < 20 ns per `allocate()` call on a warm L1 cache (Phase 0 goal).
//!
//! Run with:
//! ```bash
//! cargo bench -p kachedb-core
//! ```

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kachedb_core::{MegaslabArena, SlabClassType, SlabPool};

// ─── MegaslabArena direct benchmarks ─────────────────────────────────────────

fn bench_arena_alloc_app_small(c: &mut Criterion) {
    let mut arena = MegaslabArena::new(SlabClassType::AppSmall, 0, 0).unwrap();
    c.bench_function("arena::allocate AppSmall (128 B)", |b| {
        b.iter(|| {
            let id = arena.allocate().unwrap();
            // Immediately free so the arena never fills up.
            arena.deallocate(black_box(id)).unwrap();
        });
    });
}

fn bench_arena_alloc_app_medium(c: &mut Criterion) {
    let mut arena = MegaslabArena::new(SlabClassType::AppMedium, 0, 0).unwrap();
    c.bench_function("arena::allocate AppMedium (512 B)", |b| {
        b.iter(|| {
            let id = arena.allocate().unwrap();
            arena.deallocate(black_box(id)).unwrap();
        });
    });
}

fn bench_arena_alloc_app_large(c: &mut Criterion) {
    let mut arena = MegaslabArena::new(SlabClassType::AppLarge, 0, 0).unwrap();
    c.bench_function("arena::allocate AppLarge (4 KB)", |b| {
        b.iter(|| {
            let id = arena.allocate().unwrap();
            arena.deallocate(black_box(id)).unwrap();
        });
    });
}

fn bench_arena_alloc_tensor_64kb(c: &mut Criterion) {
    let mut arena = MegaslabArena::new(SlabClassType::Tensor64KB, 0, 0).unwrap();
    c.bench_function("arena::allocate Tensor64KB (64 KB)", |b| {
        b.iter(|| {
            let id = arena.allocate().unwrap();
            arena.deallocate(black_box(id)).unwrap();
        });
    });
}

fn bench_arena_alloc_tensor_256kb(c: &mut Criterion) {
    let mut arena = MegaslabArena::new(SlabClassType::Tensor256KB, 0, 0).unwrap();
    c.bench_function("arena::allocate Tensor256KB (256 KB)", |b| {
        b.iter(|| {
            let id = arena.allocate().unwrap();
            arena.deallocate(black_box(id)).unwrap();
        });
    });
}

// ─── SlabPool end-to-end benchmark ───────────────────────────────────────────

fn bench_pool_alloc_app_small(c: &mut Criterion) {
    const POOL_64MB: usize = 64 * 1024 * 1024;
    let mut pool = SlabPool::new(0, POOL_64MB).unwrap();
    c.bench_function("pool::allocate+deallocate AppSmall (128 B)", |b| {
        b.iter(|| {
            let id = pool.allocate(SlabClassType::AppSmall).unwrap();
            pool.deallocate(black_box(id)).unwrap();
        });
    });
}

fn bench_pool_alloc_tensor_64kb(c: &mut Criterion) {
    const POOL_256MB: usize = 256 * 1024 * 1024;
    let mut pool = SlabPool::new(0, POOL_256MB).unwrap();
    c.bench_function("pool::allocate+deallocate Tensor64KB (64 KB)", |b| {
        b.iter(|| {
            let id = pool.allocate(SlabClassType::Tensor64KB).unwrap();
            pool.deallocate(black_box(id)).unwrap();
        });
    });
}

// ─── Criterion groups ─────────────────────────────────────────────────────────

criterion_group!(
    arena_benches,
    bench_arena_alloc_app_small,
    bench_arena_alloc_app_medium,
    bench_arena_alloc_app_large,
    bench_arena_alloc_tensor_64kb,
    bench_arena_alloc_tensor_256kb,
);

criterion_group!(
    pool_benches,
    bench_pool_alloc_app_small,
    bench_pool_alloc_tensor_64kb,
);

criterion_main!(arena_benches, pool_benches);
