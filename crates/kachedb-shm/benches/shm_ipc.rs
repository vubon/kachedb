//! Criterion micro-benchmarks for `kachedb-shm` zero-copy shared memory IPC.
//!
//! Measures:
//! - Single-thread push + try_pop roundtrip latency
//! - Cross-thread streaming throughput across SPSC ring buffer in shared memory
//! - Memory mapping attachment latency
//!
//! Run with:
//! ```bash
//! cargo bench -p kachedb-shm
//! ```

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kachedb_core::SlabBlockId;
use kachedb_proto_tensor::{TensorBlockDescriptor, TensorDType};
use kachedb_shm::{IpcSlot, ShmChannel};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

fn make_slot(seq: u64) -> IpcSlot {
    let desc = TensorBlockDescriptor::new(0, 32, 16, 8, 128, TensorDType::BF16, seq);
    IpcSlot::new(desc, SlabBlockId(seq as u32), seq)
}

// ─── Single-Thread Roundtrip Benchmark ────────────────────────────────────────

fn bench_shm_push_pop_roundtrip(c: &mut Criterion) {
    let name = format!("kachedb_bench_rt_{}", std::process::id());
    let ch = ShmChannel::create(&name, 1024).unwrap();
    let slot = make_slot(1);

    c.bench_function("shm::push + try_pop roundtrip (single thread)", |b| {
        b.iter(|| {
            ch.push(black_box(slot)).unwrap();
            let popped = ch.try_pop().unwrap();
            black_box(popped);
        });
    });
}

// ─── Cross-Thread SPSC Streaming Throughput Benchmark ─────────────────────────

fn bench_shm_cross_thread_streaming(c: &mut Criterion) {
    let name = format!("kachedb_bench_stream_{}", std::process::id());
    let ch_producer = Arc::new(ShmChannel::create(&name, 4096).unwrap());
    let ch_consumer = ch_producer.clone();

    c.bench_function("shm::cross-thread SPSC transfer (1,000 slots batch)", |b| {
        b.iter_custom(|iters| {
            let mut total_duration = std::time::Duration::ZERO;

            for _ in 0..iters {
                let count = 1_000u64;
                let ch_p = ch_producer.clone();
                let ch_c = ch_consumer.clone();
                let done = Arc::new(AtomicBool::new(false));
                let done_c = done.clone();

                let start = std::time::Instant::now();

                let consumer_handle = thread::spawn(move || {
                    let mut received = 0;
                    while received < count {
                        if let Ok(slot) = ch_c.try_pop() {
                            black_box(slot);
                            received += 1;
                        } else {
                            std::hint::spin_loop();
                        }
                    }
                    done_c.store(true, Ordering::Release);
                });

                for seq in 0..count {
                    let slot = make_slot(seq);
                    while let Err(_) = ch_p.push(slot) {
                        std::hint::spin_loop();
                    }
                }

                consumer_handle.join().unwrap();
                total_duration += start.elapsed();
            }

            total_duration
        });
    });
}

criterion_group!(
    shm_benches,
    bench_shm_push_pop_roundtrip,
    bench_shm_cross_thread_streaming,
);

criterion_main!(shm_benches);
