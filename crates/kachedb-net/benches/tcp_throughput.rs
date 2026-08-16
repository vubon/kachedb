//! Criterion micro-benchmarks for `kachedb-net` connection processing & command execution.
//!
//! Measures:
//! - In-memory GET/SET execution pipeline throughput (zero TCP overhead baseline)
//! - Direct command dispatch + slab lookup + RESP serialization
//!
//! Run with:
//! ```bash
//! cargo bench -p kachedb-net
//! ```

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kachedb_core::SlabPool;
use kachedb_hash::SwissTable;
use kachedb_net::Connection;

fn bench_in_memory_set_pipeline(c: &mut Criterion) {
    let mut conn = Connection::new();
    let mut table = SwissTable::with_capacity(1024);
    let mut pool = SlabPool::new(0, 64 * 1024 * 1024).unwrap();

    let raw_set = b"*3\r\n$3\r\nSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n";
    let raw_del = b"*2\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n";

    c.bench_function("net::in_memory SET pipeline (parse + alloc + hash + encode)", |b| {
        b.iter(|| {
            // SET
            conn.read_from_stream(&mut std::io::Cursor::new(raw_set)).unwrap();
            conn.process_incoming(&mut table, &mut pool).unwrap();

            // DEL (to reclaim slab slot for next iteration)
            conn.read_from_stream(&mut std::io::Cursor::new(raw_del)).unwrap();
            conn.process_incoming(&mut table, &mut pool).unwrap();

            black_box(&conn);
        });
    });
}

fn bench_in_memory_get_pipeline(c: &mut Criterion) {
    let mut table = SwissTable::with_capacity(1024);
    let mut pool = SlabPool::new(0, 64 * 1024 * 1024).unwrap();

    // Populate key once
    let mut setup_conn = Connection::new();
    setup_conn.read_from_stream(&mut std::io::Cursor::new(b"*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n")).unwrap();
    setup_conn.process_incoming(&mut table, &mut pool).unwrap();

    let raw_get = b"*2\r\n$3\r\nGET\r\n$4\r\nuser\r\n";
    let mut conn = Connection::new();

    c.bench_function("net::in_memory GET hit pipeline (parse + hash + slab + encode)", |b| {
        b.iter(|| {
            conn.read_from_stream(&mut std::io::Cursor::new(raw_get)).unwrap();
            conn.process_incoming(&mut table, &mut pool).unwrap();
            black_box(&conn);
        });
    });
}

criterion_group!(
    net_benches,
    bench_in_memory_set_pipeline,
    bench_in_memory_get_pipeline,
);

criterion_main!(net_benches);
