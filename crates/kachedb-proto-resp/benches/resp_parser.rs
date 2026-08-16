//! Criterion micro-benchmarks for `kachedb-proto-resp` wire parser.
//!
//! Measures:
//! - Frame parsing latency for PING, GET, SET, MGET commands
//! - Full Command decoding latency
//! - Frame serialization throughput
//!
//! Run with:
//! ```bash
//! cargo bench -p kachedb-proto-resp
//! ```

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use kachedb_proto_resp::{Command, encode_bulk_string, parse_frame};

fn bench_parse_get_command(c: &mut Criterion) {
    let raw_input = b"*2\r\n$3\r\nGET\r\n$16\r\nuser:session:100\r\n";
    c.bench_function("resp::parse + decode GET command", |b| {
        b.iter(|| {
            let (frame, consumed) = parse_frame(black_box(raw_input)).unwrap().unwrap();
            let cmd = Command::from_frame(frame).unwrap();
            black_box((cmd, consumed));
        });
    });
}

fn bench_parse_set_command(c: &mut Criterion) {
    let raw_input = b"*3\r\n$3\r\nSET\r\n$16\r\nuser:session:100\r\n$64\r\n0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\r\n";
    c.bench_function("resp::parse + decode SET command (64B value)", |b| {
        b.iter(|| {
            let (frame, consumed) = parse_frame(black_box(raw_input)).unwrap().unwrap();
            let cmd = Command::from_frame(frame).unwrap();
            black_box((cmd, consumed));
        });
    });
}

fn bench_parse_mget_command(c: &mut Criterion) {
    let raw_input = b"*5\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n$4\r\nkey4\r\n";
    c.bench_function("resp::parse + decode MGET command (4 keys)", |b| {
        b.iter(|| {
            let (frame, consumed) = parse_frame(black_box(raw_input)).unwrap().unwrap();
            let cmd = Command::from_frame(frame).unwrap();
            black_box((cmd, consumed));
        });
    });
}

fn bench_encode_bulk_string(c: &mut Criterion) {
    let payload = b"0123456789abcdef0123456789abcdef";
    let mut buf = Vec::with_capacity(128);

    c.bench_function("resp::encode_bulk_string (32B payload)", |b| {
        b.iter(|| {
            buf.clear();
            encode_bulk_string(&mut buf, black_box(payload));
            black_box(&buf);
        });
    });
}

criterion_group!(
    resp_benches,
    bench_parse_get_command,
    bench_parse_set_command,
    bench_parse_mget_command,
    bench_encode_bulk_string,
);

criterion_main!(resp_benches);
