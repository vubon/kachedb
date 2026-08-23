# KacheDB: Standardized Benchmark Reproduction Protocol

**Document Version:** 1.0  
**Target Engine:** KacheDB v0.1.0+  

This guide provides the exact methodology, hardware parameters, and commands to independently reproduce the published performance numbers for KacheDB.

---

## 1. Hardware & Environment Reference

To achieve identical peak throughput numbers, tests should be run on a dedicated bare-metal or high-priority virtualized host:

- **CPU:** Multi-core modern x86_64 or ARM64 (e.g. Apple Silicon M-series, AMD EPYC 9004, or Intel Xeon 4th Gen)
- **Linux Kernel:** Linux 6.8+ (for SQPOLL kernel thread pooling and `io_uring` multishot socket support)
- **RAM:** Minimum 16 GB DDR4/DDR5
- **IPC Mount:** `/dev/shm` (POSIX Shared Memory mounted as `tmpfs`)

---

## 2. One-Command Turnkey Reproduction (Docker Linux)

Run the full end-to-end multi-core benchmark in an isolated Linux container with kernel `io_uring` + `SQPOLL`:

```bash
# From the repository root:
make benchmark-reproduce
```

This runs the automated suite inside Docker with `--privileged` and `--ipc host`, measuring:
1. **PING Throughput & Latencies** (50 concurrent clients, pipeline 16)
2. **SET Throughput & Latencies** (50 concurrent clients, pipeline 16, 64-byte payload)
3. **GET Throughput & Latencies** (50 concurrent clients, pipeline 16)

---

## 3. Local Micro-Benchmarks (Criterion.rs)

Run the statistical Criterion micro-benchmarks with nanosecond-level resolution:

```bash
# Memory Allocator & Pool Benchmarks
cargo bench -p kachedb-core

# Swiss Table Point Index Benchmarks
cargo bench -p kachedb-hash

# Full Workspace Benchmarks
cargo bench --workspace
```

### Measured Baselines:
- `arena::allocate (128 B)`: **~3.84 ns**
- `pool::allocate+deallocate`: **~5.40 ns**
- `swiss_table::lookup hit`: **~3.09 ns**
- `radix::lookup (1,024 tokens)`: **~2.45 µs**

---

## 4. Live Multi-Core TCP Benchmarks (`kachedb-bench`)

Start the multi-worker server and load generator manually:

```bash
# Build release binaries
cargo build --release --workspace

# Start server on 4 dedicated cores
./target/release/kachedb-server -p 6379 -w 4 &
SERVER_PID=$!
sleep 1

# Run PING benchmark
./target/release/kachedb-bench -p 6379 -n 100000 -c 50 --pipeline 16 --command PING

# Run SET benchmark
./target/release/kachedb-bench -p 6379 -n 100000 -c 50 --pipeline 16 --command SET

# Run GET benchmark
./target/release/kachedb-bench -p 6379 -n 100000 -c 50 --pipeline 16 --command GET

# Terminate server
kill $SERVER_PID
```
