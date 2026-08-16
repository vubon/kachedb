//! `kachedb-net` — High-performance asynchronous TCP engine with thread-per-core event loops.
//!
//! Exposes a Redis/Valkey wire-compatible TCP server engine executing zero-copy
//! command dispatch directly against core-local `SwissTable` and `SlabPool` structures.
//!
//! # Architecture
//!
//! - **Thread-per-core**: 1 worker thread per physical CPU core.
//! - **Shared-nothing memory**: Zero cross-thread mutex locks during request handling.
//! - **Cross-platform**: Uses `io_uring` on Linux and `mio` on macOS/BSD.

pub mod connection;
pub mod engine;
pub mod error;

pub use connection::Connection;
pub use engine::WorkerThread;
pub use error::NetError;
