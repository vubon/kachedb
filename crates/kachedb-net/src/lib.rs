#![allow(clippy::collapsible_if, clippy::manual_is_multiple_of)]

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

pub mod accept;
pub mod aof_encode;
pub mod connection;
pub mod engine;
pub mod error;
pub mod tls;

#[cfg(target_os = "linux")]
pub mod engine_uring;

pub use accept::{AcceptDispatcher, create_dispatch_channels};
pub use aof_encode::{AofOp, emit_aof, encode_frame, set_aof_channel};
pub use connection::{Connection, DEFAULT_VECTORS, get_requirepass, set_requirepass};
pub use engine::WorkerThread;
pub use error::NetError;
pub use tls::{TlsState, init_crypto_provider, load_server_config};

#[cfg(target_os = "linux")]
pub use engine_uring::UringWorkerThread;
