//! `kachedb-net` — Error types for the network engine.

use thiserror::Error;

/// Errors returned by the network subsystem.
#[derive(Debug, Error)]
pub enum NetError {
    /// Standard I/O error from socket operations.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// RESP wire protocol framing error.
    #[error("RESP protocol error: {0}")]
    Resp(#[from] kachedb_proto_resp::RespError),

    /// Slab allocation failed during request handling.
    #[error("Slab memory error: {0}")]
    Slab(#[from] kachedb_core::CoreError),

    /// The connection was closed by the client.
    #[error("connection closed by client")]
    ConnectionClosed,

    /// Worker thread initialization failure.
    #[error("failed to start worker thread on core {core_id}: {reason}")]
    WorkerInitFailed { core_id: usize, reason: String },
}
