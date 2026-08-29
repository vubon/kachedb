//! `kachedb-proto-resp` — Zero-allocation streaming parser & serializer for RESP2 and RESP3 wire protocol.
//!
//! Provides high-throughput framing and command translation for Redis and Valkey compatible wire protocol.
//!
//! # Highlights
//!
//! - **Zero heap allocation**: `Frame<'a>` and `Command<'a>` borrow sub-slices directly from the incoming I/O buffer.
//! - **Streaming parser**: `parse_frame` cleanly handles fragmented TCP stream buffers.
//! - **RESP2 & RESP3 support**: Supports standard arrays, bulk strings, nulls, simple strings, and errors.

pub mod command;
pub mod error;
pub mod frame;

pub use command::{ClientSubcommand, Command, parse_command};
pub use error::RespError;
pub use frame::{
    Frame, encode_array_header, encode_bulk_string, encode_error, encode_integer, encode_null,
    encode_simple_string, parse_frame,
};
