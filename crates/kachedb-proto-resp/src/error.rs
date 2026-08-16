//! `kachedb-proto-resp` — Error definitions for the RESP protocol parser.

use thiserror::Error;

/// Errors that can occur during RESP frame parsing and command decoding.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum RespError {
    /// The buffer contains an invalid frame prefix byte (e.g. not `*`, `$`, `+`, `-`, `:`, `_`).
    #[error("invalid frame type marker '{marker:#x}'")]
    InvalidTypeMarker { marker: u8 },

    /// The line was not properly terminated with `\r\n`.
    #[error("line missing CRLF terminator")]
    MissingCrlf,

    /// An integer header (e.g. array length or bulk string length) could not be parsed.
    #[error("invalid integer payload in frame")]
    InvalidInteger,

    /// A frame exceeded the maximum supported nested depth or bulk size.
    #[error("frame size exceeds protocol limits: {size} bytes")]
    FrameTooLarge { size: usize },

    /// Command is empty (array of 0 elements).
    #[error("empty command array")]
    EmptyCommand,

    /// Command syntax is invalid (wrong number of arguments).
    #[error("wrong number of arguments for '{command}' command")]
    WrongArgumentCount { command: String },
}
