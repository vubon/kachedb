//! Incremental framing for a single command, before borrowing its arguments.

use std::ops::Range;

use smallvec::SmallVec;

use crate::command::{Command, ParsedCommandFrame, decode_inline};
use crate::error::RespError;

/// Maximum encoded bytes in one request, including the RESP framing.
pub const MAX_COMMAND_BYTES: usize = 64 * 1024 * 1024;
/// Maximum payload bytes in one argument.
pub const MAX_ARGUMENT_BYTES: usize = 2 * 1024 * 1024;
/// Maximum command elements, including the command name.
pub const MAX_ARGUMENTS: usize = 65_536;

// The shortest supported element is an empty RESP simple string.
const MIN_ARGUMENT_WIRE_BYTES: usize = 3;

#[derive(Clone, Copy)]
struct Limits {
    arguments: usize,
    argument_bytes: usize,
}

const INGRESS: Limits = Limits {
    arguments: MAX_ARGUMENTS,
    argument_bytes: MAX_ARGUMENT_BYTES,
};

/// Decodes checked AOF command records written before the ingress limits.
/// These records were already bounded by the journal's 64 MiB record limit.
pub fn parse_command_frame_for_replay(src: &[u8]) -> ParsedCommandFrame<'_> {
    CommandFrameDecoder::with_limits(Limits {
        arguments: MAX_COMMAND_BYTES,
        argument_bytes: MAX_COMMAND_BYTES,
    })
    .parse(src)
}

#[derive(Default)]
struct Decimal {
    value: usize,
    started: bool,
    negative: bool,
    has_digit: bool,
}

impl Decimal {
    fn push(
        &mut self,
        byte: u8,
        limit: usize,
        too_large: impl Fn() -> RespError,
    ) -> Result<(), RespError> {
        if !self.started && matches!(byte, b'+' | b'-') {
            self.negative = byte == b'-';
        } else {
            if !byte.is_ascii_digit() {
                return Err(RespError::InvalidInteger);
            }
            self.has_digit = true;
            self.value = self
                .value
                .checked_mul(10)
                .and_then(|value| value.checked_add(usize::from(byte - b'0')))
                .filter(|&value| value <= limit)
                .ok_or_else(too_large)?;
        }
        self.started = true;
        Ok(())
    }
}

#[derive(Default)]
enum Stage {
    #[default]
    Start,
    Count(Decimal),
    Argument,
    BulkLength(Decimal),
    Bulk(Range<usize>),
    Simple {
        start: usize,
    },
    Inline {
        arguments: usize,
        token_len: usize,
    },
}

/// Tracks framing offsets across reads without keeping references into the input.
/// Pass the same command prefix on each call, with new bytes appended. On success
/// the decoder resets so the caller can advance by the returned byte count.
pub struct CommandFrameDecoder {
    limits: Limits,
    stage: Stage,
    cursor: usize,
    remaining: usize,
}

impl Default for CommandFrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandFrameDecoder {
    pub fn new() -> Self {
        Self::with_limits(INGRESS)
    }

    fn with_limits(limits: Limits) -> Self {
        Self {
            limits,
            stage: Stage::Start,
            cursor: 0,
            remaining: 0,
        }
    }

    /// Returns the next complete frame or keeps the scan position for a partial frame.
    pub fn parse<'a>(&mut self, src: &'a [u8]) -> ParsedCommandFrame<'a> {
        if let Some(consumed) = self.scan(src, |_| {})? {
            *self = Self::with_limits(self.limits);
            if src[0] != b'*' {
                return decode_inline(&src[..consumed - 2], consumed);
            }

            // Materialize argument slices only after validating the complete frame.
            let mut arguments = SmallVec::new();
            let mut decoder = Self::with_limits(self.limits);
            decoder.scan(&src[..consumed], |range| arguments.push(&src[range]))?;
            return Ok(Some((Command::from_raw_args(arguments), consumed)));
        }
        Ok(None)
    }

    fn scan(
        &mut self,
        src: &[u8],
        mut push_argument: impl FnMut(Range<usize>),
    ) -> Result<Option<usize>, RespError> {
        let src = &src[..src.len().min(MAX_COMMAND_BYTES)];
        let max_args = self.limits.arguments;
        let too_many_args = || RespError::TooManyArguments { limit: max_args };
        while let Some(&byte) = src.get(self.cursor) {
            match &mut self.stage {
                Stage::Start => {
                    if byte == b'*' {
                        self.stage = Stage::Count(Decimal::default());
                        self.cursor += 1;
                    } else {
                        self.stage = Stage::Inline {
                            arguments: 0,
                            token_len: 0,
                        };
                    }
                }
                Stage::Count(decimal) => {
                    if byte != b'\r' {
                        decimal.push(byte, self.limits.arguments, too_many_args)?;
                        self.cursor += 1;
                        continue;
                    }
                    if !decimal.has_digit {
                        return Err(RespError::InvalidInteger);
                    }
                    let Some(end) = crlf_end(src, self.cursor)? else {
                        break;
                    };
                    let count = decimal.value;
                    if decimal.negative || count == 0 {
                        return Err(RespError::EmptyCommand);
                    }
                    check_command_size(end, count)?;
                    self.remaining = count;
                    self.cursor = end;
                    self.stage = Stage::Argument;
                }
                Stage::Argument => {
                    self.cursor += 1;
                    self.stage = match byte {
                        b'$' => Stage::BulkLength(Decimal::default()),
                        b'+' => Stage::Simple { start: self.cursor },
                        marker => return Err(RespError::InvalidTypeMarker { marker }),
                    };
                }
                Stage::BulkLength(decimal) => {
                    if byte != b'\r' {
                        decimal.push(byte, self.limits.argument_bytes, || {
                            argument_too_large(self.limits.argument_bytes)
                        })?;
                        self.cursor += 1;
                        continue;
                    }
                    if !decimal.has_digit || (decimal.negative && decimal.value != 0) {
                        return Err(RespError::InvalidInteger);
                    }
                    let Some(start) = crlf_end(src, self.cursor)? else {
                        break;
                    };
                    let end = start
                        .checked_add(decimal.value)
                        .ok_or(RespError::FrameTooLarge { size: usize::MAX })?;
                    check_command_size(end + 2, self.remaining - 1)?;
                    self.cursor = start;
                    self.stage = Stage::Bulk(start..end);
                }
                Stage::Bulk(range) => {
                    let end = range.end;
                    let Some(trailer) = src.get(end..end + 2) else {
                        break;
                    };
                    if trailer != b"\r\n" {
                        return Err(RespError::MissingCrlf);
                    }
                    push_argument(range.clone());
                    self.cursor = end + 2;
                    if self.finish_argument() {
                        return Ok(Some(self.cursor));
                    }
                }
                Stage::Simple { start } => {
                    if byte == b'\r' {
                        match src.get(self.cursor + 1) {
                            Some(b'\n') => {
                                push_argument(*start..self.cursor);
                                self.cursor += 2;
                                check_command_size(self.cursor, self.remaining - 1)?;
                                if self.finish_argument() {
                                    return Ok(Some(self.cursor));
                                }
                                continue;
                            }
                            None => break,
                            _ => {}
                        }
                    }
                    self.cursor += 1;
                    let bytes = self.cursor - *start;
                    if bytes > self.limits.argument_bytes {
                        return Err(RespError::FrameTooLarge { size: bytes });
                    }
                }
                Stage::Inline {
                    arguments,
                    token_len,
                } => {
                    if byte == b'\r' {
                        match src.get(self.cursor + 1) {
                            Some(b'\n') => return Ok(Some(self.cursor + 2)),
                            None => break,
                            _ => {}
                        }
                    }
                    if matches!(byte, b' ' | b'\t') {
                        *token_len = 0;
                    } else {
                        if *token_len == 0 {
                            *arguments += 1;
                            if *arguments > self.limits.arguments {
                                return Err(too_many_args());
                            }
                        }
                        *token_len += 1;
                        if *token_len > self.limits.argument_bytes {
                            return Err(RespError::FrameTooLarge { size: *token_len });
                        }
                    }
                    self.cursor += 1;
                }
            }
        }
        match &self.stage {
            Stage::Start => {}
            Stage::Count(count) => check_command_size(self.cursor + 2, count.value.max(1))?,
            Stage::Argument => check_command_size(self.cursor, self.remaining)?,
            Stage::BulkLength(length) => {
                check_command_size(self.cursor + 4 + length.value, self.remaining - 1)?;
            }
            Stage::Bulk(_) => {} // Its full size was validated with the length header.
            Stage::Simple { .. } => check_command_size(self.cursor + 2, self.remaining - 1)?,
            Stage::Inline { .. } => check_command_size(self.cursor + 2, 0)?,
        }
        Ok(None)
    }

    fn finish_argument(&mut self) -> bool {
        self.remaining -= 1;
        self.stage = Stage::Argument;
        self.remaining == 0
    }
}

fn crlf_end(src: &[u8], offset: usize) -> Result<Option<usize>, RespError> {
    match src.get(offset + 1) {
        Some(b'\n') => Ok(Some(offset + 2)),
        Some(_) => Err(RespError::MissingCrlf),
        None => Ok(None),
    }
}

fn argument_too_large(limit: usize) -> RespError {
    RespError::FrameTooLarge { size: limit + 1 }
}

fn check_command_size(bytes: usize, remaining: usize) -> Result<(), RespError> {
    let size = remaining
        .checked_mul(MIN_ARGUMENT_WIRE_BYTES)
        .and_then(|remaining| bytes.checked_add(remaining))
        .ok_or(RespError::FrameTooLarge { size: usize::MAX })?;
    if size > MAX_COMMAND_BYTES {
        return Err(RespError::FrameTooLarge { size });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use crate::parse_command_frame;

    #[test]
    fn declarations_are_checked_without_their_payload() {
        let result = parse_command_frame(b"*65536\r\n").unwrap();
        assert!(result.is_none());
        assert_eq!(
            parse_command_frame(b"*65537").unwrap_err(),
            RespError::TooManyArguments {
                limit: MAX_ARGUMENTS
            }
        );
        assert!(
            parse_command_frame(b"*1\r\n$2097152\r\n")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            parse_command_frame(b"*1\r\n$2097153").unwrap_err(),
            RespError::FrameTooLarge {
                size: MAX_ARGUMENT_BYTES + 1
            }
        );
        assert_eq!(
            parse_command_frame(b"*1\r\n$184467440737095516160\r\n").unwrap_err(),
            RespError::FrameTooLarge {
                size: MAX_ARGUMENT_BYTES + 1
            }
        );
        assert!(
            parse_command_frame_for_replay(b"*1\r\n$2097153\r\n")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            parse_command_frame(b"*1\r\n$-1\r\n").unwrap_err(),
            RespError::InvalidInteger
        );
    }

    #[test]
    fn incremental_decoder_resumes_split_headers_bodies_and_trailers() {
        let mut decoder = CommandFrameDecoder::new();
        let mut wire = &b"*+02\r\n+PING\r\n$0003\r\na\rb\r\nPING\r\n"[..];
        let first = wire.len() - b"PING\r\n".len();
        for split in 0..first {
            assert!(decoder.parse(&wire[..split]).unwrap().is_none());
        }
        let (command, consumed) = decoder.parse(wire).unwrap().unwrap();
        assert_eq!(consumed, first);
        assert_eq!(
            command.unwrap(),
            Command::Ping {
                message: Some(b"a\rb")
            }
        );
        wire = &wire[consumed..];
        assert!(decoder.parse(&wire[..wire.len() - 1]).unwrap().is_none());
        let (command, consumed) = decoder.parse(wire).unwrap().unwrap();
        assert_eq!(consumed, wire.len());
        assert_eq!(command.unwrap(), Command::Ping { message: None });
    }

    #[test]
    fn counts_and_argument_sizes_apply_to_simple_strings_and_inline_commands() {
        let mut wire = b"*2\r\n+PING\r\n+".to_vec();
        wire.resize(wire.len() + MAX_ARGUMENT_BYTES, b'x');
        let end = wire.len();
        assert!(parse_command_frame(&wire).unwrap().is_none());
        wire.extend_from_slice(b"\r\n");
        assert!(parse_command_frame(&wire).unwrap().unwrap().0.is_ok());
        wire.truncate(end);
        wire.push(b'x');
        assert!(matches!(
            parse_command_frame(&wire),
            Err(RespError::FrameTooLarge { .. })
        ));

        let mut wire = b"PING".to_vec();
        for _ in 1..MAX_ARGUMENTS {
            wire.extend_from_slice(b" x");
        }
        wire.extend_from_slice(b"\r\n");
        assert!(parse_command_frame(&wire).unwrap().unwrap().0.is_ok());
        wire.truncate(wire.len() - 2);
        wire.extend_from_slice(b" x");
        assert_eq!(
            parse_command_frame(&wire).unwrap_err(),
            RespError::TooManyArguments {
                limit: MAX_ARGUMENTS
            }
        );
    }

    #[test]
    fn aggregate_limit_counts_framing_and_resets_for_pipelined_commands() {
        let mut wire = Vec::with_capacity(MAX_COMMAND_BYTES + b"PING\r\n".len());
        wire.extend_from_slice(b"*33\r\n+COMMAND\r\n");
        for _ in 0..31 {
            write!(wire, "${MAX_ARGUMENT_BYTES}\r\n").unwrap();
            wire.resize(wire.len() + MAX_ARGUMENT_BYTES, b'x');
            wire.extend_from_slice(b"\r\n");
        }

        let offset = wire.len();
        write!(wire, "${MAX_ARGUMENT_BYTES}\r\n").unwrap();
        assert!(matches!(
            parse_command_frame(&wire), Err(RespError::FrameTooLarge { size }) if size > MAX_COMMAND_BYTES
        ));
        wire.truncate(offset);

        // The final length still uses seven decimal digits, so its complete
        // header and trailing CRLF together consume twelve bytes.
        let len = MAX_COMMAND_BYTES - offset - 12;
        write!(wire, "${len}\r\n").unwrap();
        wire.resize(wire.len() + len, b'y');
        wire.extend_from_slice(b"\r\nPING\r\n");
        let (command, consumed) = parse_command_frame(&wire).unwrap().unwrap();
        assert_eq!(command.unwrap(), Command::CommandDoc);
        assert_eq!(consumed, MAX_COMMAND_BYTES);
        assert_eq!(
            parse_command_frame(&wire[consumed..])
                .unwrap()
                .unwrap()
                .0
                .unwrap(),
            Command::Ping { message: None }
        );
    }
}
