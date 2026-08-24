//! `kachedb-proto-resp` — Redis command representations and decoder.

use crate::error::RespError;
use crate::frame::Frame;
use smallvec::SmallVec;

/// Strongly typed Redis commands parsed without heap allocation.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Command<'a> {
    /// `PING [message]`
    Ping { message: Option<&'a [u8]> },
    /// `GET <key>`
    Get { key: &'a [u8] },
    /// `SET <key> <value> [EX seconds | PX milliseconds]`
    Set {
        key: &'a [u8],
        value: &'a [u8],
        ttl_ms: Option<u64>,
    },
    /// `MGET <key1> <key2> ...`
    MGet { keys: SmallVec<[&'a [u8]; 8]> },
    /// `DEL <key1> <key2> ...`
    Del { keys: SmallVec<[&'a [u8]; 8]> },
    /// `EXISTS <key1> <key2> ...`
    Exists { keys: SmallVec<[&'a [u8]; 8]> },
    /// `COMMAND ...` (client capability discovery)
    CommandDoc,
    /// `QUIT`
    Quit,
    /// Unrecognized command
    Unknown { name: &'a [u8] },
}

/// Fast zero-allocation parser for Redis wire commands directly from raw TCP stream slices.
///
/// Parses array of bulk strings directly into a stack-allocated [`SmallVec`] without
/// allocating any [`Box`] or heap memory for incoming frames.
pub fn parse_command<'a>(src: &'a [u8]) -> Result<Option<(Command<'a>, usize)>, RespError> {
    if src.is_empty() {
        return Ok(None);
    }

    if src[0] == b'*' {
        // Multi-bulk array command: *<count>\r\n$<len>\r\n<arg1>\r\n...
        let crlf_pos = match find_crlf(&src[1..]) {
            Some(pos) => pos + 1,
            None => return Ok(None),
        };

        let count_str =
            std::str::from_utf8(&src[1..crlf_pos]).map_err(|_| RespError::InvalidInteger)?;
        let count = count_str
            .parse::<i64>()
            .map_err(|_| RespError::InvalidInteger)?;
        if count <= 0 {
            return Err(RespError::EmptyCommand);
        }

        let count = count as usize;
        let mut args: SmallVec<[&'a [u8]; 8]> = SmallVec::with_capacity(count.min(8));
        let mut offset = crlf_pos + 2;

        for _ in 0..count {
            if offset >= src.len() {
                return Ok(None);
            }

            match src[offset] {
                b'$' => {
                    let len_crlf = match find_crlf(&src[offset + 1..]) {
                        Some(pos) => offset + 1 + pos,
                        None => return Ok(None),
                    };

                    let len_str = std::str::from_utf8(&src[offset + 1..len_crlf])
                        .map_err(|_| RespError::InvalidInteger)?;
                    let len = len_str
                        .parse::<i64>()
                        .map_err(|_| RespError::InvalidInteger)?;

                    if len < 0 {
                        // Null bulk string in command argument is invalid
                        return Err(RespError::InvalidInteger);
                    }

                    let len = len as usize;
                    let data_start = len_crlf + 2;
                    let total_needed = data_start + len + 2;

                    if src.len() < total_needed {
                        return Ok(None); // Need more TCP data
                    }

                    if src[data_start + len] != b'\r' || src[data_start + len + 1] != b'\n' {
                        return Err(RespError::MissingCrlf);
                    }

                    args.push(&src[data_start..data_start + len]);
                    offset = total_needed;
                }
                b'+' => {
                    let str_crlf = match find_crlf(&src[offset + 1..]) {
                        Some(pos) => offset + 1 + pos,
                        None => return Ok(None),
                    };
                    args.push(&src[offset + 1..str_crlf]);
                    offset = str_crlf + 2;
                }
                marker => return Err(RespError::InvalidTypeMarker { marker }),
            }
        }

        let cmd = Command::from_raw_args(args)?;
        Ok(Some((cmd, offset)))
    } else {
        // Inline command fallback (e.g. PING\r\n or QUIT\r\n)
        let crlf_pos = match find_crlf(src) {
            Some(pos) => pos,
            None => return Ok(None),
        };

        let line = &src[..crlf_pos];
        if line.eq_ignore_ascii_case(b"PING") {
            Ok(Some((Command::Ping { message: None }, crlf_pos + 2)))
        } else if line.eq_ignore_ascii_case(b"QUIT") {
            Ok(Some((Command::Quit, crlf_pos + 2)))
        } else {
            let mut parts = line
                .split(|&b| b == b' ' || b == b'\t')
                .filter(|p| !p.is_empty());
            if let Some(cmd_name) = parts.next() {
                if cmd_name.eq_ignore_ascii_case(b"PING") {
                    let msg = parts.next();
                    Ok(Some((Command::Ping { message: msg }, crlf_pos + 2)))
                } else if cmd_name.eq_ignore_ascii_case(b"GET") {
                    if let Some(key) = parts.next() {
                        Ok(Some((Command::Get { key }, crlf_pos + 2)))
                    } else {
                        Err(RespError::WrongArgumentCount {
                            command: "GET".into(),
                        })
                    }
                } else {
                    Ok(Some((Command::Unknown { name: cmd_name }, crlf_pos + 2)))
                }
            } else {
                Err(RespError::EmptyCommand)
            }
        }
    }
}

#[inline(always)]
fn find_crlf(src: &[u8]) -> Option<usize> {
    (0..src.len().saturating_sub(1)).find(|&i| src[i] == b'\r' && src[i + 1] == b'\n')
}

impl<'a> Command<'a> {
    /// Decodes a flat list of borrowed argument byte slices into a structured [`Command`].
    ///
    /// 100% Zero heap allocation.
    pub fn from_raw_args(args: SmallVec<[&'a [u8]; 8]>) -> Result<Self, RespError> {
        if args.is_empty() {
            return Err(RespError::EmptyCommand);
        }

        let cmd_name = args[0];

        if cmd_name.eq_ignore_ascii_case(b"GET") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "GET".into(),
                });
            }
            Ok(Command::Get { key: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"SET") {
            if args.len() < 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "SET".into(),
                });
            }
            let key = args[1];
            let value = args[2];

            let mut ttl_ms = None;
            let mut i = 3;
            while i < args.len() {
                let opt = args[i];
                if opt.eq_ignore_ascii_case(b"EX") && i + 1 < args.len() {
                    let sec = std::str::from_utf8(args[i + 1])
                        .unwrap_or("")
                        .parse::<u64>()
                        .unwrap_or(0);
                    if sec > 0 {
                        ttl_ms = Some(sec * 1000);
                    }
                    i += 2;
                    continue;
                } else if opt.eq_ignore_ascii_case(b"PX") && i + 1 < args.len() {
                    let ms = std::str::from_utf8(args[i + 1])
                        .unwrap_or("")
                        .parse::<u64>()
                        .unwrap_or(0);
                    if ms > 0 {
                        ttl_ms = Some(ms);
                    }
                    i += 2;
                    continue;
                }
                i += 1;
            }

            Ok(Command::Set { key, value, ttl_ms })
        } else if cmd_name.eq_ignore_ascii_case(b"PING") {
            let message = if args.len() > 1 { Some(args[1]) } else { None };
            Ok(Command::Ping { message })
        } else if cmd_name.eq_ignore_ascii_case(b"MGET") {
            if args.len() < 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "MGET".into(),
                });
            }
            let mut keys = SmallVec::with_capacity(args.len() - 1);
            for &arg in &args[1..] {
                keys.push(arg);
            }
            Ok(Command::MGet { keys })
        } else if cmd_name.eq_ignore_ascii_case(b"DEL") {
            if args.len() < 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "DEL".into(),
                });
            }
            let mut keys = SmallVec::with_capacity(args.len() - 1);
            for &arg in &args[1..] {
                keys.push(arg);
            }
            Ok(Command::Del { keys })
        } else if cmd_name.eq_ignore_ascii_case(b"EXISTS") {
            if args.len() < 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "EXISTS".into(),
                });
            }
            let mut keys = SmallVec::with_capacity(args.len() - 1);
            for &arg in &args[1..] {
                keys.push(arg);
            }
            Ok(Command::Exists { keys })
        } else if cmd_name.eq_ignore_ascii_case(b"COMMAND") {
            Ok(Command::CommandDoc)
        } else if cmd_name.eq_ignore_ascii_case(b"QUIT") {
            Ok(Command::Quit)
        } else {
            Ok(Command::Unknown { name: cmd_name })
        }
    }

    /// Decodes a parsed [`Frame`] into a structured [`Command`].
    pub fn from_frame(frame: Frame<'a>) -> Result<Self, RespError> {
        match frame {
            Frame::Array(args) => {
                if args.is_empty() {
                    return Err(RespError::EmptyCommand);
                }

                let cmd_name = match args[0].as_ref() {
                    Frame::BulkString(bytes) | Frame::SimpleString(bytes) => *bytes,
                    _ => return Err(RespError::InvalidTypeMarker { marker: b'?' }),
                };

                // Match command name (case-insensitive ASCII)
                if cmd_name.eq_ignore_ascii_case(b"PING") {
                    let message = if args.len() > 1 {
                        extract_bulk_bytes(&args[1])?
                    } else {
                        None
                    };
                    Ok(Command::Ping { message })
                } else if cmd_name.eq_ignore_ascii_case(b"GET") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "GET".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "GET")?;
                    Ok(Command::Get { key })
                } else if cmd_name.eq_ignore_ascii_case(b"SET") {
                    if args.len() < 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "SET".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "SET")?;
                    let value = extract_required_bytes(&args[2], "SET")?;

                    let mut ttl_ms = None;
                    let mut i = 3;
                    while i < args.len() {
                        if let Ok(opt) = extract_required_bytes(&args[i], "SET") {
                            if opt.eq_ignore_ascii_case(b"EX") && i + 1 < args.len() {
                                if let Ok(sec_bytes) = extract_required_bytes(&args[i + 1], "SET") {
                                    let sec = std::str::from_utf8(sec_bytes)
                                        .unwrap_or("")
                                        .parse::<u64>()
                                        .unwrap_or(0);
                                    if sec > 0 {
                                        ttl_ms = Some(sec * 1000);
                                    }
                                }
                                i += 2;
                                continue;
                            } else if opt.eq_ignore_ascii_case(b"PX") && i + 1 < args.len() {
                                if let Ok(ms_bytes) = extract_required_bytes(&args[i + 1], "SET") {
                                    let ms = std::str::from_utf8(ms_bytes)
                                        .unwrap_or("")
                                        .parse::<u64>()
                                        .unwrap_or(0);
                                    if ms > 0 {
                                        ttl_ms = Some(ms);
                                    }
                                }
                                i += 2;
                                continue;
                            }
                        }
                        i += 1;
                    }

                    Ok(Command::Set { key, value, ttl_ms })
                } else if cmd_name.eq_ignore_ascii_case(b"MGET") {
                    if args.len() < 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "MGET".into(),
                        });
                    }
                    let mut keys = SmallVec::with_capacity(args.len() - 1);
                    for arg in &args[1..] {
                        keys.push(extract_required_bytes(arg, "MGET")?);
                    }
                    Ok(Command::MGet { keys })
                } else if cmd_name.eq_ignore_ascii_case(b"DEL") {
                    if args.len() < 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "DEL".into(),
                        });
                    }
                    let mut keys = SmallVec::with_capacity(args.len() - 1);
                    for arg in &args[1..] {
                        keys.push(extract_required_bytes(arg, "DEL")?);
                    }
                    Ok(Command::Del { keys })
                } else if cmd_name.eq_ignore_ascii_case(b"EXISTS") {
                    if args.len() < 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "EXISTS".into(),
                        });
                    }
                    let mut keys = SmallVec::with_capacity(args.len() - 1);
                    for arg in &args[1..] {
                        keys.push(extract_required_bytes(arg, "EXISTS")?);
                    }
                    Ok(Command::Exists { keys })
                } else if cmd_name.eq_ignore_ascii_case(b"COMMAND") {
                    Ok(Command::CommandDoc)
                } else if cmd_name.eq_ignore_ascii_case(b"QUIT") {
                    Ok(Command::Quit)
                } else {
                    Ok(Command::Unknown { name: cmd_name })
                }
            }
            // Inline commands (e.g. raw PING\r\n)
            Frame::SimpleString(s) if s.eq_ignore_ascii_case(b"PING") => {
                Ok(Command::Ping { message: None })
            }
            _ => Err(RespError::InvalidTypeMarker { marker: b'?' }),
        }
    }
}

fn extract_bulk_bytes<'a>(frame: &Frame<'a>) -> Result<Option<&'a [u8]>, RespError> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(Some(*bytes)),
        Frame::Null => Ok(None),
        _ => Err(RespError::InvalidTypeMarker { marker: b'?' }),
    }
}

fn extract_required_bytes<'a>(frame: &Frame<'a>, cmd: &str) -> Result<&'a [u8], RespError> {
    match frame {
        Frame::BulkString(bytes) | Frame::SimpleString(bytes) => Ok(*bytes),
        _ => Err(RespError::WrongArgumentCount {
            command: cmd.into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::parse_frame;

    #[test]
    fn parse_ping_command() {
        let (frame, _) = parse_frame(b"*1\r\n$4\r\nPING\r\n").unwrap().unwrap();
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Ping { message: None });
    }

    #[test]
    fn parse_get_command() {
        let (frame, _) = parse_frame(b"*2\r\n$3\r\nGET\r\n$6\r\nmy_key\r\n")
            .unwrap()
            .unwrap();
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(cmd, Command::Get { key: b"my_key" });
    }

    #[test]
    fn parse_set_command_with_ex() {
        let (frame, _) =
            parse_frame(b"*5\r\n$3\r\nSET\r\n$4\r\nuser\r\n$3\r\n100\r\n$2\r\nEX\r\n$2\r\n60\r\n")
                .unwrap()
                .unwrap();
        let cmd = Command::from_frame(frame).unwrap();
        assert_eq!(
            cmd,
            Command::Set {
                key: b"user",
                value: b"100",
                ttl_ms: Some(60_000),
            }
        );
    }

    #[test]
    fn parse_mget_command() {
        let (frame, _) = parse_frame(b"*3\r\n$4\r\nMGET\r\n$2\r\nk1\r\n$2\r\nk2\r\n")
            .unwrap()
            .unwrap();
        let cmd = Command::from_frame(frame).unwrap();
        match cmd {
            Command::MGet { keys } => {
                assert_eq!(keys.len(), 2);
                assert_eq!(keys[0], b"k1");
                assert_eq!(keys[1], b"k2");
            }
            _ => panic!("expected MGet"),
        }
    }

    #[test]
    fn zero_alloc_parse_command_get() {
        let input = b"*2\r\n$3\r\nGET\r\n$6\r\nmy_key\r\n";
        let (cmd, consumed) = parse_command(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(cmd, Command::Get { key: b"my_key" });
    }

    #[test]
    fn zero_alloc_parse_command_set_ex() {
        let input = b"*5\r\n$3\r\nSET\r\n$4\r\nuser\r\n$3\r\n100\r\n$2\r\nEX\r\n$2\r\n60\r\n";
        let (cmd, consumed) = parse_command(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(
            cmd,
            Command::Set {
                key: b"user",
                value: b"100",
                ttl_ms: Some(60_000),
            }
        );
    }

    #[test]
    fn zero_alloc_parse_command_partial() {
        let partial = b"*2\r\n$3\r\nGET\r\n$6\r\nmy_";
        assert_eq!(parse_command(partial).unwrap(), None);
    }
}
