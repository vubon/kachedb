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

impl<'a> Command<'a> {
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
}
