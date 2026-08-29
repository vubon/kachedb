//! `kachedb-proto-resp` — Redis command representations and decoder.

use crate::error::RespError;
use crate::frame::Frame;
use smallvec::SmallVec;

/// Strongly typed Redis commands parsed without heap allocation.
#[derive(Debug, PartialEq, Clone)]
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
    /// `VADD <index> <id> <dim> <vector_bytes> [PAYLOAD <payload>] [EX <seconds>]`
    VAdd {
        index: &'a [u8],
        id: &'a [u8],
        dim: usize,
        vector_bytes: &'a [u8],
        payload: Option<&'a [u8]>,
        ttl_sec: Option<u32>,
    },
    /// `VSEARCH <index> <query_vector_bytes> [TOPK <k>] [THRESHOLD <min_similarity>]`
    VSearch {
        index: &'a [u8],
        query_bytes: &'a [u8],
        top_k: usize,
        threshold: f32,
    },
    /// `VDEL <index> <id>`
    VDel { index: &'a [u8], id: &'a [u8] },
    /// `VSTATS <index>`
    VStats { index: &'a [u8] },
    /// `EXPIRE <key> <seconds>`
    Expire { key: &'a [u8], seconds: i64 },
    /// `PEXPIRE <key> <milliseconds>`
    PExpire { key: &'a [u8], milliseconds: i64 },
    /// `EXPIREAT <key> <timestamp_sec>`
    ExpireAt { key: &'a [u8], timestamp: i64 },
    /// `PEXPIREAT <key> <timestamp_ms>`
    PExpireAt { key: &'a [u8], timestamp_ms: i64 },
    /// `TTL <key>`
    Ttl { key: &'a [u8] },
    /// `PTTL <key>`
    PTtl { key: &'a [u8] },
    /// `PERSIST <key>`
    Persist { key: &'a [u8] },
    /// `MSET <key1> <value1> <key2> <value2> ...`
    MSet {
        pairs: SmallVec<[(&'a [u8], &'a [u8]); 8]>,
    },
    /// `INCR <key>`
    Incr { key: &'a [u8] },
    /// `DECR <key>`
    Decr { key: &'a [u8] },
    /// `INCRBY <key> <delta>`
    IncrBy { key: &'a [u8], delta: i64 },
    /// `DECRBY <key> <delta>`
    DecrBy { key: &'a [u8], delta: i64 },
    /// `APPEND <key> <value>`
    Append { key: &'a [u8], value: &'a [u8] },
    /// `STRLEN <key>`
    Strlen { key: &'a [u8] },
    /// `HELLO [protover [AUTH username password] [SETNAME name]]`
    Hello {
        protover: Option<i64>,
        auth: Option<(&'a [u8], &'a [u8])>,
        setname: Option<&'a [u8]>,
    },
    /// `CLIENT <subcommand> [args...]`
    Client { subcommand: ClientSubcommand<'a> },
    /// `INFO [section]`
    Info { section: Option<&'a [u8]> },
    /// `COMMAND ...` (client capability discovery)
    CommandDoc,
    /// `QUIT`
    Quit,
    /// Unrecognized command
    Unknown { name: &'a [u8] },
}

/// Subcommand for `CLIENT <subcommand>`.
#[derive(Debug, PartialEq, Clone)]
pub enum ClientSubcommand<'a> {
    SetName(&'a [u8]),
    GetName,
    Id,
    List,
    Unrecognized(&'a [u8]),
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
        } else if cmd_name.eq_ignore_ascii_case(b"VADD") {
            if args.len() < 5 {
                return Err(RespError::WrongArgumentCount {
                    command: "VADD".into(),
                });
            }
            let index = args[1];
            let id = args[2];
            let dim_str = std::str::from_utf8(args[3]).map_err(|_| RespError::InvalidInteger)?;
            let dim = dim_str
                .parse::<usize>()
                .map_err(|_| RespError::InvalidInteger)?;
            let vector_bytes = args[4];

            let mut payload = None;
            let mut ttl_sec = None;
            let mut i = 5;
            while i < args.len() {
                let opt = args[i];
                if opt.eq_ignore_ascii_case(b"PAYLOAD") && i + 1 < args.len() {
                    payload = Some(args[i + 1]);
                    i += 2;
                } else if opt.eq_ignore_ascii_case(b"EX") && i + 1 < args.len() {
                    let sec = std::str::from_utf8(args[i + 1])
                        .unwrap_or("")
                        .parse::<u32>()
                        .unwrap_or(0);
                    if sec > 0 {
                        ttl_sec = Some(sec);
                    }
                    i += 2;
                } else {
                    i += 1;
                }
            }

            Ok(Command::VAdd {
                index,
                id,
                dim,
                vector_bytes,
                payload,
                ttl_sec,
            })
        } else if cmd_name.eq_ignore_ascii_case(b"VSEARCH") {
            if args.len() < 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "VSEARCH".into(),
                });
            }
            let index = args[1];
            let query_bytes = args[2];
            let mut top_k = 1usize;
            let mut threshold = 0.0f32;

            let mut i = 3;
            while i < args.len() {
                let opt = args[i];
                if opt.eq_ignore_ascii_case(b"TOPK") && i + 1 < args.len() {
                    top_k = std::str::from_utf8(args[i + 1])
                        .unwrap_or("")
                        .parse::<usize>()
                        .unwrap_or(1)
                        .max(1);
                    i += 2;
                } else if opt.eq_ignore_ascii_case(b"THRESHOLD") && i + 1 < args.len() {
                    threshold = std::str::from_utf8(args[i + 1])
                        .unwrap_or("")
                        .parse::<f32>()
                        .unwrap_or(0.0);
                    i += 2;
                } else {
                    i += 1;
                }
            }

            Ok(Command::VSearch {
                index,
                query_bytes,
                top_k,
                threshold,
            })
        } else if cmd_name.eq_ignore_ascii_case(b"VDEL") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "VDEL".into(),
                });
            }
            Ok(Command::VDel {
                index: args[1],
                id: args[2],
            })
        } else if cmd_name.eq_ignore_ascii_case(b"VSTATS") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "VSTATS".into(),
                });
            }
            Ok(Command::VStats { index: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"EXPIRE") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "EXPIRE".into(),
                });
            }
            let key = args[1];
            let sec_str = std::str::from_utf8(args[2]).map_err(|_| RespError::InvalidInteger)?;
            let seconds = sec_str
                .parse::<i64>()
                .map_err(|_| RespError::InvalidInteger)?;
            Ok(Command::Expire { key, seconds })
        } else if cmd_name.eq_ignore_ascii_case(b"PEXPIRE") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "PEXPIRE".into(),
                });
            }
            let key = args[1];
            let ms_str = std::str::from_utf8(args[2]).map_err(|_| RespError::InvalidInteger)?;
            let milliseconds = ms_str
                .parse::<i64>()
                .map_err(|_| RespError::InvalidInteger)?;
            Ok(Command::PExpire { key, milliseconds })
        } else if cmd_name.eq_ignore_ascii_case(b"EXPIREAT") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "EXPIREAT".into(),
                });
            }
            let key = args[1];
            let ts_str = std::str::from_utf8(args[2]).map_err(|_| RespError::InvalidInteger)?;
            let timestamp = ts_str
                .parse::<i64>()
                .map_err(|_| RespError::InvalidInteger)?;
            Ok(Command::ExpireAt { key, timestamp })
        } else if cmd_name.eq_ignore_ascii_case(b"PEXPIREAT") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "PEXPIREAT".into(),
                });
            }
            let key = args[1];
            let ts_str = std::str::from_utf8(args[2]).map_err(|_| RespError::InvalidInteger)?;
            let timestamp_ms = ts_str
                .parse::<i64>()
                .map_err(|_| RespError::InvalidInteger)?;
            Ok(Command::PExpireAt { key, timestamp_ms })
        } else if cmd_name.eq_ignore_ascii_case(b"TTL") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "TTL".into(),
                });
            }
            Ok(Command::Ttl { key: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"PTTL") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "PTTL".into(),
                });
            }
            Ok(Command::PTtl { key: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"PERSIST") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "PERSIST".into(),
                });
            }
            Ok(Command::Persist { key: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"MSET") {
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                return Err(RespError::WrongArgumentCount {
                    command: "MSET".into(),
                });
            }
            let pair_count = (args.len() - 1) / 2;
            let mut pairs = SmallVec::with_capacity(pair_count.min(8));
            let mut i = 1;
            while i < args.len() {
                pairs.push((args[i], args[i + 1]));
                i += 2;
            }
            Ok(Command::MSet { pairs })
        } else if cmd_name.eq_ignore_ascii_case(b"INCR") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "INCR".into(),
                });
            }
            Ok(Command::Incr { key: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"DECR") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "DECR".into(),
                });
            }
            Ok(Command::Decr { key: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"INCRBY") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "INCRBY".into(),
                });
            }
            let key = args[1];
            let delta_str = std::str::from_utf8(args[2]).map_err(|_| RespError::InvalidInteger)?;
            let delta = delta_str
                .parse::<i64>()
                .map_err(|_| RespError::InvalidInteger)?;
            Ok(Command::IncrBy { key, delta })
        } else if cmd_name.eq_ignore_ascii_case(b"DECRBY") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "DECRBY".into(),
                });
            }
            let key = args[1];
            let delta_str = std::str::from_utf8(args[2]).map_err(|_| RespError::InvalidInteger)?;
            let delta = delta_str
                .parse::<i64>()
                .map_err(|_| RespError::InvalidInteger)?;
            Ok(Command::DecrBy { key, delta })
        } else if cmd_name.eq_ignore_ascii_case(b"APPEND") {
            if args.len() != 3 {
                return Err(RespError::WrongArgumentCount {
                    command: "APPEND".into(),
                });
            }
            Ok(Command::Append {
                key: args[1],
                value: args[2],
            })
        } else if cmd_name.eq_ignore_ascii_case(b"STRLEN") {
            if args.len() != 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "STRLEN".into(),
                });
            }
            Ok(Command::Strlen { key: args[1] })
        } else if cmd_name.eq_ignore_ascii_case(b"HELLO") {
            let mut protover = None;
            let mut auth = None;
            let mut setname = None;
            let mut idx = 1;

            if let Some(ver) = args
                .get(idx)
                .and_then(|a| std::str::from_utf8(a).ok())
                .and_then(|s| s.parse::<i64>().ok())
            {
                protover = Some(ver);
                idx += 1;
            }

            while idx < args.len() {
                if args[idx].eq_ignore_ascii_case(b"AUTH") {
                    if idx + 2 < args.len() {
                        auth = Some((args[idx + 1], args[idx + 2]));
                        idx += 3;
                    } else {
                        return Err(RespError::WrongArgumentCount {
                            command: "HELLO AUTH".into(),
                        });
                    }
                } else if args[idx].eq_ignore_ascii_case(b"SETNAME") {
                    if idx + 1 < args.len() {
                        setname = Some(args[idx + 1]);
                        idx += 2;
                    } else {
                        return Err(RespError::WrongArgumentCount {
                            command: "HELLO SETNAME".into(),
                        });
                    }
                } else {
                    idx += 1;
                }
            }

            Ok(Command::Hello {
                protover,
                auth,
                setname,
            })
        } else if cmd_name.eq_ignore_ascii_case(b"CLIENT") {
            if args.len() < 2 {
                return Err(RespError::WrongArgumentCount {
                    command: "CLIENT".into(),
                });
            }
            let sub = args[1];
            if sub.eq_ignore_ascii_case(b"SETNAME") {
                if args.len() != 3 {
                    return Err(RespError::WrongArgumentCount {
                        command: "CLIENT SETNAME".into(),
                    });
                }
                Ok(Command::Client {
                    subcommand: ClientSubcommand::SetName(args[2]),
                })
            } else if sub.eq_ignore_ascii_case(b"GETNAME") {
                Ok(Command::Client {
                    subcommand: ClientSubcommand::GetName,
                })
            } else if sub.eq_ignore_ascii_case(b"ID") {
                Ok(Command::Client {
                    subcommand: ClientSubcommand::Id,
                })
            } else if sub.eq_ignore_ascii_case(b"LIST") {
                Ok(Command::Client {
                    subcommand: ClientSubcommand::List,
                })
            } else {
                Ok(Command::Client {
                    subcommand: ClientSubcommand::Unrecognized(sub),
                })
            }
        } else if cmd_name.eq_ignore_ascii_case(b"INFO") {
            let section = if args.len() >= 2 { Some(args[1]) } else { None };
            Ok(Command::Info { section })
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
                } else if cmd_name.eq_ignore_ascii_case(b"VADD") {
                    if args.len() < 5 {
                        return Err(RespError::WrongArgumentCount {
                            command: "VADD".into(),
                        });
                    }
                    let index = extract_required_bytes(&args[1], "VADD")?;
                    let id = extract_required_bytes(&args[2], "VADD")?;
                    let dim_bytes = extract_required_bytes(&args[3], "VADD")?;
                    let dim_str =
                        std::str::from_utf8(dim_bytes).map_err(|_| RespError::InvalidInteger)?;
                    let dim = dim_str
                        .parse::<usize>()
                        .map_err(|_| RespError::InvalidInteger)?;
                    let vector_bytes = extract_required_bytes(&args[4], "VADD")?;

                    let mut payload = None;
                    let mut ttl_sec = None;
                    let mut i = 5;
                    while i < args.len() {
                        if let Ok(opt) = extract_required_bytes(&args[i], "VADD") {
                            if opt.eq_ignore_ascii_case(b"PAYLOAD") && i + 1 < args.len() {
                                if let Ok(p) = extract_required_bytes(&args[i + 1], "VADD") {
                                    payload = Some(p);
                                }
                                i += 2;
                                continue;
                            } else if opt.eq_ignore_ascii_case(b"EX") && i + 1 < args.len() {
                                if let Ok(sec_bytes) = extract_required_bytes(&args[i + 1], "VADD")
                                {
                                    let sec = std::str::from_utf8(sec_bytes)
                                        .unwrap_or("")
                                        .parse::<u32>()
                                        .unwrap_or(0);
                                    if sec > 0 {
                                        ttl_sec = Some(sec);
                                    }
                                }
                                i += 2;
                                continue;
                            }
                        }
                        i += 1;
                    }

                    Ok(Command::VAdd {
                        index,
                        id,
                        dim,
                        vector_bytes,
                        payload,
                        ttl_sec,
                    })
                } else if cmd_name.eq_ignore_ascii_case(b"VSEARCH") {
                    if args.len() < 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "VSEARCH".into(),
                        });
                    }
                    let index = extract_required_bytes(&args[1], "VSEARCH")?;
                    let query_bytes = extract_required_bytes(&args[2], "VSEARCH")?;
                    let mut top_k = 1usize;
                    let mut threshold = 0.0f32;

                    let mut i = 3;
                    while i < args.len() {
                        if let Ok(opt) = extract_required_bytes(&args[i], "VSEARCH") {
                            if opt.eq_ignore_ascii_case(b"TOPK") && i + 1 < args.len() {
                                if let Ok(topk_bytes) =
                                    extract_required_bytes(&args[i + 1], "VSEARCH")
                                {
                                    top_k = std::str::from_utf8(topk_bytes)
                                        .unwrap_or("")
                                        .parse::<usize>()
                                        .unwrap_or(1)
                                        .max(1);
                                }
                                i += 2;
                                continue;
                            } else if opt.eq_ignore_ascii_case(b"THRESHOLD") && i + 1 < args.len() {
                                if let Ok(th_bytes) =
                                    extract_required_bytes(&args[i + 1], "VSEARCH")
                                {
                                    threshold = std::str::from_utf8(th_bytes)
                                        .unwrap_or("")
                                        .parse::<f32>()
                                        .unwrap_or(0.0);
                                }
                                i += 2;
                                continue;
                            }
                        }
                        i += 1;
                    }

                    Ok(Command::VSearch {
                        index,
                        query_bytes,
                        top_k,
                        threshold,
                    })
                } else if cmd_name.eq_ignore_ascii_case(b"VDEL") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "VDEL".into(),
                        });
                    }
                    let index = extract_required_bytes(&args[1], "VDEL")?;
                    let id = extract_required_bytes(&args[2], "VDEL")?;
                    Ok(Command::VDel { index, id })
                } else if cmd_name.eq_ignore_ascii_case(b"VSTATS") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "VSTATS".into(),
                        });
                    }
                    let index = extract_required_bytes(&args[1], "VSTATS")?;
                    Ok(Command::VStats { index })
                } else if cmd_name.eq_ignore_ascii_case(b"EXPIRE") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "EXPIRE".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "EXPIRE")?;
                    let sec_bytes = extract_required_bytes(&args[2], "EXPIRE")?;
                    let sec_str =
                        std::str::from_utf8(sec_bytes).map_err(|_| RespError::InvalidInteger)?;
                    let seconds = sec_str
                        .parse::<i64>()
                        .map_err(|_| RespError::InvalidInteger)?;
                    Ok(Command::Expire { key, seconds })
                } else if cmd_name.eq_ignore_ascii_case(b"PEXPIRE") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "PEXPIRE".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "PEXPIRE")?;
                    let ms_bytes = extract_required_bytes(&args[2], "PEXPIRE")?;
                    let ms_str =
                        std::str::from_utf8(ms_bytes).map_err(|_| RespError::InvalidInteger)?;
                    let milliseconds = ms_str
                        .parse::<i64>()
                        .map_err(|_| RespError::InvalidInteger)?;
                    Ok(Command::PExpire { key, milliseconds })
                } else if cmd_name.eq_ignore_ascii_case(b"EXPIREAT") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "EXPIREAT".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "EXPIREAT")?;
                    let ts_bytes = extract_required_bytes(&args[2], "EXPIREAT")?;
                    let ts_str =
                        std::str::from_utf8(ts_bytes).map_err(|_| RespError::InvalidInteger)?;
                    let timestamp = ts_str
                        .parse::<i64>()
                        .map_err(|_| RespError::InvalidInteger)?;
                    Ok(Command::ExpireAt { key, timestamp })
                } else if cmd_name.eq_ignore_ascii_case(b"PEXPIREAT") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "PEXPIREAT".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "PEXPIREAT")?;
                    let ts_bytes = extract_required_bytes(&args[2], "PEXPIREAT")?;
                    let ts_str =
                        std::str::from_utf8(ts_bytes).map_err(|_| RespError::InvalidInteger)?;
                    let timestamp_ms = ts_str
                        .parse::<i64>()
                        .map_err(|_| RespError::InvalidInteger)?;
                    Ok(Command::PExpireAt { key, timestamp_ms })
                } else if cmd_name.eq_ignore_ascii_case(b"TTL") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "TTL".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "TTL")?;
                    Ok(Command::Ttl { key })
                } else if cmd_name.eq_ignore_ascii_case(b"PTTL") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "PTTL".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "PTTL")?;
                    Ok(Command::PTtl { key })
                } else if cmd_name.eq_ignore_ascii_case(b"PERSIST") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "PERSIST".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "PERSIST")?;
                    Ok(Command::Persist { key })
                } else if cmd_name.eq_ignore_ascii_case(b"MSET") {
                    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
                        return Err(RespError::WrongArgumentCount {
                            command: "MSET".into(),
                        });
                    }
                    let pair_count = (args.len() - 1) / 2;
                    let mut pairs = SmallVec::with_capacity(pair_count.min(8));
                    let mut i = 1;
                    while i < args.len() {
                        let k = extract_required_bytes(&args[i], "MSET")?;
                        let v = extract_required_bytes(&args[i + 1], "MSET")?;
                        pairs.push((k, v));
                        i += 2;
                    }
                    Ok(Command::MSet { pairs })
                } else if cmd_name.eq_ignore_ascii_case(b"INCR") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "INCR".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "INCR")?;
                    Ok(Command::Incr { key })
                } else if cmd_name.eq_ignore_ascii_case(b"DECR") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "DECR".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "DECR")?;
                    Ok(Command::Decr { key })
                } else if cmd_name.eq_ignore_ascii_case(b"INCRBY") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "INCRBY".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "INCRBY")?;
                    let delta_bytes = extract_required_bytes(&args[2], "INCRBY")?;
                    let delta_str =
                        std::str::from_utf8(delta_bytes).map_err(|_| RespError::InvalidInteger)?;
                    let delta = delta_str
                        .parse::<i64>()
                        .map_err(|_| RespError::InvalidInteger)?;
                    Ok(Command::IncrBy { key, delta })
                } else if cmd_name.eq_ignore_ascii_case(b"DECRBY") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "DECRBY".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "DECRBY")?;
                    let delta_bytes = extract_required_bytes(&args[2], "DECRBY")?;
                    let delta_str =
                        std::str::from_utf8(delta_bytes).map_err(|_| RespError::InvalidInteger)?;
                    let delta = delta_str
                        .parse::<i64>()
                        .map_err(|_| RespError::InvalidInteger)?;
                    Ok(Command::DecrBy { key, delta })
                } else if cmd_name.eq_ignore_ascii_case(b"APPEND") {
                    if args.len() != 3 {
                        return Err(RespError::WrongArgumentCount {
                            command: "APPEND".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "APPEND")?;
                    let value = extract_required_bytes(&args[2], "APPEND")?;
                    Ok(Command::Append { key, value })
                } else if cmd_name.eq_ignore_ascii_case(b"STRLEN") {
                    if args.len() != 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "STRLEN".into(),
                        });
                    }
                    let key = extract_required_bytes(&args[1], "STRLEN")?;
                    Ok(Command::Strlen { key })
                } else if cmd_name.eq_ignore_ascii_case(b"HELLO") {
                    let mut protover = None;
                    let mut auth = None;
                    let mut setname = None;
                    let mut idx = 1;

                    if let Some(ver) = args
                        .get(idx)
                        .and_then(|f| extract_required_bytes(f, "HELLO").ok())
                        .and_then(|b| std::str::from_utf8(b).ok())
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        protover = Some(ver);
                        idx += 1;
                    }

                    while idx < args.len() {
                        if let Ok(flag_bytes) = extract_required_bytes(&args[idx], "HELLO") {
                            if flag_bytes.eq_ignore_ascii_case(b"AUTH") {
                                if idx + 2 < args.len() {
                                    let u = extract_required_bytes(&args[idx + 1], "HELLO AUTH")?;
                                    let p = extract_required_bytes(&args[idx + 2], "HELLO AUTH")?;
                                    auth = Some((u, p));
                                    idx += 3;
                                } else {
                                    return Err(RespError::WrongArgumentCount {
                                        command: "HELLO AUTH".into(),
                                    });
                                }
                            } else if flag_bytes.eq_ignore_ascii_case(b"SETNAME") {
                                if idx + 1 < args.len() {
                                    let n =
                                        extract_required_bytes(&args[idx + 1], "HELLO SETNAME")?;
                                    setname = Some(n);
                                    idx += 2;
                                } else {
                                    return Err(RespError::WrongArgumentCount {
                                        command: "HELLO SETNAME".into(),
                                    });
                                }
                            } else {
                                idx += 1;
                            }
                        } else {
                            idx += 1;
                        }
                    }

                    Ok(Command::Hello {
                        protover,
                        auth,
                        setname,
                    })
                } else if cmd_name.eq_ignore_ascii_case(b"CLIENT") {
                    if args.len() < 2 {
                        return Err(RespError::WrongArgumentCount {
                            command: "CLIENT".into(),
                        });
                    }
                    let sub = extract_required_bytes(&args[1], "CLIENT")?;
                    if sub.eq_ignore_ascii_case(b"SETNAME") {
                        if args.len() != 3 {
                            return Err(RespError::WrongArgumentCount {
                                command: "CLIENT SETNAME".into(),
                            });
                        }
                        let name = extract_required_bytes(&args[2], "CLIENT SETNAME")?;
                        Ok(Command::Client {
                            subcommand: ClientSubcommand::SetName(name),
                        })
                    } else if sub.eq_ignore_ascii_case(b"GETNAME") {
                        Ok(Command::Client {
                            subcommand: ClientSubcommand::GetName,
                        })
                    } else if sub.eq_ignore_ascii_case(b"ID") {
                        Ok(Command::Client {
                            subcommand: ClientSubcommand::Id,
                        })
                    } else if sub.eq_ignore_ascii_case(b"LIST") {
                        Ok(Command::Client {
                            subcommand: ClientSubcommand::List,
                        })
                    } else {
                        Ok(Command::Client {
                            subcommand: ClientSubcommand::Unrecognized(sub),
                        })
                    }
                } else if cmd_name.eq_ignore_ascii_case(b"INFO") {
                    let section = if args.len() >= 2 {
                        Some(extract_required_bytes(&args[1], "INFO")?)
                    } else {
                        None
                    };
                    Ok(Command::Info { section })
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

    #[test]
    fn parse_vadd_command() {
        // VADD faq doc1 3 <bytes> PAYLOAD "my answer" EX 3600
        let vec_bytes = [0u8; 12];
        let mut input = Vec::new();
        input.extend_from_slice(
            b"*9\r\n$4\r\nVADD\r\n$3\r\nfaq\r\n$4\r\ndoc1\r\n$1\r\n3\r\n$12\r\n",
        );
        input.extend_from_slice(&vec_bytes);
        input.extend_from_slice(
            b"\r\n$7\r\nPAYLOAD\r\n$9\r\nmy answer\r\n$2\r\nEX\r\n$4\r\n3600\r\n",
        );

        let (cmd, consumed) = parse_command(&input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(
            cmd,
            Command::VAdd {
                index: b"faq",
                id: b"doc1",
                dim: 3,
                vector_bytes: &vec_bytes,
                payload: Some(b"my answer"),
                ttl_sec: Some(3600),
            }
        );
    }

    #[test]
    fn parse_vsearch_command() {
        // VSEARCH faq <bytes> TOPK 5 THRESHOLD 0.88
        let vec_bytes = [0u8; 12];
        let mut input = Vec::new();
        input.extend_from_slice(b"*7\r\n$7\r\nVSEARCH\r\n$3\r\nfaq\r\n$12\r\n");
        input.extend_from_slice(&vec_bytes);
        input.extend_from_slice(b"\r\n$4\r\nTOPK\r\n$1\r\n5\r\n$9\r\nTHRESHOLD\r\n$4\r\n0.88\r\n");

        let (cmd, consumed) = parse_command(&input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(
            cmd,
            Command::VSearch {
                index: b"faq",
                query_bytes: &vec_bytes,
                top_k: 5,
                threshold: 0.88,
            }
        );
    }

    #[test]
    fn parse_vdel_and_vstats() {
        let (del_cmd, _) = parse_command(b"*3\r\n$4\r\nVDEL\r\n$3\r\nfaq\r\n$4\r\ndoc1\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            del_cmd,
            Command::VDel {
                index: b"faq",
                id: b"doc1"
            }
        );

        let (stats_cmd, _) = parse_command(b"*2\r\n$6\r\nVSTATS\r\n$3\r\nfaq\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(stats_cmd, Command::VStats { index: b"faq" });
    }

    #[test]
    fn parse_expire_and_pexpire_commands() {
        let (exp_cmd, _) = parse_command(b"*3\r\n$6\r\nEXPIRE\r\n$7\r\nsession\r\n$3\r\n300\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            exp_cmd,
            Command::Expire {
                key: b"session",
                seconds: 300
            }
        );

        let (pexp_cmd, _) =
            parse_command(b"*3\r\n$7\r\nPEXPIRE\r\n$7\r\nsession\r\n$5\r\n50000\r\n")
                .unwrap()
                .unwrap();
        assert_eq!(
            pexp_cmd,
            Command::PExpire {
                key: b"session",
                milliseconds: 50000
            }
        );
    }

    #[test]
    fn parse_expireat_and_pexpireat_commands() {
        let (eat_cmd, _) =
            parse_command(b"*3\r\n$8\r\nEXPIREAT\r\n$3\r\nkey\r\n$10\r\n1893456000\r\n")
                .unwrap()
                .unwrap();
        assert_eq!(
            eat_cmd,
            Command::ExpireAt {
                key: b"key",
                timestamp: 1893456000
            }
        );

        let (peat_cmd, _) =
            parse_command(b"*3\r\n$9\r\nPEXPIREAT\r\n$3\r\nkey\r\n$13\r\n1893456000000\r\n")
                .unwrap()
                .unwrap();
        assert_eq!(
            peat_cmd,
            Command::PExpireAt {
                key: b"key",
                timestamp_ms: 1893456000000
            }
        );
    }

    #[test]
    fn parse_ttl_pttl_and_persist_commands() {
        let (ttl_cmd, _) = parse_command(b"*2\r\n$3\r\nTTL\r\n$4\r\nuser\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(ttl_cmd, Command::Ttl { key: b"user" });

        let (pttl_cmd, _) = parse_command(b"*2\r\n$4\r\nPTTL\r\n$4\r\nuser\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(pttl_cmd, Command::PTtl { key: b"user" });

        let (persist_cmd, _) = parse_command(b"*2\r\n$7\r\nPERSIST\r\n$4\r\nuser\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(persist_cmd, Command::Persist { key: b"user" });
    }

    #[test]
    fn parse_mset_command() {
        let (cmd, _) = parse_command(
            b"*7\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n$2\r\nk3\r\n$2\r\nv3\r\n",
        )
        .unwrap()
        .unwrap();

        match cmd {
            Command::MSet { pairs } => {
                assert_eq!(pairs.len(), 3);
                assert_eq!(pairs[0], (b"k1".as_slice(), b"v1".as_slice()));
                assert_eq!(pairs[1], (b"k2".as_slice(), b"v2".as_slice()));
                assert_eq!(pairs[2], (b"k3".as_slice(), b"v3".as_slice()));
            }
            _ => panic!("Expected MSet command"),
        }
    }

    #[test]
    fn parse_incr_decr_and_by_commands() {
        let (incr_cmd, _) = parse_command(b"*2\r\n$4\r\nINCR\r\n$3\r\nctr\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(incr_cmd, Command::Incr { key: b"ctr" });

        let (decr_cmd, _) = parse_command(b"*2\r\n$4\r\nDECR\r\n$3\r\nctr\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(decr_cmd, Command::Decr { key: b"ctr" });

        let (incrby_cmd, _) = parse_command(b"*3\r\n$6\r\nINCRBY\r\n$3\r\nctr\r\n$2\r\n10\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            incrby_cmd,
            Command::IncrBy {
                key: b"ctr",
                delta: 10
            }
        );

        let (decrby_cmd, _) = parse_command(b"*3\r\n$6\r\nDECRBY\r\n$3\r\nctr\r\n$1\r\n5\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            decrby_cmd,
            Command::DecrBy {
                key: b"ctr",
                delta: 5
            }
        );
    }

    #[test]
    fn parse_append_and_strlen_commands() {
        let (app_cmd, _) = parse_command(b"*3\r\n$6\r\nAPPEND\r\n$3\r\nmsg\r\n$5\r\nworld\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            app_cmd,
            Command::Append {
                key: b"msg",
                value: b"world"
            }
        );

        let (str_cmd, _) = parse_command(b"*2\r\n$6\r\nSTRLEN\r\n$3\r\nmsg\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(str_cmd, Command::Strlen { key: b"msg" });
    }

    #[test]
    fn parse_hello_client_and_info_commands() {
        let (hello_cmd, _) =
            parse_command(b"*4\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$7\r\nSETNAME\r\n$9\r\nmy-client\r\n")
                .unwrap()
                .unwrap();
        assert_eq!(
            hello_cmd,
            Command::Hello {
                protover: Some(3),
                auth: None,
                setname: Some(b"my-client")
            }
        );

        let (client_set_cmd, _) =
            parse_command(b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$7\r\nworker1\r\n")
                .unwrap()
                .unwrap();
        assert_eq!(
            client_set_cmd,
            Command::Client {
                subcommand: ClientSubcommand::SetName(b"worker1")
            }
        );

        let (client_get_cmd, _) = parse_command(b"*2\r\n$6\r\nCLIENT\r\n$7\r\nGETNAME\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            client_get_cmd,
            Command::Client {
                subcommand: ClientSubcommand::GetName
            }
        );

        let (info_cmd, _) = parse_command(b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(
            info_cmd,
            Command::Info {
                section: Some(b"server")
            }
        );
    }
}
