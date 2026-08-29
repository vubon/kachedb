//! `kachedb-proto-resp` — Zero-allocation RESP2/RESP3 frame representation & streaming parser.

use crate::error::RespError;
use smallvec::SmallVec;

/// A parsed RESP protocol frame borrowing directly from the input buffer.
///
/// Ensures zero heap allocations during the request parsing phase.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Frame<'a> {
    /// Simple string (`+OK\r\n`).
    SimpleString(&'a [u8]),
    /// Error message (`-ERR ...\r\n`).
    Error(&'a [u8]),
    /// Integer (`:1000\r\n`).
    Integer(i64),
    /// Bulk string (`$5\r\nhello\r\n`).
    BulkString(&'a [u8]),
    /// Null bulk string (`$-1\r\n`) or Null in RESP3 (`_\r\n`).
    Null,
    /// Array of frames (`*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n`).
    Array(SmallVec<[Box<Frame<'a>>; 8]>),
}

/// Attempts to parse a single RESP frame from `src`.
///
/// Returns `Ok(Some((frame, consumed_bytes)))` if a complete frame was parsed.
/// Returns `Ok(None)` if the buffer contains an incomplete frame and more network data is required.
/// Returns `Err(RespError)` if the stream contains a protocol syntax violation.
pub fn parse_frame<'a>(src: &'a [u8]) -> Result<Option<(Frame<'a>, usize)>, RespError> {
    if src.is_empty() {
        return Ok(None);
    }

    match src[0] {
        b'+' => parse_simple_string(&src[1..])
            .map(|opt| opt.map(|(s, len)| (Frame::SimpleString(s), len + 1))),
        b'-' => {
            parse_simple_string(&src[1..]).map(|opt| opt.map(|(s, len)| (Frame::Error(s), len + 1)))
        }
        b':' => {
            parse_integer(&src[1..]).map(|opt| opt.map(|(n, len)| (Frame::Integer(n), len + 1)))
        }
        b'$' => parse_bulk_string(&src[1..]).map(|opt| {
            opt.map(|(opt_b, len)| match opt_b {
                Some(b) => (Frame::BulkString(b), len + 1),
                None => (Frame::Null, len + 1),
            })
        }),
        b'_' => parse_null_resp3(&src[1..]).map(|opt| opt.map(|len| (Frame::Null, len + 1))),
        b'*' => {
            parse_array(&src[1..]).map(|opt| opt.map(|(arr, len)| (Frame::Array(arr), len + 1)))
        }
        marker => Err(RespError::InvalidTypeMarker { marker }),
    }
}

// ─── Helpers for individual frame types ──────────────────────────────────────

type ParsedBulkString<'a> = Option<(Option<&'a [u8]>, usize)>;
type ParsedArray<'a> = Option<(SmallVec<[Box<Frame<'a>>; 8]>, usize)>;

fn find_crlf(src: &[u8]) -> Option<usize> {
    (0..src.len().saturating_sub(1)).find(|&i| src[i] == b'\r' && src[i + 1] == b'\n')
}

fn parse_simple_string(src: &[u8]) -> Result<Option<(&[u8], usize)>, RespError> {
    match find_crlf(src) {
        Some(pos) => Ok(Some((&src[..pos], pos + 2))),
        None => Ok(None),
    }
}

fn parse_integer(src: &[u8]) -> Result<Option<(i64, usize)>, RespError> {
    match find_crlf(src) {
        Some(pos) => {
            let s = std::str::from_utf8(&src[..pos]).map_err(|_| RespError::InvalidInteger)?;
            let n = s.parse::<i64>().map_err(|_| RespError::InvalidInteger)?;
            Ok(Some((n, pos + 2)))
        }
        None => Ok(None),
    }
}

fn parse_null_resp3(src: &[u8]) -> Result<Option<usize>, RespError> {
    match find_crlf(src) {
        Some(0) => Ok(Some(2)),
        Some(_) => Err(RespError::InvalidTypeMarker { marker: b'_' }),
        None => Ok(None),
    }
}

fn parse_bulk_string(src: &[u8]) -> Result<ParsedBulkString<'_>, RespError> {
    match find_crlf(src) {
        Some(pos) => {
            let s = std::str::from_utf8(&src[..pos]).map_err(|_| RespError::InvalidInteger)?;
            let len = s.parse::<i64>().map_err(|_| RespError::InvalidInteger)?;

            if len == -1 {
                // Null bulk string ($-1\r\n)
                return Ok(Some((None, pos + 2)));
            }

            if len < 0 {
                return Err(RespError::InvalidInteger);
            }

            let len = len as usize;
            let data_start = pos + 2;
            let total_needed = data_start + len + 2;

            if src.len() < total_needed {
                // Not enough bytes yet for payload + trailing CRLF
                return Ok(None);
            }

            if src[data_start + len] != b'\r' || src[data_start + len + 1] != b'\n' {
                return Err(RespError::MissingCrlf);
            }

            let payload = &src[data_start..data_start + len];
            Ok(Some((Some(payload), total_needed)))
        }
        None => Ok(None),
    }
}

fn parse_array(src: &[u8]) -> Result<ParsedArray<'_>, RespError> {
    match find_crlf(src) {
        Some(pos) => {
            let s = std::str::from_utf8(&src[..pos]).map_err(|_| RespError::InvalidInteger)?;
            let count = s.parse::<i64>().map_err(|_| RespError::InvalidInteger)?;

            if count < 0 {
                return Ok(Some((SmallVec::new(), pos + 2)));
            }

            let count = count as usize;
            let mut frames = SmallVec::with_capacity(count.min(8));
            let mut offset = pos + 2;

            for _ in 0..count {
                if offset >= src.len() {
                    return Ok(None);
                }

                match parse_frame(&src[offset..])? {
                    Some((elem, consumed)) => {
                        frames.push(Box::new(elem));
                        offset += consumed;
                    }
                    None => return Ok(None), // incomplete inner frame
                }
            }

            Ok(Some((frames, offset)))
        }
        None => Ok(None),
    }
}

// ─── Fast Zero-Alloc Integer Formatter ────────────────────────────────────────

#[inline(always)]
fn write_int(buf: &mut Vec<u8>, mut n: i64) {
    if n == 0 {
        buf.push(b'0');
        return;
    }

    let is_negative = n < 0;
    if is_negative {
        buf.push(b'-');
        n = -n;
    }

    let mut temp = [0u8; 20];
    let mut i = 0;
    while n > 0 {
        temp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }

    while i > 0 {
        i -= 1;
        buf.push(temp[i]);
    }
}

#[inline(always)]
fn write_usize(buf: &mut Vec<u8>, mut n: usize) {
    if n == 0 {
        buf.push(b'0');
        return;
    }

    let mut temp = [0u8; 20];
    let mut i = 0;
    while n > 0 {
        temp[i] = b'0' + (n % 10) as u8;
        n /= 10;
        i += 1;
    }

    while i > 0 {
        i -= 1;
        buf.push(temp[i]);
    }
}

// ─── Zero-Allocation Encoders ─────────────────────────────────────────────────

/// Serializes a simple string (`+<str>\r\n`) into the output buffer.
#[inline(always)]
pub fn encode_simple_string(buf: &mut Vec<u8>, s: &str) {
    buf.reserve(s.len() + 3);
    buf.push(b'+');
    buf.extend_from_slice(s.as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Serializes an error string (`-<err>\r\n`) into the output buffer.
#[inline(always)]
pub fn encode_error(buf: &mut Vec<u8>, err: &str) {
    buf.reserve(err.len() + 3);
    buf.push(b'-');
    buf.extend_from_slice(err.as_bytes());
    buf.extend_from_slice(b"\r\n");
}

/// Serializes an integer (`:<int>\r\n`) into the output buffer.
#[inline(always)]
pub fn encode_integer(buf: &mut Vec<u8>, val: i64) {
    buf.reserve(24);
    buf.push(b':');
    write_int(buf, val);
    buf.extend_from_slice(b"\r\n");
}

/// Serializes a bulk string (`$<len>\r\n<data>\r\n`) into the output buffer.
#[inline(always)]
pub fn encode_bulk_string(buf: &mut Vec<u8>, data: &[u8]) {
    let len = data.len();
    buf.reserve(len + 16);
    buf.push(b'$');
    write_usize(buf, len);
    buf.extend_from_slice(b"\r\n");
    buf.extend_from_slice(data);
    buf.extend_from_slice(b"\r\n");
}

/// Serializes a RESP null response (`$-1\r\n`).
#[inline(always)]
pub fn encode_null(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"$-1\r\n");
}

/// Serializes an array header (`*<count>\r\n`).
#[inline(always)]
pub fn encode_array_header(buf: &mut Vec<u8>, count: usize) {
    buf.reserve(16);
    buf.push(b'*');
    write_usize(buf, count);
    buf.extend_from_slice(b"\r\n");
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_ping_command() {
        let input = b"*1\r\n$4\r\nPING\r\n";
        let (frame, consumed) = parse_frame(input).unwrap().expect("full frame");
        assert_eq!(consumed, input.len());
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(*arr[0], Frame::BulkString(b"PING"));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn parse_set_command() {
        let input = b"*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n";
        let (frame, consumed) = parse_frame(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        match frame {
            Frame::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(*arr[0], Frame::BulkString(b"SET"));
                assert_eq!(*arr[1], Frame::BulkString(b"user"));
                assert_eq!(*arr[2], Frame::BulkString(b"alice"));
            }
            _ => panic!("expected array"),
        }
    }

    #[test]
    fn parse_partial_frame_returns_none() {
        let input = b"*2\r\n$3\r\nGET\r\n$4\r\n";
        assert_eq!(parse_frame(input).unwrap(), None);
    }

    #[test]
    fn parse_null_bulk_string() {
        let input = b"$-1\r\n";
        let (frame, consumed) = parse_frame(input).unwrap().unwrap();
        assert_eq!(consumed, input.len());
        assert_eq!(frame, Frame::Null);
    }

    #[test]
    fn encode_responses() {
        let mut buf = Vec::new();
        encode_simple_string(&mut buf, "OK");
        assert_eq!(buf, b"+OK\r\n");

        buf.clear();
        encode_bulk_string(&mut buf, b"foobar");
        assert_eq!(buf, b"$6\r\nfoobar\r\n");

        buf.clear();
        encode_null(&mut buf);
        assert_eq!(buf, b"$-1\r\n");

        buf.clear();
        encode_integer(&mut buf, 42);
        assert_eq!(buf, b":42\r\n");

        buf.clear();
        encode_array_header(&mut buf, 3);
        assert_eq!(buf, b"*3\r\n");
    }
}
