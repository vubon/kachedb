//! `kachedb-hash` — Zero-allocation Redis-compatible glob pattern matcher (`stringmatchlen`).
//!
//! Supports `*` (any byte sequence), `?` (any single byte), `[abc]` / `[a-z]`
//! (character sets and ranges), `[^abc]` / `[!abc]` (negated character classes),
//! and `\x` (escaped literals). Pre-extracts literal prefixes for L1 cache-line
//! fast-path rejection against `HashEntry::inline_key`.

use crate::entry::{HashEntry, INLINE_KEY_CAPACITY};
use crate::table::SwissTable;

/// Maximum permitted `MATCH` pattern length in bytes.
pub const MAX_PATTERN_LEN: usize = 256;

/// Pre-analyzed glob pattern with literal prefix extraction for zero-allocation L1 cache filtering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompiledPattern<'a> {
    raw: &'a [u8],
    literal_prefix: &'a [u8],
    is_match_all: bool,
    is_pure_prefix: bool,
}

impl<'a> CompiledPattern<'a> {
    /// Compiles a raw pattern slice in $O(P)$ time with zero heap allocations.
    #[inline]
    pub fn new(raw: &'a [u8]) -> Self {
        let is_match_all = !raw.is_empty() && raw.iter().all(|&b| b == b'*');
        let mut prefix_end = 0;
        while prefix_end < raw.len() {
            match raw[prefix_end] {
                b'*' | b'?' | b'[' | b'\\' => break,
                _ => prefix_end += 1,
            }
        }
        let literal_prefix = &raw[..prefix_end];
        let is_pure_prefix = prefix_end < raw.len() && raw[prefix_end..].iter().all(|&b| b == b'*');

        Self {
            raw,
            literal_prefix,
            is_match_all,
            is_pure_prefix,
        }
    }

    /// Returns the raw pattern bytes.
    #[inline(always)]
    pub fn as_bytes(&self) -> &'a [u8] {
        self.raw
    }

    /// Returns the extracted leading literal prefix (before any `*`, `?`, `[`, `\`).
    #[inline(always)]
    pub fn literal_prefix(&self) -> &'a [u8] {
        self.literal_prefix
    }

    /// Returns `true` if the pattern is `*` (or `**...`) and matches any key.
    #[inline(always)]
    pub fn is_match_all(&self) -> bool {
        self.is_match_all
    }

    /// Evaluates the pattern against an arbitrary byte slice `text`.
    #[inline]
    pub fn matches(&self, text: &[u8]) -> bool {
        if self.is_match_all {
            return true;
        }
        if !text.starts_with(self.literal_prefix) {
            return false;
        }
        if self.is_pure_prefix {
            return true;
        }
        glob_match(self.raw, text)
    }

    /// Evaluates the pattern against a `HashEntry` inside `table`, rejecting non-matching
    /// keys directly from the 40-byte inline SSO buffer in L1 cache before touching `overflow_keys`.
    #[inline]
    pub fn matches_entry(&self, table: &SwissTable, entry: &HashEntry) -> bool {
        if self.is_match_all {
            return true;
        }

        let key_len = entry.key_len as usize;
        if key_len < self.literal_prefix.len() {
            return false;
        }

        // L1 Cache-Line Fast Path: compare up to 40 bytes of literal_prefix against inline_key
        let head_len = self.literal_prefix.len().min(INLINE_KEY_CAPACITY);
        if head_len > 0 && entry.inline_key[..head_len] != self.literal_prefix[..head_len] {
            return false;
        }

        if self.is_pure_prefix && self.literal_prefix.len() <= INLINE_KEY_CAPACITY {
            return true;
        }

        let full_key = table.key_bytes(entry);
        if self.is_pure_prefix {
            full_key.starts_with(self.literal_prefix)
        } else {
            glob_match(self.raw, full_key)
        }
    }
}

/// Zero-allocation byte-slice glob matcher compatible with Redis `stringmatchlen`.
///
/// Runs in $O(P \cdot T)$ worst-case time with single-star backtracking (immune to
/// exponential backtracking / CVE-2022-36021).
#[inline]
pub fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    let mut p = 0;
    let mut t = 0;
    let mut star_p: Option<usize> = None;
    let mut star_t: usize = 0;

    while t < text.len() {
        if p < pattern.len() {
            match pattern[p] {
                b'*' => {
                    // Collapse consecutive '*' wildcards
                    while p < pattern.len() && pattern[p] == b'*' {
                        p += 1;
                    }
                    if p == pattern.len() {
                        return true;
                    }
                    star_p = Some(p);
                    star_t = t;
                    continue;
                }
                b'?' => {
                    p += 1;
                    t += 1;
                    continue;
                }
                b'[' => {
                    if let Some((matched, next_p)) = match_char_class(&pattern[p + 1..], text[t]) {
                        if matched {
                            p += 1 + next_p;
                            t += 1;
                            continue;
                        }
                    } else if text[t] == b'[' {
                        // Unclosed '[' is treated as a literal '['
                        p += 1;
                        t += 1;
                        continue;
                    }
                }
                b'\\' => {
                    let (escaped, consumed) = if p + 1 < pattern.len() {
                        (pattern[p + 1], 2)
                    } else {
                        (b'\\', 1)
                    };
                    if escaped == text[t] {
                        p += consumed;
                        t += 1;
                        continue;
                    }
                }
                lit if lit == text[t] => {
                    p += 1;
                    t += 1;
                    continue;
                }
                _ => {}
            }
        }

        // Mismatch: backtrack to the most recent '*'
        if let Some(sp) = star_p {
            p = sp;
            star_t += 1;
            t = star_t;
        } else {
            return false;
        }
    }

    while p < pattern.len() && pattern[p] == b'*' {
        p += 1;
    }

    p == pattern.len()
}

/// Parses `pattern` starting immediately after `[`.
/// Returns `Some((matched, bytes_consumed_after_open_bracket))` if a closing `]`
/// exists, or `None` if the bracket is unclosed.
#[inline]
fn match_char_class(pattern: &[u8], ch: u8) -> Option<(bool, usize)> {
    let mut i = 0;
    let mut invert = false;
    if i < pattern.len() && (pattern[i] == b'^' || pattern[i] == b'!') {
        invert = true;
        i += 1;
    }
    let mut matched = false;
    while i < pattern.len() && pattern[i] != b']' {
        let start = if pattern[i] == b'\\' && i + 1 < pattern.len() {
            i += 1;
            pattern[i]
        } else {
            pattern[i]
        };
        if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
            i += 2;
            let end = if pattern[i] == b'\\' && i + 1 < pattern.len() {
                i += 1;
                pattern[i]
            } else {
                pattern[i]
            };
            let (lo, hi) = if start <= end {
                (start, end)
            } else {
                (end, start)
            };
            if ch >= lo && ch <= hi {
                matched = true;
            }
        } else if ch == start {
            matched = true;
        }
        i += 1;
    }
    if i < pattern.len() && pattern[i] == b']' {
        Some((matched ^ invert, i + 1))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_basic_wildcards() {
        assert!(glob_match(b"*", b""));
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"***", b"anything"));
        assert!(glob_match(b"user:*", b"user:1001"));
        assert!(glob_match(b"user:*", b"user:"));
        assert!(!glob_match(b"user:*", b"usr:1001"));
        assert!(glob_match(b"*session*", b"my_session_key"));
        assert!(glob_match(b"a?c", b"abc"));
        assert!(!glob_match(b"a?c", b"ac"));
        assert!(!glob_match(b"a?c", b"abbc"));
    }

    #[test]
    fn test_glob_char_classes_and_ranges() {
        // Basic set
        assert!(glob_match(b"user:[123]", b"user:1"));
        assert!(glob_match(b"user:[123]", b"user:3"));
        assert!(!glob_match(b"user:[123]", b"user:4"));
        // Must NOT match literal '[' when bracket is closed!
        assert!(!glob_match(b"[a]", b"["));
        assert!(glob_match(b"[a]", b"a"));

        // Ranges
        assert!(glob_match(b"key:[a-z][0-9]", b"key:m7"));
        assert!(!glob_match(b"key:[a-z][0-9]", b"key:M7"));
        assert!(glob_match(b"key:[z-a]", b"key:m")); // reversed range supported like Redis

        // Negation with ^ and !
        assert!(glob_match(b"key:[^0-9]", b"key:a"));
        assert!(!glob_match(b"key:[^0-9]", b"key:5"));
        assert!(glob_match(b"key:[!a-z]", b"key:9"));
        assert!(!glob_match(b"key:[!a-z]", b"key:b"));

        // Unclosed '[' treated as literal '['
        assert!(glob_match(b"arr[0", b"arr[0"));
        assert!(!glob_match(b"arr[0", b"arr0"));
    }

    #[test]
    fn test_glob_escapes() {
        assert!(glob_match(b"foo\\*bar", b"foo*bar"));
        assert!(!glob_match(b"foo\\*bar", b"fooxxxbar"));
        assert!(glob_match(b"foo\\?bar", b"foo?bar"));
        assert!(!glob_match(b"foo\\?bar", b"fooxbar"));
        assert!(glob_match(b"\\[a\\]", b"[a]"));
        assert!(!glob_match(b"\\[a\\]", b"a"));
        assert!(glob_match(b"a\\\\b", b"a\\b"));
        assert!(glob_match(b"trailing\\", b"trailing\\"));
    }

    #[test]
    fn test_compiled_pattern_fast_paths() {
        let p_all = CompiledPattern::new(b"*");
        assert!(p_all.is_match_all());
        assert!(p_all.matches(b"any:key"));

        let p_prefix = CompiledPattern::new(b"doc:parent:*");
        assert!(!p_prefix.is_match_all());
        assert_eq!(p_prefix.literal_prefix(), b"doc:parent:");
        assert!(p_prefix.is_pure_prefix);
        assert!(p_prefix.matches(b"doc:parent:12345"));
        assert!(!p_prefix.matches(b"doc:chunk:12345"));

        let p_complex = CompiledPattern::new(b"user:[0-9]*:profile");
        assert_eq!(p_complex.literal_prefix(), b"user:");
        assert!(!p_complex.is_pure_prefix);
        assert!(p_complex.matches(b"user:42:profile"));
        assert!(!p_complex.matches(b"user:abc:profile"));
    }
}
