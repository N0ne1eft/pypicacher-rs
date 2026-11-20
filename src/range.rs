/// Inclusive byte range `[start, end]` resolved against a known `total_len`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    pub fn len(&self) -> u64 {
        self.end - self.start + 1
    }

    pub fn is_empty(&self) -> bool {
        self.end < self.start
    }

    pub fn content_range_header(&self, total_len: u64) -> String {
        format!("bytes {}-{}/{}", self.start, self.end, total_len)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeParseResult {
    /// A single valid satisfiable byte range.
    Satisfiable(ByteRange),
    /// The range header is syntactically valid `bytes=...` but outside `[0, total_len - 1]`.
    Unsatisfiable,
    /// Unsupported/multi-range/malformed header: RFC 9110 allows ignoring and serving 200 OK.
    Ignore,
}

/// Parses an HTTP `Range` header value against `total_len` per RFC 9110 Section 14.1.2.
///
/// Supports single byte ranges:
/// - `bytes=start-end`
/// - `bytes=start-`
/// - `bytes=-suffix`
///
/// Multi-ranges (e.g. `bytes=0-10, 20-30`) return `RangeParseResult::Ignore` so the
/// server can fall back to sending a full `200 OK` response.
pub fn parse_range_header(header_val: &str, total_len: u64) -> RangeParseResult {
    let trimmed = header_val.trim();
    let Some(spec) = trimmed
        .strip_prefix("bytes=")
        .or_else(|| trimmed.strip_prefix("BYTES="))
    else {
        return RangeParseResult::Ignore;
    };

    // Multi-range requests: ignore and serve full representation.
    if spec.contains(',') {
        return RangeParseResult::Ignore;
    }

    let Some((start_str, end_str)) = spec.trim().split_once('-') else {
        return RangeParseResult::Ignore;
    };
    let start_str = start_str.trim();
    let end_str = end_str.trim();

    if total_len == 0 {
        return RangeParseResult::Unsatisfiable;
    }

    if start_str.is_empty() {
        // Suffix range: bytes=-N
        if end_str.is_empty() {
            return RangeParseResult::Ignore;
        }
        let Ok(suffix_len) = end_str.parse::<u64>() else {
            return RangeParseResult::Ignore;
        };
        if suffix_len == 0 {
            return RangeParseResult::Unsatisfiable;
        }
        let start = total_len.saturating_sub(suffix_len);
        let end = total_len - 1;
        return RangeParseResult::Satisfiable(ByteRange { start, end });
    }

    let Ok(start) = start_str.parse::<u64>() else {
        return RangeParseResult::Ignore;
    };
    if start >= total_len {
        return RangeParseResult::Unsatisfiable;
    }

    if end_str.is_empty() {
        // Open-ended range: bytes=start-
        return RangeParseResult::Satisfiable(ByteRange {
            start,
            end: total_len - 1,
        });
    }

    let Ok(end_raw) = end_str.parse::<u64>() else {
        return RangeParseResult::Ignore;
    };
    if end_raw < start {
        return RangeParseResult::Ignore;
    }
    let end = end_raw.min(total_len - 1);
    RangeParseResult::Satisfiable(ByteRange { start, end })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_standard_ranges() {
        assert_eq!(
            parse_range_header("bytes=0-99", 1000),
            RangeParseResult::Satisfiable(ByteRange { start: 0, end: 99 })
        );
        assert_eq!(
            parse_range_header("bytes=500-", 1000),
            RangeParseResult::Satisfiable(ByteRange {
                start: 500,
                end: 999
            })
        );
        // End beyond EOF gets clamped to total_len - 1 per RFC 9110
        assert_eq!(
            parse_range_header("bytes=900-5000", 1000),
            RangeParseResult::Satisfiable(ByteRange {
                start: 900,
                end: 999
            })
        );
    }

    #[test]
    fn test_suffix_ranges_for_uv() {
        assert_eq!(
            parse_range_header("bytes=-16384", 100_000),
            RangeParseResult::Satisfiable(ByteRange {
                start: 83_616,
                end: 99_999
            })
        );
        // Suffix larger than file size returns whole file
        assert_eq!(
            parse_range_header("bytes=-16384", 500),
            RangeParseResult::Satisfiable(ByteRange { start: 0, end: 499 })
        );
        assert_eq!(
            parse_range_header("bytes=-0", 500),
            RangeParseResult::Unsatisfiable
        );
    }

    #[test]
    fn test_unsatisfiable_and_ignored() {
        assert_eq!(
            parse_range_header("bytes=1000-1005", 1000),
            RangeParseResult::Unsatisfiable
        );
        assert_eq!(
            parse_range_header("bytes=0-10", 0),
            RangeParseResult::Unsatisfiable
        );
        assert_eq!(
            parse_range_header("bytes=0-10, 20-30", 1000),
            RangeParseResult::Ignore
        );
        assert_eq!(
            parse_range_header("items=0-10", 1000),
            RangeParseResult::Ignore
        );
        assert_eq!(
            parse_range_header("bytes=50-10", 1000),
            RangeParseResult::Ignore
        );
    }
}
