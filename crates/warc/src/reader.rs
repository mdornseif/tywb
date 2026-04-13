//! Synchronous streaming WARC parser.
//!
//! Reads from any `std::io::Read` source — a file, an in-memory buffer, a
//! decompressing wrapper, a network socket, etc.  Records are yielded one by
//! one; the block bytes are buffered in memory but the overall stream is not.
//!
//! # Usage
//!
//! ```no_run
//! use std::fs::File;
//! use warc::reader::WarcReader;
//!
//! let f = File::open("archive.warc").unwrap();
//! let mut reader = WarcReader::new(f);
//! while let Some(record) = reader.next_record().unwrap() {
//!     println!("{:?}", record.header.record_type());
//! }
//! ```

use std::io::{BufRead, BufReader, Read};
use bytes::Bytes;

use crate::error::{WarcError, Result};
use crate::record::{WarcHeader, WarcRecord, WarcVersion};

/// Maximum size of the WARC header block (64 KiB).
/// Prevents unbounded memory use on malformed input.
const MAX_HEADER_BYTES: usize = 64 * 1024;

/// Streaming WARC parser over any `std::io::Read`.
pub struct WarcReader<R: Read> {
    inner: BufReader<R>,
    offset: u64,
}

impl<R: Read> WarcReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            inner: BufReader::new(reader),
            offset: 0,
        }
    }

    pub fn new_with_capacity(reader: R, capacity: usize) -> Self {
        Self {
            inner: BufReader::with_capacity(capacity, reader),
            offset: 0,
        }
    }

    /// Current byte offset in the (uncompressed) stream.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Read and return the next WARC record, or `None` at end of file.
    pub fn next_record(&mut self) -> Result<Option<WarcRecord>> {
        // Skip any blank lines between records (the spec mandates exactly
        // \r\n\r\n after each block, but real-world files sometimes have
        // additional newlines).
        loop {
            let mut peek = String::new();
            let n = self.inner.read_line(&mut peek)?;
            if n == 0 {
                return Ok(None); // EOF
            }
            self.offset += n as u64;
            let trimmed = peek.trim();
            if !trimmed.is_empty() {
                // This should be the WARC version line.
                let version = WarcVersion::parse(trimmed)?;
                return self.read_record_body(version).map(Some);
            }
        }
    }

    fn read_record_body(&mut self, version: WarcVersion) -> Result<WarcRecord> {
        let record_offset = self.offset - /* version line already consumed */ 0;

        // ── Parse header fields ───────────────────────────────────────────────
        let mut fields: Vec<(String, String)> = Vec::new();
        let mut header_bytes: usize = 0;

        loop {
            let mut line = String::new();
            let n = self.inner.read_line(&mut line)?;
            if n == 0 {
                return Err(WarcError::TruncatedHeader);
            }
            self.offset += n as u64;
            header_bytes += n;

            if header_bytes > MAX_HEADER_BYTES {
                return Err(WarcError::HeaderTooLarge {
                    size: header_bytes,
                    limit: MAX_HEADER_BYTES,
                });
            }

            let trimmed = line.trim_end_matches(['\r', '\n']);
            if trimmed.is_empty() {
                // Blank line → end of WARC header block.
                break;
            }

            // Field lines look like:  Name: value
            // Continuation lines (RFC 2822 folding) start with whitespace —
            // we append them to the previous field.
            if trimmed.starts_with([' ', '\t']) {
                if let Some(last) = fields.last_mut() {
                    last.1.push(' ');
                    last.1.push_str(trimmed.trim());
                }
                // Ignore leading continuation with no previous field.
                continue;
            }

            let (name, value) = trimmed.split_once(':').ok_or_else(|| {
                WarcError::MalformedHeader {
                    line: fields.len() + 1,
                    raw: trimmed.to_owned(),
                }
            })?;
            fields.push((
                name.trim().to_ascii_lowercase(),
                value.trim().to_owned(),
            ));
        }

        let header = WarcHeader::from_fields(version, fields);

        // ── Read block ────────────────────────────────────────────────────────
        let content_length = header.content_length()?;
        let mut block = vec![0u8; content_length];
        self.inner.read_exact(&mut block).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                WarcError::UnexpectedEof {
                    bytes_read: content_length,
                }
            } else {
                WarcError::Io(e)
            }
        })?;
        self.offset += content_length as u64;

        // ── Consume trailing \r\n\r\n ─────────────────────────────────────────
        // The spec requires exactly two CRLFs after the block.
        // We read up to 4 bytes and verify.
        let mut terminator = [0u8; 4];
        self.inner
            .read_exact(&mut terminator)
            .map_err(|_| WarcError::MissingTerminator)?;
        self.offset += 4;
        if &terminator != b"\r\n\r\n" {
            return Err(WarcError::MissingTerminator);
        }

        Ok(WarcRecord {
            header,
            block: Bytes::from(block),
            offset: record_offset,
        })
    }
}

// ── Iterator adapter ──────────────────────────────────────────────────────────

/// Wraps `WarcReader` as a fallible iterator.
/// Iteration stops on the first error.
pub struct WarcIter<R: Read> {
    reader: WarcReader<R>,
    done: bool,
}

impl<R: Read> WarcIter<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: WarcReader::new(reader),
            done: false,
        }
    }
}

impl<R: Read> Iterator for WarcIter<R> {
    type Item = Result<WarcRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.reader.next_record() {
            Ok(Some(r)) => Some(Ok(r)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

// ── Test helpers ──────────────────────────────────────────────────────────────

/// Build a minimal valid WARC record as bytes for use in tests.
pub fn build_warc_record(
    version: &str,
    fields: &[(&str, &str)],
    block: &[u8],
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(version.as_bytes());
    out.extend_from_slice(b"\r\n");
    for (k, v) in fields {
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(b": ");
        out.extend_from_slice(v.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    // Content-Length is always added by the builder
    out.extend_from_slice(format!("Content-Length: {}\r\n", block.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(block);
    out.extend_from_slice(b"\r\n\r\n");
    out
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::RecordType;
    use std::io::Cursor;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn minimal_response(url: &str, body: &[u8]) -> Vec<u8> {
        let http = {
            let mut h = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n".to_vec();
            h.extend_from_slice(body);
            h
        };
        build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000001>"),
                ("WARC-Target-URI", url),
            ],
            &http,
        )
    }

    fn parse_one(data: &[u8]) -> WarcRecord {
        let mut r = WarcReader::new(Cursor::new(data));
        r.next_record().unwrap().expect("expected a record")
    }

    // ── Basic parsing ─────────────────────────────────────────────────────────

    #[test]
    fn parse_minimal_response_record() {
        let data = minimal_response("https://example.com/", b"<html>hi</html>");
        let rec = parse_one(&data);
        assert_eq!(rec.record_type().unwrap(), RecordType::Response);
        assert_eq!(rec.target_uri().unwrap(), "https://example.com/");
    }

    #[test]
    fn parse_warcinfo_record() {
        let block = b"software: warc-search/0.1\r\nformat: WARC File Format 1.0\r\n";
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "warcinfo"),
                ("WARC-Date", "2024-01-01T00:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000002>"),
                ("WARC-Filename", "archive.warc"),
            ],
            block,
        );
        let rec = parse_one(&data);
        assert_eq!(rec.record_type().unwrap(), RecordType::Warcinfo);
    }

    #[test]
    fn parse_request_record() {
        let block = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "request"),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000003>"),
                ("WARC-Target-URI", "https://example.com/"),
            ],
            block,
        );
        let rec = parse_one(&data);
        assert_eq!(rec.record_type().unwrap(), RecordType::Request);
    }

    #[test]
    fn parse_record_v1_1() {
        let data = minimal_response("https://example.com/v11", b"body");
        // Patch the version line
        let data = String::from_utf8_lossy(&data)
            .replace("WARC/1.0", "WARC/1.1")
            .into_bytes();
        let rec = parse_one(&data);
        assert_eq!(rec.header.version, WarcVersion::V1_1);
    }

    #[test]
    fn parse_empty_block() {
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "metadata"),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000004>"),
            ],
            b"",
        );
        let rec = parse_one(&data);
        assert_eq!(rec.block.len(), 0);
    }

    #[test]
    fn parse_large_block() {
        let body = vec![b'x'; 1_000_000];
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "resource"),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000005>"),
            ],
            &body,
        );
        let rec = parse_one(&data);
        assert_eq!(rec.block.len(), 1_000_000);
    }

    // ── Multi-record stream ───────────────────────────────────────────────────

    #[test]
    fn parse_two_records_sequentially() {
        let mut data = minimal_response("https://a.com/", b"page a");
        data.extend(minimal_response("https://b.com/", b"page b"));

        let mut reader = WarcReader::new(Cursor::new(data));
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        let eof = reader.next_record().unwrap();

        assert_eq!(r1.target_uri().unwrap(), "https://a.com/");
        assert_eq!(r2.target_uri().unwrap(), "https://b.com/");
        assert!(eof.is_none());
    }

    #[test]
    fn parse_ten_records() {
        let mut data = Vec::new();
        for i in 0..10 {
            data.extend(minimal_response(
                &format!("https://example.com/{i}"),
                format!("body {i}").as_bytes(),
            ));
        }
        let iter = WarcIter::new(Cursor::new(data));
        let records: Vec<_> = iter.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(records.len(), 10);
        for (i, rec) in records.iter().enumerate() {
            assert_eq!(
                rec.target_uri().unwrap(),
                format!("https://example.com/{i}")
            );
        }
    }

    #[test]
    fn offset_advances_between_records() {
        let mut data = minimal_response("https://a.com/", b"aaa");
        data.extend(minimal_response("https://b.com/", b"bbb"));

        let mut reader = WarcReader::new(Cursor::new(&data));
        reader.next_record().unwrap().unwrap();
        let mid_offset = reader.offset();
        reader.next_record().unwrap().unwrap();
        let end_offset = reader.offset();

        assert!(mid_offset > 0);
        assert!(end_offset > mid_offset);
        assert_eq!(end_offset, data.len() as u64);
    }

    // ── Header parsing edge cases ─────────────────────────────────────────────

    #[test]
    fn parse_extra_blank_lines_between_records() {
        let mut data = minimal_response("https://a.com/", b"a");
        // Add extra blank lines between records (common in real archives)
        data.extend_from_slice(b"\r\n\r\n\r\n");
        data.extend(minimal_response("https://b.com/", b"b"));

        let mut reader = WarcReader::new(Cursor::new(data));
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        assert_eq!(r1.target_uri().unwrap(), "https://a.com/");
        assert_eq!(r2.target_uri().unwrap(), "https://b.com/");
    }

    #[test]
    fn parse_header_field_with_colon_in_value() {
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000006>"),
                ("WARC-Target-URI", "https://example.com/path?a=1&b=2"),
                // Value contains colons — must not split on them
                ("Content-Type", "application/json; charset=utf-8"),
            ],
            b"{}",
        );
        let rec = parse_one(&data);
        assert_eq!(rec.header.content_type().unwrap(), "application/json; charset=utf-8");
    }

    #[test]
    fn parse_header_whitespace_trimmed_from_value() {
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "  response  "),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000007>"),
            ],
            b"x",
        );
        let rec = parse_one(&data);
        // Value is trimmed; type must still parse
        assert_eq!(rec.record_type().unwrap(), RecordType::Response);
    }

    #[test]
    fn parse_digest_field() {
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000008>"),
                ("WARC-Block-Digest", "sha1:ABCDEFGHIJKLMNOPQRST123456"),
            ],
            b"body",
        );
        let rec = parse_one(&data);
        assert_eq!(rec.header.block_digest().unwrap(), "sha1:ABCDEFGHIJKLMNOPQRST123456");
    }

    #[test]
    fn parse_ip_address_field() {
        let data = build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-01-15T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:aaaaaaaa-0000-0000-0000-000000000009>"),
                ("WARC-IP-Address", "93.184.216.34"),
            ],
            b"body",
        );
        let rec = parse_one(&data);
        assert_eq!(rec.header.ip_address().unwrap(), "93.184.216.34");
    }

    // ── Error cases ───────────────────────────────────────────────────────────

    #[test]
    fn error_on_bad_version() {
        let data = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n\r\n\r\n";
        let mut reader = WarcReader::new(Cursor::new(data));
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn error_on_missing_content_length() {
        // Build a record without Content-Length manually
        let data = b"WARC/1.0\r\nWARC-Type: response\r\nWARC-Date: 2024-01-01T00:00:00Z\r\nWARC-Record-ID: <urn:uuid:test>\r\n\r\nbody\r\n\r\n";
        let mut reader = WarcReader::new(Cursor::new(data));
        // Missing Content-Length — should fail when we try to read the block
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn error_on_truncated_stream() {
        // Build a valid header but truncate the block
        let mut data = "WARC/1.0\r\nWARC-Type: response\r\nWARC-Date: 2024-01-01T00:00:00Z\r\n".to_string();
        data.push_str("WARC-Record-ID: <urn:uuid:test>\r\nContent-Length: 10000\r\n\r\n");
        data.push_str("only 20 bytes here  "); // << far less than 10000
        let mut reader = WarcReader::new(Cursor::new(data.as_bytes()));
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn error_on_empty_input() {
        let mut reader = WarcReader::new(Cursor::new(b"" as &[u8]));
        // Empty input → Ok(None)
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn error_on_malformed_header_field() {
        // A header line with no colon
        let data = b"WARC/1.0\r\nWARC-Type: response\r\nBAD_LINE_NO_COLON\r\nContent-Length: 0\r\n\r\n\r\n\r\n";
        let mut reader = WarcReader::new(Cursor::new(data));
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn iterator_stops_at_first_error() {
        // First record valid, then garbage
        let mut data = minimal_response("https://ok.com/", b"ok");
        data.extend_from_slice(b"GARBAGE DATA NOT A WARC RECORD\r\n");

        let iter = WarcIter::new(Cursor::new(data));
        let results: Vec<_> = iter.collect();
        // First record ok, second is an error
        assert!(results[0].is_ok());
        assert!(results[1].is_err());
        assert_eq!(results.len(), 2);
    }

    // ── WARC iter ─────────────────────────────────────────────────────────────

    #[test]
    fn warc_iter_empty_stream() {
        let iter = WarcIter::new(Cursor::new(b"" as &[u8]));
        let records: Vec<_> = iter.collect();
        assert!(records.is_empty());
    }

    #[test]
    fn warc_iter_all_ok() {
        let mut data = Vec::new();
        data.extend(minimal_response("https://x.com/1", b"1"));
        data.extend(minimal_response("https://x.com/2", b"2"));
        data.extend(minimal_response("https://x.com/3", b"3"));

        let iter = WarcIter::new(Cursor::new(data));
        let records: Vec<_> = iter.collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(records.len(), 3);
    }
}
