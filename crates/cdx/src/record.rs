//! CDX record — one entry per WARC response record in the index.

use chrono::{DateTime, NaiveDateTime, Utc};
use crate::error::{CdxError, Result};
use warc::WarcRecord;

/// A single CDX index entry.
///
/// Each entry describes the location of one WARC record inside an S3 object,
/// plus enough metadata to serve a Wayback-compatible CDX API response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CdxRecord {
    /// SURT-canonicalized URL, e.g. `com,example)/path?q=1`
    pub surt_url: String,
    /// Capture timestamp in `YYYYMMDDHHmmss` format (14 digits).
    pub timestamp: String,
    /// Original URL as found in `WARC-Target-URI`.
    pub original_url: String,
    /// MIME type of the response body (from `Content-Type` or identified type).
    pub mime: Option<String>,
    /// HTTP status code (e.g. 200, 301, 404).
    pub status: Option<u16>,
    /// `WARC-Block-Digest`, e.g. `sha1:ABCDEF…`
    pub digest: Option<String>,
    /// S3 object key of the WARC file containing this record.
    pub s3_key: String,
    /// Byte offset of the record within the (uncompressed) WARC stream.
    pub offset: u64,
    /// Byte length of the record block (Content-Length value).
    pub length: u64,
}

impl CdxRecord {
    /// Parse the 14-digit timestamp into a `DateTime<Utc>`.
    pub fn datetime(&self) -> Result<DateTime<Utc>> {
        parse_timestamp(&self.timestamp)
    }
}

/// Parse a 14-digit `YYYYMMDDHHmmss` timestamp into `DateTime<Utc>`.
pub fn parse_timestamp(ts: &str) -> Result<DateTime<Utc>> {
    if ts.len() != 14 || !ts.chars().all(|c| c.is_ascii_digit()) {
        return Err(CdxError::InvalidTimestamp(ts.to_owned()));
    }
    let dt = NaiveDateTime::parse_from_str(ts, "%Y%m%d%H%M%S")
        .map_err(|_| CdxError::InvalidTimestamp(ts.to_owned()))?;
    Ok(DateTime::from_naive_utc_and_offset(dt, Utc))
}

/// Format a `DateTime<Utc>` as a 14-digit CDX timestamp.
pub fn format_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y%m%d%H%M%S").to_string()
}

/// Build a `CdxRecord` from a `WarcRecord` plus S3 location metadata.
///
/// Returns `None` if the WARC record is not a `response` or `resource` type
/// (i.e. types we don't index in the CDX).
pub fn from_warc_record(
    warc: &WarcRecord,
    s3_key: &str,
) -> Result<Option<CdxRecord>> {
    use warc::RecordType;

    let record_type = warc.header.record_type()?;
    match record_type {
        RecordType::Response | RecordType::Resource => {}
        _ => return Ok(None),
    }

    let original_url = warc
        .header
        .target_uri()
        .ok_or(CdxError::MissingField("WARC-Target-URI"))?
        .to_owned();

    let surt_url = crate::surt::to_surt(&original_url)?;

    let dt = warc.header.date()?;
    let timestamp = format_timestamp(&dt);

    let mime = warc.header.content_type().map(|s| {
        // Strip parameters: "text/html; charset=utf-8" → "text/html"
        s.split(';').next().unwrap_or(s).trim().to_owned()
    });

    // Try to extract HTTP status from the response block
    let status = extract_http_status(&warc.block);

    let digest = warc.header.block_digest().map(|s| s.to_owned());
    let length = warc.header.content_length()? as u64;

    Ok(Some(CdxRecord {
        surt_url,
        timestamp,
        original_url,
        mime,
        status,
        digest,
        s3_key: s3_key.to_owned(),
        offset: warc.offset,
        length,
    }))
}

/// Extract HTTP status code from the first line of a response block.
/// Returns `None` if the block doesn't look like an HTTP response.
fn extract_http_status(block: &[u8]) -> Option<u16> {
    // First line ends at \r\n or \n
    let end = block.iter().position(|&b| b == b'\n').unwrap_or(block.len());
    let line = std::str::from_utf8(&block[..end]).ok()?;
    // "HTTP/1.1 200 OK" — status is the second token
    let mut parts = line.split_whitespace();
    parts.next()?; // HTTP/1.x
    parts.next()?.parse::<u16>().ok()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use warc::{WarcRecord, reader::{WarcReader, build_warc_record}};
    use std::io::Cursor;

    /// Build a WarcRecord from field pairs and a raw block, using the public
    /// `build_warc_record` helper.  `offset` is patched in after parsing.
    fn make_warc_record(fields: &[(&str, &str)], block: &[u8], offset: u64) -> WarcRecord {
        // build_warc_record adds Content-Length automatically; strip duplicates.
        let filtered: Vec<(&str, &str)> = fields
            .iter()
            .filter(|(k, _)| !k.eq_ignore_ascii_case("content-length"))
            .copied()
            .collect();
        let raw = build_warc_record("WARC/1.0", &filtered, block);
        let mut reader = WarcReader::new(Cursor::new(raw));
        let mut rec = reader.next_record().unwrap().unwrap();
        rec.offset = offset;
        rec
    }

    fn response_record(url: &str, status: u16, body: &[u8]) -> WarcRecord {
        let http = format!("HTTP/1.1 {status} OK\r\nContent-Type: text/html\r\n\r\n");
        let mut block = http.into_bytes();
        block.extend_from_slice(body);
        make_warc_record(
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0001>"),
                ("WARC-Target-URI", url),
                ("WARC-Block-Digest", "sha1:TESTHASH"),
                // Content-Type at the WARC level tells CDX what the payload MIME is.
                ("Content-Type", "text/html"),
                ("Content-Length", &block.len().to_string()),
            ],
            &block,
            0,
        )
    }

    // ── parse_timestamp ───────────────────────────────────────────────────────

    #[test]
    fn parse_timestamp_valid() {
        let dt = parse_timestamp("20240315120000").unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 3);
        assert_eq!(dt.day(), 15);
        assert_eq!(dt.hour(), 12);
    }

    #[test]
    fn parse_timestamp_too_short() {
        assert!(parse_timestamp("2024031512").is_err());
    }

    #[test]
    fn parse_timestamp_too_long() {
        assert!(parse_timestamp("202403151200000").is_err());
    }

    #[test]
    fn parse_timestamp_non_digits() {
        assert!(parse_timestamp("2024-03-15T12:00").is_err());
    }

    #[test]
    fn parse_timestamp_empty() {
        assert!(parse_timestamp("").is_err());
    }

    #[test]
    fn parse_timestamp_invalid_date() {
        // Month 13 is invalid
        assert!(parse_timestamp("20241399000000").is_err());
    }

    // ── format_timestamp ──────────────────────────────────────────────────────

    #[test]
    fn format_timestamp_roundtrip() {
        let ts = "20240315120000";
        let dt = parse_timestamp(ts).unwrap();
        assert_eq!(format_timestamp(&dt), ts);
    }

    #[test]
    fn format_timestamp_zero_padded() {
        let ts = "20240101090501";
        let dt = parse_timestamp(ts).unwrap();
        assert_eq!(format_timestamp(&dt), ts);
    }

    // ── extract_http_status ───────────────────────────────────────────────────

    #[test]
    fn http_status_200() {
        let block = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\nbody";
        assert_eq!(extract_http_status(block), Some(200));
    }

    #[test]
    fn http_status_301() {
        let block = b"HTTP/1.1 301 Moved Permanently\r\nLocation: /new\r\n\r\n";
        assert_eq!(extract_http_status(block), Some(301));
    }

    #[test]
    fn http_status_404() {
        let block = b"HTTP/1.0 404 Not Found\r\n\r\n";
        assert_eq!(extract_http_status(block), Some(404));
    }

    #[test]
    fn http_status_non_http_block() {
        let block = b"not an http response";
        assert_eq!(extract_http_status(block), None);
    }

    #[test]
    fn http_status_empty_block() {
        assert_eq!(extract_http_status(b""), None);
    }

    // ── from_warc_record ──────────────────────────────────────────────────────

    #[test]
    fn from_warc_response_record() {
        let rec = response_record("https://example.com/page", 200, b"hello");
        let cdx = from_warc_record(&rec, "crawls/2024/archive.warc.gz")
            .unwrap()
            .unwrap();
        assert_eq!(cdx.original_url, "https://example.com/page");
        assert_eq!(cdx.surt_url, "com,example)/page");
        assert_eq!(cdx.timestamp, "20240315120000");
        assert_eq!(cdx.status, Some(200));
        assert_eq!(cdx.mime.as_deref(), Some("text/html"));
        assert_eq!(cdx.digest.as_deref(), Some("sha1:TESTHASH"));
        assert_eq!(cdx.s3_key, "crawls/2024/archive.warc.gz");
    }

    #[test]
    fn from_warc_request_record_returns_none() {
        let rec = make_warc_record(
            &[
                ("WARC-Type", "request"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0002>"),
                ("WARC-Target-URI", "https://example.com/"),
                ("Content-Length", "0"),
            ],
            b"",
            0,
        );
        let result = from_warc_record(&rec, "test.warc").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn from_warc_warcinfo_record_returns_none() {
        let rec = make_warc_record(
            &[
                ("WARC-Type", "warcinfo"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0003>"),
                ("Content-Length", "0"),
            ],
            b"",
            0,
        );
        let result = from_warc_record(&rec, "test.warc").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn from_warc_resource_record_is_indexed() {
        let rec = make_warc_record(
            &[
                ("WARC-Type", "resource"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0004>"),
                ("WARC-Target-URI", "https://example.com/file.pdf"),
                ("Content-Type", "application/pdf"),
                ("Content-Length", "4"),
            ],
            b"%PDF",
            100,
        );
        let cdx = from_warc_record(&rec, "test.warc").unwrap().unwrap();
        assert_eq!(cdx.mime.as_deref(), Some("application/pdf"));
        assert_eq!(cdx.offset, 100);
    }

    #[test]
    fn from_warc_mime_strips_parameters() {
        let block = b"HTTP/1.1 200 OK\r\n\r\nbody";
        let rec = make_warc_record(
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0005>"),
                ("WARC-Target-URI", "https://example.com/"),
                ("Content-Type", "text/html; charset=utf-8"),
                ("Content-Length", &block.len().to_string()),
            ],
            block,
            0,
        );
        let cdx = from_warc_record(&rec, "test.warc").unwrap().unwrap();
        assert_eq!(cdx.mime.as_deref(), Some("text/html"));
    }

    #[test]
    fn from_warc_301_status() {
        let block = b"HTTP/1.1 301 Moved Permanently\r\nLocation: /new\r\n\r\n";
        let rec = make_warc_record(
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0006>"),
                ("WARC-Target-URI", "https://example.com/old"),
                ("Content-Length", &block.len().to_string()),
            ],
            block,
            0,
        );
        let cdx = from_warc_record(&rec, "test.warc").unwrap().unwrap();
        assert_eq!(cdx.status, Some(301));
    }

    #[test]
    fn from_warc_missing_target_uri_returns_error() {
        let rec = make_warc_record(
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0007>"),
                // No WARC-Target-URI
                ("Content-Length", "0"),
            ],
            b"",
            0,
        );
        assert!(from_warc_record(&rec, "test.warc").is_err());
    }

    use chrono::Timelike;
    use chrono::Datelike;
}
