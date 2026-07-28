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
    /// Compressed byte offset of the gzip member in the `.warc.gz` S3 object.
    ///
    /// `None` for uncompressed `.warc` files, or for records indexed before
    /// this field was introduced.  When `Some`, the replay handler should use
    /// this offset for the S3 Range GET instead of `offset`.
    pub c_offset: Option<u64>,
    /// Name of the collection this record belongs to. `"warc"` is the default
    /// WARC archive; other values name additional sources (e.g. a bucket of
    /// standalone PDFs). Determines which bucket replay fetches from.
    pub collection: String,
}

/// The default collection name for records from the primary WARC archive.
pub const DEFAULT_COLLECTION: &str = "warc";

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
        // Revisit records (dedup captures whose payload equals an earlier one)
        // carry the HTTP response headers but no body. We include them in the
        // CDX so the capture history is complete, but they get no fulltext doc
        // (the content is already indexed via the record they refer to).
        RecordType::Response | RecordType::Resource | RecordType::Revisit => {}
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

    // The WARC Content-Type for response records is usually
    // "application/http; msgtype=response" — that describes the WARC container,
    // not the page.  Extract the real MIME from the HTTP response headers inside
    // the block instead.  Fall back to the WARC Content-Type for resource records
    // (PDFs, images, etc.) where it IS the actual MIME type.
    let warc_ct = warc.header.content_type().map(|s| {
        s.split(';').next().unwrap_or(s).trim().to_ascii_lowercase()
    });
    let mime = match warc_ct.as_deref() {
        Some("application/http") | None => extract_http_content_type(&warc.block),
        Some(_) => warc_ct,
    };

    // Try to extract HTTP status from the response block
    let status = extract_http_status(&warc.block);

    // For a revisit, the block digest is of the headers-only block; the
    // *payload* digest is the identity of the referenced content and is what a
    // later replay resolver would match against. Prefer it for revisits.
    let digest = match record_type {
        RecordType::Revisit => warc.header.payload_digest().or_else(|| warc.header.block_digest()),
        _ => warc.header.block_digest(),
    }
    .map(|s| s.to_owned());
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
        c_offset: None, // set by the caller for .warc.gz files
        collection: DEFAULT_COLLECTION.to_owned(), // overridden for non-WARC sources
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

/// Extract the `Content-Type` MIME type from HTTP response headers in `block`.
///
/// Strips parameters (`text/html; charset=utf-8` → `text/html`) and lower-cases
/// the result.  Returns `None` if no `Content-Type` header is present.
fn extract_http_content_type(block: &[u8]) -> Option<String> {
    // Find end of HTTP header section (\r\n\r\n).
    let hdr_end = block.windows(4).position(|w| w == b"\r\n\r\n")
        .unwrap_or(block.len());
    let hdr_str = std::str::from_utf8(&block[..hdr_end]).ok()?;

    // Skip the HTTP status line and scan headers.
    for line in hdr_str.lines().skip(1) {
        if line.is_empty() { break; }
        if let Some((name, val)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-type") {
                let mime = val.trim()
                    .split(';')
                    .next()
                    .unwrap_or(val.trim())
                    .trim()
                    .to_ascii_lowercase();
                if !mime.is_empty() {
                    return Some(mime);
                }
            }
        }
    }
    None
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

    // When the WARC Content-Type is "application/http" (the standard WARC
    // container type for response records), we must look inside the HTTP
    // block to get the real MIME type.
    #[test]
    fn from_warc_response_with_application_http_content_type() {
        let block = b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<html/>";
        let rec = make_warc_record(
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0010>"),
                ("WARC-Target-URI", "https://example.com/"),
                // Standard WARC Content-Type for response records
                ("Content-Type", "application/http; msgtype=response"),
                ("Content-Length", &block.len().to_string()),
            ],
            block,
            0,
        );
        let cdx = from_warc_record(&rec, "test.warc.gz").unwrap().unwrap();
        // Must use HTTP Content-Type, not WARC Content-Type
        assert_eq!(cdx.mime.as_deref(), Some("text/html"));
    }

    #[test]
    fn from_warc_response_application_http_json() {
        let block = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{}";
        let rec = make_warc_record(
            &[
                ("WARC-Type", "response"),
                ("WARC-Date", "2024-03-15T12:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:test-0011>"),
                ("WARC-Target-URI", "https://api.example.com/data"),
                ("Content-Type", "application/http; msgtype=response"),
                ("Content-Length", &block.len().to_string()),
            ],
            block,
            0,
        );
        let cdx = from_warc_record(&rec, "test.warc.gz").unwrap().unwrap();
        assert_eq!(cdx.mime.as_deref(), Some("application/json"));
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

    #[test]
    fn from_warc_revisit_record_is_included_with_payload_digest() {
        // A revisit record: HTTP headers, no body, WARC-Payload-Digest links to
        // the original content.
        let http = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n";
        let rec = make_warc_record(
            &[
                ("WARC-Type", "revisit"),
                ("WARC-Date", "2026-07-20T10:00:00Z"),
                ("WARC-Record-ID", "<urn:uuid:rev-1>"),
                ("WARC-Target-URI", "https://obst.example/page"),
                ("WARC-Refers-To", "<urn:uuid:orig-1>"),
                ("WARC-Payload-Digest", "sha1:PAYLOADHASH"),
                ("WARC-Block-Digest", "sha1:HEADERSONLY"),
                ("WARC-Profile", "https://iana.org/assignments/warc/1.1/revisit/identical-payload-digest"),
                ("Content-Type", "application/http; msgtype=response"),
            ],
            http,
            4242,
        );
        let cdx = from_warc_record(&rec, "warc/zeno-00002.warc.gz").unwrap()
            .expect("revisit records are now included in the CDX");
        assert_eq!(cdx.original_url, "https://obst.example/page");
        assert_eq!(cdx.timestamp, "20260720100000");
        assert_eq!(cdx.status, Some(200));
        assert_eq!(cdx.mime.as_deref(), Some("text/html"));
        // Digest is the PAYLOAD digest (content identity), not the headers-only
        // block digest — so a future resolver can match it to the original.
        assert_eq!(cdx.digest.as_deref(), Some("sha1:PAYLOADHASH"));
        assert_eq!(cdx.offset, 4242);
    }
}
