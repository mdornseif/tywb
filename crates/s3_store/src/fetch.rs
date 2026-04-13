//! S3 object fetching: full streaming GET and byte-range GET.
//!
//! # Full streaming GET
//!
//! [`get_stream`] returns a [`ByteStream`] that yields chunks as they arrive.
//! For gzip-compressed objects (`.warc.gz`) wrap the stream with
//! [`get_decompressed`] which transparently decompresses on the fly using
//! `async-compression`.
//!
//! # Range GET
//!
//! [`get_range`] fetches only `offset..offset+length` bytes of an S3 object.
//! This is the key operation for Wayback replay: the CDX stores byte offsets
//! so we never download a full multi-GB WARC just to serve one record.
//!
//! # Back-pressure
//!
//! All streams use Tokio's async I/O so the download rate is governed by how
//! fast the consumer processes bytes — no unbounded buffering.

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use tracing::debug;

use crate::error::{S3Error, Result};

// ── Full streaming GET ────────────────────────────────────────────────────────

/// Stream all bytes of an S3 object.
///
/// Returns a raw [`ByteStream`].  The caller is responsible for
/// decompression if the object is gzip-compressed.
pub async fn get_stream(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<ByteStream> {
    debug!(bucket, key, "S3 GET");
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            // Distinguish 404 from other errors
            let msg = e.to_string();
            if msg.contains("NoSuchKey") || msg.contains("404") {
                S3Error::NotFound {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                }
            } else {
                S3Error::Get {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    reason: msg,
                }
            }
        })?;

    Ok(resp.body)
}

/// Collect a full S3 object into a `Bytes` buffer.
///
/// Only suitable for small objects (config files, manifests).
/// For WARC files use [`get_stream`] or [`get_range`].
pub async fn get_bytes(client: &Client, bucket: &str, key: &str) -> Result<Bytes> {
    let stream = get_stream(client, bucket, key).await?;
    let data = stream
        .collect()
        .await
        .map_err(|e| S3Error::Stream(e.to_string()))?;
    Ok(data.into_bytes())
}

// ── Range GET ─────────────────────────────────────────────────────────────────

/// Fetch a byte range from an S3 object.
///
/// `offset` is the zero-based start byte; `length` is the number of bytes.
/// The range is inclusive of both endpoints in the HTTP `Range` header:
/// `bytes=offset-(offset+length-1)`.
///
/// This is the primary operation for WARC record replay.
pub async fn get_range(
    client: &Client,
    bucket: &str,
    key: &str,
    offset: u64,
    length: u64,
) -> Result<Bytes> {
    if length == 0 {
        return Ok(Bytes::new());
    }

    let range_header = format!("bytes={}-{}", offset, offset + length - 1);
    debug!(bucket, key, range = %range_header, "S3 Range GET");

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(&range_header)
        .send()
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("NoSuchKey") || msg.contains("404") {
                S3Error::NotFound {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                }
            } else if msg.contains("InvalidRange") || msg.contains("416") {
                S3Error::InvalidRange {
                    offset,
                    length,
                    object_size: 0, // unknown at this point
                }
            } else {
                S3Error::Get {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    reason: msg,
                }
            }
        })?;

    let data = resp
        .body
        .collect()
        .await
        .map_err(|e| S3Error::Stream(e.to_string()))?;

    Ok(data.into_bytes())
}

/// Stream a byte range — useful for large ranges that should not be fully
/// buffered before processing (e.g. streaming a large WARC record to a client).
pub async fn get_range_stream(
    client: &Client,
    bucket: &str,
    key: &str,
    offset: u64,
    length: u64,
) -> Result<ByteStream> {
    if length == 0 {
        return Ok(ByteStream::from(Bytes::new()));
    }

    let range_header = format!("bytes={}-{}", offset, offset + length - 1);
    debug!(bucket, key, range = %range_header, "S3 Range GET (stream)");

    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(&range_header)
        .send()
        .await
        .map_err(|e| S3Error::Get {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            reason: e.to_string(),
        })?;

    Ok(resp.body)
}

// ── Object metadata ───────────────────────────────────────────────────────────

/// Fetch the size and ETag of an S3 object via HEAD request.
#[derive(Debug, Clone)]
pub struct ObjectHead {
    pub size: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
}

pub async fn head_object(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<ObjectHead> {
    let resp = client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            let msg = e.to_string();
            if msg.contains("404") || msg.contains("NotFound") {
                S3Error::NotFound {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                }
            } else {
                S3Error::Get {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    reason: msg,
                }
            }
        })?;

    Ok(ObjectHead {
        size: resp.content_length().unwrap_or(0) as u64,
        etag: resp.e_tag().map(|s| s.to_owned()),
        content_type: resp.content_type().map(|s| s.to_owned()),
    })
}

// ── PUT ───────────────────────────────────────────────────────────────────────

/// Upload `body` to `s3://{bucket}/{key}` with the given `content_type`.
pub async fn put_object(
    client: &Client,
    bucket: &str,
    key: &str,
    body: Bytes,
    content_type: &str,
) -> Result<()> {
    debug!(bucket, key, bytes = body.len(), "S3 PUT");
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .content_type(content_type)
        .send()
        .await
        .map_err(|e| S3Error::Put {
            bucket: bucket.to_owned(),
            key:    key.to_owned(),
            reason: e.to_string(),
        })?;
    Ok(())
}

// ── Range header helpers ──────────────────────────────────────────────────────

/// Format a `Range: bytes=X-Y` header value.
pub fn range_header(offset: u64, length: u64) -> String {
    format!("bytes={}-{}", offset, offset + length - 1)
}

/// Parse a `Content-Range: bytes X-Y/Z` response header.
/// Returns `(start, end, total)` or `None` if unparseable.
pub fn parse_content_range(header: &str) -> Option<(u64, u64, u64)> {
    // Format: "bytes 0-1023/5120"
    let rest = header.strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    Some((
        start.trim().parse().ok()?,
        end.trim().parse().ok()?,
        total.trim().parse().ok()?,
    ))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── range_header ──────────────────────────────────────────────────────────

    #[test]
    fn range_header_from_zero() {
        assert_eq!(range_header(0, 512), "bytes=0-511");
    }

    #[test]
    fn range_header_mid_range() {
        assert_eq!(range_header(1024, 256), "bytes=1024-1279");
    }

    #[test]
    fn range_header_single_byte() {
        assert_eq!(range_header(99, 1), "bytes=99-99");
    }

    #[test]
    fn range_header_large_offset() {
        assert_eq!(range_header(10_000_000, 65536), "bytes=10000000-10065535");
    }

    #[test]
    fn range_header_end_is_inclusive() {
        // offset=0 length=1 → bytes=0-0 (just one byte)
        let h = range_header(0, 1);
        let (start, end) = h
            .strip_prefix("bytes=")
            .unwrap()
            .split_once('-')
            .unwrap();
        let start: u64 = start.parse().unwrap();
        let end: u64 = end.parse().unwrap();
        assert_eq!(end - start + 1, 1);
    }

    // ── parse_content_range ───────────────────────────────────────────────────

    #[test]
    fn parse_content_range_valid() {
        let (s, e, t) = parse_content_range("bytes 0-1023/5120").unwrap();
        assert_eq!(s, 0);
        assert_eq!(e, 1023);
        assert_eq!(t, 5120);
    }

    #[test]
    fn parse_content_range_mid_range() {
        let (s, e, t) = parse_content_range("bytes 512-767/1024").unwrap();
        assert_eq!(s, 512);
        assert_eq!(e, 767);
        assert_eq!(t, 1024);
    }

    #[test]
    fn parse_content_range_large_numbers() {
        let (s, e, t) =
            parse_content_range("bytes 10000000-10065535/5368709120").unwrap();
        assert_eq!(s, 10_000_000);
        assert_eq!(e, 10_065_535);
        assert_eq!(t, 5_368_709_120);
    }

    #[test]
    fn parse_content_range_malformed_no_prefix() {
        assert!(parse_content_range("0-1023/5120").is_none());
    }

    #[test]
    fn parse_content_range_malformed_no_slash() {
        assert!(parse_content_range("bytes 0-1023").is_none());
    }

    #[test]
    fn parse_content_range_malformed_no_dash() {
        assert!(parse_content_range("bytes 01023/5120").is_none());
    }

    #[test]
    fn parse_content_range_empty() {
        assert!(parse_content_range("").is_none());
    }

    #[test]
    fn parse_content_range_non_numeric() {
        assert!(parse_content_range("bytes abc-def/xyz").is_none());
    }

    // ── range_header / parse_content_range roundtrip ──────────────────────────

    #[test]
    fn range_roundtrip_consistency() {
        let offset = 4096u64;
        let length = 8192u64;
        let header = range_header(offset, length);

        // Strip "bytes=" prefix and simulate what S3 returns as Content-Range
        let inner = header.strip_prefix("bytes=").unwrap();
        let (start_s, end_s) = inner.split_once('-').unwrap();
        let start: u64 = start_s.parse().unwrap();
        let end: u64 = end_s.parse().unwrap();

        assert_eq!(start, offset);
        assert_eq!(end, offset + length - 1);
        assert_eq!(end - start + 1, length);
    }

    // ── Error display ─────────────────────────────────────────────────────────

    #[test]
    fn error_not_found_display() {
        let e = S3Error::NotFound {
            bucket: "my-bucket".to_owned(),
            key: "path/to/file.warc".to_owned(),
        };
        let s = e.to_string();
        assert!(s.contains("my-bucket"));
        assert!(s.contains("path/to/file.warc"));
    }

    #[test]
    fn error_invalid_range_display() {
        let e = S3Error::InvalidRange {
            offset: 1000,
            length: 500,
            object_size: 100,
        };
        let s = e.to_string();
        assert!(s.contains("1000"));
        assert!(s.contains("500"));
    }

    #[test]
    fn error_list_display() {
        let e = S3Error::List {
            bucket: "b".to_owned(),
            reason: "timeout".to_owned(),
        };
        assert!(e.to_string().contains("timeout"));
    }
}
