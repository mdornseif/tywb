//! Fetching one WARC record out of S3 by its CDX coordinates.
//!
//! This is the operation the whole design rests on: a CDX entry carries the S3
//! key plus the byte offset of its record, so reading one archived page costs
//! one small Range GET no matter how large the WARC file is.
//!
//! Used by replay, by `/text`, and by the wire-format scan.

use aws_sdk_s3::Client;
use std::io::Read as _;

use warc_search_cdx::CdxRecord;
use warc_search_s3::get_range;

/// Why a single-record fetch failed. Callers map these to their own reporting —
/// the server to HTTP statuses, the scanner to a skipped sample.
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// The CDX entry predates compressed-offset indexing, so the gzip member
    /// holding this record cannot be located. Only `index --force` fixes it.
    #[error("record has no compressed offset — re-index this file")]
    NoCompressedOffset,

    #[error(transparent)]
    S3(#[from] warc_search_s3::S3Error),

    #[error("gzip decode: {0}")]
    Gzip(std::io::Error),
}

/// Fetch the bytes of one WARC record: `WARC/1.0\r\n…` headers, then the block.
///
/// For a `.warc.gz` this Range-GETs the record's own gzip member and inflates
/// just that member — which is why every part of the replay path insists on
/// record-per-member files. For a plain `.warc` the record is read directly at
/// its offset.
pub async fn fetch_warc_record(
    s3: &Client,
    bucket: &str,
    rec: &CdxRecord,
) -> Result<Vec<u8>, FetchError> {
    if rec.s3_key.to_ascii_lowercase().ends_with(".gz") {
        let c_offset = rec.c_offset.ok_or(FetchError::NoCompressedOffset)?;

        // The compressed member's size is unknown but never exceeds the
        // uncompressed length; fetch that plus slack and let the decoder stop
        // at the member boundary.
        let compressed = get_range(s3, bucket, &rec.s3_key, c_offset, rec.length + 65536).await?;

        let mut out = Vec::new();
        flate2::read::GzDecoder::new(compressed.as_ref())
            .read_to_end(&mut out)
            .map_err(FetchError::Gzip)?;
        Ok(out)
    } else {
        // Enough to cover the WARC headers (~1 KB) plus the block.
        let bytes = get_range(s3, bucket, &rec.s3_key, rec.offset, rec.length + 4096).await?;
        Ok(bytes.to_vec())
    }
}

/// Skip the WARC header block and return the slice starting at the HTTP
/// response — everything up to and including the first `\r\n\r\n`.
pub fn warc_http_block(warc_bytes: &[u8]) -> &[u8] {
    const SEP: &[u8] = b"\r\n\r\n";
    warc_bytes
        .windows(4)
        .position(|w| w == SEP)
        .map(|i| &warc_bytes[i + 4..])
        .unwrap_or(warc_bytes)
}

#[cfg(test)]
mod tests {
    use super::warc_http_block;

    #[test]
    fn warc_http_block_starts_at_the_status_line() {
        let raw = b"WARC/1.0\r\nWARC-Type: response\r\nContent-Length: 5\r\n\r\n\
                    HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\nhello";
        assert!(warc_http_block(raw).starts_with(b"HTTP/1.1 200 OK"));
    }

    #[test]
    fn warc_http_block_without_a_separator_is_returned_whole() {
        assert_eq!(warc_http_block(b"no separator"), b"no separator");
    }
}
