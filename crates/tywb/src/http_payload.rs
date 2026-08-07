//! Reading the HTTP response block stored inside a WARC record.
//!
//! A WARC holds the response **exactly as it came off the wire**. That means
//! the bytes after the headers are not the content yet: they may still carry
//! `Transfer-Encoding: chunked` framing, and inside that a `Content-Encoding`
//! of gzip or deflate. Anything that wants to *read* the content — the
//! indexer, `/text`, replay — has to peel both, in that order.
//!
//! Every consumer goes through [`parse_http_block`] + [`HttpParts::payload`] so
//! there is one implementation of that peeling, not three that drift apart.
//!
//! # The headers are claims, not facts
//!
//! Crawlers rewrite bodies and forget to rewrite the headers that describe
//! them. Both directions occur in this archive:
//!
//! - framing present, `Transfer-Encoding` header absent — so chunking is
//!   detected structurally ([`dechunk`] succeeds only if the *whole* body
//!   parses as a chunk sequence), never from the header;
//! - `Content-Encoding: gzip` on a body the crawler already decoded — so a
//!   failed decode falls back to the bytes as stored, unless they really are
//!   binary, which is reported rather than passed off as text.

use std::borrow::Cow;
use std::io::Read;

use tracing::debug;

/// Ceiling on a decoded payload. A compressed body can expand enormously, and
/// nothing downstream needs more than this from a single record.
pub const MAX_DECODED_BYTES: usize = 128 * 1024 * 1024;

/// The parts of a stored HTTP response its readers need.
pub struct HttpParts<'a> {
    pub status:           Option<u16>,
    pub content_type:     Option<String>,
    /// Lowercased, so callers can match on it directly.
    pub content_encoding: Option<String>,
    pub location:         Option<String>,
    /// The body as stored — still framed and still encoded.
    /// Use [`HttpParts::payload`] to get the content.
    pub body:             &'a [u8],
}

/// Split a stored HTTP response block into its headers of interest and its body.
///
/// `block` must start with the status line. A block with no header/body
/// separator is treated as all body — that is what a `resource` record with no
/// HTTP framing looks like.
pub fn parse_http_block(block: &[u8]) -> HttpParts<'_> {
    let sep = b"\r\n\r\n";
    let Some(body_start) = block.windows(4).position(|w| w == sep).map(|i| i + 4) else {
        return HttpParts {
            status: None, content_type: None, content_encoding: None,
            location: None, body: block,
        };
    };

    let header_str = String::from_utf8_lossy(&block[..body_start - 4]);
    let mut lines = header_str.lines();

    let status = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok());

    let mut content_type     = None;
    let mut content_encoding = None;
    let mut location         = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else { continue };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-type"     => content_type     = Some(value.trim().to_owned()),
            "content-encoding" => content_encoding = Some(value.trim().to_ascii_lowercase()),
            "location"         => location         = Some(value.trim().to_owned()),
            _ => {}
        }
    }

    HttpParts { status, content_type, content_encoding, location, body: &block[body_start..] }
}

impl<'a> HttpParts<'a> {
    /// The actual content: chunk framing removed, `Content-Encoding` undone.
    ///
    /// Decoding stops at `max_out` bytes — callers that only index a prefix can
    /// pass their own limit and skip inflating the rest. Borrows the stored
    /// bytes when there was nothing to peel.
    ///
    /// `Err` means the bytes cannot be turned into content at all: an encoding
    /// with no decoder (`br`, `zstd`), or a genuinely binary body whose decode
    /// failed. The message is safe to show a client.
    pub fn payload(&self, max_out: usize) -> Result<Cow<'a, [u8]>, String> {
        let unframed: Cow<'a, [u8]> = match dechunk(self.body) {
            Some(v) => Cow::Owned(v),
            None    => Cow::Borrowed(self.body),
        };

        let Some(enc) = self.content_encoding.as_deref() else { return Ok(unframed) };
        match enc {
            "" | "identity" | "none" => Ok(unframed),
            "gzip" | "x-gzip" => {
                match inflate(flate2::read::GzDecoder::new(unframed.as_ref()), max_out) {
                    Some(out) => Ok(Cow::Owned(out)),
                    None      => stored_as_is(unframed, enc),
                }
            }
            // "deflate" on the wire is zlib-wrapped in theory and raw deflate
            // in practice; try both.
            "deflate" => {
                let zlib = inflate(flate2::read::ZlibDecoder::new(unframed.as_ref()), max_out);
                let raw  = || inflate(flate2::read::DeflateDecoder::new(unframed.as_ref()), max_out);
                match zlib.or_else(raw) {
                    Some(out) => Ok(Cow::Owned(out)),
                    None      => stored_as_is(unframed, enc),
                }
            }
            other => Err(format!("unsupported Content-Encoding: {other}")),
        }
    }
}

/// Read a decoder to the end, or `None` if the stream was not what it claimed.
///
/// A capture cut off mid-stream decodes partially; that prefix is real content,
/// so it is kept rather than thrown away.
fn inflate(decoder: impl Read, max_out: usize) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    match decoder.take(max_out as u64).read_to_end(&mut out) {
        Ok(_)                    => Some(out),
        Err(_) if !out.is_empty() => Some(out),
        Err(_)                   => None,
    }
}

/// Use the body as stored — unless it really is binary, in which case the
/// encoding was genuine and merely undecodable (a truncated or corrupt
/// stream). Returning that as text would be mojibake, so say so instead.
fn stored_as_is<'a>(body: Cow<'a, [u8]>, enc: &str) -> Result<Cow<'a, [u8]>, String> {
    if body.iter().take(1024).any(|&b| b == 0) {
        return Err(format!("body claims Content-Encoding: {enc} but could not be decoded"));
    }
    debug!(enc, bytes = body.len(), "body is not really encoded — using it as stored");
    Ok(body)
}

/// Undo `Transfer-Encoding: chunked` framing.
///
/// Returns `Some` only when the whole body parses as a chunk sequence — see the
/// module docs for why the header does not get a vote. A capture cut off
/// mid-chunk keeps what it has.
pub fn dechunk(body: &[u8]) -> Option<Vec<u8>> {
    // Reject the ordinary case in one byte. This runs over every record of a
    // multi-GB WARC, and a body that was never chunked must not pay for a scan
    // looking for a CRLF that is not there.
    if !body.first().is_some_and(u8::is_ascii_hexdigit) {
        return None;
    }

    let mut out = Vec::with_capacity(body.len());
    let mut i = 0usize;

    loop {
        let line_end = find_crlf(body, i)?;
        // chunk-size [ ";" chunk-ext ]
        let line = &body[i..line_end];
        let digits = line.split(|&b| b == b';').next()?;
        if digits.is_empty() || digits.len() > 16 || !digits.iter().all(u8::is_ascii_hexdigit) {
            return None;
        }
        let size = usize::from_str_radix(std::str::from_utf8(digits).ok()?, 16).ok()?;
        i = line_end + 2;

        // The terminating 0-size chunk; any trailer after it is not content.
        if size == 0 {
            return Some(out);
        }

        let end = i.checked_add(size)?;
        if end > body.len() {
            // A capture cut off mid-chunk: keep what is actually there.
            out.extend_from_slice(&body[i..]);
            return Some(out);
        }
        out.extend_from_slice(&body[i..end]);
        match body.get(end..end + 2) {
            Some(b"\r\n") => i = end + 2,
            // No CRLF where the framing demands one — this was never chunked.
            Some(_)       => return None,
            None          => return Some(out),   // ends exactly at the last chunk
        }
    }
}

/// Find the CRLF that ends a chunk-size line.
///
/// Bounded on purpose: a size line is a handful of hex digits plus at most a
/// short extension. Not scanning past that keeps the miss cheap on the
/// unchunked bodies that make up most of a WARC — and a "chunk" whose header
/// runs longer than this was not a chunk header.
fn find_crlf(haystack: &[u8], from: usize) -> Option<usize> {
    const MAX_LINE: usize = 64;
    let end = from.saturating_add(MAX_LINE).min(haystack.len());
    haystack.get(from..end)?.windows(2).position(|w| w == b"\r\n").map(|p| p + from)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn chunked(data: &[u8]) -> Vec<u8> {
        let mut out = format!("{:x}\r\n", data.len()).into_bytes();
        out.extend_from_slice(data);
        out.extend_from_slice(b"\r\n0\r\n\r\n");
        out
    }

    fn block(headers: &str, body: &[u8]) -> Vec<u8> {
        let mut out = format!("HTTP/1.1 200 OK\r\n{headers}\r\n").into_bytes();
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn parse_http_block_reads_the_headers_that_matter() {
        let raw = b"HTTP/1.1 301 Moved\r\n\
                    Content-Type: text/html; charset=utf-8\r\n\
                    Content-Encoding: GZIP\r\n\
                    Location: https://example.com/new\r\n\r\nbody bytes";
        let parts = parse_http_block(raw);
        assert_eq!(parts.status, Some(301));
        assert_eq!(parts.content_type.as_deref(), Some("text/html; charset=utf-8"));
        assert_eq!(parts.content_encoding.as_deref(), Some("gzip")); // lowercased
        assert_eq!(parts.location.as_deref(), Some("https://example.com/new"));
        assert_eq!(parts.body, b"body bytes");
    }

    #[test]
    fn parse_http_block_without_headers_is_all_body() {
        let parts = parse_http_block(b"just bytes");
        assert!(parts.status.is_none() && parts.content_type.is_none());
        assert_eq!(parts.body, b"just bytes");
    }

    #[test]
    fn payload_peels_chunking_then_gzip() {
        // The real shape: chunk framing wrapped around a gzip stream, which is
        // how https://example.com/ sits in this archive.
        let plain = b"<html>Apfelsorten in Deutschland</html>";
        let raw = block("Content-Encoding: gzip\r\n", &chunked(&gzip(plain)));
        let parts = parse_http_block(&raw);
        assert_eq!(parts.payload(MAX_DECODED_BYTES).unwrap().as_ref(), plain);
    }

    #[test]
    fn payload_handles_each_layer_on_its_own() {
        let plain = b"<html>Boskoop</html>";

        let only_chunked = block("Content-Type: text/html\r\n", &chunked(plain));
        assert_eq!(
            parse_http_block(&only_chunked).payload(MAX_DECODED_BYTES).unwrap().as_ref(),
            plain,
        );

        let only_gzip = block("Content-Encoding: gzip\r\n", &gzip(plain));
        assert_eq!(
            parse_http_block(&only_gzip).payload(MAX_DECODED_BYTES).unwrap().as_ref(),
            plain,
        );

        let neither = block("Content-Type: text/html\r\n", plain);
        assert_eq!(
            parse_http_block(&neither).payload(MAX_DECODED_BYTES).unwrap().as_ref(),
            plain,
        );
    }

    #[test]
    fn payload_borrows_when_there_is_nothing_to_peel() {
        let raw = block("Content-Type: text/html\r\n", b"<html>plain</html>");
        let parts = parse_http_block(&raw);
        assert!(matches!(parts.payload(MAX_DECODED_BYTES).unwrap(), Cow::Borrowed(_)));
    }

    #[test]
    fn payload_trusts_the_bytes_over_the_header() {
        // Seen in the wild: the crawler stored the decoded body but kept the
        // original Content-Encoding header.
        let plain = b"<html>Apfelsorten</html>";
        for enc in ["gzip", "deflate"] {
            let raw = block(&format!("Content-Encoding: {enc}\r\n"), plain);
            assert_eq!(
                parse_http_block(&raw).payload(MAX_DECODED_BYTES).unwrap().as_ref(),
                plain,
                "{enc}",
            );
        }
    }

    #[test]
    fn payload_refuses_undecodable_binary() {
        // A genuinely compressed body we failed to inflate must not come back
        // as mojibake.
        let raw = block(
            "Content-Encoding: gzip\r\n",
            b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03\xff\xfe not really gzip",
        );
        let err = parse_http_block(&raw).payload(MAX_DECODED_BYTES).unwrap_err();
        assert!(err.contains("gzip"), "{err}");
    }

    #[test]
    fn payload_refuses_an_encoding_it_cannot_decode() {
        let raw = block("Content-Encoding: br\r\n", b"\x1b\x2f binary");
        let err = parse_http_block(&raw).payload(MAX_DECODED_BYTES).unwrap_err();
        assert!(err.contains("br"), "{err}");
    }

    #[test]
    fn payload_stops_at_max_out() {
        let big = vec![b'x'; 4096];
        let raw = block("Content-Encoding: gzip\r\n", &gzip(&big));
        assert_eq!(parse_http_block(&raw).payload(100).unwrap().len(), 100);
    }

    #[test]
    fn payload_keeps_the_prefix_of_a_cut_off_gzip_stream() {
        let plain = vec![b'a'; 2048];
        let mut gz = gzip(&plain);
        gz.truncate(gz.len() - 8);   // lose the trailer and some data
        let raw = block("Content-Encoding: gzip\r\n", &gz);
        let out = parse_http_block(&raw).payload(MAX_DECODED_BYTES).unwrap();
        assert!(!out.is_empty() && out.iter().all(|&b| b == b'a'), "{} bytes", out.len());
    }

    #[test]
    fn dechunk_reassembles_a_chunked_body() {
        assert_eq!(dechunk(b"5\r\nRoter\r\na\r\n Berlepsch\r\n0\r\n\r\n").unwrap(),
                   b"Roter Berlepsch");
        // Chunk extensions are part of the size line, not the content.
        assert_eq!(dechunk(b"5;a=b\r\nRoter\r\n0\r\n\r\n").unwrap(), b"Roter");
    }

    #[test]
    fn dechunk_keeps_what_a_cut_off_capture_has() {
        assert_eq!(dechunk(b"20\r\nRoter Berlepsch").unwrap(), b"Roter Berlepsch");
    }

    #[test]
    fn dechunk_declines_bodies_that_are_not_chunked() {
        // Structure decides, so ordinary content must not be mistaken for it.
        assert!(dechunk(b"<html><body>Apfelsorten</body></html>").is_none());
        assert!(dechunk(b"173 is a number\r\nand this is prose\r\n").is_none());
        assert!(dechunk(b"5\r\nRoter is longer than five\r\n").is_none());
        assert!(dechunk(b"").is_none());
        // A hex-looking start with no size line in reach: bail, don't scan on.
        assert!(dechunk(&[b'a'; 100_000]).is_none());
    }
}
