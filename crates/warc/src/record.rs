use std::collections::HashMap;
use chrono::{DateTime, Utc};
use crate::error::{WarcError, Result};

// ── Record type ───────────────────────────────────────────────────────────────

/// The value of the `WARC-Type` header field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RecordType {
    Warcinfo,
    Response,
    Resource,
    Request,
    Metadata,
    Revisit,
    Conversion,
    Continuation,
    /// Any type not in the spec (forward-compat).
    Other(String),
}

impl RecordType {
    pub fn parse(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "warcinfo"     => Self::Warcinfo,
            "response"     => Self::Response,
            "resource"     => Self::Resource,
            "request"      => Self::Request,
            "metadata"     => Self::Metadata,
            "revisit"      => Self::Revisit,
            "conversion"   => Self::Conversion,
            "continuation" => Self::Continuation,
            other          => Self::Other(other.to_owned()),
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::Warcinfo      => "warcinfo",
            Self::Response      => "response",
            Self::Resource      => "resource",
            Self::Request       => "request",
            Self::Metadata      => "metadata",
            Self::Revisit       => "revisit",
            Self::Conversion    => "conversion",
            Self::Continuation  => "continuation",
            Self::Other(s)      => s.as_str(),
        }
    }
}

impl std::fmt::Display for RecordType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── WARC Version ─────────────────────────────────────────────────────────────

/// Represents the WARC version string (e.g. `WARC/1.0` or `WARC/1.1`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WarcVersion {
    pub major: u8,
    pub minor: u8,
}

impl WarcVersion {
    pub const V1_0: Self = Self { major: 1, minor: 0 };
    pub const V1_1: Self = Self { major: 1, minor: 1 };

    /// Parse `WARC/1.0` or `WARC/1.1`.
    pub fn parse(line: &str) -> Result<Self> {
        let line = line.trim();
        let rest = line
            .strip_prefix("WARC/")
            .ok_or_else(|| WarcError::InvalidVersion(line.to_owned()))?;
        let (maj, min) = rest
            .split_once('.')
            .ok_or_else(|| WarcError::InvalidVersion(line.to_owned()))?;
        let major = maj
            .trim()
            .parse::<u8>()
            .map_err(|_| WarcError::InvalidVersion(line.to_owned()))?;
        let minor = min
            .trim()
            .parse::<u8>()
            .map_err(|_| WarcError::InvalidVersion(line.to_owned()))?;
        Ok(Self { major, minor })
    }
}

impl std::fmt::Display for WarcVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WARC/{}.{}", self.major, self.minor)
    }
}

// ── Header ────────────────────────────────────────────────────────────────────

/// All header fields of a single WARC record.
///
/// Well-known fields are exposed as typed accessors; all raw fields are also
/// available via `get()` for forward-compatibility.
#[derive(Debug, Clone)]
pub struct WarcHeader {
    pub version: WarcVersion,
    /// Ordered list of (name-lowercased, original-value) pairs.
    /// Multiple values for the same field name are preserved in order.
    fields: Vec<(String, String)>,
    /// Fast lookup: lowercase field name → index into `fields`.
    index: HashMap<String, usize>,
}

impl WarcHeader {
    /// Create a header from a raw field list (order preserved).
    pub(crate) fn from_fields(version: WarcVersion, fields: Vec<(String, String)>) -> Self {
        let mut index = HashMap::with_capacity(fields.len());
        for (i, (name, _)) in fields.iter().enumerate() {
            // last value wins for the fast-path index; iterate all for multi-value
            index.insert(name.clone(), i);
        }
        Self { version, fields, index }
    }

    /// Return the last value for `name` (case-insensitive).
    pub fn get(&self, name: &str) -> Option<&str> {
        let key = name.to_ascii_lowercase();
        self.index.get(&key).map(|&i| self.fields[i].1.as_str())
    }

    /// Return all values for `name` in document order.
    pub fn get_all(&self, name: &str) -> impl Iterator<Item = &str> {
        let key = name.to_ascii_lowercase();
        self.fields
            .iter()
            .filter(move |(n, _)| *n == key)
            .map(|(_, v)| v.as_str())
    }

    /// Iterate all (name, value) pairs in document order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.fields.iter().map(|(n, v)| (n.as_str(), v.as_str()))
    }

    // ── Typed accessors for well-known fields ────────────────────────────────

    pub fn record_type(&self) -> Result<RecordType> {
        self.get("warc-type")
            .map(RecordType::parse)
            .ok_or(WarcError::MissingField("WARC-Type"))
    }

    pub fn record_id(&self) -> Result<&str> {
        self.get("warc-record-id")
            .ok_or(WarcError::MissingField("WARC-Record-ID"))
    }

    pub fn content_length(&self) -> Result<usize> {
        let raw = self
            .get("content-length")
            .ok_or(WarcError::MissingField("Content-Length"))?;
        raw.trim()
            .parse::<usize>()
            .map_err(|_| WarcError::InvalidContentLength(raw.to_owned()))
    }

    pub fn date(&self) -> Result<DateTime<Utc>> {
        let raw = self
            .get("warc-date")
            .ok_or(WarcError::MissingField("WARC-Date"))?;
        DateTime::parse_from_rfc3339(raw)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(|_| WarcError::InvalidDate(raw.to_owned()))
    }

    /// `WARC-Target-URI` — present on response/request/resource records.
    pub fn target_uri(&self) -> Option<&str> {
        self.get("warc-target-uri")
    }

    /// `Content-Type`
    pub fn content_type(&self) -> Option<&str> {
        self.get("content-type")
    }

    /// `WARC-Refers-To`
    pub fn refers_to(&self) -> Option<&str> {
        self.get("warc-refers-to")
    }

    /// `WARC-Block-Digest` e.g. `sha1:ABCDEF…`
    pub fn block_digest(&self) -> Option<&str> {
        self.get("warc-block-digest")
    }

    /// `WARC-Payload-Digest`
    pub fn payload_digest(&self) -> Option<&str> {
        self.get("warc-payload-digest")
    }

    /// `WARC-Truncated`
    pub fn truncated(&self) -> Option<&str> {
        self.get("warc-truncated")
    }

    /// `WARC-Warcinfo-ID` — links other records back to their warcinfo record.
    pub fn warcinfo_id(&self) -> Option<&str> {
        self.get("warc-warcinfo-id")
    }

    /// `WARC-Concurrent-To` — may appear multiple times.
    pub fn concurrent_to(&self) -> impl Iterator<Item = &str> {
        self.get_all("warc-concurrent-to")
    }

    /// `WARC-IP-Address`
    pub fn ip_address(&self) -> Option<&str> {
        self.get("warc-ip-address")
    }

    /// `WARC-Identified-Payload-Type`
    pub fn identified_payload_type(&self) -> Option<&str> {
        self.get("warc-identified-payload-type")
    }
}

// ── Record ────────────────────────────────────────────────────────────────────

/// A fully parsed WARC record: header + raw block bytes.
///
/// The block is the raw content-block of the record.  For `response` records
/// this is the full HTTP response (status line + headers + body).  Callers that
/// need only the HTTP body are responsible for further parsing.
#[derive(Debug, Clone)]
pub struct WarcRecord {
    pub header: WarcHeader,
    /// Raw bytes of the record block (after WARC headers, before the trailing
    /// `\r\n\r\n` separator).
    pub block: bytes::Bytes,
    /// Byte offset of this record from the start of the (uncompressed) stream.
    pub offset: u64,
}

impl WarcRecord {
    /// Convenience: the record's `WARC-Type`.
    pub fn record_type(&self) -> Result<RecordType> {
        self.header.record_type()
    }

    /// Convenience: `WARC-Target-URI`.
    pub fn target_uri(&self) -> Option<&str> {
        self.header.target_uri()
    }

    /// For HTTP response records, strip the HTTP header and return only the
    /// body bytes.  Returns `None` if the separator cannot be found.
    pub fn http_body(&self) -> Option<&[u8]> {
        // HTTP header ends at the first \r\n\r\n
        let sep = b"\r\n\r\n";
        self.block
            .windows(sep.len())
            .position(|w| w == sep)
            .map(|pos| &self.block[pos + sep.len()..])
    }

    /// Return the HTTP status line and headers as a string slice (lossy).
    pub fn http_header_str(&self) -> Option<&[u8]> {
        let sep = b"\r\n\r\n";
        self.block
            .windows(sep.len())
            .position(|w| w == sep)
            .map(|pos| &self.block[..pos])
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_header(fields: &[(&str, &str)]) -> WarcHeader {
        WarcHeader::from_fields(
            WarcVersion::V1_0,
            fields
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
                .collect(),
        )
    }

    #[test]
    fn version_parse_v1_0() {
        let v = WarcVersion::parse("WARC/1.0").unwrap();
        assert_eq!(v, WarcVersion::V1_0);
    }

    #[test]
    fn version_parse_v1_1() {
        let v = WarcVersion::parse("WARC/1.1").unwrap();
        assert_eq!(v, WarcVersion::V1_1);
    }

    #[test]
    fn version_parse_with_trailing_whitespace() {
        let v = WarcVersion::parse("WARC/1.0  \r\n").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 0);
    }

    #[test]
    fn version_parse_invalid_no_prefix() {
        assert!(WarcVersion::parse("HTTP/1.1").is_err());
    }

    #[test]
    fn version_parse_invalid_no_dot() {
        assert!(WarcVersion::parse("WARC/10").is_err());
    }

    #[test]
    fn version_display() {
        assert_eq!(WarcVersion::V1_0.to_string(), "WARC/1.0");
        assert_eq!(WarcVersion::V1_1.to_string(), "WARC/1.1");
    }

    #[test]
    fn record_type_parse_known() {
        assert_eq!(RecordType::parse("response"),     RecordType::Response);
        assert_eq!(RecordType::parse("request"),      RecordType::Request);
        assert_eq!(RecordType::parse("warcinfo"),     RecordType::Warcinfo);
        assert_eq!(RecordType::parse("metadata"),     RecordType::Metadata);
        assert_eq!(RecordType::parse("resource"),     RecordType::Resource);
        assert_eq!(RecordType::parse("revisit"),      RecordType::Revisit);
        assert_eq!(RecordType::parse("conversion"),   RecordType::Conversion);
        assert_eq!(RecordType::parse("continuation"), RecordType::Continuation);
    }

    #[test]
    fn record_type_parse_case_insensitive() {
        assert_eq!(RecordType::parse("RESPONSE"), RecordType::Response);
        assert_eq!(RecordType::parse("Response"), RecordType::Response);
    }

    #[test]
    fn record_type_parse_unknown() {
        assert_eq!(
            RecordType::parse("x-custom"),
            RecordType::Other("x-custom".to_owned())
        );
    }

    #[test]
    fn header_get_case_insensitive() {
        let h = make_header(&[("WARC-Type", "response")]);
        assert_eq!(h.get("warc-type"), Some("response"));
        assert_eq!(h.get("WARC-TYPE"), Some("response"));
        assert_eq!(h.get("Warc-Type"), Some("response"));
    }

    #[test]
    fn header_get_missing() {
        let h = make_header(&[("WARC-Type", "response")]);
        assert_eq!(h.get("does-not-exist"), None);
    }

    #[test]
    fn header_get_all_multiple_values() {
        let h = make_header(&[
            ("WARC-Concurrent-To", "<urn:uuid:aaa>"),
            ("WARC-Concurrent-To", "<urn:uuid:bbb>"),
        ]);
        let all: Vec<&str> = h.get_all("warc-concurrent-to").collect();
        assert_eq!(all, vec!["<urn:uuid:aaa>", "<urn:uuid:bbb>"]);
    }

    #[test]
    fn header_content_length_valid() {
        let h = make_header(&[("Content-Length", "1234")]);
        assert_eq!(h.content_length().unwrap(), 1234);
    }

    #[test]
    fn header_content_length_with_spaces() {
        let h = make_header(&[("Content-Length", "  42  ")]);
        assert_eq!(h.content_length().unwrap(), 42);
    }

    #[test]
    fn header_content_length_zero() {
        let h = make_header(&[("Content-Length", "0")]);
        assert_eq!(h.content_length().unwrap(), 0);
    }

    #[test]
    fn header_content_length_invalid() {
        let h = make_header(&[("Content-Length", "abc")]);
        assert!(h.content_length().is_err());
    }

    #[test]
    fn header_content_length_missing() {
        let h = make_header(&[]);
        assert!(h.content_length().is_err());
    }

    #[test]
    fn header_date_valid_rfc3339() {
        let h = make_header(&[("WARC-Date", "2024-03-15T12:00:00Z")]);
        let dt = h.date().unwrap();
        assert_eq!(dt.year(), 2024);
        assert_eq!(dt.month(), 3);
        assert_eq!(dt.day(), 15);
    }

    #[test]
    fn header_date_invalid() {
        let h = make_header(&[("WARC-Date", "not-a-date")]);
        assert!(h.date().is_err());
    }

    #[test]
    fn header_date_missing() {
        let h = make_header(&[]);
        assert!(h.date().is_err());
    }

    #[test]
    fn header_record_type_missing() {
        let h = make_header(&[]);
        assert!(h.record_type().is_err());
    }

    #[test]
    fn record_http_body_extraction() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>hello</html>";
        let rec = WarcRecord {
            header: make_header(&[
                ("WARC-Type", "response"),
                ("Content-Length", &raw.len().to_string()),
            ]),
            block: bytes::Bytes::from_static(raw),
            offset: 0,
        };
        assert_eq!(rec.http_body().unwrap(), b"<html>hello</html>");
    }

    #[test]
    fn record_http_body_no_separator() {
        let raw = b"no separator here";
        let rec = WarcRecord {
            header: make_header(&[("WARC-Type", "warcinfo"), ("Content-Length", "17")]),
            block: bytes::Bytes::from_static(raw),
            offset: 0,
        };
        assert!(rec.http_body().is_none());
    }

    #[test]
    fn record_http_body_empty_body() {
        let raw = b"HTTP/1.1 204 No Content\r\n\r\n";
        let rec = WarcRecord {
            header: make_header(&[("WARC-Type", "response"), ("Content-Length", "26")]),
            block: bytes::Bytes::from_static(raw),
            offset: 0,
        };
        assert_eq!(rec.http_body().unwrap(), b"");
    }

    #[test]
    fn record_http_header_str() {
        let raw = b"HTTP/1.1 200 OK\r\nX-Foo: bar\r\n\r\nbody";
        let rec = WarcRecord {
            header: make_header(&[("WARC-Type", "response"), ("Content-Length", "36")]),
            block: bytes::Bytes::from_static(raw),
            offset: 0,
        };
        assert_eq!(
            rec.http_header_str().unwrap(),
            b"HTTP/1.1 200 OK\r\nX-Foo: bar"
        );
    }

    // chrono re-exported for the date test above
    use chrono::Datelike;
}
