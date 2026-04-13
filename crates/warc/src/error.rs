use thiserror::Error;

#[derive(Debug, Error)]
pub enum WarcError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid WARC version line: {0:?}")]
    InvalidVersion(String),

    #[error("malformed header field (line {line}): {raw:?}")]
    MalformedHeader { line: usize, raw: String },

    #[error("missing required WARC header field: {0}")]
    MissingField(&'static str),

    #[error("invalid Content-Length value: {0:?}")]
    InvalidContentLength(String),

    #[error("block length mismatch: declared {declared}, actual {actual}")]
    BlockLengthMismatch { declared: usize, actual: usize },

    #[error("unexpected end of file after {bytes_read} bytes")]
    UnexpectedEof { bytes_read: usize },

    #[error("invalid WARC record type: {0:?}")]
    InvalidRecordType(String),

    #[error("missing block terminator (expected \\r\\n\\r\\n)")]
    MissingTerminator,

    #[error("header too large: {size} bytes exceeds limit {limit}")]
    HeaderTooLarge { size: usize, limit: usize },

    #[error("invalid date format: {0:?}")]
    InvalidDate(String),

    #[error("truncated header block")]
    TruncatedHeader,
}

pub type Result<T> = std::result::Result<T, WarcError>;
