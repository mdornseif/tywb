use thiserror::Error;

#[derive(Debug, Error)]
pub enum CdxError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("WARC record missing required field for CDX: {0}")]
    MissingField(&'static str),

    #[error("invalid URL {url:?}: {reason}")]
    InvalidUrl { url: String, reason: String },

    #[error("invalid timestamp {0:?}: expected 14-digit YYYYMMDDHHmmss")]
    InvalidTimestamp(String),

    #[error("WARC error: {0}")]
    Warc(#[from] warc::WarcError),

    #[error("record not found: url={url}, timestamp={timestamp}")]
    NotFound { url: String, timestamp: String },
}

pub type Result<T> = std::result::Result<T, CdxError>;
