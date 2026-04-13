use thiserror::Error;

#[derive(Debug, Error)]
pub enum S3Error {
    #[error("S3 error listing objects in bucket {bucket}: {reason}")]
    List { bucket: String, reason: String },

    #[error("S3 error getting object s3://{bucket}/{key}: {reason}")]
    Get { bucket: String, key: String, reason: String },

    #[error("S3 error putting object s3://{bucket}/{key}: {reason}")]
    Put { bucket: String, key: String, reason: String },

    #[error("S3 object not found: s3://{bucket}/{key}")]
    NotFound { bucket: String, key: String },

    #[error("invalid byte range: offset={offset} length={length} object_size={object_size}")]
    InvalidRange {
        offset: u64,
        length: u64,
        object_size: u64,
    },

    #[error("I/O error reading S3 stream: {0}")]
    Io(#[from] std::io::Error),

    #[error("S3 stream error: {0}")]
    Stream(String),

    #[error("state file error at {path}: {reason}")]
    StateFile { path: String, reason: String },

    #[error("JSON serialization error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("config error: {0}")]
    Config(#[from] warc_search_config::ConfigError),
}

pub type Result<T> = std::result::Result<T, S3Error>;
