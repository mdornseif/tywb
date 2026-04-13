//! `warc-search-s3` — S3 object storage access for warc-search.
//!
//! Provides three things:
//!
//! 1. **[`client`]** — build an [`aws_sdk_s3::Client`] from our [`S3Config`],
//!    handling explicit credentials, custom endpoints, and path-style addressing.
//!
//! 2. **[`list`]** — page through a bucket with optional ETag-based incremental
//!    state so unchanged objects are skipped on subsequent indexer runs.
//!
//! 3. **[`fetch`]** — streaming full GET and byte-range GET.  Range GET is the
//!    core operation for Wayback replay: fetch only the exact bytes of one WARC
//!    record from a multi-GB archive file.

pub mod client;
pub mod error;
pub mod fetch;
pub mod list;

pub use client::build_client;
pub use error::{S3Error, Result};
pub use fetch::{
    get_bytes, get_range, get_range_stream, get_stream, head_object,
    parse_content_range, range_header, ObjectHead,
};
pub use list::{default_state_path, ListState, Lister, ObjectMeta};
