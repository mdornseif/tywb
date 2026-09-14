//! `warc-search-cdx` — CDX index for WARC archives.
//!
//! Provides:
//! - [`surt`]: SURT URL canonicalization
//! - [`record`]: The [`CdxRecord`] type and helpers to build one from a [`warc::WarcRecord`]
//! - [`store`]: SQLite-backed [`CdxStore`] with Wayback-compatible closest-match lookup

pub mod error;
pub mod record;
pub mod store;
pub mod surt;

pub use error::{CdxError, Result};
pub use record::{
    format_timestamp, from_warc_record, parse_timestamp, CdxRecord, DEFAULT_COLLECTION,
};
pub use store::{BasicStats, CdxStats, CdxStore, WarcFileMeta, WarcFileRow, WarcInfoRecord};
