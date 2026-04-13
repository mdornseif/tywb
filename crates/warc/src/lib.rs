//! `warc` — a streaming WARC (Web ARChive) parser.
//!
//! This crate is intentionally free of application-specific dependencies.
//! It parses WARC records from any `std::io::Read` source and exposes typed
//! access to headers and block bytes.
//!
//! # Crate design goals
//!
//! - **Streaming**: records are parsed and yielded one at a time; the full
//!   archive is never loaded into memory.
//! - **Zero-copy friendly**: block bytes are returned as `bytes::Bytes` and
//!   can be cheaply cloned or sliced.
//! - **No application coupling**: no S3, no SQLite, no Tantivy — just WARC.
//! - **Future extraction**: this crate is designed to be published as a
//!   standalone library with a stable public API.
//!
//! # Example
//!
//! ```no_run
//! use warc::reader::WarcIter;
//! use std::fs::File;
//!
//! for result in WarcIter::new(File::open("archive.warc").unwrap()) {
//!     let record = result.unwrap();
//!     if let Some(uri) = record.target_uri() {
//!         println!("{uri}");
//!     }
//! }
//! ```

pub mod error;
pub mod record;
pub mod reader;

pub use error::{WarcError, Result};
pub use record::{RecordType, WarcHeader, WarcRecord, WarcVersion};
pub use reader::{WarcIter, WarcReader};
