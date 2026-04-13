//! S3 bucket listing with ETag-based incremental state.
//!
//! The [`Lister`] pages through all objects under a prefix and yields
//! [`ObjectMeta`] entries.  Between runs it persists a state file (JSON) so
//! that unchanged objects (same ETag) are skipped on the next invocation.
//!
//! # State file format
//!
//! ```json
//! {
//!   "seen": {
//!     "crawls/2024/archive-001.warc.gz": "\"abc123etag\"",
//!     "crawls/2024/archive-002.warc.gz": "\"def456etag\""
//!   }
//! }
//! ```
//!
//! The file is written atomically (write to `.tmp`, then rename).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use aws_sdk_s3::Client;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::error::{S3Error, Result};

// ── Types ─────────────────────────────────────────────────────────────────────

/// Metadata for a single S3 object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub key: String,
    pub size: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl ObjectMeta {
    /// Returns true if this object is a WARC file (by extension).
    pub fn is_warc(&self) -> bool {
        let k = self.key.to_ascii_lowercase();
        k.ends_with(".warc") || k.ends_with(".warc.gz")
    }

    /// Returns true if this object is a PDF file.
    pub fn is_pdf(&self) -> bool {
        self.key.to_ascii_lowercase().ends_with(".pdf")
    }

    /// Returns true if this object is gzip-compressed.
    pub fn is_gzipped(&self) -> bool {
        self.key.to_ascii_lowercase().ends_with(".gz")
    }
}

// ── State file ────────────────────────────────────────────────────────────────

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ListState {
    /// key → ETag of the last time we processed this object.
    pub seen: HashMap<String, String>,
}

impl ListState {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = std::fs::read_to_string(path).map_err(|e| S3Error::StateFile {
            path: path.display().to_string(),
            reason: e.to_string(),
        })?;
        serde_json::from_str(&text).map_err(S3Error::Json)
    }

    /// Atomically write state to `path` (write `.tmp` then rename).
    pub fn save(&self, path: &Path) -> Result<()> {
        let tmp = path.with_extension("tmp");
        let text = serde_json::to_string_pretty(self)?;
        std::fs::write(&tmp, text).map_err(|e| S3Error::StateFile {
            path: tmp.display().to_string(),
            reason: e.to_string(),
        })?;
        std::fs::rename(&tmp, path).map_err(|e| S3Error::StateFile {
            path: path.display().to_string(),
            reason: e.to_string(),
        })?;
        Ok(())
    }

    /// Returns true if we have already seen this key with this exact ETag.
    pub fn is_unchanged(&self, key: &str, etag: &Option<String>) -> bool {
        match (self.seen.get(key), etag) {
            (Some(prev), Some(curr)) => prev == curr,
            _ => false,
        }
    }

    /// Record that we processed this key successfully.
    pub fn mark_seen(&mut self, key: &str, etag: Option<String>) {
        self.seen
            .insert(key.to_owned(), etag.unwrap_or_default());
    }
}

// ── Lister ────────────────────────────────────────────────────────────────────

/// Lists objects in an S3 bucket, optionally skipping unchanged ones.
pub struct Lister<'a> {
    client: &'a Client,
    bucket: String,
    prefix: Option<String>,
}

impl<'a> Lister<'a> {
    pub fn new(client: &'a Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            prefix: None,
        }
    }

    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Page through all objects and collect their metadata.
    ///
    /// This fetches *all* pages — for very large buckets (millions of objects)
    /// you may want to stream pages lazily.  For a few-TB archive the full list
    /// is typically under a few MB.
    pub async fn list_all(&self) -> Result<Vec<ObjectMeta>> {
        let mut objects = Vec::new();
        let mut continuation: Option<String> = None;

        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket);

            if let Some(pfx) = &self.prefix {
                req = req.prefix(pfx);
            }
            if let Some(token) = &continuation {
                req = req.continuation_token(token);
            }

            let resp = req.send().await.map_err(|e| S3Error::List {
                bucket: self.bucket.clone(),
                reason: format!("{e:#?}"),
            })?;

            for obj in resp.contents() {
                let key = obj.key().unwrap_or("").to_owned();
                if key.is_empty() {
                    continue;
                }
                objects.push(ObjectMeta {
                    key,
                    size: obj.size().unwrap_or(0) as u64,
                    etag: obj.e_tag().map(|s| s.to_owned()),
                    last_modified: obj
                        .last_modified()
                        .map(|t| t.to_string()),
                });
            }

            debug!(
                bucket = %self.bucket,
                page_size = resp.contents().len(),
                total_so_far = objects.len(),
                "listed S3 page"
            );

            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(|s| s.to_owned());
            } else {
                break;
            }
        }

        info!(
            bucket = %self.bucket,
            count = objects.len(),
            "S3 listing complete"
        );
        Ok(objects)
    }

    /// List all objects, returning only those that are new or changed
    /// according to `state`.
    pub async fn list_new_or_changed(
        &self,
        state: &ListState,
    ) -> Result<Vec<ObjectMeta>> {
        let all = self.list_all().await?;
        let new_or_changed: Vec<_> = all
            .into_iter()
            .filter(|obj| !state.is_unchanged(&obj.key, &obj.etag))
            .collect();

        info!(
            count = new_or_changed.len(),
            "objects to process (new or changed ETag)"
        );
        Ok(new_or_changed)
    }
}

// ── State file path helper ────────────────────────────────────────────────────

/// Default path for the list state file, adjacent to the CDX database.
pub fn default_state_path(cdx_db_path: &str) -> PathBuf {
    let p = Path::new(cdx_db_path);
    p.with_file_name("list_state.json")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    // ── ObjectMeta helpers ────────────────────────────────────────────────────

    fn meta(key: &str) -> ObjectMeta {
        ObjectMeta {
            key: key.to_owned(),
            size: 1024,
            etag: Some("\"abc123\"".to_owned()),
            last_modified: None,
        }
    }

    #[test]
    fn is_warc_dot_warc() {
        assert!(meta("crawls/archive.warc").is_warc());
    }

    #[test]
    fn is_warc_dot_warc_gz() {
        assert!(meta("crawls/archive.warc.gz").is_warc());
    }

    #[test]
    fn is_warc_case_insensitive() {
        assert!(meta("crawls/ARCHIVE.WARC.GZ").is_warc());
        assert!(meta("crawls/ARCHIVE.WARC").is_warc());
    }

    #[test]
    fn is_warc_false_for_pdf() {
        assert!(!meta("docs/file.pdf").is_warc());
    }

    #[test]
    fn is_warc_false_for_other() {
        assert!(!meta("readme.txt").is_warc());
    }

    #[test]
    fn is_pdf_true() {
        assert!(meta("docs/paper.pdf").is_pdf());
    }

    #[test]
    fn is_pdf_case_insensitive() {
        assert!(meta("docs/paper.PDF").is_pdf());
    }

    #[test]
    fn is_pdf_false_for_warc() {
        assert!(!meta("archive.warc").is_pdf());
    }

    #[test]
    fn is_gzipped_true() {
        assert!(meta("archive.warc.gz").is_gzipped());
    }

    #[test]
    fn is_gzipped_false() {
        assert!(!meta("archive.warc").is_gzipped());
    }

    // ── ListState ─────────────────────────────────────────────────────────────

    #[test]
    fn list_state_default_is_empty() {
        let state = ListState::default();
        assert!(state.seen.is_empty());
    }

    #[test]
    fn list_state_is_unchanged_known_key() {
        let mut state = ListState::default();
        state.mark_seen("key.warc.gz", Some("\"etag1\"".to_owned()));
        assert!(state.is_unchanged("key.warc.gz", &Some("\"etag1\"".to_owned())));
    }

    #[test]
    fn list_state_is_unchanged_false_different_etag() {
        let mut state = ListState::default();
        state.mark_seen("key.warc.gz", Some("\"etag1\"".to_owned()));
        assert!(!state.is_unchanged("key.warc.gz", &Some("\"etag2\"".to_owned())));
    }

    #[test]
    fn list_state_is_unchanged_false_unknown_key() {
        let state = ListState::default();
        assert!(!state.is_unchanged("unknown.warc.gz", &Some("\"etag1\"".to_owned())));
    }

    #[test]
    fn list_state_is_unchanged_false_no_etag() {
        let mut state = ListState::default();
        state.mark_seen("key.warc.gz", Some("\"etag1\"".to_owned()));
        // Object has no ETag — treat as changed (can't compare)
        assert!(!state.is_unchanged("key.warc.gz", &None));
    }

    #[test]
    fn list_state_mark_seen_overwrites() {
        let mut state = ListState::default();
        state.mark_seen("key.warc.gz", Some("\"etag1\"".to_owned()));
        state.mark_seen("key.warc.gz", Some("\"etag2\"".to_owned()));
        assert!(state.is_unchanged("key.warc.gz", &Some("\"etag2\"".to_owned())));
        assert!(!state.is_unchanged("key.warc.gz", &Some("\"etag1\"".to_owned())));
    }

    #[test]
    fn list_state_save_and_load_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");

        let mut state = ListState::default();
        state.mark_seen("a/archive.warc.gz", Some("\"etag-a\"".to_owned()));
        state.mark_seen("b/archive.warc.gz", Some("\"etag-b\"".to_owned()));
        state.save(&path).unwrap();

        let loaded = ListState::load(&path).unwrap();
        assert_eq!(loaded.seen.len(), 2);
        assert!(loaded.is_unchanged("a/archive.warc.gz", &Some("\"etag-a\"".to_owned())));
        assert!(loaded.is_unchanged("b/archive.warc.gz", &Some("\"etag-b\"".to_owned())));
    }

    #[test]
    fn list_state_load_missing_file_returns_default() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.json");
        let state = ListState::load(&path).unwrap();
        assert!(state.seen.is_empty());
    }

    #[test]
    fn list_state_load_corrupt_file_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        std::fs::write(&path, b"not valid json {{{{").unwrap();
        assert!(ListState::load(&path).is_err());
    }

    #[test]
    fn list_state_save_is_atomic() {
        // After save the .tmp file must not exist
        let dir = tempdir().unwrap();
        let path = dir.path().join("state.json");
        let state = ListState::default();
        state.save(&path).unwrap();
        assert!(!path.with_extension("tmp").exists());
        assert!(path.exists());
    }

    #[test]
    fn list_state_multiple_keys_independence() {
        let mut state = ListState::default();
        for i in 0..100 {
            state.mark_seen(&format!("key-{i}.warc.gz"), Some(format!("\"etag-{i}\"")));
        }
        for i in 0..100 {
            assert!(state.is_unchanged(
                &format!("key-{i}.warc.gz"),
                &Some(format!("\"etag-{i}\""))
            ));
        }
    }

    // ── default_state_path ────────────────────────────────────────────────────

    #[test]
    fn default_state_path_sibling_of_cdx_db() {
        let p = default_state_path("/var/lib/warc-search/cdx.db");
        assert_eq!(p, PathBuf::from("/var/lib/warc-search/list_state.json"));
    }

    #[test]
    fn default_state_path_relative() {
        let p = default_state_path("cdx.db");
        assert_eq!(p, PathBuf::from("list_state.json"));
    }
}
