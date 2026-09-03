//! Digest-keyed OCR text cache for the WARC path — the decoupling of indexing
//! and OCR.
//!
//! OCR of a scanned volume is tens of minutes, and a synchronous index run
//! pays for it in its own wall time: a full rebuild measured 63.7 of its 74.5
//! hours in gaps at multiples of the Tika timeout, the indexer idle while two
//! Tesseract processes worked on one document. And since the fulltext index
//! does not store the text it indexed, every rebuild paid the OCR again.
//!
//! A PDF inside a WARC has no S3 object of its own, so the `store_text`
//! sidecar-next-to-the-object cannot apply. It does have a content digest,
//! though, which the CDX record already carries — the same idea works with the
//! digest in place of the ETag:
//!
//! * the index run looks the digest up here ([`OcrCache::get`]). A hit is
//!   seconds from disk; a miss is *queued* ([`OcrCache::enqueue`]) and the run
//!   moves on — the record still gets its CDX entry, just no fulltext yet;
//! * `tywb ocr-worker` drains the queue in its own process, fetches the
//!   record with one Range GET, extracts with all the time in the world, and
//!   stores the text under the digest ([`OcrCache::put`]);
//! * the next index run finds everything in the cache. The digest dedupes:
//!   the same PDF collected in two crawls is extracted once.
//!
//! The file format is the same `tywb-text/2` the `store_text` sidecars use, so
//! a change to the text pipeline bumps one version in both places — only the
//! validity marker differs: `source-digest=` instead of `source-etag=`, and
//! the digest describes the content even more precisely than the ETag did.
//!
//! ## Permanent and transient
//!
//! A PDF whose extraction fails *because of the file* — too large, truncated,
//! no text at all, undecodable payload — gets an empty cache entry rather than
//! a retry: the answer cannot change, and without this the next rebuild would
//! re-fetch and re-parse it every time. An empty entry reads as "no text for
//! this one" to the index run, which then neither queues nor indexes it.
//! Failures of the *run* (S3, Tika) are retried up to `max_attempts` and then
//! parked in `failed/`, visible instead of looping forever.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Extracted texts, sharded: `text/<xy>/<digest>.tywb.txt`.
pub const TEXT_DIR: &str = "text";
/// Pending jobs, one JSON file per digest: `queue/<digest>.job`.
pub const QUEUE_DIR: &str = "queue";
/// Jobs that exhausted their attempts, kept visible: `failed/<digest>.job`.
pub const FAILED_DIR: &str = "failed";

/// First line of a cached text: the format, and the digest it came from.
const MAGIC: &str = "tywb-text/2";

/// The counter that keeps concurrent `put`s from sharing a temp file.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

// ── The job ───────────────────────────────────────────────────────────────────

/// One deferred extraction: enough to fetch the record's bytes and to name
/// the cache entry. Serialised as JSON into `queue/`, named after the digest —
/// queueing the same document twice leaves one job.
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct OcrJob {
    /// The cache key — the CDX record's (payload-preferred) digest.
    pub digest: String,
    /// The WARC bucket the record lives in.
    pub bucket: String,
    /// S3 key of the WARC file holding the record.
    pub s3_key: String,
    /// Uncompressed offset of the record within the WARC.
    pub offset: u64,
    /// Compressed offset of the record's gzip member; `None` in a plain `.warc`.
    pub c_offset: Option<u64>,
    /// Length of the WARC record.
    pub length: u64,
    /// Original URL, for logging and the title fallback.
    pub url: String,
    /// Extraction attempts so far, bumped by the worker on transient failure.
    #[serde(default)]
    pub attempts: u32,
}

// ── The cache ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OcrCache {
    root: PathBuf,
}

impl OcrCache {
    /// Open (and create, if needed) the cache at `root`.
    ///
    /// Failing to open it is an error the caller must not swallow in the
    /// *indexer*: with the cache configured, queueing is the expected
    /// behaviour, and a silent fallback to synchronous extraction is exactly
    /// the 74-hour run this exists to prevent.
    pub fn open(root: &Path) -> io::Result<Self> {
        for dir in [TEXT_DIR, QUEUE_DIR, FAILED_DIR] {
            fs::create_dir_all(root.join(dir))?;
        }
        Ok(Self { root: root.to_owned() })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    // ── the text store ─────────────────────────────────────────────────────

    /// The extracted text for `digest`, if it is cached and belongs to it.
    ///
    /// An empty body is a *negative entry*: extraction has been tried and the
    /// file gave no text. Callers distinguish it from a miss by comparing
    /// against `Some(("", ""))`.
    ///
    /// Every failure is a miss, not an error — the caller's fallback is
    /// queueing, which is the normal path here.
    pub fn get(&self, digest: &str) -> Option<(String, String)> {
        let path = self.text_path(digest)?;
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
            Err(e) => {
                debug!(path = %path.display(), err = %e, "cached text unreadable — treating as a miss");
                return None;
            }
        };
        let raw = String::from_utf8_lossy(&bytes);
        match parse_cached_text(&raw, digest) {
            Some(v) => Some(v),
            None => {
                debug!(path = %path.display(), "cached text is stale or malformed — treating as a miss");
                None
            }
        }
    }

    /// Whether `digest` has any cache entry at all — text or negative.
    pub fn has(&self, digest: &str) -> bool {
        self.get(digest).is_some()
    }

    /// Store `title`/`text` under `digest`. Best effort where the caller
    /// chooses; in the worker it happens *before* the job is removed, so a
    /// failed store costs a re-extraction, never a lost job.
    pub fn put(&self, digest: &str, title: &str, text: &str) -> io::Result<()> {
        let path = self
            .text_path(digest)
            .ok_or_else(|| invalid_digest(digest))?;
        let body = render_cached_text(digest, title, text);
        atomic_write(&path, body.as_bytes())
    }

    // ── the queue ──────────────────────────────────────────────────────────

    /// Append a job. A job for the same digest already in the queue is
    /// replaced — any record with that digest fetches the same content, so
    /// the queue never holds the same document twice.
    pub fn enqueue(&self, job: &OcrJob) -> io::Result<()> {
        let path = self
            .queue_path(&job.digest)
            .ok_or_else(|| invalid_digest(&job.digest))?;
        let body = serde_json::to_vec(job)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        atomic_write(&path, &body)
    }

    /// All pending jobs, parsed. Unreadable entries are parked in `failed/`
    /// rather than dropped — a job that cannot be read cannot be re-created
    /// from anywhere.
    pub fn queued_jobs(&self) -> Vec<(PathBuf, OcrJob)> {
        let mut out = Vec::new();
        let entries = match fs::read_dir(self.root.join(QUEUE_DIR)) {
            Ok(e) => e,
            Err(e) => {
                warn!(path = %self.root.join(QUEUE_DIR).display(), err = %e, "could not list the OCR queue");
                return out;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("job") {
                continue; // a half-written temp file, or something not ours
            }
            match fs::read_to_string(&path)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
                .and_then(|raw| {
                    serde_json::from_str(&raw)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("bad job JSON: {e}")))
                }) {
                Ok(job) => out.push((path, job)),
                Err(e) => {
                    warn!(path = %path.display(), err = %e, "unusable queue entry — parking it");
                    if let Err(e) = self.park(&path) {
                        warn!(path = %path.display(), err = %e, "could not park the unusable entry");
                    }
                }
            }
        }
        out.sort();
        out
    }

    /// Remove a finished job. Its text (or negative entry) must already be in
    /// the cache — that ordering is what makes a crash between the two a
    /// retry, not a loss.
    pub fn remove_job(&self, path: &Path) {
        if let Err(e) = fs::remove_file(path) {
            if e.kind() != io::ErrorKind::NotFound {
                warn!(path = %path.display(), err = %e,
                      "could not remove the finished job — it will be picked up again");
            }
        }
    }

    /// Record a transient failure. Bumps the attempt counter; once it reaches
    /// `max_attempts` the job is parked in `failed/` instead — visible, and
    /// not looping forever.
    pub fn retry(&self, path: &Path, job: &OcrJob, reason: &str, max_attempts: u32) -> JobRetry {
        let attempts = job.attempts.saturating_add(1);
        if attempts >= max_attempts {
            warn!(digest = %job.digest, url = %job.url, attempts, reason,
                  "job parked after repeated failures");
            if let Err(e) = self.park(path) {
                warn!(path = %path.display(), err = %e, "could not park the job — it stays queued");
                return JobRetry::Rescheduled;
            }
            return JobRetry::Parked;
        }
        debug!(digest = %job.digest, url = %job.url, attempts, reason,
               "extraction failed — job rescheduled");
        let mut job = job.clone();
        job.attempts = attempts;
        let body = serde_json::to_vec(&job)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e));
        if let Err(e) = body.and_then(|b| atomic_write(path, &b)) {
            warn!(path = %path.display(), err = %e, "could not reschedule the job");
        }
        JobRetry::Rescheduled
    }

    /// Move a queue file into `failed/`, overwriting anything already parked
    /// under the same name (same digest, same fate).
    fn park(&self, path: &Path) -> io::Result<()> {
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "job has no file name"))?;
        let dst = self.root.join(FAILED_DIR).join(name);
        if fs::rename(path, &dst).is_err() {
            // A rename that fails usually means the destination exists; on
            // some platforms rename refuses to overwrite. Drop the old one.
            let _ = fs::remove_file(&dst);
            fs::rename(path, &dst)?;
        }
        Ok(())
    }

    // ── paths ──────────────────────────────────────────────────────────────

    /// `text/<xy>/<digest>.tywb.txt`, or `None` when the digest is unusable
    /// as a file name (the WARC header is archive-controlled; anything that
    /// could carry a path separator is rejected outright).
    fn text_path(&self, digest: &str) -> Option<PathBuf> {
        let slug = file_slug(digest)?;
        Some(self.root.join(TEXT_DIR).join(shard_dir(digest)).join(format!("{slug}.tywb.txt")))
    }

    /// `queue/<digest>.job`, same rejection rule.
    fn queue_path(&self, digest: &str) -> Option<PathBuf> {
        let slug = file_slug(digest)?;
        Some(self.root.join(QUEUE_DIR).join(format!("{slug}.job")))
    }
}

/// What [`OcrCache::retry`] did with a failed job.
#[derive(Debug, PartialEq)]
pub enum JobRetry {
    /// The job stays in the queue, attempt counter bumped.
    Rescheduled,
    /// The job was moved to `failed/` after exhausting its attempts.
    Parked,
}

/// A digest made safe for a file name: `sha1:BASE32…` → `sha1_BASE32…`.
/// `None` when it could escape the cache directory.
fn file_slug(digest: &str) -> Option<String> {
    if digest.is_empty() || digest.contains("..") {
        return None;
    }
    let ok = digest.bytes().all(|b| {
        b.is_ascii_alphanumeric() || matches!(b, b':' | b'-' | b'+' | b'_' | b'.')
    });
    if !ok {
        return None;
    }
    Some(digest.replace(':', "_"))
}

/// The two-character shard directory for a digest: the first two characters
/// of the value part, so no directory accumulates every entry.
fn shard_dir(digest: &str) -> String {
    let value = digest.rsplit_once(':').map(|(_, v)| v).unwrap_or(digest);
    let mut shard: String = value.chars().take(2).collect();
    shard.make_ascii_lowercase();
    if shard.is_empty() {
        shard.push('0');
    }
    shard
}

fn invalid_digest(digest: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("digest {digest:?} cannot be used as a cache key"),
    )
}

/// Write `bytes` to `path` atomically: a temp file, then a rename. A reader
/// either sees the old entry or the new one, never a half-written one.
fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let tmp = path.with_file_name(format!(
        "{name}.tmp-{}-{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed),
    ));
    if let Err(e) = fs::write(&tmp, bytes) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    if let Err(e) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    Ok(())
}

// ── The sidecar format ────────────────────────────────────────────────────────

/// Render a cached text: one header line binding it to its source, then the
/// text. Identical shape to the `store_text` sidecar, the digest standing in
/// for the ETag.
fn render_cached_text(digest: &str, title: &str, text: &str) -> String {
    format!(
        "{MAGIC} source-digest={digest} title={}\n{text}",
        title.replace(['\n', '\r'], " "),
    )
}

/// Read a cached text back, if it belongs to this exact digest.
///
/// `None` when the header is missing or names a different digest — the entry
/// describes other bytes. Same lenient field parsing as the `store_text`
/// sidecar: digests and titles contain no spaces by construction.
fn parse_cached_text(body: &str, want_digest: &str) -> Option<(String, String)> {
    let (header, text) = body.split_once('\n')?;
    let header = header.strip_prefix(MAGIC)?;

    let stored = header
        .split(" source-digest=")
        .nth(1)?
        .split(" title=")
        .next()?;
    if stored != want_digest {
        return None;
    }
    let title = header
        .split(" title=")
        .nth(1)
        .map(|t| t.trim().to_owned())
        .unwrap_or_default();
    Some((title, text.to_owned()))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn cache() -> (tempfile::TempDir, OcrCache) {
        let dir = tempfile::tempdir().unwrap();
        let c = OcrCache::open(dir.path()).unwrap();
        (dir, c)
    }

    fn job(digest: &str) -> OcrJob {
        OcrJob {
            digest: digest.to_owned(),
            bucket: "warc".to_owned(),
            s3_key: "crawls/2026/x.warc.gz".to_owned(),
            offset: 4096,
            c_offset: Some(2048),
            length: 123_456,
            url: "https://example.com/band30.pdf".to_owned(),
            attempts: 0,
        }
    }

    // ── the text store ─────────────────────────────────────────────────────

    #[test]
    fn a_cached_text_round_trips() {
        let (_dir, c) = cache();
        c.put("sha1:ABC", "Band 30", "Seite eins\u{000c}Seite zwei").unwrap();
        let (title, text) = c.get("sha1:ABC").unwrap();
        assert_eq!(title, "Band 30");
        assert_eq!(text, "Seite eins\u{000c}Seite zwei", "page markers must survive");
    }

    #[test]
    fn a_miss_is_a_miss() {
        let (_dir, c) = cache();
        assert!(c.get("sha1:NOTTHERE").is_none());
        assert!(!c.has("sha1:NOTTHERE"));
    }

    #[test]
    fn a_negative_entry_is_a_hit_with_empty_text() {
        // "Extraction was tried; the file gave no text." The caller tells this
        // apart from a miss — which is the difference between never queueing
        // the document again and queueing it every run.
        let (_dir, c) = cache();
        c.put("sha1:LEER", "", "").unwrap();
        assert_eq!(c.get("sha1:LEER"), Some((String::new(), String::new())));
        assert!(c.has("sha1:LEER"));
    }

    #[test]
    fn the_digest_binds_the_text() {
        let (_dir, c) = cache();
        c.put("sha1:ALT", "T", "alter Text").unwrap();
        assert!(c.get("sha1:NEU").is_none(), "other bytes, other text");
    }

    #[test]
    fn an_older_format_is_refused() {
        // Version 1 held Tika's plain text without page markers. The version
        // in the header is what makes such files re-extract.
        let (_dir, c) = cache();
        let v1 = "tywb-text/1 source-digest=sha1:X title=T\nohne Seitenmarken";
        let path = c.text_path("sha1:X").unwrap();
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, v1).unwrap();
        assert!(c.get("sha1:X").is_none());
    }

    #[test]
    fn junk_is_a_miss_not_a_crash() {
        let (_dir, c) = cache();
        for body in ["", "no header at all", "\n", "tywb-text/2 nothing=here\ntext"] {
            assert!(parse_cached_text(body, "sha1:X").is_none());
        }
    }

    // ── file names from digests ─────────────────────────────────────────────

    #[test]
    fn a_digest_becomes_a_safe_file_name() {
        assert_eq!(file_slug("sha1:ABC123"), Some("sha1_ABC123".to_owned()));
        assert_eq!(file_slug(""), None);
        assert_eq!(file_slug("../../etc/passwd"), None);
        assert_eq!(file_slug("sha1:with space"), None, "a space cannot be a path");
        assert_eq!(file_slug("sha1:a/b"), None);
        assert_eq!(file_slug("sha256:XYZ"), Some("sha256_XYZ".to_owned()));
    }

    #[test]
    fn the_shard_comes_from_the_value_part() {
        assert_eq!(shard_dir("sha1:ABC123"), "ab");
        assert_eq!(shard_dir("sha256:XYZ"), "xy");
        assert_eq!(shard_dir("lonely"), "lo");
        assert!(!shard_dir("x").is_empty(), "a one-char value still gets a dir");
    }

    #[test]
    fn texts_are_sharded_not_piled_up() {
        let (_dir, c) = cache();
        c.put("sha1:AAA", "T", "eins").unwrap();
        c.put("sha1:BBB", "T", "zwei").unwrap();
        let p1 = c.text_path("sha1:AAA").unwrap();
        let p2 = c.text_path("sha1:BBB").unwrap();
        assert_ne!(p1.parent(), p2.parent());
        assert!(p1.ends_with("sha1_AAA.tywb.txt"));
    }

    // ── the queue ────────────────────────────────────────────────────────────

    #[test]
    fn a_job_round_trips_and_is_deduplicated_by_digest() {
        let (_dir, c) = cache();
        c.enqueue(&job("sha1:DUP")).unwrap();
        let mut again = job("sha1:DUP");
        again.s3_key = "crawls/2026/y.warc.gz".to_owned(); // a second crawl of the same PDF
        c.enqueue(&again).unwrap();

        let jobs = c.queued_jobs();
        assert_eq!(jobs.len(), 1, "one document, one job — whatever record it came from");
        let parsed = &jobs[0].1;
        assert_eq!(parsed.digest, "sha1:DUP");
        assert_eq!(parsed.s3_key, again.s3_key, "the latest location wins");
        assert_eq!(parsed.c_offset, Some(2048));
        assert_eq!(parsed.length, 123_456);
    }

    #[test]
    fn a_finished_job_is_removed_and_an_empty_queue_stays_empty() {
        let (_dir, c) = cache();
        c.enqueue(&job("sha1:X")).unwrap();
        let (path, _) = &c.queued_jobs()[0];
        c.remove_job(path);
        assert!(c.queued_jobs().is_empty());
        // Removing twice is fine — the job may already be gone.
        c.remove_job(path);
    }

    #[test]
    fn an_unparseable_job_is_parked_not_dropped() {
        let (_dir, c) = cache();
        let path = c.queue_path("sha1:BROKEN").unwrap();
        fs::write(&path, "{ not json").unwrap();
        assert!(c.queued_jobs().is_empty());
        let parked = c.root.join(FAILED_DIR).join("sha1_BROKEN.job");
        assert!(parked.exists(), "the entry stays visible instead of vanishing");
    }

    #[test]
    fn a_failed_job_is_rescheduled_then_parked() {
        let (_dir, c) = cache();
        c.enqueue(&job("sha1:WACKELN")).unwrap();
        let (path, parsed) = c.queued_jobs()[0].clone();
        assert_eq!(parsed.attempts, 0);

        assert_eq!(c.retry(&path, &parsed, "tika down", 3), JobRetry::Rescheduled);
        let (path, parsed) = c.queued_jobs()[0].clone();
        assert_eq!(parsed.attempts, 1, "the counter survives the round trip");

        assert_eq!(c.retry(&path, &parsed, "tika down", 3), JobRetry::Rescheduled);
        let (path, parsed) = c.queued_jobs()[0].clone();
        assert_eq!(parsed.attempts, 2);

        // Third failure: parked in failed/, visible rather than looping.
        assert_eq!(c.retry(&path, &parsed, "tika down", 3), JobRetry::Parked);
        assert!(c.queued_jobs().is_empty());
        assert!(c.root.join(FAILED_DIR).join("sha1_WACKELN.job").exists());
    }

    #[test]
    fn a_parked_job_is_overwritten_by_its_own_next_incarnation() {
        // Same digest, same fate: the parked file may already exist.
        let (_dir, c) = cache();
        c.enqueue(&job("sha1:ZWEIMAL")).unwrap();
        let (path, parsed) = c.queued_jobs()[0].clone();
        c.retry(&path, &parsed, "x", 1);
        assert!(c.root.join(FAILED_DIR).join("sha1_ZWEIMAL.job").exists());

        // The digest comes back through the queue, fails again.
        c.enqueue(&job("sha1:ZWEIMAL")).unwrap();
        let (path, parsed) = c.queued_jobs()[0].clone();
        c.retry(&path, &parsed, "x", 1);
        assert!(c.queued_jobs().is_empty());
        assert!(c.root.join(FAILED_DIR).join("sha1_ZWEIMAL.job").exists());
    }

    #[test]
    fn nothing_but_jobs_is_listed() {
        let (_dir, c) = cache();
        c.enqueue(&job("sha1:ONLY")).unwrap();
        // The temp files atomic_write leaves behind on failure look like this.
        let dir = c.root.join(QUEUE_DIR);
        fs::write(dir.join("sha1_ONLY.job.tmp-1-2"), b"half written").unwrap();
        fs::write(dir.join("readme.txt"), b"not ours").unwrap();
        assert_eq!(c.queued_jobs().len(), 1);
    }
}