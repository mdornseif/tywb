//! `tywb recompress` — rewrite whole-file-gzip WARCs as record-per-member `.warc.gz`.
//!
//! # Why
//!
//! A `.warc.gz` is supposed to store every WARC record in its own gzip member,
//! so a reader can Range-GET one member and decompress just that record — the
//! operation the whole CDX/replay path is built on (see [`crate::gz_warc`]).
//!
//! Some producers instead gzip the entire WARC as a single deflate stream.
//! Such a file decompresses fine, but it has no record-level random access at
//! all: the indexer sees one member, reads the first record out of it, and the
//! remaining hundreds of records are never indexed.  Replay of that one record
//! then fails too, because a Range GET of `record length + slack` bytes lands
//! in the middle of a continuous deflate stream (`unexpected end of file`).
//!
//! # What this does
//!
//! For every affected object:
//!
//! 1. download it,
//! 2. skip it if it already has more than one gzip member,
//! 3. parse it record by record and write each record as its own gzip member,
//! 4. **verify** — a fresh full decompression of the original must be
//!    byte-identical to the concatenated members of the rewrite, and the member
//!    count must equal the record count,
//! 5. server-side copy `<key>` → `<key>.bak`,
//! 6. upload the rewrite over `<key>`.
//!
//! Nothing on S3 is touched before step 4 passes, and the backup is verified
//! before the original is overwritten.  Objects that fail any check are left
//! exactly as they were and reported at the end.
//!
//! Re-running is safe: anything that is already record-per-member is skipped, so
//! a rewritten object is never touched twice — and an object whose rewrite failed
//! half-way is retried rather than skipped because a stale `.bak` exists.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tracing::{info, warn};

use warc_search_config::Config;
use warc_search_s3::{
    build_client, copy_object, get_range, get_stream, head_object, put_object_from_path,
    Lister,
};

/// Bytes fetched to decide single-member vs. record-per-member.
const CLASSIFY_WINDOW: u64 = 1 << 20;
/// Objects classified concurrently (Garage gets unhappy above this).
const CLASSIFY_CONCURRENCY: usize = 4;
/// Attempts per classification before an object is queued as unknown.
const CLASSIFY_ATTEMPTS: usize = 3;
/// Attempts per object download.
const DOWNLOAD_ATTEMPTS: usize = 3;
/// Streaming chunk size for the record rewriter.
const CHUNK: usize = 64 * 1024;
/// Gzip level for the rewritten members.
const GZIP_LEVEL: u32 = 6;

// ── CLI arguments ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RecompressArgs {
    /// Explicit S3 keys to process; when empty the bucket is scanned.
    pub files: Vec<String>,
    /// Process at most this many objects (smallest first).
    pub limit: Option<usize>,
    /// Objects processed concurrently.
    pub jobs: usize,
    /// Scratch directory for downloads and rewrites.
    pub workdir: PathBuf,
    /// Verify and report, but never write to S3.
    pub dry_run: bool,
    /// Only list what would be rewritten, then stop.
    pub scan_only: bool,
    /// Suffix for the preserved original.
    pub backup_suffix: String,
}

// ── Outcome ───────────────────────────────────────────────────────────────────

#[derive(Debug)]
enum Outcome {
    Rewritten { records: u64, old_size: u64, new_size: u64 },
    WouldRewrite { records: u64, old_size: u64, new_size: u64 },
    Skipped(String),
    Failed(String),
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn run(cfg: Config, args: RecompressArgs) -> Result<()> {
    let client = Arc::new(build_client(&cfg.s3).await);
    let bucket = cfg.s3.bucket.clone();

    std::fs::create_dir_all(&args.workdir)
        .with_context(|| format!("creating workdir {}", args.workdir.display()))?;

    let targets = collect_targets(&client, &bucket, &args).await?;

    let total: u64 = targets.iter().map(|(_, s)| *s).sum();
    info!(
        objects = targets.len(),
        gigabytes = format!("{:.2}", total as f64 / 1e9),
        "objects to rewrite",
    );
    for (key, size) in &targets {
        info!(size, key = %key, "target");
    }
    if args.scan_only || targets.is_empty() {
        return Ok(());
    }

    let sem = Arc::new(Semaphore::new(args.jobs.max(1)));
    let mut tasks = Vec::new();
    for (key, size) in targets {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let bucket = bucket.clone();
        let args = args.clone();
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            let outcome = match process_one(&client, &bucket, &key, size, &args).await {
                Ok(o) => o,
                Err(e) => Outcome::Failed(format!("{e:#}")),
            };
            (key, outcome)
        }));
    }

    let mut rewritten = 0usize;
    let mut skipped = Vec::new();
    let mut failed = Vec::new();
    let mut bytes_before = 0u64;
    let mut bytes_after = 0u64;
    let mut records_total = 0u64;

    for t in tasks {
        let (key, outcome) = t.await?;
        match outcome {
            Outcome::Rewritten { records, old_size, new_size }
            | Outcome::WouldRewrite { records, old_size, new_size } => {
                rewritten += 1;
                records_total += records;
                bytes_before += old_size;
                bytes_after += new_size;
            }
            Outcome::Skipped(reason) => skipped.push((key, reason)),
            Outcome::Failed(reason) => failed.push((key, reason)),
        }
    }

    info!(
        rewritten,
        records = records_total,
        skipped = skipped.len(),
        failed = failed.len(),
        megabytes_before = format!("{:.1}", bytes_before as f64 / 1e6),
        megabytes_after = format!("{:.1}", bytes_after as f64 / 1e6),
        "recompress finished",
    );
    for (key, reason) in &skipped {
        info!(key = %key, reason = %reason, "skipped");
    }
    for (key, reason) in &failed {
        warn!(key = %key, reason = %reason, "FAILED — object left untouched");
    }
    if !failed.is_empty() {
        bail!("{} object(s) failed", failed.len());
    }
    Ok(())
}

/// Build the work list: explicit keys, or every whole-file-gzip WARC in the bucket.
async fn collect_targets(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    args: &RecompressArgs,
) -> Result<Vec<(String, u64)>> {
    let lister = {
        let l = Lister::new(client, bucket.to_owned());
        match std::env::var("WARC_S3_PREFIX") {
            Ok(p) if !p.is_empty() => l.with_prefix(p),
            _ => l,
        }
    };
    let objects = lister.list_all().await.context("listing bucket")?;

    let sizes: std::collections::HashMap<&str, u64> =
        objects.iter().map(|o| (o.key.as_str(), o.size)).collect();

    if !args.files.is_empty() {
        return Ok(args
            .files
            .iter()
            .map(|k| {
                let size = sizes.get(k.as_str()).copied().unwrap_or(0);
                (k.clone(), size)
            })
            .collect());
    }

    let candidates: Vec<(String, u64)> = objects
        .iter()
        .filter(|o| {
            o.size > 0
                && o.key.to_ascii_lowercase().contains(".warc")
                && !o.key.ends_with(&args.backup_suffix)
        })
        .map(|o| (o.key.clone(), o.size))
        .collect();

    info!(count = candidates.len(), "classifying WARC objects");

    let sem = Arc::new(Semaphore::new(CLASSIFY_CONCURRENCY));
    let mut checks = Vec::new();
    for (key, size) in candidates {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let client = client.clone();
        let bucket = bucket.to_owned();
        checks.push(tokio::spawn(async move {
            let _permit = permit;
            let verdict = classify(&client, &bucket, &key, size).await;
            (key, size, verdict)
        }));
    }

    let mut targets = Vec::new();
    for c in checks {
        let (key, size, verdict) = c.await?;
        match verdict {
            Verdict::WholeFile => targets.push((key, size)),
            Verdict::PerRecord => {}
            Verdict::NotGzip => warn!(key = %key, "not a readable gzip — skipping"),
            // Never assume an object we could not read is fine: queue it and let
            // the local member count decide (a healthy file is then skipped
            // without touching S3).
            Verdict::Unknown => {
                warn!(key = %key, "classification failed — queued, will be decided locally");
                targets.push((key, size));
            }
        }
    }

    targets.sort_by_key(|(_, size)| *size);                    // smallest first
    if let Some(limit) = args.limit {
        targets.truncate(limit);
    }
    Ok(targets)
}

/// What the leading bytes of an object say about its gzip framing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// One gzip member per record — nothing to do.
    PerRecord,
    /// A single member running past the classification window.
    WholeFile,
    /// Not a gzip stream at all.
    NotGzip,
    /// Could not be read after retries.
    Unknown,
}

/// Fetch the first [`CLASSIFY_WINDOW`] bytes and judge the gzip framing.
///
/// Garage occasionally drops a body mid-stream under concurrent range GETs, so
/// transient failures are retried before giving up.
async fn classify(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    size: u64,
) -> Verdict {
    let window = CLASSIFY_WINDOW.min(size);
    for attempt in 1..=CLASSIFY_ATTEMPTS {
        match get_range(client, bucket, key, 0, window).await {
            Ok(bytes) => {
                return match first_member_ends_within(&bytes) {
                    Some(true) => Verdict::PerRecord,
                    Some(false) => Verdict::WholeFile,
                    None => Verdict::NotGzip,
                }
            }
            Err(e) => {
                warn!(key, attempt, err = %e, "classification GET failed");
                tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64)).await;
            }
        }
    }
    Verdict::Unknown
}

/// Download, rewrite, verify and (unless `dry_run`) replace one object.
async fn process_one(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    size: u64,
    args: &RecompressArgs,
) -> Result<Outcome> {
    let stem: String = key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '.' || c == '-' { c } else { '_' })
        .collect();
    let orig = args.workdir.join(format!("{stem}.orig"));
    let new = args.workdir.join(format!("{stem}.new"));
    let _cleanup = FileCleanup(vec![orig.clone(), new.clone()]);

    info!(key, megabytes = format!("{:.1}", size as f64 / 1e6), "downloading");
    let mut got = 0u64;
    for attempt in 1..=DOWNLOAD_ATTEMPTS {
        match download(client, bucket, key, &orig).await {
            Ok(()) => {
                got = std::fs::metadata(&orig)?.len();
                if size == 0 || got == size {
                    break;
                }
                warn!(key, attempt, got, expected = size, "short download");
            }
            Err(e) => warn!(key, attempt, err = %format!("{e:#}"), "download failed"),
        }
        if attempt == DOWNLOAD_ATTEMPTS {
            bail!("download failed after {DOWNLOAD_ATTEMPTS} attempts");
        }
        tokio::time::sleep(std::time::Duration::from_secs(attempt as u64)).await;
    }

    // ── everything below is CPU/disk bound ──
    let orig_c = orig.clone();
    let new_c = new.clone();
    let key_c = key.to_owned();
    let outcome = tokio::task::spawn_blocking(move || -> Result<Outcome> {
        let members = count_members(&orig_c)?;
        if members != 1 {
            return Ok(Outcome::Skipped(format!("already {members} gzip members")));
        }
        let (_, payload_len) = sha256_gunzip(&orig_c)?;
        if payload_len == 0 {
            return Ok(Outcome::Skipped("empty after decompression".to_owned()));
        }

        info!(key = %key_c, "rewriting record-per-member");
        let records = rewrite_records(&orig_c, &new_c)
            .with_context(|| format!("rewriting {key_c}"))?;

        // Verification, independent of the record parser: the concatenated
        // payload of both files must be bit-identical.
        let (old_hash, old_len) = sha256_gunzip(&orig_c)?;
        let (new_hash, new_len) = sha256_gunzip(&new_c)?;
        if old_hash != new_hash {
            bail!("payload mismatch after rewrite ({old_len} vs {new_len} bytes)");
        }
        let new_members = count_members(&new_c)?;
        if new_members as u64 != records {
            bail!("member count {new_members} != record count {records}");
        }
        Ok(Outcome::Rewritten { records, old_size: got, new_size: std::fs::metadata(&new_c)?.len() })
    })
    .await??;

    let (records, old_size, new_size) = match outcome {
        Outcome::Rewritten { records, old_size, new_size } => (records, old_size, new_size),
        other => return Ok(other),                 // skipped
    };
    info!(
        key, records,
        megabytes_before = format!("{:.1}", old_size as f64 / 1e6),
        megabytes_after = format!("{:.1}", new_size as f64 / 1e6),
        "verified",
    );

    if args.dry_run {
        return Ok(Outcome::WouldRewrite { records, old_size, new_size });
    }

    // ── replace on S3: back up first, verify, only then overwrite ──
    let backup = format!("{key}{}", args.backup_suffix);
    info!(key, backup, "server-side copy to backup");
    copy_object(client, bucket, key, &backup).await?;
    let head = head_object(client, bucket, &backup).await?;
    if head.size != old_size {
        bail!("backup {backup} is {} bytes, expected {old_size} — not overwriting", head.size);
    }
    // Only the gzip framing changes, so the object keeps whatever content type
    // it was uploaded with; inventing a new one here would be a silent
    // metadata change for every other consumer of the bucket.
    let content_type = head.content_type.as_deref().unwrap_or("application/octet-stream");

    info!(key, content_type, "uploading rewritten object");
    put_object_from_path(client, bucket, key, &new, content_type).await?;
    let head = head_object(client, bucket, key).await?;
    if head.size != new_size {
        bail!("upload of {key} is {} bytes, expected {new_size} — original preserved at {backup}",
              head.size);
    }

    info!(key, records, "done");
    Ok(Outcome::Rewritten { records, old_size, new_size })
}

/// Stream an S3 object to a local file.
async fn download(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    dest: &Path,
) -> Result<()> {
    let stream = get_stream(client, bucket, key).await?;
    let mut pinned = Box::pin(stream);
    let mut file = tokio::io::BufWriter::new(tokio::fs::File::create(dest).await?);
    while let Some(chunk) = pinned.next().await {
        let bytes = chunk.map_err(|e| anyhow::anyhow!("S3 stream error on {key}: {e}"))?;
        file.write_all(&bytes).await?;
    }
    file.flush().await?;
    Ok(())
}

/// Delete scratch files when the pipeline leaves scope, however it leaves.
struct FileCleanup(Vec<PathBuf>);

impl Drop for FileCleanup {
    fn drop(&mut self) {
        for p in &self.0 {
            let _ = std::fs::remove_file(p);
        }
    }
}

// ── gzip helpers ──────────────────────────────────────────────────────────────

/// `Some(true)` if the first gzip member ends inside `data` (record-per-member),
/// `Some(false)` if it runs past the window (whole-file gzip), `None` if the
/// bytes are not a readable gzip stream at all.
fn first_member_ends_within(data: &[u8]) -> Option<bool> {
    if data.len() < 2 || data[0] != 0x1f || data[1] != 0x8b {
        return None;
    }
    let mut cursor = io::Cursor::new(data);
    let mut dec = flate2::bufread::GzDecoder::new(&mut cursor);
    match io::copy(&mut dec, &mut io::sink()) {
        Ok(_) => Some(true),                                          // member ended in the window
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Some(false), // still going
        Err(_) => None,                                               // corrupt
    }
}

/// Count gzip members in a local file without materialising the payload.
fn count_members(path: &Path) -> Result<usize> {
    let file = std::fs::File::open(path)?;
    let mut reader = BufReader::with_capacity(CHUNK, file);
    let mut members = 0usize;
    loop {
        if reader.fill_buf()?.is_empty() {
            break;
        }
        // `bufread::GzDecoder` stops at the end of one member and leaves the
        // rest of the stream in the BufReader for the next round.
        let mut dec = flate2::bufread::GzDecoder::new(&mut reader);
        io::copy(&mut dec, &mut io::sink())?;
        members += 1;
    }
    Ok(members)
}

/// SHA-256 and length of the fully decompressed content of a gzip file
/// (all members concatenated).
fn sha256_gunzip(path: &Path) -> Result<(String, u64)> {
    let file = std::fs::File::open(path)?;
    let mut dec = flate2::read::MultiGzDecoder::new(BufReader::with_capacity(CHUNK, file));
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; CHUNK];
    let mut total = 0u64;
    loop {
        let n = dec.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((format!("{:x}", hasher.finalize()), total))
}

// ── the rewriter ──────────────────────────────────────────────────────────────

/// Read the WARC in `src` (gzip) and write it to `dst` with one gzip member per
/// record.  Record bytes are copied verbatim — only the gzip framing changes.
///
/// Returns the number of records written.  Any structural surprise is an error,
/// so a file we do not fully understand is never rewritten.
fn rewrite_records(src: &Path, dst: &Path) -> Result<u64> {
    let file = std::fs::File::open(src)?;
    let mut src = BufReader::with_capacity(CHUNK, flate2::read::MultiGzDecoder::new(
        BufReader::with_capacity(CHUNK, file),
    ));
    let mut out = io::BufWriter::new(std::fs::File::create(dst)?);

    let mut records = 0u64;
    let mut line = Vec::with_capacity(256);

    loop {
        line.clear();
        if src.read_until(b'\n', &mut line)? == 0 {
            break;                                   // clean EOF between records
        }
        if line == b"\r\n" || line == b"\n" {
            continue;                                // stray separator — tolerate
        }
        if !line.starts_with(b"WARC/") {
            bail!("expected WARC version line, got {:?}",
                  String::from_utf8_lossy(&line[..line.len().min(40)]));
        }

        let mut header = line.clone();
        let mut content_length: Option<u64> = None;
        loop {
            line.clear();
            if src.read_until(b'\n', &mut line)? == 0 {
                bail!("EOF inside record header");
            }
            header.extend_from_slice(&line);
            if line == b"\r\n" || line == b"\n" {
                break;                               // end of header block
            }
            if let Some(colon) = line.iter().position(|&b| b == b':') {
                let name = &line[..colon];
                if name.eq_ignore_ascii_case(b"content-length") {
                    let value = String::from_utf8_lossy(&line[colon + 1..]);
                    content_length = value.trim().parse::<u64>().ok();
                }
            }
        }
        let mut remaining = content_length.context("record without Content-Length")?;

        let mut enc = GzEncoder::new(&mut out, Compression::new(GZIP_LEVEL));
        enc.write_all(&header)?;

        let mut buf = vec![0u8; CHUNK];
        while remaining > 0 {
            let want = remaining.min(CHUNK as u64) as usize;
            src.read_exact(&mut buf[..want])
                .map_err(|e| anyhow::anyhow!("EOF inside record block: {e}"))?;
            enc.write_all(&buf[..want])?;
            remaining -= want as u64;
        }

        let mut trailer = [0u8; 4];
        src.read_exact(&mut trailer)
            .map_err(|e| anyhow::anyhow!("EOF at record trailer: {e}"))?;
        if &trailer != b"\r\n\r\n" {
            bail!("expected CRLFCRLF record trailer, got {trailer:?}");
        }
        enc.write_all(&trailer)?;
        enc.finish()?;
        records += 1;
    }

    out.flush()?;
    if records == 0 {
        bail!("no records parsed");
    }
    Ok(records)
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn record(typ: &str, uri: &str, body: &[u8]) -> Vec<u8> {
        let mut r = format!(
            "WARC/1.1\r\nWARC-Type: {typ}\r\nWARC-Target-URI: {uri}\r\n\
             WARC-Date: 2026-01-01T00:00:00Z\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        r.extend_from_slice(body);
        r.extend_from_slice(b"\r\n\r\n");
        r
    }

    /// A WARC with a warcinfo and three responses, one of them large enough to
    /// span several streaming chunks.
    fn sample_warc() -> Vec<u8> {
        // Incompressible, so the gzip fixture is larger than any test window.
        let mut state: u64 = 0x2545F4914F6CDD1D;
        let big: Vec<u8> = (0..300_000u32)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect();
        let mut w = Vec::new();
        w.extend(record("warcinfo", "-", b"software: test\r\n"));
        w.extend(record("response", "http://a/", b"HTTP/1.1 200 OK\r\n\r\n<html>a</html>"));
        w.extend(record("response", "http://b/", &big));
        w.extend(record("response", "http://c/", b"HTTP/1.1 404 NF\r\n\r\nnope"));
        w
    }

    fn write_whole_file_gzip(path: &Path, payload: &[u8]) {
        let mut enc = GzEncoder::new(std::fs::File::create(path).unwrap(), Compression::new(6));
        enc.write_all(payload).unwrap();
        enc.finish().unwrap();
    }

    #[test]
    fn rewrites_whole_file_gzip_into_one_member_per_record() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("in.warc.gz");
        let dst = dir.path().join("out.warc.gz");
        let payload = sample_warc();
        write_whole_file_gzip(&src, &payload);

        assert_eq!(count_members(&src).unwrap(), 1, "fixture must be single-member");

        let records = rewrite_records(&src, &dst).unwrap();
        assert_eq!(records, 4);
        assert_eq!(count_members(&dst).unwrap(), 4, "one member per record");

        // Payload must survive bit-for-bit.
        let (h_src, n_src) = sha256_gunzip(&src).unwrap();
        let (h_dst, n_dst) = sha256_gunzip(&dst).unwrap();
        assert_eq!(h_src, h_dst);
        assert_eq!(n_src, payload.len() as u64);
        assert_eq!(n_dst, payload.len() as u64);
    }

    #[test]
    fn each_member_holds_exactly_one_record() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("in.warc.gz");
        let dst = dir.path().join("out.warc.gz");
        write_whole_file_gzip(&src, &sample_warc());
        rewrite_records(&src, &dst).unwrap();

        // Walk members the way the indexer does and check each one parses as a
        // single record whose length matches its Content-Length.
        let bytes = std::fs::read(&dst).unwrap();
        let mut splitter = crate::gz_warc::GzSplitter::new(io::Cursor::new(bytes));
        let mut seen = 0;
        while let Some((offset, member)) = splitter.next_member().unwrap() {
            assert!(member.starts_with(b"WARC/1.1"), "member at {offset} is not a record start");
            let text = String::from_utf8_lossy(&member);
            assert_eq!(text.matches("WARC/1.1\r\n").count(), 1, "member holds >1 record");
            seen += 1;
        }
        assert_eq!(seen, 4);
    }

    #[test]
    fn already_record_per_member_files_are_detected() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("in.warc.gz");
        let dst = dir.path().join("out.warc.gz");
        write_whole_file_gzip(&src, &sample_warc());
        rewrite_records(&src, &dst).unwrap();

        let whole = std::fs::read(&src).unwrap();
        let split = std::fs::read(&dst).unwrap();
        assert!(whole.len() > 4096, "fixture must exceed the test window");
        // Whole-file gzip: the single member runs past any window we look at.
        assert_eq!(first_member_ends_within(&whole[..4096]), Some(false));
        // Record-per-member: the first (warcinfo) member ends inside it.
        assert_eq!(first_member_ends_within(&split[..4096]), Some(true));
        assert_eq!(first_member_ends_within(b"not gzip at all"), None);
    }

    #[test]
    fn truncated_record_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("in.warc.gz");
        let dst = dir.path().join("out.warc.gz");
        let mut payload = sample_warc();
        payload.truncate(payload.len() - 100_000);     // cut into the large record's block
        write_whole_file_gzip(&src, &payload);

        let err = rewrite_records(&src, &dst).unwrap_err().to_string();
        assert!(err.contains("EOF inside record block"), "unexpected error: {err}");
    }

    #[test]
    fn garbage_input_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("in.warc.gz");
        let dst = dir.path().join("out.warc.gz");
        write_whole_file_gzip(&src, b"this is not a WARC file at all\r\n\r\n");

        let err = rewrite_records(&src, &dst).unwrap_err().to_string();
        assert!(err.contains("expected WARC version line"), "unexpected error: {err}");
    }
}

