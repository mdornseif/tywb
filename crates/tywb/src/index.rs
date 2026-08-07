//! `tywb index` — batch ingest worker.
//!
//! Lists WARC files from S3, streams them through the WARC parser, writes CDX
//! entries to SQLite, and adds extracted text to the Tantivy fulltext index.
//! Saves progress after each file so a crash on a large bucket is safe to resume.

use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use anyhow::Context;
use bytes::Bytes;
use serde_json;
use tracing::{debug, info, warn, error};

use warc::{WarcIter, WarcReader};
use warc_search_cdx::{CdxRecord, CdxStore, WarcFileMeta, WarcInfoRecord, from_warc_record};
use crate::gz_warc::GzSplitter;
use warc_search_config::{Config, IndexerConfig};
use warc_search_s3::{
    build_client, ListState, Lister, ObjectMeta, S3Error,
    default_state_path, get_stream, head_object, put_object,
};
use warc_search_search::{SearchIndex, IndexDoc};

// ── CLI arguments ─────────────────────────────────────────────────────────────

/// Arguments forwarded from the `index` subcommand.
pub struct IndexArgs {
    /// Only index this specific S3 key (bypasses S3 listing).
    pub file: Option<String>,
    /// Stop after processing this many WARC files.
    pub max_files: Option<usize>,
    /// Stop after accumulating at least this many new CDX entries.
    pub max_urls: Option<u64>,
    /// Re-process all objects regardless of saved ETag state.
    pub force: bool,
    /// Index only the extra collections, skipping the primary WARC bucket.
    pub collections_only: bool,
}

// ── Statistics ────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
struct FileStats {
    warc_records:     usize,
    cdx_new:          usize,
    cdx_known:        usize,
    indexed:          usize,
    skipped:          usize,
    errors:           usize,
    bytes_processed:  u64,
    duration_secs:    f64,
    warc_date_min:    Option<String>,
    warc_date_max:    Option<String>,
    /// MIME type → record count.
    mime_counts:      HashMap<String, u64>,
}

impl std::ops::AddAssign<&FileStats> for FileStats {
    fn add_assign(&mut self, o: &FileStats) {
        self.warc_records    += o.warc_records;
        self.cdx_new         += o.cdx_new;
        self.cdx_known       += o.cdx_known;
        self.indexed         += o.indexed;
        self.skipped         += o.skipped;
        self.errors          += o.errors;
        self.bytes_processed += o.bytes_processed;
    }
}

// ── Shared progress state (async ↔ spawn_blocking bridge) ────────────────────

struct SharedProgress {
    /// S3 key currently being indexed.
    current_key: RwLock<String>,
    /// URL of the most-recently-seen WARC response record.
    current_url: RwLock<String>,
    /// Uncompressed bytes read so far in the current file.
    bytes_read:  AtomicU64,
    /// WARC records processed so far in the current file.
    warc_records: AtomicUsize,
    /// CDX entries found so far in the current file.
    cdx_found:   AtomicUsize,
    /// Milliseconds at which the current file started (from `run_start`).
    file_start_ms: AtomicU64,
    /// Wall-clock start of the whole `index` run.
    run_start:   Instant,
    /// Number of files finished so far.
    files_done:  AtomicUsize,
    /// Total files to process (set once before the loop).
    files_total: AtomicUsize,
}

impl SharedProgress {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            current_key:  RwLock::new(String::new()),
            current_url:  RwLock::new(String::new()),
            bytes_read:   AtomicU64::new(0),
            warc_records: AtomicUsize::new(0),
            cdx_found:    AtomicUsize::new(0),
            file_start_ms: AtomicU64::new(0),
            run_start:    Instant::now(),
            files_done:   AtomicUsize::new(0),
            files_total:  AtomicUsize::new(0),
        })
    }

    fn reset_for_file(&self, key: &str) {
        *self.current_key.write().unwrap()  = key.to_owned();
        *self.current_url.write().unwrap()  = String::new();
        self.bytes_read.store(0, Ordering::Relaxed);
        self.warc_records.store(0, Ordering::Relaxed);
        self.cdx_found.store(0, Ordering::Relaxed);
        let elapsed_ms = self.run_start.elapsed().as_millis() as u64;
        self.file_start_ms.store(elapsed_ms, Ordering::Relaxed);
    }

    fn print_status(&self) {
        let key  = self.current_key.read().unwrap().clone();
        let url  = self.current_url.read().unwrap().clone();
        let recs = self.warc_records.load(Ordering::Relaxed);
        let cdx  = self.cdx_found.load(Ordering::Relaxed);
        let mb   = self.bytes_read.load(Ordering::Relaxed) as f64 / 1_048_576.0;

        let elapsed_ms  = self.run_start.elapsed().as_millis() as u64;
        let file_start  = self.file_start_ms.load(Ordering::Relaxed);
        let file_secs   = (elapsed_ms.saturating_sub(file_start)) as f64 / 1000.0;
        let rec_per_sec = if file_secs > 0.0 { recs as f64 / file_secs } else { 0.0 };
        let mb_per_sec  = if file_secs > 0.0 { mb / file_secs } else { 0.0 };

        let done  = self.files_done.load(Ordering::Relaxed);
        let total = self.files_total.load(Ordering::Relaxed);

        eprintln!(
            "[status] file {done}/{total} key={key} recs={recs} cdx={cdx} \
             {mb:.1}MB {rec_per_sec:.0}rec/s {mb_per_sec:.2}MB/s url={url}",
        );
    }
}

// ── Domain blacklist helpers ──────────────────────────────────────────────────

/// Convert a plain domain name to its SURT host form (reversed labels, no `)`).
/// `"example.com"` → `"com,example"`.  Used as a prefix for CDX lookups.
fn domain_to_surt_host(domain: &str) -> String {
    domain
        .trim()
        .to_ascii_lowercase()
        .split('.')
        .rev()
        .collect::<Vec<_>>()
        .join(",")
}

/// Remove all entries for `domain` (apex + subdomains) from both the CDX SQLite
/// store and the Tantivy fulltext index, then commit the fulltext changes.
fn purge_domain(
    domain: &str,
    cdx: &CdxStore,
    search: &mut SearchIndex,
) -> anyhow::Result<()> {
    let surt_host = domain_to_surt_host(domain);

    // Collect all original URLs before touching CDX, because we need them to
    // drive the Tantivy deletion.
    let urls = cdx
        .original_urls_for_domain_surt(&surt_host)
        .with_context(|| format!("fetching URLs for domain {domain}"))?;

    let cdx_deleted = cdx
        .delete_by_domain_surt(&surt_host)
        .with_context(|| format!("deleting CDX records for domain {domain}"))?;

    let search_queued = search
        .delete_urls(&urls)
        .with_context(|| format!("queuing search deletions for domain {domain}"))?;

    if cdx_deleted > 0 || search_queued > 0 {
        info!(
            domain,
            cdx_deleted,
            search_queued,
            "purged blacklisted domain"
        );
    }

    Ok(())
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn run(cfg: Config, args: IndexArgs) -> anyhow::Result<()> {
    info!("tywb index starting");

    // ── Storage ───────────────────────────────────────────────────────────────

    let mut cdx = CdxStore::open(&cfg.storage.cdx_db_path)
        .with_context(|| format!("opening CDX store at {}", cfg.storage.cdx_db_path))?;
    cdx.set_cache_kib(cfg.storage.sqlite_cache_kib)
        .context("setting SQLite cache size")?;

    let mut search = SearchIndex::open_or_create(&cfg.storage.index_path)
        .with_context(|| format!("opening search index at {}", cfg.storage.index_path))?;

    // ── Purge blacklisted domains ─────────────────────────────────────────────
    // Runs before any new ingest so blacklisted data is cleaned up even when
    // there are no new WARC files to process.

    if !cfg.indexer.blacklisted_domains.is_empty() {
        let domains: Vec<String> = cfg.indexer.blacklisted_domains.clone();
        for domain in &domains {
            purge_domain(domain, &cdx, &mut search)?;
        }
        // Commit fulltext deletions in one pass.
        search.commit().context("committing fulltext deletions for blacklisted domains")?;
    }

    // ── S3 ────────────────────────────────────────────────────────────────────

    let s3 = build_client(&cfg.s3).await;

    let state_path = default_state_path(&cfg.storage.cdx_db_path);
    let mut state = ListState::load(&state_path)
        .unwrap_or_else(|e| {
            warn!(path = %state_path.display(), err = %e, "could not load list state, starting fresh");
            ListState::default()
        });

    // Build list of objects to process
    let objects: Vec<ObjectMeta> = if args.collections_only {
        info!("--collections-only: skipping the primary WARC bucket");
        Vec::new()
    } else if let Some(ref key) = args.file {
        // Single-file mode: synthesise a fake ObjectMeta so the loop is uniform.
        info!(key = %key, "single-file mode");
        vec![ObjectMeta { key: key.clone(), size: 0, etag: None, last_modified: None }]
    } else {
        let lister = {
            let mut l = Lister::new(&s3, &cfg.s3.bucket);
            if let Some(pfx) = &cfg.s3.prefix {
                l = l.with_prefix(pfx);
            }
            l
        };
        if args.force {
            info!("--force: ignoring saved ETag state, all objects will be re-processed");
            lister.list_all().await.context("listing S3 objects")?
        } else {
            lister.list_new_or_changed(&state).await.context("listing S3 objects")?
        }
    };

    // Collections are indexed after the WARC loop (when not in --file mode), so
    // an empty WARC list is only "nothing to do" if there are no collections.
    let has_collections = args.file.is_none() && !cfg.indexer.collections.is_empty();
    if objects.is_empty() && !has_collections {
        info!("nothing to index — all objects are up to date");
        return Ok(());
    }

    // Apply --max-files cap before we start (so the progress counter is right).
    let objects: Vec<ObjectMeta> = match args.max_files {
        Some(n) => objects.into_iter().take(n).collect(),
        None    => objects,
    };

    info!(count = objects.len(), "objects to index");

    // ── Shared progress + SIGINFO ─────────────────────────────────────────────

    let progress = SharedProgress::new();
    progress.files_total.store(objects.len(), Ordering::Relaxed);

    install_status_handler(Arc::clone(&progress));

    // ── Ingest loop ───────────────────────────────────────────────────────────

    let mut totals = FileStats::default();
    let mut total_cdx_new: u64 = 0;

    for obj in &objects {
        if !obj.is_warc() {
            info!(key = %obj.key, "skipping non-WARC object");
            state.mark_seen(&obj.key, obj.etag.clone());
            continue;
        }

        info!(key = %obj.key, size = obj.size, "indexing WARC object");
        progress.reset_for_file(&obj.key);

        let file_start = Instant::now();

        match index_warc_object(
            &s3, &cfg.s3.bucket, &obj.key,
            &mut cdx, &mut search,
            &cfg.indexer,
            Arc::clone(&progress),
        ).await {
            Ok(mut stats) => {
                stats.duration_secs = file_start.elapsed().as_secs_f64();
                let rec_per_sec = if stats.duration_secs > 0.0 {
                    stats.warc_records as f64 / stats.duration_secs
                } else { 0.0 };
                let mb_per_sec = if stats.duration_secs > 0.0 {
                    stats.bytes_processed as f64 / 1_048_576.0 / stats.duration_secs
                } else { 0.0 };

                info!(
                    key          = %obj.key,
                    warc_records = stats.warc_records,
                    cdx_new      = stats.cdx_new,
                    cdx_known    = stats.cdx_known,
                    indexed      = stats.indexed,
                    skipped      = stats.skipped,
                    errors       = stats.errors,
                    mb_processed = format!("{:.1}", stats.bytes_processed as f64 / 1_048_576.0),
                    duration_s   = format!("{:.1}", stats.duration_secs),
                    rec_per_sec  = format!("{:.0}", rec_per_sec),
                    mb_per_sec   = format!("{:.2}", mb_per_sec),
                    "object indexed",
                );

                // Record per-file metadata in SQLite.
                let mime_json = serde_json::to_string(&stats.mime_counts).unwrap_or_default();
                let now_iso = utc_now_iso();
                let meta = WarcFileMeta {
                    s3_key:           obj.key.clone(),
                    etag:             obj.etag.clone(),
                    size_bytes:       obj.size,
                    indexed_at:       now_iso,
                    bucket:           Some(cfg.s3.bucket.clone()),
                    warc_records:     stats.warc_records,
                    cdx_new:          stats.cdx_new,
                    cdx_known:        stats.cdx_known,
                    fulltext_indexed: stats.indexed,
                    skipped:          stats.skipped,
                    errors:           stats.errors,
                    duration_secs:    stats.duration_secs,
                    bytes_per_sec:    mb_per_sec * 1_048_576.0,
                    records_per_sec:  rec_per_sec,
                    warc_date_min:    stats.warc_date_min.clone(),
                    warc_date_max:    stats.warc_date_max.clone(),
                    mime_summary:     if mime_json == "{}" { None } else { Some(mime_json) },
                };
                if let Err(e) = cdx.upsert_warc_file(&meta) {
                    warn!(key = %obj.key, err = %e, "could not write warc_files metadata");
                }

                total_cdx_new += stats.cdx_new as u64;
                totals += &stats;
                state.mark_seen(&obj.key, obj.etag.clone());
            }
            Err(e) => {
                error!(key = %obj.key, err = %e, "failed to index object — skipping");
            }
        }

        progress.files_done.fetch_add(1, Ordering::Relaxed);

        if let Err(e) = state.save(&state_path) {
            warn!(err = %e, "could not save list state");
        }

        // --max-urls check
        if let Some(max_u) = args.max_urls {
            if total_cdx_new >= max_u {
                info!(total_cdx_new, max_urls = max_u, "reached --max-urls limit, stopping");
                break;
            }
        }
    }

    // ── Extra collections (PDF buckets, …) ────────────────────────────────────
    // Skipped in single-file mode (--file targets one WARC key). Each collection
    // shares the primary S3 client, credentials and ETag state.
    if args.file.is_none() {
        for coll in &cfg.indexer.collections {
            match crate::pdf_collection::index_pdf_collection(
                coll, &s3, &mut cdx, &mut search, &cfg.indexer, &mut state,
            ).await {
                Ok(cs) => {
                    totals.cdx_new += cs.cdx_new;
                    totals.indexed += cs.indexed;
                    totals.skipped += cs.skipped;
                    totals.errors  += cs.errors;
                }
                Err(e) => error!(name = %coll.name, err = %format!("{e:#}"), "collection indexing failed"),
            }
            if let Err(e) = state.save(&state_path) {
                warn!(err = %e, "could not save list state after collection");
            }
        }
    }

    search.commit().context("final search index commit")?;

    let total_secs = progress.run_start.elapsed().as_secs_f64();
    let total_rec_per_sec = if total_secs > 0.0 { totals.warc_records as f64 / total_secs } else { 0.0 };
    let total_mb_per_sec  = if total_secs > 0.0 { totals.bytes_processed as f64 / 1_048_576.0 / total_secs } else { 0.0 };

    info!(
        objects_processed = progress.files_done.load(Ordering::Relaxed),
        warc_records      = totals.warc_records,
        cdx_new           = totals.cdx_new,
        cdx_known         = totals.cdx_known,
        indexed           = totals.indexed,
        skipped           = totals.skipped,
        errors            = totals.errors,
        mb_processed      = format!("{:.1}", totals.bytes_processed as f64 / 1_048_576.0),
        duration_s        = format!("{:.1}", total_secs),
        rec_per_sec       = format!("{:.0}", total_rec_per_sec),
        mb_per_sec        = format!("{:.2}", total_mb_per_sec),
        "run complete",
    );

    Ok(())
}

// ── Per-object indexing ───────────────────────────────────────────────────────

struct ParsedObject {
    records:      Vec<(CdxRecord, Option<IndexDoc>)>,
    warc_records: usize,
    skipped:      usize,
    errors:       usize,
    bytes_read:   u64,
    warc_date_min: Option<String>,
    warc_date_max: Option<String>,
    mime_counts:  HashMap<String, u64>,
    /// The first `warcinfo` record encountered in the file, if any.
    warcinfo:     Option<WarcInfoRecord>,
}

async fn index_warc_object(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    cdx: &mut CdxStore,
    search: &mut SearchIndex,
    cfg: &IndexerConfig,
    progress: Arc<SharedProgress>,
) -> anyhow::Result<FileStats> {
    let stream = get_stream(s3, bucket, key)
        .await
        .with_context(|| format!("S3 GET {key}"))?;

    let is_gz = key.to_ascii_lowercase().ends_with(".gz");
    let key_owned = key.to_owned();
    let bucket_owned = bucket.to_owned();
    let max_text = cfg.max_text_bytes;
    let index_responses = cfg.index_warc_responses;
    let cfg_owned = cfg.clone();

    let (tx, rx) = mpsc::sync_channel::<Bytes>(16);

    tokio::spawn(async move {
        let mut pinned = Box::pin(stream);
        while let Some(chunk) = pinned.next().await {
            match chunk {
                Ok(b)  => { if tx.send(b).is_err() { break; } }
                Err(e) => { warn!(err = %e, "S3 stream error"); break; }
            }
        }
    });

    let prog_clone = Arc::clone(&progress);

    let parsed: ParsedObject =
        tokio::task::spawn_blocking(move || -> anyhow::Result<ParsedObject> {
            let chan = CountingReader::new(ChannelReader::new(rx), prog_clone);

            // Build the PDF extractor once for this object. Present only when a
            // Tika backend is configured and PDF indexing is enabled; otherwise
            // PDFs are skipped from fulltext exactly as before.
            let pdf_extractor = if cfg_owned.index_pdfs {
                cfg_owned.tika.as_ref().map(crate::pdf::PdfExtractor::new)
            } else {
                None
            };

            let mut out                  = Vec::new();
            let mut warc_records         = 0usize;
            let mut skipped              = 0usize;
            let mut errors               = 0usize;
            let mut warc_date_min: Option<String> = None;
            let mut warc_date_max: Option<String> = None;
            let mut mime_counts: HashMap<String, u64> = HashMap::new();
            let mut warcinfo_record: Option<WarcInfoRecord> = None;

            const PROGRESS_EVERY: usize = 100;

            /// Inner helper: process one parsed WarcRecord, optionally with a
            /// known compressed offset (`c_offset`).  Updates `out`, counters, and
            /// shared progress state.
            #[allow(clippy::too_many_arguments)]
            fn handle_record(
                record:      &warc::WarcRecord,
                c_offset:    Option<u64>,
                key:         &str,
                index_resp:  bool,
                max_text:    usize,
                blacklist:   &warc_search_config::IndexerConfig,
                out:         &mut Vec<(CdxRecord, Option<warc_search_search::IndexDoc>)>,
                skipped:     &mut usize,
                errors:      &mut usize,
                mime_counts: &mut HashMap<String, u64>,
                warc_date_min: &mut Option<String>,
                warc_date_max: &mut Option<String>,
                progress:    &SharedProgress,
                warc_records_total: usize,
                pdf:         Option<&crate::pdf::PdfExtractor>,
            ) {
                // Track WARC-Date range.
                if let Some(d) = record.header.get("WARC-Date") {
                    let d = d.to_owned();
                    if warc_date_min.as_deref().map_or(true, |m| d.as_str() < m) {
                        *warc_date_min = Some(d.clone());
                    }
                    if warc_date_max.as_deref().map_or(true, |m| d.as_str() > m) {
                        *warc_date_max = Some(d);
                    }
                }

                // Always keep atomics current so Ctrl-T / SIGINFO is accurate.
                progress.warc_records.store(warc_records_total, Ordering::Relaxed);
                progress.cdx_found.store(out.len(), Ordering::Relaxed);

                // Periodic progress log.
                if warc_records_total % PROGRESS_EVERY == 0 {
                    let bytes_read = progress.bytes_read.load(Ordering::Relaxed);
                    let elapsed_ms = progress.run_start.elapsed().as_millis() as u64;
                    let file_start = progress.file_start_ms.load(Ordering::Relaxed);
                    let file_secs  = (elapsed_ms.saturating_sub(file_start)) as f64 / 1000.0;
                    let rec_per_sec = if file_secs > 0.0 { warc_records_total as f64 / file_secs } else { 0.0 };
                    let mb_per_sec  = if file_secs > 0.0 { bytes_read as f64 / 1_048_576.0 / file_secs } else { 0.0 };
                    info!(
                        key          = %key,
                        warc_records = warc_records_total,
                        cdx_found    = out.len(),
                        skipped      = *skipped,
                        errors       = *errors,
                        rec_per_sec  = format!("{rec_per_sec:.0}"),
                        mb_per_sec   = format!("{mb_per_sec:.2}"),
                        "indexing…",
                    );
                }

                if !index_resp {
                    *skipped += 1;
                    return;
                }

                match from_warc_record(record, key) {
                    Err(e) => {
                        warn!(key, err = %e, "CDX extraction failed — skipping record");
                        *errors += 1;
                    }
                    Ok(None) => { *skipped += 1; }
                    Ok(Some(mut cdx_rec)) => {
                        // Skip blacklisted domains.
                        if blacklist.is_url_blacklisted(&cdx_rec.original_url) {
                            tracing::debug!(
                                url = %cdx_rec.original_url,
                                "skipping blacklisted URL"
                            );
                            *skipped += 1;
                            return;
                        }

                        cdx_rec.c_offset = c_offset;

                        let mime_key = cdx_rec.mime
                            .as_deref()
                            .map(|m| m.split(';').next().unwrap_or(m).trim())
                            .unwrap_or("(none)")
                            .to_owned();
                        *mime_counts.entry(mime_key).or_insert(0) += 1;

                        *progress.current_url.write().unwrap() =
                            cdx_rec.original_url.clone();

                        tracing::debug!(
                            url    = %cdx_rec.original_url,
                            mime   = cdx_rec.mime.as_deref().unwrap_or("-"),
                            status = ?cdx_rec.status,
                            c_offset,
                            "response record",
                        );
                        let doc = build_index_doc(record, &cdx_rec, max_text, pdf);
                        out.push((cdx_rec, doc));
                    }
                }
            }

            /// Build a `WarcInfoRecord` from a parsed warcinfo WARC record.
            fn make_warcinfo(record: &warc::WarcRecord, s3_key: &str, bucket: &str) -> WarcInfoRecord {
                let headers_json = serde_json::to_string(
                    &record.header.iter()
                        .map(|(k, v)| [k, v])
                        .collect::<Vec<_>>(),
                ).ok();
                let block_text = std::str::from_utf8(&record.block)
                    .map(|s| s.to_owned())
                    .unwrap_or_else(|_| String::from_utf8_lossy(&record.block).into_owned());
                WarcInfoRecord {
                    s3_key:        s3_key.to_owned(),
                    bucket:        Some(bucket.to_owned()),
                    warc_date:     record.header.get("warc-date").map(str::to_owned),
                    warc_filename: record.header.get("warc-filename").map(str::to_owned),
                    record_id:     record.header.get("warc-record-id").map(str::to_owned),
                    headers_json,
                    block_text:    Some(block_text),
                }
            }

            if is_gz {
                // ── .warc.gz path: one gzip member per WARC record ───────────
                // GzSplitter reads one member at a time and reports the
                // compressed byte offset of each member.  We store that as
                // `c_offset` in the CDX record so replay can do a targeted
                // S3 Range GET without streaming from the beginning of the file.
                let mut gz = GzSplitter::new(chan);

                loop {
                    let (c_offset, decompressed) = match gz.next_member() {
                        Ok(None) => break,
                        Ok(Some(pair)) => pair,
                        Err(e) => {
                            warn!(
                                key  = %key_owned,
                                err  = %e,
                                "GzSplitter error — aborting remainder of object",
                            );
                            errors += 1;
                            break;
                        }
                    };

                    warc_records += 1;

                    let mut rdr = WarcReader::new(std::io::Cursor::new(decompressed));
                    let record = match rdr.next_record() {
                        Ok(None) => { skipped += 1; continue; }
                        Ok(Some(r)) => r,
                        Err(e) => {
                            warn!(key = %key_owned, c_offset, err = %e, "WARC parse error — skipping member");
                            errors += 1;
                            continue;
                        }
                    };

                    if warcinfo_record.is_none() {
                        if let Ok(warc::RecordType::Warcinfo) = record.header.record_type() {
                            warcinfo_record = Some(make_warcinfo(&record, &key_owned, &bucket_owned));
                        }
                    }

                    handle_record(
                        &record, Some(c_offset), &key_owned,
                        index_responses, max_text, &cfg_owned,
                        &mut out, &mut skipped, &mut errors,
                        &mut mime_counts, &mut warc_date_min, &mut warc_date_max,
                        &progress, warc_records, pdf_extractor.as_ref(),
                    );
                }
            } else {
                // ── .warc path: plain stream, record.offset is the file offset
                for result in WarcIter::new(chan) {
                    match result {
                        Err(e) => {
                            warn!(
                                key = %key_owned,
                                err = %e,
                                "WARC parse error — aborting remainder of object",
                            );
                            errors += 1;
                            break;
                        }
                        Ok(record) => {
                            warc_records += 1;
                            if warcinfo_record.is_none() {
                                if let Ok(warc::RecordType::Warcinfo) = record.header.record_type() {
                                    warcinfo_record = Some(make_warcinfo(&record, &key_owned, &bucket_owned));
                                }
                            }
                            handle_record(
                                &record, None, &key_owned,
                                index_responses, max_text, &cfg_owned,
                                &mut out, &mut skipped, &mut errors,
                                &mut mime_counts, &mut warc_date_min, &mut warc_date_max,
                                &progress, warc_records, pdf_extractor.as_ref(),
                            );
                        }
                    }
                }
            }

            Ok(ParsedObject {
                records: out,
                warc_records,
                skipped,
                errors,
                bytes_read: progress.bytes_read.load(Ordering::Relaxed),
                warc_date_min,
                warc_date_max,
                mime_counts,
                warcinfo: warcinfo_record,
            })
        })
        .await
        .context("spawn_blocking panicked")?
        .context("WARC parsing failed")?;

    // Store warcinfo record if found.
    if let Some(wi) = &parsed.warcinfo {
        if let Err(e) = cdx.upsert_warcinfo(wi) {
            warn!(key = %key, err = %e, "could not write warcinfo record");
        }
    }

    let cdx_records: Vec<CdxRecord> = parsed.records.iter().map(|(r, _)| r.clone()).collect();
    let (cdx_new, cdx_known) = cdx
        .upsert_batch_counted(&cdx_records)
        .context("CDX upsert_batch")?;

    // Spawn a background task to write a CDX sidecar file into the bucket
    // alongside the WARC, skipping if the file already exists.
    {
        let s3_clone  = s3.clone();
        let bucket    = bucket.to_owned();
        let key       = key.to_owned();
        let recs      = cdx_records.clone();
        tokio::spawn(async move {
            write_cdx_sidecar(&s3_clone, &bucket, &key, &recs).await;
        });
    }

    // Re-index must *replace* a file's contribution, not duplicate it:
    // add_document below appends unconditionally, so without this a second pass
    // over the same object (--force, or a --file backfill) would add a second
    // copy of every already-indexed record. delete_term only affects
    // already-committed docs (lower opstamp); the adds that follow in this same
    // commit survive. On a first index this matches nothing and is a no-op.
    search.delete_s3_key(key).context("search delete_s3_key")?;

    let mut indexed = 0usize;
    for (_, doc_opt) in &parsed.records {
        if let Some(doc) = doc_opt {
            search.add_document(doc).context("search add_document")?;
            indexed += 1;
        }
    }
    search.commit().context("search commit")?;

    Ok(FileStats {
        warc_records:    parsed.warc_records,
        cdx_new,
        cdx_known,
        indexed,
        skipped:         parsed.skipped,
        errors:          parsed.errors,
        bytes_processed: parsed.bytes_read,
        duration_secs:   0.0, // filled in by caller
        warc_date_min:   parsed.warc_date_min,
        warc_date_max:   parsed.warc_date_max,
        mime_counts:     parsed.mime_counts,
    })
}

// ── Text extraction ───────────────────────────────────────────────────────────

/// Truncate `s` to at most `max` bytes, backing up to the nearest UTF-8 char
/// boundary so we never slice through a multi-byte character. Binary or
/// mis-decoded content (e.g. a non-UTF-8 blob lossily read as text) otherwise
/// makes a naive `s[..max]` panic and skip the whole WARC object.
fn truncate_on_char_boundary(mut s: String, max: usize) -> String {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
    s
}

fn build_index_doc(
    record: &warc::WarcRecord,
    cdx: &CdxRecord,
    max_bytes: usize,
    pdf: Option<&crate::pdf::PdfExtractor>,
) -> Option<IndexDoc> {
    // Revisit records have no payload of their own — the content is already
    // indexed via the record they refer to, so they get a CDX entry (capture
    // history) but no duplicate fulltext doc.
    if matches!(record.header.record_type(), Ok(warc::RecordType::Revisit)) {
        return None;
    }

    // Skip non-HTTP URIs (urn:, data:, etc.) — internal crawler bookkeeping,
    // not real pages.
    if !cdx.original_url.starts_with("http://") && !cdx.original_url.starts_with("https://") {
        return None;
    }

    let mime = cdx.mime.as_deref().unwrap_or("");
    let is_html = mime.starts_with("text/html")
        || mime.starts_with("application/xhtml")
        || mime.starts_with("text/xml")   // some sites serve HTML as text/xml
        || mime.starts_with("application/xml");
    let is_text = mime.starts_with("text/plain");
    let is_pdf  = mime.starts_with("application/pdf");
    if !is_html && !is_text && !is_pdf {
        return None;
    }

    let ts: u64 = cdx.timestamp.parse().unwrap_or(0);

    // A WARC stores the response as it came off the wire, so the bytes after
    // the HTTP headers are not the content yet: chunk framing on the outside,
    // Content-Encoding within. Peel both before anything reads them — an
    // unpeeled body indexes as U+FFFD noise, and Tika cannot parse a PDF that
    // still has chunk headers in it.
    let parts = crate::http_payload::parse_http_block(record.block.as_ref());
    // HTML and text are truncated anyway, so stop decoding at that point; a PDF
    // must arrive whole (the xref table a parser needs lives at the end).
    let decode_limit = if is_pdf { crate::http_payload::MAX_DECODED_BYTES } else { max_bytes * 4 };
    let payload = match parts.payload(decode_limit) {
        Ok(p) => p,
        Err(e) => {
            debug!(url = %cdx.original_url, err = %e, "undecodable body — not indexed");
            return None;
        }
    };

    // ── PDF: hand the whole payload to Tika (never truncate — the xref table a
    // parser needs lives at the end of the file). Requires a configured backend.
    if is_pdf {
        let extractor = pdf?;
        let doc = extractor.extract(&cdx.original_url, &payload)?;
        let body_text = truncate_on_char_boundary(doc.body, max_bytes);
        return Some(IndexDoc {
            url:       cdx.original_url.clone(),
            timestamp: ts,
            title:     doc.title,
            body:      body_text,
            mime:      cdx.mime.clone(),
            s3_key:    cdx.s3_key.clone(),
            offset:    cdx.offset,
            length:    cdx.length,
            collection: cdx.collection.clone(),
        });
    }

    let truncated = if payload.len() > max_bytes * 4 { &payload[..max_bytes * 4] } else { &payload };
    let text = String::from_utf8_lossy(truncated);

    let (title, body_text) = if is_html {
        let t = extract_title(&text);
        let b = strip_html(&text);
        (t, b)
    } else {
        // text/plain: use first line as title, rest as body
        let mut lines = text.splitn(2, '\n');
        let first = lines.next().unwrap_or("").trim().chars().take(256).collect();
        let rest  = lines.next().unwrap_or("").to_owned();
        (first, rest)
    };
    let body_text = truncate_on_char_boundary(body_text, max_bytes);

    Some(IndexDoc {
        url:       cdx.original_url.clone(),
        timestamp: ts,
        title,
        body:      body_text,
        mime:      cdx.mime.clone(),
        s3_key:    cdx.s3_key.clone(),
        offset:    cdx.offset,
        length:    cdx.length,
        collection: cdx.collection.clone(),
    })
}

/// The document title from `<title>…</title>`, entity-decoded and collapsed.
pub(crate) fn extract_title(html: &str) -> String {
    let lower = html.as_bytes();
    let Some(open_tag_start) = memmem(lower, b"<title") else { return String::new(); };
    let Some(rel_gt) = lower[open_tag_start..].iter().position(|&b| b == b'>') else { return String::new(); };
    let content_start = open_tag_start + rel_gt + 1;
    let Some(close_start) = memmem(&lower[content_start..], b"</title") else { return String::new(); };
    let raw = &html[content_start..content_start + close_start];
    collapse_ws(&decode_entities(raw)).chars().take(512).collect()
}

/// Elements whose content is code, not prose — dropped wholesale.
const RAW_TEXT_ELEMENTS: [&str; 3] = ["script", "style", "noscript"];

/// Convert an HTML document to plain text.
///
/// Markup is removed, the content of [`RAW_TEXT_ELEMENTS`] is dropped, comments
/// are skipped, character references are decoded, and whitespace is collapsed
/// to single spaces. Every tag boundary becomes a space, so `a<br>b` reads as
/// two words rather than one.
///
/// This is deliberately a scanner, not a DOM parser: it runs over every record
/// in a multi-GB WARC and must never allocate a tree per document.
pub(crate) fn strip_html(html: &str) -> String {
    let b = html.as_bytes();
    let mut out = String::with_capacity(html.len());
    let mut text_start = 0usize;
    let mut i = 0usize;

    // `<` and `>` are ASCII, so every index computed here is a char boundary.
    while let Some(lt) = find_at(b, i, b"<") {
        out.push_str(&html[text_start..lt]);

        i = if b[lt..].starts_with(b"<!--") {
            find_at(b, lt + 4, b"-->").map(|p| p + 3).unwrap_or(b.len())
        } else if let Some(elem) = raw_text_element(&html[lt..]) {
            // Skip past the whole element, content and closing tag included.
            match find_at(b, lt + 1, format!("</{elem}").as_bytes()) {
                Some(close) => find_at(b, close, b">").map(|p| p + 1).unwrap_or(b.len()),
                None        => b.len(),
            }
        } else {
            find_at(b, lt + 1, b">").map(|p| p + 1).unwrap_or(b.len())
        };

        out.push(' ');
        text_start = i;
    }
    out.push_str(&html[text_start..]);

    collapse_ws(&decode_entities(&out))
}

/// If the tag at the start of `s` opens a [raw-text element], its lowercase
/// name; otherwise `None`.
///
/// [raw-text element]: RAW_TEXT_ELEMENTS
/// Compared on bytes, never on `&str` slices: a mis-decoded page is full of
/// `U+FFFD`, and `&s[..6]` lands inside one of those three-byte characters and
/// panics. Byte comparison has no boundary to hit.
fn raw_text_element(s: &str) -> Option<&'static str> {
    let after_lt = s.strip_prefix('<')?.as_bytes();
    RAW_TEXT_ELEMENTS.iter().copied().find(|name| {
        let name = name.as_bytes();
        after_lt.len() > name.len()
            && after_lt[..name.len()].eq_ignore_ascii_case(name)
            // `<script>`/`<script src=…>` — but not `<scriptural>`.
            && matches!(after_lt[name.len()], b'>' | b'/' | b' ' | b'\t' | b'\r' | b'\n')
    })
}

/// Decode the character references that matter for readable text: the five
/// predefined XML entities, `&nbsp;`, and numeric references. Anything else is
/// passed through verbatim — a bare `&` in the text stays a bare `&`.
fn decode_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let tail = &rest[amp..];
        // A reference is short; scanning further means it was never one.
        let end = tail[1..].find(';').map(|p| p + 2).filter(|&e| e <= 12);
        match end.and_then(|e| decode_one_entity(&tail[1..e - 1]).map(|c| (c, e))) {
            Some((c, e)) => { out.push(c); rest = &tail[e..]; }
            None         => { out.push('&'); rest = &tail[1..]; }
        }
    }
    out.push_str(rest);
    out
}

/// Decode the body of one character reference (`amp`, `#233`, `#xE9`).
fn decode_one_entity(body: &str) -> Option<char> {
    match body {
        "amp"                  => return Some('&'),
        "lt"                   => return Some('<'),
        "gt"                   => return Some('>'),
        "quot"                 => return Some('"'),
        "apos" | "#39"         => return Some('\''),
        "nbsp"                 => return Some('\u{a0}'),
        _ => {}
    }
    let num = body.strip_prefix('#')?;
    let code = match num.strip_prefix(['x', 'X']) {
        Some(hex) => u32::from_str_radix(hex, 16).ok()?,
        None      => num.parse::<u32>().ok()?,
    };
    char::from_u32(code)
}

/// Collapse every run of whitespace to a single space and trim the ends.
fn collapse_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = true;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_space { out.push(' '); }
            prev_space = true;
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out.truncate(out.trim_end().len());
    out
}

fn memmem(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() { return Some(0); }
    haystack.windows(needle.len()).position(|w| {
        w.iter().zip(needle).all(|(&h, &n)| h.to_ascii_lowercase() == n)
    })
}

/// `memmem` from byte offset `start`, returning an absolute position.
fn find_at(haystack: &[u8], start: usize, needle: &[u8]) -> Option<usize> {
    if start >= haystack.len() { return None; }
    memmem(&haystack[start..], needle).map(|p| p + start)
}

// ── SIGINFO / SIGUSR1 handler ─────────────────────────────────────────────────

fn install_status_handler(progress: Arc<SharedProgress>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        // SIGINFO (29) on macOS/BSDs — Ctrl+T in the terminal.
        // SIGUSR1 (10) on Linux — there is no SIGINFO.
        #[cfg(target_os = "macos")]
        let kind = SignalKind::info();
        #[cfg(not(target_os = "macos"))]
        let kind = SignalKind::user_defined1();

        tokio::spawn(async move {
            let mut stream = match signal(kind) {
                Ok(s)  => s,
                Err(e) => { warn!(err = %e, "could not install status signal handler"); return; }
            };
            loop {
                stream.recv().await;
                progress.print_status();
            }
        });
    }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

/// Current UTC time as an ISO8601 string (seconds precision).
fn utc_now_iso() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    // Format as YYYY-MM-DDTHH:MM:SSZ without pulling in chrono.
    let s = secs;
    let (y, mo, d, h, mi, sec) = epoch_to_ymd_hms(s);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{sec:02}Z")
}

fn epoch_to_ymd_hms(mut s: u64) -> (u32, u32, u32, u32, u32, u32) {
    let sec  = (s % 60) as u32; s /= 60;
    let min  = (s % 60) as u32; s /= 60;
    let hour = (s % 24) as u32; s /= 24;
    // Days since 1970-01-01
    let mut days = s as u32;
    let mut year = 1970u32;
    loop {
        let dy = days_in_year(year);
        if days < dy { break; }
        days -= dy;
        year += 1;
    }
    let mut month = 1u32;
    loop {
        let dm = days_in_month(year, month);
        if days < dm { break; }
        days -= dm;
        month += 1;
    }
    (year, month, days + 1, hour, min, sec)
}

fn days_in_year(y: u32) -> u32 {
    if y % 400 == 0 || (y % 4 == 0 && y % 100 != 0) { 366 } else { 365 }
}

fn days_in_month(y: u32, m: u32) -> u32 {
    match m {
        1|3|5|7|8|10|12 => 31,
        4|6|9|11        => 30,
        2 => if days_in_year(y) == 366 { 29 } else { 28 },
        _ => 30,
    }
}

// ── CDX sidecar writer ────────────────────────────────────────────────────────

/// Write a CDX-11 sidecar file to S3 alongside `warc_key` unless one already
/// exists.  The sidecar key is `{warc_key}.cdx`.
///
/// CDX-11 format (space-separated, one record per line):
/// ```
///  CDX N b a m s k r M S V g
/// ```
/// Fields: SURT, timestamp, original URL, MIME, HTTP status, digest, redirect
/// (-), meta (-), record length, byte offset, WARC filename (basename).
///
/// For `.warc.gz` files the offset field (`V`) is the compressed gzip-member
/// offset (`c_offset`).  For plain `.warc` files it is the uncompressed stream
/// offset.  Records without `c_offset` in a `.warc.gz` fall back to the
/// uncompressed offset and are still included.
///
/// Errors are logged as warnings; they never abort the indexing run.
async fn write_cdx_sidecar(
    s3:      &aws_sdk_s3::Client,
    bucket:  &str,
    warc_key: &str,
    records: &[CdxRecord],
) {
    let cdx_key = format!("{warc_key}.cdx");

    // Skip if the sidecar already exists.
    match head_object(s3, bucket, &cdx_key).await {
        Ok(_) => {
            info!(key = %cdx_key, "CDX sidecar already exists — skipping");
            return;
        }
        Err(S3Error::NotFound { .. }) => {} // expected — proceed to write
        Err(e) => {
            warn!(key = %cdx_key, err = %e, "HEAD failed for CDX sidecar — skipping write");
            return;
        }
    }

    if records.is_empty() {
        info!(key = %cdx_key, "no CDX records — skipping empty sidecar");
        return;
    }

    let basename = warc_key.rsplit('/').next().unwrap_or(warc_key);
    let is_gz    = warc_key.to_ascii_lowercase().ends_with(".gz");

    // Build CDX-11 content.
    let mut body = String::with_capacity(records.len() * 200);
    body.push_str(" CDX N b a m s k r M S V g\n");

    for r in records {
        let mime   = r.mime.as_deref().unwrap_or("-");
        let status = r.status.map(|s| s.to_string()).unwrap_or_else(|| "-".to_owned());
        let digest = r.digest.as_deref().unwrap_or("-");
        let length = r.length;
        let offset = if is_gz {
            r.c_offset.unwrap_or(r.offset)
        } else {
            r.offset
        };

        body.push_str(&r.surt_url);
        body.push(' ');
        body.push_str(&r.timestamp);
        body.push(' ');
        body.push_str(&r.original_url);
        body.push(' ');
        body.push_str(mime);
        body.push(' ');
        body.push_str(&status);
        body.push(' ');
        body.push_str(digest);
        body.push_str(" - -");
        body.push(' ');
        body.push_str(&length.to_string());
        body.push(' ');
        body.push_str(&offset.to_string());
        body.push(' ');
        body.push_str(basename);
        body.push('\n');
    }

    let bytes = bytes::Bytes::from(body.into_bytes());
    match put_object(s3, bucket, &cdx_key, bytes, "text/plain; charset=utf-8").await {
        Ok(()) => info!(
            key    = %cdx_key,
            bucket = %bucket,
            "CDX sidecar written",
        ),
        Err(e) => warn!(
            key    = %cdx_key,
            bucket = %bucket,
            err    = %e,
            "failed to write CDX sidecar",
        ),
    }
}

// ── Async → sync I/O bridge ───────────────────────────────────────────────────

struct ChannelReader {
    rx:  Receiver<Bytes>,
    buf: Bytes,
}

impl ChannelReader {
    fn new(rx: Receiver<Bytes>) -> Self {
        Self { rx, buf: Bytes::new() }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.buf.is_empty() {
            match self.rx.recv() {
                Ok(chunk) => self.buf = chunk,
                Err(_)    => return Ok(0),
            }
        }
        let n = out.len().min(self.buf.len());
        out[..n].copy_from_slice(&self.buf[..n]);
        self.buf = self.buf.slice(n..);
        Ok(n)
    }
}

/// Wraps any `Read` and counts bytes through an atomic so the async
/// SIGINFO handler can read the running total without locking.
struct CountingReader<R: Read> {
    inner:    R,
    progress: Arc<SharedProgress>,
}

impl<R: Read> CountingReader<R> {
    fn new(inner: R, progress: Arc<SharedProgress>) -> Self {
        Self { inner, progress }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.progress.bytes_read.fetch_add(n as u64, Ordering::Relaxed);
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::{build_index_doc, decode_entities, extract_title, strip_html,
                truncate_on_char_boundary};
    use std::io::Write as _;

    /// A `response` record holding `block`, parsed back the way ingest sees it.
    fn response_record(url: &str, block: &[u8]) -> warc::WarcRecord {
        let raw = warc::reader::build_warc_record(
            "WARC/1.0",
            &[
                ("WARC-Type", "response"),
                ("WARC-Target-URI", url),
                ("WARC-Date", "2026-07-20T20:09:29Z"),
                ("Content-Type", "application/http; msgtype=response"),
            ],
            block,
        );
        warc::WarcReader::new(raw.as_slice()).next_record().unwrap().unwrap()
    }

    /// The wire format that made it into the archive: chunk framing wrapped
    /// around a gzip stream, with the headers describing only the gzip.
    #[test]
    fn index_doc_reads_a_chunked_gzip_capture() {
        let html = b"<html><head><title>Example Domain</title></head>\
                     <body><p>Roter Berlepsch &amp; Boskoop</p></body></html>";
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(html).unwrap();
        let gz = gz.finish().unwrap();

        let mut block = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\
                          Content-Encoding: gzip\r\n\r\n".to_vec();
        block.extend_from_slice(format!("{:x}\r\n", gz.len()).as_bytes());
        block.extend_from_slice(&gz);
        block.extend_from_slice(b"\r\n0\r\n\r\n");

        let record = response_record("https://example.com/", &block);
        let cdx = warc_search_cdx::from_warc_record(&record, "crawl.warc.gz")
            .unwrap()
            .expect("a response record yields a CDX entry");

        let doc = build_index_doc(&record, &cdx, 524288, None)
            .expect("an HTML capture is indexable");

        // Before the fix this was a string of U+FFFD from the deflate bytes.
        assert_eq!(doc.title, "Example Domain");
        // The title is part of the document text too, as it always has been.
        assert_eq!(doc.body, "Example Domain Roter Berlepsch & Boskoop");
    }

    /// The same body without either layer must still index — the peeling is
    /// detected from the bytes, so it must not disturb plain captures.
    #[test]
    fn index_doc_still_reads_an_unencoded_capture() {
        let block = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n\
                      <html><head><title>Boskoop</title></head><body>Apfel</body></html>";
        let record = response_record("https://example.com/plain", block);
        let cdx = warc_search_cdx::from_warc_record(&record, "crawl.warc.gz").unwrap().unwrap();

        let doc = build_index_doc(&record, &cdx, 524288, None).unwrap();
        assert_eq!(doc.title, "Boskoop");
        assert_eq!(doc.body, "Boskoop Apfel");
    }

    #[test]
    fn strip_html_drops_markup_and_keeps_word_boundaries() {
        let html = "<p>Roter<br>Berlepsch</p><p>Boskoop</p>";
        assert_eq!(strip_html(html), "Roter Berlepsch Boskoop");
    }

    #[test]
    fn strip_html_drops_script_and_style_bodies() {
        let html = "<html><head><style>body{color:red}</style>\
                    <script src=\"x.js\">var apfel = 1;</script></head>\
                    <body>Apfelsorten<noscript>bitte JavaScript</noscript></body></html>";
        assert_eq!(strip_html(html), "Apfelsorten");
    }

    #[test]
    fn strip_html_keeps_elements_that_merely_start_like_script() {
        assert_eq!(strip_html("<scriptural>Text</scriptural>"), "Text");
    }

    #[test]
    fn strip_html_survives_mis_decoded_bytes() {
        // A page that isn't valid UTF-8 arrives here full of U+FFFD. Slicing a
        // &str at a fixed byte offset lands inside one of those 3-byte chars —
        // that panicked the whole request in production.
        let mangled = String::from_utf8_lossy(b"<\xffscript>x</script> Text <\xff\xfe> more");
        let out = strip_html(&mangled);
        assert!(out.contains("Text") && out.contains("more"), "{out:?}");

        // Binary noise with stray '<' bytes must not panic either.
        let noise: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
        strip_html(&String::from_utf8_lossy(&noise));
    }

    #[test]
    fn strip_html_skips_comments_and_unclosed_tags() {
        assert_eq!(strip_html("a<!-- hidden -->b"), "a b");
        // A `<` with no `>` swallows the rest — the same as a browser's parser.
        assert_eq!(strip_html("visible <div class=\"x"), "visible");
    }

    #[test]
    fn strip_html_decodes_entities() {
        assert_eq!(
            strip_html("<p>Gr&uuml;n &amp; Gelb&nbsp;&#8211; &#xe4;pfel</p>"),
            // &uuml; is not in the table and stays verbatim; nbsp collapses.
            "Gr&uuml;n & Gelb – äpfel",
        );
    }

    #[test]
    fn decode_entities_leaves_bare_ampersands_alone() {
        assert_eq!(decode_entities("Tom & Jerry"), "Tom & Jerry");
        assert_eq!(decode_entities("a&nolongerareference;b"), "a&nolongerareference;b");
        assert_eq!(decode_entities("&lt;b&gt;"), "<b>");
    }

    #[test]
    fn extract_title_decodes_and_collapses() {
        let html = "<html><head><title>Obst\n  &amp;  Garten</title></head><body>x</body></html>";
        assert_eq!(extract_title(html), "Obst & Garten");
        assert_eq!(extract_title("<html><body>no title</body></html>"), "");
    }

    #[test]
    fn truncate_backs_up_off_a_multibyte_char() {
        // 'ä' is 2 bytes. A cap landing inside it must not panic and must not
        // emit a partial char.
        let s = "aä".to_owned(); // bytes: a(1) ä(2) => len 3; boundaries at 0,1,3
        assert_eq!(truncate_on_char_boundary(s.clone(), 2), "a"); // 2 is inside 'ä' -> back to 1
        assert_eq!(truncate_on_char_boundary(s.clone(), 3), "aä"); // exact fit
        assert_eq!(truncate_on_char_boundary(s.clone(), 1), "a");
        assert_eq!(truncate_on_char_boundary(s, 99), "aä"); // shorter than cap
    }

    #[test]
    fn truncate_handles_replacement_char_at_the_cap() {
        // The real crash: U+FFFD (3 bytes) straddling a 524288-style cap.
        let mut s = "x".repeat(10);
        s.push('\u{FFFD}'); // 3 bytes, bytes 10..13
        // cap 11 and 12 land inside the replacement char -> back up to 10.
        assert_eq!(truncate_on_char_boundary(s.clone(), 11).len(), 10);
        assert_eq!(truncate_on_char_boundary(s.clone(), 12).len(), 10);
        assert_eq!(truncate_on_char_boundary(s, 13).len(), 13); // whole thing fits
    }
}
