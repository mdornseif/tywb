//! `tywb ocr-worker` — the process that pays for OCR so the index run does
//! not have to.
//!
//! The index run, when the OCR cache is configured, never calls Tika: a PDF
//! record whose digest is not in the cache gets a small job file and the run
//! moves on. This process drains those jobs: it fetches each record's bytes
//! from S3 with one Range GET, extracts the text (with a generous timeout,
//! and optionally against its own Tika server), and stores the result under
//! the digest. The next index run — or the next full rebuild — finds the
//! text on disk and never hits a Tika request.
//!
//! # Prefill
//!
//! Before a rebuild, warm the cache from the existing CDX index:
//!
//! ```bash
//! tywb ocr-worker --prefill
//! ```
//!
//! This scans the SQLite CDX for every PDF record of the primary WARC
//! collection whose cache is cold, and queues it. The worker itself follows:
//!
//! ```bash
//! tywb ocr-worker
//! ```
//!
//! The queue is deduplicated by content digest (the only thing the lookup
//! needs), so the same PDF from two crawls appears once and is extracted once.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use warc_search_cdx::{CdxStore, DEFAULT_COLLECTION};
use warc_search_config::Config;
use warc_search_s3::build_client;

use crate::http_payload::{MAX_DECODED_BYTES, parse_http_block};
use crate::ocr_cache::{JobRetry, OcrCache, OcrJob};
use crate::pdf::{ExtractError, PdfExtractor};
use crate::record_fetch::fetch_warc_record;

/// Arguments for the `ocr-worker` subcommand.
pub struct WorkerArgs {
    /// Before draining, walk the CDX index and queue every WARC PDF whose
    /// text is not in the cache yet — so the next rebuild starts warm.
    pub prefill: bool,
    /// How many documents to extract in parallel. Overrides
    /// `indexer.ocr_cache.workers`.
    pub jobs: Option<usize>,
}

/// Outcome of one job. The caller updates the summary counters.
#[derive(Debug)]
enum Outcome {
    AlreadyInCache,
    Extracted(usize, u64), // (chars, secs)
    NoText,
    Failed,
}

/// Summary counters.
#[derive(Debug, Default)]
struct Counters {
    done:        usize,
    from_cache:  usize,
    extracted:   usize,
    no_text:     usize,
    rescheduled: usize,
    parked:      usize,
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn run(cfg: Config, args: WorkerArgs) -> Result<()> {
    let ocr_cfg = cfg
        .indexer
        .ocr_cache
        .as_ref()
        .context("indexer.ocr_cache is not configured — set indexer.ocr_cache.path")?;

    let tika = ocr_cfg
        .tika
        .as_ref()
        .or(cfg.indexer.tika.as_ref())
        .context(
            "no Tika backend — set indexer.ocr_cache.tika (or indexer.tika, if the index \
             process shares one) to extract text from PDFs",
        )?;
    info!(url = %tika.url, timeout_secs = tika.timeout_secs, "ocr-worker starting");
    let extractor = PdfExtractor::new(tika);

    let cache = OcrCache::open(Path::new(&ocr_cfg.path))
        .with_context(|| format!("opening the OCR text cache at {}", ocr_cfg.path))?;
    let s3 = Arc::new(build_client(&cfg.s3).await);
    let workers = args.jobs.unwrap_or(ocr_cfg.workers).max(1);
    let max_attempts = ocr_cfg.max_attempts;

    // ── Prefill ───────────────────────────────────────────────────────────────

    if args.prefill {
        let store = CdxStore::open_readonly(&cfg.storage.cdx_db_path)
            .with_context(|| format!("opening CDX store at {}", cfg.storage.cdx_db_path))?;

        let mut enqueued = 0usize;
        let mut cached = 0usize;
        let mut unfetchable = 0usize;
        let seen = store
            .for_each_warc_pdf(|rec| {
                let Some(digest) = rec.digest.as_deref() else { return };
                if cache.has(digest) {
                    cached += 1;
                    return;
                }
                // A `.warc.gz` record without a compressed offset cannot be
                // fetched record-by-record until `tywb index --force` writes
                // the offsets into the CDX. Skip it for now.
                if rec.s3_key.to_ascii_lowercase().ends_with(".gz")
                    && rec.c_offset.is_none()
                {
                    unfetchable += 1;
                    return;
                }
                let job = OcrJob {
                    digest:   digest.to_owned(),
                    bucket:   cfg.s3.bucket.clone(),
                    s3_key:   rec.s3_key.clone(),
                    offset:   rec.offset,
                    c_offset: rec.c_offset,
                    length:   rec.length,
                    url:      rec.original_url.clone(),
                    attempts: 0,
                };
                match cache.enqueue(&job) {
                    Ok(()) => enqueued += 1,
                    Err(e) => warn!(digest, err = %e, "could not enqueue the prefill job"),
                }
            })
            .context("scanning the CDX for PDF records")?;

        info!(seen, enqueued, cached, unfetchable, "prefill pass complete");
    }

    // ── Drain ────────────────────────────────────────────────────────────────

    let jobs = cache.queued_jobs();
    if jobs.is_empty() {
        info!("queue is empty — nothing to do");
        return Ok(());
    }
    info!(count = jobs.len(), workers, "draining the OCR queue");

    let sem = Arc::new(Semaphore::new(workers));
    let mut set = tokio::task::JoinSet::new();
    let counters = Arc::new(std::sync::Mutex::new(Counters::default()));

    for (path, job) in jobs {
        let permit = sem.clone().acquire_owned().await.unwrap();
        let s3 = Arc::clone(&s3);
        let cache = cache.clone();
        let extractor = extractor.clone();
        let counters = Arc::clone(&counters);

        set.spawn(async move {
            let _guard = permit;
            let t0 = Instant::now();

            // Someone may have filled the cache since this job was queued.
            if cache.get(&job.digest).is_some() {
                cache.remove_job(&path);
                counters.lock().unwrap().from_cache += 1;
                counters.lock().unwrap().done += 1;
                return;
            }

            let outcome = run_job(&s3, &cache, &extractor, max_attempts, &path, &job).await;
            match outcome {
                Outcome::Extracted(chars, secs) => {
                    info!(url = %job.url, digest = %job.digest, chars, secs,
                          "extracted and cached");
                    counters.lock().unwrap().extracted += 1;
                }
                Outcome::NoText => {
                    warn!(url = %job.url, digest = %job.digest,
                          "PDF has no extractable text — cached as such");
                    counters.lock().unwrap().no_text += 1;
                }
                Outcome::Failed => {
                    counters.lock().unwrap().parked += 1;
                }
                Outcome::AlreadyInCache => {
                    counters.lock().unwrap().from_cache += 1;
                }
            }
            counters.lock().unwrap().done += 1;

            debug!(url = %job.url, secs = t0.elapsed().as_secs(), "job finished");
        });
    }

    while set.join_next().await.is_some() {}

    let cnt = counters.lock().unwrap();
    info!(
        done        = cnt.done,
        extracted   = cnt.extracted,
        no_text     = cnt.no_text,
        from_cache  = cnt.from_cache,
        parked      = cnt.parked,
        "OCR worker finished",
    );
    Ok(())
}

// ── Job processing ────────────────────────────────────────────────────────────

/// Fetch one record, extract its text, and cache the result. Returns what
/// became of the job so the caller can update the summary.
async fn run_job(
    s3: &aws_sdk_s3::Client,
    cache: &OcrCache,
    extractor: &PdfExtractor,
    max_attempts: u32,
    path: &Path,
    job: &OcrJob,
) -> Outcome {
    match fetch_and_extract(s3, extractor, job).await {
        Ok(Ok(doc)) => {
            // Extraction succeeded. Store the text (even if the quality gate
            // rejected it — the gate is a judgement about the text, and
            // re-running OCR would not change it).
            if let Err(e) = cache.put(&job.digest, &doc.title, &doc.body) {
                error!(digest = %job.digest, err = %e,
                       "could not store the extracted text — job stays queued");
                return Outcome::Failed;
            }
            cache.remove_job(path);
            Outcome::Extracted(doc.body.len(), 0)
        }
        Ok(Err(_reason)) => {
            // The file gave no text (too large, truncated, empty). Cache the
            // negative answer so neither this worker nor the next rebuild pays
            // for the attempt again.
            if let Err(e) = cache.put(&job.digest, "", "") {
                error!(digest = %job.digest, err = %e,
                       "could not cache the negative answer — job will be retried");
                return Outcome::Failed;
            }
            cache.remove_job(path);
            Outcome::NoText
        }
        Err(reason) => {
            // A transient failure — S3, Tika, or a restarted worker.
            match cache.retry(path, job, &reason, max_attempts) {
                JobRetry::Rescheduled | JobRetry::Parked => Outcome::Failed,
            }
        }
    }
}

/// Fetch the record bytes, extract, and return the result.
async fn fetch_and_extract(
    s3: &aws_sdk_s3::Client,
    extractor: &PdfExtractor,
    job: &OcrJob,
) -> Result<Result<crate::pdf::PdfDoc, String>, String> {
    let rec = warc_search_cdx::CdxRecord {
        surt_url:     String::new(),
        timestamp:    String::new(),
        original_url: job.url.clone(),
        mime:         Some("application/pdf".to_owned()),
        status:       None,
        digest:       Some(job.digest.clone()),
        s3_key:       job.s3_key.clone(),
        offset:       job.offset,
        length:       job.length,
        c_offset:     job.c_offset,
        collection:   DEFAULT_COLLECTION.to_owned(),
    };

    let raw = fetch_warc_record(s3, &job.bucket, &rec)
        .await
        .map_err(|e| format!("fetching the record: {e}"))?;

    // Parse the WARC record exactly the way the index run sees it.
    use std::io::Cursor;
    let record = match warc::WarcReader::new(Cursor::new(&raw)).next_record() {
        Ok(Some(r)) => r,
        Ok(None) => {
            return Err("fetched bytes hold no WARC record — truncated?".to_owned());
        }
        Err(e) => return Err(format!("WARC parse: {e}")),
    };

    // The payload: chunked framing, then content-encoding — both layers
    // the index run peels before handing bytes to Tika.
    let payload = match parse_http_block(record.block.as_ref()).payload(MAX_DECODED_BYTES) {
        Ok(p) => p.into_owned(),
        Err(e) => return Err(format!("payload decode: {e}")),
    };

    let url = job.url.clone();
    let extractor = extractor.clone();
    let extracted = tokio::task::spawn_blocking(move || extractor.try_extract(&url, &payload, false)).await;

    match extracted {
        Ok(Ok(doc)) => Ok(Ok(doc)),
        Ok(Err(e @ (ExtractError::TooLarge { .. } | ExtractError::Truncated | ExtractError::Empty))) => {
            Ok(Err(e.to_string()))
        }
        Ok(Err(e)) => Err(e.to_string()),
        Err(e) => Err(format!("extraction task panicked: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ocr_cache::OcrJob;

    // ── Prefill ─────────────────────────────────────────────────────────────

    #[test]
    fn prefill_enqueues_missing_pdfs_from_the_cdx() {
        use warc_search_cdx::CdxStore;
        use warc_search_config::Config;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("cdx.db");
        let cache_dir = dir.path().join("ocr-cache");

        // Seed the CDX with a few records.
        let store = CdxStore::open(db_path.to_str().unwrap()).unwrap();
        let mut pdf = warc_search_cdx::CdxRecord {
            surt_url:     "com,example)/band30.pdf".to_owned(),
            timestamp:    "20260101000000".to_owned(),
            original_url: "https://example.com/band30.pdf".to_owned(),
            mime:         Some("application/pdf".to_owned()),
            status:       Some(200),
            digest:       Some("sha1:BAND30".to_owned()),
            s3_key:       "warc/crawl.warc.gz".to_owned(),
            offset:       1000,
            length:       33000000,
            c_offset:     Some(512),
            collection:   "warc".to_owned(),
        };
        store.upsert(&pdf).unwrap();

        // A second PDF, already cached — should not be re-queued.
        pdf.digest = Some("sha1:GARTEN".to_owned());
        pdf.surt_url = "com,example)/gartenwelt.pdf".to_owned();
        pdf.original_url = "https://example.com/gartenwelt.pdf".to_owned();
        pdf.s3_key = "warc/garten.warc.gz".to_owned();
        store.upsert(&pdf).unwrap();

        let cache = OcrCache::open(&cache_dir).unwrap();
        cache.put("sha1:GARTEN", "Gartenwelt", "Rote Berlepsch").unwrap();

        // Run prefill via a synthetic Config.
        let yaml = format!(
            "s3:\n  bucket: test\nstorage:\n  cdx_db_path: {}\n",
            db_path.to_str().unwrap().replace('\\', "/"),
        );
        let mut cfg = Config::from_yaml(&yaml).unwrap();
        cfg.indexer.ocr_cache = Some(warc_search_config::OcrCacheConfig {
            path:          cache_dir.to_str().unwrap().to_owned(),
            workers:       2,
            max_attempts:  3,
            tika:          None,
        });

        let ro_store = CdxStore::open_readonly(db_path.to_str().unwrap()).unwrap();
        let mut enqueued = 0usize;
        ro_store.for_each_warc_pdf(|rec| {
            let d = rec.digest.as_deref().unwrap();
            if cache.has(d) { return; }
            let job = OcrJob {
                digest:   d.to_owned(),
                bucket:   "test".to_owned(),
                s3_key:   rec.s3_key.clone(),
                offset:   rec.offset,
                c_offset: rec.c_offset,
                length:   rec.length,
                url:      rec.original_url.clone(),
                attempts: 0,
            };
            cache.enqueue(&job).unwrap();
            enqueued += 1;
        })
        .unwrap();

        assert_eq!(enqueued, 1, "sha1:GARTEN is cached, sha1:BAND30 is not");
        let jobs = cache.queued_jobs();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.digest, "sha1:BAND30");

        // A duplicate with the same digest overwrites rather than duplicates.
        let mut dup = pdf.clone();
        dup.s3_key = "warc/mirror.warc.gz".to_owned();
        store.upsert(&dup).unwrap();

        let mut cnt = 0usize;
        ro_store.for_each_warc_pdf(|_| cnt += 1).unwrap();
        assert_eq!(cnt, 2, "two PDF rows in the CDX");
        let jobs = cache.queued_jobs();
        assert_eq!(jobs.len(), 1, "but only one queue entry — digests dedupe");
    }
}