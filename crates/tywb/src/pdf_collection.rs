//! Indexing a `pdf_bucket` collection — a bucket of standalone PDF objects.
//!
//! Unlike the WARC path, there is no container: each S3 object *is* one PDF.
//! For each object we build a single CDX record (its public URL, `offset 0`,
//! full length, `collection` = the source name) and, when a Tika backend is
//! configured, extract its text for the fulltext index. Replay serves the
//! object straight from the bucket (see `server::replay`).
//!
//! ETag state is shared with the WARC indexer, so re-runs only touch objects
//! whose ETag changed.

use anyhow::{Context, Result};
use tracing::{debug, error, info, warn};

use warc_search_cdx::{CdxRecord, CdxStore};
use warc_search_config::{CollectionConfig, IndexerConfig};
use warc_search_search::{IndexDoc, SearchIndex};
use warc_search_s3::{get_bytes, put_object, ListState, Lister};

use crate::pdf::{ExtractError, PdfExtractor};

/// Per-collection ingest counters.
#[derive(Debug, Default)]
pub struct CollStats {
    pub objects: usize,
    pub cdx_new: usize,
    pub cdx_known: usize,
    pub indexed: usize,
    pub skipped: usize,
    pub errors: usize,
}

/// Index every new/changed PDF object in `coll`'s bucket.
pub async fn index_pdf_collection(
    coll: &CollectionConfig,
    s3: &aws_sdk_s3::Client,
    cdx: &mut CdxStore,
    search: &mut SearchIndex,
    cfg: &IndexerConfig,
    state: &mut ListState,
) -> Result<CollStats> {
    if coll.kind != "pdf_bucket" {
        warn!(name = %coll.name, kind = %coll.kind, "unknown collection kind — skipping");
        return Ok(CollStats::default());
    }
    let base = coll.public_base_url.clone().unwrap_or_default();
    if base.is_empty() {
        warn!(name = %coll.name, "collection has no public_base_url — skipping");
        return Ok(CollStats::default());
    }

    // Text extraction is configured per collection, layered over the global
    // Tika settings: a corpus that arrives pre-OCR'd wants `no_ocr` and a size
    // limit measured in hundreds of megabytes, and the web archive next to it
    // wants neither. One shared setting made every choice wrong somewhere.
    let extractor = if cfg.index_pdfs {
        cfg.tika.as_ref().map(|tika| match &coll.tika {
            Some(over) => {
                let merged = tika.with_override(over);
                info!(name = %coll.name, ocr = %merged.ocr_strategy,
                      max_pdf_bytes = merged.max_pdf_bytes, timeout_secs = merged.timeout_secs,
                      "collection overrides the Tika settings");
                PdfExtractor::new(&merged)
            }
            None => PdfExtractor::new(tika),
        })
    } else {
        None
    };
    if extractor.is_none() {
        warn!(name = %coll.name, "no Tika backend configured — PDFs will be listed but not searchable");
    }

    // A bad key pattern stops this collection instead of being ignored: without
    // it the collection covers its entire prefix, and a rule written to narrow a
    // prefix must never fail open — here that would mean OCR-ing the lot.
    let key_pattern = match coll.compile_key_pattern() {
        Ok(p) => p,
        Err(e) => {
            error!(name = %coll.name, pattern = ?coll.key_pattern, err = %e,
                   "collection key_pattern does not compile — skipping the collection");
            return Ok(CollStats::default());
        }
    };

    let lister = {
        let l = Lister::new(s3, coll.bucket.clone());
        match &coll.prefix {
            Some(p) if !p.is_empty() => l.with_prefix(p.clone()),
            _ => l,
        }
    };
    let objects = lister
        .list_new_or_changed(state)
        .await
        .with_context(|| format!("listing collection bucket {}", coll.bucket))?;

    info!(name = %coll.name, bucket = %coll.bucket, count = objects.len(), "indexing PDF collection");

    let mut stats = CollStats::default();

    for obj in &objects {
        // Not ours: leave it entirely alone, without marking it seen, so a
        // later collection over the same prefix still picks it up.
        if key_pattern.as_ref().is_some_and(|re| !re.is_match(&obj.key)) {
            continue;
        }
        if !obj.is_pdf() {
            stats.skipped += 1;
            state.mark_seen(&obj.key, obj.etag.clone());
            continue;
        }
        stats.objects += 1;

        let url = format!("{base}{}", obj.key);
        if cfg.is_url_blacklisted(&url) {
            stats.skipped += 1;
            state.mark_seen(&obj.key, obj.etag.clone());
            continue;
        }

        // `mark_seen` records that this object, at this ETag, has been dealt
        // with — the next run skips it. It is therefore only written when the
        // object really has been dealt with. A crashed or timed-out Tika, or a
        // failed S3 GET, leaves the object unseen so the next run picks it up
        // again; marking it done is how an archive quietly accumulates records
        // that have a CDX entry and no searchable text at all.
        match index_one_pdf(coll, &url, obj, s3, cdx, search, extractor.as_ref()).await {
            Ok(outcome) => {
                stats.cdx_new += 1;
                match outcome {
                    PdfOutcome::Indexed => {
                        stats.indexed += 1;
                        state.mark_seen(&obj.key, obj.etag.clone());
                    }
                    PdfOutcome::NoText => state.mark_seen(&obj.key, obj.etag.clone()),
                    PdfOutcome::ExtractionFailed => {
                        stats.errors += 1;
                        warn!(name = %coll.name, key = %obj.key,
                              "text extraction failed — leaving the object for the next run");
                    }
                }
            }
            Err(e) => {
                warn!(name = %coll.name, key = %obj.key, err = %format!("{e:#}"),
                      "PDF indexing failed — leaving the object for the next run");
                stats.errors += 1;
            }
        }
    }

    search.commit().context("search commit for PDF collection")?;
    info!(
        name = %coll.name,
        objects = stats.objects, indexed = stats.indexed,
        skipped = stats.skipped, errors = stats.errors,
        "PDF collection indexed",
    );
    Ok(stats)
}

// ── Extracted text, kept beside the object ────────────────────────────────────
//
// Extraction is the expensive half of indexing a scanned corpus — OCR of 23
// library volumes took 17.5 hours — and afterwards the result exists nowhere:
// the fulltext index does not store the body it indexed. Every later
// improvement to text handling therefore costs those hours again, which in
// practice means the improvement does not happen.
//
// With `store_text`, the text is written back beside the object and read from
// there next time. The first line binds it to the exact bytes it came from, so
// a re-uploaded file is re-extracted instead of being served a stale copy.

/// Suffix for the sidecar. Deliberately not a bare `.txt`: these buckets
/// already carry `.txt` files from other tools, and overwriting somebody else's
/// text with ours would be a silent loss.
const TEXT_SIDECAR_SUFFIX: &str = ".tywb.txt";
/// First line of a sidecar: the format, and the ETag it was extracted from.
const TEXT_SIDECAR_MAGIC: &str = "tywb-text/1";

fn text_sidecar_key(key: &str) -> String {
    format!("{key}{TEXT_SIDECAR_SUFFIX}")
}

/// Render a sidecar: one header line binding it to its source, then the text.
fn render_text_sidecar(etag: Option<&str>, title: &str, text: &str) -> String {
    format!(
        "{TEXT_SIDECAR_MAGIC} source-etag={} title={}\n{text}",
        etag.unwrap_or("-").trim_matches('"'),
        title.replace(['\n', '\r'], " "),
    )
}

/// Read a sidecar back, if it belongs to this exact object.
///
/// Returns `None` when the header is missing or names a different ETag — the
/// object has been replaced since, and its old text describes bytes that are
/// gone.
fn parse_text_sidecar(body: &str, etag: Option<&str>) -> Option<(String, String)> {
    let (header, text) = body.split_once('\n')?;
    let header = header.strip_prefix(TEXT_SIDECAR_MAGIC)?;

    let field = |name: &str| {
        header
            .split(&format!(" {name}="))
            .nth(1)
            .map(|rest| rest.split(" title=").next().unwrap_or(rest).trim().to_owned())
    };
    let stored_etag = field("source-etag")?;
    let want = etag.unwrap_or("-").trim_matches('"');
    if stored_etag != want {
        return None;
    }
    let title = header
        .split(" title=")
        .nth(1)
        .map(|t| t.trim().to_owned())
        .unwrap_or_default();
    Some((title, text.to_owned()))
}

/// What became of one PDF, and whether the next run should try it again.
#[derive(Debug, PartialEq)]
enum PdfOutcome {
    /// Text extracted and queued for the fulltext index.
    Indexed,
    /// A CDX record, but no text — and no reason to expect a different answer
    /// next time: no Tika backend is configured, the file is larger than
    /// `max_pdf_bytes`, it is truncated, or what came back did not read like
    /// text. The object counts as handled.
    NoText,
    /// A CDX record, but no text because the *extractor* failed — Tika crashed,
    /// timed out, or returned nothing at all. That is a statement about this
    /// run, not about the file, so the object is left unseen and tried again.
    ExtractionFailed,
}

/// Fetch one PDF, write its CDX record, and (if extractable) its fulltext doc.
async fn index_one_pdf(
    coll: &CollectionConfig,
    url: &str,
    obj: &warc_search_s3::ObjectMeta,
    s3: &aws_sdk_s3::Client,
    cdx: &mut CdxStore,
    search: &mut SearchIndex,
    extractor: Option<&PdfExtractor>,
) -> Result<PdfOutcome> {
    let surt = warc_search_cdx::surt::to_surt(url)
        .with_context(|| format!("SURT for {url}"))?;
    let timestamp = last_modified_to_ts(obj.last_modified.as_deref());

    let bytes = get_bytes(s3, &coll.bucket, &obj.key)
        .await
        .with_context(|| format!("GET {}", obj.key))?;

    let record = CdxRecord {
        surt_url:     surt,
        timestamp:    timestamp.clone(),
        original_url: url.to_owned(),
        mime:         Some("application/pdf".to_owned()),
        status:       Some(200),
        digest:       None,
        s3_key:       obj.key.clone(),
        offset:       0,
        length:       bytes.len() as u64,
        c_offset:     None, // standalone object — replay reads the whole thing
        collection:   coll.name.clone(),
    };
    cdx.upsert(&record).context("CDX upsert")?;

    // Replace this object's fulltext document rather than adding a second one.
    // `add_document` appends unconditionally, and the CDX row above is an upsert
    // on (surt_url, timestamp) — so without this, re-indexing an object whose
    // ETag changed left the old text in the index next to the new, and every
    // re-upload of a re-OCR'd PDF added another copy.
    //
    // Keyed on `s3_key` like the WARC path, not on URL: a capture of this same
    // public URL may well sit in the main archive too, and that one belongs to
    // its WARC file, not to us. `delete_term` only affects already-committed
    // documents, so the add below survives the shared commit at the end of the
    // collection run. On a first index it matches nothing.
    //
    // The deletion is unconditional, before extraction: this runs only for
    // objects the lister reports as new or changed, and text extracted from the
    // previous bytes no longer describes the object.
    search.delete_s3_key(&obj.key).context("search delete_s3_key")?;

    let Some(extractor) = extractor else {
        return Ok(PdfOutcome::NoText);
    };

    // Text kept from an earlier run, if it belongs to these exact bytes. This
    // is what turns a 17-hour OCR run into a one-off.
    let sidecar = if coll.store_text {
        read_text_sidecar(s3, &coll.bucket, &obj.key, obj.etag.as_deref()).await
    } else {
        None
    };

    let doc = match sidecar {
        Some((title, body)) => {
            debug!(key = %obj.key, chars = body.len(), "text read from sidecar — no extraction");
            let quality_ok = crate::pdf::looks_like_text(&body);
            Ok(crate::pdf::PdfDoc { title, body, quality_ok })
        }
        None => {
            // Tika extraction is a blocking call; keep it off the async runtime.
            // `try_extract` rather than `extract`, because here the *reason*
            // decides whether the object is done or has to be tried again.
            let extractor = extractor.clone();
            let url_owned = url.to_owned();
            let bytes_owned = bytes.to_vec();
            let extracted = tokio::task::spawn_blocking(move || {
                extractor.try_extract(&url_owned, &bytes_owned, false)
            })
            .await
            .context("spawn_blocking panicked")?;

            // Store what the expensive step produced, before anything can go
            // wrong with it. Quality-gate failures are stored too: the gate is a
            // judgement about the text, and re-running OCR would not change it.
            if coll.store_text {
                if let Ok(d) = &extracted {
                    write_text_sidecar(s3, &coll.bucket, &obj.key, obj.etag.as_deref(), d).await;
                }
            }
            extracted
        }
    };

    let pdf_doc = match doc {
        Ok(doc) if doc.quality_ok => doc,
        Ok(doc) => {
            warn!(url, chars = doc.body.len(),
                  "PDF text rejected by quality gate (likely OCR noise)");
            return Ok(PdfOutcome::NoText);
        }
        // The file itself, or a limit set for it: the same answer next time.
        // `Empty` belongs here — Tika parsed the document and found no text,
        // which is a fact about the document (a scan with no text layer, with
        // OCR off). Retrying it would re-download and re-parse hundreds of
        // megabytes every run to be told the same thing.
        Err(e @ (ExtractError::TooLarge { .. } | ExtractError::Truncated | ExtractError::Empty)) => {
            warn!(url, bytes = bytes.len(), reason = %e, "PDF not extracted");
            return Ok(PdfOutcome::NoText);
        }
        // Tika crashed, timed out, or refused the connection: this run, not
        // this file.
        Err(e) => {
            warn!(url, err = %e, "PDF extraction failed");
            return Ok(PdfOutcome::ExtractionFailed);
        }
    };

    let ts: u64 = timestamp.parse().unwrap_or(0);
    let index_doc = IndexDoc {
        url:       url.to_owned(),
        timestamp: ts,
        title:     pdf_doc.title,
        body:      pdf_doc.body,
        mime:      Some("application/pdf".to_owned()),
        s3_key:    obj.key.clone(),
        offset:    0,
        length:    bytes.len() as u64,
        collection: coll.name.clone(),
    };
    search.add_document(&index_doc).context("search add_document")?;
    Ok(PdfOutcome::Indexed)
}

/// Fetch the sidecar for `key`, if it exists and matches `etag`.
///
/// Every failure here is a miss, not an error: a missing, unreadable or stale
/// sidecar just means the text has to be extracted, which is the normal path.
async fn read_text_sidecar(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    etag: Option<&str>,
) -> Option<(String, String)> {
    let sidecar = text_sidecar_key(key);
    let bytes = match get_bytes(s3, bucket, &sidecar).await {
        Ok(b) => b,
        Err(warc_search_s3::S3Error::NotFound { .. }) => return None,
        Err(e) => {
            debug!(key = %sidecar, err = %e, "text sidecar unreadable — extracting instead");
            return None;
        }
    };
    let body = String::from_utf8_lossy(&bytes);
    match parse_text_sidecar(&body, etag) {
        Some(v) => Some(v),
        None => {
            debug!(key = %sidecar, "text sidecar does not match this object — extracting instead");
            None
        }
    }
}

/// Write the extracted text beside the object. Best effort: a bucket that
/// refuses the write costs nothing but the next extraction.
async fn write_text_sidecar(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    etag: Option<&str>,
    doc: &crate::pdf::PdfDoc,
) {
    let sidecar = text_sidecar_key(key);
    let body = render_text_sidecar(etag, &doc.title, &doc.body);
    match put_object(s3, bucket, &sidecar, body.into(), "text/plain; charset=utf-8").await {
        Ok(()) => info!(key = %sidecar, chars = doc.body.len(), "extracted text stored"),
        Err(e) => warn!(key = %sidecar, err = %e, "could not store extracted text"),
    }
}

/// Convert an S3 `LastModified` (RFC3339, e.g. `2026-02-27T22:18:43Z`) into a
/// 14-digit `YYYYMMDDHHMMSS` CDX timestamp. Falls back to all-zeros when the
/// value is missing or unparseable, which sorts before any real capture.
fn last_modified_to_ts(last_modified: Option<&str>) -> String {
    const ZERO: &str = "00000000000000";
    let Some(s) = last_modified else {
        return ZERO.to_owned();
    };
    match chrono::DateTime::parse_from_rfc3339(s) {
        Ok(dt) => dt.format("%Y%m%d%H%M%S").to_string(),
        Err(_) => ZERO.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── The text sidecar ──────────────────────────────────────────────────

    #[test]
    fn a_sidecar_round_trips() {
        let doc = crate::pdf::PdfDoc {
            title: "Band 01".to_owned(),
            body: "Obst und Kirschen\nzweite Zeile".to_owned(),
            quality_ok: true,
        };
        let raw = render_text_sidecar(Some("\"abc123\""), &doc.title, &doc.body);
        let (title, text) = parse_text_sidecar(&raw, Some("\"abc123\"")).unwrap();
        assert_eq!(title, "Band 01");
        assert_eq!(text, doc.body, "the text must come back byte for byte, newlines and all");
    }

    #[test]
    fn a_sidecar_from_other_bytes_is_refused() {
        // The object was re-uploaded: its old text describes bytes that no
        // longer exist, and serving it would be worse than extracting again.
        let raw = render_text_sidecar(Some("old-etag"), "T", "alter Text");
        assert!(parse_text_sidecar(&raw, Some("new-etag")).is_none());
    }

    #[test]
    fn junk_in_the_sidecar_is_a_miss_not_a_crash() {
        for body in ["", "no header at all", "tywb-text/9 source-etag=x\ntext", "\n"] {
            assert!(parse_text_sidecar(body, Some("x")).is_none() || body.starts_with("tywb-text/1"));
        }
    }

    #[test]
    fn the_sidecar_key_does_not_collide_with_other_tools() {
        // These buckets already carry `.txt` files from the OCR pipeline that
        // produced the PDFs; overwriting one would be a silent loss.
        assert_eq!(text_sidecar_key("archive-org/10229044bsb/10229044bsb.pdf"),
                   "archive-org/10229044bsb/10229044bsb.pdf.tywb.txt");
        assert_ne!(text_sidecar_key("a/b.pdf"), "a/b.txt");
    }

    // ── Which failures are the object's, and which are the run's ──────────

    #[test]
    fn only_a_handled_object_counts_as_handled() {
        // The distinction that keeps an archive from filling up with CDX
        // records that have no searchable text: Tika falling over is a fact
        // about this run, so the object must come back next time.
        assert_ne!(PdfOutcome::ExtractionFailed, PdfOutcome::NoText);
        for outcome in [PdfOutcome::Indexed, PdfOutcome::NoText] {
            assert!(
                marks_seen(&outcome),
                "{outcome:?} is a final answer and must not be retried forever",
            );
        }
        assert!(
            !marks_seen(&PdfOutcome::ExtractionFailed),
            "a crashed extractor must leave the object for the next run",
        );
    }

    /// Mirrors the decision the indexing loop makes about `state.mark_seen`.
    fn marks_seen(outcome: &PdfOutcome) -> bool {
        !matches!(outcome, PdfOutcome::ExtractionFailed)
    }

    #[test]
    fn last_modified_parses_rfc3339() {
        assert_eq!(last_modified_to_ts(Some("2026-02-27T22:18:43Z")), "20260227221843");
        // AWS smithy sometimes renders a fractional second — parse_from_rfc3339 handles it.
        assert_eq!(last_modified_to_ts(Some("2026-02-27T22:18:43.5Z")), "20260227221843");
    }

    #[test]
    fn last_modified_falls_back_to_zero() {
        assert_eq!(last_modified_to_ts(None), "00000000000000");
        assert_eq!(last_modified_to_ts(Some("not a date")), "00000000000000");
    }
}
