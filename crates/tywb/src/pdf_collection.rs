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
use tracing::{info, warn};

use warc_search_cdx::{CdxRecord, CdxStore};
use warc_search_config::{CollectionConfig, IndexerConfig};
use warc_search_search::{IndexDoc, SearchIndex};
use warc_search_s3::{get_bytes, ListState, Lister};

use crate::pdf::PdfExtractor;

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

    // OCR applies to every collection: the Tika extractor is built from the
    // shared indexer config, so these PDFs get the same PDFBox + Tesseract path.
    let extractor = if cfg.index_pdfs {
        cfg.tika.as_ref().map(PdfExtractor::new)
    } else {
        None
    };
    if extractor.is_none() {
        warn!(name = %coll.name, "no Tika backend configured — PDFs will be listed but not searchable");
    }

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

        match index_one_pdf(coll, &url, obj, s3, cdx, search, extractor.as_ref()).await {
            Ok(did_index) => {
                stats.cdx_new += 1;
                if did_index {
                    stats.indexed += 1;
                }
            }
            Err(e) => {
                warn!(name = %coll.name, key = %obj.key, err = %format!("{e:#}"), "PDF indexing failed — skipping");
                stats.errors += 1;
            }
        }
        state.mark_seen(&obj.key, obj.etag.clone());
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

/// Fetch one PDF, write its CDX record, and (if extractable) its fulltext doc.
/// Returns whether a fulltext document was added.
async fn index_one_pdf(
    coll: &CollectionConfig,
    url: &str,
    obj: &warc_search_s3::ObjectMeta,
    s3: &aws_sdk_s3::Client,
    cdx: &mut CdxStore,
    search: &mut SearchIndex,
    extractor: Option<&PdfExtractor>,
) -> Result<bool> {
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

    let Some(extractor) = extractor else {
        return Ok(false);
    };

    // Tika extraction is a blocking call; keep it off the async runtime.
    let extractor = extractor.clone();
    let url_owned = url.to_owned();
    let bytes_owned = bytes.to_vec();
    let doc = tokio::task::spawn_blocking(move || extractor.extract(&url_owned, &bytes_owned))
        .await
        .context("spawn_blocking panicked")?;

    let Some(pdf_doc) = doc else {
        return Ok(false);
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
    Ok(true)
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
