//! `tywb export-pdf-urls` — the PDF URL list, from the CDX and the archive.
//!
//! The list has two halves, and they answer different questions:
//!
//! * **captured** — URLs of records that *are* PDFs. The archive holds these;
//!   the CDX knows every one of them, in every collection. A `SELECT DISTINCT`,
//!   under a second.
//! * **linked** — URLs the indexed HTML *points at*, plus the ones inside the
//!   archive's own PDFs. These are the interesting ones for an external
//!   archiver: a page linking to a document is evidence the document exists
//!   whether or not the crawl ever fetched it.
//!
//! Both are collected during indexing, where the decoded bytes are already in
//! hand (`index.rs`). This subcommand exists because a run only knows its own
//! objects, so "all of them" would otherwise mean a full re-index — days on a
//! large archive, and it re-queues OCR on the way. Instead:
//!
//! * the linked half from HTML reads the records back through one Range GET
//!   each — the CDX coordinates, not a re-download of the WARC objects. On the
//!   corpus this was written against that is ~2 GB of transfer instead of 116
//!   GB, which turns a background job of minutes into the answer instead of a
//!   rebuild;
//! * the linked half from PDFs reads the markup the OCR cache retained
//!   (`ocr_cache.keep_xhtml`) — local disk, no network, no extraction.
//!
//! Both halves always run, and there is deliberately no mode that produces only
//! the captured URLs: a list that quietly omits the links is a wrong answer
//! shaped like a right one, and the cheap captured-only list is what an index
//! run already uploads at the end of every run anyway.
//!
//! Every path renders and uploads through the same functions, so a list from
//! the CDX, a list from a run and a list from a scan are indistinguishable.

use std::path::PathBuf;

use anyhow::Context;
use bytes::Bytes;
use futures_util::StreamExt;
use tracing::{debug, error, info, warn};

use warc_search_cdx::{CdxRecord, CdxStore};
use warc_search_config::{Config, PdfUrlExportConfig};
use warc_search_s3::{build_client, put_object};

/// Arguments forwarded from the `export-pdf-urls` subcommand.
pub struct ExportArgs {
    /// Also write the list to this local file.
    pub out: Option<PathBuf>,
    /// Read, scan and report, but do not upload.
    pub dry_run: bool,
    /// Records fetched concurrently.
    pub jobs: usize,
    /// Stop the HTML scan after this many records (a sample, not a mode).
    pub limit: Option<usize>,
}

// ── Merging the two halves ────────────────────────────────────────────────────

/// What a producer collected, and what became of it.
#[derive(Debug)]
pub(crate) struct MergeReport {
    /// Deduplicated, sorted, filtered: what will be exported.
    pub urls: Vec<String>,
    /// URLs of records that are PDFs.
    pub captured: usize,
    /// URLs the HTML pointed at, counted per *occurrence* — before dedup.
    pub linked: usize,
    /// Dropped for not being a fetchable http(s) URL.
    pub not_http: usize,
    /// Dropped by the skip list.
    pub blacklisted: usize,
}

/// Merge captured PDF URLs and the links discovered in HTML into one list.
///
/// `linked` holds one entry per occurrence, so a document linked from five
/// hundred pages arrives five hundred times: deduplication is not a nicety here
/// but the step that decides how big the list is. The skip list is asked once
/// per *distinct* URL for the same reason — it is a regex set, and asking it
/// per occurrence would be the expensive version of the same question.
///
/// The result is sorted, so rendering it is deterministic and a re-run over the
/// same index produces a byte-identical file.
pub(crate) fn merge(
    captured: Vec<String>,
    linked: Vec<String>,
    blacklist: impl Fn(&str) -> bool,
) -> MergeReport {
    let (captured_n, linked_n) = (captured.len(), linked.len());

    let mut all: Vec<String> = captured.into_iter().chain(linked).collect();
    all.sort();
    all.dedup();

    let mut urls = Vec::with_capacity(all.len());
    let (mut not_http, mut blacklisted) = (0usize, 0usize);
    for url in all {
        // Only a fetchable URL is of any use to a consumer that will fetch it.
        // The CDX of an older index still holds the `dns:` and `urn:`
        // bookkeeping records that ingest drops today.
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            not_http += 1;
            continue;
        }
        // One skip list, and it decides here exactly as it decides at ingest: a
        // wanted page linking into a blacklisted site must not put that site on
        // the list handed to an archiver that will go and fetch it.
        if blacklist(&url) {
            blacklisted += 1;
            continue;
        }
        urls.push(url);
    }

    MergeReport {
        urls,
        captured: captured_n,
        linked: linked_n,
        not_http,
        blacklisted,
    }
}

// ── The artifact ──────────────────────────────────────────────────────────────

/// The placeholder in `PdfUrlExportConfig::key` that becomes the export's UTC
/// timestamp.
pub(crate) const KEY_TIMESTAMP: &str = "{timestamp}";

/// The object key to write, with `{timestamp}` resolved.
///
/// Every export gets an object of its own instead of overwriting one key, and
/// that is not tidiness: two producers write this list, and a shared key meant
/// an incremental index run — which sees only the objects it processed —
/// replaced the complete list with a fragment of it. Whatever an external
/// consumer had not read yet was simply gone. Timestamped keys make each export
/// additive, and the format is chosen so lexical order is chronological order,
/// which is what a consumer listing the bucket needs.
///
/// A pattern without the placeholder is used verbatim, so a fixed key remains
/// available to whoever wants the overwrite.
pub(crate) fn resolve_key(pattern: &str, now: chrono::DateTime<chrono::Utc>) -> String {
    pattern.replace(KEY_TIMESTAMP, &now.format("%Y%m%d-%H%M%S").to_string())
}

/// Render the PDF URL list: deduplicated, sorted, one URL per line.
///
/// Dedup and sort make the output deterministic, so a re-run over the same
/// index produces a byte-identical file and a diff-based consumer sees no
/// churn. An empty list renders as an empty string — no stray newline.
pub(crate) fn render_list(urls: &[String]) -> String {
    let mut unique: Vec<&String> = urls.iter().collect();
    let mut seen = std::collections::HashSet::with_capacity(unique.len());
    unique.retain(|u| seen.insert(u.as_str()));
    unique.sort();

    let mut body = String::new();
    for (i, url) in unique.iter().enumerate() {
        if i > 0 {
            body.push('\n');
        }
        body.push_str(url);
    }
    if !body.is_empty() {
        body.push('\n');
    }
    body
}

/// Upload the list to `s3://export.bucket/export.key`, with `{timestamp}` in
/// the key resolved to now — see [`resolve_key`].
///
/// Best effort by design: both callers have already finished their real work —
/// an index run has written and committed its indexes — so a failing upload is
/// logged and nothing else. Logged loudly, though: an export a consumer waits
/// for is exactly the thing that must not vanish from the logs.
///
/// An empty list uploads nothing at all. A run that saw no PDFs, or a CDX with
/// none in it, must not leave an object behind claiming the archive points at
/// nothing.
pub(crate) async fn upload(
    s3: &aws_sdk_s3::Client,
    export: &Option<PdfUrlExportConfig>,
    urls: &[String],
) {
    let Some(exp) = export else { return };
    let key = resolve_key(&exp.key, chrono::Utc::now());
    let body = render_list(urls);
    if body.is_empty() {
        info!(bucket = %exp.bucket, key = %key,
              "no PDF URLs — nothing exported (an empty object would claim the archive points at none)");
        return;
    }
    let count = body.lines().count();
    match put_object(
        s3,
        &exp.bucket,
        &key,
        Bytes::from(body.into_bytes()),
        "text/plain; charset=utf-8",
    )
    .await
    {
        Ok(()) => info!(bucket = %exp.bucket, key = %key, urls = count, "PDF URL list exported"),
        Err(e) => error!(bucket = %exp.bucket, key = %key, err = %e,
                         "could not export the PDF URL list"),
    }
}

// ── The link scan ─────────────────────────────────────────────────────────────

/// Read the indexed HTML back out of S3 and collect the PDFs it links to.
///
/// One Range GET per record, at the coordinates the CDX already stores — the
/// same operation replay and `/text` are built on, and the reason this is
/// minutes rather than the days a re-index costs.
async fn scan_links(
    cfg: &Config,
    cdx: &CdxStore,
    jobs: usize,
    limit: Option<usize>,
) -> anyhow::Result<Vec<String>> {
    let mut records: Vec<CdxRecord> = Vec::new();
    cdx.for_each_html_record(|r| records.push(r.clone()))
        .context("reading the HTML records out of the CDX")?;
    if let Some(n) = limit {
        records.truncate(n);
    }
    if records.is_empty() {
        warn!("the CDX holds no HTML records — nothing to scan for links");
        return Ok(Vec::new());
    }

    let s3 = build_client(&cfg.s3).await;
    let bucket = cfg.s3.bucket.clone();
    // Decode the *whole* body, not the indexer's `max_text_bytes * 4`. That
    // bound is a memory budget for a streaming loop over multi-GB WARCs; it is
    // not a statement about which links exist. This pass holds one record at a
    // time and can afford the full payload — and should, because a 7 MB portal
    // homepage linking to a PDF is exactly the find the list is for. Eight
    // records in this archive are over the indexer's cap, and they are the
    // big homepages.
    //
    // So the two producers are deliberately not identical: a run exports "the
    // PDFs this run indexed", this exports "every PDF the archive can point
    // at" — a superset. Making them match would mean either truncating here for
    // consistency's sake or lifting the ingest memory budget for a feature that
    // does not need it.
    let cap = crate::http_payload::MAX_DECODED_BYTES;
    let jobs = jobs.max(1);

    info!(
        records = records.len(),
        jobs, "scanning the indexed HTML for links to PDFs"
    );

    let started = std::time::Instant::now();
    let mut links = Vec::new();
    let (mut done, mut errors) = (0usize, 0usize);

    let mut stream = futures_util::stream::iter(records.into_iter().map(|rec| {
        let s3 = &s3;
        let bucket = bucket.as_str();
        async move {
            let found = fetch_and_extract(s3, bucket, &rec, cap).await;
            (rec, found)
        }
    }))
    .buffer_unordered(jobs);

    while let Some((rec, found)) = stream.next().await {
        done += 1;
        match found {
            Ok(found) => links.extend(found),
            // A record that cannot be fetched is skipped, not fatal: this is a
            // read-only sweep over tens of thousands of records, and one
            // pre-`c_offset` entry or one deleted object must not lose the rest.
            Err(e) => {
                errors += 1;
                debug!(url = %rec.original_url, key = %rec.s3_key, err = %format!("{e:#}"),
                       "record not scanned");
            }
        }
        if done % 500 == 0 {
            info!(
                done,
                links = links.len(),
                errors,
                secs = started.elapsed().as_secs(),
                "scanning…"
            );
        }
    }

    info!(
        records = done,
        links = links.len(),
        errors,
        secs = started.elapsed().as_secs(),
        "HTML link scan complete"
    );
    Ok(links)
}

/// One record's links: fetch it by its CDX coordinates, peel the wire framing,
/// scan the markup.
///
/// The peeling is not optional and not skippable here for the same reason it is
/// not skippable in the indexer: what a WARC stores is the response as it came
/// off the wire — chunk framing, then a `Content-Encoding` — so the bytes after
/// the HTTP headers are not the document yet. Scanning an unpeeled body finds
/// no tags at all, and reports that as "this page links to no PDFs".
async fn fetch_and_extract(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    rec: &CdxRecord,
    cap: usize,
) -> anyhow::Result<Vec<String>> {
    let bytes = crate::record_fetch::fetch_warc_record(s3, bucket, rec).await?;
    let http = crate::record_fetch::warc_http_block(&bytes);
    let parts = crate::http_payload::parse_http_block(http);
    let payload = parts.payload(cap).map_err(anyhow::Error::msg)?;

    let text = String::from_utf8_lossy(&payload);
    let mut out = Vec::new();
    crate::index::extract_pdf_links(&text, &rec.original_url, &mut out);
    Ok(out)
}

// ── Subcommand ────────────────────────────────────────────────────────────────

/// The links inside the archive's own PDFs, read from the XHTML the OCR cache
/// retained (`ocr_cache.keep_xhtml`).
///
/// No S3 traffic and no extraction: the markup is on local disk under the digest
/// the CDX already carries. That also bounds what it can find — a document
/// extracted before retention was switched on has text and no markup, and its
/// links stay unrecoverable until something else makes it re-extract. Counted
/// and reported rather than left invisible, because "the archive's PDFs link to
/// nothing" and "nobody retained the markup yet" look the same in the output.
fn cached_pdf_links(cfg: &Config, cdx: &CdxStore) -> anyhow::Result<Vec<String>> {
    let Some(ocr_cfg) = &cfg.indexer.ocr_cache else {
        info!("no OCR cache configured — skipping the PDFs' own internal links");
        return Ok(Vec::new());
    };
    let cache = crate::ocr_cache::OcrCache::open(std::path::Path::new(&ocr_cfg.path))
        .with_context(|| format!("opening the OCR text cache at {}", ocr_cfg.path))?;

    let mut links = Vec::new();
    let (mut records, mut with_xhtml) = (0usize, 0usize);
    cdx.for_each_warc_pdf(|rec| {
        records += 1;
        let Some(digest) = rec.digest.as_deref() else {
            return;
        };
        let Some(xhtml) = cache.get_xhtml(digest) else {
            return;
        };
        with_xhtml += 1;
        crate::index::extract_pdf_links(&xhtml, &rec.original_url, &mut links);
    })
    .context("walking the CDX for PDF records")?;

    info!(
        pdf_records = records,
        with_retained_xhtml = with_xhtml,
        links = links.len(),
        "PDF-internal links read from the retained XHTML"
    );
    Ok(links)
}

pub async fn run(cfg: Config, args: ExportArgs) -> anyhow::Result<()> {
    let cdx = CdxStore::open(&cfg.storage.cdx_db_path)
        .with_context(|| format!("opening the CDX store at {}", cfg.storage.cdx_db_path))?;

    let captured = cdx
        .pdf_urls()
        .context("reading the PDF URLs out of the CDX")?;

    // Both halves, always — see the module docs for why there is no mode that
    // skips the links.
    let mut linked = scan_links(&cfg, &cdx, args.jobs, args.limit).await?;
    // The archive's own PDFs link out too, and their markup is on local disk
    // rather than in S3 — no fetch, no extraction.
    linked.extend(cached_pdf_links(&cfg, &cdx)?);

    let report = merge(captured, linked, |url| cfg.indexer.is_url_blacklisted(url));
    info!(
        exportable = report.urls.len(),
        captured = report.captured,
        linked = report.linked,
        blacklisted = report.blacklisted,
        not_http = report.not_http,
        "PDF URLs collected",
    );

    if let Some(path) = &args.out {
        let body = render_list(&report.urls);
        std::fs::write(path, &body).with_context(|| format!("writing {}", path.display()))?;
        info!(path = %path.display(), bytes = body.len(), urls = body.lines().count(),
              "PDF URL list written");
    }

    if args.dry_run {
        info!("--dry-run: not uploading");
        return Ok(());
    }

    let Some(exp) = &cfg.indexer.pdf_url_export else {
        if args.out.is_some() {
            warn!("indexer.pdf_url_export is not configured — wrote the local file only");
            return Ok(());
        }
        anyhow::bail!(
            "indexer.pdf_url_export is not configured, and no --out was given — nothing to do"
        );
    };

    let s3 = build_client(&cfg.s3).await;
    info!(bucket = %exp.bucket, key = %exp.key, "uploading the PDF URL list");
    upload(&s3, &cfg.indexer.pdf_url_export, &report.urls).await;

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{merge, render_list};

    fn urls(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn the_list_is_deduplicated_sorted_one_per_line() {
        let out = render_list(&urls(&[
            "https://b.example/boskoop.pdf",
            "https://a.example/berlepsch.pdf",
            "https://b.example/boskoop.pdf",
        ]));
        assert_eq!(
            out, "https://a.example/berlepsch.pdf\nhttps://b.example/boskoop.pdf\n",
            "sorted, deduplicated, one URL per line, newline-terminated",
        );
    }

    #[test]
    fn a_single_url_is_one_line() {
        assert_eq!(
            render_list(&urls(&["https://a.example/x.pdf"])),
            "https://a.example/x.pdf\n",
        );
    }

    #[test]
    fn nothing_seen_renders_nothing() {
        // An empty body is what makes `upload` skip: a zero-byte object would
        // claim the archive points at no PDF at all.
        assert_eq!(render_list(&[]), "");
        assert_eq!(render_list(&[]).lines().count(), 0);
    }

    // ── The key each export writes to ────────────────────────────────────

    use super::resolve_key;

    fn at(rfc3339: &str) -> chrono::DateTime<chrono::Utc> {
        chrono::DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&chrono::Utc)
    }

    #[test]
    fn the_key_carries_the_timestamp_of_the_export() {
        assert_eq!(
            resolve_key("pdf-urls-{timestamp}.txt", at("2026-09-14T12:34:56Z")),
            "pdf-urls-20260914-123456.txt",
        );
    }

    #[test]
    fn timestamped_keys_sort_the_way_the_exports_happened() {
        // The property a consumer listing the bucket depends on: lexical order
        // is chronological order, so the last key is the newest export.
        let keys: Vec<String> = [
            "2026-09-14T12:34:56Z",
            "2026-11-02T09:00:00Z",
            "2027-01-15T23:59:59Z",
        ]
        .iter()
        .map(|t| resolve_key("pdf-urls-{timestamp}.txt", at(t)))
        .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "{keys:?}");
    }

    #[test]
    fn two_exports_never_write_the_same_object() {
        // The reason for the timestamp: an incremental index run used to replace
        // the complete list with the handful of URLs it had seen.
        let a = resolve_key("pdf-urls-{timestamp}.txt", at("2026-09-14T12:34:56Z"));
        let b = resolve_key("pdf-urls-{timestamp}.txt", at("2026-09-14T18:02:11Z"));
        assert_ne!(a, b);
    }

    #[test]
    fn a_key_without_the_placeholder_is_used_verbatim() {
        // A fixed key stays available to whoever wants the overwrite.
        assert_eq!(
            resolve_key("incoming.txt", at("2026-09-14T12:34:56Z")),
            "incoming.txt",
        );
    }

    #[test]
    fn rendering_does_not_reorder_what_the_caller_holds() {
        // `upload` and `--out` both render from the same slice; taking it by
        // reference means the caller's own bookkeeping is untouched.
        let held = urls(&["https://b.example/2.pdf", "https://a.example/1.pdf"]);
        let _ = render_list(&held);
        assert_eq!(held[0], "https://b.example/2.pdf");
    }

    // ── Merging the two halves ────────────────────────────────────────────

    #[test]
    fn a_link_seen_on_many_pages_is_exported_once() {
        // The common case: one document, five hundred pages pointing at it.
        // Counting occurrences is what `linked` reports; the list holds one.
        let report = merge(
            urls(&["https://a.example/held.pdf"]),
            urls(&[
                "https://b.example/found.pdf",
                "https://b.example/found.pdf",
                "https://b.example/found.pdf",
            ]),
            |_| false,
        );
        assert_eq!(
            report.urls,
            urls(&["https://a.example/held.pdf", "https://b.example/found.pdf"]),
        );
        assert_eq!((report.captured, report.linked), (1, 3));
    }

    #[test]
    fn a_pdf_the_archive_holds_and_a_page_links_to_appears_once() {
        // The two halves overlap by design: a crawled PDF is both captured and
        // linked from the page that led to it.
        let report = merge(
            urls(&["https://a.example/x.pdf"]),
            urls(&["https://a.example/x.pdf"]),
            |_| false,
        );
        assert_eq!(report.urls, urls(&["https://a.example/x.pdf"]));
        assert_eq!(report.urls.len(), 1);
    }

    #[test]
    fn the_skip_list_decides_about_links_too() {
        // A wanted page linking into a blacklisted site must not put that site
        // on a list an archiver will go and fetch from.
        let report = merge(
            vec![],
            urls(&["https://keep.example/a.pdf", "https://drop.example/b.pdf"]),
            |url| url.contains("drop.example"),
        );
        assert_eq!(report.urls, urls(&["https://keep.example/a.pdf"]));
        assert_eq!(report.blacklisted, 1);
    }

    #[test]
    fn a_url_nothing_can_fetch_is_not_exported() {
        // Older indexes still hold the crawler's bookkeeping records; a list
        // handed to an archiver must contain nothing but fetchable URLs.
        let report = merge(
            urls(&["dns:example.org?type=a", "urn:x-wpull:log"]),
            urls(&["https://a.example/x.pdf"]),
            |_| false,
        );
        assert_eq!(report.urls, urls(&["https://a.example/x.pdf"]));
        assert_eq!(report.not_http, 2);
    }

    #[test]
    fn merging_an_empty_run_produces_an_empty_list() {
        let report = merge(vec![], vec![], |_| false);
        assert!(report.urls.is_empty());
        assert_eq!(render_list(&report.urls), "", "so nothing is uploaded");
    }
}
