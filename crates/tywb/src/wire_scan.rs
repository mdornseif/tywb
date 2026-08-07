//! `tywb scan-wire-format` — find the WARC files whose captures need re-indexing.
//!
//! Until the wire-format fix, the indexer read the bytes after the HTTP headers
//! as if they were the content. For a capture stored with chunked framing or a
//! `Content-Encoding`, they are not: those documents went into the fulltext
//! index as `U+FFFD` noise, and such PDFs reached Tika unparseable.
//!
//! Deploying the fix does not repair them — only re-indexing their WARC files
//! does. This scan says which files those are and how big the job is, without
//! reading a single WARC in full: it samples a few records per object and
//! Range-GETs just those, the same way replay fetches one page.
//!
//! ```text
//! tywb scan-wire-format --sample 5 --out /var/tmp/affected.txt
//! while read -r key; do tywb index --force --file "$key"; done < /var/tmp/affected.txt
//! ```
//!
//! # Two kinds of damage, counted separately
//!
//! Not everything the fix changes was ruining the index, and conflating the two
//! overstates the re-index by a wide margin:
//!
//! - **Compressed** (`Content-Encoding: gzip`/`deflate`, or a body that will not
//!   decode at all) — the old indexer read deflate bytes as text and wrote
//!   `U+FFFD` noise. These documents are lost until their file is re-indexed.
//! - **Chunked only** — the framing interleaves short hex size lines with
//!   otherwise readable markup, so `strip_html` still produced real text; the
//!   document is searchable, just carrying a few stray tokens like `173`.
//!   Re-indexing cleans it up but is not urgent.
//!
//! A PDF is the exception: chunk framing alone already makes it unparseable for
//! Tika, so any peeling counts as damage for `application/pdf`.
//!
//! A `Content-Encoding` header on a body the crawler already decoded counts as
//! neither — the old indexer read it correctly.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use futures_util::stream::{self, StreamExt};
use tracing::{debug, info, warn};

use warc_search_cdx::CdxStore;
use warc_search_config::Config;
use warc_search_s3::build_client;

use crate::http_payload::{MAX_DECODED_BYTES, parse_http_block};
use crate::record_fetch::{fetch_warc_record, warc_http_block};

pub struct ScanArgs {
    /// Records sampled per WARC object.
    pub sample: usize,
    /// Stop after this many objects (largest first).
    pub limit: Option<usize>,
    /// Objects sampled concurrently.
    pub jobs: usize,
    /// Write the affected S3 keys here, one per line.
    pub out: Option<PathBuf>,
    /// Print a line per object, not just the summary.
    pub verbose: bool,
}

/// What sampling one WARC object found.
struct ObjectVerdict {
    s3_key: String,
    /// Records in this object that would become fulltext documents.
    indexable: u64,
    sampled: usize,
    /// Samples the old indexer turned into noise.
    damaged: usize,
    /// Samples that were chunked but still indexed as readable text.
    chunked_only: usize,
    /// Samples that could not be fetched (missing offsets, S3 errors).
    unreadable: usize,
}

impl ObjectVerdict {
    /// Holds documents the old indexer wrote as noise — a re-index recovers
    /// content that is currently unsearchable.
    fn needs_reindex(&self) -> bool {
        self.damaged > 0
    }

    /// Chunked but readable: re-indexing only tidies stray tokens.
    fn cosmetic_only(&self) -> bool {
        self.damaged == 0 && self.chunked_only > 0
    }

    /// Documents in this object we expect to be noise, extrapolated from the
    /// sample. Deliberately a projection, not a count — the alternative is
    /// reading every record of every file.
    fn estimated_bad_docs(&self) -> u64 {
        if self.sampled == 0 {
            return 0;
        }
        (self.indexable as f64 * (self.damaged as f64 / self.sampled as f64)).round() as u64
    }
}

pub async fn run(cfg: Config, args: ScanArgs) -> anyhow::Result<()> {
    let store = CdxStore::open(&cfg.storage.cdx_db_path)
        .with_context(|| format!("opening CDX store at {}", cfg.storage.cdx_db_path))?;

    let mut objects = store
        .indexable_counts_by_s3_key()
        .context("listing WARC objects from the CDX")?;
    let total_objects = objects.len();
    let total_indexable: u64 = objects.iter().map(|(_, n)| n).sum();

    if let Some(limit) = args.limit {
        objects.truncate(limit);
    }

    info!(
        objects = objects.len(),
        of_total = total_objects,
        sample = args.sample,
        jobs = args.jobs,
        "scanning stored bodies for wire format",
    );

    let s3 = Arc::new(build_client(&cfg.s3).await);
    let bucket = cfg.s3.bucket.clone();

    // One CDX handle, used up front: sampling is a cheap indexed lookup, and
    // doing it here keeps the store out of the concurrent fetch stage.
    let mut work = Vec::with_capacity(objects.len());
    for (s3_key, indexable) in objects {
        match store.sample_indexable(&s3_key, args.sample) {
            Ok(recs) => work.push((s3_key, indexable, recs)),
            Err(e) => warn!(s3_key = %s3_key, err = %e, "sampling failed — object skipped"),
        }
    }

    let done = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let work_len = work.len();

    let verdicts: Vec<ObjectVerdict> = stream::iter(work)
        .map(|(s3_key, indexable, recs)| {
            let s3 = Arc::clone(&s3);
            let bucket = bucket.clone();
            let done = Arc::clone(&done);
            async move {
                let mut v = ObjectVerdict {
                    s3_key: s3_key.clone(),
                    indexable,
                    sampled: 0,
                    damaged: 0,
                    chunked_only: 0,
                    unreadable: 0,
                };

                for rec in &recs {
                    match fetch_warc_record(&s3, &bucket, rec).await {
                        Ok(bytes) => {
                            v.sampled += 1;
                            match classify(&bytes, rec.mime.as_deref()) {
                                Damage::Encoded => v.damaged += 1,
                                Damage::Chunked => v.chunked_only += 1,
                                Damage::None    => {}
                            }
                        }
                        Err(e) => {
                            v.unreadable += 1;
                            debug!(s3_key = %s3_key, url = %rec.original_url, err = %e,
                                   "sample unreadable");
                        }
                    }
                }

                let n = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if n.is_multiple_of(50) {
                    info!(scanned = n, of = work_len, "scanning…");
                }
                v
            }
        })
        .buffer_unordered(args.jobs.max(1))
        .collect()
        .await;

    report(&verdicts, total_objects, total_indexable, &args)
}

/// What the wire format did to one record — and so what the old indexer made
/// of it. See the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Damage {
    /// Nothing to peel; the old indexer read the same bytes as the new one.
    None,
    /// Chunk framing only: text survived, with stray hex tokens in it.
    Chunked,
    /// Compressed (or undecodable): the old indexer wrote noise.
    Encoded,
}

/// Classify one fetched record.
///
/// `mime` decides how much chunk framing matters: markup survives it, a PDF
/// does not — Tika cannot parse a file with size lines spliced through it.
fn classify(warc_bytes: &[u8], mime: Option<&str>) -> Damage {
    let parts = parse_http_block(warc_http_block(warc_bytes));

    let unframed = crate::http_payload::dechunk(parts.body);
    let chunked = unframed.is_some();
    let after_framing: &[u8] = unframed.as_deref().unwrap_or(parts.body);

    // Compare against the post-framing bytes to isolate the encoding step: a
    // Content-Encoding header on an already-decoded body changes nothing.
    let encoded = match parts.payload(MAX_DECODED_BYTES) {
        Ok(payload) => payload.as_ref() != after_framing,
        // Nothing could decode it, so the old indexer wrote whatever it was.
        Err(_) => true,
    };

    let is_pdf = mime.is_some_and(|m| m.trim().to_ascii_lowercase().starts_with("application/pdf"));

    if encoded || (chunked && is_pdf) {
        Damage::Encoded
    } else if chunked {
        Damage::Chunked
    } else {
        Damage::None
    }
}

fn report(
    verdicts: &[ObjectVerdict],
    total_objects: usize,
    total_indexable: u64,
    args: &ScanArgs,
) -> anyhow::Result<()> {
    let mut damaged: Vec<&ObjectVerdict> =
        verdicts.iter().filter(|v| v.needs_reindex()).collect();
    damaged.sort_by_key(|v| std::cmp::Reverse(v.estimated_bad_docs()));
    let cosmetic: Vec<&ObjectVerdict> = verdicts.iter().filter(|v| v.cosmetic_only()).collect();

    let scanned_objects = verdicts.len();
    let sampled: usize      = verdicts.iter().map(|v| v.sampled).sum();
    let enc_samples: usize  = verdicts.iter().map(|v| v.damaged).sum();
    let chunk_samples: usize = verdicts.iter().map(|v| v.chunked_only).sum();
    let unreadable: usize   = verdicts.iter().map(|v| v.unreadable).sum();
    let no_samples = verdicts.iter().filter(|v| v.sampled == 0).count();

    let reindex_records: u64 = damaged.iter().map(|v| v.indexable).sum();
    let bad_docs: u64        = damaged.iter().map(|v| v.estimated_bad_docs()).sum();
    let cosmetic_records: u64 = cosmetic.iter().map(|v| v.indexable).sum();

    if args.verbose {
        println!("\nObjects holding noise (worst first):");
        for v in &damaged {
            println!(
                "  {:>3}/{:<3} noise  {:>3} chunked  {:>8} indexable  ~{:>8} bad  {}",
                v.damaged, v.sampled, v.chunked_only, v.indexable,
                v.estimated_bad_docs(), v.s3_key,
            );
        }
    }

    println!("\nWire-format scan");
    println!("  WARC objects in CDX          {total_objects:>10}");
    println!("  objects scanned              {scanned_objects:>10}");
    println!("  records sampled              {sampled:>10}");
    println!("    compressed (indexed noise) {enc_samples:>10}   ({:.0}%)", pct(enc_samples, sampled));
    println!("    chunked only (text usable) {chunk_samples:>10}   ({:.0}%)", pct(chunk_samples, sampled));
    if unreadable > 0 {
        println!("    unreadable                 {unreadable:>10}   (missing offsets or S3 errors)");
    }
    if no_samples > 0 {
        println!("  objects with no sample       {no_samples:>10}   (verdict unknown — not counted)");
    }

    println!("\nRe-index needed — documents are noise today");
    println!("  objects                          {:>10}   ({:.0}% of scanned)",
             damaged.len(), pct(damaged.len(), scanned_objects));
    println!("  indexable records to rebuild     {reindex_records:>10}   ({:.0}% of {total_indexable})",
             pct_u64(reindex_records, total_indexable));
    println!("  estimated unsearchable documents ~{bad_docs:>9}   (extrapolated from the samples)");

    println!("\nOptional — chunked but already searchable");
    println!("  objects                          {:>10}", cosmetic.len());
    println!("  indexable records                {cosmetic_records:>10}");
    println!("  (re-indexing only removes stray chunk-size tokens like `173`)");

    match &args.out {
        Some(path) => {
            let mut f = std::fs::File::create(path)
                .with_context(|| format!("writing {}", path.display()))?;
            for v in &damaged {
                writeln!(f, "{}", v.s3_key)?;
            }
            println!("\n  {} keys written to {}", damaged.len(), path.display());
            println!("  re-index with:");
            println!("    while read -r k; do tywb index --force --file \"$k\"; done < {}",
                     path.display());
        }
        None => println!("\n  pass --out <path> to write the keys needing a re-index"),
    }

    let by_ext = damaged.iter().fold(BTreeMap::new(), |mut m: BTreeMap<&str, usize>, v| {
        let ext = if v.s3_key.ends_with(".warc.gz") { ".warc.gz" } else { "other" };
        *m.entry(ext).or_default() += 1;
        m
    });
    debug!(?by_ext, "damaged objects by extension");

    Ok(())
}

fn pct(n: usize, of: usize) -> f64 {
    if of == 0 { 0.0 } else { n as f64 * 100.0 / of as f64 }
}

fn pct_u64(n: u64, of: u64) -> f64 {
    if of == 0 { 0.0 } else { n as f64 * 100.0 / of as f64 }
}

#[cfg(test)]
mod tests {
    // Write comes in via `super::*` (the report writes the key list).
    use super::*;

    fn warc_record(http_block: &[u8]) -> Vec<u8> {
        let mut out = b"WARC/1.0\r\nWARC-Type: response\r\n\r\n".to_vec();
        out.extend_from_slice(http_block);
        out
    }

    #[test]
    fn a_plain_capture_is_undamaged() {
        let block = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>Apfel</html>";
        assert_eq!(classify(&warc_record(block), Some("text/html")), Damage::None);
    }

    #[test]
    fn chunking_alone_left_html_readable() {
        // The correction that matters: chunk framing splices short hex size
        // lines into otherwise readable markup, so strip_html still produced
        // real text. Counting these as noise overstates the re-index.
        let plain = b"<html>Roter Berlepsch</html>";
        let mut chunked = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n".to_vec();
        chunked.extend_from_slice(format!("{:x}\r\n", plain.len()).as_bytes());
        chunked.extend_from_slice(plain);
        chunked.extend_from_slice(b"\r\n0\r\n\r\n");
        assert_eq!(classify(&warc_record(&chunked), Some("text/html")), Damage::Chunked);
    }

    #[test]
    fn chunking_alone_still_ruins_a_pdf() {
        // Tika cannot parse a PDF with size lines spliced through it.
        let body = b"%PDF-1.4 ... %%EOF";
        let mut chunked = b"HTTP/1.1 200 OK\r\nContent-Type: application/pdf\r\n\r\n".to_vec();
        chunked.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
        chunked.extend_from_slice(body);
        chunked.extend_from_slice(b"\r\n0\r\n\r\n");
        assert_eq!(classify(&warc_record(&chunked), Some("application/pdf")), Damage::Encoded);
    }

    #[test]
    fn a_compressed_capture_was_indexed_as_noise() {
        let plain = b"<html>Apfel</html>";
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(plain).unwrap();
        let gz = e.finish().unwrap();

        let mut block = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n".to_vec();
        block.extend_from_slice(&gz);
        assert_eq!(classify(&warc_record(&block), Some("text/html")), Damage::Encoded);

        // …and the real-world shape: chunk framing around the gzip stream.
        let mut both = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n".to_vec();
        both.extend_from_slice(format!("{:x}\r\n", gz.len()).as_bytes());
        both.extend_from_slice(&gz);
        both.extend_from_slice(b"\r\n0\r\n\r\n");
        assert_eq!(classify(&warc_record(&both), Some("text/html")), Damage::Encoded);
    }

    #[test]
    fn a_header_that_lies_is_not_damage() {
        // Content-Encoding on an already-decoded body: the old indexer read
        // this correctly, so re-indexing it would change nothing.
        let block = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n<html>Apfel</html>";
        assert_eq!(classify(&warc_record(block), Some("text/html")), Damage::None);
    }

    #[test]
    fn extrapolation_counts_only_the_noise() {
        let v = ObjectVerdict {
            s3_key: "a.warc.gz".into(), indexable: 1000,
            sampled: 4, damaged: 1, chunked_only: 2, unreadable: 0,
        };
        assert_eq!(v.estimated_bad_docs(), 250);
        assert!(v.needs_reindex() && !v.cosmetic_only());

        // Chunked-only objects are reported separately, never as lost documents.
        let tidy = ObjectVerdict { damaged: 0, ..v };
        assert_eq!(tidy.estimated_bad_docs(), 0);
        assert!(!tidy.needs_reindex() && tidy.cosmetic_only());
    }
}
