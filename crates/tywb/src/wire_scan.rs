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
//! # What counts as affected
//!
//! Exactly what the fix changes: a record is affected when peeling actually
//! alters its bytes — [`HttpParts::payload`] returns something other than the
//! stored body — or when the body cannot be decoded at all (the old indexer
//! wrote noise for those too). A `Content-Encoding` header on a body the
//! crawler already decoded is *not* affected: the old indexer read it fine.
//!
//! [`HttpParts::payload`]: crate::http_payload::HttpParts::payload

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
    /// Samples whose bytes changed when peeled.
    affected: usize,
    /// Samples that could not be fetched (missing offsets, S3 errors).
    unreadable: usize,
}

impl ObjectVerdict {
    fn is_affected(&self) -> bool {
        self.affected > 0
    }

    /// Documents in this object we expect to be wrong, extrapolated from the
    /// sample. Deliberately a projection, not a count — the alternative is
    /// reading every record of every file.
    fn estimated_bad_docs(&self) -> u64 {
        if self.sampled == 0 {
            return 0;
        }
        (self.indexable as f64 * (self.affected as f64 / self.sampled as f64)).round() as u64
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
                    affected: 0,
                    unreadable: 0,
                };

                for rec in &recs {
                    match fetch_warc_record(&s3, &bucket, rec).await {
                        Ok(bytes) => {
                            v.sampled += 1;
                            if needs_peeling(&bytes) {
                                v.affected += 1;
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

/// Does peeling this record's stored body change it?
fn needs_peeling(warc_bytes: &[u8]) -> bool {
    let parts = parse_http_block(warc_http_block(warc_bytes));
    match parts.payload(MAX_DECODED_BYTES) {
        // Borrowed and byte-identical means there was nothing on the wire to
        // undo, so the old indexer read the same bytes the new one does.
        Ok(payload) => payload.as_ref() != parts.body,
        // Undecodable: the old indexer wrote whatever this was into the index.
        Err(_) => true,
    }
}

fn report(
    verdicts: &[ObjectVerdict],
    total_objects: usize,
    total_indexable: u64,
    args: &ScanArgs,
) -> anyhow::Result<()> {
    let mut affected: Vec<&ObjectVerdict> = verdicts.iter().filter(|v| v.is_affected()).collect();
    affected.sort_by_key(|v| std::cmp::Reverse(v.estimated_bad_docs()));

    let scanned_objects = verdicts.len();
    let sampled: usize = verdicts.iter().map(|v| v.sampled).sum();
    let hits: usize    = verdicts.iter().map(|v| v.affected).sum();
    let unreadable: usize = verdicts.iter().map(|v| v.unreadable).sum();
    let no_samples = verdicts.iter().filter(|v| v.sampled == 0).count();

    let reindex_records: u64 = affected.iter().map(|v| v.indexable).sum();
    let bad_docs: u64        = affected.iter().map(|v| v.estimated_bad_docs()).sum();

    if args.verbose {
        println!("\nPer object (affected first):");
        for v in &affected {
            println!(
                "  {:>5}/{:<3} affected  {:>8} indexable  ~{:>8} bad  {}",
                v.affected, v.sampled, v.indexable, v.estimated_bad_docs(), v.s3_key,
            );
        }
    }

    println!("\nWire-format scan");
    println!("  WARC objects in CDX          {total_objects:>10}");
    println!("  objects scanned              {scanned_objects:>10}");
    println!("  objects affected             {:>10}   ({:.0}% of scanned)",
             affected.len(), pct(affected.len(), scanned_objects));
    println!("  records sampled              {sampled:>10}");
    println!("  samples needing peeling      {hits:>10}   ({:.0}%)", pct(hits, sampled));
    if unreadable > 0 {
        println!("  samples unreadable           {unreadable:>10}   (missing offsets or S3 errors)");
    }
    if no_samples > 0 {
        println!("  objects with no sample       {no_samples:>10}   (verdict unknown — not counted)");
    }

    println!("\nRe-index scope");
    println!("  indexable records, all objects   {total_indexable:>10}");
    println!("  indexable records to rebuild     {reindex_records:>10}   ({:.0}%)",
             pct_u64(reindex_records, total_indexable));
    println!("  estimated wrong documents        ~{bad_docs:>9}   (extrapolated from the samples)");

    match &args.out {
        Some(path) => {
            let mut f = std::fs::File::create(path)
                .with_context(|| format!("writing {}", path.display()))?;
            for v in &affected {
                writeln!(f, "{}", v.s3_key)?;
            }
            println!("\n  {} keys written to {}", affected.len(), path.display());
            println!("  re-index with:");
            println!("    while read -r k; do tywb index --force --file \"$k\"; done < {}",
                     path.display());
        }
        None => println!("\n  pass --out <path> to write the affected keys for `index --force`"),
    }

    // Which content types dominate the affected set is the practical question
    // for triage — PDFs and HTML fail differently.
    let by_ext = affected.iter().fold(BTreeMap::new(), |mut m: BTreeMap<&str, usize>, v| {
        let ext = if v.s3_key.ends_with(".warc.gz") { ".warc.gz" } else { "other" };
        *m.entry(ext).or_default() += 1;
        m
    });
    debug!(?by_ext, "affected objects by extension");

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
    use super::*;

    fn warc_record(http_block: &[u8]) -> Vec<u8> {
        let mut out = b"WARC/1.0\r\nWARC-Type: response\r\n\r\n".to_vec();
        out.extend_from_slice(http_block);
        out
    }

    #[test]
    fn a_plain_capture_needs_no_peeling() {
        let block = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n<html>Apfel</html>";
        assert!(!needs_peeling(&warc_record(block)));
    }

    #[test]
    fn a_chunked_or_gzipped_capture_needs_peeling() {
        let plain = b"<html>Apfel</html>";

        let mut chunked = b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n".to_vec();
        chunked.extend_from_slice(format!("{:x}\r\n", plain.len()).as_bytes());
        chunked.extend_from_slice(plain);
        chunked.extend_from_slice(b"\r\n0\r\n\r\n");
        assert!(needs_peeling(&warc_record(&chunked)));

        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(plain).unwrap();
        let mut gz = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n".to_vec();
        gz.extend_from_slice(&e.finish().unwrap());
        assert!(needs_peeling(&warc_record(&gz)));
    }

    #[test]
    fn a_header_that_lies_is_not_affected() {
        // Content-Encoding on an already-decoded body: the old indexer read
        // this correctly, so re-indexing it would change nothing.
        let block = b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\n\r\n<html>Apfel</html>";
        assert!(!needs_peeling(&warc_record(block)));
    }

    #[test]
    fn extrapolation_scales_with_the_sample() {
        let v = ObjectVerdict {
            s3_key: "a.warc.gz".into(), indexable: 1000,
            sampled: 4, affected: 1, unreadable: 0,
        };
        assert_eq!(v.estimated_bad_docs(), 250);
        assert!(v.is_affected());

        let none = ObjectVerdict { affected: 0, ..v };
        assert_eq!(none.estimated_bad_docs(), 0);
        assert!(!none.is_affected());
    }
}
