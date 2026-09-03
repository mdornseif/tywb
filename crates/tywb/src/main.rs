//! `tywb` — Tiny Wayback
//!
//! A resource-efficient WARC fulltext search engine and Wayback-compatible
//! replay server for WARC files stored on S3-compatible object storage.
//!
//! # Usage
//!
//! ```text
//! tywb [--config <path>] <COMMAND>
//!
//! Commands:
//!   index       Ingest WARC files from S3 into the fulltext and CDX indexes
//!   server      Run the HTTP search and replay server
//!   stats       Print statistics about the current index
//!   recompress  Rewrite whole-file-gzip WARCs as record-per-member .warc.gz
//!   scan-wire-format  Find WARC files needing a re-index after the wire-format fix
//! ```

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::info;

use warc_search_config::Config;

mod gz_warc;
mod http_payload;
mod index;
mod ocr_cache;
mod ocr_worker;
mod pdf;
mod pdf_collection;
mod recompress;
mod record_fetch;
mod server;
mod skiplist;
mod stats;
mod ui;
mod wire_scan;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(
    name    = "tywb",
    about   = "Tiny Wayback — WARC search and Wayback-compatible replay",
    version,
)]
struct Cli {
    /// Path to the configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Index WARC files from S3 into the fulltext and CDX indexes.
    Index {
        /// Only index this specific S3 key (bypasses S3 listing).
        #[arg(long)]
        file: Option<String>,
        /// Stop after processing this many WARC files.
        #[arg(long)]
        max_files: Option<usize>,
        /// Stop after this many new CDX entries have been written.
        #[arg(long)]
        max_urls: Option<u64>,
        /// Re-process all WARC files even if their ETag matches the saved state.
        /// Use this to update existing CDX records (e.g. after a bug fix).
        #[arg(long)]
        force: bool,
        /// Index only the extra collections, skipping the primary WARC bucket.
        #[arg(long)]
        collections_only: bool,
    },
    /// Run the HTTP search and replay server.
    Server,
    /// Print statistics about the current index.
    Stats,
    /// Find WARC files whose captures need re-indexing after the wire-format fix.
    ///
    /// Samples a few records per WARC object and Range-GETs just those, then
    /// reports which objects hold bodies that the old indexer read as noise
    /// (chunked framing or a Content-Encoding it never undid). Read-only.
    ScanWireFormat {
        /// Records sampled per WARC object.
        #[arg(long, default_value_t = 3)]
        sample: usize,
        /// Stop after this many objects (most records first).
        #[arg(long)]
        limit: Option<usize>,
        /// Objects sampled concurrently.
        #[arg(long, default_value_t = 8)]
        jobs: usize,
        /// Write the affected S3 keys here, one per line, for `index --force`.
        #[arg(long)]
        out: Option<PathBuf>,
        /// Print a line per affected object, not just the summary.
        #[arg(long)]
        verbose: bool,
    },
    /// Rewrite whole-file-gzip WARCs as record-per-member `.warc.gz`.
    ///
    /// Some producers gzip an entire WARC as one deflate stream instead of one
    /// member per record.  Such files cannot be indexed or replayed record by
    /// record.  This rewrites them losslessly, keeping the original as
    /// `<key>.bak`.  Re-index the affected files afterwards (`index --force`).
    Recompress {
        /// Only process these S3 keys (repeatable, bypasses bucket scanning).
        #[arg(long = "file")]
        files: Vec<String>,
        /// Process at most this many objects (smallest first).
        #[arg(long)]
        limit: Option<usize>,
        /// Objects processed concurrently.
        #[arg(long, default_value_t = 2)]
        jobs: usize,
        /// Scratch directory for downloads and rewrites.
        #[arg(long, default_value = "/var/tmp/tywb-recompress")]
        workdir: PathBuf,
        /// Verify everything but never write to S3.
        #[arg(long)]
        dry_run: bool,
        /// Only report which objects would be rewritten, then stop.
        #[arg(long)]
        scan_only: bool,
        /// Suffix under which the original object is preserved.
        #[arg(long, default_value = ".bak")]
        backup_suffix: String,
        /// Also rewrite sources whose gzip stream or last record is cut off:
        /// every complete record before the break is kept, the incomplete tail
        /// is dropped. Without this such files are reported and left alone.
        #[arg(long)]
        salvage_truncated: bool,
    },
    /// Drain the OCR queue: extract PDF text the index run deferred and store
    /// it in the digest cache for the next index run.
    OcrWorker {
        /// Before draining, walk the CDX index and queue every WARC PDF whose
        /// text is not in the cache yet — so the next rebuild starts warm.
        #[arg(long)]
        prefill: bool,
        /// Documents extracted in parallel (default: indexer.ocr_cache.workers).
        #[arg(long)]
        jobs: Option<usize>,
    },
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let cfg = Config::load(&cli.config)
        .unwrap_or_else(|e| {
            eprintln!("error loading config {}: {e}", cli.config.display());
            std::process::exit(1);
        });

    // stats writes directly to stdout — skip tracing noise.
    if matches!(cli.command, Command::Stats) {
        return stats::run(cfg).map_err(Into::into);
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cfg.log.level.as_str().into()),
        )
        .init();

    info!(config = %cli.config.display(), "tywb starting");

    // Single skip list, two halves: domains and URL patterns. Merge both static
    // files into the in-memory config. Whatever ends up here drives ingest-time
    // skipping, the index-start purge and the server's query-time display
    // filter alike.
    let mut cfg = cfg;
    match cfg.indexer.load_blacklist_file() {
        Ok(0) => {}
        Ok(n) => info!(added = n, path = ?cfg.indexer.blacklisted_domains_path, "loaded domain skip list"),
        Err(e) => tracing::warn!(err = %e, path = ?cfg.indexer.blacklisted_domains_path,
                                 "could not read domain skip list — continuing without it"),
    }
    match cfg.indexer.load_url_patterns_file() {
        Ok(0) => {}
        Ok(n) => info!(added = n, path = ?cfg.indexer.blacklisted_url_patterns_path,
                       "loaded URL skip patterns"),
        Err(e) => tracing::warn!(err = %e, path = ?cfg.indexer.blacklisted_url_patterns_path,
                                 "could not read URL skip patterns — continuing without them"),
    }
    // Nothing filters until the patterns are compiled. A rejected pattern is
    // logged rather than fatal, but logged loudly: silently dropping one means
    // cruft keeps flowing into the index and nobody notices.
    let patterns = cfg.indexer.compile_url_patterns();
    for (pattern, reason) in &patterns.rejected {
        tracing::warn!(%pattern, %reason, "ignoring invalid URL skip pattern");
    }
    if patterns.compiled > 0 {
        info!(patterns = patterns.compiled, "URL skip patterns active");
    }

    match cli.command {
        Command::Index { file, max_files, max_urls, force, collections_only } =>
            index::run(cfg, index::IndexArgs { file, max_files, max_urls, force, collections_only }).await,
        Command::Server => server::run(cfg).await,
        Command::Stats  => unreachable!(),
        Command::ScanWireFormat { sample, limit, jobs, out, verbose } =>
            wire_scan::run(cfg, wire_scan::ScanArgs { sample, limit, jobs, out, verbose }).await,
        Command::Recompress {
            files, limit, jobs, workdir, dry_run, scan_only, backup_suffix, salvage_truncated,
        } =>
            recompress::run(cfg, recompress::RecompressArgs {
                files, limit, jobs, workdir, dry_run, scan_only, backup_suffix, salvage_truncated,
            }).await,
        Command::OcrWorker { prefill, jobs } =>
            ocr_worker::run(cfg, ocr_worker::WorkerArgs { prefill, jobs }).await,
    }
}
