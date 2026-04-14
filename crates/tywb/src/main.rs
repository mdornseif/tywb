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
//!   index   Ingest WARC files from S3 into the fulltext and CDX indexes
//!   server  Run the HTTP search and replay server
//! ```

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use tracing::info;

use warc_search_config::Config;

mod gz_warc;
mod index;
mod server;
mod stats;
mod ui;

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
    },
    /// Run the HTTP search and replay server.
    Server,
    /// Print statistics about the current index.
    Stats,
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

    match cli.command {
        Command::Index { file, max_files, max_urls, force } =>
            index::run(cfg, index::IndexArgs { file, max_files, max_urls, force }).await,
        Command::Server => server::run(cfg).await,
        Command::Stats  => unreachable!(),
    }
}
