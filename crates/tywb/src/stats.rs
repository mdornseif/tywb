//! `tywb stats` — print a summary of the current index state.

use anyhow::Context;

use warc_search_cdx::CdxStore;
use warc_search_config::Config;
use warc_search_s3::{ListState, default_state_path};
use warc_search_search::SearchReader;

pub fn run(cfg: Config) -> anyhow::Result<()> {
    // ── CDX (SQLite) ──────────────────────────────────────────────────────────

    let cdx = CdxStore::open(&cfg.storage.cdx_db_path)
        .with_context(|| format!("opening CDX store at {}", cfg.storage.cdx_db_path))?;
    let cs = cdx.stats().context("reading CDX stats")?;

    // ── Fulltext index (Tantivy) ──────────────────────────────────────────────

    let search = SearchReader::open(&cfg.storage.index_path)
        .with_context(|| format!("opening search index at {}", cfg.storage.index_path))?;
    let num_docs = search.num_docs();

    // ── Ingest state (list_state.json) ────────────────────────────────────────

    let state_path = default_state_path(&cfg.storage.cdx_db_path);
    let state = ListState::load(&state_path).unwrap_or_default();
    let files_seen = state.seen.len();

    // ── Output ────────────────────────────────────────────────────────────────

    println!("CDX index  ({})", cfg.storage.cdx_db_path);
    println!("  Records:      {}", fmt_count(cs.total_records));
    println!("  Unique URLs:  {}", fmt_count(cs.unique_urls));
    println!("  WARC files:   {}", fmt_count(cs.warc_files));
    println!("  Date range:   {}",
        match (&cs.oldest_timestamp, &cs.newest_timestamp) {
            (Some(a), Some(b)) => format!("{} → {}", fmt_ts(a), fmt_ts(b)),
            _ => "(empty)".to_owned(),
        }
    );

    if !cs.mime_counts.is_empty() {
        println!();
        println!("  MIME types:");
        for (mime, n) in &cs.mime_counts {
            println!("    {:40} {:>10}", mime, fmt_count(*n));
        }
    }

    if !cs.status_counts.is_empty() {
        println!();
        println!("  HTTP status:");
        for (status, n) in &cs.status_counts {
            let label = match status {
                Some(s) => s.to_string(),
                None    => "(none)".to_owned(),
            };
            println!("    {:6} {:>10}", label, fmt_count(*n));
        }
    }

    println!();
    println!("Fulltext index  ({})", cfg.storage.index_path);
    println!("  Documents:    {}", fmt_count(num_docs as u64));

    println!();
    println!("Ingest state  ({})", state_path.display());
    println!("  Files seen:   {}", fmt_count(files_seen as u64));

    Ok(())
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Format a large integer with thousands separators.
fn fmt_count(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { out.push(','); }
        out.push(ch);
    }
    out.chars().rev().collect()
}

/// Format a 14-digit CDX timestamp as `YYYY-MM-DD HH:MM:SS`.
fn fmt_ts(ts: &str) -> String {
    if ts.len() != 14 { return ts.to_owned(); }
    format!(
        "{}-{}-{} {}:{}:{}",
        &ts[0..4], &ts[4..6], &ts[6..8],
        &ts[8..10], &ts[10..12], &ts[12..14]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fmt_count_small() {
        assert_eq!(fmt_count(0), "0");
        assert_eq!(fmt_count(999), "999");
    }

    #[test]
    fn fmt_count_thousands() {
        assert_eq!(fmt_count(1_000), "1,000");
        assert_eq!(fmt_count(1_234_567), "1,234,567");
    }

    #[test]
    fn fmt_ts_formats_correctly() {
        assert_eq!(fmt_ts("20240315120000"), "2024-03-15 12:00:00");
    }

    #[test]
    fn fmt_ts_invalid_passthrough() {
        assert_eq!(fmt_ts("bad"), "bad");
    }
}
