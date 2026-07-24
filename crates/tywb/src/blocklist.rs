//! Query-time URL blocklist for search results.
//!
//! A static text file of URL prefixes that should not appear in `/search` or
//! `/ui/search` output. This is distinct from `indexer.blacklisted_domains`,
//! which removes data from the index at ingest time: entries here only *hide*
//! matching results at query time — the records stay indexed and replayable, so
//! the block can be changed by editing the file and restarting the server, with
//! no re-index.
//!
//! # File format
//!
//! One URL prefix per line. Blank lines and lines starting with `#` are ignored,
//! and trailing `#` comments are stripped. A prefix matches a result URL when
//! the URL equals it or continues with `/`, `?`, or `#` — so
//! `https://duckduckgo.com` blocks the whole site but not
//! `https://duckduckgo.company.example`. Matching is case-insensitive on the
//! ASCII portion.

use tracing::{info, warn};

/// A set of URL prefixes to suppress from search results.
#[derive(Debug, Default, Clone)]
pub struct UrlBlocklist {
    prefixes: Vec<String>, // stored lower-cased
}

impl UrlBlocklist {
    /// Load from an optional file path. A missing path or unreadable file yields
    /// an empty (allow-everything) blocklist with a warning — a broken block
    /// file must never take the search endpoint down.
    pub fn load(path: Option<&str>) -> Self {
        let Some(path) = path else {
            return Self::default();
        };
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let list = Self::parse(&text);
                info!(path, entries = list.prefixes.len(), "loaded search blocklist");
                list
            }
            Err(e) => {
                warn!(path, err = %e, "could not read search blocklist — allowing all URLs");
                Self::default()
            }
        }
    }

    /// Parse blocklist text (exposed for tests).
    pub fn parse(text: &str) -> Self {
        let mut prefixes = Vec::new();
        for line in text.lines() {
            // Strip a trailing comment, then trim.
            let entry = line.split('#').next().unwrap_or("").trim();
            if entry.is_empty() {
                continue;
            }
            prefixes.push(entry.to_ascii_lowercase());
        }
        Self { prefixes }
    }

    #[allow(dead_code)] // used in tests and available to callers
    pub fn is_empty(&self) -> bool {
        self.prefixes.is_empty()
    }

    #[allow(dead_code)] // used in tests and available to callers
    pub fn len(&self) -> usize {
        self.prefixes.len()
    }

    /// Is `url` blocked by any prefix?
    pub fn is_blocked(&self, url: &str) -> bool {
        if self.prefixes.is_empty() {
            return false;
        }
        let url = url.to_ascii_lowercase();
        self.prefixes.iter().any(|p| prefix_matches(&url, p))
    }
}

/// True if `prefix` covers `url`: an exact match, or `url` continues past the
/// prefix at a URL boundary (`/`, `?`, `#`). A prefix that already ends in `/`
/// matches by plain `starts_with`.
fn prefix_matches(url: &str, prefix: &str) -> bool {
    if !url.starts_with(prefix) {
        return false;
    }
    match url[prefix.len()..].chars().next() {
        None => true,                                  // exact match
        Some(_) if prefix.ends_with('/') => true,      // prefix already a boundary
        Some(c) => matches!(c, '/' | '?' | '#'),       // boundary char follows
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bl(lines: &str) -> UrlBlocklist {
        UrlBlocklist::parse(lines)
    }

    #[test]
    fn blocks_the_whole_site_from_a_host_prefix() {
        let b = bl("https://duckduckgo.com");
        assert!(b.is_blocked("https://duckduckgo.com"));
        assert!(b.is_blocked("https://duckduckgo.com/"));
        assert!(b.is_blocked("https://duckduckgo.com/?q=apples"));
        assert!(b.is_blocked("https://duckduckgo.com/html/foo"));
    }

    #[test]
    fn does_not_block_a_lookalike_host() {
        let b = bl("https://duckduckgo.com");
        assert!(!b.is_blocked("https://duckduckgo.company.example/"));
        assert!(!b.is_blocked("https://notduckduckgo.com/"));
        // Different scheme is a different prefix — only https was listed.
        assert!(!b.is_blocked("http://duckduckgo.com/"));
    }

    #[test]
    fn case_insensitive() {
        let b = bl("https://DuckDuckGo.com");
        assert!(b.is_blocked("https://duckduckgo.com/HTML"));
    }

    #[test]
    fn a_specific_url_blocks_only_that_page_and_below() {
        let b = bl("https://example.org/private/report.pdf");
        assert!(b.is_blocked("https://example.org/private/report.pdf"));
        assert!(!b.is_blocked("https://example.org/private/report.pdfx"));
        assert!(!b.is_blocked("https://example.org/private/"));
    }

    #[test]
    fn comments_and_blank_lines_ignored() {
        let b = bl("# block DDG\n\n  https://duckduckgo.com   # noisy\n\n");
        assert_eq!(b.len(), 1);
        assert!(b.is_blocked("https://duckduckgo.com/x"));
    }

    #[test]
    fn empty_blocklist_blocks_nothing() {
        let b = bl("# only comments\n\n");
        assert!(b.is_empty());
        assert!(!b.is_blocked("https://duckduckgo.com/"));
    }
}
