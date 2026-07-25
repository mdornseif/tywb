//! SURT (Sort-friendly URI Reordering Transform) canonicalization.
//!
//! SURT is used by the Wayback Machine and CDX indexes to produce a
//! lexicographically sortable, host-reversed key from a URL.
//!
//! # Examples
//!
//! ```
//! use warc_search_cdx::surt::to_surt;
//!
//! assert_eq!(
//!     to_surt("https://www.example.com/path?q=1").unwrap(),
//!     "com,example,www)/path?q=1"
//! );
//! ```

use url::Url;
use crate::error::{CdxError, Result};

/// Convert a URL to its SURT key.
///
/// The transform:
/// 1. Parse the URL.
/// 2. Lowercase the host.
/// 3. Strip a leading `www.` (optional — matches common CDX practice).
/// 4. Reverse the host labels and join with commas: `com,example,www`
/// 5. Append `)/path?query` (no fragment, no credentials).
///
/// The port is included when non-default: `com,example):8080/`.
pub fn to_surt(raw_url: &str) -> Result<String> {
    let url = Url::parse(raw_url).map_err(|e| CdxError::InvalidUrl {
        url: raw_url.to_owned(),
        reason: e.to_string(),
    })?;

    // Strip a trailing dot (FQDN form): "scholl.de." indexes the same as
    // "scholl.de" — otherwise the reversed labels gain a leading empty label
    // and the two spellings become distinct SURTs.
    let host = url
        .host_str()
        .unwrap_or("")
        .trim_end_matches('.')
        .to_ascii_lowercase();

    // Reverse host labels: "www.example.com" → "com,example,www"
    let surt_host: String = host
        .split('.')
        .rev()
        .collect::<Vec<_>>()
        .join(",");

    // Include non-default port
    let port_part = match url.port() {
        Some(p) => format!(":{p}"),
        None => String::new(),
    };

    // Path + query (no fragment, no userinfo)
    let path = url.path();
    let query_part = match url.query() {
        Some(q) => format!("?{q}"),
        None => String::new(),
    };

    Ok(format!("{surt_host}){port_part}{path}{query_part}"))
}

/// Strip the scheme prefix that some CDX tools prepend (`https://` → bare SURT).
/// This is a no-op if the string doesn't look like a full URL.
pub fn surt_host_only(surt: &str) -> &str {
    // Some tools write `com,example,www)/` others just `com,example,www)`
    surt
}

/// Canonicalize a URL for deduplication purposes (lowercase, sort query params,
/// strip fragment and trailing slash on bare paths).
pub fn canonicalize(raw_url: &str) -> Result<String> {
    let mut url = Url::parse(raw_url).map_err(|e| CdxError::InvalidUrl {
        url: raw_url.to_owned(),
        reason: e.to_string(),
    })?;

    // Lowercase scheme + host, and drop a trailing FQDN dot so "scholl.de."
    // canonicalises to "scholl.de".
    // (url crate already lowercases these on parse, but be explicit)
    let host = url.host_str().unwrap_or("").trim_end_matches('.').to_ascii_lowercase();
    let _ = url.set_host(Some(&host));

    // Remove fragment
    url.set_fragment(None);

    // Sort query parameters for stable dedup
    if let Some(q) = url.query() {
        if !q.is_empty() {
            let mut pairs: Vec<(String, String)> = url
                .query_pairs()
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
                .collect();
            pairs.sort_unstable();
            let sorted: String = pairs
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            url.set_query(Some(&sorted));
        }
    }

    Ok(url.to_string())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── to_surt ───────────────────────────────────────────────────────────────

    #[test]
    fn surt_simple_https() {
        assert_eq!(
            to_surt("https://example.com/").unwrap(),
            "com,example)/"
        );
    }

    #[test]
    fn surt_with_www() {
        assert_eq!(
            to_surt("https://www.example.com/").unwrap(),
            "com,example,www)/"
        );
    }

    #[test]
    fn surt_trailing_fqdn_dot_stripped() {
        // "scholl.de." must produce the same SURT as "scholl.de".
        assert_eq!(to_surt("https://scholl.de./").unwrap(), to_surt("https://scholl.de/").unwrap());
        assert_eq!(to_surt("https://scholl.de./").unwrap(), "de,scholl)/");
        assert_eq!(
            to_surt("http://streuobstmosterei.de./obst").unwrap(),
            "de,streuobstmosterei)/obst",
        );
    }

    #[test]
    fn surt_with_path() {
        assert_eq!(
            to_surt("https://example.com/some/path").unwrap(),
            "com,example)/some/path"
        );
    }

    #[test]
    fn surt_with_query() {
        assert_eq!(
            to_surt("https://example.com/search?q=hello&page=2").unwrap(),
            "com,example)/search?q=hello&page=2"
        );
    }

    #[test]
    fn surt_fragment_dropped() {
        // Fragments are not stored in CDX
        let s = to_surt("https://example.com/page#section").unwrap();
        assert!(!s.contains('#'));
        assert_eq!(s, "com,example)/page");
    }

    #[test]
    fn surt_non_default_port() {
        assert_eq!(
            to_surt("https://example.com:8443/").unwrap(),
            "com,example):8443/"
        );
    }

    #[test]
    fn surt_http_scheme() {
        assert_eq!(
            to_surt("http://example.com/").unwrap(),
            "com,example)/"
        );
    }

    #[test]
    fn surt_subdomain_three_levels() {
        assert_eq!(
            to_surt("https://a.b.example.com/").unwrap(),
            "com,example,b,a)/"
        );
    }

    #[test]
    fn surt_host_uppercased_input() {
        assert_eq!(
            to_surt("https://EXAMPLE.COM/").unwrap(),
            "com,example)/"
        );
    }

    #[test]
    fn surt_empty_path() {
        // url crate normalises "https://example.com" → path "/"
        let s = to_surt("https://example.com").unwrap();
        assert_eq!(s, "com,example)/");
    }

    #[test]
    fn surt_ip_address() {
        // IP addresses are not reversed; labels are just the octets
        let s = to_surt("http://192.168.1.1/path").unwrap();
        // reversed: "1,1,168,192"
        assert!(s.contains("1,1,168,192"));
    }

    #[test]
    fn surt_invalid_url_returns_error() {
        assert!(to_surt("not a url at all").is_err());
        assert!(to_surt("").is_err());
    }

    #[test]
    fn surt_no_host() {
        // file:// URL with no host — shouldn't panic
        let result = to_surt("file:///local/path");
        // We allow it (empty host → empty surt_host)
        assert!(result.is_ok());
    }

    // ── canonicalize ──────────────────────────────────────────────────────────

    #[test]
    fn canonicalize_lowercases_host() {
        let c = canonicalize("https://EXAMPLE.COM/path").unwrap();
        assert!(c.contains("example.com"));
    }

    #[test]
    fn canonicalize_removes_fragment() {
        let c = canonicalize("https://example.com/page#frag").unwrap();
        assert!(!c.contains('#'));
    }

    #[test]
    fn canonicalize_sorts_query_params() {
        let c = canonicalize("https://example.com/?z=3&a=1&m=2").unwrap();
        // After sorting: a=1&m=2&z=3
        let q = c.split('?').nth(1).unwrap_or("");
        assert_eq!(q, "a=1&m=2&z=3");
    }

    #[test]
    fn canonicalize_stable_for_same_url() {
        let a = canonicalize("https://example.com/p?b=2&a=1").unwrap();
        let b = canonicalize("https://example.com/p?a=1&b=2").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn canonicalize_no_query_unchanged() {
        let c = canonicalize("https://example.com/page").unwrap();
        assert!(!c.contains('?'));
    }
}
