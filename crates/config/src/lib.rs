//! Configuration loading for warc-search.
//!
//! Values are resolved with the following precedence (highest first):
//!
//! 1. Environment variables (see mapping below)
//! 2. `config.yaml` (or the path given to [`Config::load`])
//! 3. Compiled-in defaults
//!
//! ## AWS credential precedence
//!
//! The AWS SDK itself handles credential resolution; we just feed it what we
//! find.  Precedence inside this crate:
//!
//! 1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` env vars
//! 2. `s3.access_key_id` / `s3.secret_access_key` in config.yaml
//! 3. Standard SDK fallback (~/.aws/credentials, instance metadata, etc.)
//!    — signalled by returning `None` from the typed accessors.
//!
//! ## Environment variable mapping
//!
//! | Environment variable        | Config field                  |
//! |-----------------------------|-------------------------------|
//! | `AWS_ACCESS_KEY_ID`         | `s3.access_key_id`            |
//! | `AWS_SECRET_ACCESS_KEY`     | `s3.secret_access_key`        |
//! | `AWS_DEFAULT_REGION`        | `s3.region`                   |
//! | `AWS_ENDPOINT_URL`          | `s3.endpoint_url`             |
//! | `WARC_S3_BUCKET`            | `s3.bucket`                   |
//! | `WARC_S3_PREFIX`            | `s3.prefix`                   |
//! | `WARC_S3_CONCURRENCY`       | `s3.concurrency`              |
//! | `WARC_INDEX_PATH`           | `storage.index_path`          |
//! | `WARC_CDX_DB_PATH`          | `storage.cdx_db_path`         |
//! | `WARC_SERVER_BIND`          | `server.bind`                 |
//! | `RUST_LOG`                  | `log.level`                   |

use std::path::Path;
use regex::{Regex, RegexSet};
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not read config file {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },
    #[error("could not parse YAML config: {0}")]
    ParseYaml(#[from] serde_yaml::Error),

    #[error("invalid integer in environment variable {var}: {value:?}")]
    InvalidEnvInt { var: &'static str, value: String },
}

pub type Result<T> = std::result::Result<T, ConfigError>;

// ── Sub-configs ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub bucket: String,
    #[serde(default = "default_region")]
    pub region: String,
    #[serde(default)]
    pub endpoint_url: Option<String>,
    #[serde(default)]
    pub force_path_style: bool,
    /// Credentials — prefer env vars; these are a fallback.
    #[serde(default)]
    pub access_key_id: Option<String>,
    #[serde(default)]
    pub secret_access_key: Option<String>,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    #[serde(default = "default_index_path")]
    pub index_path: String,
    #[serde(default = "default_cdx_db_path")]
    pub cdx_db_path: String,
    #[serde(default = "default_sqlite_cache_kib")]
    pub sqlite_cache_kib: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexerConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_max_text_bytes")]
    pub max_text_bytes: usize,
    #[serde(default = "default_true")]
    pub index_pdfs: bool,
    #[serde(default = "default_true")]
    pub index_warc_responses: bool,
    /// Domains (and all their subdomains) to exclude from indexing and remove
    /// from any existing index.  Plain hostnames without scheme or path, e.g.
    /// `example.com`.  Subdomains are matched automatically: listing `example.com`
    /// also excludes `www.example.com`, `cdn.example.com`, etc.
    #[serde(default)]
    pub blacklisted_domains: Vec<String>,
    /// Optional path to a static file of domains to skip, one per line
    /// (`#` comments allowed). Merged into [`blacklisted_domains`] at load time,
    /// so it applies at both index time (skip + purge) and — via the server's
    /// query-time filter — in search results. Editable without re-rendering
    /// config.yaml; a re-index purges already-stored entries.
    #[serde(default)]
    pub blacklisted_domains_path: Option<String>,
    /// Regular expressions matched against the whole URL. The second half of the
    /// skip list: where [`blacklisted_domains`] removes a *site*, these remove a
    /// *kind of page* on sites that are otherwise wanted — wiki talk pages,
    /// version histories, `action=edit` links and the rest of the machinery a
    /// CMS hangs off every article.
    ///
    /// Syntax is the `regex` crate's (RE2-compatible: no backreferences, no
    /// lookaround), so the same expressions also work as Zeno exclusions —
    /// which is what the server's `/skiplist.zeno` serves. Prefix `(?i)` for
    /// case-insensitivity. Patterns are unanchored: they match anywhere in the
    /// URL unless you write `^`.
    ///
    /// Compile with [`compile_url_patterns`] before the list has any effect.
    ///
    /// [`blacklisted_domains`]: Self::blacklisted_domains
    /// [`compile_url_patterns`]: Self::compile_url_patterns
    #[serde(default)]
    pub blacklisted_url_patterns: Vec<String>,
    /// Optional path to a static file of URL patterns, one regex per line.
    /// Merged into [`blacklisted_url_patterns`] by [`load_url_patterns_file`].
    ///
    /// Only whole-line `#` comments are recognised — unlike the domain file, a
    /// trailing `#` is *not* stripped, because `#` is a legitimate regex
    /// character (`[?&#]`).
    ///
    /// [`blacklisted_url_patterns`]: Self::blacklisted_url_patterns
    /// [`load_url_patterns_file`]: Self::load_url_patterns_file
    #[serde(default)]
    pub blacklisted_url_patterns_path: Option<String>,
    /// Compiled form of [`blacklisted_url_patterns`], built once at startup by
    /// [`compile_url_patterns`]. A `RegexSet` tests every pattern in a single
    /// pass over the URL, which matters: this runs per record over multi-GB
    /// WARCs. Never serialized — it is derived state.
    ///
    /// [`blacklisted_url_patterns`]: Self::blacklisted_url_patterns
    /// [`compile_url_patterns`]: Self::compile_url_patterns
    #[serde(skip)]
    url_patterns: Option<RegexSet>,
    /// The pattern file exactly as read, minus its header: comments, blank
    /// lines and patterns in their original order. Kept so the list can be
    /// handed on — to a crawler, to the UI — with the reasoning that came with
    /// it. An exclusion file whose rules nobody can explain is one nobody dares
    /// to edit. Empty when no file was loaded.
    #[serde(skip)]
    url_pattern_source: Vec<String>,
    /// Optional Apache Tika backend for extracting text from PDFs. When unset,
    /// PDFs are not fulltext-indexed (they remain browsable and replayable) —
    /// this keeps the dependency-free deployment possible. See [`TikaConfig`].
    #[serde(default)]
    pub tika: Option<TikaConfig>,
    /// Additional named sources indexed alongside the primary WARC bucket
    /// (`s3.bucket`, collection `"warc"`). Currently used for buckets of
    /// standalone PDFs. See [`CollectionConfig`].
    #[serde(default)]
    pub collections: Vec<CollectionConfig>,
}

/// An additional indexed source beyond the primary WARC archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionConfig {
    /// Unique collection name, stored on every CDX record and used to pick the
    /// bucket at replay time. Must not be `"warc"` (the primary archive).
    pub name: String,
    /// Source kind. `"pdf_bucket"` = a bucket of standalone PDF objects, each
    /// indexed and replayed directly (no WARC container).
    #[serde(rename = "type", default = "default_collection_type")]
    pub kind: String,
    /// S3 bucket holding the collection's objects. Uses the same endpoint and
    /// credentials as the primary `s3` block.
    pub bucket: String,
    /// Optional key prefix to restrict which objects are indexed.
    #[serde(default)]
    pub prefix: Option<String>,
    /// Optional regex over the object key, applied on top of [`prefix`].
    ///
    /// A prefix can only describe a contiguous run of keys, and material worth
    /// separating is not always stored that way: a digitisation whose volumes
    /// each live in their own directory shares its prefix with everything else
    /// under it. The pattern names the rest of the rule.
    ///
    /// Same RE2 syntax as the URL skip list. Objects that do not match are left
    /// untouched — *not* recorded as seen — so a later collection over the same
    /// prefix still gets them. That is what lets one prefix be split between
    /// two collections by listing the narrower one first.
    ///
    /// [`prefix`]: Self::prefix
    #[serde(default)]
    pub key_pattern: Option<String>,
    /// For `pdf_bucket`: the public base URL an object is reachable at. The
    /// record's original URL is `public_base_url` + key, e.g.
    /// `https://obst-pdfs.23.nu/` + `1152-hochstamm.pdf`.
    #[serde(default)]
    pub public_base_url: Option<String>,
    /// Per-collection text-extraction settings, layered over the global
    /// [`IndexerConfig::tika`]. A collection is a body of documents with
    /// properties of its own — one that arrives pre-OCR'd wants `no_ocr` and a
    /// generous size limit, while the web archive around it wants neither.
    /// Without this the two had to share one setting, and the compromise was
    /// wrong for both. See [`TikaOverride`].
    #[serde(default)]
    pub tika: Option<TikaOverride>,
}

impl CollectionConfig {
    /// Compile [`key_pattern`], if set.
    ///
    /// An error here must stop the collection rather than be logged and
    /// ignored: without the pattern the collection covers its whole prefix, and
    /// for a rule that exists to *narrow* a prefix that is the opposite of what
    /// was asked for — it would index, and possibly OCR, everything.
    ///
    /// [`key_pattern`]: Self::key_pattern
    pub fn compile_key_pattern(&self) -> std::result::Result<Option<Regex>, regex::Error> {
        self.key_pattern.as_deref().map(Regex::new).transpose()
    }
}

/// Text-extraction settings for one collection.
///
/// Every field is optional and unset means "as configured globally": this
/// adjusts *how* documents are parsed, never *where* — the Tika server is one
/// service, and its URL stays in [`IndexerConfig::tika`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TikaOverride {
    /// `no_ocr`, `auto` or `ocr_only` for this collection's documents.
    #[serde(default)]
    pub ocr_strategy: Option<String>,
    /// Tesseract languages, e.g. `deu+frk` for a German Fraktur corpus.
    #[serde(default)]
    pub ocr_languages: Option<String>,
    /// Size ceiling for this collection. Library digitisations run to hundreds
    /// of megabytes where ordinary web PDFs do not.
    #[serde(default)]
    pub max_pdf_bytes: Option<usize>,
    /// Per-document timeout. Must stay below Tika's own `taskTimeoutMillis`, or
    /// the server kills its parser first and the client sees a broken
    /// connection instead of a deadline.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

impl TikaConfig {
    /// This configuration with a collection's overrides applied.
    ///
    /// Unset fields keep the global value, so a collection states only what is
    /// special about it.
    pub fn with_override(&self, o: &TikaOverride) -> TikaConfig {
        TikaConfig {
            url:           self.url.clone(),
            ocr_strategy:  o.ocr_strategy.clone().unwrap_or_else(|| self.ocr_strategy.clone()),
            ocr_languages: o.ocr_languages.clone().unwrap_or_else(|| self.ocr_languages.clone()),
            max_pdf_bytes: o.max_pdf_bytes.unwrap_or(self.max_pdf_bytes),
            timeout_secs:  o.timeout_secs.unwrap_or(self.timeout_secs),
        }
    }
}

/// Configuration for the optional Apache Tika text-extraction backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TikaConfig {
    /// Base URL of a running `tika-server`, e.g. `http://127.0.0.1:9998`.
    pub url: String,
    /// PDF OCR strategy passed to Tika as `X-Tika-PDFOcrStrategy`:
    /// `no_ocr` (text layer only), `auto` (OCR only when there is no text
    /// layer), or `ocr_only`. Default `auto`.
    #[serde(default = "default_tika_ocr_strategy")]
    pub ocr_strategy: String,
    /// Tesseract language(s) for OCR, `X-Tika-OCRLanguage`, e.g. `deu+frk+eng`.
    #[serde(default = "default_tika_ocr_languages")]
    pub ocr_languages: String,
    /// PDFs larger than this (uncompressed HTTP payload) are skipped rather than
    /// sent to Tika, to bound extraction time and JVM heap. Default 100 MiB.
    #[serde(default = "default_max_pdf_bytes")]
    pub max_pdf_bytes: usize,
    /// Per-document timeout for a Tika request, in seconds. OCR of a large scan
    /// can be slow, so this is generous. Default 300.
    #[serde(default = "default_tika_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for TikaConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            ocr_strategy:  default_tika_ocr_strategy(),
            ocr_languages: default_tika_ocr_languages(),
            max_pdf_bytes: default_max_pdf_bytes(),
            timeout_secs:  default_tika_timeout_secs(),
        }
    }
}

impl IndexerConfig {
    /// Merge the domains listed in [`blacklisted_domains_path`] into
    /// [`blacklisted_domains`]. One domain per line; blank lines and lines
    /// starting with `#` are ignored, as is a trailing `# comment`.
    ///
    /// This is the single source of the domain skip list: after this runs, the
    /// same set drives ingest-time skipping, the index-start purge, and the
    /// server's query-time display filter. A missing path is a no-op; an
    /// unreadable file returns the I/O error for the caller to log.
    ///
    /// [`blacklisted_domains_path`]: Self::blacklisted_domains_path
    /// [`blacklisted_domains`]: Self::blacklisted_domains
    pub fn load_blacklist_file(&mut self) -> std::io::Result<usize> {
        let Some(path) = self.blacklisted_domains_path.clone() else {
            return Ok(0);
        };
        let text = std::fs::read_to_string(&path)?;
        let existing: std::collections::HashSet<String> = self
            .blacklisted_domains
            .iter()
            .map(|d| d.trim().to_ascii_lowercase())
            .collect();
        let mut added = 0;
        let mut seen = existing;
        for line in text.lines() {
            let entry = line.split('#').next().unwrap_or("").trim().to_ascii_lowercase();
            if entry.is_empty() {
                continue;
            }
            if seen.insert(entry.clone()) {
                self.blacklisted_domains.push(entry);
                added += 1;
            }
        }
        Ok(added)
    }

    /// Merge the regexes listed in [`blacklisted_url_patterns_path`] into
    /// [`blacklisted_url_patterns`]. One pattern per line; blank lines and lines
    /// whose first non-space character is `#` are ignored.
    ///
    /// A trailing `# comment` is deliberately *not* stripped here (the domain
    /// file does strip it): `#` occurs inside real patterns, and silently
    /// truncating one would leave a shorter, broader regex behind.
    ///
    /// Nothing is compiled yet — call [`compile_url_patterns`] afterwards.
    /// A missing path is a no-op; an unreadable file returns the I/O error.
    ///
    /// [`blacklisted_url_patterns_path`]: Self::blacklisted_url_patterns_path
    /// [`blacklisted_url_patterns`]: Self::blacklisted_url_patterns
    /// [`compile_url_patterns`]: Self::compile_url_patterns
    pub fn load_url_patterns_file(&mut self) -> std::io::Result<usize> {
        let Some(path) = self.blacklisted_url_patterns_path.clone() else {
            return Ok(0);
        };
        let text = std::fs::read_to_string(&path)?;
        let mut seen: std::collections::HashSet<String> =
            self.blacklisted_url_patterns.iter().map(|p| p.trim().to_owned()).collect();
        let mut added = 0;
        // The file's header — the leading comment block, ending at the first
        // blank line — explains this file to whoever edits it and is not worth
        // carrying into an export. Everything after it, section headings
        // included, is.
        let mut in_header = true;
        for line in text.lines() {
            let entry = line.trim();
            if in_header {
                if entry.starts_with('#') {
                    continue;
                }
                in_header = false;
                if entry.is_empty() {
                    continue;
                }
            }
            self.url_pattern_source.push(entry.to_owned());
            if entry.is_empty() || entry.starts_with('#') {
                continue;
            }
            if seen.insert(entry.to_owned()) {
                self.blacklisted_url_patterns.push(entry.to_owned());
                added += 1;
            }
        }
        Ok(added)
    }

    /// Compile [`blacklisted_url_patterns`] into the set used by
    /// [`is_url_blacklisted`]. Call once at startup, after
    /// [`load_url_patterns_file`].
    ///
    /// Bad patterns are dropped, not fatal: a typo in a skip list must not stop
    /// the server from serving or the indexer from indexing. Every rejection is
    /// returned so the caller can log it — a silently ignored pattern is how a
    /// skip list rots.
    ///
    /// Patterns are compiled individually first, both to name the offender and
    /// to catch the one mistake that is worse than a typo: an expression that
    /// matches the empty string (`.*`, a stray `(?i)`) matches *every* URL, and
    /// would purge the whole archive on the next `tywb index`.
    ///
    /// [`blacklisted_url_patterns`]: Self::blacklisted_url_patterns
    /// [`is_url_blacklisted`]: Self::is_url_blacklisted
    /// [`load_url_patterns_file`]: Self::load_url_patterns_file
    pub fn compile_url_patterns(&mut self) -> UrlPatternReport {
        let mut report = UrlPatternReport::default();
        let mut good: Vec<String> = Vec::new();

        for pattern in &self.blacklisted_url_patterns {
            let p = pattern.trim();
            if p.is_empty() {
                continue;
            }
            match Regex::new(p) {
                Err(e) => report.rejected.push((p.to_owned(), e.to_string())),
                Ok(re) if re.is_match("") => report.rejected.push((
                    p.to_owned(),
                    "matches the empty string — would blacklist every URL".to_owned(),
                )),
                Ok(_) => good.push(p.to_owned()),
            }
        }

        self.url_patterns = if good.is_empty() {
            None
        } else {
            match RegexSet::new(&good) {
                Ok(set) => {
                    report.compiled = good.len();
                    Some(set)
                }
                // Every pattern compiled on its own above, so this is the set's
                // own size limit. Report it against the list as a whole.
                Err(e) => {
                    report.rejected.push(("<pattern set>".to_owned(), e.to_string()));
                    None
                }
            }
        };
        report
    }

    /// Return `true` if the URL matches one of the compiled URL skip patterns.
    /// Always `false` before [`compile_url_patterns`] has run.
    ///
    /// [`compile_url_patterns`]: Self::compile_url_patterns
    pub fn matches_url_pattern(&self, url: &str) -> bool {
        self.url_patterns.as_ref().is_some_and(|set| set.is_match(url))
    }

    /// The patterns actually in force — those that compiled. This is what to
    /// show and to export: [`blacklisted_url_patterns`] is the *request*, this
    /// is the answer, and they differ whenever a pattern was rejected.
    ///
    /// [`blacklisted_url_patterns`]: Self::blacklisted_url_patterns
    pub fn active_url_patterns(&self) -> &[String] {
        self.url_patterns.as_ref().map_or(&[], |set| set.patterns())
    }

    /// The pattern file as read — comments, blank lines and patterns in their
    /// original order, header stripped. Empty when the patterns came from
    /// `config.yaml` alone.
    pub fn url_pattern_source(&self) -> &[String] {
        &self.url_pattern_source
    }

    /// Return `true` if `host` (a bare hostname, lower-cased) is covered by
    /// the domain blacklist — i.e. it equals a blacklisted domain or is a
    /// subdomain of one.
    pub fn is_host_blacklisted(&self, host: &str) -> bool {
        let h = host.to_ascii_lowercase();
        for blocked in &self.blacklisted_domains {
            let b = blocked.trim().to_ascii_lowercase();
            if h == b || h.ends_with(&format!(".{b}")) {
                return true;
            }
        }
        false
    }

    /// Return `true` if the URL is covered by the skip list — either its host is
    /// a blacklisted domain, or the URL matches one of the compiled URL
    /// patterns. This is the single question asked at ingest, at purge time and
    /// by the server's display filter.
    ///
    /// The host check only applies to URLs with a scheme; the pattern check runs
    /// against whatever string it is given.
    pub fn is_url_blacklisted(&self, url: &str) -> bool {
        if !self.blacklisted_domains.is_empty() {
            // Fast path: extract host without full URL parsing.
            if let Some(pos) = url.find("://") {
                let host = url[pos + 3..]
                    .split(['/', ':', '?', '#'])
                    .next()
                    .unwrap_or("")
                    .to_ascii_lowercase();
                if self.is_host_blacklisted(&host) {
                    return true;
                }
            }
        }
        self.matches_url_pattern(url)
    }
}

/// What [`IndexerConfig::compile_url_patterns`] made of the configured URL
/// patterns.
#[derive(Debug, Default)]
pub struct UrlPatternReport {
    /// Number of patterns that compiled and are now in force.
    pub compiled: usize,
    /// `(pattern, reason)` for every pattern that was thrown away.
    pub rejected: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_max_results")]
    pub max_results: usize,
    #[serde(default = "default_true")]
    pub enable_replay: bool,
    #[serde(default)]
    pub static_dir: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

// ── Root config ───────────────────────────────────────────────────────────────

/// Every block here rejects keys it does not know (`deny_unknown_fields`).
///
/// A misplaced key is otherwise invisible: `tika:` or `collections:` written at
/// the top level instead of under `indexer:` is silently dropped, the run
/// reports `nothing to index — all objects are up to date`, and exits happily
/// having done nothing. Serde's error names the stray key and lists the ones
/// that block accepts, which is the whole diagnosis.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub s3: S3Config,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub indexer: IndexerConfig,
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub log: LogConfig,
}

impl Config {
    /// Load config from `path`, then apply environment variable overrides.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
            path: path.display().to_string(),
            source,
        })?;
        let mut cfg: Config = serde_yaml::from_str(&text)?;
        cfg.apply_env()?;
        Ok(cfg)
    }

    /// Load from a YAML string (useful for tests).
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let mut cfg: Config = serde_yaml::from_str(yaml)?;
        cfg.apply_env()?;
        Ok(cfg)
    }

    /// Apply environment variable overrides.  Called automatically by `load`
    /// and `from_yaml`.  Public so callers can re-apply after mutation.
    pub fn apply_env(&mut self) -> Result<()> {
        use std::env::var;

        // AWS credentials (standard names take priority)
        if let Ok(v) = var("AWS_ACCESS_KEY_ID") {
            self.s3.access_key_id = Some(v);
        }
        if let Ok(v) = var("AWS_SECRET_ACCESS_KEY") {
            self.s3.secret_access_key = Some(v);
        }
        if let Ok(v) = var("AWS_DEFAULT_REGION") {
            self.s3.region = v;
        }
        if let Ok(v) = var("AWS_ENDPOINT_URL") {
            self.s3.endpoint_url = Some(v);
        }

        // warc-search specific
        if let Ok(v) = var("WARC_S3_BUCKET") {
            self.s3.bucket = v;
        }
        if let Ok(v) = var("WARC_S3_PREFIX") {
            self.s3.prefix = Some(v);
        }
        if let Ok(v) = var("WARC_S3_CONCURRENCY") {
            self.s3.concurrency = parse_env_usize("WARC_S3_CONCURRENCY", &v)?;
        }
        if let Ok(v) = var("WARC_INDEX_PATH") {
            self.storage.index_path = v;
        }
        if let Ok(v) = var("WARC_CDX_DB_PATH") {
            self.storage.cdx_db_path = v;
        }
        if let Ok(v) = var("WARC_SERVER_BIND") {
            self.server.bind = v;
        }
        if let Ok(v) = var("RUST_LOG") {
            self.log.level = v;
        }

        Ok(())
    }

    /// Returns `(access_key_id, secret_access_key)` if explicitly configured.
    /// Returns `None` to let the AWS SDK use its own credential chain.
    pub fn explicit_credentials(&self) -> Option<(&str, &str)> {
        match (&self.s3.access_key_id, &self.s3.secret_access_key) {
            (Some(k), Some(s)) if !k.is_empty() && !s.is_empty() => {
                Some((k.as_str(), s.as_str()))
            }
            _ => None,
        }
    }
}

fn parse_env_usize(var: &'static str, value: &str) -> Result<usize> {
    value.trim().parse::<usize>().map_err(|_| ConfigError::InvalidEnvInt {
        var,
        value: value.to_owned(),
    })
}

// ── Default impls ─────────────────────────────────────────────────────────────

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            index_path: default_index_path(),
            cdx_db_path: default_cdx_db_path(),
            sqlite_cache_kib: default_sqlite_cache_kib(),
        }
    }
}
impl Default for IndexerConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            max_text_bytes: default_max_text_bytes(),
            index_pdfs: true,
            index_warc_responses: true,
            blacklisted_domains: vec![],
            blacklisted_domains_path: None,
            blacklisted_url_patterns: vec![],
            blacklisted_url_patterns_path: None,
            url_patterns: None,
            url_pattern_source: vec![],
            tika: None,
            collections: vec![],
        }
    }
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            max_results: default_max_results(),
            enable_replay: true,
            static_dir: None,
        }
    }
}
impl Default for LogConfig {
    fn default() -> Self {
        Self { level: default_log_level() }
    }
}

// ── Default value fns (required by serde) ────────────────────────────────────

fn default_region()         -> String  { "us-east-1".into() }
fn default_concurrency()    -> usize   { 4 }
fn default_index_path()     -> String  { "/var/lib/warc-search/index".into() }
fn default_cdx_db_path()    -> String  { "/var/lib/warc-search/cdx.db".into() }
fn default_sqlite_cache_kib() -> u32  { 8192 }
fn default_batch_size()     -> usize   { 5000 }
fn default_max_text_bytes() -> usize   { 524_288 }
fn default_bind()           -> String  { "0.0.0.0:8080".into() }
fn default_max_results()    -> usize   { 50 }
fn default_log_level()      -> String  { "info".into() }
fn default_true()           -> bool    { true }
fn default_tika_ocr_strategy()  -> String { "auto".to_owned() }
fn default_tika_ocr_languages() -> String { "deu+frk+eng".to_owned() }
fn default_max_pdf_bytes()      -> usize  { 100 * 1024 * 1024 }
fn default_tika_timeout_secs()  -> u64    { 300 }
fn default_collection_type()    -> String { "pdf_bucket".to_owned() }

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Fixtures ──────────────────────────────────────────────────────────────

    const MINIMAL_YAML: &str = r#"
s3:
  bucket: test-bucket
"#;

    const FULL_YAML: &str = r#"
s3:
  bucket: my-bucket
  region: eu-west-1
  endpoint_url: "https://minio.example.com"
  force_path_style: true
  access_key_id: "AKIAIOSFODNN7EXAMPLE"
  secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
  prefix: "crawls/2024/"
  concurrency: 8

storage:
  index_path: "/data/index"
  cdx_db_path: "/data/cdx.db"
  sqlite_cache_kib: 32768

indexer:
  batch_size: 10000
  max_text_bytes: 1048576
  index_pdfs: false
  index_warc_responses: true
  blacklisted_url_patterns:
    - "(?i)[?&]action=(edit|history)"
    - "(?i)/wiki/Diskussion:"
  tika:
    url: "http://127.0.0.1:9998"
    ocr_strategy: "no_ocr"

server:
  bind: "127.0.0.1:9000"
  max_results: 100
  enable_replay: false
  static_dir: "/opt/ui"

log:
  level: "debug"
"#;

    // ── Parsing ───────────────────────────────────────────────────────────────

    #[test]
    fn minimal_yaml_parses() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert_eq!(cfg.s3.bucket, "test-bucket");
    }

    #[test]
    fn minimal_yaml_defaults_applied() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert_eq!(cfg.s3.region, "us-east-1");
        assert_eq!(cfg.s3.concurrency, 4);
        assert_eq!(cfg.storage.index_path, "/var/lib/warc-search/index");
        assert_eq!(cfg.storage.cdx_db_path, "/var/lib/warc-search/cdx.db");
        assert_eq!(cfg.storage.sqlite_cache_kib, 8192);
        assert_eq!(cfg.indexer.batch_size, 5000);
        assert_eq!(cfg.indexer.max_text_bytes, 524_288);
        assert!(cfg.indexer.index_pdfs);
        assert!(cfg.indexer.index_warc_responses);
        assert!(cfg.indexer.blacklisted_url_patterns.is_empty());
        assert!(cfg.indexer.tika.is_none(), "Tika is opt-in — absent by default");
        assert_eq!(cfg.server.bind, "0.0.0.0:8080");
        assert_eq!(cfg.server.max_results, 50);
        assert!(cfg.server.enable_replay);
        assert!(cfg.server.static_dir.is_none());
        assert_eq!(cfg.log.level, "info");
    }

    #[test]
    fn full_yaml_parses_all_fields() {
        let cfg = Config::from_yaml(FULL_YAML).unwrap();
        assert_eq!(cfg.s3.bucket, "my-bucket");
        assert_eq!(cfg.s3.region, "eu-west-1");
        assert_eq!(cfg.s3.endpoint_url.as_deref(), Some("https://minio.example.com"));
        assert!(cfg.s3.force_path_style);
        assert_eq!(cfg.s3.access_key_id.as_deref(), Some("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(cfg.s3.prefix.as_deref(), Some("crawls/2024/"));
        assert_eq!(cfg.s3.concurrency, 8);
        assert_eq!(cfg.storage.index_path, "/data/index");
        assert_eq!(cfg.storage.sqlite_cache_kib, 32768);
        assert_eq!(cfg.indexer.batch_size, 10000);
        assert!(!cfg.indexer.index_pdfs);
        assert_eq!(
            cfg.indexer.blacklisted_url_patterns,
            vec!["(?i)[?&]action=(edit|history)", "(?i)/wiki/Diskussion:"],
        );
        assert_eq!(cfg.server.bind, "127.0.0.1:9000");
        assert_eq!(cfg.server.max_results, 100);
        assert!(!cfg.server.enable_replay);
        assert_eq!(cfg.server.static_dir.as_deref(), Some("/opt/ui"));
        assert_eq!(cfg.log.level, "debug");

        let tika = cfg.indexer.tika.as_ref().expect("tika block parsed");
        assert_eq!(tika.url, "http://127.0.0.1:9998");
        assert_eq!(tika.ocr_strategy, "no_ocr");
        // Unspecified Tika fields fall back to their defaults.
        assert_eq!(tika.ocr_languages, "deu+frk+eng");
        assert_eq!(tika.max_pdf_bytes, 100 * 1024 * 1024);
        assert_eq!(tika.timeout_secs, 300);
    }

    #[test]
    fn invalid_yaml_returns_error() {
        let result = Config::from_yaml("not: valid: yaml: [[[");
        assert!(result.is_err());
    }

    #[test]
    fn missing_required_bucket_field_returns_error() {
        // s3 section missing entirely — bucket has no default
        let result = Config::from_yaml("server:\n  bind: '0.0.0.0:8080'\n");
        assert!(result.is_err());
    }

    // ── Per-collection Tika settings ──────────────────────────────────────────

    const COLLECTION_YAML: &str = r#"
s3:
  bucket: warc
indexer:
  tika:
    url: "http://127.0.0.1:9998"
    ocr_strategy: "auto"
    ocr_languages: "deu+frk+eng"
    max_pdf_bytes: 104857600
    timeout_secs: 600
  collections:
    - name: pomologie
      type: pdf_bucket
      bucket: obst-pdfs
      public_base_url: "https://obst-pdfs.23.nu/"
      tika:
        ocr_strategy: "no_ocr"
        max_pdf_bytes: 629145600
    - name: plain
      type: pdf_bucket
      bucket: other
      public_base_url: "https://other.example/"
"#;

    #[test]
    fn a_collection_overrides_only_what_it_names() {
        let cfg = Config::from_yaml(COLLECTION_YAML).unwrap();
        let tika = cfg.indexer.tika.as_ref().unwrap();
        let over = cfg.indexer.collections[0].tika.as_ref().unwrap();
        let merged = tika.with_override(over);

        assert_eq!(merged.ocr_strategy, "no_ocr", "stated by the collection");
        assert_eq!(merged.max_pdf_bytes, 629145600, "stated by the collection");
        // Everything unstated stays global — a collection says what is special
        // about it, not everything about it.
        assert_eq!(merged.url, tika.url);
        assert_eq!(merged.ocr_languages, "deu+frk+eng");
        assert_eq!(merged.timeout_secs, 600);
    }

    #[test]
    fn the_global_settings_are_untouched_by_a_merge() {
        let cfg = Config::from_yaml(COLLECTION_YAML).unwrap();
        let tika = cfg.indexer.tika.as_ref().unwrap();
        let _ = tika.with_override(cfg.indexer.collections[0].tika.as_ref().unwrap());
        assert_eq!(tika.ocr_strategy, "auto", "the WARC archive keeps its own setting");
        assert_eq!(tika.max_pdf_bytes, 104857600);
    }

    #[test]
    fn a_collection_without_overrides_has_none() {
        let cfg = Config::from_yaml(COLLECTION_YAML).unwrap();
        assert!(cfg.indexer.collections[1].tika.is_none());
    }

    #[test]
    fn an_empty_override_changes_nothing() {
        let cfg = Config::from_yaml(COLLECTION_YAML).unwrap();
        let tika = cfg.indexer.tika.as_ref().unwrap();
        let merged = tika.with_override(&TikaOverride::default());
        assert_eq!(merged.ocr_strategy,  tika.ocr_strategy);
        assert_eq!(merged.ocr_languages, tika.ocr_languages);
        assert_eq!(merged.max_pdf_bytes, tika.max_pdf_bytes);
        assert_eq!(merged.timeout_secs,  tika.timeout_secs);
    }

    #[test]
    fn an_unknown_key_in_a_collection_override_is_an_error() {
        // The whole point of deny_unknown_fields: `ocr: "no_ocr"` instead of
        // `ocr_strategy:` would otherwise be dropped and OCR would run anyway.
        let err = Config::from_yaml(
            "s3:\n  bucket: warc\nindexer:\n  collections:\n    - name: c\n      \
             type: pdf_bucket\n      bucket: b\n      tika:\n        ocr: \"no_ocr\"\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("ocr"), "error should name the stray key: {err}");
    }

    // ── Collection key patterns ───────────────────────────────────────────────

    const KEY_PATTERN_YAML: &str = r#"
s3:
  bucket: warc
indexer:
  collections:
    - name: bsb-scans
      type: pdf_bucket
      bucket: obst-pdfs
      prefix: "archive-org/"
      key_pattern: "^archive-org/[0-9]+bsb/"
      public_base_url: "https://obst-pdfs.23.nu/"
    - name: archive-org
      type: pdf_bucket
      bucket: obst-pdfs
      prefix: "archive-org/"
      public_base_url: "https://obst-pdfs.23.nu/"
"#;

    #[test]
    fn a_key_pattern_narrows_a_prefix_the_two_collections_share() {
        let cfg = Config::from_yaml(KEY_PATTERN_YAML).unwrap();
        let narrow = cfg.indexer.collections[0].compile_key_pattern().unwrap().unwrap();

        // The digitisations, each in a directory of its own — no prefix can
        // describe them, which is why the pattern exists.
        assert!(narrow.is_match("archive-org/10229044bsb/10229044bsb.pdf"));
        assert!(narrow.is_match("archive-org/11756677bsb/11756677bsb.pdf"));
        // Everything else under the same prefix stays with the wider collection.
        assert!(!narrow.is_match("archive-org/dictionnairedepo01lero/dictionnairedepo01lero.pdf"));
        assert!(!narrow.is_match("archive-org/CAT31309742003/cat31309742003.pdf"));
        // And the wider collection has no pattern at all.
        assert!(cfg.indexer.collections[1].compile_key_pattern().unwrap().is_none());
    }

    #[test]
    fn a_broken_key_pattern_is_reported_not_swallowed() {
        // The caller must be able to stop: a collection whose narrowing rule
        // failed covers its whole prefix instead, which is the opposite of what
        // the rule asked for.
        let cfg = Config::from_yaml(
            "s3:\n  bucket: warc\nindexer:\n  collections:\n    - name: c\n      \
             type: pdf_bucket\n      bucket: b\n      key_pattern: \"[unclosed\"\n",
        )
        .unwrap();
        assert!(cfg.indexer.collections[0].compile_key_pattern().is_err());
    }

    // ── Unknown keys ──────────────────────────────────────────────────────────

    #[test]
    fn misplaced_top_level_block_is_an_error() {
        // The real mistake: `tika:` and `collections:` belong under `indexer:`.
        // Written at the top level they used to be dropped without a word, and
        // the indexer then found nothing to do and called that success.
        let err = Config::from_yaml(
            "s3:\n  bucket: warc\ntika:\n  url: 'http://127.0.0.1:9998'\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("tika"), "error should name the stray key: {err}");
    }

    #[test]
    fn unknown_key_inside_a_block_is_an_error() {
        let err = Config::from_yaml(
            "s3:\n  bucket: warc\nindexer:\n  max_pdf_bytes: 100\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("max_pdf_bytes"), "error should name the key: {err}");
    }

    #[test]
    fn the_shipped_config_still_parses() {
        // config.yaml is the file every deployment starts from; deny_unknown_fields
        // makes a drifted key in it fatal, so it is checked here rather than in
        // production.
        let yaml = include_str!("../../../config.yaml");
        Config::from_yaml(yaml).expect("shipped config.yaml must load");
    }

    // ── Credentials ───────────────────────────────────────────────────────────

    #[test]
    fn explicit_credentials_both_set() {
        let cfg = Config::from_yaml(FULL_YAML).unwrap();
        let creds = cfg.explicit_credentials().unwrap();
        assert_eq!(creds.0, "AKIAIOSFODNN7EXAMPLE");
    }

    #[test]
    fn explicit_credentials_none_when_missing() {
        let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
        assert!(cfg.explicit_credentials().is_none());
    }

    #[test]
    fn explicit_credentials_none_when_only_key_set() {
        let yaml = "s3:\n  bucket: b\n  access_key_id: key\n";
        let cfg = Config::from_yaml(yaml).unwrap();
        assert!(cfg.explicit_credentials().is_none());
    }

    #[test]
    fn explicit_credentials_none_when_empty_strings() {
        let yaml = r#"
s3:
  bucket: b
  access_key_id: ""
  secret_access_key: ""
"#;
        let cfg = Config::from_yaml(yaml).unwrap();
        assert!(cfg.explicit_credentials().is_none());
    }

    // ── Environment variable overrides ────────────────────────────────────────
    // These tests mutate environment variables, so they must be run with
    // `-- --test-threads=1` or use a serial test framework in CI.
    // We guard each test with a unique env var to reduce interference.

    fn with_env<F: FnOnce()>(vars: &[(&str, &str)], f: F) {
        for (k, v) in vars {
            std::env::set_var(k, v);
        }
        f();
        for (k, _) in vars {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn env_aws_access_key_overrides_yaml() {
        with_env(&[("AWS_ACCESS_KEY_ID", "ENV_KEY_ID")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.s3.access_key_id.as_deref(), Some("ENV_KEY_ID"));
        });
    }

    #[test]
    fn env_aws_secret_key_overrides_yaml() {
        with_env(&[("AWS_SECRET_ACCESS_KEY", "ENV_SECRET")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.s3.secret_access_key.as_deref(), Some("ENV_SECRET"));
        });
    }

    #[test]
    fn env_aws_region_overrides_yaml() {
        with_env(&[("AWS_DEFAULT_REGION", "ap-southeast-1")], || {
            let cfg = Config::from_yaml(FULL_YAML).unwrap();
            // env var beats eu-west-1 in FULL_YAML
            assert_eq!(cfg.s3.region, "ap-southeast-1");
        });
    }

    #[test]
    fn env_aws_endpoint_url_overrides_yaml() {
        with_env(&[("AWS_ENDPOINT_URL", "https://env-endpoint.example.com")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.s3.endpoint_url.as_deref(), Some("https://env-endpoint.example.com"));
        });
    }

    #[test]
    fn env_warc_s3_bucket_overrides_yaml() {
        with_env(&[("WARC_S3_BUCKET", "env-bucket")], || {
            let cfg = Config::from_yaml(FULL_YAML).unwrap();
            assert_eq!(cfg.s3.bucket, "env-bucket");
        });
    }

    #[test]
    fn env_warc_s3_prefix_overrides_yaml() {
        with_env(&[("WARC_S3_PREFIX", "env-prefix/")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.s3.prefix.as_deref(), Some("env-prefix/"));
        });
    }

    #[test]
    fn env_warc_s3_concurrency_overrides_yaml() {
        with_env(&[("WARC_S3_CONCURRENCY", "16")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.s3.concurrency, 16);
        });
    }

    #[test]
    fn env_warc_s3_concurrency_invalid_returns_error() {
        with_env(&[("WARC_S3_CONCURRENCY", "not-a-number")], || {
            let result = Config::from_yaml(MINIMAL_YAML);
            assert!(result.is_err());
        });
    }

    #[test]
    fn env_warc_index_path_overrides_yaml() {
        with_env(&[("WARC_INDEX_PATH", "/tmp/env-index")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.storage.index_path, "/tmp/env-index");
        });
    }

    #[test]
    fn env_warc_cdx_db_path_overrides_yaml() {
        with_env(&[("WARC_CDX_DB_PATH", "/tmp/env.db")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.storage.cdx_db_path, "/tmp/env.db");
        });
    }

    #[test]
    fn env_warc_server_bind_overrides_yaml() {
        with_env(&[("WARC_SERVER_BIND", "127.0.0.1:3000")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.server.bind, "127.0.0.1:3000");
        });
    }

    #[test]
    fn env_rust_log_overrides_yaml() {
        with_env(&[("RUST_LOG", "trace")], || {
            let cfg = Config::from_yaml(MINIMAL_YAML).unwrap();
            assert_eq!(cfg.log.level, "trace");
        });
    }

    #[test]
    fn env_vars_beat_yaml_credentials() {
        with_env(
            &[
                ("AWS_ACCESS_KEY_ID", "ENV_WINS"),
                ("AWS_SECRET_ACCESS_KEY", "ENV_SECRET_WINS"),
            ],
            || {
                // FULL_YAML has hardcoded creds — env should win
                let cfg = Config::from_yaml(FULL_YAML).unwrap();
                let (k, _) = cfg.explicit_credentials().unwrap();
                assert_eq!(k, "ENV_WINS");
            },
        );
    }

    // ── Load from file ────────────────────────────────────────────────────────

    #[test]
    fn load_from_nonexistent_file_returns_error() {
        let result = Config::load("/tmp/this-file-does-not-exist-warc-search.yaml");
        assert!(result.is_err());
    }

    #[test]
    fn load_blacklist_file_merges_and_dedups() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join(format!("tywb-skip-{}.txt", std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "# noise domains").unwrap();
        writeln!(f, "instagram.com").unwrap();
        writeln!(f, "  google.com   # search engine").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "PINTEREST.COM").unwrap();     // case-normalised
        writeln!(f, "instagram.com").unwrap();     // duplicate within file
        f.flush().unwrap();

        let mut cfg = IndexerConfig {
            blacklisted_domains: vec!["google.com".to_owned()], // pre-existing, should not double
            blacklisted_domains_path: Some(path.to_string_lossy().into_owned()),
            ..IndexerConfig::default()
        };
        let added = cfg.load_blacklist_file().unwrap();
        assert_eq!(added, 2, "instagram + pinterest are new; google is already present");

        // The merged set drives both index skip and display filter.
        assert!(cfg.is_url_blacklisted("https://www.instagram.com/foo"));
        assert!(cfg.is_url_blacklisted("https://maps.google.com/"));
        assert!(cfg.is_url_blacklisted("http://pinterest.com/pin/1"));
        assert!(!cfg.is_url_blacklisted("https://pomologen-verein.de/"));
        // No duplicate google entry.
        assert_eq!(cfg.blacklisted_domains.iter().filter(|d| *d == "google.com").count(), 1);

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn load_blacklist_file_missing_path_is_noop() {
        let mut cfg = IndexerConfig::default();
        assert_eq!(cfg.load_blacklist_file().unwrap(), 0);
    }

    // ── URL skip patterns ─────────────────────────────────────────────────────

    /// Helper: a config with `patterns` loaded and compiled.
    fn with_patterns(patterns: &[&str]) -> IndexerConfig {
        let mut cfg = IndexerConfig {
            blacklisted_url_patterns: patterns.iter().map(|p| (*p).to_owned()).collect(),
            ..IndexerConfig::default()
        };
        cfg.compile_url_patterns();
        cfg
    }

    #[test]
    fn url_patterns_do_nothing_until_compiled() {
        let cfg = IndexerConfig {
            blacklisted_url_patterns: vec!["(?i)action=edit".to_owned()],
            ..IndexerConfig::default()
        };
        assert!(
            !cfg.is_url_blacklisted("https://wiki.example.org/w/index.php?action=edit"),
            "an uncompiled list must not filter — compile_url_patterns() is the switch",
        );
    }

    #[test]
    fn url_patterns_catch_mediawiki_cruft_but_spare_the_article() {
        let cfg = with_patterns(&[
            r"(?i)[?&]action=(edit|history)",
            r"(?i)/wiki/(Diskussion|Spezial|Special):",
        ]);
        assert!(cfg.is_url_blacklisted("https://wiki.example.org/w/index.php?title=Apfel&action=history"));
        assert!(cfg.is_url_blacklisted("https://wiki.example.org/wiki/Diskussion:Apfel"));
        assert!(cfg.is_url_blacklisted("https://wiki.example.org/wiki/Spezial:Letzte_%C3%84nderungen"));
        // The article itself stays.
        assert!(!cfg.is_url_blacklisted("https://wiki.example.org/wiki/Apfel"));
        // …and so does a page that merely mentions the word.
        assert!(!cfg.is_url_blacklisted("https://example.org/geschichte-des-apfels"));
    }

    #[test]
    fn domain_and_url_pattern_lists_are_independent() {
        let mut cfg = with_patterns(&[r"(?i)[?&]action=edit"]);
        cfg.blacklisted_domains = vec!["instagram.com".to_owned()];

        assert!(cfg.is_url_blacklisted("https://www.instagram.com/foo"), "domain hit");
        assert!(cfg.is_url_blacklisted("https://wiki.example.org/w/?action=edit"), "pattern hit");
        assert!(!cfg.is_url_blacklisted("https://wiki.example.org/wiki/Apfel"), "neither");
    }

    #[test]
    fn compile_rejects_broken_and_catch_all_patterns() {
        let mut cfg = IndexerConfig {
            blacklisted_url_patterns: vec![
                r"(?i)/wiki/Diskussion:".to_owned(), // fine
                r"[unclosed".to_owned(),             // syntax error
                r".*".to_owned(),                    // matches everything
                r"(?i)".to_owned(),                  // bare flag: also matches everything
            ],
            ..IndexerConfig::default()
        };
        let report = cfg.compile_url_patterns();

        assert_eq!(report.compiled, 1);
        assert_eq!(report.rejected.len(), 3);
        // The good pattern still works — one bad line does not disarm the list.
        assert!(cfg.is_url_blacklisted("https://wiki.example.org/wiki/Diskussion:Apfel"));
        // And the catch-all did not survive to eat the archive.
        assert!(!cfg.is_url_blacklisted("https://pomologen-verein.de/"));
    }

    #[test]
    fn load_url_patterns_file_keeps_case_and_hashes() {
        use std::io::Write;
        let dir = std::env::temp_dir();
        let path = dir.join(format!("tywb-skip-urls-{}.txt", std::process::id()));
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "# whole-line comment").unwrap();
        writeln!(f).unwrap();
        writeln!(f, "  (?i)/wiki/Diskussion:  ").unwrap(); // trimmed
        writeln!(f, r"[?&#]action=edit").unwrap();          // '#' inside the pattern survives
        writeln!(f, "(?i)/wiki/Diskussion:").unwrap();      // duplicate within the file
        f.flush().unwrap();

        let mut cfg = IndexerConfig {
            blacklisted_url_patterns_path: Some(path.to_string_lossy().into_owned()),
            ..IndexerConfig::default()
        };
        let added = cfg.load_url_patterns_file().unwrap();
        assert_eq!(added, 2, "two distinct patterns; the comment and the repeat drop out");
        assert_eq!(cfg.blacklisted_url_patterns[1], r"[?&#]action=edit");

        let report = cfg.compile_url_patterns();
        assert_eq!(report.compiled, 2, "{:?}", report.rejected);
        assert!(cfg.is_url_blacklisted("https://wiki.example.org/page#action=edit"));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn load_url_patterns_file_missing_path_is_noop() {
        let mut cfg = IndexerConfig::default();
        assert_eq!(cfg.load_url_patterns_file().unwrap(), 0);
    }
}
