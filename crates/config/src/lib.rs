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
pub struct StorageConfig {
    #[serde(default = "default_index_path")]
    pub index_path: String,
    #[serde(default = "default_cdx_db_path")]
    pub cdx_db_path: String,
    #[serde(default = "default_sqlite_cache_kib")]
    pub sqlite_cache_kib: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexerConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_max_text_bytes")]
    pub max_text_bytes: usize,
    #[serde(default = "default_true")]
    pub index_pdfs: bool,
    #[serde(default = "default_true")]
    pub index_warc_responses: bool,
    #[serde(default)]
    pub skip_patterns: Vec<String>,
    /// Domains (and all their subdomains) to exclude from indexing and remove
    /// from any existing index.  Plain hostnames without scheme or path, e.g.
    /// `example.com`.  Subdomains are matched automatically: listing `example.com`
    /// also excludes `www.example.com`, `cdn.example.com`, etc.
    #[serde(default)]
    pub blacklisted_domains: Vec<String>,
    /// Optional Apache Tika backend for extracting text from PDFs. When unset,
    /// PDFs are not fulltext-indexed (they remain browsable and replayable) —
    /// this keeps the dependency-free deployment possible. See [`TikaConfig`].
    #[serde(default)]
    pub tika: Option<TikaConfig>,
}

/// Configuration for the optional Apache Tika text-extraction backend.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    /// Return `true` if the URL's host is covered by the domain blacklist.
    /// Non-HTTP/HTTPS URLs are never blacklisted.
    pub fn is_url_blacklisted(&self, url: &str) -> bool {
        if self.blacklisted_domains.is_empty() {
            return false;
        }
        // Fast path: extract host without full URL parsing.
        let after_scheme = if let Some(pos) = url.find("://") {
            &url[pos + 3..]
        } else {
            return false;
        };
        let host = after_scheme
            .split(['/', ':', '?', '#'])
            .next()
            .unwrap_or("")
            .to_ascii_lowercase();
        self.is_host_blacklisted(&host)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
}

// ── Root config ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
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
            skip_patterns: vec![],
            blacklisted_domains: vec![],
            tika: None,
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
  skip_patterns:
    - "*.css"
    - "*.js"
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
        assert!(cfg.indexer.skip_patterns.is_empty());
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
        assert_eq!(cfg.indexer.skip_patterns, vec!["*.css", "*.js"]);
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
}
