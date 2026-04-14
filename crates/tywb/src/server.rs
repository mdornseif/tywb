//! `tywb server` — HTTP API + Wayback-compatible replay server.
//!
//! # Endpoints
//!
//! | Method | Path               | Description              |
//! |--------|--------------------|--------------------------|
//! | GET    | `/`                | Web UI — home / stats    |
//! | GET    | `/ui/search`       | Web UI — fulltext search |
//! | GET    | `/ui/url`          | Web UI — URL captures    |
//! | GET    | `/ui/files`        | Web UI — WARC file list  |
//! | GET    | `/api/stats`       | JSON stats               |
//! | GET    | `/search`          | JSON fulltext search     |
//! | GET    | `/web/<ts>/<url>`  | Wayback replay           |
//! | GET    | `/cdx`             | CDX API (JSON)           |
//! | GET    | `/web/timemap/cdx` | CDX timemap (Zeno/pywb)  |
//! | GET    | `/healthz`         | Health check             |

use std::sync::{Arc, Mutex};

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json,
};
use serde::Deserialize;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

use std::io::Read as _;

use warc_search_cdx::{CdxStore, surt::to_surt};
use warc_search_config::Config;
use warc_search_s3::{build_client, get_range};
use warc_search_search::SearchReader;

use crate::ui;

// ── Application state ─────────────────────────────────────────────────────────

pub struct AppState {
    cdx:    Arc<Mutex<CdxStore>>,
    search: Arc<SearchReader>,
    s3:     Arc<aws_sdk_s3::Client>,
    config: Config,
}

// ── Entry point ───────────────────────────────────────────────────────────────

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    let cdx_store = CdxStore::open(&cfg.storage.cdx_db_path)
        .unwrap_or_else(|e| {
            error!(path = %cfg.storage.cdx_db_path, err = %e, "failed to open CDX store");
            std::process::exit(1);
        });
    cdx_store
        .set_cache_kib(cfg.storage.sqlite_cache_kib)
        .unwrap_or_else(|e| warn!(err = %e, "could not set SQLite cache size"));

    let search_reader = SearchReader::open(&cfg.storage.index_path)
        .unwrap_or_else(|e| {
            error!(path = %cfg.storage.index_path, err = %e, "failed to open search index");
            std::process::exit(1);
        });

    info!(docs = search_reader.num_docs(), "search index opened");

    let s3 = build_client(&cfg.s3).await;

    let state = Arc::new(AppState {
        cdx:    Arc::new(Mutex::new(cdx_store)),
        search: Arc::new(search_reader),
        s3:     Arc::new(s3),
        config: cfg.clone(),
    });

    let app = Router::new()
        // ── Web UI ──────────────────────────────────────────────────────
        .route("/",           get(home_handler))
        .route("/ui/search",  get(ui_search_handler))
        .route("/ui/browse",  get(browse_handler))
        .route("/ui/url",     get(ui_url_handler))
        .route("/ui/files",   get(ui_files_handler))
        // ── JSON API ─────────────────────────────────────────────────────
        .route("/api/stats", get(api_stats_handler))
        .route("/search",    get(search_handler))
        .route("/cdx",               get(cdx_handler))
        // ── CDX timemap (Zeno / gowarc deduplication) ─────────────────────
        // Must be registered before /web/*rest so the static path wins.
        .route("/web/timemap/cdx",   get(cdx_timemap_handler))
        // ── Wayback replay ────────────────────────────────────────────────
        .route("/web/*rest",         get(replay_handler))
        // ── Static assets ─────────────────────────────────────────────────
        .route("/_wb/wombat.js",     get(wombat_js_handler))
        // ── Health ────────────────────────────────────────────────────────
        .route("/healthz",   get(health_handler))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let bind = cfg.server.bind.clone();
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    info!(bind = %bind, "listening");
    axum::serve(listener, app).await?;

    Ok(())
}

// ── Handlers ──────────────────────────────────────────────────────────────────

async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

// ── / — homepage ──────────────────────────────────────────────────────────────

async fn home_handler(State(state): State<Arc<AppState>>) -> Response {
    let cdx_stats = { state.cdx.lock().unwrap().stats() };
    match cdx_stats {
        Ok(stats) => Html(ui::homepage_html(&stats, state.search.num_docs())).into_response(),
        Err(e) => {
            error!(err = %e, "stats query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

// ── /ui/search — HTML fulltext search ────────────────────────────────────────

#[derive(Deserialize)]
struct UiSearchParams {
    q:     Option<String>,
    from:  Option<String>,
    to:    Option<String>,
    limit: Option<usize>,
}

async fn ui_search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UiSearchParams>,
) -> Response {
    let q = params.q.as_deref().unwrap_or("").trim().to_owned();

    if q.is_empty() {
        return Html(ui::search_html("", &[], params.from.as_deref(), params.to.as_deref(), false))
            .into_response();
    }

    let limit   = params.limit.unwrap_or(state.config.server.max_results).min(200);
    let from_ts = params.from.as_deref().and_then(|s| {
        // Accept either 8-digit date (YYYYMMDD) or 14-digit timestamp.
        let padded = if s.len() == 8 { format!("{s}000000") } else { s.to_owned() };
        padded.parse::<u64>().ok()
    });
    let to_ts = params.to.as_deref().and_then(|s| {
        let padded = if s.len() == 8 { format!("{s}235959") } else { s.to_owned() };
        padded.parse::<u64>().ok()
    });

    debug!(q = %q, from = ?from_ts, to = ?to_ts, limit, "ui search");

    match state.search.search(&q, limit, from_ts, to_ts) {
        Ok(hits) => Html(ui::search_html(
            &q, &hits,
            params.from.as_deref(),
            params.to.as_deref(),
            false,
        )).into_response(),
        Err(e) => {
            error!(err = %e, "search failed");
            Html(ui::search_html(&q, &[], params.from.as_deref(), params.to.as_deref(), true))
                .into_response()
        }
    }
}

// ── /ui/url — HTML URL captures ───────────────────────────────────────────────

#[derive(Deserialize)]
struct UiUrlParams {
    url:  Option<String>,
    from: Option<String>,
    to:   Option<String>,
}

async fn ui_url_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UiUrlParams>,
) -> Response {
    let url = params.url.as_deref().unwrap_or("").trim().to_owned();

    if url.is_empty() {
        return Html(ui::url_html("", &[], false)).into_response();
    }

    let normalised = if url.contains("://") { url.clone() } else { format!("https://{url}") };
    let surt = match to_surt(&normalised) {
        Ok(s)  => s,
        Err(e) => {
            warn!(url = %url, err = %e, "invalid URL in ui_url");
            return Html(ui::url_html(&url, &[], true)).into_response();
        }
    };

    let from = params.from.as_deref().unwrap_or("00000000000000").to_owned();
    let to   = params.to.as_deref().unwrap_or("99999999999999").to_owned();

    debug!(url = %url, surt = %surt, "ui url lookup");

    let records = { state.cdx.lock().unwrap().get_by_surt_range(&surt, &from, &to) };
    match records {
        Ok(recs) => Html(ui::url_html(&url, &recs, false)).into_response(),
        Err(e) => {
            error!(err = %e, "CDX lookup failed");
            Html(ui::url_html(&url, &[], true)).into_response()
        }
    }
}

// ── /ui/files — WARC file list ────────────────────────────────────────────────

async fn ui_files_handler(State(state): State<Arc<AppState>>) -> Response {
    let rows = {
        state.cdx.lock().unwrap().recent_warc_files(200)
    };
    match rows {
        Ok(rows) => Html(ui::files_html(&rows, false)).into_response(),
        Err(e) => {
            error!(err = %e, "warc_files query failed");
            Html(ui::files_html(&[], true)).into_response()
        }
    }
}

// ── /ui/browse — domain hierarchy browser ────────────────────────────────────

#[derive(Deserialize)]
struct BrowseParams {
    tld:    Option<String>,
    domain: Option<String>,
}

async fn browse_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<BrowseParams>,
) -> Response {
    if let Some(domain) = &params.domain {
        // Level 3: captures under a domain name (e.g. "example.com")
        let domain = domain.trim().to_owned();
        let surt_prefix = ui::domain_to_surt_prefix(&domain);

        // Extract TLD for the back-link breadcrumb
        let tld = domain.rsplit('.').next().unwrap_or("").to_owned();

        const MAX_CAPTURES: usize = 5000;
        let records = {
            state.cdx.lock().unwrap()
                .get_by_surt_prefix(&surt_prefix, MAX_CAPTURES + 1)
        };
        match records {
            Ok(mut recs) => {
                let truncated = recs.len() > MAX_CAPTURES;
                if truncated { recs.truncate(MAX_CAPTURES); }
                Html(ui::browse_captures_html(&domain, &tld, &recs, truncated)).into_response()
            }
            Err(e) => {
                error!(err = %e, "browse captures query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else if let Some(tld) = &params.tld {
        // Level 2: hostnames under a TLD
        let tld = tld.trim().to_ascii_lowercase();
        const MAX_DOMAINS: usize = 2000;
        let domains = {
            state.cdx.lock().unwrap().browse_domains(&tld, MAX_DOMAINS + 1)
        };
        match domains {
            Ok(mut rows) => {
                let truncated = rows.len() > MAX_DOMAINS;
                if truncated { rows.truncate(MAX_DOMAINS); }
                Html(ui::browse_domains_html(&tld, &rows, truncated)).into_response()
            }
            Err(e) => {
                error!(err = %e, "browse domains query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    } else {
        // Level 1: TLD list
        const MAX_TLDS: usize = 500;
        let tlds = { state.cdx.lock().unwrap().browse_tlds(MAX_TLDS + 1) };
        match tlds {
            Ok(mut rows) => {
                let truncated = rows.len() > MAX_TLDS;
                if truncated { rows.truncate(MAX_TLDS); }
                Html(ui::browse_tlds_html(&rows, truncated)).into_response()
            }
            Err(e) => {
                error!(err = %e, "browse TLDs query failed");
                (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
            }
        }
    }
}

// ── /api/stats — JSON stats ───────────────────────────────────────────────────

async fn api_stats_handler(State(state): State<Arc<AppState>>) -> Response {
    let cdx_stats = { state.cdx.lock().unwrap().stats() };
    match cdx_stats {
        Ok(s) => Json(ui::ApiStats {
            cdx: ui::ApiCdxStats {
                total_records:    s.total_records,
                unique_urls:      s.unique_urls,
                warc_files:       s.warc_files,
                oldest_timestamp: s.oldest_timestamp,
                newest_timestamp: s.newest_timestamp,
            },
            search: ui::ApiSearchStats {
                num_docs: state.search.num_docs(),
            },
        }).into_response(),
        Err(e) => {
            error!(err = %e, "api/stats failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

// ── /search — JSON fulltext search ───────────────────────────────────────────

#[derive(Deserialize)]
struct SearchParams {
    q:     String,
    from:  Option<String>,
    to:    Option<String>,
    limit: Option<usize>,
}

async fn search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> Response {
    let limit   = params.limit.unwrap_or(state.config.server.max_results).min(500);
    let from_ts = params.from.as_deref().and_then(|s| s.parse::<u64>().ok());
    let to_ts   = params.to.as_deref().and_then(|s| s.parse::<u64>().ok());

    debug!(q = %params.q, from = ?from_ts, to = ?to_ts, limit, "search");

    match state.search.search(&params.q, limit, from_ts, to_ts) {
        Ok(hits) => Json(hits).into_response(),
        Err(e) => {
            error!(err = %e, "search failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

// ── /web/<timestamp>/<url> → Wayback replay ───────────────────────────────────

async fn replay_handler(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    Path(rest): Path<String>,
) -> Response {
    let Some(slash) = rest.find('/') else {
        return (StatusCode::BAD_REQUEST, "expected /web/<timestamp>/<url>").into_response();
    };

    let timestamp    = &rest[..slash];
    let url_path     = &rest[slash + 1..];
    let original_url = match uri.query() {
        Some(q) => format!("{url_path}?{q}"),
        None    => url_path.to_owned(),
    };

    debug!(timestamp, url = %original_url, "replay");

    let surt = match to_surt(&original_url) {
        Ok(s)  => s,
        Err(e) => {
            warn!(url = %original_url, err = %e, "invalid URL in replay request");
            return (StatusCode::BAD_REQUEST, format!("invalid URL: {e}")).into_response();
        }
    };

    let cdx_rec = {
        let store = state.cdx.lock().unwrap();
        match store.closest(&surt, timestamp) {
            Ok(Some(r)) => r,
            Ok(None)    => return (StatusCode::NOT_FOUND, "not found in CDX index").into_response(),
            Err(e) => {
                error!(err = %e, "CDX closest lookup failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    };

    let is_gz = cdx_rec.s3_key.to_ascii_lowercase().ends_with(".gz");

    let warc_bytes: Vec<u8> = if is_gz {
        // ── Compressed replay ─────────────────────────────────────────────────
        // Each WARC record in a .warc.gz lives in its own gzip member.
        // `c_offset` is the compressed byte offset of that member in the S3
        // object; we Range-GET from there and decompress exactly one member.
        let c_offset = match cdx_rec.c_offset {
            Some(o) => o,
            None => {
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "This record predates compressed-offset indexing. \
                     Re-index to enable replay (tywb index).",
                )
                    .into_response();
            }
        };

        // Compressed member size is unknown, but always ≤ uncompressed size.
        // Fetch `length + 64 KiB` compressed bytes; GzDecoder stops at member end.
        let fetch_len = cdx_rec.length + 65536;
        let compressed = match get_range(
            &state.s3, &state.config.s3.bucket,
            &cdx_rec.s3_key, c_offset, fetch_len,
        ).await {
            Ok(b)  => b,
            Err(e) => {
                error!(err = %e, s3_key = %cdx_rec.s3_key, "S3 range GET failed");
                return (StatusCode::BAD_GATEWAY, e.to_string()).into_response();
            }
        };

        let mut gz = flate2::read::GzDecoder::new(compressed.as_ref());
        let mut decompressed = Vec::new();
        if let Err(e) = gz.read_to_end(&mut decompressed) {
            error!(err = %e, s3_key = %cdx_rec.s3_key, c_offset, "gzip decode failed");
            return (StatusCode::BAD_GATEWAY, format!("gzip decode: {e}")).into_response();
        }
        decompressed
    } else {
        // ── Uncompressed replay ───────────────────────────────────────────────
        // offset is the byte position of the WARC record in the plain .warc file.
        // Fetch enough bytes to cover WARC headers (~1 KB) plus the HTTP block.
        let fetch_len = cdx_rec.length + 4096;
        match get_range(
            &state.s3, &state.config.s3.bucket,
            &cdx_rec.s3_key, cdx_rec.offset, fetch_len,
        ).await {
            Ok(b)  => b.to_vec(),
            Err(e) => {
                error!(err = %e, s3_key = %cdx_rec.s3_key, "S3 range GET failed");
                return (StatusCode::BAD_GATEWAY, e.to_string()).into_response();
            }
        }
    };

    // `warc_bytes` begins at the WARC record (WARC/1.0\r\n...).
    // Skip the WARC header block to get the HTTP response block.
    let http_block = extract_warc_http_block(&warc_bytes);
    serve_warc_response(http_block, &cdx_rec.original_url, &cdx_rec.timestamp)
}

/// Skip WARC headers (everything up to and including the first `\r\n\r\n`) and
/// return the slice starting at the HTTP response block.
fn extract_warc_http_block(warc_bytes: &[u8]) -> &[u8] {
    const SEP: &[u8] = b"\r\n\r\n";
    warc_bytes
        .windows(4)
        .position(|w| w == SEP)
        .map(|i| &warc_bytes[i + 4..])
        .unwrap_or(warc_bytes)
}

/// Parse an HTTP response block and return an Axum `Response`.
///
/// `block` must start with the HTTP status line (`HTTP/1.x NNN ...`).
/// The function extracts status code, Content-Type, Location (for redirects),
/// and the response body.  For HTML responses a Wayback toolbar and wombat.js
/// URL-rewriting shim are injected.
fn serve_warc_response(block: &[u8], original_url: &str, timestamp: &str) -> Response {
    let sep = b"\r\n\r\n";
    let body_start = block.windows(4).position(|w| w == sep).map(|i| i + 4).unwrap_or(block.len());
    let header_bytes = &block[..body_start.saturating_sub(4)];
    let body = &block[body_start..];

    let header_str = String::from_utf8_lossy(header_bytes);
    let mut lines = header_str.lines();

    let status_code = lines
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(200);
    let status = StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK);

    let mut content_type_val = String::new();
    let mut headers = HeaderMap::new();
    for line in lines {
        if line.is_empty() { break; }
        if let Some((name, val)) = line.split_once(':') {
            match name.trim().to_ascii_lowercase().as_str() {
                "content-type" => {
                    content_type_val = val.trim().to_owned();
                    if let Ok(hv) = HeaderValue::from_str(&content_type_val) {
                        headers.insert(header::CONTENT_TYPE, hv);
                    }
                }
                "location" if status.is_redirection() => {
                    if let Ok(hv) = HeaderValue::from_str(val.trim()) {
                        headers.insert(header::LOCATION, hv);
                    }
                }
                _ => {}
            }
        }
    }

    if let Ok(hv) = HeaderValue::from_str(original_url) {
        headers.insert("X-Archive-Orig-URL", hv);
    }

    let is_html = content_type_val
        .split(';')
        .next()
        .map(|t| t.trim().eq_ignore_ascii_case("text/html"))
        .unwrap_or(false);

    let final_body: Vec<u8> = if is_html {
        inject_wayback_ui(body.to_vec(), original_url, timestamp)
    } else {
        body.to_vec()
    };

    (status, headers, final_body).into_response()
}

// ── Wayback UI injection ──────────────────────────────────────────────────────

/// Inject the Wayback toolbar and wombat.js initializer into an HTML page.
///
/// Two injections are made:
///  1. Before `</head>` — wombat.js `<script>` tag + initialization block.
///  2. After the opening `<body …>` tag — sticky toolbar banner.
///
/// If neither injection point is found the document is returned unchanged.
fn inject_wayback_ui(mut html: Vec<u8>, original_url: &str, timestamp: &str) -> Vec<u8> {
    // Build the wombat init block.
    // wombat_sec = Unix seconds for the timestamp (approximate; we parse YYYYMMDDHHMMSS).
    let wombat_sec = timestamp_to_unix(timestamp);
    let scheme = if original_url.starts_with("https") { "https" } else { "http" };
    let host = {
        let after_scheme = original_url.find("://").map(|i| &original_url[i + 3..]).unwrap_or(original_url);
        after_scheme.split('/').next().unwrap_or("")
    };

    // Percent-encode the original URL for use in the toolbar link.
    let mut encoded_url = String::new();
    ui::push_url_encoded(&mut encoded_url, original_url);

    // Human-readable date from timestamp (YYYYMMDDHHMMSS → YYYY-MM-DD HH:MM:SS)
    let display_date = format_timestamp_display(timestamp);

    let head_injection = format!(
        r#"<script src="/_wb/wombat.js"></script>
<script>
(function() {{
  var wbinfo = {{
    "url": {url_json},
    "timestamp": {ts_json},
    "request_ts": {ts_json},
    "prefix": "/web/",
    "mod": "",
    "is_framed": false,
    "is_live": false,
    "static_prefix": "/_wb/",
    "enable_auto_fetch": false,
    "wombat_ts": {ts_json},
    "wombat_sec": {wombat_sec},
    "wombat_scheme": {scheme_json},
    "wombat_host": {host_json},
    "wombat_opts": {{}}
  }};
  if (window._WBWombatInit) {{ window._WBWombatInit(wbinfo); }}
}})();
</script>
"#,
        url_json    = json_str(original_url),
        ts_json     = json_str(timestamp),
        wombat_sec  = wombat_sec,
        scheme_json = json_str(scheme),
        host_json   = json_str(host),
    );

    let toolbar_html = format!(
        r#"<div id="__tywb_bar" style="position:fixed;top:0;left:0;right:0;z-index:2147483647;background:#2b2b2b;color:#eee;font:13px/1.4 sans-serif;padding:4px 12px;display:flex;align-items:center;gap:12px;box-shadow:0 2px 6px rgba(0,0,0,.4)">
  <span style="font-weight:bold;color:#f90">tywb</span>
  <span>Archived: <b>{date}</b></span>
  <span style="flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><a href="{orig_url}" style="color:#8cf">{orig_url_display}</a></span>
  <a href="/ui/url?url={encoded_url}" style="color:#fc8;white-space:nowrap">other captures</a>
</div>
<div style="height:32px"></div>
"#,
        date             = html_escape(&display_date),
        orig_url         = html_escape(original_url),
        orig_url_display = html_escape(truncate_url(original_url, 80)),
        encoded_url      = encoded_url,
    );

    // ── Inject before </head> ─────────────────────────────────────────────────
    if let Some(pos) = find_case_insensitive(&html, b"</head>") {
        let injection = head_injection.as_bytes();
        html.splice(pos..pos, injection.iter().copied());
    }

    // ── Inject after <body ...> ───────────────────────────────────────────────
    if let Some(pos) = find_body_open(&html) {
        let injection = toolbar_html.as_bytes();
        html.splice(pos..pos, injection.iter().copied());
    }

    html
}

/// Case-insensitive forward search for `needle` in `haystack`.
/// Returns the byte position of the first match, or `None`.
fn find_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() { return None; }
    haystack.windows(needle.len()).position(|w| {
        w.iter().zip(needle.iter()).all(|(a, b)| a.to_ascii_lowercase() == b.to_ascii_lowercase())
    })
}

/// Find the position immediately after the `>` that closes the opening `<body` tag.
fn find_body_open(haystack: &[u8]) -> Option<usize> {
    let body_pos = find_case_insensitive(haystack, b"<body")?;
    // Scan forward from `<body` to find the closing `>`.
    let close = haystack[body_pos..].iter().position(|&b| b == b'>')?;
    Some(body_pos + close + 1)
}

/// Escape `<`, `>`, `&`, and `"` for embedding in HTML attribute or text.
fn html_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&'  => out.push_str("&amp;"),
            '<'  => out.push_str("&lt;"),
            '>'  => out.push_str("&gt;"),
            '"'  => out.push_str("&quot;"),
            _    => out.push(c),
        }
    }
    out
}

/// Wrap a string in JSON double quotes with minimal escaping (for JS literals).
fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"'  => out.push_str(r#"\""#),
            '\\' => out.push_str(r"\\"),
            '\n' => out.push_str(r"\n"),
            '\r' => out.push_str(r"\r"),
            _    => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Truncate a URL to at most `max` characters, appending `…` if truncated.
fn truncate_url(url: &str, max: usize) -> &str {
    if url.len() <= max { url } else { &url[..max] }
}

/// Convert a 14-digit WARC timestamp (YYYYMMDDHHMMSS) to approximate Unix seconds.
/// Returns 0 if the timestamp cannot be parsed.
fn timestamp_to_unix(ts: &str) -> i64 {
    if ts.len() < 14 { return 0; }
    let year:  i64 = ts[0..4].parse().unwrap_or(1970);
    let month: i64 = ts[4..6].parse().unwrap_or(1);
    let day:   i64 = ts[6..8].parse().unwrap_or(1);
    let hour:  i64 = ts[8..10].parse().unwrap_or(0);
    let min:   i64 = ts[10..12].parse().unwrap_or(0);
    let sec:   i64 = ts[12..14].parse().unwrap_or(0);

    // Days since Unix epoch (Jan 1 1970).  Approximate — ignores leap seconds.
    let days = days_from_civil(year, month, day);
    days * 86400 + hour * 3600 + min * 60 + sec
}

/// Days since 1970-01-01 for a proleptic Gregorian date (Howard Hinnant algorithm).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;                            // [0, 399]
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;  // [0, 146096]
    era * 146097 + doe - 719468
}

/// Format a 14-digit timestamp as `YYYY-MM-DD HH:MM:SS`.
fn format_timestamp_display(ts: &str) -> String {
    if ts.len() < 14 {
        return ts.to_owned();
    }
    format!(
        "{}-{}-{} {}:{}:{}",
        &ts[0..4], &ts[4..6], &ts[6..8],
        &ts[8..10], &ts[10..12], &ts[12..14],
    )
}

// ── /_wb/wombat.js — embedded static asset ───────────────────────────────────

async fn wombat_js_handler() -> impl IntoResponse {
    static WOMBAT_JS: &[u8] = include_bytes!("wombat.js");
    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE,  "application/javascript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=86400"),
        ],
        WOMBAT_JS,
    )
}

// ── /cdx — CDX API ───────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct CdxParams {
    url:        Option<String>,
    from:       Option<String>,
    to:         Option<String>,
    #[serde(default = "default_cdx_limit")]
    limit:      usize,
    #[serde(rename = "matchType")]
    match_type: Option<String>,
    #[allow(dead_code)]
    output:     Option<String>,
}

fn default_cdx_limit() -> usize { 100 }

async fn cdx_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<CdxParams>,
) -> Response {
    let raw_url = match &params.url {
        Some(u) => u.clone(),
        None    => return (StatusCode::BAD_REQUEST, "url parameter required").into_response(),
    };

    let limit = params.limit.min(10_000);

    let (lookup_url, is_prefix) = if raw_url.ends_with('*') {
        (raw_url.trim_end_matches('*').to_owned(), true)
    } else {
        let prefix = params.match_type.as_deref()
            .map(|s| s.eq_ignore_ascii_case("prefix"))
            .unwrap_or(false);
        (raw_url.clone(), prefix)
    };

    let surt_lookup = {
        let normalised = if lookup_url.contains("://") {
            lookup_url.clone()
        } else {
            format!("http://{lookup_url}")
        };
        match to_surt(&normalised) {
            Ok(s)  => s,
            Err(e) => return (StatusCode::BAD_REQUEST, format!("invalid url: {e}")).into_response(),
        }
    };

    let from = params.from.as_deref().unwrap_or("00000000000000").to_owned();
    let to   = params.to.as_deref().unwrap_or("99999999999999").to_owned();

    debug!(url = %raw_url, surt = %surt_lookup, prefix = is_prefix, "CDX lookup");

    let records = {
        let store = state.cdx.lock().unwrap();
        if is_prefix {
            let domain_prefix = surt_lookup.find(')')
                .map(|pos| surt_lookup[..=pos].to_owned())
                .unwrap_or_else(|| surt_lookup.clone());
            store.get_by_surt_prefix(&domain_prefix, limit)
        } else {
            store.get_by_surt_range(&surt_lookup, &from, &to)
        }
    };

    let records = match records {
        Ok(r)  => r,
        Err(e) => {
            error!(err = %e, "CDX query failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    let header = serde_json::json!(["urlkey","timestamp","original","mimetype","statuscode","digest","length"]);
    let mut rows: Vec<serde_json::Value> = vec![header];
    for r in &records {
        rows.push(serde_json::json!([
            r.surt_url, r.timestamp, r.original_url,
            r.mime.as_deref().unwrap_or("-"),
            r.status.map(|s| s.to_string()).as_deref().unwrap_or("-"),
            r.digest.as_deref().unwrap_or("-"),
            r.length.to_string(),
        ]));
    }

    Json(rows).into_response()
}

// ── /web/timemap/cdx — CDX timemap for Zeno/gowarc deduplication ─────────────
//
// gowarc's dedupe.go queries:
//   GET /web/timemap/cdx?url=<url>&limit=-1
//
// Response format: space-separated plain-text, one WARC record per line:
//   {urlkey} {timestamp} {original} {mime} {status} {digest} {length}
//
// The digest is returned WITHOUT the hash-algorithm prefix ("sha1:", "sha256:",
// etc.) because gowarc strips it before comparing:
//   digest = strings.SplitN(digest, ":", 2)[1]
//
// Negative `limit` values follow the pywb convention: limit=-N returns the N
// most-recent records (tail of the time-ordered result set), most-recent first.
// limit=-1 therefore returns the single latest capture for the URL.

#[derive(Deserialize)]
struct TimemapCdxParams {
    url:   Option<String>,
    limit: Option<i64>,
}

async fn cdx_timemap_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TimemapCdxParams>,
) -> Response {
    let raw_url = match params.url.as_deref() {
        Some(u) if !u.is_empty() => u.to_owned(),
        _ => return (StatusCode::BAD_REQUEST, "url parameter required").into_response(),
    };

    let surt = match to_surt(&raw_url) {
        Ok(s)  => s,
        Err(e) => return (StatusCode::BAD_REQUEST, format!("invalid url: {e}")).into_response(),
    };

    debug!(url = %raw_url, surt = %surt, "CDX timemap lookup");

    // Fetch all captures for this SURT URL (ascending timestamp order).
    let all = {
        let store = state.cdx.lock().unwrap();
        store.get_by_surt(&surt)
    };
    let all = match all {
        Ok(r)  => r,
        Err(e) => {
            error!(err = %e, "CDX timemap query failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
        }
    };

    // Apply limit.  Positive N → first N (oldest).  Negative N → last N
    // (most-recent), returned most-recent-first.
    let limit = params.limit.unwrap_or(i64::MAX);
    let records: Vec<_> = if limit < 0 {
        let n = limit.unsigned_abs() as usize;
        all.into_iter().rev().take(n).collect()
    } else {
        all.into_iter().take(limit as usize).collect()
    };

    // Serialise as space-separated plain text.
    // Digest field: strip the hash-algorithm prefix so gowarc can compare it
    // directly to the raw digest it computed locally.
    let mut body = String::new();
    for r in &records {
        let mime   = r.mime.as_deref().unwrap_or("-");
        let status = r.status.map(|s| s.to_string()).unwrap_or_else(|| "-".to_owned());
        let raw_dig = r.digest.as_deref().unwrap_or("-");
        let digest  = raw_dig.find(':')
            .map(|i| &raw_dig[i + 1..])
            .unwrap_or(raw_dig);
        let _ = std::fmt::write(
            &mut body,
            format_args!(
                "{} {} {} {} {} {} {}\n",
                r.surt_url, r.timestamp, r.original_url,
                mime, status, digest, r.length,
            ),
        );
    }

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
        .into_response()
}
