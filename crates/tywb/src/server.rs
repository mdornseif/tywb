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
//! | GET    | `/text`            | Plain text of a capture  |
//! | GET    | `/text/<ts>/<url>` | Plain text of a capture  |
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

use warc_search_cdx::{CdxRecord, CdxStore, surt::to_surt};
use warc_search_config::Config;
use warc_search_s3::{build_client, get_range};
use warc_search_search::SearchReader;

use crate::pdf::PdfExtractor;
use crate::ui;

// ── Application state ─────────────────────────────────────────────────────────

pub struct AppState {
    cdx:    Arc<Mutex<CdxStore>>,
    search: Arc<SearchReader>,
    s3:     Arc<aws_sdk_s3::Client>,
    config: Config,
    /// Tika client for `/text` on PDFs. `None` when `indexer.tika` is unset —
    /// PDFs are then served for replay but their text cannot be extracted.
    pdf:    Option<PdfExtractor>,
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

    let pdf = cfg.indexer.tika.as_ref().map(PdfExtractor::new);
    if pdf.is_none() {
        info!("indexer.tika unset — /text will not extract PDFs");
    }

    let state = Arc::new(AppState {
        cdx:    Arc::new(Mutex::new(cdx_store)),
        search: Arc::new(search_reader),
        s3:     Arc::new(s3),
        config: cfg.clone(),
        pdf,
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
        // ── Plain text of a capture ───────────────────────────────────────
        .route("/text",              get(text_handler))
        .route("/text/*rest",        get(text_path_handler))
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
    let (cdx_stats, collections) = {
        let store = state.cdx.lock().unwrap();
        (store.stats(), store.collection_counts().unwrap_or_default())
    };
    match cdx_stats {
        Ok(stats) => Html(ui::homepage_html(&stats, state.search.num_docs(), &collections)).into_response(),
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
    /// Restrict results to a single collection (e.g. "obst-pdfs").
    collection: Option<String>,
}

async fn ui_search_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<UiSearchParams>,
) -> Response {
    let q = params.q.as_deref().unwrap_or("").trim().to_owned();

    if q.is_empty() {
        return Html(ui::search_html("", &[], params.from.as_deref(), params.to.as_deref(),
                                   params.collection.as_deref(), false))
            .into_response();
    }

    // The UI collapses hits to one row per domain, so a plain `max_results`
    // worth of hits would often boil down to a handful of rows. Pull a deeper
    // slice and let the grouping thin it out.
    const GROUPING_FACTOR: usize = 4;
    const HARD_CAP: usize = 400;
    let limit = params
        .limit
        .unwrap_or(state.config.server.max_results)
        .saturating_mul(GROUPING_FACTOR)
        .min(HARD_CAP);
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
        Ok(mut hits) => {
            hits.retain(|h| !state.config.indexer.is_url_blacklisted(&h.url));
            if let Some(coll) = params.collection.as_deref() {
                hits.retain(|h| h.collection.as_deref() == Some(coll));
            }
            Html(ui::search_html(
                &q, &hits,
                params.from.as_deref(),
                params.to.as_deref(),
                params.collection.as_deref(),
                false,
            )).into_response()
        }
        Err(e) => {
            error!(err = %e, "search failed");
            Html(ui::search_html(&q, &[], params.from.as_deref(), params.to.as_deref(),
                                 params.collection.as_deref(), true))
                .into_response()
        }
    }
}

/// Level 3 — the captures of one hostname.
async fn browse_captures(
    state: &Arc<AppState>,
    host: &str,
    tld: &str,
    show_all: bool,
) -> Response {
    const MAX_CAPTURES: usize = 5000;
    let surt_prefix = ui::domain_to_surt_prefix(host);
    let records = {
        state.cdx.lock().unwrap().get_by_surt_prefix(&surt_prefix, MAX_CAPTURES + 1)
    };
    match records {
        Ok(mut recs) => {
            let truncated = recs.len() > MAX_CAPTURES;
            if truncated { recs.truncate(MAX_CAPTURES); }
            Html(ui::browse_captures_html(host, tld, &recs, truncated, show_all)).into_response()
        }
        Err(e) => {
            error!(err = %e, "browse captures query failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
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
    /// A registered domain — lists the hostnames under it.
    domain: Option<String>,
    /// An exact hostname — lists its captures.
    ///
    /// This exists because the level cannot be derived from the name: for a
    /// site served from its apex (`example.de` with no `www.`), "registered
    /// domain" and "hostname" are the same string, and guessing by dot count
    /// sent every such domain back to the hostname list instead of its
    /// captures.
    host:   Option<String>,
    /// When present and non-zero, show non-2xx URLs too.
    all:    Option<u8>,
}

async fn browse_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<BrowseParams>,
) -> Response {
    // Level 3, explicit: captures of one hostname.
    if let Some(host) = &params.host {
        let host = host.trim().to_owned();
        let tld = host.rsplit('.').next().unwrap_or("").to_owned();
        return browse_captures(&state, &host, &tld, params.all.unwrap_or(0) != 0).await;
    }

    if let Some(domain) = &params.domain {
        let domain = domain.trim().to_owned();
        let tld = domain.rsplit('.').next().unwrap_or("").to_owned();
        let dot_count = domain.chars().filter(|&c| c == '.').count();

        if dot_count == 1 {
            // Level 2b: registered domain (e.g. "obstsortendatenbank.de") —
            // show all hostnames under it before diving into captures.
            let surt_prefix = {
                // domain_to_surt_prefix appends ')'; strip it to get the bare prefix.
                let p = ui::domain_to_surt_prefix(&domain);
                p[..p.len() - 1].to_owned()
            };
            const MAX_HOSTS: usize = 500;
            let hosts = {
                state.cdx.lock().unwrap().browse_subdomains(&surt_prefix, MAX_HOSTS + 1)
            };
            match hosts {
                Ok(mut rows) => {
                    let truncated = rows.len() > MAX_HOSTS;
                    if truncated { rows.truncate(MAX_HOSTS); }
                    Html(ui::browse_subdomains_html(&tld, &domain, &rows, truncated)).into_response()
                }
                Err(e) => {
                    error!(err = %e, "browse subdomains query failed");
                    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
                }
            }
        } else {
            // Level 3: a name with subdomains is unambiguously a hostname.
            browse_captures(&state, &domain, &tld, params.all.unwrap_or(0) != 0).await
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
    collection: Option<String>,
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
        Ok(mut hits) => {
            hits.retain(|h| !state.config.indexer.is_url_blacklisted(&h.url));
            if let Some(coll) = params.collection.as_deref() {
                hits.retain(|h| h.collection.as_deref() == Some(coll));
            }
            Json(hits).into_response()
        }
        Err(e) => {
            error!(err = %e, "search failed");
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

// ── Fetching the bytes behind a CDX record ────────────────────────────────────

/// The stored bytes a CDX record points at.
enum Payload {
    /// One WARC record, starting at `WARC/1.0`, out of the archive bucket.
    Warc(Vec<u8>),
    /// A standalone object from a non-WARC collection (e.g. a PDF bucket).
    Object(Vec<u8>),
}

/// Fetch the bytes for `rec` — a Range GET of a single WARC record, or the
/// whole object for a non-WARC collection.
///
/// Shared by replay and `/text`: both need exactly these bytes and differ only
/// in what they do with them. The error case is a ready-made `Response` so the
/// two handlers report S3 and index problems identically.
async fn fetch_payload(state: &Arc<AppState>, rec: &CdxRecord) -> Result<Payload, Response> {
    // Records from a non-WARC collection are standalone objects with no WARC
    // container — read them straight from their own bucket.
    if rec.collection != warc_search_cdx::DEFAULT_COLLECTION {
        let Some(coll) = state
            .config
            .indexer
            .collections
            .iter()
            .find(|c| c.name == rec.collection)
        else {
            error!(collection = %rec.collection, "request for unknown collection");
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unknown collection: {}", rec.collection),
            )
                .into_response());
        };

        return match warc_search_s3::get_bytes(&state.s3, &coll.bucket, &rec.s3_key).await {
            Ok(bytes) => Ok(Payload::Object(bytes.to_vec())),
            Err(warc_search_s3::S3Error::NotFound { .. }) => {
                Err((StatusCode::NOT_FOUND, "object not found in collection bucket").into_response())
            }
            Err(e) => {
                error!(err = %e, bucket = %coll.bucket, key = %rec.s3_key,
                       "collection object GET failed");
                Err((StatusCode::BAD_GATEWAY, e.to_string()).into_response())
            }
        };
    }

    let is_gz = rec.s3_key.to_ascii_lowercase().ends_with(".gz");

    if is_gz {
        // ── Compressed ────────────────────────────────────────────────────────
        // Each WARC record in a .warc.gz lives in its own gzip member.
        // `c_offset` is the compressed byte offset of that member in the S3
        // object; we Range-GET from there and decompress exactly one member.
        let Some(c_offset) = rec.c_offset else {
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "This record predates compressed-offset indexing. \
                 Re-index (tywb index --force) then restart the server.",
            )
                .into_response());
        };

        // Compressed member size is unknown, but always ≤ uncompressed size.
        // Fetch `length + 64 KiB` compressed bytes; GzDecoder stops at member end.
        let fetch_len = rec.length + 65536;
        let compressed = match get_range(
            &state.s3, &state.config.s3.bucket,
            &rec.s3_key, c_offset, fetch_len,
        ).await {
            Ok(b)  => b,
            Err(e) => {
                error!(err = %e, s3_key = %rec.s3_key, "S3 range GET failed");
                return Err((StatusCode::BAD_GATEWAY, e.to_string()).into_response());
            }
        };

        let mut gz = flate2::read::GzDecoder::new(compressed.as_ref());
        let mut decompressed = Vec::new();
        if let Err(e) = gz.read_to_end(&mut decompressed) {
            error!(err = %e, s3_key = %rec.s3_key, c_offset, "gzip decode failed");
            return Err((StatusCode::BAD_GATEWAY, format!("gzip decode: {e}")).into_response());
        }
        Ok(Payload::Warc(decompressed))
    } else {
        // ── Uncompressed ──────────────────────────────────────────────────────
        // offset is the byte position of the WARC record in the plain .warc file.
        // Fetch enough bytes to cover WARC headers (~1 KB) plus the HTTP block.
        let fetch_len = rec.length + 4096;
        match get_range(
            &state.s3, &state.config.s3.bucket,
            &rec.s3_key, rec.offset, fetch_len,
        ).await {
            Ok(b)  => Ok(Payload::Warc(b.to_vec())),
            Err(e) => {
                error!(err = %e, s3_key = %rec.s3_key, "S3 range GET failed");
                Err((StatusCode::BAD_GATEWAY, e.to_string()).into_response())
            }
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

    match fetch_payload(&state, &cdx_rec).await {
        // `warc_bytes` begins at the WARC record (WARC/1.0\r\n...).
        // Skip the WARC header block to get the HTTP response block.
        Ok(Payload::Warc(warc_bytes)) => serve_warc_response(
            extract_warc_http_block(&warc_bytes),
            &cdx_rec.original_url,
            &cdx_rec.timestamp,
        ),
        Ok(Payload::Object(bytes)) => {
            let ctype = cdx_rec.mime.as_deref().unwrap_or("application/octet-stream");
            (
                [(header::CONTENT_TYPE, HeaderValue::from_str(ctype)
                    .unwrap_or(HeaderValue::from_static("application/octet-stream")))],
                bytes,
            )
                .into_response()
        }
        Err(resp) => resp,
    }
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

// ── /text — plain text of one capture ────────────────────────────────────────
//
// The archive already knows how to turn a capture into readable text: the
// indexer strips HTML and pushes PDFs through Tika/OCR. This endpoint exposes
// exactly that pipeline, so a client does not have to re-implement HTML
// stripping, gzip decoding, or PDF extraction to work with archived content.
//
// The text is extracted on demand from the stored bytes, not read back out of
// the fulltext index — index bodies are truncated to `indexer.max_text_bytes`
// and hold no titles for text/plain records.

#[derive(Deserialize)]
struct TextParams {
    url:       Option<String>,
    /// 14-digit capture timestamp, or any prefix of one. Omitted → newest.
    timestamp: Option<String>,
    /// `text` (default) or `json`.
    output:    Option<String>,
}

/// `GET /text?url=<url>[&timestamp=<ts>][&output=json]`
async fn text_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TextParams>,
) -> Response {
    let Some(url) = params.url.as_deref().map(str::trim).filter(|u| !u.is_empty()) else {
        return (StatusCode::BAD_REQUEST, "url parameter required").into_response();
    };
    let as_json = params.output.as_deref().is_some_and(|o| o.eq_ignore_ascii_case("json"));
    text_response(&state, url, params.timestamp.as_deref(), as_json).await
}

/// `GET /text/<timestamp>/<url>` — the same lookup addressed like a replay URL,
/// so any `/web/…` link becomes text by swapping the prefix.
///
/// The query string belongs to the target URL in this form (as it does for
/// `/web/`), leaving no room for an `output` parameter: this form always
/// answers `text/plain`. Use `/text?url=…&output=json` for JSON.
async fn text_path_handler(
    State(state): State<Arc<AppState>>,
    uri: Uri,
    Path(rest): Path<String>,
) -> Response {
    let Some(slash) = rest.find('/') else {
        return (StatusCode::BAD_REQUEST, "expected /text/<timestamp>/<url>").into_response();
    };
    let timestamp = rest[..slash].to_owned();
    let url = match uri.query() {
        Some(q) => format!("{}?{q}", &rest[slash + 1..]),
        None    => rest[slash + 1..].to_owned(),
    };
    text_response(&state, &url, Some(&timestamp), false).await
}

async fn text_response(
    state: &Arc<AppState>,
    url: &str,
    timestamp: Option<&str>,
    as_json: bool,
) -> Response {
    let normalised = if url.contains("://") { url.to_owned() } else { format!("https://{url}") };
    let surt = match to_surt(&normalised) {
        Ok(s) => s,
        Err(e) => {
            warn!(url, err = %e, "invalid URL in /text request");
            return (StatusCode::BAD_REQUEST, format!("invalid URL: {e}")).into_response();
        }
    };

    debug!(url, ?timestamp, "text");

    let rec = match lookup_capture(state, &surt, timestamp) {
        Ok(r)              => r,
        Err((status, msg)) => return (status, msg).into_response(),
    };

    let payload = match fetch_payload(state, &rec).await {
        Ok(p)     => p,
        Err(resp) => return resp,
    };

    let extracted = match extract_capture_text(state, &rec, payload).await {
        Ok(t)     => t,
        Err(resp) => return resp,
    };

    if as_json {
        return Json(serde_json::json!({
            "url":         rec.original_url,
            "timestamp":   rec.timestamp,
            "collection":  rec.collection,
            "status":      rec.status,
            "mime":        extracted.mime,
            "title":       extracted.title,
            "chars":       extracted.text.chars().count(),
            "low_quality": !extracted.quality_ok,
            "text":        extracted.text,
        }))
        .into_response();
    }

    // Plain text: the body is only the text, everything else is a header.
    let mut headers = HeaderMap::new();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));
    for (name, value) in [
        ("X-Archive-Orig-URL", rec.original_url.as_str()),
        ("X-Tywb-Timestamp",   rec.timestamp.as_str()),
        ("X-Tywb-Collection",  rec.collection.as_str()),
        ("X-Tywb-Mime",        extracted.mime.as_str()),
    ] {
        if let Ok(hv) = HeaderValue::from_str(value) {
            headers.insert(name, hv);
        }
    }
    if !extracted.quality_ok {
        headers.insert("X-Tywb-Low-Quality", HeaderValue::from_static("1"));
    }

    (StatusCode::OK, headers, extracted.text).into_response()
}

/// The capture to extract: the one closest to `timestamp`, or — with no
/// timestamp — the most recent one.
fn lookup_capture(
    state: &Arc<AppState>,
    surt: &str,
    timestamp: Option<&str>,
) -> Result<CdxRecord, (StatusCode, String)> {
    let found = {
        let store = state.cdx.lock().unwrap();
        match timestamp.map(str::trim).filter(|t| !t.is_empty()) {
            Some(ts) => {
                if !ts.chars().all(|c| c.is_ascii_digit()) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "timestamp must be digits (YYYYMMDDHHMMSS or a prefix of it)".to_owned(),
                    ));
                }
                store.closest(surt, &pad_timestamp(ts))
            }
            // get_by_surt is timestamp-ascending, so the last row is the newest.
            None => store.get_by_surt(surt).map(|recs| recs.into_iter().next_back()),
        }
    };

    match found {
        Ok(Some(r)) => Ok(r),
        Ok(None)    => Err((StatusCode::NOT_FOUND, "not found in CDX index".to_owned())),
        Err(e) => {
            error!(err = %e, "CDX lookup failed");
            Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    }
}

/// Pad a partial timestamp to the 14 digits the CDX store expects.
/// Missing month and day default to `01`, missing time fields to `00`, so
/// `2024` means 2024-01-01 00:00:00.
fn pad_timestamp(ts: &str) -> String {
    const TEMPLATE: &str = "00010101000000";
    if ts.len() >= TEMPLATE.len() {
        return ts[..TEMPLATE.len()].to_owned();
    }
    format!("{ts}{}", &TEMPLATE[ts.len()..])
}

/// The plain text of one capture and what it was derived from.
struct CaptureText {
    title: String,
    text:  String,
    /// The content type the text was extracted from.
    mime:  String,
    /// PDFs only: `false` when the text failed the OCR quality gate. Such text
    /// is still returned — a caller asking for one named document can judge
    /// OCR noise itself, whereas the index cannot.
    quality_ok: bool,
}

async fn extract_capture_text(
    state: &Arc<AppState>,
    rec: &CdxRecord,
    payload: Payload,
) -> Result<CaptureText, Response> {
    let (mime, body): (String, Vec<u8>) = match payload {
        // A collection object is the file itself — no HTTP envelope around it.
        Payload::Object(bytes) => (
            rec.mime.clone().unwrap_or_else(|| "application/octet-stream".to_owned()),
            bytes,
        ),
        Payload::Warc(warc_bytes) => {
            let (ctype, encoding, http_body) =
                parse_http_block(extract_warc_http_block(&warc_bytes));
            let mime = ctype
                .or_else(|| rec.mime.clone())
                .unwrap_or_else(|| "application/octet-stream".to_owned());
            // WARC response records hold the body exactly as it came off the
            // wire, which for most servers means gzip.
            let body = decode_content_encoding(http_body, encoding.as_deref())
                .map_err(|e| (StatusCode::UNSUPPORTED_MEDIA_TYPE, e).into_response())?;
            (mime, body)
        }
    };

    let essence = mime.split(';').next().unwrap_or("").trim().to_ascii_lowercase();

    let is_html = essence.starts_with("text/html")
        || essence.starts_with("application/xhtml")
        || essence.starts_with("text/xml")
        || essence.starts_with("application/xml");

    if is_html {
        // Decoded lossily as UTF-8, exactly as the indexer does. Pages in a
        // legacy single-byte charset lose their non-ASCII characters.
        let raw = String::from_utf8_lossy(&body);
        return Ok(CaptureText {
            title: crate::index::extract_title(&raw),
            text:  crate::index::strip_html(&raw),
            mime,
            quality_ok: true,
        });
    }

    if essence.starts_with("text/") {
        let raw = String::from_utf8_lossy(&body).into_owned();
        let title = raw.lines().next().unwrap_or("").trim().chars().take(256).collect();
        return Ok(CaptureText { title, text: raw, mime, quality_ok: true });
    }

    if essence == "application/pdf" {
        let Some(extractor) = state.pdf.clone() else {
            return Err((
                StatusCode::NOT_IMPLEMENTED,
                "PDF text extraction is not configured — set indexer.tika",
            )
                .into_response());
        };
        let url = rec.original_url.clone();
        // Tika's client is blocking, and extraction of a big scan is slow.
        let result = tokio::task::spawn_blocking(move || {
            // allow_truncated: a single on-demand request can afford the OCR
            // attempt on a cut-off PDF that the bulk indexer skips.
            extractor.try_extract(&url, &body, true)
        })
        .await;

        return match result {
            Ok(Ok(doc)) => Ok(CaptureText {
                title: doc.title,
                text:  doc.body,
                mime,
                quality_ok: doc.quality_ok,
            }),
            Ok(Err(e)) => {
                use crate::pdf::ExtractError;
                let status = match e {
                    ExtractError::TooLarge { .. }               => StatusCode::PAYLOAD_TOO_LARGE,
                    ExtractError::Tika(_)                       => StatusCode::BAD_GATEWAY,
                    ExtractError::Empty | ExtractError::Truncated => StatusCode::UNPROCESSABLE_ENTITY,
                };
                warn!(url = %rec.original_url, err = %e, "PDF text extraction failed");
                Err((status, e.to_string()).into_response())
            }
            Err(e) => {
                error!(err = %e, "PDF extraction task panicked");
                Err((StatusCode::INTERNAL_SERVER_ERROR, "extraction task failed").into_response())
            }
        };
    }

    Err((
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        format!("no text extractor for {essence} — use /web/ to fetch the raw capture"),
    )
        .into_response())
}

/// Split an HTTP response block into `(content-type, content-encoding, body)`.
///
/// `block` must start with the status line; a block without a header/body
/// separator is treated as all body, which is what a stray `resource` record
/// without HTTP framing looks like.
fn parse_http_block(block: &[u8]) -> (Option<String>, Option<String>, &[u8]) {
    let sep = b"\r\n\r\n";
    let Some(body_start) = block.windows(4).position(|w| w == sep).map(|i| i + 4) else {
        return (None, None, block);
    };
    let header_str = String::from_utf8_lossy(&block[..body_start - 4]);

    let mut content_type = None;
    let mut encoding     = None;
    for line in header_str.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else { continue };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-type"     => content_type = Some(value.trim().to_owned()),
            "content-encoding" => encoding     = Some(value.trim().to_ascii_lowercase()),
            _ => {}
        }
    }
    (content_type, encoding, &block[body_start..])
}

/// Undo the `Content-Encoding` of a stored HTTP body.
///
/// Returns `Err` with a client-facing message for encodings we cannot decode,
/// rather than handing back compressed bytes that would look like broken text.
fn decode_content_encoding(body: &[u8], encoding: Option<&str>) -> Result<Vec<u8>, String> {
    let Some(enc) = encoding else { return Ok(body.to_vec()) };
    match enc {
        "" | "identity" | "none" => Ok(body.to_vec()),
        "gzip" | "x-gzip" => {
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(body)
                .read_to_end(&mut out)
                .map_err(|e| format!("gzip body decode failed: {e}"))?;
            Ok(out)
        }
        "deflate" => {
            // "deflate" on the wire is zlib-wrapped in theory and raw deflate
            // in practice; try both before giving up.
            let mut out = Vec::new();
            if flate2::read::ZlibDecoder::new(body).read_to_end(&mut out).is_ok() {
                return Ok(out);
            }
            out.clear();
            flate2::read::DeflateDecoder::new(body)
                .read_to_end(&mut out)
                .map_err(|e| format!("deflate body decode failed: {e}"))?;
            Ok(out)
        }
        other => Err(format!("unsupported Content-Encoding: {other}")),
    }
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

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{decode_content_encoding, pad_timestamp, parse_http_block};
    use std::io::Write as _;

    #[test]
    fn pad_timestamp_fills_from_the_start_of_the_period() {
        assert_eq!(pad_timestamp("2024"),           "20240101000000");
        assert_eq!(pad_timestamp("202403"),         "20240301000000");
        assert_eq!(pad_timestamp("20240315"),       "20240315000000");
        assert_eq!(pad_timestamp("20240315120000"), "20240315120000");
        // Over-long input is cut, not rejected — the CDX store validates it.
        assert_eq!(pad_timestamp("202403151200009"), "20240315120000");
    }

    #[test]
    fn parse_http_block_reads_type_and_encoding() {
        let block = b"HTTP/1.1 200 OK\r\n\
                      Content-Type: text/html; charset=utf-8\r\n\
                      Content-Encoding: GZIP\r\n\r\nbody bytes";
        let (ctype, enc, body) = parse_http_block(block);
        assert_eq!(ctype.as_deref(), Some("text/html; charset=utf-8"));
        assert_eq!(enc.as_deref(), Some("gzip"));   // lowercased for matching
        assert_eq!(body, b"body bytes");
    }

    #[test]
    fn parse_http_block_without_headers_is_all_body() {
        let (ctype, enc, body) = parse_http_block(b"just bytes");
        assert!(ctype.is_none() && enc.is_none());
        assert_eq!(body, b"just bytes");
    }

    #[test]
    fn decode_content_encoding_handles_gzip_and_identity() {
        let plain = b"Roter Berlepsch";
        assert_eq!(decode_content_encoding(plain, None).unwrap(), plain);
        assert_eq!(decode_content_encoding(plain, Some("identity")).unwrap(), plain);

        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        enc.write_all(plain).unwrap();
        let gz = enc.finish().unwrap();
        assert_eq!(decode_content_encoding(&gz, Some("gzip")).unwrap(), plain);
    }

    #[test]
    fn decode_content_encoding_handles_both_flavours_of_deflate() {
        let plain = b"Boskoop";

        let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
        zlib.write_all(plain).unwrap();
        assert_eq!(
            decode_content_encoding(&zlib.finish().unwrap(), Some("deflate")).unwrap(),
            plain,
        );

        // Servers that send raw deflate despite the spec.
        let mut raw = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
        raw.write_all(plain).unwrap();
        assert_eq!(
            decode_content_encoding(&raw.finish().unwrap(), Some("deflate")).unwrap(),
            plain,
        );
    }

    #[test]
    fn decode_content_encoding_rejects_what_it_cannot_decode() {
        // Better a clear error than compressed bytes masquerading as text.
        let err = decode_content_encoding(b"\x1b\x2f", Some("br")).unwrap_err();
        assert!(err.contains("br"), "{err}");
    }
}
