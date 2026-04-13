//! `tywb server` — HTTP API + Wayback-compatible replay server.
//!
//! # Endpoints
//!
//! | Method | Path               | Description             |
//! |--------|--------------------|-------------------------|
//! | GET    | `/search`          | Fulltext search         |
//! | GET    | `/web/<ts>/<url>`  | Wayback replay          |
//! | GET    | `/cdx`             | CDX API (JSON)          |
//! | GET    | `/healthz`         | Health check            |

use std::sync::{Arc, Mutex};

use axum::{
    Router,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
    Json,
};
use serde::Deserialize;
use tower_http::trace::TraceLayer;
use tracing::{debug, error, info, warn};

use warc_search_cdx::{CdxStore, surt::to_surt};
use warc_search_config::Config;
use warc_search_s3::{build_client, get_range};
use warc_search_search::SearchReader;

// ── Application state ─────────────────────────────────────────────────────────

struct AppState {
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
        .route("/healthz",   get(health_handler))
        .route("/search",    get(search_handler))
        .route("/web/*rest", get(replay_handler))
        .route("/cdx",       get(cdx_handler))
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

// ── /search ───────────────────────────────────────────────────────────────────

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

    let timestamp   = &rest[..slash];
    let url_path    = &rest[slash + 1..];
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
            Err(e)      => {
                error!(err = %e, "CDX closest lookup failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
            }
        }
    };

    let raw = match get_range(
        &state.s3, &state.config.s3.bucket,
        &cdx_rec.s3_key, cdx_rec.offset, cdx_rec.length,
    ).await {
        Ok(b)  => b,
        Err(e) => {
            error!(err = %e, s3_key = %cdx_rec.s3_key, "S3 range GET failed");
            return (StatusCode::BAD_GATEWAY, e.to_string()).into_response();
        }
    };

    serve_warc_response(&raw, &cdx_rec.original_url)
}

fn serve_warc_response(block: &[u8], original_url: &str) -> Response {
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

    let mut headers = HeaderMap::new();
    for line in lines {
        if line.is_empty() { break; }
        if let Some((name, val)) = line.split_once(':') {
            match name.trim().to_ascii_lowercase().as_str() {
                "content-type" => {
                    if let Ok(hv) = HeaderValue::from_str(val.trim()) {
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

    (status, headers, body.to_vec()).into_response()
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
