//! HTML page generation for the tywb web UI.
//!
//! Pure functions only — no axum, no async. Each function takes data and
//! returns a complete HTML document as a `String`.
//!
//! Design goals:
//! - Zero external dependencies (no JS framework, no CDN)
//! - Works without JavaScript enabled
//! - Inspired by Wayback Machine / pywb visual style

use std::collections::HashMap;

use warc_search_cdx::{BasicStats, CdxRecord, WarcFileRow};
use warc_search_search::SearchHit;

// ── CSS ───────────────────────────────────────────────────────────────────────
// Written as a plain string so the page_html builder can push it directly
// (format! cannot contain literal CSS because of the { } characters).

const CSS: &str = "
:root{
  --navy:#1e2d5f;--blue:#2563eb;--blue-lt:#dbeafe;
  --green:#15803d;--amber:#d97706;--red:#dc2626;
  --bg:#f1f5f9;--card:#fff;--border:#e2e8f0;
  --text:#0f172a;--muted:#64748b;
}
*,::before,::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:var(--bg);color:var(--text);line-height:1.5;font-size:15px}
a{color:var(--blue);text-decoration:none}
a:hover{text-decoration:underline}

/* ── header ── */
header{background:var(--navy);color:#fff;display:flex;align-items:center;
  gap:1.5rem;padding:0 1.5rem;height:54px;position:sticky;top:0;z-index:10;
  box-shadow:0 2px 6px rgba(0,0,0,.25)}
.logo{font-weight:800;font-size:1.3rem;color:#fff;flex-shrink:0;
  text-decoration:none!important}
.logo .sub{font-weight:400;font-size:.82rem;color:#93a8d8;margin-left:.4rem}
nav{display:flex;gap:.25rem;flex:1}
nav a{color:#93a8d8;font-size:.875rem;padding:.3rem .65rem;border-radius:6px;
  text-decoration:none!important}
nav a:hover,nav a.active{color:#fff;background:rgba(255,255,255,.12)}
.nav-right{margin-left:auto;display:flex;gap:.25rem}

/* ── layout ── */
.container{max-width:900px;margin:0 auto;padding:1.75rem 1rem}

/* ── stat cards ── */
.stat-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
  gap:.75rem;margin-bottom:1.25rem}
.stat-card{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:1rem 1.25rem}
.stat-card .n{font-size:1.65rem;font-weight:700;color:var(--navy);
  letter-spacing:-.5px;line-height:1.1}
.stat-card .l{font-size:.71rem;color:var(--muted);text-transform:uppercase;
  letter-spacing:.5px;margin-top:.2rem}

/* ── coverage bar ── */
.coverage{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:.7rem 1.25rem;margin-bottom:1.25rem;
  font-size:.875rem;color:var(--muted)}
.coverage strong{color:var(--text)}

/* ── two-column grid ── */
.cols{display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-bottom:1.25rem}
@media(max-width:580px){.cols{grid-template-columns:1fr}}

/* ── form cards ── */
.form-card{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:1.25rem 1.5rem}
.form-card h2{font-size:.95rem;font-weight:600;margin-bottom:.875rem}
.input-row{display:flex;gap:.4rem}
input[type=text],input[type=search]{
  flex:1;padding:.55rem .875rem;border:1px solid var(--border);
  border-radius:7px;font-size:.9rem;color:var(--text);background:#fff;width:100%}
input[type=text]:focus,input[type=search]:focus{
  outline:none;border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-lt)}
.date-row{display:flex;gap:.4rem;margin-top:.4rem}
.date-row input{font-size:.8rem}
.btn{padding:.55rem 1.1rem;background:var(--blue);color:#fff;border:none;
  border-radius:7px;font-size:.9rem;cursor:pointer;font-weight:500;
  white-space:nowrap}
.btn:hover{background:#1d4ed8}
.btn-sm{padding:.3rem .7rem;font-size:.8rem}

/* ── mime breakdown ── */
.mime-section{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:1.25rem 1.5rem;margin-bottom:1.25rem}
.mime-section h2{font-size:.95rem;font-weight:600;margin-bottom:.875rem}
.mime-row{display:grid;grid-template-columns:1fr auto;align-items:center;
  gap:.75rem;margin-bottom:.55rem;font-size:.8rem}
.mime-bar{height:4px;background:#e2e8f0;border-radius:2px;
  overflow:hidden;margin-top:.25rem}
.mime-fill{height:100%;background:var(--blue);border-radius:2px}
.mime-count{color:var(--muted);font-size:.78rem;text-align:right}

/* ── search results ── */
.search-top{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:1rem 1.25rem;margin-bottom:1rem}
.result-count{font-size:.85rem;color:var(--muted);margin-bottom:.875rem}
.result-list{display:flex;flex-direction:column;gap:.55rem}
.result{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:.875rem 1.1rem;
  display:flex;gap:.75rem;align-items:flex-start}
.result-body{flex:1;min-width:0}
.result-title{display:block;font-size:1rem;font-weight:600;color:var(--navy);
  margin-bottom:.2rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.result-title:hover{color:var(--blue);text-decoration:underline}
.result-url{font-size:.78rem;color:var(--green);
  word-break:break-all;margin-bottom:.3rem}
.result-meta{display:flex;gap:.4rem;flex-wrap:wrap;align-items:center;
  font-size:.75rem;color:var(--muted)}
.badge{background:var(--bg);border:1px solid var(--border);
  padding:.1rem .45rem;border-radius:4px;font-size:.72rem;white-space:nowrap}
.replay-btn{flex-shrink:0;padding:.32rem .7rem;background:var(--navy);
  color:#fff!important;border-radius:6px;font-size:.76rem;font-weight:500;
  text-decoration:none!important;display:inline-block}
.replay-btn:hover{background:#2d3e7e}
/* collection badges + filter + homepage cards */
.coll-badge{background:var(--blue-lt);color:var(--navy);border:1px solid #bcd0f5;
  padding:.1rem .45rem;border-radius:4px;font-size:.72rem;font-weight:600;
  white-space:nowrap;text-decoration:none!important}
a.coll-badge:hover{background:#cfe0fb}
.coll-clear{font-size:.78rem;color:var(--blue)}
.coll-section{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:1rem 1.25rem;margin-bottom:1.25rem}
.coll-section h2{font-size:.95rem;font-weight:600;margin-bottom:.75rem}
.coll-row{display:flex;flex-wrap:wrap;gap:.6rem}
.coll-card{display:flex;flex-direction:column;gap:.15rem;min-width:130px;
  background:var(--bg);border:1px solid var(--border);border-radius:8px;
  padding:.6rem .9rem;text-decoration:none!important}
.coll-card:hover{border-color:var(--blue)}
.coll-name{font-weight:600;color:var(--navy);font-size:.9rem}
.coll-cnt{font-size:.75rem;color:var(--muted)}
/* one row per domain — the rest of that domain's hits fold away */
.result-group{display:flex;flex-direction:column;gap:.25rem}
.more{margin-left:1.1rem}
.more>summary{cursor:pointer;font-size:.78rem;color:var(--blue);
  padding:.15rem .2rem;list-style:none;width:fit-content}
.more>summary::-webkit-details-marker{display:none}
.more>summary::before{content:'▸ '}
.more[open]>summary::before{content:'▾ '}
.more>summary:hover{text-decoration:underline}
.more-list{display:flex;flex-direction:column;gap:.15rem;
  margin:.2rem 0 .3rem;padding-left:.9rem;border-left:2px solid var(--border)}
.more-item{display:flex;gap:.5rem;align-items:baseline;padding:.2rem .3rem;
  border-radius:5px;text-decoration:none!important}
.more-item:hover{background:var(--card)}
.more-title{font-size:.82rem;color:var(--navy);font-weight:500;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex-shrink:1}
.more-url{font-size:.72rem;color:var(--green);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1;min-width:0}
.more-ts{font-size:.72rem;color:var(--muted);white-space:nowrap;flex-shrink:0}

/* ── captures table ── */
.cap-header{margin-bottom:.75rem}
.cap-url{font-size:.95rem;font-weight:600;color:var(--green);
  word-break:break-all;margin-top:.2rem}
.cap-count{font-size:.85rem;color:var(--muted);margin-bottom:.875rem}
.table-wrap{background:var(--card);border:1px solid var(--border);
  border-radius:10px;overflow:hidden}
table{width:100%;border-collapse:collapse}
th{padding:.5rem .875rem;text-align:left;font-size:.7rem;
  text-transform:uppercase;letter-spacing:.5px;color:var(--muted);
  border-bottom:2px solid var(--border);background:var(--bg)}
td{padding:.5rem .875rem;font-size:.875rem;
  border-bottom:1px solid var(--border);vertical-align:middle}
tr:last-child td{border-bottom:none}
.ts{font-family:ui-monospace,SFMono-Regular,monospace;
  font-size:.8rem;white-space:nowrap}
.num{text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}
.s200{color:var(--green);font-weight:600}
.s3xx{color:var(--amber);font-weight:600}
.s4xx,.s5xx{color:var(--red);font-weight:600}
.s-none{color:var(--muted)}

/* ── warc file list ── */
.warc-list{display:flex;flex-direction:column;gap:.5rem}
.warc-item{background:var(--card);border:1px solid var(--border);
  border-radius:9px;padding:.75rem 1rem;font-size:.82rem}
.warc-key{font-family:ui-monospace,SFMono-Regular,monospace;
  font-size:.78rem;color:var(--muted);word-break:break-all;margin-bottom:.35rem}
.warc-meta{display:flex;gap:.75rem;flex-wrap:wrap;color:var(--muted);font-size:.78rem}
.warc-stat{display:flex;gap:.25rem;align-items:center}
.pill{background:var(--bg);border:1px solid var(--border);
  padding:.05rem .4rem;border-radius:4px;font-size:.72rem}
.pill-blue{background:var(--blue-lt);border-color:#93c5fd;color:var(--blue)}

/* ── domain browse ── */
.breadcrumb{font-size:.82rem;color:var(--muted);margin-bottom:1rem;display:flex;gap:.35rem;align-items:center;flex-wrap:wrap}
.breadcrumb a{color:var(--blue)}
.breadcrumb .sep{color:var(--border)}
.browse-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.55rem;margin-bottom:1.25rem}
.browse-card{background:var(--card);border:1px solid var(--border);border-radius:9px;
  padding:.65rem 1rem;display:flex;justify-content:space-between;align-items:center;
  transition:border-color .15s,box-shadow .15s}
.browse-card:hover{border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-lt);text-decoration:none!important}
.browse-card .domain{font-size:.9rem;font-weight:600;color:var(--navy);word-break:break-all}
.browse-card .cnt{font-size:.78rem;color:var(--muted);white-space:nowrap;margin-left:.5rem;flex-shrink:0}
.tld-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:.55rem;margin-bottom:1.25rem}
.tld-card{background:var(--card);border:1px solid var(--border);border-radius:9px;
  padding:.65rem 1rem;text-align:center;transition:border-color .15s,box-shadow .15s}
.tld-card:hover{border-color:var(--blue);box-shadow:0 0 0 3px var(--blue-lt);text-decoration:none!important}
.tld-card .tld-name{font-size:1.05rem;font-weight:700;color:var(--navy);display:block}
.tld-card .tld-cnt{font-size:.78rem;color:var(--muted)}
.section-head{font-size:.95rem;font-weight:600;margin-bottom:.75rem;color:var(--text)}
.more-note{font-size:.8rem;color:var(--muted);margin-top:.5rem}

/* ── query language help ── */
.ql-help{background:var(--card);border:1px solid var(--border);
  border-radius:10px;padding:.65rem 1.1rem;margin-bottom:1rem;font-size:.875rem}
.ql-help summary{cursor:pointer;color:var(--muted);user-select:none;
  font-size:.82rem;padding:.15rem 0}
.ql-help summary:hover{color:var(--text)}
.ql-body{margin-top:.75rem}
.ql-table{width:100%;border-collapse:collapse;margin-bottom:.6rem}
.ql-table th{padding:.3rem .65rem;text-align:left;font-size:.7rem;
  text-transform:uppercase;letter-spacing:.5px;color:var(--muted);
  border-bottom:2px solid var(--border)}
.ql-table td{padding:.3rem .65rem;font-size:.8rem;
  border-bottom:1px solid var(--border);vertical-align:top}
.ql-table tr:last-child td{border-bottom:none}
.ql-table code{background:var(--bg);padding:.05rem .3rem;border-radius:4px;
  font-size:.8rem}
.ql-note{font-size:.78rem;color:var(--muted);margin-top:.4rem}

/* ── empty / error states ── */
.empty{text-align:center;padding:3rem 1rem;color:var(--muted)}
.empty-icon{font-size:2rem;margin-bottom:.5rem}
.error{background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;
  padding:.75rem 1rem;color:var(--red);margin-bottom:1rem;font-size:.875rem}

/* ── calendar heat-map strip ── */
.heatmap{margin-bottom:1.25rem}
.heatmap h2{font-size:.95rem;font-weight:600;margin-bottom:.75rem}
.year-grid{display:flex;gap:2px;flex-wrap:wrap}
.year-cell{width:14px;height:14px;border-radius:2px;background:#e2e8f0;
  flex-shrink:0}
.yr0{background:#e2e8f0}
.yr1{background:#93c5fd}
.yr2{background:#3b82f6}
.yr3{background:#1d4ed8}
.yr4{background:var(--navy)}
";

// ── Page shell ────────────────────────────────────────────────────────────────

/// Wrap page content in the full HTML shell (doctype, head, header, container).
fn page_html(title: &str, active: &str, content: &str) -> String {
    let mut out = String::with_capacity(CSS.len() + content.len() + 2048);

    out.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n");
    out.push_str("<meta charset=\"utf-8\">\n");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n");
    out.push_str("<title>");
    push_esc(&mut out, title);
    out.push_str(" — tywb</title>\n<style>\n");
    out.push_str(CSS);
    out.push_str("</style>\n</head>\n<body>\n");

    // ── Sticky header ──────────────────────────────────────────────────────
    out.push_str("<header>\n");
    out.push_str("  <a href=\"/\" class=\"logo\">tywb<span class=\"sub\">Tiny Wayback</span></a>\n");
    out.push_str("  <nav>\n");
    nav_link(&mut out, "/",           "Home",       active == "home");
    nav_link(&mut out, "/ui/search",  "Search",     active == "search");
    nav_link(&mut out, "/ui/browse",  "Browse",     active == "browse");
    nav_link(&mut out, "/ui/url",     "By URL",     active == "url");
    nav_link(&mut out, "/ui/files",   "WARC Files", active == "files");
    nav_link(&mut out, "/ui/skiplist", "Skip List", active == "skiplist");
    nav_link(&mut out, "/ui/stats",   "Statistics", active == "stats");
    out.push_str("    <span class=\"nav-right\">");
    out.push_str("<a href=\"/api/stats\">API</a>");
    out.push_str("</span>\n");
    out.push_str("  </nav>\n</header>\n");

    out.push_str("<div class=\"container\">\n");
    out.push_str(content);
    out.push_str("\n</div>\n</body>\n</html>");
    out
}

fn nav_link(out: &mut String, href: &str, label: &str, active: bool) {
    out.push_str("    <a href=\"");
    out.push_str(href);
    out.push_str(if active { "\" class=\"active\">" } else { "\">" });
    out.push_str(label);
    out.push_str("</a>\n");
}

// ── HTML helpers ──────────────────────────────────────────────────────────────

fn push_esc(out: &mut String, s: &str) {
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            c   => out.push(c),
        }
    }
}

// ── Formatting helpers ────────────────────────────────────────────────────────

pub fn fmt_count(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 { out.push(','); }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn fmt_ts(ts: &str) -> String {
    if ts.len() == 14 {
        format!("{}-{}-{} {}:{}:{}",
            &ts[0..4], &ts[4..6], &ts[6..8],
            &ts[8..10], &ts[10..12], &ts[12..14])
    } else {
        ts.to_owned()
    }
}

fn fmt_size(n: u64) -> String {
    if n >= 1_048_576 { format!("{:.1} MB", n as f64 / 1_048_576.0) }
    else if n >= 1024 { format!("{:.0} KB", n as f64 / 1024.0) }
    else               { format!("{n} B") }
}

fn status_class(status: Option<u16>) -> &'static str {
    match status {
        Some(s) if s < 300 => "s200",
        Some(s) if s < 400 => "s3xx",
        Some(s) if s < 500 => "s4xx",
        Some(_)            => "s5xx",
        None               => "s-none",
    }
}

fn status_str(status: Option<u16>) -> String {
    status.map(|s| s.to_string()).unwrap_or_else(|| "—".to_owned())
}

fn short_mime<'a>(mime: Option<&'a str>) -> &'a str {
    mime.unwrap_or("—").split(';').next().unwrap_or("—").trim()
}

// ── Homepage ──────────────────────────────────────────────────────────────────

/// The homepage.
///
/// Deliberately built from scalar counts alone — no `GROUP BY` runs here. The
/// breakdowns this page used to carry (content types, HTTP status, collections)
/// cost seconds each on a large archive, and because every handler shares one
/// SQLite connection, that cost was charged to replay and search as well as to
/// whoever loaded `/`. They live on [`stats_html`] now, which runs them
/// concurrently on connections of its own.
pub fn homepage_html(stats: &BasicStats, num_docs: u64, collections: &[(String, u64)]) -> String {
    let mut c = String::with_capacity(4096);

    // ── Stat cards ────────────────────────────────────────────────────────
    c.push_str("<div class=\"stat-grid\">\n");
    stat_card(&mut c, &fmt_count(stats.total_records), "CDX records");
    stat_card(&mut c, &fmt_count(stats.unique_urls),   "Unique URLs");
    stat_card(&mut c, &fmt_count(stats.warc_files),    "WARC files");
    stat_card(&mut c, &fmt_count(num_docs),            "Fulltext docs");
    c.push_str("</div>\n");

    // ── Collections ───────────────────────────────────────────────────────
    // Cheap here on purpose: counted per configured name through
    // idx_cdx_collection, with the archive itself derived by subtraction.
    // See server::collection_cards.
    push_collection_cards(&mut c, collections);

    // ── Date coverage ─────────────────────────────────────────────────────
    match (&stats.oldest_timestamp, &stats.newest_timestamp) {
        (Some(a), Some(b)) => {
            c.push_str("<div class=\"coverage\">Archive coverage: <strong>");
            push_esc(&mut c, &fmt_ts(a));
            c.push_str("</strong> &nbsp;\u{2192}&nbsp; <strong>");
            push_esc(&mut c, &fmt_ts(b));
            c.push_str("</strong> &nbsp;\u{00b7}&nbsp; <a href=\"/ui/stats\">Collections, content types and status codes</a></div>\n");
        }
        _ => {
            c.push_str("<div class=\"coverage\">No data indexed yet. Run <code>tywb index</code> to ingest WARC files.</div>\n");
        }
    }

    // ── Two-column forms ──────────────────────────────────────────────────
    c.push_str("<div class=\"cols\">\n");

    c.push_str("<div class=\"form-card\">\n<h2>Search archived pages</h2>\n");
    c.push_str("<form action=\"/ui/search\" method=\"get\">\n");
    c.push_str("  <div class=\"input-row\">\n");
    c.push_str("    <input name=\"q\" type=\"search\" placeholder=\"search terms\u{2026}\" autocomplete=\"off\">\n");
    c.push_str("    <button class=\"btn\" type=\"submit\">Search</button>\n");
    c.push_str("  </div>\n");
    c.push_str("  <div class=\"date-row\">\n");
    c.push_str("    <input name=\"from\" type=\"text\" placeholder=\"From YYYYMMDD\" maxlength=\"8\">\n");
    c.push_str("    <input name=\"to\"   type=\"text\" placeholder=\"To YYYYMMDD\"   maxlength=\"8\">\n");
    c.push_str("  </div>\n");
    c.push_str("</form>\n</div>\n");

    c.push_str("<div class=\"form-card\">\n<h2>Browse by URL</h2>\n");
    c.push_str("<form action=\"/ui/url\" method=\"get\">\n");
    c.push_str("  <div class=\"input-row\">\n");
    c.push_str("    <input name=\"url\" type=\"text\" placeholder=\"https://example.com/\" autocomplete=\"off\">\n");
    c.push_str("    <button class=\"btn\" type=\"submit\">Captures</button>\n");
    c.push_str("  </div>\n");
    c.push_str("</form>\n</div>\n");

    c.push_str("</div>\n"); // end cols

    page_html("Home", "home", &c)
}

/// The statistics page: everything the homepage no longer computes.
///
/// `elapsed_ms` is how long the queries took *together*, which is roughly the
/// slowest one rather than their sum — they run concurrently, each on its own
/// read-only connection. It is shown because it is the number that decides
/// whether this page ever belongs on the request path of something else.
pub fn stats_html(
    stats: &BasicStats,
    num_docs: u64,
    collections: &[(String, u64)],
    mime_counts: &[(String, u64)],
    status_counts: &[(Option<u16>, u64)],
    elapsed_ms: u128,
) -> String {
    let mut c = String::with_capacity(8192);

    c.push_str("<div class=\"stat-grid\">\n");
    stat_card(&mut c, &fmt_count(stats.total_records), "CDX records");
    stat_card(&mut c, &fmt_count(stats.unique_urls),   "Unique URLs");
    stat_card(&mut c, &fmt_count(stats.warc_files),    "WARC files");
    stat_card(&mut c, &fmt_count(num_docs),            "Fulltext docs");
    c.push_str("</div>\n");

    // ── Collections ───────────────────────────────────────────────────────
    push_collection_cards(&mut c, collections);

    // ── Content types breakdown ───────────────────────────────────────────
    if !mime_counts.is_empty() {
        let max_n = mime_counts.first().map(|(_, n)| *n).unwrap_or(1).max(1);
        c.push_str("<div class=\"mime-section\">\n<h2>Content types</h2>\n");
        for (mime, count) in mime_counts.iter().take(12) {
            let pct = (*count as f64 / max_n as f64 * 100.0) as u32;
            c.push_str("  <div class=\"mime-row\">\n    <div>\n      <div style=\"font-size:.8rem\">");
            push_esc(&mut c, mime);
            c.push_str("</div>\n      <div class=\"mime-bar\"><div class=\"mime-fill\" style=\"width:");
            push_esc(&mut c, &pct.to_string());
            c.push_str("%\"></div></div>\n    </div>\n    <div class=\"mime-count\">");
            push_esc(&mut c, &fmt_count(*count));
            c.push_str("</div>\n  </div>\n");
        }
        c.push_str("</div>\n");
    }

    // ── HTTP status breakdown ─────────────────────────────────────────────
    if !status_counts.is_empty() {
        c.push_str("<div class=\"form-card\" style=\"margin-bottom:1.25rem\">\n<h2>HTTP status codes</h2>\n");
        c.push_str("<div style=\"display:flex;gap:1rem;flex-wrap:wrap;margin-top:.25rem\">\n");
        for (status, count) in status_counts {
            let sc  = status_class(*status);
            let ss  = status_str(*status);
            c.push_str("  <div style=\"display:flex;align-items:center;gap:.4rem\">\n");
            c.push_str("    <span class=\"");
            c.push_str(sc);
            c.push_str("\" style=\"font-weight:700\">");
            push_esc(&mut c, &ss);
            c.push_str("</span>\n    <span style=\"color:var(--muted);font-size:.82rem\">");
            push_esc(&mut c, &fmt_count(*count));
            c.push_str("</span>\n  </div>\n");
        }
        c.push_str("</div>\n</div>\n");
    }

    c.push_str("<div class=\"coverage\">Four aggregate queries over the whole CDX table, run in parallel: <strong>");
    push_esc(&mut c, &elapsed_ms.to_string());
    c.push_str("\u{202f}ms</strong>. They are kept off the homepage for that reason.</div>\n");

    page_html("Statistics", "stats", &c)
}

/// The collection cards, shown on both the homepage and the statistics page.
///
/// Only worth showing once there is more than the default WARC archive — a
/// single card saying "everything" is noise.
fn push_collection_cards(out: &mut String, collections: &[(String, u64)]) {
    if !collections.iter().any(|(name, _)| name != "warc") {
        return;
    }
    out.push_str("<div class=\"coll-section\">\n<h2>Collections</h2>\n<div class=\"coll-row\">\n");
    for (name, n) in collections {
        out.push_str("  <a class=\"coll-card\" href=\"/ui/search?q=&collection=");
        push_url_encoded(out, name);
        out.push_str("\">\n    <span class=\"coll-name\">");
        push_esc(out, name);
        out.push_str("</span>\n    <span class=\"coll-cnt\">");
        push_esc(out, &fmt_count(*n));
        out.push_str("</span>\n  </a>\n");
    }
    out.push_str("</div>\n</div>\n");
}

fn stat_card(out: &mut String, value: &str, label: &str) {
    out.push_str("  <div class=\"stat-card\"><div class=\"n\">");
    out.push_str(value);
    out.push_str("</div><div class=\"l\">");
    out.push_str(label);
    out.push_str("</div></div>\n");
}

// ── Search results ────────────────────────────────────────────────────────────

pub fn search_html(
    q: &str,
    hits: &[SearchHit],
    from: Option<&str>,
    to: Option<&str>,
    collection: Option<&str>,
    error: bool,
) -> String {
    let mut c = String::with_capacity(4096 + hits.len() * 512);

    // ── Search form (always visible at top) ───────────────────────────────
    c.push_str("<div class=\"search-top\">\n");
    c.push_str("<form action=\"/ui/search\" method=\"get\">\n");
    // Keep an active collection filter across searches.
    if let Some(coll) = collection.filter(|c| !c.is_empty()) {
        c.push_str("<input type=\"hidden\" name=\"collection\" value=\"");
        push_esc(&mut c, coll);
        c.push_str("\">\n");
    }
    c.push_str("  <div class=\"input-row\">\n");
    c.push_str("    <input name=\"q\" type=\"search\" value=\"");
    push_esc(&mut c, q);
    c.push_str("\" placeholder=\"search terms\u{2026}\" autocomplete=\"off\">\n");
    c.push_str("    <button class=\"btn\" type=\"submit\">Search</button>\n");
    c.push_str("  </div>\n");
    c.push_str("  <div class=\"date-row\">\n");
    c.push_str("    <input name=\"from\" type=\"text\" placeholder=\"From YYYYMMDD\" maxlength=\"8\" value=\"");
    push_esc(&mut c, from.unwrap_or(""));
    c.push_str("\">\n");
    c.push_str("    <input name=\"to\" type=\"text\" placeholder=\"To YYYYMMDD\" maxlength=\"8\" value=\"");
    push_esc(&mut c, to.unwrap_or(""));
    c.push_str("\">\n");
    c.push_str("  </div>\n</form>\n</div>\n");

    // ── Query language help ───────────────────────────────────────────────
    c.push_str(r#"<details class="ql-help">
<summary>Query language</summary>
<div class="ql-body">
<table class="ql-table">
<tr><th>Syntax</th><th>Meaning</th><th>Example</th></tr>
<tr><td><code>word</code></td><td>Match either word in title or body (OR)</td><td><code>apple orange</code></td></tr>
<tr><td><code>word1 AND word2</code></td><td>Both words must appear</td><td><code>climate AND policy</code></td></tr>
<tr><td><code>word1 OR word2</code></td><td>Either word (explicit)</td><td><code>colour OR color</code></td></tr>
<tr><td><code>NOT word</code></td><td>Exclude word</td><td><code>python NOT snake</code></td></tr>
<tr><td><code>+word</code></td><td>Word must appear</td><td><code>+rust programming</code></td></tr>
<tr><td><code>-word</code></td><td>Word must not appear</td><td><code>java -coffee</code></td></tr>
<tr><td><code>"phrase"</code></td><td>Exact phrase</td><td><code>"machine learning"</code></td></tr>
<tr><td><code>title:word</code></td><td>Search in page title only</td><td><code>title:homepage</code></td></tr>
<tr><td><code>body:word</code></td><td>Search in page body only</td><td><code>body:contact</code></td></tr>
<tr><td><code>mime:type</code></td><td>Filter by MIME type (exact)</td><td><code>mime:text/html</code></td></tr>
</table>
<p class="ql-note">Use the <strong>From</strong> / <strong>To</strong> fields to restrict by capture date (YYYYMMDD). Default fields searched are <em>title</em> and <em>body</em>.</p>
</div>
</details>
"#);

    // No terms and nothing to show: the bare search page, just the form and the
    // syntax help. With hits in hand there *is* something to show even without
    // terms — browsing a collection is exactly that, and the collection cards on
    // the homepage link to it.
    if q.is_empty() && hits.is_empty() {
        return page_html("Search", "search", &c);
    }

    if error {
        c.push_str("<div class=\"error\">Search failed — please try again or check the server logs.</div>\n");
        return page_html("Search", "search", &c);
    }

    // ── Result count ──────────────────────────────────────────────────────
    // One row per domain: the newest capture is shown, the rest of that
    // domain's hits stay one click away.
    let groups = group_by_domain(hits);

    c.push_str("<p class=\"result-count\">");
    // "for <terms>" only when there are terms: browsing a collection has none,
    // and a dangling "for" with nothing after it reads like a truncated page.
    if hits.is_empty() {
        c.push_str("No results");
        if !q.is_empty() {
            c.push_str(" for <strong>");
            push_esc(&mut c, q);
            c.push_str("</strong>");
        }
        c.push('.');
    } else {
        push_esc(&mut c, &fmt_count(hits.len() as u64));
        c.push_str(if hits.len() == 1 { " result" } else { " results" });
        c.push_str(" from ");
        push_esc(&mut c, &fmt_count(groups.len() as u64));
        c.push_str(if groups.len() == 1 { " domain" } else { " domains" });
        if !q.is_empty() {
            c.push_str(" for <strong>");
            push_esc(&mut c, q);
            c.push_str("</strong>");
        }
    }
    // Active collection filter: show it with a one-click "clear".
    if let Some(coll) = collection.filter(|c| !c.is_empty()) {
        c.push_str(" · in collection <span class=\"coll-badge\">");
        push_esc(&mut c, coll);
        c.push_str("</span> <a class=\"coll-clear\" href=\"/ui/search?q=");
        push_url_encoded(&mut c, q);
        c.push_str("\">clear</a>");
    }
    c.push_str("</p>\n");

    // ── Results ───────────────────────────────────────────────────────────
    if !hits.is_empty() {
        c.push_str("<div class=\"result-list\">\n");
        for group in &groups {
            c.push_str("<div class=\"result-group\">\n");
            render_hit(&mut c, group.newest);
            render_more(&mut c, group);
            c.push_str("</div>\n");
        }
        c.push_str("</div>\n");
    }

    page_html("Search", "search", &c)
}

// ── Grouping by domain ────────────────────────────────────────────────────────

/// One domain's hits: the newest capture plus everything else, newest first.
struct DomainGroup<'a> {
    domain: String,
    newest: &'a SearchHit,
    rest:   Vec<&'a SearchHit>,
}

/// Collapse hits to one row per domain.
///
/// Domains keep the order in which the search engine first mentioned them, so
/// the most relevant site still comes first. Within a domain the newest capture
/// is what gets shown; the remaining hits stay available behind "more results".
///
/// Subdomains fold into their second-level domain, matching the
/// TLD → domain → subdomain hierarchy of `/ui/browse`.
fn group_by_domain(hits: &[SearchHit]) -> Vec<DomainGroup<'_>> {
    let mut order: Vec<String> = Vec::new();
    let mut buckets: HashMap<String, Vec<&SearchHit>> = HashMap::new();

    for hit in hits {
        let domain = url_domain(&hit.url);
        buckets.entry(domain.clone()).or_insert_with(|| {
            order.push(domain.clone());
            Vec::new()
        }).push(hit);
    }

    order
        .into_iter()
        .map(|domain| {
            let mut items = buckets.remove(&domain).unwrap_or_default();
            // Newest capture first, then the rest by descending timestamp.
            items.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
            let newest = items.remove(0);
            DomainGroup { domain, newest, rest: items }
        })
        .collect()
}

/// Second-level domain of a URL's host — the group key for search results.
///
/// `https://obstsorten.pomologen-verein.de/seite` → `pomologen-verein.de`
///
/// Hosts that are bare IP addresses or single labels are returned unchanged.
pub fn url_domain(url: &str) -> String {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .rsplit_once('@')
        .map_or_else(|| rest.split(['/', '?', '#']).next().unwrap_or(""), |(_, h)| h);
    // Strip a :port suffix (but leave bracketed IPv6 literals alone).
    let host = match host.rsplit_once(':') {
        Some((h, port)) if !h.ends_with(']') && port.chars().all(|c| c.is_ascii_digit()) => h,
        _ => host,
    };
    let host = host.trim_end_matches('.').to_ascii_lowercase();

    let labels: Vec<&str> = host.split('.').collect();
    let is_ipv4 = labels.len() == 4 && labels.iter().all(|l| l.parse::<u8>().is_ok());
    if labels.len() < 2 || is_ipv4 || host.starts_with('[') {
        return host;
    }
    labels[labels.len() - 2..].join(".")
}

/// Render the collapsed remainder of a domain group, if any.
fn render_more(out: &mut String, group: &DomainGroup<'_>) {
    if group.rest.is_empty() {
        return;
    }

    out.push_str("<details class=\"more\">\n  <summary>");
    push_esc(out, &fmt_count(group.rest.len() as u64));
    out.push_str(if group.rest.len() == 1 { " more result from " } else { " more results from " });
    push_esc(out, &group.domain);
    out.push_str("</summary>\n  <div class=\"more-list\">\n");

    for hit in &group.rest {
        let replay = format!("/web/{}/{}", hit.timestamp, hit.url);
        let title = if hit.title.is_empty() { hit.url.as_str() } else { hit.title.as_str() };

        out.push_str("    <a class=\"more-item\" href=\"");
        push_esc(out, &replay);
        out.push_str("\">\n      <span class=\"more-title\">");
        push_esc(out, title);
        out.push_str("</span>\n      <span class=\"more-url\">");
        push_esc(out, &hit.url);
        out.push_str("</span>\n      <span class=\"more-ts\">");
        push_esc(out, &fmt_ts(&hit.timestamp));
        out.push_str("</span>\n    </a>\n");
    }

    out.push_str("  </div>\n</details>\n");
}

fn render_hit(out: &mut String, hit: &SearchHit) {
    let replay = format!("/web/{}/{}", hit.timestamp, hit.url);
    let display_title = if hit.title.is_empty() { hit.url.as_str() } else { hit.title.as_str() };
    let mime = short_mime(hit.mime.as_deref());

    out.push_str("<div class=\"result\">\n  <div class=\"result-body\">\n");

    // Title → replay link
    out.push_str("    <a class=\"result-title\" href=\"");
    push_esc(out, &replay);
    out.push_str("\" title=\"");
    push_esc(out, display_title);
    out.push_str("\">");
    push_esc(out, display_title);
    out.push_str("</a>\n");

    // Green URL
    out.push_str("    <div class=\"result-url\">");
    push_esc(out, &hit.url);
    out.push_str("</div>\n");

    // Metadata badges
    out.push_str("    <div class=\"result-meta\">\n");
    out.push_str("      <span class=\"badge\">");
    push_esc(out, &fmt_ts(&hit.timestamp));
    out.push_str("</span>\n");
    if mime != "—" {
        out.push_str("      <span class=\"badge\">");
        push_esc(out, mime);
        out.push_str("</span>\n");
    }
    // Collection badge — only for non-default collections; links to a filtered
    // search of that collection.
    if let Some(coll) = hit.collection.as_deref() {
        if coll != warc_search_cdx::DEFAULT_COLLECTION && !coll.is_empty() {
            out.push_str("      <a class=\"coll-badge\" href=\"/ui/search?collection=");
            push_url_encoded(out, coll);
            out.push_str("&q=\" title=\"filter this collection\">");
            push_esc(out, coll);
            out.push_str("</a>\n");
        }
    }
    out.push_str("    </div>\n");

    out.push_str("  </div>\n");

    // Replay button
    out.push_str("  <a class=\"replay-btn\" href=\"");
    push_esc(out, &replay);
    out.push_str("\">Replay \u{2197}</a>\n");

    out.push_str("</div>\n");
}

// ── URL captures page ─────────────────────────────────────────────────────────

pub fn url_html(url: &str, records: &[CdxRecord], error: bool) -> String {
    let mut c = String::with_capacity(2048 + records.len() * 256);

    // ── URL lookup form ───────────────────────────────────────────────────
    c.push_str("<div class=\"search-top\">\n");
    c.push_str("<form action=\"/ui/url\" method=\"get\">\n");
    c.push_str("  <div class=\"input-row\">\n");
    c.push_str("    <input name=\"url\" type=\"text\" value=\"");
    push_esc(&mut c, url);
    c.push_str("\" placeholder=\"https://example.com/\" autocomplete=\"off\">\n");
    c.push_str("    <button class=\"btn\" type=\"submit\">Captures</button>\n");
    c.push_str("  </div>\n</form>\n</div>\n");

    if url.is_empty() {
        return page_html("Browse URL", "url", &c);
    }

    if error {
        c.push_str("<div class=\"error\">Lookup failed — please check the URL and try again.</div>\n");
        return page_html("Browse URL", "url", &c);
    }

    // ── URL heading ───────────────────────────────────────────────────────
    c.push_str("<div class=\"cap-header\"><div class=\"cap-url\">");
    push_esc(&mut c, url);
    c.push_str("</div></div>\n");

    if records.is_empty() {
        c.push_str("<div class=\"empty\"><div class=\"empty-icon\">\u{1f5c4}\u{fe0f}</div>No captures found for this URL.</div>\n");
        return page_html("Browse URL", "url", &c);
    }

    // ── Capture count ─────────────────────────────────────────────────────
    c.push_str("<p class=\"cap-count\">");
    push_esc(&mut c, &fmt_count(records.len() as u64));
    c.push_str(if records.len() == 1 { " capture" } else { " captures" });
    c.push_str("</p>\n");

    // ── Captures table (newest first) ─────────────────────────────────────
    c.push_str("<div class=\"table-wrap\">\n<table>\n");
    c.push_str("<thead><tr><th>Date / time</th><th>Status</th><th>Type</th><th>Size</th><th></th></tr></thead>\n<tbody>\n");

    for rec in records.iter().rev() {
        let sc = status_class(rec.status);
        let ss = status_str(rec.status);
        let mime = short_mime(rec.mime.as_deref());
        let replay = format!("/web/{}/{}", rec.timestamp, rec.original_url);

        c.push_str("<tr>\n  <td class=\"ts\">");
        push_esc(&mut c, &fmt_ts(&rec.timestamp));
        c.push_str("</td>\n  <td class=\"");
        c.push_str(sc);
        c.push_str("\">");
        push_esc(&mut c, &ss);
        c.push_str("</td>\n  <td>");
        push_esc(&mut c, mime);
        c.push_str("</td>\n  <td style=\"white-space:nowrap\">");
        push_esc(&mut c, &fmt_size(rec.length));
        c.push_str("</td>\n  <td><a class=\"replay-btn\" href=\"");
        push_esc(&mut c, &replay);
        c.push_str("\">Replay \u{2197}</a></td>\n</tr>\n");
    }

    c.push_str("</tbody>\n</table>\n</div>\n");

    page_html("Browse URL", "url", &c)
}

// ── WARC files page ───────────────────────────────────────────────────────────

pub fn files_html(files: &[WarcFileRow], error: bool) -> String {
    let mut c = String::with_capacity(2048 + files.len() * 512);

    c.push_str("<h1 style=\"font-size:1.15rem;font-weight:700;margin-bottom:1rem\">Indexed WARC files</h1>\n");

    if error {
        c.push_str("<div class=\"error\">Could not read WARC file metadata — is the CDX database open?</div>\n");
        return page_html("WARC Files", "files", &c);
    }

    if files.is_empty() {
        c.push_str("<div class=\"empty\"><div class=\"empty-icon\">\u{1f4e6}</div>No WARC files indexed yet.</div>\n");
        return page_html("WARC Files", "files", &c);
    }

    c.push_str("<div class=\"warc-list\">\n");
    for f in files {
        c.push_str("  <div class=\"warc-item\">\n");

        // S3 key
        c.push_str("    <div class=\"warc-key\">");
        push_esc(&mut c, &f.s3_key);
        c.push_str("</div>\n");

        // Stat pills
        c.push_str("    <div class=\"warc-meta\">\n");

        // Date
        if !f.last_indexed.is_empty() {
            c.push_str("      <span class=\"warc-stat\">\u{1f4c5} ");
            push_esc(&mut c, &f.last_indexed[..f.last_indexed.len().min(10)]);
            c.push_str("</span>\n");
        }

        // WARC records
        c.push_str("      <span class=\"warc-stat\">\u{1f4c4} ");
        push_esc(&mut c, &fmt_count(f.warc_records as u64));
        c.push_str(" records</span>\n");

        // CDX new / known
        c.push_str("      <span class=\"warc-stat\"><span class=\"pill pill-blue\">+");
        push_esc(&mut c, &fmt_count(f.cdx_new as u64));
        c.push_str(" new</span></span>\n");
        if f.cdx_known > 0 {
            c.push_str("      <span class=\"warc-stat\"><span class=\"pill\">");
            push_esc(&mut c, &fmt_count(f.cdx_known as u64));
            c.push_str(" known</span></span>\n");
        }

        // Fulltext
        if f.fulltext_indexed > 0 {
            c.push_str("      <span class=\"warc-stat\">\u{1f50d} ");
            push_esc(&mut c, &fmt_count(f.fulltext_indexed as u64));
            c.push_str(" indexed</span>\n");
        }

        // Size
        if let Some(sz) = f.size_bytes {
            c.push_str("      <span class=\"warc-stat\">");
            push_esc(&mut c, &fmt_size(sz as u64));
            c.push_str("</span>\n");
        }

        // Throughput
        if let (Some(dur), Some(rps)) = (f.duration_secs, f.records_per_sec) {
            c.push_str("      <span class=\"warc-stat\">");
            push_esc(&mut c, &format!("{:.0}s @ {:.0} rec/s", dur, rps));
            c.push_str("</span>\n");
        }

        // Errors
        if f.errors > 0 {
            c.push_str("      <span class=\"warc-stat\" style=\"color:var(--red)\">\u{26a0}\u{fe0f} ");
            push_esc(&mut c, &fmt_count(f.errors as u64));
            c.push_str(" errors</span>\n");
        }

        c.push_str("    </div>\n  </div>\n");
    }
    c.push_str("</div>\n");

    page_html("WARC Files", "files", &c)
}

// ── Domain browse ─────────────────────────────────────────────────────────────

/// Convert a SURT domain prefix back to a human-readable hostname.
/// `"com,example"` → `"example.com"`,  `"com,example,www"` → `"www.example.com"`
pub fn surt_to_domain(surt_prefix: &str) -> String {
    surt_prefix.split(',').rev().collect::<Vec<_>>().join(".")
}

/// Convert a human-readable hostname into a SURT domain prefix (with trailing `)`)
/// suitable for prefix queries.
/// `"example.com"` → `"com,example)"`,  `"www.example.com"` → `"com,example,www)"`
pub fn domain_to_surt_prefix(domain: &str) -> String {
    let parts: Vec<&str> = domain.split('.').rev().collect();
    format!("{})", parts.join(","))
}

/// Level 1 — list of TLDs.
pub fn browse_tlds_html(tlds: &[(String, u64)], truncated: bool) -> String {
    let mut c = String::with_capacity(4096 + tlds.len() * 80);
    c.push_str("<h1 class=\"section-head\">Browse archive by domain</h1>\n");

    if tlds.is_empty() {
        c.push_str("<div class=\"empty\"><div class=\"empty-icon\">\u{1f310}</div>No domains indexed yet.</div>\n");
        return page_html("Browse", "browse", &c);
    }

    c.push_str("<p style=\"font-size:.85rem;color:var(--muted);margin-bottom:.875rem\">");
    push_esc(&mut c, &fmt_count(tlds.len() as u64));
    c.push_str(" top-level domains — click to explore.</p>\n");

    c.push_str("<div class=\"tld-grid\">\n");
    for (tld, n) in tlds {
        c.push_str("  <a class=\"tld-card\" href=\"/ui/browse?tld=");
        // TLDs are safe ASCII; no escaping needed for URL, but escape for HTML
        push_esc(&mut c, tld);
        c.push_str("\">\n    <span class=\"tld-name\">.");
        push_esc(&mut c, tld);
        c.push_str("</span>\n    <span class=\"tld-cnt\">");
        push_esc(&mut c, &fmt_count(*n));
        c.push_str("</span>\n  </a>\n");
    }
    c.push_str("</div>\n");

    if truncated {
        c.push_str("<p class=\"more-note\">Showing top TLDs by record count.</p>\n");
    }

    page_html("Browse", "browse", &c)
}

/// Level 2 — list of hostnames under a TLD.
pub fn browse_domains_html(tld: &str, domains: &[(String, u64)], truncated: bool) -> String {
    let mut c = String::with_capacity(4096 + domains.len() * 120);

    // Breadcrumb
    c.push_str("<div class=\"breadcrumb\">\n");
    c.push_str("  <a href=\"/ui/browse\">Browse</a>\n");
    c.push_str("  <span class=\"sep\">/</span>\n");
    c.push_str("  <strong>.");
    push_esc(&mut c, tld);
    c.push_str("</strong>\n</div>\n");

    let title = format!(".{tld}");

    if domains.is_empty() {
        c.push_str("<div class=\"empty\"><div class=\"empty-icon\">\u{1f310}</div>No domains found under this TLD.</div>\n");
        return page_html(&title, "browse", &c);
    }

    c.push_str("<p style=\"font-size:.85rem;color:var(--muted);margin-bottom:.875rem\">");
    push_esc(&mut c, &fmt_count(domains.len() as u64));
    c.push_str(" host");
    if domains.len() != 1 { c.push_str("names"); } else { c.push_str("name"); }
    c.push_str(" under <strong>.");
    push_esc(&mut c, tld);
    c.push_str("</strong> — click to view captures.</p>\n");

    c.push_str("<div class=\"browse-grid\">\n");
    for (surt_domain, n) in domains {
        let hostname = surt_to_domain(surt_domain);
        c.push_str("  <a class=\"browse-card\" href=\"/ui/browse?domain=");
        // URL-encode the domain for the query string
        for ch in hostname.chars() {
            if ch.is_alphanumeric() || ch == '.' || ch == '-' {
                c.push(ch);
            } else {
                c.push_str(&format!("%{:02X}", ch as u32));
            }
        }
        c.push_str("\">\n    <span class=\"domain\">");
        push_esc(&mut c, &hostname);
        c.push_str("</span>\n    <span class=\"cnt\">");
        push_esc(&mut c, &fmt_count(*n));
        c.push_str("</span>\n  </a>\n");
    }
    c.push_str("</div>\n");

    if truncated {
        c.push_str("<p class=\"more-note\">Showing top 2,000 hostnames by record count.</p>\n");
    }

    page_html(&title, "browse", &c)
}

/// Level 2b — list of specific hostnames under a registered domain.
/// `tld`: e.g. `"de"`, `registered_domain`: e.g. `"obstsortendatenbank.de"`.
/// Each entry links to the captures page for that exact hostname.
pub fn browse_subdomains_html(
    tld: &str,
    registered_domain: &str,
    hostnames: &[(String, u64)],
    truncated: bool,
) -> String {
    let mut c = String::with_capacity(2048 + hostnames.len() * 120);

    c.push_str("<div class=\"breadcrumb\">\n");
    c.push_str("  <a href=\"/ui/browse\">Browse</a>\n");
    c.push_str("  <span class=\"sep\">/</span>\n");
    c.push_str("  <a href=\"/ui/browse?tld=");
    push_esc(&mut c, tld);
    c.push_str("\">.");
    push_esc(&mut c, tld);
    c.push_str("</a>\n");
    c.push_str("  <span class=\"sep\">/</span>\n");
    c.push_str("  <strong>");
    push_esc(&mut c, registered_domain);
    c.push_str("</strong>\n</div>\n");

    let title = registered_domain.to_owned();

    if hostnames.is_empty() {
        c.push_str("<div class=\"empty\">No hostnames found.</div>\n");
        return page_html(&title, "browse", &c);
    }

    c.push_str("<p style=\"font-size:.85rem;color:var(--muted);margin-bottom:.875rem\">");
    push_esc(&mut c, &fmt_count(hostnames.len() as u64));
    c.push_str(" hostname");
    if hostnames.len() != 1 { c.push('s'); }
    c.push_str(" under <strong>");
    push_esc(&mut c, registered_domain);
    c.push_str("</strong> — click to view captures.</p>\n");

    c.push_str("<div class=\"browse-grid\">\n");
    for (surt_domain, n) in hostnames {
        let hostname = surt_to_domain(surt_domain);
        // `host=`, not `domain=`: for a site served from its apex the two names
        // are identical, and `domain=` would route straight back to this page.
        c.push_str("  <a class=\"browse-card\" href=\"/ui/browse?host=");
        for ch in hostname.chars() {
            if ch.is_alphanumeric() || ch == '.' || ch == '-' {
                c.push(ch);
            } else {
                c.push_str(&format!("%{:02X}", ch as u32));
            }
        }
        c.push_str("\">\n    <span class=\"domain\">");
        push_esc(&mut c, &hostname);
        c.push_str("</span>\n    <span class=\"cnt\">");
        push_esc(&mut c, &fmt_count(*n));
        c.push_str("</span>\n  </a>\n");
    }
    c.push_str("</div>\n");

    if truncated {
        c.push_str("<p class=\"more-note\">Showing top 500 hostnames by record count.</p>\n");
    }

    page_html(&title, "browse", &c)
}

/// Level 3 — captures under a specific hostname/domain prefix.
/// Reuses the captures table layout from url_html.
///
/// `show_all`: when false (default) URLs whose every capture has a non-2xx
/// status code are hidden.  When true all URLs are shown.
pub fn browse_captures_html(
    domain: &str,
    tld: &str,
    records: &[CdxRecord],
    truncated: bool,
    show_all: bool,
) -> String {
    let mut c = String::with_capacity(2048 + records.len() * 128);

    // Breadcrumb
    c.push_str("<div class=\"breadcrumb\">\n");
    c.push_str("  <a href=\"/ui/browse\">Browse</a>\n");
    c.push_str("  <span class=\"sep\">/</span>\n");
    c.push_str("  <a href=\"/ui/browse?tld=");
    push_esc(&mut c, tld);
    c.push_str("\">.");
    push_esc(&mut c, tld);
    c.push_str("</a>\n");
    c.push_str("  <span class=\"sep\">/</span>\n");
    c.push_str("  <strong>");
    push_esc(&mut c, domain);
    c.push_str("</strong>\n</div>\n");

    let title = domain.to_owned();

    if records.is_empty() {
        c.push_str("<div class=\"empty\"><div class=\"empty-icon\">\u{1f5c4}\u{fe0f}</div>No captures found.</div>\n");
        return page_html(&title, "browse", &c);
    }

    // Deduplicate: count captures per unique URL, preserving first-seen order.
    // Also track whether any capture for a URL returned a 2xx status.
    struct UrlEntry<'a> {
        url: &'a str,
        count: usize,
        has_2xx: bool,
    }
    let mut all_urls: Vec<UrlEntry> = Vec::new();
    let mut last: Option<&str> = None;
    for rec in records {
        let is_2xx = rec.status.map_or(false, |s| s >= 200 && s < 300);
        if last == Some(rec.original_url.as_str()) {
            let e = all_urls.last_mut().unwrap();
            e.count += 1;
            e.has_2xx |= is_2xx;
        } else {
            all_urls.push(UrlEntry { url: rec.original_url.as_str(), count: 1, has_2xx: is_2xx });
            last = Some(rec.original_url.as_str());
        }
    }

    let suppressed = all_urls.iter().filter(|e| !e.has_2xx).count();
    let visible: Vec<&UrlEntry> = all_urls
        .iter()
        .filter(|e| show_all || e.has_2xx)
        .collect();

    // Build the toggle URL: /ui/browse?domain=<domain>[&all=1]
    let mut base_url = String::from("/ui/browse?domain=");
    push_url_encoded(&mut base_url, domain);
    let toggle_url = if show_all {
        base_url.clone()                 // uncheck → remove &all=1
    } else {
        format!("{base_url}&all=1")       // check → add &all=1
    };

    // Summary line + checkbox toggle
    c.push_str("<div style=\"display:flex;align-items:baseline;gap:1.2rem;flex-wrap:wrap;margin-bottom:.7rem\">\n");
    c.push_str("<p class=\"cap-count\" style=\"margin:0\">");
    push_esc(&mut c, &fmt_count(visible.len() as u64));
    if truncated { c.push_str("+"); }
    c.push_str(if visible.len() == 1 { " URL" } else { " URLs" });
    c.push_str(" &nbsp;·&nbsp; ");
    push_esc(&mut c, &fmt_count(records.len() as u64));
    if truncated { c.push_str("+"); }
    c.push_str(if records.len() == 1 { " capture" } else { " captures" });
    c.push_str(" under <strong>");
    push_esc(&mut c, domain);
    c.push_str("</strong></p>\n");

    // Checkbox — navigates to toggle URL on change (no form needed)
    c.push_str("<label style=\"font-size:.85rem;color:var(--muted);cursor:pointer;white-space:nowrap\">\n");
    c.push_str("  <input type=\"checkbox\" onchange=\"location.href='");
    push_esc(&mut c, &toggle_url);
    c.push_str("'\"");
    if show_all { c.push_str(" checked"); }
    c.push_str("> Show non-2xx");
    if suppressed > 0 && !show_all {
        c.push_str(" <span style=\"color:var(--muted)\">(");
        c.push_str(&suppressed.to_string());
        c.push_str(" hidden)</span>");
    }
    c.push_str("\n</label>\n");
    c.push_str("</div>\n");

    if visible.is_empty() {
        c.push_str("<div class=\"empty\"><div class=\"empty-icon\">\u{2139}\u{fe0f}</div>All captures for this domain returned non-2xx status codes. Check \u{201c}Show non-2xx\u{201d} above to see them.</div>\n");
        return page_html(&title, "browse", &c);
    }

    // Table: URL | captures
    c.push_str("<div class=\"table-wrap\">\n<table>\n");
    c.push_str("<thead><tr><th>URL</th><th class=\"num\">Captures</th></tr></thead>\n<tbody>\n");

    for entry in &visible {
        c.push_str("<tr>\n  <td style=\"max-width:520px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\" title=\"");
        push_esc(&mut c, entry.url);
        c.push_str("\"><a href=\"/ui/url?url=");
        push_url_encoded(&mut c, entry.url);
        c.push_str("\">");
        push_esc(&mut c, entry.url);
        c.push_str("</a></td>\n  <td class=\"num\">");
        c.push_str(&entry.count.to_string());
        c.push_str("</td>\n</tr>\n");
    }

    c.push_str("</tbody>\n</table>\n</div>\n");

    if truncated {
        c.push_str("<p class=\"more-note\">Showing first 5,000 captures. Use the CDX API for the full list.</p>\n");
    }

    page_html(&title, "browse", &c)
}

/// Percent-encode a string for use as a URL query-parameter value.
/// Encodes everything except unreserved characters (A–Z a–z 0–9 - _ . ~).
pub fn push_url_encoded(out: &mut String, s: &str) {
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9'
            | b'-' | b'_' | b'.' | b'~' => out.push(byte as char),
            b => {
                out.push('%');
                out.push(HEX[(b >> 4) as usize] as char);
                out.push(HEX[(b & 0xf) as usize] as char);
            }
        }
    }
}

const HEX: &[u8; 16] = b"0123456789ABCDEF";

// ── /ui/skiplist — what never enters the index ───────────────────────────────

/// Render the skip list: the domains and URL patterns in force, what they do,
/// and a link to the same list in the crawler's format.
///
/// Shows the list the server is *actually applying* — the patterns that
/// compiled. Anything configured but rejected at startup is called out
/// separately, because a rule that silently does nothing is worse than no rule.
pub fn skiplist_html(
    domains: &[String],
    source_lines: &[String],
    active_patterns: &[String],
    rejected: &[String],
) -> String {
    let mut c = String::with_capacity(4096 + source_lines.len() * 96);

    c.push_str("<h1 style=\"font-size:1.15rem;font-weight:700;margin-bottom:.35rem\">Skip list</h1>\n");
    c.push_str(
        "<p style=\"color:var(--muted);font-size:.875rem;margin-bottom:1rem;max-width:60rem\">\
         What never enters the index. A match is skipped during ingest, purged from CDX and \
         the fulltext index on the next <code>tywb index</code> run, and hidden from search \
         results straight away. Index entries only — the captures stay in the WARC files, so \
         removing a rule and re-indexing brings them back.</p>\n",
    );

    // Download row.
    c.push_str("<div style=\"display:flex;gap:.5rem;align-items:center;flex-wrap:wrap;margin-bottom:1.25rem\">\n");
    c.push_str(
        "  <a href=\"/skiplist.zeno\" download style=\"padding:.4rem .8rem;background:var(--navy);\
         color:#fff;border-radius:6px;font-size:.85rem;font-weight:600;text-decoration:none\">\
         \u{2b07} Download for Zeno</a>\n",
    );
    c.push_str(
        "  <a href=\"/skiplist.zeno?inline=1\" style=\"padding:.4rem .8rem;border:1px solid #d5dae5;\
         border-radius:6px;font-size:.85rem;text-decoration:none;color:var(--navy)\">View as text</a>\n",
    );
    c.push_str(
        "  <span style=\"color:var(--muted);font-size:.8rem\">an exclusion file for \
         <code>Zeno --exclusion-file</code></span>\n",
    );
    c.push_str("</div>\n");

    // Counts.
    c.push_str("<div class=\"stat-grid\" style=\"margin-bottom:1.25rem\">\n");
    stat_card(&mut c, &fmt_count(domains.len() as u64), "domains");
    stat_card(&mut c, &fmt_count(active_patterns.len() as u64), "URL patterns");
    if !rejected.is_empty() {
        stat_card(&mut c, &fmt_count(rejected.len() as u64), "rejected");
    }
    c.push_str("</div>\n");

    if !rejected.is_empty() {
        c.push_str("<div class=\"error\" style=\"margin-bottom:1.25rem\">\n");
        c.push_str("  <strong>Not in force.</strong> These were configured but did not compile, \
                    or would have matched every URL. They filter nothing, here or in the crawler:\n");
        c.push_str("  <ul style=\"margin:.5rem 0 0 1.1rem\">\n");
        for pattern in rejected {
            c.push_str("    <li><code>");
            push_esc(&mut c, pattern);
            c.push_str("</code></li>\n");
        }
        c.push_str("  </ul>\n</div>\n");
    }

    // URL patterns, with the comments they were written with.
    c.push_str("<h2 style=\"font-size:.95rem;font-weight:700;margin:0 0 .5rem\">URL patterns</h2>\n");
    if active_patterns.is_empty() {
        c.push_str("<div class=\"empty\">No URL patterns configured.</div>\n");
    } else {
        c.push_str(
            "<pre style=\"background:#fff;border:1px solid #e3e7ef;border-radius:8px;padding:.9rem 1rem;\
             overflow-x:auto;font-size:.8rem;line-height:1.55;margin-bottom:1.5rem\">",
        );
        let active: std::collections::HashSet<&str> =
            active_patterns.iter().map(String::as_str).collect();
        if source_lines.is_empty() {
            for pattern in active_patterns {
                push_esc(&mut c, pattern);
                c.push('\n');
            }
        } else {
            for line in source_lines {
                if line.is_empty() {
                    c.push('\n');
                } else if line.starts_with('#') {
                    c.push_str("<span style=\"color:#8b93a7\">");
                    push_esc(&mut c, line);
                    c.push_str("</span>\n");
                } else if active.contains(line.as_str()) {
                    push_esc(&mut c, line);
                    c.push('\n');
                }
            }
        }
        c.push_str("</pre>\n");
    }

    // Domains.
    c.push_str("<h2 style=\"font-size:.95rem;font-weight:700;margin:0 0 .5rem\">Domains</h2>\n");
    if domains.is_empty() {
        c.push_str("<div class=\"empty\">No domains blocked.</div>\n");
    } else {
        c.push_str(
            "<p style=\"color:var(--muted);font-size:.8rem;margin-bottom:.5rem\">\
             Each covers the domain and every subdomain of it.</p>\n",
        );
        c.push_str("<div class=\"browse-grid\">\n");
        for domain in domains {
            c.push_str("  <div class=\"browse-card\"><span class=\"domain\">");
            push_esc(&mut c, domain);
            c.push_str("</span></div>\n");
        }
        c.push_str("</div>\n");
    }

    page_html("Skip list", "skiplist", &c)
}

// ── API stats response struct (re-exported for server.rs) ─────────────────────

#[derive(serde::Serialize)]
pub struct ApiStats {
    pub cdx: ApiCdxStats,
    pub search: ApiSearchStats,
}

#[derive(serde::Serialize)]
pub struct ApiCdxStats {
    pub total_records:    u64,
    pub unique_urls:      u64,
    pub warc_files:       u64,
    pub oldest_timestamp: Option<String>,
    pub newest_timestamp: Option<String>,
}

#[derive(serde::Serialize)]
pub struct ApiSearchStats {
    pub num_docs: u64,
}

// ── tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn basic() -> BasicStats {
        BasicStats {
            total_records:    4_135_367,
            unique_urls:      2_000_000,
            warc_files:       637,
            oldest_timestamp: Some("20200101120000".to_owned()),
            newest_timestamp: Some("20260811120000".to_owned()),
        }
    }

    // ── Homepage: counts only ─────────────────────────────────────────────

    #[test]
    fn homepage_shows_the_counts() {
        let html = homepage_html(&basic(), 2_053_279, &[]);
        assert!(html.contains("CDX records"));
        assert!(html.contains("WARC files"));
        assert!(html.contains("Fulltext docs"));
    }

    #[test]
    fn homepage_carries_no_group_by_output() {
        // The point of the split: nothing here may need a GROUP BY. If a
        // breakdown creeps back onto this page, the query behind it comes with
        // it, and it is charged to replay and search too — one shared SQLite
        // connection.
        let html = homepage_html(&basic(), 2_053_279, &[]);
        assert!(!html.contains("Content types"), "mime breakdown is a GROUP BY");
        assert!(!html.contains("HTTP status codes"), "status breakdown is a GROUP BY");
        assert!(html.contains("href=\"/ui/stats\""), "but it must link to where they live");
    }


    #[test]
    fn an_empty_query_with_hits_still_renders_them() {
        // Browsing a collection: no terms, but results. The page used to stop
        // at the search form and drop them on the floor, which is what made the
        // homepage collection cards look broken.
        let mut h = hit("https://obst-pdfs.23.nu/monatshefte-ocr/Band_01.pdf", "20260814173550", "Band 01");
        h.collection = Some("monatshefte".to_owned());
        let html = search_html("", &[h], None, None, Some("monatshefte"), false);
        assert!(html.contains("Band 01"), "the hit must be rendered");
        assert!(html.contains("in collection"));
    }

    #[test]
    fn browsing_a_collection_does_not_say_for_nothing() {
        let mut h = hit("https://obst-pdfs.23.nu/monatshefte-ocr/Band_01.pdf", "20260814173550", "Band 01");
        h.collection = Some("monatshefte".to_owned());
        let html = search_html("", &[h], None, None, Some("monatshefte"), false);
        assert!(html.contains("1 result from 1 domain ·"), "no dangling \"for\": {html:?}");
        // With terms it still names them.
        let html = search_html("Obstbau", &[hit("https://a.de/", "20240101120000", "A")], None, None, None, false);
        assert!(html.contains("for <strong>Obstbau</strong>"));
    }

    #[test]
    fn the_bare_search_page_stays_bare() {
        let html = search_html("", &[], None, None, None, false);
        assert!(!html.contains("class=\"result-title\""));
        assert!(html.contains("Query language"));
    }

    #[test]
    fn homepage_shows_collection_cards() {
        // Back on the homepage, but counted per name through the collection
        // index rather than by grouping the whole table — see
        // server::collection_cards.
        let html = homepage_html(
            &basic(),
            2_053_279,
            &[("warc".to_owned(), 4_133_667), ("monatshefte".to_owned(), 54)],
        );
        assert!(html.contains("<h2>Collections</h2>"));
        assert!(html.contains("coll-card\" href=\"/ui/search?q=&collection=monatshefte\""));
    }

    #[test]
    fn homepage_hides_the_collection_row_for_a_plain_archive() {
        let html = homepage_html(&basic(), 1, &[("warc".to_owned(), 10)]);
        assert!(!html.contains("<h2>Collections</h2>"));
    }

    // ── Statistics page: the expensive half ───────────────────────────────

    #[test]
    fn stats_page_shows_every_breakdown() {
        let html = stats_html(
            &basic(),
            2_053_279,
            &[("warc".to_owned(), 4_135_367), ("monatshefte".to_owned(), 54)],
            &[("text/html".to_owned(), 3_000_000), ("application/pdf".to_owned(), 12_000)],
            &[(Some(200), 3_500_000), (Some(404), 12_000), (None, 3)],
            2941,
        );
        assert!(html.contains("Content types"));
        assert!(html.contains("text/html"));
        assert!(html.contains("HTTP status codes"));
        assert!(html.contains("Collections"));
        assert!(html.contains("monatshefte"));
        // The cost is on the page, because it is the number that justifies the split.
        assert!(html.contains("2941"));
    }

    #[test]
    fn stats_page_hides_collections_when_there_is_only_the_archive() {
        let html = stats_html(&basic(), 1, &[("warc".to_owned(), 10)], &[], &[], 5);
        assert!(!html.contains("<h2>Collections</h2>"));
    }

    fn hit(url: &str, ts: &str, title: &str) -> SearchHit {
        SearchHit {
            url: url.to_owned(),
            timestamp: ts.to_owned(),
            title: title.to_owned(),
            mime: Some("text/html".to_owned()),
            s3_key: "k".to_owned(),
            offset: 0,
            length: 0,
            score: 1.0,
            collection: None,
        }
    }

    #[test]
    fn collection_badge_renders_for_non_warc_hits() {
        let mut h = hit("https://obst-pdfs.23.nu/1152-hochstamm.pdf", "20260227000000", "Hochstamm");
        h.mime = Some("application/pdf".to_owned());
        h.collection = Some("obst-pdfs".to_owned());
        let mut warc = hit("https://example.de/", "20260101000000", "Ex");
        warc.collection = Some("warc".to_owned());
        let html = search_html("x", &[h, warc], None, None, None, false);
        // obst-pdfs gets a badge linking to its filtered search…
        assert!(html.contains("coll-badge\" href=\"/ui/search?collection=obst-pdfs&q=\""));
        // …but the default "warc" collection is not badged.
        assert!(!html.contains(">warc</a>"));
    }

    #[test]
    fn active_collection_filter_shows_clear_link() {
        let html = search_html("apfel", &[], None, None, Some("obst-pdfs"), false);
        assert!(html.contains("in collection <span class=\"coll-badge\">obst-pdfs"));
        assert!(html.contains("coll-clear\" href=\"/ui/search?q=apfel\""));
        // The filter persists as a hidden form field.
        assert!(html.contains("name=\"collection\" value=\"obst-pdfs\""));
    }

    #[test]
    fn url_domain_folds_subdomains_into_second_level() {
        assert_eq!(url_domain("https://obstsorten.pomologen-verein.de/x"), "pomologen-verein.de");
        assert_eq!(url_domain("http://www.pomologen-verein.de"), "pomologen-verein.de");
        assert_eq!(url_domain("https://pomologen-verein.de/"), "pomologen-verein.de");
        assert_eq!(url_domain("https://a.b.c.example.co/page?q=1#frag"), "example.co");
    }

    #[test]
    fn url_domain_handles_ports_case_and_odd_hosts() {
        assert_eq!(url_domain("http://Example.COM:8080/a"), "example.com");
        assert_eq!(url_domain("https://example.com./"), "example.com");
        assert_eq!(url_domain("http://192.168.0.7:3900/x"), "192.168.0.7");
        assert_eq!(url_domain("http://localhost:8080/"), "localhost");
        assert_eq!(url_domain("example.de/no-scheme"), "example.de");
    }

    #[test]
    fn groups_keep_relevance_order_and_show_newest_first() {
        let hits = vec![
            hit("https://www.a.de/1", "20240101000000", "a old"),
            hit("https://b.de/1", "20200101000000", "b only"),
            hit("https://shop.a.de/2", "20260101000000", "a new"),
            hit("https://www.a.de/3", "20250101000000", "a mid"),
        ];
        let groups = group_by_domain(&hits);

        // Domain order follows the first hit of each domain, not the timestamps.
        assert_eq!(groups.iter().map(|g| g.domain.as_str()).collect::<Vec<_>>(), ["a.de", "b.de"]);

        // Newest capture represents the domain, the rest descend by date.
        assert_eq!(groups[0].newest.title, "a new");
        assert_eq!(
            groups[0].rest.iter().map(|h| h.title.as_str()).collect::<Vec<_>>(),
            ["a mid", "a old"],
        );
        assert!(groups[1].rest.is_empty());
    }

    #[test]
    fn search_page_collapses_domains_and_offers_more() {
        let hits = vec![
            hit("https://www.a.de/1", "20240101000000", "a old"),
            hit("https://obstsorten.a.de/2", "20260101000000", "a new"),
            hit("https://b.de/1", "20200101000000", "b only"),
        ];
        let html = search_html("Roter Berlepsch", &hits, None, None, None, false);

        assert!(html.contains("3 results from 2 domains"), "count line: {html:.0}");
        // The newest capture of a.de is the visible row…
        assert!(html.contains("class=\"result-title\" href=\"/web/20260101000000/https://obstsorten.a.de/2\""));
        // …and the older one is behind the disclosure, which names the domain.
        assert!(html.contains("1 more result from a.de"));
        assert!(html.contains("class=\"more-item\" href=\"/web/20240101000000/https://www.a.de/1\""));
        // A domain with a single hit gets no disclosure.
        assert_eq!(html.matches("<details class=\"more\">").count(), 1);
    }

    #[test]
    fn empty_result_set_renders_no_groups() {
        let html = search_html("nothing", &[], None, None, None, false);
        assert!(html.contains("No results for"));
        assert!(!html.contains("class=\"more\""));
    }

    #[test]
    fn hostname_listing_links_to_captures_not_back_to_itself() {
        // The apex host and its registered domain are the same string; linking
        // it as `domain=` sent the user back to this very page (the bug that
        // made walnussmeisterei.de unbrowsable).
        let hosts = vec![
            ("de,walnussmeisterei".to_owned(), 2874u64),
            ("de,walnussmeisterei,www".to_owned(), 6u64),
        ];
        let html = browse_subdomains_html("de", "walnussmeisterei.de", &hosts, false);

        assert!(html.contains("href=\"/ui/browse?host=walnussmeisterei.de\""),
                "apex host must link to its captures");
        assert!(html.contains("href=\"/ui/browse?host=www.walnussmeisterei.de\""));
        // No hostname card may point back at the hostname listing.
        assert!(!html.contains("browse-card\" href=\"/ui/browse?domain="),
                "a hostname card still links to the domain level");
    }

    #[test]
    fn skiplist_page_shows_the_list_in_force_and_what_is_not() {
        let html = skiplist_html(
            &["instagram.com".to_owned()],
            &[
                "# ── wiki ──".to_owned(),
                r"(?i)/wiki/Diskussion:".to_owned(),
                "[unclosed".to_owned(), // in the file, but never compiled
            ],
            &[r"(?i)/wiki/Diskussion:".to_owned()],
            &["[unclosed".to_owned()],
        );

        assert!(html.contains("instagram.com"));
        assert!(html.contains("── wiki ──"), "section comments are kept");
        assert!(html.contains("/wiki/Diskussion:"));
        // The rejected pattern appears once, in the warning — never in the
        // listing, where it would read as a rule that is doing something.
        assert!(html.contains("Not in force."));
        assert_eq!(html.matches("[unclosed").count(), 1, "{html}");
        // Both ways to get the list out are offered.
        assert!(html.contains("href=\"/skiplist.zeno\" download"));
        assert!(html.contains("/skiplist.zeno?inline=1"));
    }

    #[test]
    fn skiplist_page_escapes_patterns_into_html() {
        // Patterns are full of `&`, `<` and quotes; none of it may reach the
        // page as markup.
        let pattern = r#"(?i)[?&]a=<b>&"x""#.to_owned();
        let html = skiplist_html(&[], &[], std::slice::from_ref(&pattern), &[]);
        assert!(!html.contains("<b>"), "raw markup leaked into the page");
        assert!(html.contains("&lt;b&gt;"));
        assert!(html.contains("&amp;"));
    }

    #[test]
    fn surt_and_domain_round_trip() {
        assert_eq!(surt_to_domain("de,walnussmeisterei"), "walnussmeisterei.de");
        assert_eq!(surt_to_domain("de,walnussmeisterei,www"), "www.walnussmeisterei.de");
        assert_eq!(domain_to_surt_prefix("walnussmeisterei.de"), "de,walnussmeisterei)");
        // The apex prefix must not swallow subdomains: a subdomain SURT continues
        // with ',' where the apex prefix has ')'.
        assert!(!"de,walnussmeisterei,www)/".starts_with(&domain_to_surt_prefix("walnussmeisterei.de")));
    }
}
