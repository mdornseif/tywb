//! HTML page generation for the tywb web UI.
//!
//! Pure functions only — no axum, no async. Each function takes data and
//! returns a complete HTML document as a `String`.
//!
//! Design goals:
//! - Zero external dependencies (no JS framework, no CDN)
//! - Works without JavaScript enabled
//! - Inspired by Wayback Machine / pywb visual style

use warc_search_cdx::{CdxRecord, CdxStats, WarcFileRow};
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

pub fn homepage_html(stats: &CdxStats, num_docs: u64) -> String {
    let mut c = String::with_capacity(8192);

    // ── Stat cards ────────────────────────────────────────────────────────
    c.push_str("<div class=\"stat-grid\">\n");
    stat_card(&mut c, &fmt_count(stats.total_records), "CDX records");
    stat_card(&mut c, &fmt_count(stats.unique_urls),   "Unique URLs");
    stat_card(&mut c, &fmt_count(stats.warc_files),    "WARC files");
    stat_card(&mut c, &fmt_count(num_docs),            "Fulltext docs");
    c.push_str("</div>\n");

    // ── Date coverage ─────────────────────────────────────────────────────
    match (&stats.oldest_timestamp, &stats.newest_timestamp) {
        (Some(a), Some(b)) => {
            c.push_str("<div class=\"coverage\">Archive coverage: <strong>");
            push_esc(&mut c, &fmt_ts(a));
            c.push_str("</strong> &nbsp;→&nbsp; <strong>");
            push_esc(&mut c, &fmt_ts(b));
            c.push_str("</strong></div>\n");
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

    // ── Content types breakdown ───────────────────────────────────────────
    if !stats.mime_counts.is_empty() {
        let max_n = stats.mime_counts.first().map(|(_, n)| *n).unwrap_or(1).max(1);
        c.push_str("<div class=\"mime-section\">\n<h2>Content types</h2>\n");
        for (mime, count) in stats.mime_counts.iter().take(12) {
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
    if !stats.status_counts.is_empty() {
        c.push_str("<div class=\"form-card\" style=\"margin-bottom:1.25rem\">\n<h2>HTTP status codes</h2>\n");
        c.push_str("<div style=\"display:flex;gap:1rem;flex-wrap:wrap;margin-top:.25rem\">\n");
        for (status, count) in &stats.status_counts {
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

    page_html("Home", "home", &c)
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
    error: bool,
) -> String {
    let mut c = String::with_capacity(4096 + hits.len() * 512);

    // ── Search form (always visible at top) ───────────────────────────────
    c.push_str("<div class=\"search-top\">\n");
    c.push_str("<form action=\"/ui/search\" method=\"get\">\n");
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

    if q.is_empty() {
        return page_html("Search", "search", &c);
    }

    if error {
        c.push_str("<div class=\"error\">Search failed — please try again or check the server logs.</div>\n");
        return page_html("Search", "search", &c);
    }

    // ── Result count ──────────────────────────────────────────────────────
    c.push_str("<p class=\"result-count\">");
    if hits.is_empty() {
        c.push_str("No results for <strong>");
        push_esc(&mut c, q);
        c.push_str("</strong>.");
    } else {
        push_esc(&mut c, &fmt_count(hits.len() as u64));
        c.push_str(if hits.len() == 1 { " result" } else { " results" });
        c.push_str(" for <strong>");
        push_esc(&mut c, q);
        c.push_str("</strong>");
    }
    c.push_str("</p>\n");

    // ── Results ───────────────────────────────────────────────────────────
    if !hits.is_empty() {
        c.push_str("<div class=\"result-list\">\n");
        for hit in hits {
            render_hit(&mut c, hit);
        }
        c.push_str("</div>\n");
    }

    page_html("Search", "search", &c)
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

/// Level 3 — captures under a specific hostname/domain prefix.
/// Reuses the captures table layout from url_html.
pub fn browse_captures_html(
    domain: &str,
    tld: &str,
    records: &[CdxRecord],
    truncated: bool,
) -> String {
    let mut c = String::with_capacity(2048 + records.len() * 256);

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

    c.push_str("<p class=\"cap-count\">");
    push_esc(&mut c, &fmt_count(records.len() as u64));
    if truncated {
        c.push_str("+ ");
    }
    c.push_str(if records.len() == 1 { " capture" } else { " captures" });
    c.push_str(" under <strong>");
    push_esc(&mut c, domain);
    c.push_str("</strong></p>\n");

    // Table: URL | date | status | type | replay
    c.push_str("<div class=\"table-wrap\">\n<table>\n");
    c.push_str("<thead><tr><th>URL</th><th>Date / time</th><th>Status</th><th>Type</th><th></th></tr></thead>\n<tbody>\n");

    for rec in records.iter().rev() {
        let sc     = status_class(rec.status);
        let ss     = status_str(rec.status);
        let mime   = short_mime(rec.mime.as_deref());
        let replay = format!("/web/{}/{}", rec.timestamp, rec.original_url);

        c.push_str("<tr>\n  <td style=\"max-width:340px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap\" title=\"");
        push_esc(&mut c, &rec.original_url);
        c.push_str("\">");
        push_esc(&mut c, &rec.original_url);
        c.push_str("</td>\n  <td class=\"ts\">");
        push_esc(&mut c, &fmt_ts(&rec.timestamp));
        c.push_str("</td>\n  <td class=\"");
        c.push_str(sc);
        c.push_str("\">");
        push_esc(&mut c, &ss);
        c.push_str("</td>\n  <td>");
        push_esc(&mut c, mime);
        c.push_str("</td>\n  <td><a class=\"replay-btn\" href=\"");
        push_esc(&mut c, &replay);
        c.push_str("\">Replay \u{2197}</a></td>\n</tr>\n");
    }

    c.push_str("</tbody>\n</table>\n</div>\n");

    if truncated {
        c.push_str("<p class=\"more-note\">Showing first 5,000 captures. Use the CDX API for the full list.</p>\n");
    }

    page_html(&title, "browse", &c)
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
