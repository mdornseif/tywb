# tywb — Tiny Wayback

A resource-efficient fulltext search engine and Wayback-compatible replay server for WARC files stored on S3-compatible object storage. Written in pure Rust. Designed to run on a 1–2 GB VPS or headless macOS machine.

```
tywb [--config <path>] <COMMAND>

Commands:
  index   Ingest WARC files from S3 into the fulltext and CDX indexes
  server  Run the HTTP search and replay server
  stats   Print a summary of the current index state
```

## Features

- **Fulltext search** — Tantivy-powered, ~30 MB idle RAM regardless of index size
- **Wayback replay** — `GET /web/{timestamp}/{url}` fetches only the relevant bytes via S3 Range GET; a 10 GB WARC costs one small range request per replay
- **Wayback toolbar** — sticky archive bar injected into replayed HTML pages showing the capture date, original URL, and a link to other captures; uses [wombat.js](https://github.com/webrecorder/wombat) for client-side URL rewriting
- **Text API** — `GET /text?url=…` returns the plain text of a capture, HTML stripped and PDFs OCR'd, so clients need no parsing of their own
- **CDX API** — Wayback-compatible `/cdx` endpoint with exact and prefix URL lookup
- **CDX timemap** — `GET /web/timemap/cdx` compatible with Zeno and gowarc deduplication
- **Domain browser** — hierarchical TLD → domain → captures navigation at `/ui/browse`
- **URL captures page** — `/ui/url?url=<url>` lists all captures for a specific URL
- **S3-compatible storage** — works with AWS S3, MinIO, Cloudflare R2, Backblaze B2
- **Incremental indexing** — ETag-based state file skips unchanged objects on re-runs
- **SQLite CDX index** — WAL-mode, concurrent reads, no daemon overhead
- **Compressed WARC replay** — per-gzip-member offsets stored in CDX so `.warc.gz` replay is a targeted range GET, not a full decompression
- **Skip list** — exclude whole domains (with their subdomains) or single kinds of page by URL pattern, e.g. wiki talk pages and version histories; matches are skipped at ingest, purged from an existing index on the next run, and hidden from search results at once
- **warcinfo storage** — the `warcinfo` record from each WARC file is stored in SQLite for audit and provenance tracking

## Architecture

```
tywb/
├── crates/
│   ├── warc/       # Streaming WARC parser (sync, zero-copy, no deps)
│   ├── config/     # YAML + env-var config loading
│   ├── cdx/        # CDX record types, SURT canonicalization, SQLite store
│   ├── s3_store/   # S3 client, paginated listing, streaming GET, Range GET
│   ├── search/     # Tantivy fulltext index wrapper
│   └── tywb/       # bin: tywb — single binary with `index` and `server` subcommands
```

## Quick start

### 1. Configure

```bash
cp config.yaml config.local.yaml
$EDITOR config.local.yaml
```

Minimal config:

```yaml
s3:
  bucket: my-warc-bucket
  region: us-east-1

storage:
  index_path: ./var/index
  cdx_db_path: ./var/cdx.db
```

For MinIO or another S3-compatible service, add:

```yaml
s3:
  endpoint_url: "https://minio.example.com"
  force_path_style: true
```

Credentials are loaded from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables (recommended), from `config.yaml`, or from the standard AWS SDK chain (`~/.aws/credentials`, instance metadata, etc.).

### 2. Index

```bash
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
cargo run --release -p tywb -- --config config.local.yaml index
```

Streams each WARC file from S3, parses it record-by-record, writes CDX entries to SQLite, and adds extracted text to the Tantivy index. Saves progress after each file, so a crash on a large bucket is safe to resume.

After each WARC file is indexed, a CDX sidecar file is written to the same S3 bucket alongside the WARC (see [CDX sidecar files](#cdx-sidecar-files) below). If the sidecar already exists it is left untouched.

**Indexer options:**

| Flag | Description |
|------|-------------|
| `--file <KEY>` | Index only this S3 key (bypasses listing) |
| `--max-files <N>` | Stop after processing N WARC files |
| `--max-urls <N>` | Stop after writing N new CDX entries |
| `--force` | Re-process all WARC files even if their ETag matches the saved state (use to repair existing index data) |
| `--collections-only` | Skip the primary WARC bucket and index only `indexer.collections` — the way to add or refresh a PDF collection without re-walking the archive |

**Live status:** press **Ctrl+T** (macOS/BSD) or send **SIGUSR1** (Linux) to print current file, URL, throughput (rec/s, MB/s) to stderr without interrupting indexing.

### Skip list

One list decides what never enters the index. It has two halves, and both act in the same three places.

**Domains** drop a whole site:

```yaml
indexer:
  blacklisted_domains:
    - spam-site.example.com
    - ads.example.net
```

Subdomain matching is automatic: `example.com` covers `www.example.com`, `cdn.example.com`, `deep.sub.example.com`, etc.

**URL patterns** drop a kind of page on sites that are otherwise worth keeping — the talk pages, version histories and `action=edit` links every wiki hangs off each article:

```yaml
indexer:
  blacklisted_url_patterns:
    - "(?i)[?&]action=(edit|history)"
    - "(?i)(/|[?&]title=)(Diskussion|Talk):"
```

Each pattern is a regular expression matched against the whole URL, unanchored, in RE2 syntax (no backreferences, no lookaround). Prefix `(?i)` for case-insensitivity. A pattern that matches the empty string is refused at startup, with a warning: it would match every URL and purge the entire index. A pattern that fails to compile is dropped and logged, never fatal.

Both halves are better kept in static files, editable without re-rendering `config.yaml` — a server restart applies the display filter, a re-index purges what is already stored:

```yaml
indexer:
  blacklisted_domains_path:      "/etc/tywb/skip-domains.txt"
  blacklisted_url_patterns_path: "/etc/tywb/skip-urls.txt"
```

The repository ships a ready-made [`skip-urls.txt`](skip-urls.txt) covering MediaWiki and DokuWiki cruft: talk namespaces in German and English, `Spezial:`/`Special:`, `action=`/`do=` verbs, `oldid=`/`diff=` permalinks, `api.php` and friends. Category, file and portal pages are deliberately left in — they carry captions, provenance and the wiki's own navigation — with a commented-out rule for anyone who disagrees.

What happens to a match:

1. **Skipped during ingest** — a WARC record whose `WARC-Target-URI` matches is never written to CDX or the fulltext index.

2. **Purged from existing data** — before processing new files, `tywb index` removes the CDX records and fulltext entries of everything that matches. Adding an entry and re-running `tywb index` is enough to clean up data indexed earlier; no manual database surgery. The domain half is a SURT-prefix query; the pattern half is one sequential scan of the CDX table, so it only runs when patterns are configured. Nothing is deleted from the WARC files themselves — remove the rule, re-index, and the captures come back.

3. **Hidden from search results** — the server applies the same test at query time, so an edit to either file takes effect on restart without waiting for a re-index.

### Viewing the list, and feeding it to a crawler

`GET /ui/skiplist` shows what is in force: the domains, the URL patterns with the comments they were written with, and — separately — anything configured that did *not* compile. A rule that silently does nothing is worse than no rule, so the page names it.

`GET /skiplist.zeno` serves the same list as an exclusion file for the [Zeno](https://github.com/internetarchive/Zeno) crawler. Crawling pages tywb would only throw away costs bandwidth on both ends, and both sides use RE2, so the URL patterns transfer unchanged; only the domains are translated, to `^https?://([^/]*\.)?host([:/?#]|$)`.

```bash
# in a crawl script, before the run
curl -o exclusions.txt http://tywb.example.org/skiplist.zeno
Zeno get list seeds.txt --exclusion-file exclusions.txt …
```

The list is edited in one place — tywb's skip list — and the crawler fetches it. There is no second copy to drift, and no local generator step. Add `?inline=1` to read it in the browser instead of downloading it. Patterns that failed to compile are absent from the export as well: inert in tywb, inert in the crawler.

### 3. Serve

```bash
cargo run --release -p tywb -- --config config.local.yaml server
```

The server binds to `server.bind` (default `0.0.0.0:8080`).

## API

### Fulltext search

```
GET /search?q=<query>[&from=<timestamp>][&to=<timestamp>][&limit=<n>]
```

| Parameter | Description |
|-----------|-------------|
| `q`       | Query string (required). Supports Tantivy query syntax: `rust AND programming`, `"exact phrase"`, `title:rust`. |
| `from`    | Lower bound timestamp, 14 digits: `20240101000000` |
| `to`      | Upper bound timestamp, 14 digits: `20241231235959` |
| `limit`   | Max results (default: `server.max_results`, capped at 500) |

**Response** — JSON array of search hits:

```json
[
  {
    "url":       "https://example.com/page",
    "timestamp": "20240315120000",
    "title":     "Example Page",
    "mime":      "text/html",
    "s3_key":    "crawls/2024/archive.warc.gz",
    "offset":    1048576,
    "length":    8192,
    "score":     1.42
  }
]
```

```bash
curl 'http://localhost:8080/search?q=rust+programming&from=20240101000000&limit=10'
```

### Plain text of a capture

```
GET /text?url=<url>[&timestamp=<ts>][&output=json]
GET /text/<timestamp>/<url>
```

Returns the readable text of one archived capture — HTML stripped, PDFs run
through Tika/OCR — so a client does not have to re-implement any of that.
It is the same extraction pipeline the indexer uses, run on demand.

| Parameter   | Description |
|-------------|-------------|
| `url`       | The archived URL (required). A missing scheme is read as `https://`. |
| `timestamp` | 14-digit capture timestamp, or any prefix of one: `2024` means `20240101000000`. The closest capture wins. Omitted → the most recent capture. |
| `output`    | `text` (default) or `json`. |

The path form `/text/<timestamp>/<url>` mirrors `/web/<timestamp>/<url>`, so any
replay link becomes text by swapping the prefix. Because the query string
belongs to the target URL there, that form always answers `text/plain`; use
`/text?url=…&output=json` for JSON.

```bash
# newest capture, as plain text
curl 'http://localhost:8080/text?url=example.com/page'

# a specific capture, with metadata
curl 'http://localhost:8080/text?url=https://example.com/page&timestamp=20240315&output=json'

# any replay URL, as text
curl 'http://localhost:8080/text/20240315120000/https://example.com/page'
```

**Plain-text response** — the body is only the text; everything else is a header:

```
Content-Type: text/plain; charset=utf-8
X-Archive-Orig-URL: https://example.com/page
X-Tywb-Timestamp:   20240315120000
X-Tywb-Collection:  warc
X-Tywb-Mime:        text/html
X-Tywb-Low-Quality: 1        (only when the OCR quality gate rejected the text)
```

**JSON response:**

```json
{
  "url":         "https://example.com/page",
  "timestamp":   "20240315120000",
  "collection":  "warc",
  "status":      200,
  "mime":        "text/html",
  "title":       "Example Page",
  "chars":       2395,
  "low_quality": false,
  "text":        "Example Page …"
}
```

**What gets extracted**

| Content type | Result |
|--------------|--------|
| `text/html`, `application/xhtml+xml`, `text/xml`, `application/xml` | Markup removed, `<script>`/`<style>`/`<noscript>` bodies dropped, character references decoded, whitespace collapsed. `title` comes from `<title>`. |
| `text/*` | Returned verbatim, line breaks intact. `title` is the first line. |
| `application/pdf` | Text via Tika — PDFBox for born-digital files, Tesseract OCR for scans. |
| anything else | `415`, with a pointer to `/web/` for the raw bytes. |

<a name="wire-format"></a>
**Wire format** — a WARC stores the response exactly as it came off the wire, so
the bytes after the HTTP headers are not the content yet. They are peeled in the
order the wire put them on, by every reader — the indexer, `/text` and replay
alike (`crates/tywb/src/http_payload.rs`):

1. `Transfer-Encoding: chunked` framing is removed. Stored headers are not
   trustworthy about this (crawlers drop the header and keep the framing), so
   the body is de-chunked when — and only when — it actually parses as a chunk
   sequence. A capture cut off mid-chunk keeps what it has.
2. A `Content-Encoding` of `gzip` or `deflate` is undone. That header is a
   claim too: some crawlers store the decoded body and keep the header, so a
   failed decode falls back to the bytes as stored — unless they really are
   binary, which is reported rather than served as mojibake. Encodings with no
   decoder at all (`br`, `zstd`) are refused by `/text` with `415`.

A capture whose body is cut off mid-stream still yields the prefix that did
decode, and decoding stops at 128 MiB so a decompression bomb cannot exhaust
memory.

Text is decoded as UTF-8, lossily — pages in a legacy single-byte charset lose
their non-ASCII characters. This matches what the fulltext index holds.

Unlike the indexer, `/text` returns text that fails the OCR quality gate (with
`low_quality`/`X-Tywb-Low-Quality` set) and attempts extraction on truncated
PDFs: one on-demand request can afford an OCR attempt the bulk indexer skips,
and a caller asking for one named document can judge the noise itself.

**Errors**

| Status | Cause |
|--------|-------|
| `400`  | No `url`, unparseable URL, or a non-numeric `timestamp` |
| `404`  | No capture of that URL in the CDX index |
| `413`  | PDF larger than `indexer.tika.max_pdf_bytes` |
| `415`  | No text extractor for that content type, or an undecodable `Content-Encoding` |
| `422`  | Tika parsed the PDF but found no text |
| `501`  | A PDF was requested but `indexer.tika` is not configured |
| `502`  | S3 or Tika failed |

### Wayback replay

```
GET /web/<timestamp>/<url>
```

Replays an archived page. The server looks up the closest CDX record, fetches only that WARC record's bytes from S3 via a Range GET, and serves the original HTTP response body with its original status and `Content-Type`.

The body is served **decoded** — chunk framing removed, `Content-Encoding` undone (see [Wire format](#wire-format)). It has to be: replay forwards neither `Transfer-Encoding` nor `Content-Encoding`, so a browser handed the stored bytes would render the chunk headers and the deflate stream as text. A body that cannot be decoded is served as stored rather than refused; replay hands back what was captured.

```bash
curl 'http://localhost:8080/web/20240315120000/https://example.com/'
```

This is compatible with standard Wayback Machine client tooling and browser extensions.

**Wayback toolbar** — for `text/html` responses tywb automatically injects:

1. A sticky dark toolbar at the top of the page showing the archive date, the original URL, and a link to all captures of that URL (`/ui/url?url=…`).
2. [wombat.js](https://github.com/webrecorder/wombat) — Webrecorder's client-side URL rewriting library — loaded from `/_wb/wombat.js` and initialized so that links and resources resolve correctly within the archive context.

No configuration is required; the toolbar is always active for HTML replay responses.

### CDX API

```
GET /cdx?url=<url>[&from=<ts>][&to=<ts>][&limit=<n>][&matchType=prefix]
```

Returns index entries as a JSON array of arrays (CDX-API format). Append `*` to `url` for a prefix search over a whole domain.

```bash
# Exact URL lookup
curl 'http://localhost:8080/cdx?url=https://example.com/'

# All pages under a domain
curl 'http://localhost:8080/cdx?url=example.com/*&limit=100'
```

**Response:**

```json
[
  ["urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length"],
  ["com,example)/", "20240315120000", "https://example.com/", "text/html", "200", "sha1:…", "8192"]
]
```

### CDX timemap (Zeno / gowarc deduplication)

```
GET /web/timemap/cdx?url=<url>[&limit=<n>]
```

Returns captures for a URL as space-separated plain text, one record per line — the format used by [Zeno](https://github.com/internetarchive/Zeno) and [gowarc](https://github.com/internetarchive/gowarc) for CDX-based WARC deduplication.

Each line has seven fields:

```
{urlkey} {timestamp} {original} {mime} {status} {digest} {length}
```

The digest is returned **without** the hash-algorithm prefix (`sha1:`, `sha256:`, etc.) to match what gowarc expects.

`limit` follows the pywb convention: positive values return the N oldest captures; negative values return the N most-recent captures (e.g. `limit=-1` returns the single latest capture). gowarc always uses `limit=-1`.

```bash
# Most recent capture (gowarc dedup query)
curl 'http://localhost:8080/web/timemap/cdx?url=https://example.com/&limit=-1'
# → com,example)/ 20240315120000 https://example.com/ text/html 200 ABCDEF1234 8192

# Five oldest captures
curl 'http://localhost:8080/web/timemap/cdx?url=https://example.com/&limit=5'
```

To use tywb as a deduplication server with Zeno, pass:

```
--warc-cdx-dedupe-server http://<tywb-host>:8080
```

### Web UI

| Path | Description |
|------|-------------|
| `/` | Homepage — record count, unique URLs, WARC files, date range, search forms |
| `/ui/stats` | Collections, content types and HTTP status codes |
| `/ui/search` | HTML fulltext search form |
| `/ui/browse` | Domain browser — TLD → domain → captures hierarchy |
| `/ui/url?url=<url>` | All archived captures for a specific URL, sorted by date |
| `/ui/files` | List of indexed WARC files with per-file statistics |

#### Splitting one prefix between two collections

`prefix` can only describe a contiguous run of keys. When the material worth
separating is interleaved with the rest — a digitisation whose volumes each sit
in a directory of their own — add `key_pattern`, a regex over the object key in
the same RE2 syntax as the skip list:

```yaml
    - name: bsb-scans          # listed first — it claims what is its
      type: pdf_bucket
      bucket: obst-pdfs.23.nu
      prefix: "archive-org/"
      key_pattern: "^archive-org/[0-9]+bsb/"
      tika:
        ocr_strategy: "ocr_only"
    - name: archive-org        # everything else under the same prefix
      type: pdf_bucket
      bucket: obst-pdfs.23.nu
      prefix: "archive-org/"
```

An object that does not match is left untouched — not recorded as seen — so a
later collection over the same prefix still picks it up. Order therefore
decides: within one run the first collection to claim an object keeps it, so the
narrower one goes first.

A `key_pattern` that fails to compile stops that collection rather than being
logged and ignored. A rule written to *narrow* a prefix must not fail open:
without it the collection would cover the whole prefix, which here would mean
sending 1,800 volumes through OCR.

#### Per-collection text extraction

A collection is a body of documents with properties of its own. A digitised
library corpus arrives already OCR'd, in volumes of several hundred megabytes;
the web crawl beside it holds small PDFs of unknown provenance where OCR is the
only way to get any text at all. One `indexer.tika` block had to serve both, and
the compromise was wrong for each: `auto` spent hours in Tesseract re-reading
text that was already there, while a size limit sized for web PDFs silently
dropped exactly the volumes worth having.

Each collection may now carry its own `tika` block, layered over the global one:

```yaml
indexer:
  tika:
    url: "http://127.0.0.1:9998"
    ocr_strategy: "auto"          # right for the web archive
    max_pdf_bytes: 104857600
  collections:
    - name: pomologie
      type: pdf_bucket
      bucket: obst-pdfs.23.nu
      prefix: "pomologie-ocr/"
      public_base_url: "https://obst-pdfs.23.nu/"
      tika:
        ocr_strategy: "no_ocr"    # already OCR'd — just read the text layer
        max_pdf_bytes: 629145600  # 600 MiB
```

State only what differs; every unset field keeps the global value. The Tika
**URL** is deliberately not overridable — this decides how a document is parsed,
not which service parses it.

`/text` applies the same override as the indexer did, so asking for a
document's text returns what was indexed rather than a second, differently
extracted reading of the same bytes.

#### Why statistics have their own page

The homepage asks SQLite only for scalar counts. Every breakdown — content
types, HTTP status codes, records per collection — is a `GROUP BY` over the
whole CDX table with no index to lean on, and on a 4-million-record archive the
three of them together take about seven seconds.

That was never just the homepage's problem. The server keeps **one** SQLite
connection behind a mutex, so for those seconds every other request waited too:
replay, search, CDX lookups. A health check with a ten-second budget duly
reported the server as unreachable while it was serving perfectly well.

`/ui/stats` runs each of those queries on its own short-lived **read-only**
connection, concurrently — in WAL mode readers do not block each other — so the
page costs roughly its slowest query instead of the sum, and costs the rest of
the server nothing. It prints the measured time at the bottom, which is the
number that decides whether anything may ever put those queries back on a hot
path.

#### Domain browser

`/ui/browse` provides a three-level hierarchy:

- `/ui/browse` — TLDs sorted by capture count
- `/ui/browse?tld=de` — domains under a TLD
- `/ui/browse?domain=example.de` — all captures for a domain, deduplicated by URL (each URL appears once, with a count of how many captures exist; clicking links to `/ui/url`)

#### URL captures page

`/ui/url?url=<url>` shows every archived capture of the given URL in chronological order. Each row links directly to the Wayback replay at `/web/<timestamp>/<url>`. Accepts optional `from` and `to` parameters (14-digit timestamps) to filter by date range.

### Health check

```
GET /healthz  →  200 OK
```

## SQLite schema

Four tables are maintained in `cdx.db`:

**`cdx`** — one row per indexed WARC record:

| Column | Description |
|--------|-------------|
| `surt_url` | SURT-canonicalized URL (primary key component) |
| `timestamp` | 14-digit capture timestamp (primary key component) |
| `original` | Original URL |
| `mime` | HTTP Content-Type of the response body (extracted from HTTP headers, not the WARC envelope) |
| `status` | HTTP status code |
| `digest` | `WARC-Block-Digest` (e.g. `sha1:ABC…`) |
| `s3_key` | S3 object key of the WARC file |
| `offset` | Byte offset of the record in the uncompressed stream |
| `length` | Content-Length of the record block |
| `c_offset` | Compressed byte offset of the gzip member (`.warc.gz` only; `NULL` for plain `.warc`) |

**`warc_files`** — one row per indexed WARC file:

| Column | Description |
|--------|-------------|
| `s3_key` | S3 object key (primary key) |
| `etag` | S3 ETag, used for incremental skip logic |
| `bucket` | S3 bucket name |
| `first_seen` / `last_indexed` | ISO-8601 UTC timestamps |
| `warc_records` | Total WARC records parsed |
| `cdx_new` / `cdx_known` | New vs. updated CDX entries written |
| `fulltext_indexed` | Documents added to the Tantivy index |
| `skipped` / `errors` | Non-indexed records and parse errors |
| `duration_secs` / `bytes_per_sec` / `records_per_sec` | Throughput metrics |
| `warc_date_min` / `warc_date_max` | Earliest and latest `WARC-Date` values seen |
| `mime_summary` | JSON object mapping MIME type → record count |

**`warcinfo`** — the `warcinfo` WARC record from the start of each file:

| Column | Description |
|--------|-------------|
| `s3_key` | S3 key of the source WARC (primary key) |
| `bucket` | S3 bucket name |
| `warc_date` | `WARC-Date` from the warcinfo record |
| `warc_filename` | `WARC-Filename` header value |
| `record_id` | `WARC-Record-ID` header value |
| `headers_json` | All WARC headers serialized as a JSON array of `[name, value]` pairs |
| `block_text` | Raw text content of the warcinfo block (crawler metadata, operator info, etc.) |

Useful for auditing crawler software versions and operator metadata across a large archive.

## CDX sidecar files

After each WARC file is successfully indexed, tywb writes a CDX sidecar file into the same S3 bucket alongside the WARC. The sidecar key is the WARC key with `.cdx` appended:

```
crawls/2024/archive.warc.gz   →   crawls/2024/archive.warc.gz.cdx
crawls/2024/archive.warc      →   crawls/2024/archive.warc.cdx
```

If a sidecar already exists (detected via a HEAD request) it is left untouched. The write runs as a background task so it never slows down the main indexing loop.

### Format

Sidecar files use the standard CDX-11 plain-text format (`Content-Type: text/plain`):

```
 CDX N b a m s k r M S V g
com,example)/ 20240315120000 https://example.com/ text/html 200 sha1:ABC… - - 8192 0 archive.warc.gz
com,example)/page 20240315120001 https://example.com/page text/html 200 sha1:DEF… - - 4096 8192 archive.warc.gz
```

| Field | Header char | Content |
|-------|-------------|---------|
| SURT URL | `N` | Canonicalized, sort-friendly URL |
| Timestamp | `b` | 14-digit `YYYYMMDDHHmmss` |
| Original URL | `a` | Verbatim `WARC-Target-URI` |
| MIME type | `m` | HTTP `Content-Type` of the response body |
| HTTP status | `s` | e.g. `200`, `301` |
| Digest | `k` | `WARC-Block-Digest`, e.g. `sha1:ABC…` |
| Redirect | `r` | Always `-` (not captured) |
| Meta | `M` | Always `-` |
| Record length | `S` | `Content-Length` of the WARC block in bytes |
| Byte offset | `V` | For `.warc.gz`: compressed gzip-member offset (`c_offset`). For `.warc`: uncompressed stream offset. |
| Filename | `g` | Basename of the WARC file |

The byte offset field (`V`) matches what is stored in the CDX SQLite database and is suitable for S3 Range GET requests for replay.

### Why

CDX sidecar files make the archived content independently usable without tywb's SQLite database:

- Standard CDX consumers (pywb, OpenWayback, CDX server tools) can read them directly
- Provides a backup index that lives with the data in S3
- Enables other tools to locate and replay individual WARC records without running tywb

## Re-indexing after the wire-format fix

Until the [wire format](#wire-format) was peeled in the indexer, a capture stored
with chunk framing or a `Content-Encoding` was indexed as `U+FFFD` noise, and
such PDFs reached Tika unparseable. Deploying the fix does not repair those
documents — only re-indexing their WARC files does.

`scan-wire-format` sizes that job without reading a WARC in full: it samples a
few indexable records per object and Range-GETs only those.

```bash
tywb --config config.yaml scan-wire-format --sample 4 --out /var/tmp/affected.txt
```

It separates two kinds of damage, because conflating them overstates the job by
a wide margin:

| Stored as | What the old indexer wrote | Re-index |
|-----------|----------------------------|----------|
| **Compressed** (`Content-Encoding: gzip`/`deflate`, or undecodable) | deflate bytes read as text — `U+FFFD` noise, unsearchable | **needed** — the content is lost until then |
| **Chunked only** | readable text, plus stray hex tokens like `173` where the chunk-size lines fell | optional — only tidies those tokens |
| **Neither** | correct | none |

A PDF is the exception: chunk framing alone already makes it unparseable for
Tika, so any peeling counts as damage for `application/pdf`. A
`Content-Encoding` header on a body the crawler had already decoded counts as
neither — the old indexer read it correctly.

```
Wire-format scan
  WARC objects in CDX                  392
  objects scanned                      392
  records sampled                     1375
    compressed (indexed noise)         ...   (..%)
    chunked only (text usable)         ...   (..%)

Re-index needed — documents are noise today
  objects                              ...
  indexable records to rebuild         ...
  estimated unsearchable documents    ~...   (extrapolated from the samples)

Optional — chunked but already searchable
  objects                              ...
  indexable records                    ...
```

| Flag | Meaning |
|------|---------|
| `--sample N` | Records sampled per WARC object (default 3). Higher is more accurate and costs one Range GET each. |
| `--limit N` | Stop after N objects, most records first. |
| `--jobs N` | Objects sampled concurrently (default 8). |
| `--out <path>` | Write the S3 keys that need a re-index, one per line. |
| `--verbose` | One line per damaged object instead of just the summary. |

The scan is read-only. Re-index what it found:

```bash
while read -r key; do
  tywb --config config.yaml index --force --file "$key"
done < /var/tmp/affected.txt
```

The document count is extrapolated from the sample, not counted; the record
counts are exact.

## Index statistics

```bash
tywb --config config.yaml stats
```

Prints a human-readable summary of the current index state:

```
CDX index  (./var/cdx.db)
  Records:      1,234,567
  Unique URLs:    890,123
  WARC files:         42
  Date range:   2020-01-01 00:00:00 → 2024-12-31 23:59:59

  MIME types:
    text/html                                  987,654
    application/pdf                             42,000
    ...

  HTTP status:
    200         1,100,000
    301            80,000
    ...

Fulltext index  (./var/index)
  Documents:      987,654

Ingest state  (./var/list_state.json)
  Files seen:          42
```

Per-file metadata (MIME histogram, date range, throughput, error counts, bucket name) is recorded in the `warc_files` table of `cdx.db` after each successful index run.

## Configuration reference

All values can be overridden by environment variables. Environment variables win.

| Environment variable      | Config field              | Default |
|---------------------------|---------------------------|---------|
| `AWS_ACCESS_KEY_ID`       | `s3.access_key_id`        | — |
| `AWS_SECRET_ACCESS_KEY`   | `s3.secret_access_key`    | — |
| `AWS_DEFAULT_REGION`      | `s3.region`               | `us-east-1` |
| `AWS_ENDPOINT_URL`        | `s3.endpoint_url`         | — |
| `WARC_S3_BUCKET`          | `s3.bucket`               | — |
| `WARC_S3_PREFIX`          | `s3.prefix`               | — |
| `WARC_S3_CONCURRENCY`     | `s3.concurrency`          | `4` |
| `WARC_INDEX_PATH`         | `storage.index_path`      | `/var/lib/warc-search/index` |
| `WARC_CDX_DB_PATH`        | `storage.cdx_db_path`     | `/var/lib/warc-search/cdx.db` |
| `WARC_SERVER_BIND`        | `server.bind`             | `0.0.0.0:8080` |
| `RUST_LOG`                | `log.level`               | `info` |

Full annotated config: see [`config.yaml`](config.yaml).

### Unknown keys are an error

Every block rejects keys it does not know. This is deliberate: a key in the
wrong place is otherwise invisible. Writing `tika:` or `collections:` at the top
level instead of under `indexer:` used to be dropped without a word — the run
then reported `nothing to index — all objects are up to date` and exited
successfully, having done nothing at all. Now it says:

```
unknown field `tika`, expected one of `s3`, `storage`, `indexer`, `server`, `log`
```

### The index schema and older indexes

The Tantivy index stores its schema on disk, and fields have only ever been
*appended* to it (`collection`, when collections arrived). An index written
before a field existed still opens, still answers searches and can still be
written to — the indexer reads the field handles back from the index rather
than assuming them, and logs on startup which fields the index predates:

```
WARN index predates these fields — it stays searchable and writable, but new
     documents will not carry them (a collection filter will not find them).
     Rebuild the index to get them back.
```

That is the whole cost: documents added to such an index carry no collection,
so `/ui/search?collection=…` does not find them. Replay is unaffected — the
collection that selects the bucket lives in the CDX database, which has no such
restriction. To get the field back, move the index directory aside and re-run
`tywb index`; the CDX database is kept.

The schema is not rewritten in place to add the field, tempting as it looks:
segments written without a field carry no field norms for it, and the first
background merge of an old segment with a new one fails with `Field norm not
found for field`. An index that works for hours and then dies in a merge is
worse than one that says up front what it cannot do.

A schema that differs in any *other* way — a renamed field, changed indexing
options — is refused at startup, naming the field and telling you to rebuild.

## Building

```bash
# Debug build
cargo build

# Release build (use for deployment / benchmarking)
cargo build --release -p tywb
```

### Cross-compile for Linux from macOS

```bash
cargo install cross
cross build --release --target x86_64-unknown-linux-musl -p tywb
```

## Tests

```bash
# All crates
cargo test

# Config tests must be single-threaded (they mutate env vars)
cargo test -p warc-search-config -- --test-threads=1

# Specific crate
cargo test -p warc
cargo test -p warc-search-search

# With output
cargo test -p warc -- --nocapture
```

## Resource usage

| Resource | Idle | During ingest |
|----------|------|---------------|
| RAM (server) | ~60 MB | — |
| RAM (indexer) | — | ~100–200 MB (controlled by `indexer.batch_size`) |
| SQLite cache | 8 MiB (default) | configurable |
| Tantivy index | OS page cache | `mmap`-based, OS manages eviction |

Designed to fit comfortably on a 1 GB VPS. The indexer and server can run simultaneously — the server opens the Tantivy index read-only and picks up new segments as the indexer commits.

## `tywb index` and `tywb server`: two processes, one machine

`tywb index` and `tywb server` are subcommands of the same binary but are designed to run as separate processes on the same machine, sharing the same data files:

| | `tywb index` | `tywb server` |
|---|---|---|
| **Process lifetime** | One-shot batch job | Long-running daemon |
| **Typical schedule** | Nightly (cron / systemd timer) | Always running |
| **CDX database** | Writes new records | Reads only |
| **Tantivy index** | Writes new segments, commits | Reads only (no file lock held) |

Because the server opens the Tantivy index in read-only mode and SQLite runs in WAL mode, the two processes can run at the same time without conflict. When the indexer commits a batch, the server picks up the new segments automatically on the next query — no restart required.

This split keeps the server's RAM footprint small and predictable (~60 MB idle). The indexer's higher peak usage (~100–200 MB during ingest) is transient and does not affect the running server.

## Deployment notes

Run the indexer periodically (e.g. nightly via cron or systemd timer) and keep the server running continuously:

```
# /etc/systemd/system/tywb-server.service
[Unit]
Description=tywb HTTP server
After=network.target

[Service]
ExecStart=/usr/local/bin/tywb --config /etc/tywb/config.yaml server
Environment=AWS_ACCESS_KEY_ID=...
Environment=AWS_SECRET_ACCESS_KEY=...
Restart=always

[Install]
WantedBy=multi-user.target
```

```
# /etc/systemd/system/tywb-index.service
[Unit]
Description=tywb indexer (one-shot)
After=network.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/tywb --config /etc/tywb/config.yaml index
Environment=AWS_ACCESS_KEY_ID=...
Environment=AWS_SECRET_ACCESS_KEY=...
```

```
# /etc/systemd/system/tywb-index.timer
[Unit]
Description=Run tywb indexer nightly

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
```

## Linting

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

## License

Licensed under either of [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE) at your option.
