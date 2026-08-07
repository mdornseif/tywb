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
- **Domain blacklist** — exclude domains (and their subdomains) from indexing; existing entries are purged automatically on next run
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

**Live status:** press **Ctrl+T** (macOS/BSD) or send **SIGUSR1** (Linux) to print current file, URL, throughput (rec/s, MB/s) to stderr without interrupting indexing.

### Domain blacklist

Domains (and all their subdomains) can be excluded from indexing by listing them under `indexer.blacklisted_domains` in `config.yaml`:

```yaml
indexer:
  blacklisted_domains:
    - spam-site.example.com
    - ads.example.net
```

Two things happen automatically each time `tywb index` runs:

1. **Skip during ingest** — any WARC record whose `WARC-Target-URI` belongs to a blacklisted domain (or one of its subdomains) is silently skipped; nothing is written to the CDX or fulltext index for that record.

2. **Purge existing data** — before processing new files, tywb removes all CDX records and fulltext index entries for every blacklisted domain from the current index.  This means adding a domain to the blacklist and re-running `tywb index` is sufficient to clean up previously-indexed data — no manual database surgery required.

Subdomain matching is automatic: `example.com` in the blacklist covers `www.example.com`, `cdn.example.com`, `deep.sub.example.com`, etc.

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

A `Content-Encoding` of `gzip` or `deflate` on the stored response is undone
first. `br` and other encodings we cannot decode are refused with `415` rather
than returned as broken text.

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
| `/` | Homepage with index statistics (record count, date range, MIME breakdown) |
| `/ui/search` | HTML fulltext search form |
| `/ui/browse` | Domain browser — TLD → domain → captures hierarchy |
| `/ui/url?url=<url>` | All archived captures for a specific URL, sorted by date |
| `/ui/files` | List of indexed WARC files with per-file statistics |

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
