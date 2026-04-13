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
- **CDX API** — Wayback-compatible `/cdx` endpoint with exact and prefix URL lookup
- **S3-compatible storage** — works with AWS S3, MinIO, Cloudflare R2, Backblaze B2
- **Incremental indexing** — ETag-based state file skips unchanged objects on re-runs
- **SQLite CDX index** — WAL-mode, concurrent reads, no daemon overhead

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

**Indexer options:**

| Flag | Description |
|------|-------------|
| `--file <KEY>` | Index only this S3 key (bypasses listing) |
| `--max-files <N>` | Stop after processing N WARC files |
| `--max-urls <N>` | Stop after writing N new CDX entries |

**Live status:** press **Ctrl+T** (macOS/BSD) or send **SIGUSR1** (Linux) to print current file, URL, throughput (rec/s, MB/s) to stderr without interrupting indexing.

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

### Wayback replay

```
GET /web/<timestamp>/<url>
```

Replays an archived page. The server looks up the closest CDX record, fetches only that WARC record's bytes from S3 via a Range GET, and serves the original HTTP response body with its original status and `Content-Type`.

```bash
curl 'http://localhost:8080/web/20240315120000/https://example.com/'
```

This is compatible with standard Wayback Machine client tooling and browser extensions.

### CDX API

```
GET /cdx?url=<url>[&from=<ts>][&to=<ts>][&limit=<n>]
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

### Health check

```
GET /healthz  →  200 OK
```

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

Per-file metadata (MIME histogram, date range, throughput, error counts) is also recorded in the `warc_files` table of `cdx.db` after each successful index run.

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
