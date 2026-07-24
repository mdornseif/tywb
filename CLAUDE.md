# warc-search — Claude Code Guide

A resource-efficient fulltext search engine and Wayback-compatible replay server
for WARC and PDF files stored on S3-compatible object storage.
Written in Rust. Runs on an ordinary Linux server or a headless macOS machine.

---

## Workspace layout

```
warc-search/
├── Cargo.toml              # workspace root
├── config.yaml             # runtime configuration (see Configuration below)
├── CLAUDE.md               # this file
└── crates/
    ├── warc/               # lib: WARC record types + streaming parser
    ├── config/             # lib: config loading (YAML + env vars)
    ├── cdx/                # lib: CDX index, SQLite, SURT canonicalization
    ├── s3_store/           # lib: S3 access, streaming, Range GET
    ├── search/             # lib: Tantivy fulltext index wrapper
    └── tywb/               # bin: tywb — index, server, stats, recompress subcommands
```

### Dependency rules (enforce strictly)

```
warc        →  (no internal deps — must stay extractable as a standalone crate)
config      →  (no internal deps)
cdx         →  warc
s3_store    →  (no internal deps)
search      →  (no internal deps)
tywb        →  warc, config, cdx, s3_store, search
```

`warc/` must never import from any other crate in this workspace.
It is designed to be published as a standalone crate on crates.io later.

---

## Build

```bash
# debug build (all crates)
cargo build

# release build (optimised, use for benchmarking / deployment)
cargo build --release

# build the binary
cargo build --release -p tywb
```

---

## Tests

```bash
# run all tests across the workspace
cargo test

# run tests for a specific crate
cargo test -p warc
cargo test -p warc-search-config

# run a specific test by name
cargo test -p warc reader::tests::parse_two_records_sequentially

# run tests with output visible (useful for debugging)
cargo test -p warc -- --nocapture

# config tests mutate env vars — run single-threaded to avoid races
cargo test -p warc-search-config -- --test-threads=1
```

### Test conventions

- Every public function in `warc/` and `config/` must have tests.
- WARC parser tests live in `crates/warc/src/reader.rs` (inline `#[cfg(test)]`).
- Record/header tests live in `crates/warc/src/record.rs`.
- Config tests live in `crates/config/src/lib.rs`.
- Use `build_warc_record()` from `warc::reader` to construct test fixtures —
  do not hardcode raw byte strings unless testing a specific malformed-input case.
- Tests that set environment variables must clean up after themselves.
  Use the `with_env()` helper already defined in `config/src/lib.rs`.
- Config env-var tests must be run with `--test-threads=1` (see above).

---

## Configuration

Runtime config is read from `config.yaml` at startup.
All values can be overridden by environment variables (env vars win).

### Env var mapping

| Environment variable      | Config field              | Notes                          |
|---------------------------|---------------------------|--------------------------------|
| `AWS_ACCESS_KEY_ID`       | `s3.access_key_id`        | Standard AWS SDK var           |
| `AWS_SECRET_ACCESS_KEY`   | `s3.secret_access_key`    | Standard AWS SDK var           |
| `AWS_DEFAULT_REGION`      | `s3.region`               | Standard AWS SDK var           |
| `AWS_ENDPOINT_URL`        | `s3.endpoint_url`         | For MinIO / R2 / B2            |
| `WARC_S3_BUCKET`          | `s3.bucket`               |                                |
| `WARC_S3_PREFIX`          | `s3.prefix`               |                                |
| `WARC_S3_CONCURRENCY`     | `s3.concurrency`          |                                |
| `WARC_INDEX_PATH`         | `storage.index_path`      |                                |
| `WARC_CDX_DB_PATH`        | `storage.cdx_db_path`     |                                |
| `WARC_SERVER_BIND`        | `server.bind`             |                                |
| `RUST_LOG`                | `log.level`               | Standard tracing-subscriber var|

### Credential precedence

1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
2. `s3.access_key_id` / `s3.secret_access_key` in `config.yaml`
3. Standard AWS SDK fallback (`~/.aws/credentials`, instance metadata, etc.)

Prefer env vars for credentials in production. Never commit real credentials to
`config.yaml`.

---

## Key design decisions

### `warc/` crate isolation
The WARC parser has zero dependencies on the rest of this workspace.
It accepts any `std::io::Read` and yields `WarcRecord` values.
This is intentional: it will be extracted as a standalone published crate.
Do not add S3, SQLite, Tantivy, or config imports to `warc/`.

### Streaming everywhere
WARC files can be multi-GB. Nothing in the ingest pipeline loads a full file.
The S3 client streams bytes → the WARC parser reads them record-by-record →
the indexer writes to Tantivy in batches → batches are flushed and memory freed.

### S3 Range GET for replay
The CDX index stores the S3 object key + byte offset + byte length for every
WARC record. Replay fetches only those bytes via an HTTP Range request.
A 10 GB WARC file costs one small Range GET to replay a single page.

### SQLite for CDX (not Postgres, not Redis)
CDX lookups are point queries and small range scans by (surt_url, timestamp).
SQLite in WAL mode handles concurrent reads with zero daemon overhead.
`sqlite_cache_kib` (default 8 MiB) bounds the page cache SQLite keeps in memory.

### Tantivy for fulltext search
Tantivy uses `mmap` for index segments — the OS page cache manages memory.
RAM usage at idle is ~30 MB regardless of index size.
Set `indexer.batch_size` higher to speed up ingest at the cost of peak RAM.

### Record-per-member `.warc.gz`
Every part of the replay path assumes a `.warc.gz` stores **one gzip member per
WARC record** — that is what makes a Range GET of a single record possible.
Files that gzip the whole WARC as one deflate stream cannot be indexed or
replayed record by record: the indexer sees a single member, takes the first
record out of it, and drops the rest.
`tywb recompress` rewrites such files losslessly (record bytes are copied
verbatim, only the gzip framing changes), keeps the original as `<key>.bak`,
and verifies that the concatenated payload is byte-identical before replacing
anything. Run `index --force` on the affected keys afterwards so their CDX
entries pick up the real per-record offsets.

Some archives in the wild simply stop mid-stream (aborted crawl or upload):
`gzip -t` fails on the object itself. `recompress --salvage-truncated` keeps
every complete record before the break and drops the incomplete tail; the
verification then requires the payload to be an exact *prefix* of the source.
Without the flag those files are reported and left untouched — bytes are never
dropped silently.

### PDF text extraction (optional Tika backend)
PDFs are not readable by the HTML/text indexer. When `indexer.tika` is set to a
running `tika-server`, `application/pdf` records are extracted to searchable
text (PDFBox for born-digital PDFs, Tesseract OCR for scans). The whole payload
goes to Tika — PDFs must never be truncated, the xref table is at the end. A
quality gate (`pdf::looks_like_text`) drops OCR noise before it reaches the
index. With `tika` unset, PDFs stay browsable but unsearchable — no external
dependency. See `crates/tywb/src/pdf.rs` and `playbooks/tika.yml` (ops repo).

### No async in `warc/`
The core parser is sync (`std::io::Read`). Async callers wrap it in
`tokio::task::spawn_blocking`. This keeps the crate simple and dependency-free.

---

## Adding a new crate

1. `mkdir -p crates/<name>/src`
2. Write `crates/<name>/Cargo.toml` — inherit `version`, `edition`, `authors`,
   `license` from the workspace.
3. Add `"crates/<name>"` to `[workspace.members]` in the root `Cargo.toml`.
4. Add shared dependencies to `[workspace.dependencies]` rather than
   duplicating version strings.

---

## Running locally

```bash
# 1. Copy and edit config
cp config.yaml config.local.yaml
$EDITOR config.local.yaml

# 2. Run the indexer (one-shot ingest)
WARC_S3_BUCKET=my-bucket \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
cargo run --release -p tywb -- --config config.local.yaml index

# 3. Run the server
cargo run --release -p tywb -- --config config.local.yaml server

# The server binds to server.bind (default 0.0.0.0:8080).
# Fulltext search:  GET /search?q=<query>&from=20240101&to=20241231
# Wayback replay:   GET /web/20240315120000/https://example.com/
# CDX API:          GET /cdx?url=example.com/*&output=json

# 4. Repair whole-file-gzip WARCs (see "Record-per-member .warc.gz" below)
cargo run --release -p tywb -- --config config.local.yaml recompress --scan-only
cargo run --release -p tywb -- --config config.local.yaml recompress
```

---

## Linting and formatting

```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

CI should fail on any clippy warning. Run both before opening a PR.

---

## Crate status

| Crate              | Status      | Notes                                  |
|--------------------|-------------|----------------------------------------|
| `warc`             | ✅ complete  | Parser + record types + full test suite |
| `config`           | ✅ complete  | YAML + env var loading + full test suite|
| `cdx`              | ✅ complete  | SURT, SQLite store, closest-match lookup, CDX builder from WarcRecord, `warc_files` metadata table |
| `s3_store`         | ✅ complete  | Client builder, paginated listing, ETag state, streaming GET, Range GET |
| `search`           | 🔲 stub      | Next: Tantivy schema, index, query      |
| `tywb`             | 🔄 in progress | `index` working (streaming, throughput, SIGINFO, limits); `server` stub; `stats` complete; `recompress` complete |
