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
├── skip-urls.txt           # shipped URL skip patterns (wiki cruft, crawler traps)
├── CLAUDE.md               # this file
└── crates/
    ├── warc/               # lib: WARC record types + streaming parser
    ├── config/             # lib: config loading (YAML + env vars)
    ├── cdx/                # lib: CDX index, SQLite, SURT canonicalization
    ├── s3_store/           # lib: S3 access, streaming, Range GET
    ├── search/             # lib: Tantivy fulltext index wrapper
    └── tywb/               # bin: tywb — index, server, stats, recompress,
                            #      scan-wire-format subcommands
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

### The index schema only ever grows, and old indexes keep working
Tantivy stores its schema on disk and refuses an index whose schema differs from
the one it is handed. Fields here have only ever been *appended* (`collection`),
so the write path opens the index as it stands and reads the field handles back
from it — `SearchIndex::with_heap`, the same thing `SearchReader::open` always
did. An index that predates a field stays searchable and writable; the field is
simply not written, which `add_document` and `search_impl` already handle, and
startup logs which fields are missing and what that costs (a collection filter
will not find the new documents; replay is unaffected, the collection lives in
the CDX).

Do **not** "fix" this by rewriting `meta.json` to append the field. Reads would
work — and then the first background merge of a pre-field segment with a new one
dies with `Field norm not found for field`, hours in. Anything but a pure
suffix of missing fields (a rename, changed options) is refused at startup with
the field named. `schema_compat` decides which case it is, and the merge is
under test.

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

### The stored body is not the content
A WARC holds the HTTP response exactly as it came off the wire: after the
headers can come `Transfer-Encoding: chunked` framing, and inside that a
`Content-Encoding` of gzip or deflate. Anything that reads a body must peel
both, in that order, via `http_payload::parse_http_block` +
`HttpParts::payload` — never `record.http_body()` straight into a parser.
Skipping it indexes U+FFFD noise, feeds Tika unparseable PDFs, and serves
browsers a deflate stream labelled `text/html`.

The headers lie in both directions, so neither gets a vote: chunking is
detected structurally (`dechunk` succeeds only if the whole body parses as a
chunk sequence), and a failed decode falls back to the stored bytes unless they
are genuinely binary. `dechunk` runs over every record of a multi-GB WARC —
keep its rejection path O(1).

### One text pipeline, two consumers
`/text?url=…` returns the readable text of a capture (HTML stripped, PDFs via
Tika/OCR) so clients need no parser of their own. It runs the *same* extraction
the indexer runs — `index::strip_html`, `index::extract_title`,
`pdf::PdfExtractor` — on demand against the stored bytes, rather than reading
text back out of the fulltext index: index bodies are truncated to
`indexer.max_text_bytes` and are not meant to be reproduced verbatim.
Keep the two in step. Anything that improves extraction belongs in those shared
functions, not in the handler. The endpoint differs from the indexer in exactly
two documented ways: it returns quality-gate-rejected OCR text (flagged) and it
attempts truncated PDFs. See `server::extract_capture_text`.

### One skip list, two halves, three places
`indexer.blacklisted_domains` drops a whole site; `indexer.blacklisted_url_patterns`
drops a kind of page on sites that are otherwise wanted — wiki talk pages,
version histories, `action=edit`. Both are asked the same question through
`IndexerConfig::is_url_blacklisted`, and that question is asked in three places:
ingest skips the record, `tywb index` purges what is already stored, and the
server filters search results at query time. Add a rule in one place and all
three follow.

Patterns are regexes compiled once into a `RegexSet` at startup — the test runs
per record over multi-GB WARCs, so it must be one pass, not N. Nothing filters
until `compile_url_patterns()` has run; a bad pattern is logged and dropped
rather than fatal, and one that matches the empty string is refused outright,
because it would match every URL and purge the entire index on the next run.
The purge deletes index entries, never WARC bytes.

The syntax is deliberately the RE2 subset both Rust's `regex` and Go's `regexp`
accept, because tywb is the *source* of the list, not just a consumer of it:
`GET /skiplist.zeno` serves it as an exclusion file for the Zeno crawler
(`crates/tywb/src/skiplist.rs`), so one rule keeps cruft out of the crawl *and*
out of the index. A crawl script `curl`s that URL before the run; there is no
generator step and no second copy to drift. That is also why the crawler-side
rules — private address ranges, logout chains, share buttons — now live in
`skip-urls.txt` rather than in the crawler's own file.

Exported is the list *in force* — the patterns that compiled. A rejected pattern
filters nothing here and must not appear to filter anything there; `/ui/skiplist`
names it separately instead.

`skip-urls.txt` is the shipped list. It is content that decides what gets
deleted, so `index.rs` tests it like code.

### Page boundaries are structure, not whitespace
The PDF extractor asks Tika for XHTML (`/rmeta`), not plain text (`/rmeta/text`):
only the former marks pages. `xhtml_to_text` flattens it with pages separated by
U+000C, matching `pdftotext`/`ocrmypdf` so text from any source is handled alike.
Two invariants keep the page numbers trustworthy, and both have tests: blank
pages keep their slot (dropping one shifts every later page number), and
`normalize_ws` works page by page because `trim`/`split_whitespace` treat U+000C
as whitespace and would silently delete every marker.

### Extraction is expensive; keep what it produced
The fulltext index does not store the body it indexes, so extracted text exists
nowhere after a run. For scanned material that is not a detail: OCR of 23
library volumes took 17.5 hours, and the first improvement to text handling
(folding the Fraktur long s) had to pay for all of it again just to re-derive
text we already had. A collection with `store_text: true` writes
`<key>.tywb.txt` beside each object and reads it back next time; the header line
binds it to the source ETag, so replaced objects are re-extracted rather than
served stale. Off by default: writing into someone's bucket is not a default.

`pdf::normalise_historic_forms` runs before the quality gate, so the indexer and
`/text` see the same text. It folds the long s (`ſ`) and the f-ligatures and
nothing else — NFKC would also rewrite fractions and full-width forms, which is
a much larger promise than the problem needs.

### One prefix, two collections
`prefix` describes a contiguous run of keys; `key_pattern` (RE2, as in the skip
list) narrows it further, for material that is interleaved rather than grouped —
digitisations whose volumes each sit in their own directory. Non-matching
objects are deliberately *not* marked as seen, because the ETag state is shared
across collections within a run: the first collection to claim an object keeps
it, so the narrower collection is listed first and the wider one takes the rest.
A pattern that fails to compile stops that collection rather than being ignored;
failing open would widen exactly the rule that was written to narrow.

### Tika settings belong to the collection, not to the deployment
A digitised corpus and a web crawl want opposite extraction settings, and one
`indexer.tika` block forced a compromise that was wrong for both: `auto` re-OCRs
scans that already carry text (hours of Tesseract for nothing), and a size limit
sized for web PDFs drops the library volumes worth the most. A collection may
carry a `tika:` block of its own — `TikaOverride`, merged over the global one by
`TikaConfig::with_override`; unset fields inherit. The URL is not overridable:
this decides *how* a document is parsed, not *where*.

The server applies the same override on `/text` (`AppState::extractor_for`), so
the on-demand text of a capture is the text that was indexed — the same rule as
"one text pipeline, two consumers" below.

### Collections
The primary WARC bucket is the implicit collection `warc`. `indexer.collections`
adds further sources; type `pdf_bucket` indexes a bucket of standalone PDFs
(each object = one CDX record + one fulltext doc, extracted via Tika). Every
CDX record carries a `collection` name, which selects the bucket at replay
time. Non-WARC collections are served directly from their bucket, not from a
WARC container. See `crates/tywb/src/pdf_collection.rs`.

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
# Plain text:       GET /text?url=https://example.com/&timestamp=2024&output=json
# CDX API:          GET /cdx?url=example.com/*&output=json

# 4. Repair whole-file-gzip WARCs (see "Record-per-member .warc.gz" below)
cargo run --release -p tywb -- --config config.local.yaml recompress --scan-only
cargo run --release -p tywb -- --config config.local.yaml recompress

# 5. Size the re-index the wire-format fix needs (read-only; samples records)
cargo run --release -p tywb -- --config config.local.yaml \
    scan-wire-format --sample 5 --out /var/tmp/affected.txt

# 6. Inspect the skip list:   http://localhost:8080/ui/skiplist
#    Hand it to the crawler — this is the whole handover, run it in the
#    crawl script before Zeno starts:
curl -o exclusions.txt http://localhost:8080/skiplist.zeno
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
| `search`           | ✅ complete  | Tantivy schema, writer + reader, timestamp filter, per-file replace, forward-compatible open of older schemas |
| `tywb`             | 🔄 in progress | `index` working (streaming, throughput, SIGINFO, limits); `server` serving UI, `/search`, `/text`, replay, CDX; `stats` complete; `recompress` complete; `scan-wire-format` complete |
