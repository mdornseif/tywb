# Changelog

All notable changes to tywb are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project does not yet publish tagged releases, so everything lands under
*Unreleased* until the first version is cut.

## [Unreleased]

- **Text endpoint.** `GET /text?url=…[&timestamp=…][&output=json]` (and the
  replay-shaped `GET /text/<timestamp>/<url>`) returns the readable text of an
  archived capture: HTML stripped, chunk framing and `Content-Encoding` undone,
  PDFs extracted through Tika/OCR. Clients that only want the content no longer
  have to re-implement any of that on top of `/web/`. It is the indexer's own
  extraction run on demand against the stored bytes — not a read-back of the
  fulltext index, whose bodies are truncated to `indexer.max_text_bytes`.

  - Plain text by default; `output=json` adds title, timestamp, collection,
    HTTP status, MIME and a character count.
  - Two deliberate differences from indexing: text that fails the OCR quality
    gate is returned with a `low_quality` flag (a caller asking for one named
    document can judge the noise itself), and truncated PDFs are attempted —
    one on-demand OCR is affordable where a bulk pass is not.
  - PDFs need `indexer.tika`; without it the endpoint answers `501` for them.
    Everything else works with no external dependency.

- **`tywb scan-wire-format`** — sizes the re-index that the wire-format fix
  requires. It samples a few indexable records per WARC object and Range-GETs
  only those, then reports the affected objects, the records a re-index would
  rebuild, and an extrapolated count of wrong documents; `--out` writes the keys
  for `index --force --file`. Read-only. A record counts as affected only when
  peeling actually changes its bytes — a `Content-Encoding` header on a body the
  crawler had already decoded does not, because the old indexer read that
  correctly.

- **Fix: the wire format is now peeled everywhere, not just in `/text`.** A WARC
  stores the response exactly as it came off the wire, so the bytes after the
  HTTP headers may still carry `Transfer-Encoding: chunked` framing wrapped
  around a `Content-Encoding` of gzip or deflate. Only `/text` undid that; the
  indexer and replay read the raw bytes.

  - **Indexer** — such captures were indexed as `U+FFFD` noise (title and body
    both), and PDFs still carrying chunk headers were handed to Tika, which
    cannot parse them. `build_index_doc` now decodes first. **Existing
    documents are not repaired by this; a re-index of the affected WARCs is
    needed to fix them.**
  - **Replay** — `/web/` forwards neither `Transfer-Encoding` nor
    `Content-Encoding`, so browsers received chunk headers and a deflate stream
    labelled `text/html`. The body is now decoded before it is served (and
    before the toolbar is injected). A body that cannot be decoded is still
    served as stored — replay hands back what was captured.

  The peeling lives in one place, `crates/tywb/src/http_payload.rs`, together
  with the HTTP-block parser both the replay and text paths had been
  duplicating. Decoding is capped at 128 MiB and keeps the decodable prefix of
  a stream that was cut off mid-capture.

- **Fix: `strip_html` could panic on a mis-decoded page.** A page that is not
  valid UTF-8 arrives full of `U+FFFD`, and the raw-text-element check sliced
  the string at a fixed byte offset — six bytes into a three-byte character.
  In the server that dropped the connection; in the indexer it could abort a
  WARC file mid-pass. The comparison is on bytes now.

- **Better HTML-to-text extraction**, in the indexer as well as `/text`: the
  content of `<script>`, `<style>` and `<noscript>` is dropped instead of being
  indexed as prose, HTML comments are skipped, and character references
  (`&amp;`, `&nbsp;`, `&#8211;`, `&#xe4;`) are decoded. Re-index to get the
  cleaner text into existing documents; nothing breaks if you don't.

- **Unified domain skip list.** One list of domains now controls skipping
  everywhere: entries are not indexed, purged from CDX + fulltext on the next
  `tywb index`, and hidden from search results immediately at query time. This
  replaces the separate query-time URL-prefix blacklist (`search_blacklist_path`)
  and folds it together with `blacklisted_domains` — the earlier DuckDuckGo
  prefix moved in as a domain. Configure via `indexer.blacklisted_domains_path`
  (a static file, one domain per line; subdomains match automatically) and/or
  the inline `indexer.blacklisted_domains`; the file is merged into the list at
  startup. Seeded with instagram/google/pinterest, search engines, social
  platforms and ad/tracker CDNs.


- **Collections** — index sources beyond the primary WARC bucket. The archive
  is the implicit collection `warc`; `indexer.collections` adds more. The first
  supported type is `pdf_bucket`: a bucket of standalone PDF objects, each
  indexed as one CDX record (public URL, `collection` name) and its text
  extracted via the same Tika/OCR path as WARC PDFs. Replay serves such objects
  straight from their bucket. A `collection` column was added to the CDX table
  (existing rows default to `warc`; migrated in place).


### Added

- **PDF fulltext search via Apache Tika (optional).** When `indexer.tika` points
  at a `tika-server`, the indexer extracts text from `application/pdf` records
  and makes them searchable. Tika (PDFBox) reads born-digital PDFs directly; for
  scans it runs Tesseract OCR (`ocr_strategy`, `ocr_languages`). Measured on the
  archive, ~96% of PDFs are born-digital with a clean text layer.

  - The whole PDF payload is sent to Tika — never truncated, because the xref
    table a parser needs lives at the end of the file. Payloads over
    `max_pdf_bytes` (default 100 MiB) are skipped.
  - A quality gate (`pdf::looks_like_text`) drops output that does not read like
    prose, so wrong-language or bad-scan OCR noise never enters the index.
    Verified: born-digital PDFs score alnum-ratio 0.90–0.98, truncated-PDF
    garbage 0.03 — the thresholds sit safely in the gap.
  - Extraction is a blocking localhost call inside the parse loop, so peak
    memory stays at one PDF and no async plumbing is needed.
  - Tika absent → PDFs behave exactly as before (browsable, not searchable),
    so the dependency-free deployment is unaffected. Ops: `playbooks/tika.yml`.


### Added

- **`tywb recompress`** — repairs `.warc.gz` files that were gzipped as a single
  deflate stream instead of one gzip member per WARC record.

  Such files decompress fine but have no record-level random access, so the
  indexer stores exactly one CDX entry per file (the first record) and replay of
  that entry fails with `gzip decode failed: unexpected end of file`, because a
  Range GET of `record length + 64 KiB` lands in the middle of a continuous
  deflate stream.

  The subcommand scans the bucket (or takes explicit `--file` keys), rewrites
  affected objects with one member per record, and replaces them on S3. Record
  bytes are copied verbatim — only the gzip framing changes. Guard rails:

  - a fresh full decompression of the original must be byte-identical to the
    concatenated members of the rewrite, and the member count must equal the
    record count, before anything on S3 is touched;
  - the original is preserved by a server-side copy to `<key>.bak`, whose size
    is verified before the object is overwritten;
  - objects that already have a `.bak` sibling, or that already are
    record-per-member, are skipped, so re-runs are safe;
  - anything structurally unexpected (missing `Content-Length`, truncated
    record, non-WARC content) aborts that object and leaves it untouched.

  `--salvage-truncated` additionally handles sources that simply stop — a gzip
  stream without its trailer, or a last WARC record cut off mid-block. Every
  complete record before the break is kept, the incomplete tail is dropped, and
  the verification invariant weakens from "payload is identical" to "payload is
  an exact prefix of the source" — still byte-for-byte over everything kept.
  Without the flag such files are reported and left alone, so bytes are never
  dropped silently.

  After replacing an object, recompress deletes its `<key>.cdx` sidecar: the
  sidecar lists compressed member offsets that the rewrite invalidated, and
  `write_cdx_sidecar` refuses to overwrite an existing sidecar, so a stale one
  would otherwise survive every future re-index. Re-indexing regenerates it.

  Flags: `--file`, `--limit`, `--jobs`, `--workdir`, `--dry-run`, `--scan-only`,
  `--backup-suffix`, `--salvage-truncated`.

  The rewritten object keeps the content type of the original — only the gzip
  framing changes, so inventing a new one would be a silent metadata change for
  every other consumer of the bucket.

- `s3_store`: `copy_object`, `delete_object` and `put_object_from_path`
  (streams a local file to S3 without buffering it in memory).

- **Search results are collapsed to one row per domain.** `/ui/search` used to
  list every hit, so a site with many matching pages pushed everything else off
  the page. Each domain now contributes a single row — its *newest* capture —
  followed by a `N more results from <domain>` disclosure holding that domain's
  remaining hits, newest first. Domains keep the order in which the search
  engine first mentioned them, so the most relevant site still comes first.

  Subdomains fold into their second-level domain, matching the
  TLD → domain → subdomain hierarchy of `/ui/browse`. The disclosure is plain
  `<details>` — no JavaScript. Because grouping thins the list out, the UI now
  retrieves 4× `server.max_results` hits (capped at 400) before grouping; the
  JSON `/search` endpoint is unchanged.

### Fixed

- **Browsing a domain served from its apex led nowhere.** `/ui/browse` decided
  whether a name was a registered domain or a hostname by counting dots, so for
  a site without a `www.` host (e.g. `walnussmeisterei.de`) the hostname listing
  linked back to itself and its 2,874 captures were unreachable. Captures now
  have an explicit `?host=` route, which cannot be confused with the domain
  level. Existing `?domain=<hostname>` links keep working.

- **Large S3 transfers were aborted mid-flight.** The SDK's stalled-stream
  protection kills a transfer whose throughput drops to 0 B/s for a moment
  ("dispatch failure: timeout: minimum throughput was specified at 1 B/s").
  Against a single-node Garage moving several hundred-MB objects at once that
  is routine, and it failed 28 of 67 objects in the first recompress run. The
  protection is now disabled and uploads retry like downloads already did.

- **Uploads to Garage failed with `InvalidRequest: Invalid payload signature`.**
  Since AWS SDK 1.72 the default `RequestChecksumCalculation::WhenSupported`
  wraps request bodies in `aws-chunked` framing with a trailing checksum, which
  Garage rejects. The client now uses `WhenRequired` for request checksums and
  response validation. This affects every S3 write tywb makes, not just
  `recompress`.

### Changed

- S3 errors now carry their full `source()` chain. The AWS SDK's top-level
  `Display` is just `service error`; the HTTP status and S3 error code that
  actually identify the problem live further down the chain.

### Notes for operators

After recompressing, re-index the affected keys with `tywb index --force` so
their CDX entries pick up the real per-record compressed offsets. Until then the
files still have their single stale CDX entry.

Known gap, not addressed here: `index` reads only the *first* WARC record out of
each gzip member. That is correct for conforming `.warc.gz` files, and
`recompress` makes non-conforming ones conform, but the indexer will still
silently drop records if such a file reappears.
