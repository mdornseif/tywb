# Changelog

All notable changes to tywb are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project does not yet publish tagged releases, so everything lands under
*Unreleased* until the first version is cut.

## [Unreleased]

- **The homepage no longer stalls the whole server.** `/` computed six
  aggregates over the CDX table on every request, three of them `GROUP BY`s
  with no index: ~7.7 s on a 4.1-million-record archive. Since all handlers
  share one SQLite connection behind a mutex, those seconds were charged to
  replay, search and CDX lookups as well — a probe with a 10 s timeout reported
  the service as unreachable while it was serving fine.

  The homepage now asks for scalar counts only (`CdxStore::basic_stats`), and
  the breakdowns moved to a new page, `/ui/stats`. That page runs each query on
  its own short-lived read-only connection, concurrently — WAL mode lets
  readers overlap — so it costs about its slowest query rather than the sum, and
  costs the rest of the server nothing. The measured time is printed on the page.

  `/api/stats` only ever exposed the scalar fields and is now cheap too.

  The collection cards stayed on the homepage: with a new `idx_cdx_collection`
  the *configured* collection names can be counted one at a time, touching only
  their own index entries, and the primary archive — the one collection large
  enough for counting to hurt — is derived by subtracting them from the total
  rather than counted at all. `/ui/stats` keeps the grouping version, which
  also finds collections no longer in the configuration.

- **`PRAGMA busy_timeout`.** The server and the indexer are separate processes
  on one database file. Without a timeout, a write landing while the other held
  the lock failed on the spot — and on the indexer that loses a record, because
  a failed object is marked as seen regardless.

- **OCR of Fraktur is folded into the letters people type.** Tesseract returns
  what is printed, and Fraktur prints the long s: `ſ` is a different code point
  from `s`, so an index built from it answers `Obſt` and not `Obst`. Measured on
  22 freshly OCR'd library volumes — `Obst` found none of them, `Obſt` found 21.
  A corpus nobody can search with modern spelling is not searchable.

  `pdf::normalise_historic_forms` folds the long s and the `ﬁ`/`ﬂ`/`ﬀ`
  ligatures before the quality gate sees the text, so both the indexer and
  `/text` get it. Deliberately a short explicit table rather than Unicode NFKC,
  which would also rewrite fractions, superscripts and full-width forms — none
  of which is the problem.

- **`store_text`: the extracted text is kept beside the object.** Extraction is
  the expensive half of indexing scanned material (17.5 hours for 23 volumes)
  and its result existed nowhere afterwards — the fulltext index does not store
  the body it indexes. Every later improvement to text handling therefore cost
  those hours again, which in practice means it does not happen: the long-s fix
  above needed a second full OCR run purely to re-derive text we had already
  produced.

  With `store_text: true` a collection writes `<key>.tywb.txt` next to each
  object and reads it back on the next run instead of extracting again. The
  first line binds the text to the ETag it came from, so a re-uploaded file is
  re-extracted rather than served a stale copy. Off by default — writing into
  someone's bucket is not something to start doing unasked — and the suffix is
  not a bare `.txt`, because these buckets already carry `.txt` files from the
  tools that made the PDFs.

- **A collection can narrow its prefix with `key_pattern`.** A prefix names a
  contiguous run of keys, and the material worth separating is not always stored
  that way: the 23 library digitisations in the pomologie bucket each sit in a
  directory of their own, interleaved with 1,800 other volumes under the same
  prefix. They are also the only ones with no text layer, so they are the one
  body of material that OCR is for — and pointing OCR at the whole prefix
  instead would have been days of Tesseract for nothing.

  Objects that do not match are left untouched rather than recorded as seen, so
  a later collection over the same prefix still gets them; within a run the
  first collection to claim an object keeps it, so the narrower one is listed
  first. A pattern that fails to compile stops that collection — a rule written
  to narrow a prefix must not fail open.

- **Text extraction is configured per collection.** `indexer.tika` was one
  block for every source, and a digitised library corpus and a web crawl want
  opposite settings from it. The compromise cost both: `auto` sent pre-OCR'd
  scans through Tesseract for hours, and a `max_pdf_bytes` sized for web PDFs
  dropped the highest-resolution volumes — the ones most worth having.

  A collection may now carry a `tika:` block of its own, layered over the global
  one: state what is special, inherit the rest. The Tika URL stays global on
  purpose — this decides how a document is parsed, not which service parses it.
  `/text` applies the same override the indexer used, so the text served on
  demand is the text that was indexed.

- **The collection filter is a query, not a sieve over the results.** `/search`
  and `/ui/search` retrieved the top N and then dropped everything not in the
  requested collection. With 4.1 million web captures next to a few thousand
  books, the whole first page is archive long before a book appears — so the
  filter reliably returned nothing, and the collection cards on the homepage led
  to an empty result page. The collection is indexed as a raw string precisely
  so Tantivy can do this itself; it is now a `TermQuery` combined with the
  parsed terms.

  An empty query with a collection now means "browse this collection" rather
  than "no results", which is what a collection card is asking for. Against an
  index written before the field existed, a collection filter returns nothing —
  no document there has a collection — rather than silently ignoring the filter.

- **A PDF whose extraction failed is tried again.** The collection indexer
  marked every object as seen the moment it had written a CDX record —
  including objects where Tika had just crashed or timed out. The next run
  skipped them, so a transient failure became permanent: a CDX entry, a
  replayable PDF, and no searchable text, forever. Observed live on 480 MB
  library scans that took the Tika server down with them.

  `mark_seen` is now only written when the object really was handled. The
  reason decides: too large, truncated, or text that failed the quality gate is
  an answer about the file and counts as done; a crashed extractor, a timeout
  or a failed S3 GET is an answer about the run, and the object comes back next
  time. `index_one_pdf` returns a `PdfOutcome` instead of a bare bool, because
  the caller cannot make that distinction from "no text".

- **A re-indexed collection PDF replaces its document instead of adding a
  second one.** The WARC path has always deleted a file's documents before
  re-adding them; the PDF-collection path did not, so an object whose ETag
  changed — a re-OCR'd scan re-uploaded, say — left its old text in the fulltext
  index beside the new. The CDX row was always an upsert and stayed correct,
  which is what made the duplication easy to miss.

- **An index one field behind no longer blocks every index run.** `tywb index`
  against an index written before `collection` existed aborted before touching
  a single object, with Tantivy's bare *"An index exists but the schema does not
  match"* — a message that names neither the field nor a remedy, on a run that
  had nothing to do with collections.

  The write path now opens the index as it stands and reads the field handles
  back from it, the way the read path always has. An older index stays
  searchable and writable; documents added to it carry no `collection`, and
  startup says exactly that, along with what it costs (a collection filter will
  not find them) and how to get it back (rebuild). Replay is unaffected either
  way — the collection that picks the bucket lives in the CDX database.

  A schema that differs in any *other* way — a renamed field, changed indexing
  options — is still refused, but now names the field and says what to do.

  The obvious-looking fix, rewriting `meta.json` in place to append the field,
  is deliberately not taken: segments written without a field carry no field
  norms for it, and the first background merge of an old segment with a new one
  dies with `Field norm not found for field`. There is a test for that merge,
  which is how we know.

- **Skipped PDFs say so.** A PDF over `indexer.tika.max_pdf_bytes` (default
  100 MiB) was dropped at `debug` level, i.e. silently in any normal run. The
  documents this hits are the high-resolution library scans — often the ones
  most worth having — and a quiet skip is indistinguishable from full coverage
  in the result. It is a `WARN` now, with the key, the size, and the setting to
  raise. Truncated PDFs stay at `debug`: whole crawls are capped at ~1 MiB, and
  at `WARN` they would drown out everything else.

- **Unknown configuration keys are an error.** `tika:` or `collections:`
  written at the top level instead of under `indexer:` was silently discarded;
  the run then reported `nothing to index — all objects are up to date` and
  exited successfully, having done nothing. Every block now rejects keys it
  does not know, and serde's error names the stray key and lists the ones that
  block accepts. `config.yaml` itself is parsed by a test, so the shipped file
  cannot drift out of the schema it documents.

- **`--collections-only` is documented.** The only way to index the PDF
  collections without re-walking the WARC bucket existed solely in the source.

- **`config.yaml` points at `s3.foxel.org`.** The endpoint it shipped with sits
  behind a proxy that rewrites `accept-encoding`, which breaks the SigV4
  signature: every request comes back as
  `400 InvalidRequest: signed header 'accept-encoding' is not present`, which
  reads like a credentials problem and is not one. The note above the setting
  now says so.

- **URL skip patterns.** The skip list gained a second half. Where
  `indexer.blacklisted_domains` drops a whole site,
  `indexer.blacklisted_url_patterns` (and
  `blacklisted_url_patterns_path`) drops a *kind of page* on sites that are
  otherwise wanted: wiki talk pages, version histories, `action=edit` links —
  the machinery every wiki hangs off each article, which until now was indexed
  as if it were content.

  - Each pattern is a regex matched against the whole URL, compiled once into a
    `RegexSet` at startup because the test runs per record over multi-GB WARCs.
  - Patterns act in the same three places domains already did: skipped at
    ingest, purged from CDX and the fulltext index on the next `tywb index`
    run, hidden from search results at query time. The purge is one sequential
    CDX scan — a pattern cannot be pushed into SQL the way a SURT prefix can —
    so it only runs when patterns are configured. Index entries are deleted,
    never WARC bytes: remove the rule, re-index, and the captures return.
  - A pattern that fails to compile is logged and dropped, not fatal. One that
    matches the empty string is refused outright: it would match every URL and
    purge the whole index.
  - `skip-urls.txt` ships the list. For wikis: talk namespaces in German and
    English, user pages and their subpages, `Spezial:`/`Special:`, the
    `action=`/`do=` verbs, `oldid=`/`diff=` permalinks, `api.php` and friends.
    `Kategorie:`, `Datei:` and `Portal:` are deliberately kept — navigation and
    provenance are not cruft. It is tested like code, because it decides what
    gets deleted.
  - It also absorbed the rules that used to live only in the Zeno crawler's own
    exclusion file: private address ranges and internal TLDs (a crawl seeded
    from browser history reaches router config pages and NAS boxes — nobody's
    archive, and sometimes somebody's private data), login/logout endpoints,
    `wp-admin`, session IDs, sort and calendar parameters, share buttons. The
    flow only works in one direction now, so a rule kept on the crawler side
    would have been a second, drifting copy.
  - The unimplemented `indexer.skip_patterns` glob field is gone. It never did
    anything; keeping a second, dead spelling of "URL patterns to skip" next to
    a working one only invites configuring the wrong one.

- **The skip list in the web UI, and in the crawler's format.**
  `GET /ui/skiplist` shows what is in force — domains, URL patterns with the
  comments they were written with, and separately anything configured that did
  not compile. A rule that silently does nothing is worse than no rule, so the
  page names it rather than leaving it to a log line at startup.

  `GET /skiplist.zeno` serves the same list as a Zeno `--exclusion-file`.
  Because both sides speak RE2 the patterns transfer unchanged; only domains
  are translated, to `^https?://([^/]*\.)?host([:/?#]|$)`. A crawl script
  `curl`s the URL before the run, which is the whole handover — one list, one
  place to edit it, no generator step. `?inline=1` reads it in the browser.

  This replaced a Python converter that did the same job offline. Two
  implementations of one conversion drift; and the machine it ran on had no
  PyYAML, so half of it did not run there at all.

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
  only those, then reports what a re-index would recover; `--out` writes the
  keys for `index --force --file`. Read-only.

  It counts two kinds of damage separately, because conflating them overstates
  the job badly: a **compressed** body (gzip/deflate, or undecodable) was read
  as text and indexed as `U+FFFD` noise, so those documents are unsearchable
  until re-indexed; a body that was only **chunked** still stripped to readable
  text, carrying nothing worse than stray hex tokens like `173` where the
  chunk-size lines fell. PDFs are the exception — chunk framing alone already
  makes them unparseable for Tika. A `Content-Encoding` header on a body the
  crawler had already decoded counts as neither.

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
