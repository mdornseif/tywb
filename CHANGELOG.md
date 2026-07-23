# Changelog

All notable changes to tywb are recorded here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
this project does not yet publish tagged releases, so everything lands under
*Unreleased* until the first version is cut.

## [Unreleased]

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

  Flags: `--file`, `--limit`, `--jobs`, `--workdir`, `--dry-run`, `--scan-only`,
  `--backup-suffix`.

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
