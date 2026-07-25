//! SQLite-backed CDX store.
//!
//! Schema holds one row per indexed WARC record with enough information to:
//! - Answer CDX API queries (URL lookup, date-range filtering)
//! - Locate the exact WARC record bytes in S3 for replay
//!
//! The store is opened in WAL mode so reads never block writes and multiple
//! concurrent readers are supported without any locking overhead.

use rusqlite::{Connection, OptionalExtension, params};
use crate::error::{CdxError, Result};
use crate::record::{CdxRecord, parse_timestamp};

// ── DDL ───────────────────────────────────────────────────────────────────────

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS cdx (
    surt_url    TEXT    NOT NULL,
    timestamp   TEXT    NOT NULL,   -- 14-digit YYYYMMDDHHmmss
    original    TEXT    NOT NULL,
    mime        TEXT,
    status      INTEGER,
    digest      TEXT,
    s3_key      TEXT    NOT NULL,
    offset      INTEGER NOT NULL,
    length      INTEGER NOT NULL,
    c_offset    INTEGER,            -- compressed offset of gzip member (NULL for .warc)
    collection  TEXT NOT NULL DEFAULT 'warc',  -- source collection; drives replay bucket
    PRIMARY KEY (surt_url, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_cdx_ts      ON cdx(timestamp);
CREATE INDEX IF NOT EXISTS idx_cdx_s3_key  ON cdx(s3_key);

CREATE TABLE IF NOT EXISTS warc_files (
    s3_key           TEXT    PRIMARY KEY,
    etag             TEXT,
    size_bytes       INTEGER,
    first_seen       TEXT    NOT NULL,  -- ISO8601 UTC, set once on first insert
    last_indexed     TEXT    NOT NULL,  -- ISO8601 UTC, updated every run
    warc_records     INTEGER NOT NULL DEFAULT 0,
    cdx_new          INTEGER NOT NULL DEFAULT 0,
    cdx_known        INTEGER NOT NULL DEFAULT 0,
    fulltext_indexed INTEGER NOT NULL DEFAULT 0,
    skipped          INTEGER NOT NULL DEFAULT 0,
    errors           INTEGER NOT NULL DEFAULT 0,
    duration_secs    REAL,
    bytes_per_sec    REAL,             -- uncompressed throughput
    records_per_sec  REAL,
    warc_date_min    TEXT,             -- earliest WARC-Date seen in this file
    warc_date_max    TEXT,             -- latest WARC-Date seen in this file
    mime_summary     TEXT,             -- JSON object: {\"text/html\": 42, ...}
    bucket           TEXT              -- S3 bucket name this file was read from
);

CREATE TABLE IF NOT EXISTS warcinfo (
    s3_key        TEXT    NOT NULL PRIMARY KEY,  -- links to warc_files.s3_key
    bucket        TEXT,                          -- S3 bucket name
    warc_date     TEXT,                          -- WARC-Date header value
    warc_filename TEXT,                          -- WARC-Filename header value
    record_id     TEXT,                          -- WARC-Record-ID header value
    headers_json  TEXT,                          -- JSON array of [name, value] pairs
    block_text    TEXT                           -- UTF-8 (lossy) content of the block
);
";

const PRAGMAS: &str = "
PRAGMA journal_mode       = WAL;
PRAGMA synchronous        = NORMAL;
PRAGMA foreign_keys       = ON;
PRAGMA wal_autocheckpoint = 0;
";

// ── WARC file metadata ────────────────────────────────────────────────────────

/// One row of the `warc_files` table — records everything known about
/// a single WARC object after it has been indexed.
#[derive(Debug, Default)]
pub struct WarcFileMeta {
    pub s3_key:           String,
    pub etag:             Option<String>,
    /// Compressed object size in bytes as reported by S3.
    pub size_bytes:       u64,
    /// ISO8601 UTC timestamp of this indexing run.
    pub indexed_at:       String,
    /// S3 bucket this file was read from.
    pub bucket:           Option<String>,
    pub warc_records:     usize,
    pub cdx_new:          usize,
    pub cdx_known:        usize,
    pub fulltext_indexed: usize,
    pub skipped:          usize,
    pub errors:           usize,
    pub duration_secs:    f64,
    /// Uncompressed decompressed bytes / duration.
    pub bytes_per_sec:    f64,
    pub records_per_sec:  f64,
    /// Earliest / latest `WARC-Date` header value seen in this file.
    pub warc_date_min:    Option<String>,
    pub warc_date_max:    Option<String>,
    /// JSON object mapping MIME type → record count.
    pub mime_summary:     Option<String>,
}

/// One row of the `warcinfo` table — the parsed `warcinfo` WARC record
/// from the start of each WARC file.
#[derive(Debug, Default)]
pub struct WarcInfoRecord {
    /// S3 key of the WARC file this record came from.
    pub s3_key:        String,
    /// S3 bucket name.
    pub bucket:        Option<String>,
    /// `WARC-Date` header value (ISO8601).
    pub warc_date:     Option<String>,
    /// `WARC-Filename` header value, if present.
    pub warc_filename: Option<String>,
    /// `WARC-Record-ID` header value.
    pub record_id:     Option<String>,
    /// All WARC header fields serialized as a JSON array of `[name, value]` pairs.
    pub headers_json:  Option<String>,
    /// UTF-8 (lossy) text of the warcinfo block (typically `application/warc-fields`).
    pub block_text:    Option<String>,
}

/// Lightweight row returned by [`CdxStore::recent_warc_files`].
/// All nullable columns from `warc_files` are wrapped in `Option`.
#[derive(Debug)]
pub struct WarcFileRow {
    pub s3_key:           String,
    pub last_indexed:     String,
    pub warc_records:     i64,
    pub cdx_new:          i64,
    pub cdx_known:        i64,
    pub fulltext_indexed: i64,
    pub errors:           i64,
    pub size_bytes:       Option<i64>,
    pub duration_secs:    Option<f64>,
    pub records_per_sec:  Option<f64>,
}

// ── Stats ─────────────────────────────────────────────────────────────────────

/// Aggregate statistics over the CDX table, returned by [`CdxStore::stats`].
#[derive(Debug)]
pub struct CdxStats {
    pub total_records:    u64,
    pub unique_urls:      u64,
    pub warc_files:       u64,
    pub oldest_timestamp: Option<String>,
    pub newest_timestamp: Option<String>,
    /// MIME type → record count, sorted descending by count (top 20).
    pub mime_counts:      Vec<(String, u64)>,
    /// HTTP status code (or `None` for unknown) → record count, sorted descending.
    pub status_counts:    Vec<(Option<u16>, u64)>,
}

// ── Store ─────────────────────────────────────────────────────────────────────

/// A handle to the SQLite CDX database.
pub struct CdxStore {
    conn: Connection,
}

impl CdxStore {
    /// Open (or create) the CDX database at `path`.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        let store = Self { conn };
        store.init()?;
        Ok(store)
    }

    /// Open an in-memory database (useful for tests).
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let store = Self { conn };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> Result<()> {
        self.conn.execute_batch(PRAGMAS)?;
        self.conn.execute_batch(SCHEMA)?;
        // Migrations: ALTER TABLE ADD COLUMN silently fails if column already exists.
        let _ = self.conn.execute("ALTER TABLE cdx ADD COLUMN c_offset INTEGER", []);
        let _ = self.conn.execute("ALTER TABLE warc_files ADD COLUMN bucket TEXT", []);
        // Existing rows predate collections and belong to the primary WARC archive.
        let _ = self.conn.execute(
            "ALTER TABLE cdx ADD COLUMN collection TEXT NOT NULL DEFAULT 'warc'", [],
        );
        Ok(())
    }

    /// Apply a `PRAGMA cache_size = -N` where N is in KiB.
    /// Call after `open()` if you want to constrain page-cache RAM.
    pub fn set_cache_kib(&self, kib: u32) -> Result<()> {
        self.conn
            .execute_batch(&format!("PRAGMA cache_size = -{kib};"))?;
        Ok(())
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Insert or replace a single CDX record.
    pub fn upsert(&self, r: &CdxRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO cdx
             (surt_url, timestamp, original, mime, status, digest, s3_key, offset, length, c_offset, collection)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                r.surt_url,
                r.timestamp,
                r.original_url,
                r.mime,
                r.status,
                r.digest,
                r.s3_key,
                r.offset as i64,
                r.length as i64,
                r.c_offset.map(|v| v as i64),
                r.collection,
            ],
        )?;
        Ok(())
    }

    /// Insert a batch of records in a single transaction.
    /// Much faster than calling `upsert` in a loop.
    pub fn upsert_batch(&mut self, records: &[CdxRecord]) -> Result<()> {
        self.upsert_batch_counted(records)?;
        Ok(())
    }

    /// Insert a batch of records and return `(new, existing)` counts.
    ///
    /// `new` — records whose (surt_url, timestamp) primary key did not exist before.
    /// `existing` — records that already existed and were updated in place.
    pub fn upsert_batch_counted(&mut self, records: &[CdxRecord]) -> Result<(usize, usize)> {
        let tx = self.conn.transaction()?;
        let mut new_count = 0usize;
        let mut existing_count = 0usize;
        {
            // INSERT OR IGNORE: affected rows = 1 for new, 0 for existing.
            let mut insert_stmt = tx.prepare_cached(
                "INSERT OR IGNORE INTO cdx
                 (surt_url, timestamp, original, mime, status, digest, s3_key, offset, length, c_offset, collection)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            )?;
            // UPDATE for rows that already existed so metadata stays current.
            let mut update_stmt = tx.prepare_cached(
                "UPDATE cdx SET original = ?3, mime = ?4, status = ?5, digest = ?6,
                 s3_key = ?7, offset = ?8, length = ?9, c_offset = ?10, collection = ?11
                 WHERE surt_url = ?1 AND timestamp = ?2",
            )?;
            for r in records {
                let c_off = r.c_offset.map(|v| v as i64);
                let inserted = insert_stmt.execute(params![
                    r.surt_url, r.timestamp, r.original_url, r.mime,
                    r.status, r.digest, r.s3_key, r.offset as i64, r.length as i64, c_off,
                    r.collection,
                ])?;
                if inserted == 1 {
                    new_count += 1;
                } else {
                    update_stmt.execute(params![
                        r.surt_url, r.timestamp, r.original_url, r.mime,
                        r.status, r.digest, r.s3_key, r.offset as i64, r.length as i64, c_off,
                        r.collection,
                    ])?;
                    existing_count += 1;
                }
            }
        }
        tx.commit()?;
        Ok((new_count, existing_count))
    }

    /// Delete all records belonging to an S3 key (e.g. when re-indexing a file).
    pub fn delete_by_s3_key(&self, s3_key: &str) -> Result<usize> {
        let n = self.conn.execute(
            "DELETE FROM cdx WHERE s3_key = ?1",
            params![s3_key],
        )?;
        Ok(n)
    }

    /// Return all distinct `original` URLs whose SURT key belongs to a domain.
    ///
    /// `surt_host` is the reversed-label form of the domain **without** the
    /// trailing `)`, e.g. `"com,example"` for `example.com`.  The query matches
    /// both the apex domain (`com,example)/…`) and any subdomain
    /// (`com,example,www)/…`, `com,example,cdn)/…`, etc.).
    pub fn original_urls_for_domain_surt(&self, surt_host: &str) -> Result<Vec<String>> {
        let apex_pat = format!("{surt_host})%");
        let sub_pat  = format!("{surt_host},%");
        let mut stmt = self.conn.prepare_cached(
            "SELECT DISTINCT original FROM cdx
             WHERE surt_url LIKE ?1 OR surt_url LIKE ?2",
        )?;
        let rows = stmt.query_map(params![apex_pat, sub_pat], |row| row.get(0))?;
        let mut urls = Vec::new();
        for r in rows {
            urls.push(r?);
        }
        Ok(urls)
    }

    /// Delete all CDX records for a domain (apex + all subdomains).
    ///
    /// `surt_host` — same format as [`original_urls_for_domain_surt`].
    /// Returns the number of rows deleted.
    pub fn delete_by_domain_surt(&self, surt_host: &str) -> Result<usize> {
        let apex_pat = format!("{surt_host})%");
        let sub_pat  = format!("{surt_host},%");
        let n = self.conn.execute(
            "DELETE FROM cdx WHERE surt_url LIKE ?1 OR surt_url LIKE ?2",
            params![apex_pat, sub_pat],
        )?;
        Ok(n)
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Look up records for an exact SURT URL, ordered by timestamp ascending.
    pub fn get_by_surt(&self, surt_url: &str) -> Result<Vec<CdxRecord>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT surt_url, timestamp, original, mime, status, digest, s3_key, offset, length, c_offset, collection
             FROM cdx WHERE surt_url = ?1 ORDER BY timestamp ASC",
        )?;
        let rows = stmt.query_map(params![surt_url], row_to_record)?;
        rows.map(|r| r.map_err(CdxError::from)).collect()
    }

    /// Look up records for an exact SURT URL within a timestamp range.
    /// `from` and `to` are 14-digit strings; both are inclusive.
    pub fn get_by_surt_range(
        &self,
        surt_url: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<CdxRecord>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT surt_url, timestamp, original, mime, status, digest, s3_key, offset, length, c_offset, collection
             FROM cdx
             WHERE surt_url = ?1 AND timestamp >= ?2 AND timestamp <= ?3
             ORDER BY timestamp ASC",
        )?;
        let rows = stmt.query_map(params![surt_url, from, to], row_to_record)?;
        rows.map(|r| r.map_err(CdxError::from)).collect()
    }

    /// Find the single record whose timestamp is closest to `target_ts`.
    /// Prefers an exact match or the closest earlier timestamp; falls back to
    /// the closest later timestamp if nothing earlier exists.
    ///
    /// This is the core lookup for Wayback replay.
    pub fn closest(
        &self,
        surt_url: &str,
        target_ts: &str,
    ) -> Result<Option<CdxRecord>> {
        // Validate the timestamp
        parse_timestamp(target_ts)?;

        // Try: latest record with timestamp <= target
        let before: Option<CdxRecord> = self
            .conn
            .query_row(
                "SELECT surt_url, timestamp, original, mime, status, digest, s3_key, offset, length, c_offset, collection
                 FROM cdx
                 WHERE surt_url = ?1 AND timestamp <= ?2
                 ORDER BY timestamp DESC LIMIT 1",
                params![surt_url, target_ts],
                row_to_record,
            )
            .optional()?;

        // Try: earliest record with timestamp > target
        let after: Option<CdxRecord> = self
            .conn
            .query_row(
                "SELECT surt_url, timestamp, original, mime, status, digest, s3_key, offset, length, c_offset, collection
                 FROM cdx
                 WHERE surt_url = ?1 AND timestamp > ?2
                 ORDER BY timestamp ASC LIMIT 1",
                params![surt_url, target_ts],
                row_to_record,
            )
            .optional()?;

        Ok(match (before, after) {
            (Some(b), Some(a)) => {
                // Pick whichever is numerically closer
                let dist_b = ts_distance(target_ts, &b.timestamp);
                let dist_a = ts_distance(target_ts, &a.timestamp);
                if dist_b <= dist_a { Some(b) } else { Some(a) }
            }
            (Some(b), None) => Some(b),
            (None, Some(a)) => Some(a),
            (None, None)    => None,
        })
    }

    /// Prefix search: return records where `surt_url` starts with `prefix`.
    /// Used for CDX API wildcard queries like `example.com/*`.
    pub fn get_by_surt_prefix(
        &self,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<CdxRecord>> {
        // SQLite LIKE or GLOB would work but BETWEEN on the prefix is faster
        // and avoids LIKE special-character escaping.
        let end = next_prefix(prefix);
        let mut stmt = self.conn.prepare_cached(
            "SELECT surt_url, timestamp, original, mime, status, digest, s3_key, offset, length, c_offset, collection
             FROM cdx
             WHERE surt_url >= ?1 AND surt_url < ?2
             ORDER BY surt_url ASC, timestamp ASC
             LIMIT ?3",
        )?;
        let rows = stmt.query_map(params![prefix, end, limit as i64], row_to_record)?;
        rows.map(|r| r.map_err(CdxError::from)).collect()
    }

    /// Total number of records in the database.
    pub fn count(&self) -> Result<u64> {
        let n: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM cdx", [], |r| r.get(0))?;
        Ok(n as u64)
    }

    /// Aggregate statistics over the whole CDX table.
    pub fn stats(&self) -> Result<CdxStats> {
        let total: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM cdx", [], |r| r.get(0))?;

        let unique_urls: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT surt_url) FROM cdx", [], |r| r.get(0))?;

        let warc_files: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT s3_key) FROM cdx", [], |r| r.get(0))?;

        let (oldest, newest): (Option<String>, Option<String>) = self.conn.query_row(
            "SELECT MIN(timestamp), MAX(timestamp) FROM cdx", [],
            |r| Ok((r.get(0)?, r.get(1)?)))?;

        let mut mime_stmt = self.conn.prepare_cached(
            "SELECT COALESCE(mime, '(none)'), COUNT(*) AS n \
             FROM cdx GROUP BY mime ORDER BY n DESC LIMIT 20")?;
        let mime_counts: Vec<(String, u64)> = mime_stmt
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64)))?
            .filter_map(|r| r.ok())
            .collect();

        let mut status_stmt = self.conn.prepare_cached(
            "SELECT status, COUNT(*) AS n \
             FROM cdx GROUP BY status ORDER BY n DESC")?;
        let status_counts: Vec<(Option<u16>, u64)> = status_stmt
            .query_map([], |r| {
                let s: Option<i64> = r.get(0)?;
                let n: i64 = r.get(1)?;
                Ok((s.map(|v| v as u16), n as u64))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(CdxStats {
            total_records: total as u64,
            unique_urls:   unique_urls as u64,
            warc_files:    warc_files as u64,
            oldest_timestamp: oldest,
            newest_timestamp: newest,
            mime_counts,
            status_counts,
        })
    }

    /// Return the most-recently-indexed WARC files (up to `limit`).
    ///
    /// Returns a lightweight row type suitable for display; columns that may be
    /// NULL are returned as `Option`.
    pub fn recent_warc_files(&self, limit: usize) -> Result<Vec<WarcFileRow>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT s3_key, last_indexed, warc_records, cdx_new, cdx_known,
                    fulltext_indexed, errors, size_bytes, duration_secs, records_per_sec
             FROM warc_files
             ORDER BY last_indexed DESC
             LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |row| {
            Ok(WarcFileRow {
                s3_key:           row.get(0)?,
                last_indexed:     row.get(1)?,
                warc_records:     row.get(2)?,
                cdx_new:          row.get(3)?,
                cdx_known:        row.get(4)?,
                fulltext_indexed: row.get(5)?,
                errors:           row.get(6)?,
                size_bytes:       row.get(7)?,
                duration_secs:    row.get(8)?,
                records_per_sec:  row.get(9)?,
            })
        })?;
        rows.map(|r| r.map_err(CdxError::from)).collect()
    }

    /// Upsert a `WarcFileMeta` row into the `warc_files` table.
    ///
    /// `first_seen` is preserved from any existing row; only `last_indexed`
    /// and the statistics columns are updated on conflict.
    pub fn upsert_warc_file(&mut self, meta: &WarcFileMeta) -> Result<()> {
        self.conn.execute(
            "INSERT INTO warc_files
             (s3_key, etag, size_bytes, first_seen, last_indexed,
              bucket, warc_records, cdx_new, cdx_known, fulltext_indexed, skipped, errors,
              duration_secs, bytes_per_sec, records_per_sec,
              warc_date_min, warc_date_max, mime_summary)
             VALUES (?1, ?2, ?3, ?4, ?4,
                     ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                     ?12, ?13, ?14,
                     ?15, ?16, ?17)
             ON CONFLICT(s3_key) DO UPDATE SET
               etag             = excluded.etag,
               size_bytes       = excluded.size_bytes,
               last_indexed     = excluded.last_indexed,
               bucket           = excluded.bucket,
               warc_records     = excluded.warc_records,
               cdx_new          = excluded.cdx_new,
               cdx_known        = excluded.cdx_known,
               fulltext_indexed = excluded.fulltext_indexed,
               skipped          = excluded.skipped,
               errors           = excluded.errors,
               duration_secs    = excluded.duration_secs,
               bytes_per_sec    = excluded.bytes_per_sec,
               records_per_sec  = excluded.records_per_sec,
               warc_date_min    = excluded.warc_date_min,
               warc_date_max    = excluded.warc_date_max,
               mime_summary     = excluded.mime_summary",
            params![
                meta.s3_key,
                meta.etag,
                meta.size_bytes as i64,
                meta.indexed_at,
                meta.bucket,
                meta.warc_records as i64,
                meta.cdx_new as i64,
                meta.cdx_known as i64,
                meta.fulltext_indexed as i64,
                meta.skipped as i64,
                meta.errors as i64,
                meta.duration_secs,
                meta.bytes_per_sec,
                meta.records_per_sec,
                meta.warc_date_min,
                meta.warc_date_max,
                meta.mime_summary,
            ],
        )?;
        Ok(())
    }

    /// Insert or replace a `warcinfo` record.
    pub fn upsert_warcinfo(&self, wi: &WarcInfoRecord) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO warcinfo
             (s3_key, bucket, warc_date, warc_filename, record_id, headers_json, block_text)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                wi.s3_key,
                wi.bucket,
                wi.warc_date,
                wi.warc_filename,
                wi.record_id,
                wi.headers_json,
                wi.block_text,
            ],
        )?;
        Ok(())
    }

    // ── Domain browsing ───────────────────────────────────────────────────────

    /// Return TLDs (first SURT component, e.g. `com`, `de`) with record counts,
    /// sorted descending by count.
    pub fn browse_tlds(&self, limit: usize) -> Result<Vec<(String, u64)>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT substr(surt_url, 1, instr(surt_url, ',') - 1) AS tld,
                    COUNT(*) AS n
             FROM   cdx
             GROUP  BY tld
             ORDER  BY n DESC
             LIMIT  ?1",
        )?;
        let rows = stmt.query_map(params![limit as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
        })?;
        rows.map(|r| r.map_err(CdxError::from)).collect()
    }

    /// Return all unique registered-domain SURT prefixes (`tld,name`) under
    /// `tld`, with record counts summed across all subdomains, sorted descending.
    ///
    /// E.g. for `tld = "de"` returns `("de,obstsortendatenbank", 42)` where 42
    /// is the total count for both `obstsortendatenbank.de` and
    /// `www.obstsortendatenbank.de`.
    pub fn browse_domains(&self, tld: &str, limit: usize) -> Result<Vec<(String, u64)>> {
        let lower = format!("{},", tld);
        let upper = next_prefix(&lower);
        // Extract only the first two SURT labels (tld,name) so all subdomains
        // of the same registered domain collapse into one row.
        // length(?1) is len("de,") = len(tld)+1; the label starts at position
        // length(?1)+1.  If a second comma exists in that suffix the label ends
        // just before it; otherwise we take everything up to ')'.
        let mut stmt = self.conn.prepare_cached(
            "SELECT CASE
                      WHEN instr(substr(surt_url, length(?1)+1), ',') > 0
                      THEN substr(surt_url, 1,
                                  length(?1) + instr(substr(surt_url, length(?1)+1), ',') - 1)
                      ELSE substr(surt_url, 1, instr(surt_url, ')') - 1)
                    END AS surt_domain,
                    COUNT(*) AS n
             FROM   cdx
             WHERE  surt_url >= ?1 AND surt_url < ?2
             GROUP  BY surt_domain
             ORDER  BY n DESC
             LIMIT  ?3",
        )?;
        let rows = stmt.query_map(params![lower, upper, limit as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
        })?;
        rows.map(|r| r.map_err(CdxError::from)).collect()
    }

    /// Return all distinct hostnames (full SURT domain prefix, before `)`) under
    /// a registered-domain SURT prefix, with record counts sorted descending.
    ///
    /// `surt_prefix` should be the two-label form, e.g. `"de,obstsortendatenbank"`.
    /// Returns both the apex (`de,obstsortendatenbank`) and all subdomains
    /// (`de,obstsortendatenbank,www`, …).
    pub fn browse_subdomains(&self, surt_prefix: &str, limit: usize) -> Result<Vec<(String, u64)>> {
        // ')' (0x29) < ',' (0x2C), so apex records sort before subdomain records.
        // Range [surt_prefix+")", next_prefix(surt_prefix+",")] covers both.
        let lower = format!("{})", surt_prefix);
        let upper = next_prefix(&format!("{},", surt_prefix));
        let mut stmt = self.conn.prepare_cached(
            "SELECT substr(surt_url, 1, instr(surt_url, ')') - 1) AS surt_domain,
                    COUNT(*) AS n
             FROM   cdx
             WHERE  surt_url >= ?1 AND surt_url < ?2
             GROUP  BY surt_domain
             ORDER  BY n DESC
             LIMIT  ?3",
        )?;
        let rows = stmt.query_map(params![lower, upper, limit as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)? as u64))
        })?;
        rows.map(|r| r.map_err(CdxError::from)).collect()
    }

    /// Check whether a record exists for this (surt_url, timestamp) pair.
    pub fn exists(&self, surt_url: &str, timestamp: &str) -> Result<bool> {
        let n: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM cdx WHERE surt_url = ?1 AND timestamp = ?2",
            params![surt_url, timestamp],
            |r| r.get(0),
        )?;
        Ok(n > 0)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<CdxRecord> {
    Ok(CdxRecord {
        surt_url:     row.get(0)?,
        timestamp:    row.get(1)?,
        original_url: row.get(2)?,
        mime:         row.get(3)?,
        status:       row.get::<_, Option<i64>>(4)?.map(|v| v as u16),
        digest:       row.get(5)?,
        s3_key:       row.get(6)?,
        offset:       row.get::<_, i64>(7)? as u64,
        length:       row.get::<_, i64>(8)? as u64,
        c_offset:     row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
        collection:   row.get(10)?,
    })
}

/// Numeric distance between two 14-digit timestamp strings.
/// Works because they are zero-padded and lexicographically ordered by value.
fn ts_distance(a: &str, b: &str) -> u64 {
    let ai: u64 = a.parse().unwrap_or(0);
    let bi: u64 = b.parse().unwrap_or(0);
    ai.abs_diff(bi)
}

/// Given a string prefix, return the "next" string for use in a BETWEEN query.
/// Increments the last byte; if overflow, pops and retries.
fn next_prefix(prefix: &str) -> String {
    let mut bytes = prefix.as_bytes().to_vec();
    loop {
        match bytes.last_mut() {
            Some(b) if *b < 0xFF => {
                *b += 1;
                break;
            }
            Some(_) => {
                bytes.pop();
            }
            None => break, // empty prefix — return empty (no upper bound)
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(surt: &str, ts: &str, original: &str) -> CdxRecord {
        CdxRecord {
            surt_url:     surt.to_owned(),
            timestamp:    ts.to_owned(),
            original_url: original.to_owned(),
            mime:         Some("text/html".to_owned()),
            status:       Some(200),
            digest:       Some("sha1:TESTHASH".to_owned()),
            s3_key:       "test/archive.warc.gz".to_owned(),
            offset:       1024,
            length:       512,
            c_offset:     None,
            collection:   "warc".to_owned(),
        }
    }

    fn store_with_samples() -> CdxStore {
        let mut store = CdxStore::open_in_memory().unwrap();
        let records = vec![
            sample("com,example)/",     "20240101120000", "https://example.com/"),
            sample("com,example)/",     "20240601120000", "https://example.com/"),
            sample("com,example)/",     "20241201120000", "https://example.com/"),
            sample("com,example)/page", "20240315080000", "https://example.com/page"),
            sample("com,example)/page", "20240315160000", "https://example.com/page"),
            sample("org,other)/",       "20240601000000", "https://other.org/"),
        ];
        store.upsert_batch(&records).unwrap();
        store
    }

    // ── Schema / open ─────────────────────────────────────────────────────────

    #[test]
    fn open_in_memory_succeeds() {
        CdxStore::open_in_memory().unwrap();
    }

    #[test]
    fn open_creates_schema() {
        let store = CdxStore::open_in_memory().unwrap();
        assert_eq!(store.count().unwrap(), 0);
    }

    // ── Upsert / count ────────────────────────────────────────────────────────

    #[test]
    fn upsert_single_record() {
        let store = CdxStore::open_in_memory().unwrap();
        store.upsert(&sample("com,example)/", "20240101000000", "https://example.com/")).unwrap();
        assert_eq!(store.count().unwrap(), 1);
    }

    #[test]
    fn upsert_replaces_on_duplicate_key() {
        let store = CdxStore::open_in_memory().unwrap();
        let mut r = sample("com,example)/", "20240101000000", "https://example.com/");
        store.upsert(&r).unwrap();
        r.s3_key = "updated.warc.gz".to_owned();
        store.upsert(&r).unwrap();
        assert_eq!(store.count().unwrap(), 1);
        let results = store.get_by_surt("com,example)/").unwrap();
        assert_eq!(results[0].s3_key, "updated.warc.gz");
    }

    #[test]
    fn upsert_batch_inserts_all() {
        let store = store_with_samples();
        assert_eq!(store.count().unwrap(), 6);
    }

    #[test]
    fn upsert_batch_is_transactional() {
        let mut store = CdxStore::open_in_memory().unwrap();
        // No panic / partial insert
        let records = vec![
            sample("com,example)/", "20240101000000", "https://example.com/"),
            sample("com,example)/", "20240201000000", "https://example.com/"),
        ];
        store.upsert_batch(&records).unwrap();
        assert_eq!(store.count().unwrap(), 2);
    }

    // ── get_by_surt ───────────────────────────────────────────────────────────

    #[test]
    fn get_by_surt_returns_in_ts_order() {
        let store = store_with_samples();
        let rows = store.get_by_surt("com,example)/").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].timestamp, "20240101120000");
        assert_eq!(rows[1].timestamp, "20240601120000");
        assert_eq!(rows[2].timestamp, "20241201120000");
    }

    #[test]
    fn get_by_surt_unknown_url_returns_empty() {
        let store = store_with_samples();
        let rows = store.get_by_surt("com,unknown)/").unwrap();
        assert!(rows.is_empty());
    }

    // ── get_by_surt_range ─────────────────────────────────────────────────────

    #[test]
    fn get_by_surt_range_filters_correctly() {
        let store = store_with_samples();
        let rows = store
            .get_by_surt_range("com,example)/", "20240101000000", "20240701000000")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, "20240101120000");
        assert_eq!(rows[1].timestamp, "20240601120000");
    }

    #[test]
    fn get_by_surt_range_exclusive_bounds() {
        let store = store_with_samples();
        // Exactly one record at 20240601120000
        let rows = store
            .get_by_surt_range("com,example)/", "20240601120000", "20240601120000")
            .unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn get_by_surt_range_no_results() {
        let store = store_with_samples();
        let rows = store
            .get_by_surt_range("com,example)/", "20230101000000", "20230201000000")
            .unwrap();
        assert!(rows.is_empty());
    }

    // ── closest ───────────────────────────────────────────────────────────────

    #[test]
    fn closest_exact_match() {
        let store = store_with_samples();
        let rec = store.closest("com,example)/", "20240601120000").unwrap().unwrap();
        assert_eq!(rec.timestamp, "20240601120000");
    }

    #[test]
    fn closest_prefers_earlier() {
        let store = store_with_samples();
        // Between 20240101 and 20240601 — closer to 20240101
        let rec = store.closest("com,example)/", "20240201000000").unwrap().unwrap();
        assert_eq!(rec.timestamp, "20240101120000");
    }

    #[test]
    fn closest_picks_later_when_only_later_exists() {
        let store = store_with_samples();
        // Before all records
        let rec = store.closest("com,example)/", "20230101000000").unwrap().unwrap();
        assert_eq!(rec.timestamp, "20240101120000");
    }

    #[test]
    fn closest_picks_earlier_when_only_earlier_exists() {
        let store = store_with_samples();
        // After all records
        let rec = store.closest("com,example)/", "20991231235959").unwrap().unwrap();
        assert_eq!(rec.timestamp, "20241201120000");
    }

    #[test]
    fn closest_unknown_url_returns_none() {
        let store = store_with_samples();
        let rec = store.closest("com,unknown)/", "20240601000000").unwrap();
        assert!(rec.is_none());
    }

    #[test]
    fn closest_invalid_timestamp_returns_error() {
        let store = store_with_samples();
        assert!(store.closest("com,example)/", "bad-ts").is_err());
    }

    // ── prefix search ─────────────────────────────────────────────────────────

    #[test]
    fn prefix_search_finds_all_under_domain() {
        let store = store_with_samples();
        let rows = store.get_by_surt_prefix("com,example)", 100).unwrap();
        // Should match "com,example)/" and "com,example)/page"
        assert_eq!(rows.len(), 5); // 3 + 2
    }

    #[test]
    fn prefix_search_respects_limit() {
        let store = store_with_samples();
        let rows = store.get_by_surt_prefix("com,example)", 2).unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn prefix_search_no_match() {
        let store = store_with_samples();
        let rows = store.get_by_surt_prefix("net,example)", 100).unwrap();
        assert!(rows.is_empty());
    }

    // ── exists ────────────────────────────────────────────────────────────────

    #[test]
    fn exists_true_for_known_record() {
        let store = store_with_samples();
        assert!(store.exists("com,example)/", "20240101120000").unwrap());
    }

    #[test]
    fn exists_false_for_unknown_record() {
        let store = store_with_samples();
        assert!(!store.exists("com,example)/", "19990101000000").unwrap());
    }

    // ── delete ────────────────────────────────────────────────────────────────

    #[test]
    fn delete_by_s3_key_removes_records() {
        let store = store_with_samples();
        let deleted = store.delete_by_s3_key("test/archive.warc.gz").unwrap();
        assert_eq!(deleted, 6);
        assert_eq!(store.count().unwrap(), 0);
    }

    #[test]
    fn delete_by_s3_key_unknown_key_deletes_zero() {
        let store = store_with_samples();
        let deleted = store.delete_by_s3_key("nonexistent.warc.gz").unwrap();
        assert_eq!(deleted, 0);
        assert_eq!(store.count().unwrap(), 6);
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    #[test]
    fn next_prefix_increments_last_byte() {
        assert_eq!(next_prefix("com,example)"), "com,example*");
    }

    #[test]
    fn next_prefix_empty_string() {
        assert_eq!(next_prefix(""), "");
    }

    #[test]
    fn ts_distance_zero_for_equal() {
        assert_eq!(ts_distance("20240101120000", "20240101120000"), 0);
    }

    #[test]
    fn ts_distance_positive() {
        assert!(ts_distance("20240101120000", "20240101130000") > 0);
    }
}
