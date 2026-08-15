//! Tantivy fulltext index wrapper for warc-search.
//!
//! Provides two types:
//! - [`SearchIndex`]: open/create, add documents, commit.  Used by the indexer.
//! - [`SearchReader`]: open an existing index read-only.  Used by the server.

use std::path::Path;
use tantivy::{
    Index, IndexWriter, IndexReader, ReloadPolicy, TantivyDocument,
    schema::{Schema, Field, NumericOptions, STRING, TEXT, STORED},
    directory::MmapDirectory,
    query::QueryParser,
    collector::TopDocs,
};
use thiserror::Error;
use tracing::warn;

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum SearchError {
    #[error("tantivy error: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("query parse error: {0}")]
    QueryParse(#[from] tantivy::query::QueryParserError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("schema field not found: {0}")]
    FieldNotFound(String),
    #[error("the index at {path} was written with an incompatible schema: {reason}.\n\
             It cannot be written to by this build — move the directory aside and \
             re-run `tywb index` to rebuild it from S3.")]
    SchemaMismatch { path: String, reason: String },
}

pub type Result<T> = std::result::Result<T, SearchError>;

// ── Document types ────────────────────────────────────────────────────────────

/// A document to add to the fulltext index.
#[derive(Debug, Clone)]
pub struct IndexDoc {
    /// Original URL (e.g. `https://example.com/page`).
    pub url: String,
    /// 14-digit capture timestamp as u64 (e.g. `20240315120000`).
    pub timestamp: u64,
    /// Page title (extracted from `<title>` or similar).
    pub title: String,
    /// Page body text, HTML-stripped and truncated to `max_text_bytes`.
    pub body: String,
    /// MIME type (e.g. `text/html`), if known.
    pub mime: Option<String>,
    /// S3 object key of the source WARC file.
    pub s3_key: String,
    /// Byte offset of the WARC record within the uncompressed stream.
    pub offset: u64,
    /// Byte length of the WARC record block.
    pub length: u64,
    /// Name of the source collection ("warc" for the primary archive, or a
    /// named collection such as "obst-pdfs").
    pub collection: String,
}

/// A single fulltext search result.
#[derive(Debug, Clone, serde::Serialize)]
pub struct SearchHit {
    pub url: String,
    /// 14-digit capture timestamp (zero-padded).
    pub timestamp: String,
    pub title: String,
    pub mime: Option<String>,
    pub s3_key: String,
    pub offset: u64,
    pub length: u64,
    pub score: f32,
    /// Source collection, if the index carries it (older indexes may not).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collection: Option<String>,
}

// ── Schema internals ──────────────────────────────────────────────────────────

/// Holds all schema field handles.  Field indices must match the order in
/// `build_schema`, so they stay stable across opens of the same index directory.
struct Fields {
    url: Field,
    timestamp: Field,
    title: Field,
    body: Field,
    mime: Field,
    s3_key: Field,
    offset: Field,
    length: Field,
    /// Present only when the index schema carries a `collection` field.
    collection: Option<Field>,
}

fn build_schema() -> (Schema, Fields) {
    let mut b = Schema::builder();

    let url       = b.add_text_field("url",       STRING | STORED);
    // Timestamp is stored only; range filtering is done post-retrieval.
    let timestamp = b.add_u64_field("timestamp",  NumericOptions::default().set_stored());
    let title     = b.add_text_field("title",     TEXT | STORED);
    // Body is indexed but not stored to save space.
    let body      = b.add_text_field("body",      TEXT);
    let mime      = b.add_text_field("mime",      STRING | STORED);
    let s3_key    = b.add_text_field("s3_key",    STRING | STORED);
    let offset    = b.add_u64_field("offset",     NumericOptions::default().set_stored());
    let length    = b.add_u64_field("length",     NumericOptions::default().set_stored());
    let collection = b.add_text_field("collection", STRING | STORED);

    let schema = b.build();
    let fields = Fields { url, timestamp, title, body, mime, s3_key, offset, length,
                          collection: Some(collection) };
    (schema, fields)
}

/// Read field handles from an already-opened index schema.
fn fields_from_index_schema(schema: &Schema) -> Result<Fields> {
    let f = |name: &str| {
        schema
            .get_field(name)
            .map_err(|_| SearchError::FieldNotFound(name.to_owned()))
    };
    Ok(Fields {
        url:       f("url")?,
        timestamp: f("timestamp")?,
        title:     f("title")?,
        body:      f("body")?,
        mime:      f("mime")?,
        s3_key:    f("s3_key")?,
        offset:    f("offset")?,
        length:    f("length")?,
        // Optional: indexes built before collections lack this field.
        collection: schema.get_field("collection").ok(),
    })
}

// ── Schema compatibility ──────────────────────────────────────────────────────
//
// Tantivy stores the schema in `meta.json` and refuses to open an index whose
// schema differs from the one handed to `open_or_create` — the error is a bare
// "An index exists but the schema does not match", which says nothing about
// which field or what to do.  Every field this project has added since the
// first release was *appended*: `collection` is the current example, and an
// index written before collections existed is otherwise perfectly usable.
//
// Such an index is opened as it is, with the field handles read back from it
// rather than assumed — the same thing the read path has always done.  The
// appended field is then simply not written: `add_document` skips it and
// `search_impl` reports `None` for it, which both already handle.
//
// The schema is deliberately *not* rewritten in place to add the field.  That
// looks like it should work — old segments would just read back empty — but a
// segment written without the field carries no field norms for it, and the
// first background merge of an old segment with a new one dies with
// "Field norm not found for field". An index that opens and indexes fine and
// then loses everything on a merge hours later is worse than one that says up
// front what it cannot do. Bringing an old index up to a new field means
// rebuilding it; nothing else is honest.

/// How the schema stored in an index relates to the one this build writes.
#[derive(Debug, PartialEq)]
enum SchemaCompat {
    /// Field for field the same — full functionality.
    Identical,
    /// The stored schema is this build's with a suffix of fields missing.
    /// Readable and writable; the named fields stay empty.  Carries their names.
    Behind(Vec<String>),
    /// Anything else: a renamed field, changed options, a removed field, or a
    /// field this build does not know.  Carries a human-readable reason.
    Incompatible(String),
}

/// Field entries of a schema, as the JSON Tantivy itself stores in `meta.json`.
fn schema_fields(schema: &Schema) -> Result<Vec<serde_json::Value>> {
    match serde_json::to_value(schema).map_err(std::io::Error::other)? {
        serde_json::Value::Array(v) => Ok(v),
        other => Err(SearchError::SchemaMismatch {
            path: "meta.json".to_owned(),
            reason: format!("schema did not serialise to a field list but to {other}"),
        }),
    }
}

fn field_name(entry: &serde_json::Value) -> String {
    entry.get("name").and_then(|n| n.as_str()).unwrap_or("?").to_owned()
}

fn schema_compat(stored: &Schema, target: &Schema) -> Result<SchemaCompat> {
    let old = schema_fields(stored)?;
    let new = schema_fields(target)?;

    if old == new {
        return Ok(SchemaCompat::Identical);
    }
    if old.len() > new.len() {
        return Ok(SchemaCompat::Incompatible(format!(
            "the index has {} fields, this build writes {} — fields were removed",
            old.len(),
            new.len()
        )));
    }
    for (i, stored_field) in old.iter().enumerate() {
        if stored_field != &new[i] {
            return Ok(SchemaCompat::Incompatible(format!(
                "field {i} differs: the index has `{}`, this build writes `{}` \
                 (a renamed field, or changed indexing options)",
                field_name(stored_field),
                field_name(&new[i])
            )));
        }
    }
    Ok(SchemaCompat::Behind(new[old.len()..].iter().map(field_name).collect()))
}

// ── Shared search implementation ──────────────────────────────────────────────

fn search_impl(
    index: &Index,
    reader: &IndexReader,
    fields: &Fields,
    query_str: &str,
    limit: usize,
    from_ts: Option<u64>,
    to_ts: Option<u64>,
) -> Result<Vec<SearchHit>> {
    let searcher = reader.searcher();

    let qp = QueryParser::for_index(index, vec![fields.title, fields.body]);
    let query = qp.parse_query(query_str)?;

    // Fetch extra results when post-filtering by timestamp is needed.
    let fetch_limit = if from_ts.is_some() || to_ts.is_some() {
        (limit * 20).max(200)
    } else {
        limit
    };

    let top_docs = searcher.search(&query, &TopDocs::with_limit(fetch_limit))?;

    let mut hits = Vec::with_capacity(limit);
    for (score, addr) in top_docs {
        if hits.len() >= limit {
            break;
        }
        let doc: TantivyDocument = searcher.doc(addr)?;

        let ts = get_u64(&doc, fields.timestamp).unwrap_or(0);
        if let Some(from) = from_ts {
            if ts < from {
                continue;
            }
        }
        if let Some(to) = to_ts {
            if ts > to {
                continue;
            }
        }

        hits.push(SearchHit {
            url:       get_str(&doc, fields.url).unwrap_or_default(),
            timestamp: format!("{ts:014}"),
            title:     get_str(&doc, fields.title).unwrap_or_default(),
            mime:      get_str(&doc, fields.mime),
            s3_key:    get_str(&doc, fields.s3_key).unwrap_or_default(),
            offset:    get_u64(&doc, fields.offset).unwrap_or(0),
            length:    get_u64(&doc, fields.length).unwrap_or(0),
            score,
            collection: fields.collection.and_then(|f| get_str(&doc, f)),
        });
    }

    Ok(hits)
}

// ── SearchIndex ───────────────────────────────────────────────────────────────

/// Default Tantivy writer heap (50 MiB).
pub const DEFAULT_HEAP_BYTES: usize = 50 * 1024 * 1024;

/// Fulltext index with a writer — used by the indexer binary.
///
/// Holds both the read and write handles.  Only one `SearchIndex` per directory
/// may exist at a time (Tantivy acquires a file lock for the writer).
pub struct SearchIndex {
    index: Index,
    writer: IndexWriter,
    reader: IndexReader,
    fields: Fields,
}

impl SearchIndex {
    /// Open an existing index at `path`, or create a new one.
    pub fn open_or_create(path: impl AsRef<Path>) -> Result<Self> {
        Self::with_heap(path, DEFAULT_HEAP_BYTES)
    }

    /// Open/create with a custom writer heap size in bytes.
    ///
    /// An existing index is opened as it stands and its field handles read back
    /// from it, the way the read path has always done — an index written before
    /// a field was appended stays writable, minus that field (see
    /// [`SchemaCompat`]).  Only a genuinely incompatible schema is an error, and
    /// it says which field and why.
    pub fn with_heap(path: impl AsRef<Path>, heap_bytes: usize) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let (schema, _) = build_schema();
        let dir = MmapDirectory::open(path)
            .map_err(|e| std::io::Error::other(e.to_string()))?;

        let exists = Index::exists(&dir).map_err(|e| std::io::Error::other(e.to_string()))?;
        let index = if exists {
            let existing = Index::open(dir)?;
            match schema_compat(&existing.schema(), &schema)? {
                SchemaCompat::Identical => {}
                SchemaCompat::Behind(missing) => warn!(
                    index = %path.display(),
                    fields = %missing.join(", "),
                    "index predates these fields — it stays searchable and writable, \
                     but new documents will not carry them (a collection filter \
                     will not find them). Rebuild the index to get them back.",
                ),
                SchemaCompat::Incompatible(reason) => {
                    return Err(SearchError::SchemaMismatch {
                        path: path.display().to_string(),
                        reason,
                    });
                }
            }
            existing
        } else {
            Index::create(dir, schema, tantivy::IndexSettings::default())?
        };

        let fields = fields_from_index_schema(&index.schema())?;
        let writer = index.writer(heap_bytes)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(Self { index, writer, reader, fields })
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Queue a document for indexing.  Not visible until [`commit`] is called.
    pub fn add_document(&mut self, doc: &IndexDoc) -> Result<()> {
        let mut d = TantivyDocument::default();
        d.add_text(self.fields.url,       &doc.url);
        d.add_u64( self.fields.timestamp, doc.timestamp);
        d.add_text(self.fields.title,     &doc.title);
        d.add_text(self.fields.body,      &doc.body);
        if let Some(m) = &doc.mime {
            d.add_text(self.fields.mime, m);
        }
        d.add_text(self.fields.s3_key, &doc.s3_key);
        d.add_u64( self.fields.offset, doc.offset);
        d.add_u64( self.fields.length, doc.length);
        if let Some(f) = self.fields.collection {
            d.add_text(f, &doc.collection);
        }
        self.writer.add_document(d)?;
        Ok(())
    }

    /// Commit buffered documents to disk and expose them for searching.
    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        // Force the reader to pick up the new segment immediately rather than
        // waiting for the async reload delay.
        self.reader.reload()?;
        Ok(())
    }

    /// Queue deletion of all documents whose `url` field exactly matches one of
    /// the given URLs.  Changes are not visible until [`commit`] is called.
    ///
    /// Returns the number of delete operations queued (one per distinct URL).
    pub fn delete_urls(&mut self, urls: &[String]) -> Result<usize> {
        for url in urls {
            let term = tantivy::Term::from_field_text(self.fields.url, url);
            self.writer.delete_term(term);
        }
        Ok(urls.len())
    }

    /// Queue deletion of every document belonging to a single WARC object,
    /// matched by its exact `s3_key`.  Used to cleanly *replace* a file's
    /// contribution when it is re-indexed: `delete_term` only affects
    /// already-committed documents (lower opstamp), so documents added after
    /// this call in the same commit survive.  Not visible until [`commit`].
    ///
    /// Keyed on `s3_key` rather than `url` on purpose: the archive holds
    /// multiple captures of the same URL across different WARC files (revisits),
    /// and deleting by URL would strip the other files' captures too.
    pub fn delete_s3_key(&mut self, s3_key: &str) -> Result<()> {
        let term = tantivy::Term::from_field_text(self.fields.s3_key, s3_key);
        self.writer.delete_term(term);
        Ok(())
    }

    // ── Reads ─────────────────────────────────────────────────────────────────

    /// Fulltext search.
    ///
    /// `from_ts` / `to_ts` are optional 14-digit timestamps (as u64) to filter
    /// by capture date.  Filtering is applied post-retrieval.
    pub fn search(
        &self,
        query_str: &str,
        limit: usize,
        from_ts: Option<u64>,
        to_ts: Option<u64>,
    ) -> Result<Vec<SearchHit>> {
        search_impl(&self.index, &self.reader, &self.fields, query_str, limit, from_ts, to_ts)
    }

    /// Total number of documents visible to the current reader.
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}

// ── SearchReader ──────────────────────────────────────────────────────────────

/// Read-only handle to a Tantivy index — used by the server binary.
///
/// Does not acquire a writer lock, so it can coexist with a running indexer.
/// The reader picks up new segments whenever the indexer commits.
pub struct SearchReader {
    index: Index,
    reader: IndexReader,
    fields: Fields,
}

impl SearchReader {
    /// Open an existing index at `path` for reading.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let dir = MmapDirectory::open(path.as_ref())
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let index = Index::open(dir)?;
        let fields = fields_from_index_schema(&index.schema())?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;
        Ok(Self { index, reader, fields })
    }

    /// Fulltext search (same semantics as [`SearchIndex::search`]).
    pub fn search(
        &self,
        query_str: &str,
        limit: usize,
        from_ts: Option<u64>,
        to_ts: Option<u64>,
    ) -> Result<Vec<SearchHit>> {
        search_impl(&self.index, &self.reader, &self.fields, query_str, limit, from_ts, to_ts)
    }

    /// Total number of indexed documents.
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}

// ── Document field helpers ────────────────────────────────────────────────────

fn get_str(doc: &TantivyDocument, field: Field) -> Option<String> {
    doc.get_first(field).and_then(|v| {
        if let tantivy::schema::OwnedValue::Str(s) = v {
            Some(s.clone())
        } else {
            None
        }
    })
}

fn get_u64(doc: &TantivyDocument, field: Field) -> Option<u64> {
    doc.get_first(field).and_then(|v| {
        if let tantivy::schema::OwnedValue::U64(n) = v {
            Some(*n)
        } else {
            None
        }
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn doc(url: &str, title: &str, body: &str, ts: u64) -> IndexDoc {
        IndexDoc {
            url:       url.to_owned(),
            timestamp: ts,
            title:     title.to_owned(),
            body:      body.to_owned(),
            mime:      Some("text/html".to_owned()),
            s3_key:    "test/archive.warc.gz".to_owned(),
            offset:    1024,
            length:    512,
            collection: "warc".to_owned(),
        }
    }

    /// Like [`doc`] but with an explicit `s3_key` — for exercising per-file
    /// replace semantics ([`SearchIndex::delete_s3_key`]).
    fn doc_in(url: &str, s3_key: &str) -> IndexDoc {
        IndexDoc {
            url:       url.to_owned(),
            timestamp: 20240315120000,
            title:     "t".to_owned(),
            body:      "some indexable body text".to_owned(),
            mime:      Some("text/html".to_owned()),
            s3_key:    s3_key.to_owned(),
            offset:    0,
            length:    1,
            collection: "warc".to_owned(),
        }
    }

    // ── Construction ──────────────────────────────────────────────────────────

    #[test]
    fn open_or_create_empty() {
        let dir = tempdir().unwrap();
        let idx = SearchIndex::open_or_create(dir.path()).unwrap();
        assert_eq!(idx.num_docs(), 0);
    }

    #[test]
    fn add_and_commit_single_doc() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc(
            "https://example.com/",
            "Example Domain",
            "This domain is for illustrative examples in documentation",
            20240315120000,
        ))
        .unwrap();
        idx.commit().unwrap();
        assert_eq!(idx.num_docs(), 1);
    }

    // ── Search ────────────────────────────────────────────────────────────────

    #[test]
    fn search_finds_body_match() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc(
            "https://example.com/",
            "Example",
            "illustrative documentation examples",
            20240315120000,
        ))
        .unwrap();
        idx.commit().unwrap();

        let hits = idx.search("illustrative", 10, None, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].url, "https://example.com/");
    }

    #[test]
    fn search_finds_title_match() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc(
            "https://example.com/",
            "UniqueXYZTitle",
            "ordinary body",
            20240315120000,
        ))
        .unwrap();
        idx.commit().unwrap();

        let hits = idx.search("UniqueXYZTitle", 10, None, None).unwrap();
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn search_empty_for_no_match() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc("https://a.com/", "Title", "content here", 20240101120000))
            .unwrap();
        idx.commit().unwrap();

        let hits = idx.search("xyzzy_nonexistent_term", 10, None, None).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn search_respects_limit() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        for i in 0..10u64 {
            idx.add_document(&doc(
                &format!("https://example.com/{i}"),
                &format!("Page {i}"),
                "rust programming language tutorial",
                20240101000000 + i * 10000,
            ))
            .unwrap();
        }
        idx.commit().unwrap();

        let hits = idx.search("rust", 3, None, None).unwrap();
        assert!(hits.len() <= 3);
    }

    // ── Timestamp filtering ───────────────────────────────────────────────────

    #[test]
    fn search_filter_from_ts() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc("https://a.com/", "A", "rust search", 20240101120000))
            .unwrap();
        idx.add_document(&doc("https://b.com/", "B", "rust search", 20240601120000))
            .unwrap();
        idx.add_document(&doc("https://c.com/", "C", "rust search", 20241201120000))
            .unwrap();
        idx.commit().unwrap();

        let hits = idx.search("rust", 10, Some(20240601000000), None).unwrap();
        assert_eq!(hits.len(), 2);
        for h in &hits {
            let ts: u64 = h.timestamp.parse().unwrap();
            assert!(ts >= 20240601000000);
        }
    }

    #[test]
    fn search_filter_to_ts() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc("https://a.com/", "A", "rust search", 20240101120000))
            .unwrap();
        idx.add_document(&doc("https://b.com/", "B", "rust search", 20240601120000))
            .unwrap();
        idx.add_document(&doc("https://c.com/", "C", "rust search", 20241201120000))
            .unwrap();
        idx.commit().unwrap();

        let hits = idx.search("rust", 10, None, Some(20240601235959)).unwrap();
        assert_eq!(hits.len(), 2);
    }

    #[test]
    fn search_filter_ts_range() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc("https://a.com/", "A", "rust search", 20240101120000))
            .unwrap();
        idx.add_document(&doc("https://b.com/", "B", "rust search", 20240601120000))
            .unwrap();
        idx.add_document(&doc("https://c.com/", "C", "rust search", 20241201120000))
            .unwrap();
        idx.commit().unwrap();

        let hits = idx
            .search("rust", 10, Some(20240601000000), Some(20240701000000))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].url, "https://b.com/");
    }

    // ── Persistence ───────────────────────────────────────────────────────────

    #[test]
    fn index_survives_reopen() {
        let dir = tempdir().unwrap();
        {
            let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
            idx.add_document(&doc("https://persist.com/", "Persisted", "content here", 20240101120000))
                .unwrap();
            idx.commit().unwrap();
        }
        let idx = SearchIndex::open_or_create(dir.path()).unwrap();
        assert_eq!(idx.num_docs(), 1);
        let hits = idx.search("content", 10, None, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].url, "https://persist.com/");
    }

    #[test]
    fn search_reader_reads_existing_index() {
        let dir = tempdir().unwrap();
        {
            let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
            idx.add_document(&doc("https://reader-test.com/", "Reader Test", "content here", 20240101120000))
                .unwrap();
            idx.commit().unwrap();
        }
        let reader = SearchReader::open(dir.path()).unwrap();
        assert_eq!(reader.num_docs(), 1);
        let hits = reader.search("content", 10, None, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].url, "https://reader-test.com/");
    }

    // ── SearchHit fields ──────────────────────────────────────────────────────

    #[test]
    fn hit_fields_are_populated() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&IndexDoc {
            url:       "https://full.com/page".to_owned(),
            timestamp: 20240315120000,
            title:     "Full Page".to_owned(),
            body:      "complete body text".to_owned(),
            mime:      Some("text/html".to_owned()),
            s3_key:    "crawls/full.warc.gz".to_owned(),
            offset:    2048,
            length:    4096,
            collection: "warc".to_owned(),
        })
        .unwrap();
        idx.commit().unwrap();

        let hits = idx.search("complete", 1, None, None).unwrap();
        assert_eq!(hits.len(), 1);
        let h = &hits[0];
        assert_eq!(h.url, "https://full.com/page");
        assert_eq!(h.timestamp, "20240315120000");
        assert_eq!(h.title, "Full Page");
        assert_eq!(h.mime.as_deref(), Some("text/html"));
        assert_eq!(h.s3_key, "crawls/full.warc.gz");
        assert_eq!(h.offset, 2048);
        assert_eq!(h.length, 4096);
        assert!(h.score > 0.0);
    }

    #[test]
    fn hit_timestamp_is_zero_padded() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc("https://a.com/", "A", "test keyword", 20240101090501))
            .unwrap();
        idx.commit().unwrap();

        let hits = idx.search("keyword", 1, None, None).unwrap();
        assert_eq!(hits[0].timestamp, "20240101090501");
    }

    // ── Schema compatibility ──────────────────────────────────────────────────

    /// The schema as it was before `collection` was appended.  Field options are
    /// copied from `build_schema` verbatim — an older index must be recognised
    /// as one field behind, not as one with changed options.
    fn legacy_schema() -> Schema {
        let mut b = Schema::builder();
        b.add_text_field("url",       STRING | STORED);
        b.add_u64_field( "timestamp", NumericOptions::default().set_stored());
        b.add_text_field("title",     TEXT | STORED);
        b.add_text_field("body",      TEXT);
        b.add_text_field("mime",      STRING | STORED);
        b.add_text_field("s3_key",    STRING | STORED);
        b.add_u64_field( "offset",    NumericOptions::default().set_stored());
        b.add_u64_field( "length",    NumericOptions::default().set_stored());
        b.build()
    }

    /// Write an index in the pre-`collection` schema, the way a build from
    /// before collections would have left it on disk.
    fn write_legacy_index(path: &Path, urls: &[&str]) {
        let schema = legacy_schema();
        let dir = MmapDirectory::open(path).unwrap();
        let index = Index::create(dir, schema.clone(), tantivy::IndexSettings::default()).unwrap();
        let mut w: IndexWriter = index.writer(DEFAULT_HEAP_BYTES).unwrap();
        let f = |n: &str| schema.get_field(n).unwrap();
        for url in urls {
            let mut d = TantivyDocument::default();
            d.add_text(f("url"), url);
            d.add_u64( f("timestamp"), 20240101120000);
            d.add_text(f("title"), "Legacy");
            d.add_text(f("body"), "legacy body text");
            d.add_text(f("mime"), "text/html");
            d.add_text(f("s3_key"), "old.warc.gz");
            d.add_u64( f("offset"), 0);
            d.add_u64( f("length"), 1);
            w.add_document(d).unwrap();
        }
        w.commit().unwrap();
    }

    #[test]
    fn schema_compat_identical_for_same_schema() {
        let (schema, _) = build_schema();
        assert_eq!(schema_compat(&schema, &schema).unwrap(), SchemaCompat::Identical);
    }

    #[test]
    fn schema_compat_reports_missing_appended_field() {
        let (current, _) = build_schema();
        assert_eq!(
            schema_compat(&legacy_schema(), &current).unwrap(),
            SchemaCompat::Behind(vec!["collection".to_owned()]),
        );
    }

    #[test]
    fn schema_compat_refuses_removed_field() {
        let (current, _) = build_schema();
        // An index with a field this build no longer writes: not just behind.
        let diff = schema_compat(&current, &legacy_schema()).unwrap();
        assert!(matches!(diff, SchemaCompat::Incompatible(r) if r.contains("removed")));
    }

    #[test]
    fn schema_compat_refuses_changed_options() {
        let (current, _) = build_schema();
        let mut b = Schema::builder();
        // `url` indexed differently — same names, different meaning.
        b.add_text_field("url", TEXT | STORED);
        b.add_u64_field( "timestamp", NumericOptions::default().set_stored());
        b.add_text_field("title",  TEXT | STORED);
        b.add_text_field("body",   TEXT);
        b.add_text_field("mime",   STRING | STORED);
        b.add_text_field("s3_key", STRING | STORED);
        b.add_u64_field( "offset", NumericOptions::default().set_stored());
        b.add_u64_field( "length", NumericOptions::default().set_stored());
        let diff = schema_compat(&b.build(), &current).unwrap();
        assert!(matches!(diff, SchemaCompat::Incompatible(r) if r.contains("field 0 differs")));
    }

    #[test]
    fn opening_a_pre_collection_index_works_instead_of_failing() {
        let dir = tempdir().unwrap();
        write_legacy_index(dir.path(), &["https://old.com/1", "https://old.com/2"]);

        // This is the call that used to abort with
        // "An index exists but the schema does not match".
        let idx = SearchIndex::open_or_create(dir.path()).unwrap();
        assert_eq!(idx.num_docs(), 2, "existing documents are left alone");

        let hits = idx.search("legacy", 10, None, None).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|h| h.collection.is_none()));
    }

    #[test]
    fn writing_to_a_pre_collection_index_drops_only_the_collection() {
        let dir = tempdir().unwrap();
        write_legacy_index(dir.path(), &["https://old.com/1"]);

        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        let mut pdf = doc("https://obst.example/1.pdf", "Pomologie", "Bigarreau Kernobst", 20260101000000);
        pdf.collection = "obst-pdfs".to_owned();
        idx.add_document(&pdf).unwrap();
        idx.commit().unwrap();
        assert_eq!(idx.num_docs(), 2);

        // The document is indexed and findable — only its collection is lost,
        // which is what the warning on open says.
        let hits = idx.search("Bigarreau", 10, None, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].url, "https://obst.example/1.pdf");
        assert!(hits[0].collection.is_none());

        drop(idx);
        assert_eq!(SearchReader::open(dir.path()).unwrap().num_docs(), 2);
    }

    #[test]
    fn merging_segments_of_a_pre_collection_index_keeps_every_document() {
        // Writing into an old index must not leave a merge landmine: the schema
        // on disk and the segments agree, so a merge of old and new segments is
        // an ordinary merge.
        let dir = tempdir().unwrap();
        write_legacy_index(dir.path(), &["https://old.com/1", "https://old.com/2"]);

        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc("https://obst.example/1.pdf", "Pomologie", "legacy body text", 20260101000000))
            .unwrap();
        idx.commit().unwrap();

        let segments = idx.index.searchable_segment_ids().unwrap();
        assert!(segments.len() >= 2, "want an old and a new segment");
        idx.writer.merge(&segments).wait().unwrap();
        idx.commit().unwrap();

        assert_eq!(idx.index.searchable_segment_ids().unwrap().len(), 1);
        assert_eq!(idx.num_docs(), 3);
        assert_eq!(idx.search("legacy", 10, None, None).unwrap().len(), 3);
    }

    // ── Per-file replace (delete_s3_key) ────────────────────────────────────────

    #[test]
    fn delete_s3_key_removes_only_that_files_docs() {
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc_in("https://a.com/1", "fileA.warc.gz")).unwrap();
        idx.add_document(&doc_in("https://a.com/2", "fileA.warc.gz")).unwrap();
        idx.add_document(&doc_in("https://b.com/1", "fileB.warc.gz")).unwrap();
        idx.commit().unwrap();
        assert_eq!(idx.num_docs(), 3);

        // Deleting fileA drops both of its docs, leaves fileB untouched.
        idx.delete_s3_key("fileA.warc.gz").unwrap();
        idx.commit().unwrap();
        assert_eq!(idx.num_docs(), 1);
        let hits = idx.search("indexable", 10, None, None).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].s3_key, "fileB.warc.gz");
    }

    #[test]
    fn reindex_same_file_replaces_not_duplicates() {
        // The real re-index invariant: delete the file's docs, then re-add in
        // the SAME commit. delete_term only affects already-committed docs
        // (lower opstamp), so the re-adds survive and there is no duplication.
        let dir = tempdir().unwrap();
        let mut idx = SearchIndex::open_or_create(dir.path()).unwrap();
        idx.add_document(&doc_in("https://a.com/1", "fileA.warc.gz")).unwrap();
        idx.add_document(&doc_in("https://a.com/2", "fileA.warc.gz")).unwrap();
        idx.commit().unwrap();
        assert_eq!(idx.num_docs(), 2);

        // Re-index the same file: delete-then-add within one commit.
        idx.delete_s3_key("fileA.warc.gz").unwrap();
        idx.add_document(&doc_in("https://a.com/1", "fileA.warc.gz")).unwrap();
        idx.add_document(&doc_in("https://a.com/2", "fileA.warc.gz")).unwrap();
        idx.add_document(&doc_in("https://a.com/3", "fileA.warc.gz")).unwrap();
        idx.commit().unwrap();

        // 3 docs, not 5 — the old two were replaced, the new record added.
        assert_eq!(idx.num_docs(), 3);
    }
}
