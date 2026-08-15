//! PDF text extraction via an Apache Tika server.
//!
//! The archive holds thousands of PDFs that the HTML/text indexer cannot read.
//! When [`IndexerConfig::tika`][cfg] is configured, [`PdfExtractor`] posts each
//! PDF to a `tika-server` and gets back its text plus title. Tika (PDFBox)
//! reads the text layer of born-digital PDFs directly; for pure scans it hands
//! the pages to Tesseract, governed by the OCR strategy in the config.
//!
//! [cfg]: warc_search_config::IndexerConfig::tika
//!
//! # Why the request is synchronous
//!
//! Extraction runs inside the indexer's `spawn_blocking` parse loop, which
//! processes one WARC record at a time. A blocking HTTP call there keeps peak
//! memory to a single PDF and needs no async plumbing. Tika runs on the same
//! host, so the round trip is local.
//!
//! # OCR quality gate
//!
//! OCR with the wrong language or on a bad scan does not fail — it returns
//! letter-salad, which would pollute the fulltext index far more harmfully than
//! a missing document. [`looks_like_text`] rejects output that does not read
//! like prose, so a garbage OCR result is dropped rather than indexed.
//!
//! # Two entry points
//!
//! [`PdfExtractor::extract`] is what the indexer calls: text or nothing, with
//! the reason logged. [`PdfExtractor::try_extract`] backs the `/text` endpoint,
//! which reports the failure to its caller and hands back gate-rejected text
//! flagged rather than dropped.

use std::fmt;
use std::time::Duration;

use serde_json::Value;
use tracing::{debug, warn};

use warc_search_config::TikaConfig;

/// Extracted text for one PDF.
pub struct PdfDoc {
    pub title: String,
    pub body:  String,
    /// Whether [`looks_like_text`] accepted the body.
    ///
    /// The indexer drops a document that fails the gate; the `/text` endpoint
    /// hands it to the caller anyway, flagged, because a human asking for one
    /// specific document can judge OCR noise for themselves.
    pub quality_ok: bool,
}

/// Why a PDF yielded no text.
#[derive(Debug)]
pub enum ExtractError {
    /// Larger than `tika.max_pdf_bytes`.
    TooLarge { bytes: usize, limit: usize },
    /// No `%%EOF` trailer — the capture was cut short mid-file.
    Truncated,
    /// Tika refused the request, timed out, or returned an unusable body.
    Tika(String),
    /// Tika parsed the file but returned no text at all.
    Empty,
}

impl fmt::Display for ExtractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { bytes, limit } =>
                write!(f, "PDF is {bytes} bytes, over the {limit}-byte limit"),
            Self::Truncated => write!(f, "PDF is truncated (no %%EOF trailer)"),
            Self::Tika(e)   => write!(f, "Tika extraction failed: {e}"),
            Self::Empty     => write!(f, "Tika returned no text"),
        }
    }
}

/// A configured client for a Tika server.
///
/// Cheap to clone (the underlying `ureq::Agent` is an `Arc` internally), and
/// `Send + Sync`, so it can be moved into the blocking parse task.
#[derive(Clone)]
pub struct PdfExtractor {
    agent:         ureq::Agent,
    endpoint:      String,      // {url}/rmeta/text
    ocr_strategy:  String,
    ocr_languages: String,
    max_pdf_bytes: usize,
}

impl PdfExtractor {
    pub fn new(cfg: &TikaConfig) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .build();
        let base = cfg.url.trim_end_matches('/');
        Self {
            agent,
            // /rmeta/text returns text *and* metadata (title, page count) from a
            // single parse — one request, not one for text and one for the title.
            endpoint:      format!("{base}/rmeta/text"),
            ocr_strategy:  cfg.ocr_strategy.clone(),
            ocr_languages: cfg.ocr_languages.clone(),
            max_pdf_bytes: cfg.max_pdf_bytes,
        }
    }

    /// Extract text from PDF bytes for the fulltext index.
    ///
    /// Returns `None` when the PDF is too large, truncated, Tika fails, or the
    /// result does not look like usable text. Use [`try_extract`] when the
    /// caller needs to know *why*.
    ///
    /// [`try_extract`]: PdfExtractor::try_extract
    pub fn extract(&self, url: &str, pdf: &[u8]) -> Option<PdfDoc> {
        match self.try_extract(url, pdf, false) {
            Ok(doc) if doc.quality_ok => Some(doc),
            Ok(doc) => {
                warn!(url, chars = doc.body.len(),
                      "PDF text rejected by quality gate (likely OCR noise)");
                None
            }
            // A size limit is a configuration decision that silently drops a
            // whole document, and the documents it drops are the big scans —
            // often the most valuable ones. Say so at WARN with the number to
            // raise: a quiet skip looks like full coverage in the result.
            Err(e @ ExtractError::TooLarge { .. }) => {
                warn!(url, bytes = pdf.len(), reason = %e,
                      "PDF skipped — raise indexer.tika.max_pdf_bytes to index it");
                None
            }
            // Truncation, by contrast, is a property of the capture itself and
            // affects whole crawls at a time (CommonCrawl caps PDFs at ~1 MiB).
            // At WARN it would drown out everything else.
            Err(e @ ExtractError::Truncated) => {
                debug!(url, bytes = pdf.len(), reason = %e, "PDF skipped");
                None
            }
            Err(e) => {
                warn!(url, err = %e, "PDF extraction failed");
                None
            }
        }
    }

    /// Extract text from PDF bytes, reporting the reason for any failure.
    ///
    /// The returned [`PdfDoc`] carries `quality_ok = false` when the text does
    /// not read like prose — the caller decides whether to use it anyway.
    ///
    /// `allow_truncated` sends a PDF with no `%%EOF` trailer to Tika regardless.
    /// The indexer never does that: OCR of a cut-off capture produces noise the
    /// quality gate drops anyway, and the attempt is what makes indexing crawl.
    /// A single on-demand request can afford it.
    pub fn try_extract(
        &self,
        url: &str,
        pdf: &[u8],
        allow_truncated: bool,
    ) -> Result<PdfDoc, ExtractError> {
        if pdf.len() > self.max_pdf_bytes {
            return Err(ExtractError::TooLarge { bytes: pdf.len(), limit: self.max_pdf_bytes });
        }
        // Truncated PDFs (e.g. CommonCrawl/wayback capped at ~1 MiB) have no
        // `%%EOF` trailer.
        if !allow_truncated && !pdf_has_eof(pdf) {
            return Err(ExtractError::Truncated);
        }

        let resp = self
            .agent
            .put(&self.endpoint)
            .set("Content-Type", "application/pdf")
            .set("Accept", "application/json")
            // Without an explicit content type Tika would sniff the body as
            // form data and extract nothing.
            .set("X-Tika-PDFOcrStrategy", &self.ocr_strategy)
            .set("X-Tika-OCRLanguage", &self.ocr_languages)
            .send_bytes(pdf);

        let body = match resp {
            Ok(r) => r.into_string()
                .map_err(|e| ExtractError::Tika(format!("reading response: {e}")))?,
            Err(e) => return Err(ExtractError::Tika(e.to_string())),
        };

        let (title, text) = parse_rmeta(&body)
            .ok_or_else(|| ExtractError::Tika("unparseable /rmeta/text response".to_owned()))?;

        if text.trim().is_empty() {
            return Err(ExtractError::Empty);
        }

        let quality_ok = looks_like_text(&text);

        let title = if title.trim().is_empty() {
            title_from_url(url)
        } else {
            title.trim().chars().take(256).collect()
        };
        Ok(PdfDoc { title, body: text, quality_ok })
    }
}

/// Pull `(title, text)` out of Tika's `/rmeta/text` JSON.
///
/// The response is an array of documents (the PDF plus any embedded files); the
/// first entry is the PDF itself. Text from embedded entries is appended so an
/// attached document's content is searchable too.
fn parse_rmeta(json: &str) -> Option<(String, String)> {
    let docs: Value = serde_json::from_str(json).ok()?;
    let arr = docs.as_array()?;
    let first = arr.first()?;

    let title = first
        .get("dc:title")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();

    let mut text = String::new();
    for doc in arr {
        if let Some(c) = doc.get("X-TIKA:content").and_then(Value::as_str) {
            text.push_str(c);
            text.push('\n');
        }
    }
    // Collapse the runs of whitespace Tika emits between layout blocks.
    let text = normalize_ws(&text);
    Some((title, text))
}

/// Collapse whitespace: many single spaces and newlines, one blank line max.
fn normalize_ws(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut blank_run = 0;
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() {
            blank_run += 1;
            if blank_run <= 1 && !out.is_empty() {
                out.push('\n');
            }
        } else {
            blank_run = 0;
            // Collapse the intra-line runs of spaces Tika leaves between glyphs.
            let mut first = true;
            for word in line.split_whitespace() {
                if !first { out.push(' '); }
                out.push_str(word);
                first = false;
            }
            out.push('\n');
        }
    }
    out.truncate(out.trim_end().len());
    out
}

/// Last resort title: the PDF's filename from its URL.
fn title_from_url(url: &str) -> String {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let name = path.rsplit('/').find(|s| !s.is_empty()).unwrap_or(url);
    let name = name.strip_suffix(".pdf").or_else(|| name.strip_suffix(".PDF")).unwrap_or(name);
    // Filenames often use _ or %20 as spaces.
    name.replace(['_', '+'], " ").replace("%20", " ").trim().chars().take(256).collect()
}

/// A complete PDF ends with a `%%EOF` trailer within its last stretch of bytes.
/// A truncated capture (cut at the crawler's size cap) lacks it. Checking a
/// generous tail tolerates trailing whitespace, incremental-update xref
/// sections, and a little padding after the marker.
fn pdf_has_eof(pdf: &[u8]) -> bool {
    let tail = &pdf[pdf.len().saturating_sub(4096)..];
    tail.windows(5).any(|w| w == b"%%EOF")
}

/// Does `s` read like prose rather than OCR noise?
///
/// The guard against wrong-language or wrong-script OCR, which returns long
/// strings of stray letters and punctuation. Born-digital text passes this
/// easily; garbage recognition does not. Kept lenient on purpose — the cost of
/// dropping a real document is higher than the cost of an occasional weak one.
pub fn looks_like_text(s: &str) -> bool {
    let mut non_ws = 0usize;
    let mut alnum = 0usize;
    for c in s.chars() {
        if c.is_whitespace() {
            continue;
        }
        non_ws += 1;
        if c.is_alphanumeric() {
            alnum += 1;
        }
    }
    // Too little content to be worth an index entry.
    if non_ws < 40 {
        return false;
    }

    let mut tokens = 0usize;
    let mut word_tokens = 0usize;
    for token in s.split_whitespace() {
        tokens += 1;
        let alpha = token.chars().filter(|c| c.is_alphabetic()).count();
        // A "word" is a token that is mostly letters and at least two long —
        // "Berlepsch" yes, "001" or "%$#" no.
        if alpha >= 2 && alpha * 2 >= token.chars().count() {
            word_tokens += 1;
        }
    }

    let alnum_ratio = alnum as f64 / non_ws as f64;
    let word_ratio  = if tokens > 0 { word_tokens as f64 / tokens as f64 } else { 0.0 };
    alnum_ratio >= 0.55 && word_ratio >= 0.4
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ordinary_prose() {
        let de = "Der Rote Berlepsch ist eine alte Apfelsorte aus dem Rheinland, \
                  die um 1880 gezüchtet wurde und bis heute im Streuobstbau geschätzt wird.";
        assert!(looks_like_text(de));
    }

    #[test]
    fn accepts_a_list_with_numbers() {
        let list = "001 Alberge Aprikose aus Tours 002 Alberge de Montgame \
                    026 Nepaul Abricot de 027 Noir Schwarze Aprikose";
        assert!(looks_like_text(list));
    }

    #[test]
    fn rejects_ocr_letter_salad() {
        let noise = "l1 ﬁ 3 rn |\\| . ,, '' ° ~ \\ / ] [ ; : rn1 l|l ¢ ° ﬂ 4 5 ‚‚ „ "
            .repeat(6);
        assert!(!looks_like_text(&noise));
    }

    #[test]
    fn rejects_near_empty() {
        assert!(!looks_like_text("   \n  x  \n "));
    }

    #[test]
    fn title_from_url_uses_filename() {
        assert_eq!(
            title_from_url("https://bund-lemgo.de/download/Vanicek_-_Obstbau_im_Garten.pdf"),
            "Vanicek - Obstbau im Garten",
        );
        assert_eq!(title_from_url("https://x.de/a/b/"), "b");
    }

    #[test]
    fn parse_rmeta_takes_title_and_concatenates_content() {
        let json = r#"[
            {"dc:title":"Sortenliste","X-TIKA:content":"Roter  Berlepsch\n\n\nBoskoop"},
            {"X-TIKA:content":"angehängtes Dokument"}
        ]"#;
        let (title, text) = parse_rmeta(json).unwrap();
        assert_eq!(title, "Sortenliste");
        assert!(text.contains("Roter Berlepsch"));
        assert!(text.contains("angehängtes Dokument"));
        assert!(!text.contains("\n\n\n"), "whitespace not collapsed: {text:?}");
    }

    #[test]
    fn detects_pdf_eof_trailer() {
        let mut good = b"%PDF-1.4\n...body...\n".to_vec();
        good.extend_from_slice(b"xref\n0 1\ntrailer\n<<>>\nstartxref\n9\n%%EOF\n");
        assert!(pdf_has_eof(&good));
        // trailing whitespace/padding after the marker still counts
        good.extend_from_slice(b"   \r\n");
        assert!(pdf_has_eof(&good));
    }

    #[test]
    fn detects_truncated_pdf() {
        // 1 MiB of body with no %%EOF (a truncated capture)
        let trunc = vec![b'x'; 1_048_576];
        assert!(!pdf_has_eof(&trunc));
    }
}
