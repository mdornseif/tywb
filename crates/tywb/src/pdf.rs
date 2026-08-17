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
            // /rmeta returns metadata *and* content from a single parse — one
            // request, not one for the text and one for the title.
            //
            // The XHTML variant, not /rmeta/text: only this one marks page
            // boundaries (`<div class="page">`). Plain text arrives as one
            // undivided block, and a corpus of 400-page volumes cannot be cut
            // into per-cultivar excerpts without knowing where a page ends.
            endpoint:      format!("{base}/rmeta"),
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

        // Before the gate, so the gate judges the text a reader would search.
        let text = normalise_historic_forms(&text);
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
            text.push_str(&xhtml_to_text(c));
            text.push('\n');
        }
    }
    // Collapse the runs of whitespace Tika emits between layout blocks.
    let text = normalize_ws(&text);
    Some((title, text))
}

/// Flatten Tika's XHTML into text, keeping the one thing only it knows: where
/// each page ends.
///
/// Pages are separated by U+000C FORM FEED, the convention `pdftotext` and
/// `ocrmypdf` already use, so text from this path and text produced by those
/// tools can be treated alike. Everything before the first page marker — Tika's
/// `<head>` full of `<meta>` elements — is dropped rather than indexed.
fn xhtml_to_text(xhtml: &str) -> String {
    const PAGE_MARK: &str = "<div class=\"page\">";

    let body = match xhtml.find(PAGE_MARK) {
        Some(i) => &xhtml[i..],
        // No page markers: not the XHTML we expect (an older Tika, or a format
        // whose parser emits none). Take it as it is rather than lose it.
        None => xhtml,
    };

    let mut out = String::with_capacity(body.len());
    if !body.starts_with(PAGE_MARK) {
        strip_tags_into(body, &mut out);
        return out;
    }
    // `split` on a string that starts with the marker yields an empty first
    // element; the pages follow. Blank pages keep their slot: dropping them
    // would shift every page number after them, and a page number that is off
    // by one points at the wrong cultivar.
    for (i, page) in body.split(PAGE_MARK).skip(1).enumerate() {
        if i > 0 {
            out.push('\u{000c}');
        }
        strip_tags_into(page, &mut out);
    }
    out
}

/// Append `xhtml` to `out` with its tags removed and its entities resolved.
/// Tags that end a block become a newline, so paragraphs do not run together.
fn strip_tags_into(xhtml: &str, out: &mut String) {
    let mut rest = xhtml;
    while let Some(lt) = rest.find('<') {
        push_entities(&rest[..lt], out);
        let Some(gt) = rest[lt..].find('>') else {
            // Unclosed tag: the remainder is not markup, keep it as text.
            push_entities(&rest[lt..], out);
            return;
        };
        let tag = &rest[lt + 1..lt + gt];
        let name = tag.trim_start_matches('/').split([' ', '/']).next().unwrap_or("");
        if matches!(name, "p" | "div" | "br" | "li" | "tr" | "h1" | "h2" | "h3" | "table") {
            out.push('\n');
        }
        rest = &rest[lt + gt + 1..];
    }
    push_entities(rest, out);
}

/// Append text, resolving the XML entities Tika emits.
fn push_entities(s: &str, out: &mut String) {
    let mut rest = s;
    while let Some(amp) = rest.find('&') {
        out.push_str(&rest[..amp]);
        let after = &rest[amp..];
        let Some(semi) = after.find(';').filter(|i| *i <= 10) else {
            out.push('&');
            rest = &rest[amp + 1..];
            continue;
        };
        let ent = &after[1..semi];
        let ch = match ent {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "nbsp" => Some(' '),
            _ => ent
                .strip_prefix('#')
                .and_then(|n| match n.strip_prefix(['x', 'X']) {
                    Some(hex) => u32::from_str_radix(hex, 16).ok(),
                    None => n.parse::<u32>().ok(),
                })
                .and_then(char::from_u32),
        };
        match ch {
            Some(c) => out.push(c),
            // Not an entity we know: leave it exactly as written.
            None => out.push_str(&after[..=semi]),
        }
        rest = &after[semi + 1..];
    }
    out.push_str(rest);
}

/// Collapse whitespace: many single spaces and newlines, one blank line max.
fn normalize_ws(s: &str) -> String {
    // Page by page: `trim` and `split_whitespace` both treat U+000C as ordinary
    // whitespace and would quietly eat every page marker — the one piece of
    // structure the XHTML parse exists to preserve.
    let mut out = String::with_capacity(s.len());
    let mut first = true;
    for page in s.split('\u{000c}') {
        if !first {
            out.push('\u{000c}');
        }
        first = false;
        out.push_str(&normalize_ws_page(page));
    }
    out
}

fn normalize_ws_page(s: &str) -> String {
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

/// Fold the typography of historic prints into the letters people type.
///
/// OCR of Fraktur returns what is printed, and what is printed is not what
/// anyone will search for: the long s (`ſ`) is a distinct code point from `s`,
/// so an index built from that text answers `Obſt` and not `Obst`. Measured on
/// 22 OCR'd library volumes: `Obst` found none of them, `Obſt` found 21. The
/// same goes for the `ﬁ`/`ﬂ`/`ﬀ` ligatures that both Fraktur and older
/// born-digital PDFs carry.
///
/// This is deliberately a short, explicit table rather than Unicode NFKC.
/// NFKC would also fold `½` into `1⁄2`, superscripts into digits and full-width
/// forms into ASCII — defensible for search, but a much larger change to reason
/// about, and none of it is the problem in front of us.
///
/// The scan is the hot path — it runs over every extracted document — so text
/// without any of these characters is returned untouched, with no allocation.
pub fn normalise_historic_forms(s: &str) -> String {
    fn replacement(c: char) -> Option<&'static str> {
        Some(match c {
            'ſ' => "s",   // U+017F LATIN SMALL LETTER LONG S
            'ﬀ' => "ff",
            'ﬁ' => "fi",
            'ﬂ' => "fl",
            'ﬃ' => "ffi",
            'ﬄ' => "ffl",
            'ﬅ' => "st",  // long s + t
            'ﬆ' => "st",
            _ => return None,
        })
    }

    // ASCII-only text — nearly everything — leaves here without allocating.
    if s.is_ascii() || !s.chars().any(|c| replacement(c).is_some()) {
        return s.to_owned();
    }
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match replacement(c) {
            Some(r) => out.push_str(r),
            None => out.push(c),
        }
    }
    out
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
    fn the_long_s_becomes_an_s() {
        // The finding that made 22 OCR'd volumes unfindable: an index built
        // from Fraktur answers "Obſt" and not "Obst".
        assert_eq!(normalise_historic_forms("Obſt und Kirſchen"), "Obst und Kirschen");
        assert_eq!(normalise_historic_forms("Waſſer"), "Wasser");
    }

    #[test]
    fn ligatures_are_spelled_out() {
        assert_eq!(normalise_historic_forms("ﬁnden"), "finden");
        assert_eq!(normalise_historic_forms("Pﬂaume"), "Pflaume");
        assert_eq!(normalise_historic_forms("Schiﬀ"), "Schiff");
    }

    #[test]
    fn ordinary_text_is_returned_unchanged() {
        for s in ["Obst und Kirschen", "", "plain ascii 123", "Äpfel, Birnen — Größe 5"] {
            assert_eq!(normalise_historic_forms(s), s);
        }
    }

    #[test]
    fn nothing_else_is_touched() {
        // Explicitly not NFKC: fractions, superscripts and the rest keep their
        // code points, because none of them is the problem this solves.
        for s in ["½ Pfund", "m²", "Ｆｕｌｌｗｉｄｔｈ", "Straße"] {
            assert_eq!(normalise_historic_forms(s), s);
        }
    }

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

    // ── XHTML → text, with page boundaries ────────────────────────────────

    #[test]
    fn pages_are_separated_by_a_form_feed() {
        // The whole reason for using Tika's XHTML: /rmeta/text hands back one
        // undivided block, and a 400-page volume cannot be cut into per-cultivar
        // excerpts without knowing where a page ends.
        let xhtml = concat!(
            "<html><head><meta name=\"pdf:PDFVersion\" content=\"1.6\"/></head><body>",
            "<div class=\"page\"><p>Seite eins</p></div>",
            "<div class=\"page\"><p>Seite zwei</p></div>",
            "</body></html>",
        );
        let text = xhtml_to_text(xhtml);
        let pages: Vec<&str> = text.split('\u{000c}').map(str::trim).collect();
        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0], "Seite eins");
        assert_eq!(pages[1], "Seite zwei");
    }

    #[test]
    fn the_metadata_head_is_not_indexed() {
        // Tika's <head> is a wall of <meta> elements. It is markup about the
        // document, not text of it.
        let xhtml = "<html><head><meta name=\"xmp:CreatorTool\" content=\"Acrobat PDFMaker\"/>\
                     </head><body><div class=\"page\"><p>Obstbau</p></div></body></html>";
        let text = xhtml_to_text(xhtml);
        assert!(!text.contains("PDFMaker"), "got: {text:?}");
        assert!(text.contains("Obstbau"));
    }

    #[test]
    fn entities_come_back_as_characters() {
        let xhtml = "<div class=\"page\"><p>Gr&#246;&#223;e &amp; G&uuml;te &lt;1 kg&gt;</p></div>";
        // &uuml; is HTML, not XML — Tika emits numeric references, and anything
        // unknown must survive rather than be swallowed.
        let text = xhtml_to_text(xhtml);
        assert!(text.contains("Größe & G"), "got: {text:?}");
        assert!(text.contains("<1 kg>"));
        assert!(text.contains("&uuml;"), "an unknown entity is left as written");
    }

    #[test]
    fn a_blank_page_keeps_its_slot() {
        // Page numbers are the whole point. Dropping an empty scan would shift
        // every page after it, and an off-by-one page number points at the
        // wrong cultivar.
        let xhtml = concat!(
            "<div class=\"page\"><p>eins</p></div>",
            "<div class=\"page\"></div>",
            "<div class=\"page\"><p>drei</p></div>",
        );
        let pages: Vec<String> = normalize_ws(&xhtml_to_text(xhtml))
            .split('\u{000c}')
            .map(|p| p.trim().to_owned())
            .collect();
        assert_eq!(pages, vec!["eins", "", "drei"]);
    }

    #[test]
    fn whitespace_normalisation_keeps_the_page_markers() {
        // Runs of blank lines collapse to one, as documented — the marker
        // between the pages is what must not be swallowed.
        let text = normalize_ws("Apfel   und\n\n\n  Birne\u{000c}\n Kirsche  \n");
        assert_eq!(text, "Apfel und\n\nBirne\u{000c}Kirsche");
    }

    #[test]
    fn xhtml_without_page_markers_is_kept_whole() {
        // An older Tika, or a parser that emits no pages: take the text rather
        // than lose the document.
        let text = xhtml_to_text("<html><body><p>Kein Seitenmarker</p></body></html>");
        assert!(text.contains("Kein Seitenmarker"));
        assert!(!text.contains('\u{000c}'));
    }

    #[test]
    fn block_tags_keep_paragraphs_apart() {
        let text = xhtml_to_text("<div class=\"page\"><p>Apfel</p><p>Birne</p></div>");
        assert!(text.contains("Apfel\nBirne") || text.contains("Apfel\n\nBirne"), "got: {text:?}");
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
