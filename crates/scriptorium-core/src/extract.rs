//! Source-format extraction router.
//!
//! Historically ingest accepted UTF-8 text only — binary formats never
//! reached the vault at all. This module routes each source through the
//! best available extractor while the core stays a single pure-Rust
//! binary:
//!
//! | Format | Extractor |
//! |--------|-----------|
//! | md / txt / no-ext | UTF-8 read (unchanged) |
//! | html / htm | Readability + html→md (same engine as URL ingest) |
//! | pdf | `pdftotext` (poppler) if on PATH, else `docling` |
//! | docx / pptx / xlsx / epub | `docling` if on PATH |
//!
//! External converters are **feature-detected subprocesses** — never
//! compiled in, never required. Raw bytes are still interned unchanged
//! into `sources/`; only the text handed to the curator comes from the
//! extractor.

use std::path::Path;
use std::process::Command;

use crate::error::{Error, Result};

/// Extracted text plus which converter produced it (`None` = plain UTF-8).
#[derive(Debug, Clone)]
pub struct Extracted {
    pub text: String,
    pub converter: Option<&'static str>,
}

/// Route `raw` (read from `source_path`) through the right extractor.
pub fn extract_source_text(source_path: &Path, raw: &[u8]) -> Result<Extracted> {
    let ext = source_path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "pdf" => extract_pdf(source_path),
        "docx" | "pptx" | "xlsx" | "epub" => extract_via_docling(source_path, &ext),
        "html" | "htm" => extract_local_html(source_path, raw),
        _ => utf8_text(raw),
    }
}

fn utf8_text(raw: &[u8]) -> Result<Extracted> {
    let text = String::from_utf8(raw.to_vec())
        .map_err(|e| Error::Other(anyhow::anyhow!("source is not UTF-8: {e}")))?;
    Ok(Extracted {
        text,
        converter: None,
    })
}

/// Local HTML files go through the same Readability + html→markdown
/// engine as URL ingest, so markup never lands in the curator prompt.
fn extract_local_html(source_path: &Path, raw: &[u8]) -> Result<Extracted> {
    let html = String::from_utf8(raw.to_vec())
        .map_err(|e| Error::Other(anyhow::anyhow!("html source is not UTF-8: {e}")))?;
    let pseudo_url = format!("file://{}", source_path.display());
    let doc = crate::url_fetch::convert(&html, &pseudo_url, chrono::Utc::now())?;
    Ok(Extracted {
        text: doc.markdown,
        converter: Some("readability"),
    })
}

fn extract_pdf(source_path: &Path) -> Result<Extracted> {
    if tool_on_path("pdftotext") {
        let out = Command::new("pdftotext")
            .arg("-layout")
            .arg(source_path)
            .arg("-") // stdout
            .output()
            .map_err(|e| Error::Other(anyhow::anyhow!("pdftotext spawn: {e}")))?;
        if out.status.success() {
            let text = String::from_utf8_lossy(&out.stdout).into_owned();
            if text.trim().is_empty() {
                return Err(Error::Other(anyhow::anyhow!(
                    "pdftotext produced no text — the PDF may be scanned images; \
                     install `docling` for OCR support"
                )));
            }
            return Ok(Extracted {
                text,
                converter: Some("pdftotext"),
            });
        }
        // pdftotext present but failed: fall through to docling if any.
        if !tool_on_path("docling") {
            return Err(Error::Other(anyhow::anyhow!(
                "pdftotext failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
    }
    extract_via_docling(source_path, "pdf")
}

/// Convert a document with the `docling` CLI (layout-aware tables, OCR,
/// Office formats). Writes markdown into a temp dir and reads it back.
fn extract_via_docling(source_path: &Path, ext: &str) -> Result<Extracted> {
    if !tool_on_path("docling") {
        return Err(Error::Other(anyhow::anyhow!(
            "ingesting .{ext} requires the `docling` CLI on PATH \
             (pip install docling){}",
            if ext == "pdf" {
                " — or `pdftotext` (poppler) for digital PDFs"
            } else {
                ""
            }
        )));
    }
    let tmp = tempfile::tempdir().map_err(|e| Error::Other(anyhow::anyhow!("tempdir: {e}")))?;
    let out = Command::new("docling")
        .arg(source_path)
        .args(["--to", "md", "--output"])
        .arg(tmp.path())
        .output()
        .map_err(|e| Error::Other(anyhow::anyhow!("docling spawn: {e}")))?;
    if !out.status.success() {
        return Err(Error::Other(anyhow::anyhow!(
            "docling failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        )));
    }
    // docling writes <stem>.md into the output dir; find it.
    let md = std::fs::read_dir(tmp.path())
        .map_err(|e| Error::Other(anyhow::anyhow!("read docling output: {e}")))?
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().and_then(|x| x.to_str()) == Some("md"))
        .ok_or_else(|| Error::Other(anyhow::anyhow!("docling produced no .md output")))?;
    let text =
        std::fs::read_to_string(&md).map_err(|e| Error::Other(anyhow::anyhow!("read md: {e}")))?;
    if text.trim().is_empty() {
        return Err(Error::Other(anyhow::anyhow!("docling produced empty text")));
    }
    Ok(Extracted {
        text,
        converter: Some("docling"),
    })
}

fn tool_on_path(name: &str) -> bool {
    // `--help`-probing is slow; `which`-style PATH scan is enough and
    // avoids running the tool at detection time.
    std::env::var_os("PATH")
        .map(|paths| {
            std::env::split_paths(&paths).any(|dir| {
                let candidate = dir.join(name);
                candidate.is_file()
            })
        })
        .unwrap_or(false)
}

// ---------- near-duplicate detection (pre-LLM ingest gate) ----------

/// Word-shingle Jaccard similarity between two texts (8-word shingles,
/// hashed). Deterministic and cheap; used to skip re-curating a source
/// that is a near-copy of one already interned.
pub fn shingle_jaccard(a: &str, b: &str) -> f32 {
    let sa = shingles(a);
    let sb = shingles(b);
    if sa.is_empty() || sb.is_empty() {
        return 0.0;
    }
    let inter = sa.intersection(&sb).count();
    let union = sa.union(&sb).count();
    #[allow(clippy::cast_precision_loss)]
    let sim = inter as f32 / union as f32;
    sim
}

fn shingles(text: &str) -> std::collections::HashSet<u64> {
    use std::hash::{Hash, Hasher};
    const W: usize = 8;
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < W {
        // Short text: hash the whole thing as one shingle.
        let mut h = std::hash::DefaultHasher::new();
        words.hash(&mut h);
        return std::iter::once(h.finish()).collect();
    }
    words
        .windows(W)
        .map(|w| {
            let mut h = std::hash::DefaultHasher::new();
            w.hash(&mut h);
            h.finish()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utf8_passthrough_for_markdown() {
        let e = extract_source_text(Path::new("note.md"), b"# hi\n").unwrap();
        assert_eq!(e.text, "# hi\n");
        assert!(e.converter.is_none());
    }

    #[test]
    fn non_utf8_markdown_still_errors() {
        let err = extract_source_text(Path::new("note.md"), &[0xff, 0xfe, 0x00]).unwrap_err();
        assert!(err.to_string().contains("not UTF-8"), "{err}");
    }

    #[test]
    fn office_without_docling_gives_actionable_error() {
        if tool_on_path("docling") {
            return; // environment has docling; the error path can't fire
        }
        let err = extract_source_text(Path::new("deck.pptx"), b"zip-bytes").unwrap_err();
        assert!(err.to_string().contains("docling"), "{err}");
    }

    #[test]
    fn shingle_jaccard_detects_near_copies() {
        let a = "one two three four five six seven eight nine ten eleven twelve \
                 thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty";
        let b = format!("{a} twentyone"); // tiny addition
        assert!(shingle_jaccard(a, &b) > 0.8);
        let c = "completely different words in this other document about other topics \
                 nothing shared at all between the two texts in any window";
        assert!(shingle_jaccard(a, c) < 0.1);
    }

    #[test]
    fn shingle_jaccard_handles_short_texts() {
        assert!((shingle_jaccard("a b c", "a b c") - 1.0).abs() < f32::EPSILON);
        assert!(shingle_jaccard("", "x") < f32::EPSILON);
    }
}
