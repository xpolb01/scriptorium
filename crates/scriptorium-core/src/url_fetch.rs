//! Fetch a URL, extract main article content via Mozilla Readability,
//! convert to markdown, and prepend a provenance block.
//!
//! This is the network-and-html half of the URL-ingest pipeline. The
//! ingest itself happens via the existing [`crate::ingest`] code path:
//! [`fetch_to_tempfile`] produces a [`ResolvedSource`] holding a tempfile
//! path that downstream code feeds to `ingest_with_options` as if it were
//! a regular local file. Splitting it this way means the LLM and `VaultTx`
//! code stay completely unaware of URLs as a concept.
//!
//! The module lives in `scriptorium-core` (rather than the CLI crate) so
//! both the CLI binary and the MCP server can share the same fetch +
//! extraction + tempfile pipeline. Errors return [`anyhow::Result`] because
//! `url_fetch` failures are non-domain (HTTP, HTML, IO) — callers wrap at
//! their own boundary (`miette::Result` for the CLI, `ToolError` for MCP).
//!
//! Limitations (documented, not silently swallowed):
//!
//! - **JS-rendered SPAs** return only the skeleton HTML the server sends.
//!   Readability typically extracts very little; this function returns an
//!   error rather than producing a near-empty page.
//! - **Paywalled content** typically returns the subscribe-prompt body.
//!   Readability will extract that, and the resulting wiki page will
//!   reflect the limitation; we make no attempt to detect or evade paywalls.
//! - **Non-HTML responses** (PDF, JSON, plaintext) are rejected with an
//!   error pointing the user at downloading the file directly.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use dom_smoothie::{Article, Config, Readability};

/// Hard timeout for the HTTP fetch. Generous enough for slow blogs, short
/// enough that a hung connection doesn't lock the caller forever.
const DEFAULT_TIMEOUT_SECS: u64 = 30;
/// Soft cap on the rendered markdown length. Beyond this, we truncate and
/// append a marker so the LLM prompt budget stays predictable.
const MAX_CONTENT_BYTES: usize = 200_000;
/// Polite identification on outbound requests.
const USER_AGENT: &str = concat!("scriptorium/", env!("CARGO_PKG_VERSION"));

/// One fetched + converted document, ready to be written to a file and fed
/// to the regular file-ingest pipeline.
#[derive(Debug, Clone)]
pub struct FetchedDoc {
    /// The URL we were asked to fetch.
    pub url: String,
    /// Title extracted by Readability, or the URL itself if Readability
    /// produced no title.
    pub title: String,
    /// Final markdown body, including the `> Source:` provenance block.
    pub markdown: String,
    /// When the fetch completed, used in the provenance block and surfaced
    /// to callers for logging.
    pub fetched_at: DateTime<Utc>,
}

/// A resolved URL ingest: a path on disk plus the temp directory keeping
/// it alive plus the [`FetchedDoc`] metadata. Drop the struct after the
/// ingest call to clean up the tempdir.
///
/// `path` is private and accessed via [`Self::path`]; the `_tmpdir` field
/// must outlive any code reading from `path`, which the borrow checker
/// guarantees by holding it inside this struct.
pub struct ResolvedSource {
    /// Metadata about the fetched document. Public so callers can log the
    /// title, URL, byte count, and fetch timestamp without needing a second
    /// API call.
    pub doc: FetchedDoc,
    path: PathBuf,
    _tmpdir: tempfile::TempDir,
}

impl ResolvedSource {
    /// The temp file path containing the converted markdown. Valid as long
    /// as `self` is alive.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Fetch the URL, run Readability, convert HTML → markdown, prepend the
/// provenance block. Network call + parse + conversion in one async call.
///
/// Use [`convert`] directly when you already have HTML and want to test
/// the conversion path without hitting the network.
pub async fn fetch_and_convert(url: &str) -> Result<FetchedDoc> {
    let html = fetch(url).await?;
    convert(&html, url, Utc::now())
}

/// High-level entry point: fetch the URL, convert it, write the resulting
/// markdown to a tempfile inside a fresh tempdir. The returned
/// [`ResolvedSource`] holds the tempdir alive — drop it after the ingest
/// call to clean up.
///
/// This is the function both the CLI and MCP frontends call. They each
/// then pass `resolved.path()` to `ingest::ingest_with_options` and let
/// the regular file-ingest pipeline take over.
pub async fn fetch_to_tempfile(url: &str) -> Result<ResolvedSource> {
    let doc = fetch_and_convert(url).await?;
    let dir = tempfile::tempdir().context("create tempdir for URL ingest")?;
    let slug = slug_from_title(&doc.title);
    let path = dir.path().join(format!("{slug}.md"));
    std::fs::write(&path, &doc.markdown)
        .with_context(|| format!("write tempfile {}", path.display()))?;
    Ok(ResolvedSource {
        doc,
        path,
        _tmpdir: dir,
    })
}

/// HTTP GET the URL with our standard timeout, redirect, and user-agent
/// settings. Errors clearly on non-success status codes and on non-HTML
/// content types.
async fn fetch(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .user_agent(USER_AGENT)
        .redirect(reqwest::redirect::Policy::limited(10))
        .build()
        .context("build http client")?;

    let resp = client
        .get(url)
        .header("Accept", "text/html, application/xhtml+xml, text/*;q=0.9")
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;

    if !resp.status().is_success() {
        return Err(anyhow!("{} returned HTTP {}", url, resp.status()));
    }

    let content_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    if !content_type.is_empty() && !content_type.contains("html") {
        return Err(anyhow!(
            "expected text/html from {url}, got {content_type:?}; download the file manually first and ingest with `scriptorium ingest <path>`"
        ));
    }

    let body = resp.text().await.context("read response body")?;
    if body.is_empty() {
        return Err(anyhow!("empty response body from {url}"));
    }
    Ok(body)
}

/// Convert an HTML string + source URL into a [`FetchedDoc`]. Pure (no I/O)
/// so unit tests can exercise the conversion path against fixture HTML
/// without needing a live server.
pub fn convert(html: &str, url: &str, fetched_at: DateTime<Utc>) -> Result<FetchedDoc> {
    let cfg = Config::default();
    let mut readability = Readability::new(html, Some(url), Some(cfg))
        .map_err(|e| anyhow!("readability init: {e}"))?;
    let article: Article = readability
        .parse()
        .map_err(|e| anyhow!("readability extract: {e}"))?;

    if article.content.trim().is_empty() {
        return Err(anyhow!(
            "readability extracted no content from {url}; the page may require JavaScript or be empty"
        ));
    }

    // HTML → markdown via fast_html2md (imported as `html2md` per the
    // crate's lib name). `rewrite_html` lives under the default `rewriter`
    // feature; `parse_html` is behind the opt-in `scraper` feature which we
    // don't enable. Both functions have the same signature. The boolean is
    // the crate's commonmark-strictness toggle; `false` keeps the looser
    // default which produces cleaner output for blog HTML.
    let mut markdown = html2md::rewrite_html(&article.content, false);

    // Soft cap so prompt budgets stay predictable on absurdly large pages.
    if markdown.len() > MAX_CONTENT_BYTES {
        let mut cap = MAX_CONTENT_BYTES;
        // Snap back to a UTF-8 boundary so we don't slice mid-codepoint.
        while cap > 0 && !markdown.is_char_boundary(cap) {
            cap -= 1;
        }
        markdown.truncate(cap);
        markdown.push_str("\n\n…[truncated by scriptorium url-ingest]\n");
    }

    let title = if article.title.trim().is_empty() {
        url.to_string()
    } else {
        article.title.trim().to_string()
    };

    let final_markdown = format!(
        "> Source: {url}\n> Fetched: {}\n\n# {title}\n\n{markdown}\n",
        fetched_at.to_rfc3339()
    );

    Ok(FetchedDoc {
        url: url.to_string(),
        title,
        markdown: final_markdown,
        fetched_at,
    })
}

/// Lowercase, alphanumeric-only slug safe for filesystem stems. The temp
/// filename written by [`fetch_to_tempfile`] feeds
/// [`crate::ingest::ingest_with_options`], which derives the interned
/// filename from the temp file stem. A clean slug here means a clean
/// filename in `sources/articles/`.
#[must_use]
pub fn slug_from_title(title: &str) -> String {
    let mut out = String::new();
    let mut prev_dash = true;
    for ch in title.chars() {
        if ch.is_alphanumeric() {
            for lc in ch.to_lowercase() {
                out.push(lc);
            }
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let trimmed = out.trim_matches('-').to_string();
    if trimmed.is_empty() {
        return "untitled".to_string();
    }
    if trimmed.chars().count() > 64 {
        return trimmed
            .chars()
            .take(64)
            .collect::<String>()
            .trim_end_matches('-')
            .to_string();
    }
    trimmed
}

#[cfg(test)]
mod tests {
    use std::fmt::Write as _;

    use super::*;
    use chrono::TimeZone;

    fn fake_time() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 7, 12, 0, 0).unwrap()
    }

    #[test]
    fn convert_extracts_title_and_body_from_minimal_article() {
        let html = r"<!DOCTYPE html><html><head><title>Hello World</title></head>
            <body><article><h1>Hello World</h1>
            <p>This is the first paragraph of an article. It has more than just a sentence so readability will keep it as meaningful content.</p>
            <p>And this is the second paragraph, also long enough to survive the readability cleaner with at least a few clauses worth of words.</p>
            </article></body></html>";
        let doc = convert(html, "https://example.com/hello", fake_time()).unwrap();
        assert_eq!(doc.url, "https://example.com/hello");
        assert!(doc.title.to_lowercase().contains("hello"));
        assert!(doc.markdown.contains("> Source: https://example.com/hello"));
        assert!(doc
            .markdown
            .contains("> Fetched: 2026-04-07T12:00:00+00:00"));
        assert!(doc.markdown.contains("# Hello World"));
        assert!(doc.markdown.contains("first paragraph"));
        assert!(doc.markdown.contains("second paragraph"));
    }

    #[test]
    fn convert_uses_url_as_title_when_readability_returns_none() {
        let html = r"<html><body><article>
            <p>This article has no title element at all but contains enough body text to survive the readability extractor and still produce content for us.</p>
            <p>A second paragraph also gives the extractor more signal to work with so it does not give up entirely on the page.</p>
            </article></body></html>";
        let doc = convert(html, "https://example.com/untitled", fake_time()).unwrap();
        assert!(!doc.title.is_empty());
        assert!(doc
            .markdown
            .contains("> Source: https://example.com/untitled"));
    }

    #[test]
    fn slug_from_title_lowercases_and_dashes() {
        assert_eq!(slug_from_title("Hello World"), "hello-world");
        assert_eq!(slug_from_title("  __foo__BAR__  "), "foo-bar");
        assert_eq!(
            slug_from_title("Why JSON.parse loses BigInt precision"),
            "why-json-parse-loses-bigint-precision"
        );
    }

    #[test]
    fn slug_from_title_returns_untitled_for_empty_or_punctuation_only() {
        assert_eq!(slug_from_title(""), "untitled");
        assert_eq!(slug_from_title("!!!"), "untitled");
        assert_eq!(slug_from_title("   "), "untitled");
    }

    #[test]
    fn slug_from_title_caps_at_64_code_points() {
        let long = "a".repeat(200);
        assert_eq!(slug_from_title(&long).chars().count(), 64);
    }

    #[test]
    fn slug_from_title_preserves_unicode_alphanumerics() {
        let s = slug_from_title("Щоденник емоцій");
        assert!(s.contains('щ'));
        assert!(s.contains('-'));
    }

    #[test]
    fn convert_truncates_huge_markdown_at_byte_cap() {
        let mut body = String::from("<article>");
        for i in 0..5000 {
            // write! against String is infallible.
            let _ = write!(
                body,
                "<p>Paragraph number {i} contains substantial text that survives the readability extractor with at least a couple of clauses worth of words for the cap test.</p>"
            );
        }
        body.push_str("</article>");
        let html = format!("<html><head><title>Long</title></head><body>{body}</body></html>");
        let doc = convert(&html, "https://example.com/long", fake_time()).unwrap();
        assert!(
            doc.markdown
                .contains("[truncated by scriptorium url-ingest]"),
            "expected truncation marker in markdown of length {}",
            doc.markdown.len()
        );
    }
}
