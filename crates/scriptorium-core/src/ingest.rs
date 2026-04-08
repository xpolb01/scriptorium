//! Ingest pipeline: source file → prompt → LLM → `VaultTx` → git commit.
//!
//! The pipeline is deliberately linear and boring. Every step is testable in
//! isolation, and the whole flow is exercised end-to-end by the integration
//! tests under `tests/e2e.rs` using a [`MockProvider`](crate::llm::MockProvider).
//!
//! ```text
//! source.md  ──►  read + hash + intern into sources/
//!                 │
//!                 ▼
//!     Schema::render()  ──►  ingest_prompt()  ──►  LLM.complete()
//!                                                      │
//!                                                      ▼ IngestPlan (JSON)
//!                              ┌───────────────────────┘
//!                              ▼
//!      for each page action:  vault.begin() → write_page()
//!                              │
//!                              ▼
//!                        log.md append
//!                              │
//!                              ▼
//!                       tx.commit()  ──►  git commit id
//! ```
//!
//! Retrieval of "relevant existing pages" uses a title-match heuristic by
//! default: pages whose title or filename stem appear as case-insensitive
//! substrings of the source text are passed into the prompt so the LLM can
//! prefer updating an existing page over creating a near-duplicate. The
//! match is capped at [`MAX_RELEVANT_PAGES`] to keep the prompt under
//! budget. Embeddings-based retrieval is the natural upgrade once query
//! infrastructure is proven to scale.

use std::fs;
use std::path::Path;

use camino::{Utf8Path, Utf8PathBuf};
use chrono::Utc;
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};
use crate::llm::{
    ingest_prompt, record_usage, IngestAction, IngestPlan, LlmProvider, PromptContext,
};
use crate::schema::Schema;
use crate::vault::{Frontmatter, Page, Vault};

/// Maximum number of existing pages to inject into the ingest prompt as
/// retrieval context. Higher = better LLM awareness of the existing vault,
/// but also more tokens in every call. 10 is a conservative default that
/// keeps even a small provider context window comfortable.
pub const MAX_RELEVANT_PAGES: usize = 10;

/// Summary returned from a successful [`ingest`] call.
#[derive(Debug, Clone)]
pub struct IngestReport {
    /// Vault-relative path where the source was interned.
    pub source: Utf8PathBuf,
    /// The git commit id (40-char hex) created for this ingest, or the
    /// string `"dry-run"` if this was a dry-run that did not commit.
    pub commit_id: String,
    /// Number of new wiki pages created (or that would have been created).
    pub created: usize,
    /// Number of existing wiki pages updated (or that would have been).
    pub updated: usize,
    /// One-line `summary` field returned by the LLM.
    pub summary: String,
    /// Preview of the change set if this was a dry run. Empty on a real
    /// commit.
    pub dry_run_diff: Vec<crate::vault::ChangeSummary>,
}

/// Run the full ingest pipeline for a single source file.
///
/// The source is interned into `sources/<category>/<hash_prefix>-<slug>.<ext>`
/// (copied if it is not already inside the vault). The LLM's `IngestPlan` is
/// translated into a [`VaultTx`](crate::vault::VaultTx) containing one
/// `write_page` per page action plus a single append to `log.md`. The whole
/// batch is committed atomically.
///
/// Calls [`ingest_with_options`] with defaults; prefer that function if you
/// need dry-run mode.
pub async fn ingest(
    vault: &Vault,
    provider: &dyn LlmProvider,
    source_path: &Path,
) -> Result<IngestReport> {
    ingest_with_options(vault, provider, source_path, IngestOptions::default()).await
}

/// Options for [`ingest_with_options`]. Add fields here instead of growing
/// the argument list; `Default::default()` gives a sensible baseline.
#[derive(Debug, Clone, Default)]
pub struct IngestOptions {
    /// If `true`, stage all writes but do not commit — return a
    /// [`IngestReport`] with `commit_id = "dry-run"` and the `dry_run_diff`
    /// populated so the caller can preview the change set. The source file
    /// is still interned into `sources/` (that's not the risky part) but
    /// no pages are written to `wiki/` and no git commit is made.
    pub dry_run: bool,
}

/// Run the ingest pipeline with configurable options. See [`ingest`] for the
/// default path and [`IngestOptions`] for the knobs.
#[allow(clippy::too_many_lines)] // one big linear pipeline reads better than helpers
pub async fn ingest_with_options(
    vault: &Vault,
    provider: &dyn LlmProvider,
    source_path: &Path,
    options: IngestOptions,
) -> Result<IngestReport> {
    // 1. Read source text.
    let raw = fs::read(source_path).map_err(|e| Error::io(source_path.to_path_buf(), e))?;
    let source_text = String::from_utf8(raw.clone())
        .map_err(|e| Error::Other(anyhow::anyhow!("source is not UTF-8: {e}")))?;

    // 2. Intern into `sources/`.
    let interned = intern_source(vault, source_path, &raw)?;

    // 3. Load + render schema. Budget ~1/4 of the provider's context window.
    let schema = Schema::load(vault)?;
    let budget = provider.context_window() / 4;
    let rendered_schema = schema.render(budget);

    // 4. Build the prompt. Retrieval picks existing pages whose title or
    //    filename stem appears in the source text, capped at
    //    MAX_RELEVANT_PAGES. This makes the LLM aware of what the vault
    //    already contains so it can choose `update` over `create` for
    //    related pages. Zero LLM cost; works for Claude-only users.
    let prior_scan = vault.scan()?;
    let relevant_pages = select_relevant_pages(&prior_scan.pages, &source_text, MAX_RELEVANT_PAGES);
    let ctx = PromptContext::new(&rendered_schema, &relevant_pages);
    let label = source_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("source");
    let req = ingest_prompt(&ctx, label, &source_text);

    // 5. Call the LLM and parse the structured response.
    let response = provider
        .complete(req)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("llm ingest call: {e}")))?;
    // Best-effort usage logging; don't let a failed log block the ingest.
    let _ = record_usage(
        &vault.meta_dir(),
        "ingest",
        provider.name(),
        &response.model,
        &response.usage,
    );
    let plan: IngestPlan = serde_json::from_str(&response.text).map_err(|e| {
        Error::Other(anyhow::anyhow!(
            "llm ingest returned invalid IngestPlan json: {e}; raw = {}",
            response.text
        ))
    })?;

    // 6. Translate the plan into a VaultTx. Reuse the scan we did for
    //    retrieval — the vault state hasn't changed since step 4.
    let scan = prior_scan;
    let mut tx = vault.begin();
    let mut created = 0usize;
    let mut updated = 0usize;
    let source_ref = interned.to_string();
    // Mirror of `scan.pages` after every staged write, used to render the
    // updated `index.md` in step 7a. Starts as a copy of the current scan
    // and is patched in lockstep with each `tx.write_page` call.
    let mut future_pages: Vec<Page> = scan.pages.clone();

    for action in &plan.pages {
        let path = Utf8PathBuf::from(&action.path);
        let existing = scan.pages.iter().find(|p| p.path == path);
        let page = if let (IngestAction::Update, Some(existing)) = (action.action, existing) {
            let mut next = existing.clone();
            next.frontmatter.updated = Utc::now();
            next.frontmatter.tags.clone_from(&action.tags);
            if !next.frontmatter.sources.contains(&source_ref) {
                next.frontmatter.sources.push(source_ref.clone());
            }
            next.body.clone_from(&action.body);
            updated += 1;
            next
        } else {
            // Create (explicit create, or update-of-missing falls through).
            let mut fm = Frontmatter::new(&action.title);
            fm.tags.clone_from(&action.tags);
            fm.sources = vec![source_ref.clone()];
            created += 1;
            Page {
                path: path.clone(),
                frontmatter: fm,
                body: action.body.clone(),
            }
        };
        tx.write_page(&page)?;
        // Mirror the staged write into future_pages so the index render
        // below sees the post-ingest state.
        if let Some(idx) = future_pages.iter().position(|p| p.path == page.path) {
            future_pages[idx] = page;
        } else {
            future_pages.push(page);
        }
    }

    // 7. Append to log.md.
    let today = Utc::now().format("%Y-%m-%d");
    let log_entry = format!(
        "\n## [{today}] ingest | {summary}\n\n{entry}\n",
        summary = plan.summary,
        entry = plan.log_entry,
    );
    tx.append(Utf8Path::new("log.md"), &log_entry)?;

    // 7a. Regenerate index.md from the post-ingest page set. Skip the put
    //     when the rendered output is byte-identical to the on-disk version
    //     so we don't churn empty diffs into git history.
    let new_index = crate::index::render(&future_pages);
    let current_index =
        std::fs::read_to_string(vault.root().join("index.md").as_std_path()).unwrap_or_default();
    if new_index != current_index {
        tx.put_file(Utf8Path::new("index.md"), new_index)?;
    }

    // 8. Commit (or preview on dry-run).
    if options.dry_run {
        let diff = tx.diff();
        drop(tx); // explicit rollback; the Drop impl is a no-op anyway
        return Ok(IngestReport {
            source: interned,
            commit_id: "dry-run".into(),
            created,
            updated,
            summary: plan.summary,
            dry_run_diff: diff,
        });
    }
    let commit_message = format!("[ingest] {}", plan.summary);
    let commit_id = tx.commit(&commit_message)?;

    Ok(IngestReport {
        source: interned,
        commit_id,
        created,
        updated,
        summary: plan.summary,
        dry_run_diff: Vec::new(),
    })
}

/// Pick the `N` existing pages most likely to be relevant to a source text,
/// using a pure title/stem substring match.
///
/// For each candidate page, we compute a coarse score: 2 if the title is a
/// case-insensitive substring of the source, 1 if the filename stem is,
/// 0 otherwise. Pages tying on score break by longest title (longer titles
/// are more specific), then by lexicographic path order (deterministic).
///
/// This is deliberately simple. It costs nothing (no embeddings, no LLM
/// call), works for Claude-only users who can't afford an embeddings
/// provider, and meaningfully reduces the duplication rate on re-ingests
/// of related material. It will miss semantic relationships ("attention"
/// vs "self-attention") — that's what embeddings-based retrieval is for,
/// once a working embed provider is wired in.
fn select_relevant_pages<'a>(pages: &'a [Page], source_text: &str, top_n: usize) -> Vec<&'a Page> {
    if pages.is_empty() || top_n == 0 {
        return Vec::new();
    }
    let haystack = source_text.to_lowercase();
    let mut scored: Vec<(i32, usize, &'a Page)> = pages
        .iter()
        .map(|p| {
            let title = p.frontmatter.title.to_lowercase();
            let stem = p
                .path
                .file_stem()
                .map(str::to_lowercase)
                .unwrap_or_default();
            // Ignore trivially short matches (1-2 chars) which produce
            // nonsense hits ("a", "is", etc.).
            let title_hit =
                !title.is_empty() && title.chars().count() > 2 && haystack.contains(&title);
            let stem_hit = !stem.is_empty() && stem.chars().count() > 2 && haystack.contains(&stem);
            let score = match (title_hit, stem_hit) {
                (true, _) => 2,
                (false, true) => 1,
                (false, false) => 0,
            };
            (score, title.chars().count(), p)
        })
        .filter(|(score, _, _)| *score > 0)
        .collect();

    // Sort: score desc, title length desc, then path asc for determinism.
    scored.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(b.1.cmp(&a.1))
            .then(a.2.path.cmp(&b.2.path))
    });
    scored.truncate(top_n);
    scored.into_iter().map(|(_, _, p)| p).collect()
}

/// Copy a source file into `sources/<category>/<hash_prefix>-<slug>.<ext>`
/// inside the vault if it is not already there. Returns the vault-relative
/// path of the interned copy.
fn intern_source(vault: &Vault, source_path: &Path, bytes: &[u8]) -> Result<Utf8PathBuf> {
    let hash = sha256_hex(bytes);
    let hash_prefix = &hash[..12];
    let ext = source_path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("bin");
    let category = category_for_ext(ext);
    let stem = source_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("source");
    let slug = slugify(stem);
    let rel = Utf8PathBuf::from(format!("sources/{category}/{hash_prefix}-{slug}.{ext}"));
    let abs = vault.root().join(&rel);
    if !abs.as_std_path().exists() {
        if let Some(parent) = abs.as_std_path().parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.to_path_buf(), e))?;
        }
        std::fs::write(abs.as_std_path(), bytes)
            .map_err(|e| Error::io(abs.clone().into_std_path_buf(), e))?;
    }
    Ok(rel)
}

fn category_for_ext(ext: &str) -> &'static str {
    match ext.to_ascii_lowercase().as_str() {
        "md" | "markdown" | "txt" | "text" => "articles",
        "pdf" => "pdfs",
        "html" | "htm" => "web",
        _ => "data",
    }
}

/// Turn a freeform filename stem into a safe filesystem slug.
///
/// Unicode-aware: Cyrillic, CJK, accented Latin, and other non-ASCII
/// alphanumerics are preserved and lowercased. Non-alphanumeric runs
/// collapse to a single `-`, and the result is capped at 64 characters so
/// filenames stay manageable even on filesystems with strict limits.
///
/// Empty or all-punctuation inputs return an empty string; the caller is
/// responsible for providing a hash prefix that keeps filenames unique when
/// the slug is empty.
fn slugify(input: &str) -> String {
    const MAX_LEN: usize = 64;
    let mut out = String::with_capacity(input.len());
    let mut prev_dash = true;
    for ch in input.chars() {
        if ch.is_alphanumeric() {
            // `to_lowercase` yields an iterator because some Unicode chars
            // lowercase to multiple code points (e.g. German ß → ss).
            for lc in ch.to_lowercase() {
                out.push(lc);
            }
            prev_dash = false;
        } else if !prev_dash {
            out.push('-');
            prev_dash = true;
        }
    }
    let trimmed = out.trim_matches('-');
    // Cap at MAX_LEN code points (not bytes) so multi-byte scripts
    // don't silently overflow a bytes-only cap.
    if trimmed.chars().count() <= MAX_LEN {
        trimmed.to_string()
    } else {
        trimmed
            .chars()
            .take(MAX_LEN)
            .collect::<String>()
            .trim_end_matches('-')
            .to_string()
    }
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push(HEX_DIGITS[(b >> 4) as usize] as char);
        out.push(HEX_DIGITS[(b & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slugify_lowercases_and_dashes() {
        assert_eq!(slugify("Hello World"), "hello-world");
        assert_eq!(slugify("  __foo__BAR__  "), "foo-bar");
        assert_eq!(slugify(""), "");
        assert_eq!(slugify("123_abc"), "123-abc");
    }

    #[test]
    fn slugify_preserves_non_ascii_alphanumerics() {
        // Cyrillic — the previous ASCII-only version dropped all these.
        assert_eq!(slugify("Щоденник емоцій"), "щоденник-емоцій");
        assert_eq!(slugify("Архітектура"), "архітектура");
        // CJK
        assert_eq!(slugify("日本語のテスト"), "日本語のテスト");
        // Mixed scripts
        assert_eq!(slugify("Health Hub — обзор"), "health-hub-обзор");
        // Emoji is punctuation by Unicode class → collapsed to dash
        assert_eq!(slugify("🎬 Films & Movies"), "films-movies");
        // Accented Latin
        assert_eq!(slugify("Café Résumé"), "café-résumé");
    }

    #[test]
    fn slugify_caps_at_64_code_points() {
        let long = "a".repeat(200);
        assert_eq!(slugify(&long).chars().count(), 64);
        // Mixed-script long string also capped at code points, not bytes.
        let mixed = "щ".repeat(100);
        assert_eq!(slugify(&mixed).chars().count(), 64);
    }

    #[test]
    fn slugify_drops_all_punctuation_returns_empty() {
        assert_eq!(slugify("!!!"), "");
        assert_eq!(slugify("   "), "");
    }

    fn fake_page(stem: &str, title: &str) -> Page {
        use crate::vault::page::{Frontmatter, PageId};
        use chrono::{TimeZone, Utc};
        use std::collections::BTreeMap;
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from(format!("wiki/concepts/{stem}.md")),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: title.into(),
                created: now,
                updated: now,
                sources: vec![],
                tags: vec![],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: String::new(),
        }
    }

    #[test]
    fn select_relevant_pages_matches_title_substring() {
        let pages = vec![
            fake_page("attention", "Attention"),
            fake_page("transformers", "Transformers"),
            fake_page("orphan", "Orphan Note"),
        ];
        let source = "This article discusses how Attention works inside Transformers.";
        let hits = select_relevant_pages(&pages, source, 10);
        let titles: Vec<_> = hits.iter().map(|p| p.frontmatter.title.as_str()).collect();
        assert!(titles.contains(&"Attention"));
        assert!(titles.contains(&"Transformers"));
        assert!(!titles.contains(&"Orphan Note"));
    }

    #[test]
    fn select_relevant_pages_is_case_insensitive() {
        let pages = vec![fake_page("attention", "Attention")];
        let source = "lowercase attention is what we need";
        let hits = select_relevant_pages(&pages, source, 10);
        assert_eq!(hits.len(), 1);
    }

    #[test]
    fn select_relevant_pages_ignores_short_titles() {
        // Title "Is" would false-positive on every source containing "is".
        let pages = vec![fake_page("is", "Is")];
        let source = "this is a test";
        assert!(select_relevant_pages(&pages, source, 10).is_empty());
    }

    #[test]
    fn select_relevant_pages_caps_at_top_n() {
        let pages: Vec<Page> = (0..20)
            .map(|i| fake_page(&format!("page{i:02}"), &format!("Page {i:02}")))
            .collect();
        let source = (0..20)
            .map(|i| format!("Page {i:02}"))
            .collect::<Vec<_>>()
            .join(" ");
        let hits = select_relevant_pages(&pages, &source, 5);
        assert_eq!(hits.len(), 5);
    }

    #[test]
    fn select_relevant_pages_empty_inputs_return_empty() {
        assert!(select_relevant_pages(&[], "any text", 10).is_empty());
        let pages = vec![fake_page("a", "Alpha")];
        assert!(select_relevant_pages(&pages, "", 10).is_empty());
        assert!(select_relevant_pages(&pages, "alpha", 0).is_empty());
    }

    #[test]
    fn select_relevant_pages_prefers_title_over_stem_match() {
        let pages = vec![
            fake_page("transformers", "Transformers"),
            fake_page("attention", "Attention"),
        ];
        // Source mentions "transformers" (both title and stem) but only the
        // stem "attention". Both should score, but Transformers should sort
        // first because its title matched (score 2) vs Attention's stem (1).
        let source = "Transformers use attention as their core primitive.";
        let hits = select_relevant_pages(&pages, source, 10);
        assert_eq!(hits[0].frontmatter.title, "Transformers");
    }

    #[test]
    fn category_for_ext_groups_known_formats() {
        assert_eq!(category_for_ext("md"), "articles");
        assert_eq!(category_for_ext("MD"), "articles");
        assert_eq!(category_for_ext("pdf"), "pdfs");
        assert_eq!(category_for_ext("html"), "web");
        assert_eq!(category_for_ext("zip"), "data");
    }
}
