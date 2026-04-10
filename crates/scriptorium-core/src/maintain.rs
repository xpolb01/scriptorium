//! Dream cycle / autonomous maintenance command.
//!
//! Runs all mechanical maintenance tasks in a single pass and returns a
//! structured report. Optionally auto-fixes safe issues (re-embed stale
//! pages, fix bad timestamps).
//!
//! Designed to be triggered by cron, Claude Code hooks, or the MCP
//! `scriptorium_maintain` tool. No LLM required for reporting; the embed
//! provider is only needed when `fix = true` and there are stale embeddings.
//!
//! Reference: `GBrain`'s `skills/maintain/SKILL.md`.

use serde::{Deserialize, Serialize};

use crate::embed::{self, EmbeddingsStore};
use crate::error::Result;
use crate::lint::{self, stale, LintReport, Severity};
use crate::llm::LlmProvider;
use crate::vault::Vault;

/// Options for [`maintain`].
#[derive(Debug, Clone, Default)]
pub struct MaintainOptions {
    /// Auto-fix safe issues: re-embed stale pages, fix bad timestamps.
    pub fix: bool,
}

/// Report from a [`maintain`] run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintainReport {
    /// Full lint report (broken links, orphans, frontmatter, stale pages).
    pub lint: LintReport,
    /// Pages with stale embeddings (content changed since last embed).
    pub stale_embedding_pages: Vec<String>,
    /// Embedding coverage: `(embedded_count, total_count)`.
    pub embedding_coverage: (usize, usize),
    /// Number of issues auto-fixed (0 if `fix = false`).
    pub auto_fixed: usize,
    /// Number of chunks re-embedded (0 if `fix = false` or no stale).
    pub chunks_reembedded: usize,
}

impl MaintainReport {
    /// Summary counts for display.
    pub fn summary(&self) -> MaintainSummary {
        MaintainSummary {
            errors: self.lint.count_by_severity(Severity::Error),
            warnings: self.lint.count_by_severity(Severity::Warning),
            stale_pages: self
                .lint
                .issues
                .iter()
                .filter(|i| i.rule == stale::STALE_PAGE)
                .count(),
            stale_embeddings: self.stale_embedding_pages.len(),
            embedded: self.embedding_coverage.0,
            total_pages: self.embedding_coverage.1,
            auto_fixed: self.auto_fixed,
            chunks_reembedded: self.chunks_reembedded,
        }
    }
}

/// Condensed summary for CLI output.
#[derive(Debug)]
pub struct MaintainSummary {
    pub errors: usize,
    pub warnings: usize,
    pub stale_pages: usize,
    pub stale_embeddings: usize,
    pub embedded: usize,
    pub total_pages: usize,
    pub auto_fixed: usize,
    pub chunks_reembedded: usize,
}

/// Run all maintenance tasks.
///
/// `embed_provider` is only needed when `options.fix` is true and there are
/// stale embeddings to refresh. Pass `None` for report-only mode.
pub async fn maintain(
    vault: &Vault,
    store: &EmbeddingsStore,
    embed_provider: Option<&dyn LlmProvider>,
    embed_model: &str,
    options: &MaintainOptions,
) -> Result<MaintainReport> {
    // 1. Run full lint (includes stale page detection from Phase 5).
    let lint_report = lint::run(vault)?;

    // 2. Detect stale embeddings.
    let scan = vault.scan()?;
    let total_pages = scan.pages.len();
    let embedded = store.distinct_page_count().unwrap_or(0);

    let mut stale_embedding_pages = Vec::new();
    for page in &scan.pages {
        let Ok(hash) = page.content_hash() else {
            continue;
        };
        // A page is "stale-embedded" if no provider/model combo has the
        // current content hash. We check all providers by looking for the
        // hash in ANY row for this page.
        let has_current: bool = store
            .has_any_version(page.frontmatter.id, &hash)
            .unwrap_or(false);
        if !has_current {
            stale_embedding_pages.push(page.path.to_string());
        }
    }

    // 3. Auto-fix if requested.
    let mut auto_fixed = 0usize;
    let mut chunks_reembedded = 0usize;

    if options.fix {
        // Fix bad timestamps via lint::fix.
        if let Ok(fix_report) = lint::fix::run(vault) {
            auto_fixed = fix_report.fixed.len();
        }

        // Re-embed stale pages.
        if !stale_embedding_pages.is_empty() {
            if let Some(provider) = embed_provider {
                match embed::reindex(vault, store, provider, embed_model).await {
                    Ok(n) => chunks_reembedded = n,
                    Err(e) => {
                        tracing::warn!(error = %e, "re-embed during maintain failed");
                    }
                }
            }
        }
    }

    Ok(MaintainReport {
        lint: lint_report,
        stale_embedding_pages,
        embedding_coverage: (embedded, total_pages),
        auto_fixed,
        chunks_reembedded,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embed::EmbeddingsStore;
    use crate::vault::Vault;

    #[tokio::test]
    async fn maintain_reports_on_empty_vault() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        git2::Repository::init(dir.path()).unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        let store = EmbeddingsStore::in_memory().unwrap();

        let report = maintain(&vault, &store, None, "mock-1", &MaintainOptions::default())
            .await
            .unwrap();

        assert!(report.lint.is_clean());
        assert!(report.stale_embedding_pages.is_empty());
        assert_eq!(report.embedding_coverage, (0, 0));
    }
}
