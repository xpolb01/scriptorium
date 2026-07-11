//! Orchestrate a full vault reindex: refresh embeddings, regenerate
//! `index.md`, and run mechanical lint. The cheap checks all happen in one
//! place so a "reindex" is the single command users run after pulling
//! external edits or after a config change.
//!
//! Three steps, fixed order:
//!
//! 1. Embeddings: `embed::reindex` walks the vault and embeds anything not
//!    already cached. Most expensive step (network I/O); runs first so its
//!    failure aborts the rest.
//! 2. `index.md`: re-render from the latest scan and commit if changed.
//!    Uses [`commit_without_validation`](crate::vault::VaultTx::commit_without_validation)
//!    so a pre-existing broken link in some other page does not block the
//!    derived-state refresh — the lint step at the end surfaces those
//!    issues to the caller anyway.
//! 3. Lint: run mechanical rules over the post-update vault and return the
//!    report alongside the embedding count and index status.

use camino::Utf8Path;

use crate::embed::{self, EmbeddingsStore};
use crate::error::Result;
use crate::index;
use crate::lint::{self, LintReport};
use crate::llm::LlmProvider;
use crate::vault::Vault;

/// Outcome of [`reindex_all`].
#[derive(Debug)]
pub struct ReindexReport {
    /// Number of new embedding chunks written by this run. Cache hits do
    /// not count, so a no-op reindex reports `0`.
    pub embeddings_written: usize,
    /// `true` if `index.md` was regenerated and re-committed. `false` when
    /// the rendered index was byte-identical to the on-disk version.
    pub index_updated: bool,
    /// Mechanical lint report taken after the embeddings + index steps land.
    pub lint: LintReport,
}

/// Walk the vault and refresh every derived artifact: embeddings, index, lint.
pub async fn reindex_all(
    vault: &Vault,
    store: &EmbeddingsStore,
    embed_provider: &dyn LlmProvider,
    embed_model: &str,
) -> Result<ReindexReport> {
    reindex_all_with(vault, store, embed_provider, embed_model, None, false).await
}

/// Like [`reindex_all`], with the contextual-retrieval chat provider and
/// an optional full rebuild (drop all cached rows first, so strategy or
/// contextual-mode changes actually re-embed).
pub async fn reindex_all_with(
    vault: &Vault,
    store: &EmbeddingsStore,
    embed_provider: &dyn LlmProvider,
    embed_model: &str,
    contextual_chat: Option<&dyn LlmProvider>,
    rebuild: bool,
) -> Result<ReindexReport> {
    if rebuild {
        store.retain_page_versions(&[])?;
    }
    let embeddings_written = embed::reindex_contextual(
        vault,
        store,
        embed_provider,
        embed_model,
        crate::config::ChunkStrategy::default(),
        contextual_chat,
    )
    .await?;

    // Regenerate index.md if it differs from the on-disk version. This is a
    // standalone commit (the embed step doesn't touch git at all). We bypass
    // validation because the index.md write is mechanically derived from the
    // current page set; pre-existing lint errors elsewhere in the vault
    // shouldn't block a derived-state refresh.
    let scan = vault.scan()?;
    let new_index = index::render(&scan.pages);
    let current_index =
        std::fs::read_to_string(vault.root().join("index.md").as_std_path()).unwrap_or_default();
    let index_updated = new_index != current_index;
    if index_updated {
        let mut tx = vault.begin();
        tx.put_file(Utf8Path::new("index.md"), new_index)?;
        tx.commit_without_validation("[reindex] regenerate index.md")?;
    }

    let lint = lint::run(vault)?;

    Ok(ReindexReport {
        embeddings_written,
        index_updated,
        lint,
    })
}
