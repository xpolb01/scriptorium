//! Helpers that tie [`chunk_page`](super::chunk_page),
//! [`EmbeddingsStore`](super::EmbeddingsStore), and an [`LlmProvider`]
//! together so callers don't have to reassemble the pipeline each time.
//!
//! - [`embed_page`] embeds a single page, skipping the work if the store
//!   already has a row for this `(page_id, content_hash, provider, model)`.
//! - [`reindex`] walks the vault and embeds every page it finds.
//!
//! Both are intentionally async — the `LlmProvider::embed` call hits the
//! network for real providers. The mock provider returns immediately so the
//! integration tests stay fast.

use tracing::debug;

use super::{chunk_page, EmbeddingRow, EmbeddingsStore};
use crate::error::{Error, Result};
use crate::llm::LlmProvider;
use crate::vault::{Page, Vault};

/// Default chunk size, in characters. ~4000 chars ≈ ~1000 tokens, which
/// comfortably fits every popular embedding model's per-request limit.
pub const DEFAULT_CHUNK_CHARS: usize = 4000;

/// Embed a single page, inserting one row per chunk. Returns the number of
/// chunks embedded, or `0` if the store already had cached vectors for the
/// current `(page_id, content_hash, provider, model)`.
pub async fn embed_page(
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    page: &Page,
) -> Result<usize> {
    let hash = page.content_hash()?;
    if store.has_page_version(page.frontmatter.id, &hash, provider.name(), model)? {
        debug!(page = %page.path, "embed cache hit");
        return Ok(0);
    }
    let chunks = chunk_page(&page.body, DEFAULT_CHUNK_CHARS);
    if chunks.is_empty() {
        return Ok(0);
    }
    let texts: Vec<String> = chunks.iter().map(|c| c.text.clone()).collect();
    let vectors = provider
        .embed(&texts)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("embed call: {e}")))?;
    if vectors.len() != chunks.len() {
        return Err(Error::Other(anyhow::anyhow!(
            "provider returned {} vectors for {} chunks",
            vectors.len(),
            chunks.len()
        )));
    }
    for (chunk, vector) in chunks.iter().zip(vectors.into_iter()) {
        store.upsert(&EmbeddingRow {
            page_id: page.frontmatter.id,
            content_hash: hash.clone(),
            chunk_idx: chunk.idx,
            chunk_text: chunk.text.clone(),
            heading: chunk.heading.clone(),
            provider: provider.name().to_string(),
            model: model.to_string(),
            vector,
        })?;
    }
    Ok(chunks.len())
}

/// Walk the vault and embed every page that isn't already cached. Returns
/// the total number of chunks embedded across all pages.
pub async fn reindex(
    vault: &Vault,
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
) -> Result<usize> {
    let scan = vault.scan()?;
    let mut total = 0;
    for page in &scan.pages {
        total += embed_page(store, provider, model, page).await?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockProvider;
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    fn make_page(stem: &str, body: &str) -> Page {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from(format!("wiki/{stem}.md")),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: stem.into(),
                created: now,
                updated: now,
                sources: vec![],
                tags: vec![],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: body.into(),
        }
    }

    #[tokio::test]
    async fn embed_page_populates_store_and_is_cached() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let mock = MockProvider::constant("");
        let page = make_page("alpha", "## First\n\nSome content.\n\n## Second\n\nMore.\n");
        let inserted = embed_page(&store, &mock, "mock-1", &page).await.unwrap();
        assert_eq!(inserted, 2, "two H2 sections → two chunks");
        assert_eq!(store.len().unwrap(), 2);
        // A second call is a cache hit.
        let again = embed_page(&store, &mock, "mock-1", &page).await.unwrap();
        assert_eq!(again, 0);
        assert_eq!(store.len().unwrap(), 2);
    }

    #[tokio::test]
    async fn embed_page_handles_empty_body() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let mock = MockProvider::constant("");
        let page = make_page("empty", "");
        let inserted = embed_page(&store, &mock, "mock-1", &page).await.unwrap();
        assert_eq!(inserted, 0);
        assert_eq!(store.len().unwrap(), 0);
    }
}
