//! Hybrid search: vector + keyword + RRF fusion + dedup.
//!
//! This module orchestrates multiple retrieval strategies (embedding-based
//! vector search, FTS5 keyword search, and optionally LLM-based query
//! expansion) into a single ranked result set via Reciprocal Rank Fusion.
//!
//! The primary entry point is [`hybrid_search`], which:
//! 1. Expands the query into alternative phrasings (optional, non-fatal).
//! 2. Embeds all query variants and runs vector search for each.
//! 3. Runs keyword search on the original query.
//! 4. Fuses all result lists with RRF.
//! 5. Deduplicates and diversifies the fused results.

pub mod dedup;
pub mod expansion;
pub mod fusion;

use std::collections::HashMap;

use crate::embed::{EmbeddingsStore, SearchHit};
use crate::error::{Error, Result};
use crate::llm::LlmProvider;
use crate::vault::{Page, PageId};

pub use dedup::DedupConfig;

/// Options for [`hybrid_search`].
#[derive(Debug, Clone)]
pub struct HybridSearchOpts {
    /// Final result count.
    pub top_k: usize,
    /// Enable multi-query expansion via LLM.
    pub expansion: bool,
    /// Max results per query variant from vector search.
    pub vector_limit: usize,
    /// Max results from keyword search.
    pub keyword_limit: usize,
    /// Dedup configuration.
    pub dedup: DedupConfig,
}

impl Default for HybridSearchOpts {
    fn default() -> Self {
        Self {
            top_k: 5,
            expansion: true,
            vector_limit: 20,
            keyword_limit: 20,
            dedup: DedupConfig::default(),
        }
    }
}

impl HybridSearchOpts {
    /// Create opts with the given `top_k`, deriving sensible limits.
    pub fn with_top_k(top_k: usize) -> Self {
        Self {
            top_k,
            vector_limit: top_k * 4,
            keyword_limit: top_k * 4,
            ..Default::default()
        }
    }
}

/// Run hybrid search: vector + keyword + RRF fusion + dedup.
///
/// `expansion_provider` is used for multi-query expansion (can be the same
/// as `embed_provider` — it just needs `complete()`). `pages` is the
/// current vault page set, used to populate `page_path` on results for
/// type-diversity dedup.
pub async fn hybrid_search(
    store: &EmbeddingsStore,
    embed_provider: &dyn LlmProvider,
    expansion_provider: &dyn LlmProvider,
    model: &str,
    question: &str,
    pages: &[Page],
    opts: &HybridSearchOpts,
) -> Result<Vec<SearchHit>> {
    // 1. Expand the query (non-fatal — falls back to original).
    let variants = if opts.expansion {
        expansion::expand_query(expansion_provider, question, 2).await
    } else {
        vec![question.to_string()]
    };

    // 2. Embed all query variants.
    let embeddings = embed_provider
        .embed(&variants)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("embed query variants: {e}")))?;

    // 3. Vector search for each variant.
    let mut all_lists: Vec<Vec<SearchHit>> = Vec::new();
    for emb in &embeddings {
        let hits = store.search(emb, embed_provider.name(), model, opts.vector_limit)?;
        all_lists.push(hits);
    }

    // 4. Keyword search on the original query.
    let kw_hits = store.keyword_search(question, embed_provider.name(), model, opts.keyword_limit)?;
    all_lists.push(kw_hits);

    // 5. RRF fusion.
    let fused = fusion::rrf_fuse(&all_lists);

    // 6. Annotate with page_path for type-diversity dedup.
    let page_paths = build_page_path_map(pages);
    let annotated: Vec<SearchHit> = fused
        .into_iter()
        .map(|mut hit| {
            hit.page_path = page_paths.get(&hit.page_id).cloned();
            hit
        })
        .collect();

    // 7. Dedup pipeline.
    let deduped = dedup::dedup_pipeline(annotated, &opts.dedup);

    // 8. Truncate to top_k.
    Ok(deduped.into_iter().take(opts.top_k).collect())
}

/// Build a map from `PageId` → vault-relative path string.
fn build_page_path_map(pages: &[Page]) -> HashMap<PageId, String> {
    pages
        .iter()
        .map(|p| (p.frontmatter.id, p.path.to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embed::{EmbeddingRow, EmbeddingsStore};
    use crate::llm::MockProvider;
    use crate::vault::PageId;

    fn unit(v: Vec<f32>) -> Vec<f32> {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.into_iter().map(|x| x / norm).collect()
    }

    fn embed_row(page_id: PageId, text: &str, vector: Vec<f32>) -> EmbeddingRow {
        EmbeddingRow {
            page_id,
            content_hash: "h1".into(),
            chunk_idx: 0,
            chunk_text: text.into(),
            heading: None,
            provider: "mock".into(),
            model: "mock-1".into(),
            vector,
        }
    }

    #[tokio::test]
    async fn hybrid_combines_vector_and_keyword() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id_a = PageId::new();
        let id_b = PageId::new();

        // Chunk A: high cosine similarity to query direction [1,0,0].
        store
            .upsert(&embed_row(id_a, "alpha vector match", unit(vec![0.95, 0.05, 0.0])))
            .unwrap();
        // Chunk B: low cosine similarity but contains the keyword "quantum".
        store
            .upsert(&embed_row(id_b, "quantum mechanics is fascinating", unit(vec![0.0, 0.0, 1.0])))
            .unwrap();

        let mock = MockProvider::constant(""); // expansion will fail → falls back to original
        let opts = HybridSearchOpts {
            top_k: 10,
            expansion: false, // disable expansion for determinism
            vector_limit: 10,
            keyword_limit: 10,
            dedup: DedupConfig::default(),
        };

        let results = hybrid_search(&store, &mock, &mock, "mock-1", "quantum", &[], &opts)
            .await
            .unwrap();

        // Should find at least the keyword match.
        let has_keyword_hit = results.iter().any(|h| h.chunk_text.contains("quantum"));
        assert!(has_keyword_hit, "keyword-only match should appear in hybrid results");
    }

    #[tokio::test]
    async fn hybrid_without_expansion_uses_original_only() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id = PageId::new();
        store
            .upsert(&embed_row(id, "some content", unit(vec![1.0, 0.0])))
            .unwrap();

        let mock = MockProvider::constant("");
        let opts = HybridSearchOpts {
            top_k: 5,
            expansion: false,
            vector_limit: 10,
            keyword_limit: 10,
            dedup: DedupConfig::default(),
        };

        // Should not panic or error even without expansion.
        let results = hybrid_search(&store, &mock, &mock, "mock-1", "content", &[], &opts)
            .await
            .unwrap();
        assert!(!results.is_empty());
    }
}
