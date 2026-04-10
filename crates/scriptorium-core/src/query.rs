//! Query pipeline: question → hybrid retrieval → LLM → cited answer.
//!
//! ```text
//! question  ──►  expand_query() → embed variants → vector search (N lists)
//!                                                   keyword search (1 list)
//!                                                         │
//!                                                         ▼
//!                                          RRF fusion → dedup pipeline
//!                                                         │
//!                                                         ▼
//!                              dedupe chunks → page ids → backlink expansion
//!                                                         │
//!                                                         ▼
//!                         PromptContext { schema, relevant_pages }
//!                                                         │
//!                                                         ▼
//!                             query_prompt()  ──►  provider.complete()
//!                                                         │
//!                                                         ▼
//!                                                  QueryAnswer (JSON)
//! ```
//!
//! Retrieval uses [`crate::search::hybrid_search`], which combines vector
//! similarity, FTS5 keyword search, multi-query expansion, and RRF fusion
//! into a single ranked result set. For tests, the mock provider gives
//! deterministic embeddings and expansion gracefully degrades.

use std::collections::{HashMap, HashSet};

use crate::embed::EmbeddingsStore;
use crate::error::{Error, Result};
use crate::lint::stale;
use crate::llm::{query_prompt, record_usage, LlmProvider, PromptContext, QueryAnswer};
use crate::schema::Schema;
use crate::search::{self, HybridSearchOpts};
use crate::vault::{LinkGraph, Page, PageId, Vault};

/// Report accompanying a [`query`] result: the answer itself plus which pages
/// were retrieved and which of them the LLM cited.
#[derive(Debug, Clone)]
pub struct QueryReport {
    pub answer: QueryAnswer,
    pub retrieved: Vec<PageId>,
    pub cited_stems: Vec<String>,
    /// Pages in the retrieved set whose source material is newer than the
    /// page's last update. These pages may contain outdated information.
    pub stale_pages: Vec<PageId>,
}

/// Run the full query pipeline.
///
/// `top_k` is the number of chunks pulled from the embeddings store; the
/// resulting page set is then expanded one hop through the link graph
/// (pages that link to any of the top hits are included as context).
///
/// Takes **two provider references** on purpose: `llm_provider` handles the
/// `complete()` call (generates the answer) and `embed_provider` handles
/// the `embed()` call (vectorizes the question for retrieval). They can be
/// the same underlying instance — e.g. `gemini` does both — or different,
/// which is the common setup when using Claude (no embed API) with a
/// separate embeddings backend.
pub async fn query(
    vault: &Vault,
    store: &EmbeddingsStore,
    llm_provider: &dyn LlmProvider,
    embed_provider: &dyn LlmProvider,
    model: &str,
    question: &str,
    top_k: usize,
) -> Result<QueryReport> {
    // 1-3. Hybrid search: vector + keyword + RRF fusion + dedup.
    let scan = vault.scan()?;
    let opts = HybridSearchOpts::with_top_k(top_k);
    let hits = search::hybrid_search(
        store,
        embed_provider,
        llm_provider,
        model,
        question,
        &scan.pages,
        &opts,
    )
    .await?;

    // Build page scores from the fused results.
    let mut page_scores: HashMap<PageId, f32> = HashMap::new();
    for hit in &hits {
        page_scores
            .entry(hit.page_id)
            .and_modify(|s| *s = s.max(hit.score))
            .or_insert(hit.score);
    }

    // 4. Build the graph for backlink expansion.
    let graph = LinkGraph::build(&scan.pages);

    let mut relevant_ids: HashSet<PageId> = page_scores.keys().copied().collect();
    let seed_ids: Vec<PageId> = page_scores.keys().copied().collect();
    for id in seed_ids {
        for back in graph.backlinks(id) {
            relevant_ids.insert(back);
        }
    }

    // 5. Order the relevant pages by score (fall back to 0 for expansion hits).
    let mut ordered_pages: Vec<(&Page, f32)> = scan
        .pages
        .iter()
        .filter(|p| relevant_ids.contains(&p.frontmatter.id))
        .map(|p| {
            let page_score = page_scores.get(&p.frontmatter.id).copied().unwrap_or(0.0);
            (p, page_score)
        })
        .collect();
    ordered_pages.sort_by(|a, b| b.1.total_cmp(&a.1));
    let relevant_refs: Vec<&Page> = ordered_pages.iter().map(|(p, _)| *p).collect();
    let retrieved: Vec<PageId> = relevant_refs.iter().map(|p| p.frontmatter.id).collect();

    // 6. Render schema and build prompt.
    let schema = Schema::load(vault)?;
    let rendered = schema.render(llm_provider.context_window() / 4);
    let ctx = PromptContext::new(&rendered, &relevant_refs);
    let req = query_prompt(&ctx, question);

    // 7. Call the LLM and parse the structured response.
    let resp = llm_provider
        .complete(req)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("query llm: {e}")))?;
    // Best-effort usage logging; don't let a failed log block the query.
    let _ = record_usage(
        &vault.meta_dir(),
        "query",
        llm_provider.name(),
        &resp.model,
        &resp.usage,
    );
    let answer: QueryAnswer = serde_json::from_str(&resp.text).map_err(|e| {
        Error::Other(anyhow::anyhow!(
            "llm query returned invalid QueryAnswer json: {e}; raw = {}",
            resp.text
        ))
    })?;

    // 8. Validate citations — the model must only cite pages we supplied.
    let retrieved_stems: HashSet<String> = relevant_refs
        .iter()
        .filter_map(|p| p.path.file_stem().map(std::string::ToString::to_string))
        .collect();
    let mut cited = Vec::new();
    for stem in &answer.citations {
        if retrieved_stems.contains(stem) {
            cited.push(stem.clone());
        }
    }

    // 9. Detect stale pages in the retrieved set.
    let stale_pages: Vec<PageId> = relevant_refs
        .iter()
        .filter(|p| stale::is_stale(vault, p))
        .map(|p| p.frontmatter.id)
        .collect();

    Ok(QueryReport {
        answer,
        retrieved,
        cited_stems: cited,
        stale_pages,
    })
}
