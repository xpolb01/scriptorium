//! Query pipeline: question → hybrid retrieval → LLM → cited answer.
//!
//! ```text
//! question  ──►  expand_query() → embed variants → vector search (N lists)
//!                                                   keyword search (1 list)
//!                                                   PPR graph walk (1 list)
//!                                                         │
//!                                                         ▼
//!                                          RRF fusion → dedup pipeline
//!                                                         │
//!                                                         ▼
//!                                          dedupe chunks → page ids
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
//! similarity, FTS5 keyword search, multi-query expansion, Personalized
//! `PageRank` over the wikilink graph (multi-hop expansion — replaces the
//! old single-hop backlink hop), and RRF fusion into a single ranked result
//! set. For tests, the mock provider gives deterministic embeddings and
//! expansion gracefully degrades.

use std::collections::{HashMap, HashSet};

use crate::config::SearchConfig;
use crate::embed::EmbeddingsStore;
use crate::error::{Error, Result};
use crate::lint::stale;
use crate::llm::{query_prompt, record_usage, LlmProvider, PromptContext, QueryAnswer};
use crate::schema::Schema;
use crate::search::rerank::{llm_rerank, RerankCandidate};
use crate::search::{self, HybridSearchOpts};
use crate::vault::{Page, PageId, Vault};

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
/// `top_k` is the number of chunks pulled from the embeddings store. Graph
/// context comes from the Personalized `PageRank` list inside
/// [`search::hybrid_search`]: pages linked (multi-hop, forward or backward)
/// to the top hits compete in the fusion and surface directly in the
/// ranked result set.
///
/// Takes **two provider references** on purpose: `llm_provider` handles the
/// `complete()` call (generates the answer) and `embed_provider` handles
/// the `embed()` call (vectorizes the question for retrieval). They can be
/// the same underlying instance — e.g. `gemini` does both — or different,
/// which is the common setup when using Claude (no embed API) with a
/// separate embeddings backend.
// Two providers + tuning knobs put this one over the parameter limit; the
// call sites read fine and a params struct would just move the noise.
#[allow(clippy::too_many_arguments)]
pub async fn query(
    vault: &Vault,
    store: &EmbeddingsStore,
    llm_provider: &dyn LlmProvider,
    embed_provider: &dyn LlmProvider,
    model: &str,
    question: &str,
    top_k: usize,
    search_cfg: &SearchConfig,
) -> Result<QueryReport> {
    // 1-3. Hybrid search: vector + keyword + PPR graph expansion + RRF
    //      fusion + dedup.
    let scan = vault.scan()?;
    let mut opts = HybridSearchOpts::with_top_k(top_k);
    opts.hyde = search_cfg.hyde;
    opts.dedup.mmr_lambda = search_cfg.mmr_lambda;
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

    // 4. Build page scores from the fused results. Graph-relevant pages are
    //    already folded in: hybrid search fuses a PPR list over the wikilink
    //    graph, so multi-hop neighbors of strong hits arrive here scored.
    let mut page_scores: HashMap<PageId, f32> = HashMap::new();
    for hit in &hits {
        page_scores
            .entry(hit.page_id)
            .and_modify(|s| *s = s.max(hit.score))
            .or_insert(hit.score);
    }
    let relevant_ids: HashSet<PageId> = page_scores.keys().copied().collect();

    // 4b. Optional recency boost: recently-updated pages rank higher.
    //     (Keys are unchanged — only scores move — so the id set above
    //     stays valid.)
    apply_recency_boost(&mut page_scores, &scan.pages, search_cfg);

    // 5. Order the relevant pages by score.
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
    let mut relevant_refs: Vec<&Page> = ordered_pages.iter().map(|(p, _)| *p).collect();

    // 5b. Optional listwise LLM rerank of the top candidates. Best-effort:
    // failure keeps the fused order. Reordering here changes which pages
    // lead the prompt context (and what the model reads first).
    if search_cfg.rerank {
        maybe_rerank(&mut relevant_refs, llm_provider, question, search_cfg).await;
    }
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

/// Multiply page scores by a recency factor when the boost is enabled:
/// `score × (1 + weight · 0.5^(age_days / half_life))`. A page updated
/// today gets the full `weight`; one updated `half_life` days ago gets
/// half of it. Scores only ever increase, so relative order among pages
/// of the same age is preserved.
fn apply_recency_boost(page_scores: &mut HashMap<PageId, f32>, pages: &[Page], cfg: &SearchConfig) {
    if cfg.recency_half_life_days <= 0.0 || cfg.recency_weight <= 0.0 {
        return;
    }
    let now = chrono::Utc::now();
    for page in pages {
        if let Some(score) = page_scores.get_mut(&page.frontmatter.id) {
            #[allow(clippy::cast_precision_loss)]
            let age_days = (now - page.frontmatter.updated).num_seconds().max(0) as f32 / 86_400.0;
            let boost =
                1.0 + cfg.recency_weight * 0.5f32.powf(age_days / cfg.recency_half_life_days);
            *score *= boost;
        }
    }
}

/// Rerank the leading pages of `refs` in place with one listwise LLM call.
/// Any failure leaves the order untouched.
async fn maybe_rerank(
    refs: &mut Vec<&Page>,
    llm_provider: &dyn LlmProvider,
    question: &str,
    cfg: &SearchConfig,
) {
    if refs.len() < 2 {
        return;
    }
    let n = cfg.rerank_top_n.max(2).min(refs.len());
    let candidates: Vec<RerankCandidate> = refs[..n]
        .iter()
        .map(|p| RerankCandidate {
            label: p
                .path
                .file_stem()
                .map_or_else(|| p.frontmatter.title.clone(), ToString::to_string),
            snippet: snippet_of(&p.body, 240),
        })
        .collect();
    if let Some(order) = llm_rerank(llm_provider, question, &candidates).await {
        let head: Vec<&Page> = order.iter().map(|&i| refs[i]).collect();
        let tail: Vec<&Page> = refs[n..].to_vec();
        *refs = head.into_iter().chain(tail).collect();
    }
}

/// First `max_chars` of a body, whitespace-collapsed to one line.
fn snippet_of(body: &str, max_chars: usize) -> String {
    let collapsed: String = body.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.chars().take(max_chars).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, Utc};

    use crate::vault::Frontmatter;

    fn page_updated_days_ago(days: i64) -> Page {
        let now = Utc::now();
        let frontmatter = Frontmatter {
            id: PageId::new(),
            title: format!("page-{days}"),
            created: now - Duration::days(days),
            updated: now - Duration::days(days),
            sources: Vec::new(),
            tags: Vec::new(),
            aliases: Vec::new(),
            schema_version: crate::vault::page::SCHEMA_VERSION,
            extra: std::collections::BTreeMap::new(),
        };
        Page {
            path: format!("wiki/concepts/page-{days}.md").into(),
            frontmatter,
            body: String::new(),
        }
    }

    #[test]
    fn recency_boost_disabled_by_default_config() {
        let fresh = page_updated_days_ago(0);
        let old = page_updated_days_ago(300);
        let mut scores: HashMap<PageId, f32> = HashMap::new();
        scores.insert(fresh.frontmatter.id, 0.5);
        scores.insert(old.frontmatter.id, 0.5);
        let cfg = SearchConfig::default();
        apply_recency_boost(&mut scores, &[fresh.clone(), old.clone()], &cfg);
        assert!((scores[&fresh.frontmatter.id] - 0.5).abs() < f32::EPSILON);
        assert!((scores[&old.frontmatter.id] - 0.5).abs() < f32::EPSILON);
    }

    #[test]
    fn recency_boost_prefers_fresh_pages() {
        let fresh = page_updated_days_ago(0);
        let old = page_updated_days_ago(300);
        let mut scores: HashMap<PageId, f32> = HashMap::new();
        scores.insert(fresh.frontmatter.id, 0.5);
        scores.insert(old.frontmatter.id, 0.5);
        let cfg = SearchConfig {
            recency_half_life_days: 30.0,
            recency_weight: 0.3,
            ..SearchConfig::default()
        };
        apply_recency_boost(&mut scores, &[fresh.clone(), old.clone()], &cfg);
        let f = scores[&fresh.frontmatter.id];
        let o = scores[&old.frontmatter.id];
        assert!(f > o, "fresh page must outrank stale page: {f} vs {o}");
        assert!(f <= 0.5 * 1.3 + f32::EPSILON, "boost is bounded by weight");
        assert!(o >= 0.5, "boost never lowers a score");
    }

    #[test]
    fn snippet_collapses_whitespace_and_truncates() {
        let s = snippet_of("line one\n\n  line   two\nthree", 12);
        assert_eq!(s, "line one lin");
    }
}
