//! Related-page suggestion: candidate `[[wikilinks]]` for a page.
//!
//! The Smart-Connections-class feature, native: embed the page, search
//! the existing store for the nearest chunks of *other* pages, and rank
//! the owning pages. Suggestions the page already links to are marked so
//! callers can filter or display them differently. The curator (human or
//! LLM) decides what to write — and the transactional lint guarantees
//! whatever gets written resolves.

use serde::{Deserialize, Serialize};

use crate::embed::EmbeddingsStore;
use crate::error::{Error, Result};
use crate::llm::LlmProvider;
use crate::vault::{LinkGraph, Vault};

/// One suggested link target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkSuggestion {
    /// Wikilink stem (what goes inside `[[...]]`).
    pub stem: String,
    pub title: String,
    /// Max chunk cosine similarity to the subject page.
    pub score: f32,
    /// The subject page already links here.
    pub already_linked: bool,
}

/// Suggest up to `top_k` related pages for the page with stem `stem`.
pub async fn suggest_links(
    vault: &Vault,
    store: &EmbeddingsStore,
    embed_provider: &dyn LlmProvider,
    model: &str,
    stem: &str,
    top_k: usize,
) -> Result<Vec<LinkSuggestion>> {
    let scan = vault.scan()?;
    let subject = scan
        .pages
        .iter()
        .find(|p| p.path.file_stem() == Some(stem))
        .ok_or_else(|| Error::Other(anyhow::anyhow!("no page with stem '{stem}'")))?;

    // Embed a prefix of the subject body (single call).
    let text: String = subject.body.chars().take(2000).collect();
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let vectors = embed_provider
        .embed(&[text])
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("embed subject page: {e}")))?;
    let query_vec = vectors
        .first()
        .ok_or_else(|| Error::Other(anyhow::anyhow!("provider returned no vector")))?;

    // Nearest chunks across the store; keep the best score per other page.
    let hits = store.search(query_vec, embed_provider.name(), model, top_k * 6)?;
    let graph = LinkGraph::build(&scan.pages);
    let outgoing: std::collections::HashSet<String> = graph
        .forward(subject.frontmatter.id)
        .map(|links| {
            links
                .iter()
                .filter(|l| !l.link.target.is_empty())
                .map(|l| l.link.target.clone())
                .collect()
        })
        .unwrap_or_default();

    let mut best: std::collections::HashMap<crate::vault::PageId, f32> =
        std::collections::HashMap::new();
    for hit in hits {
        if hit.page_id == subject.frontmatter.id {
            continue;
        }
        best.entry(hit.page_id)
            .and_modify(|s| *s = s.max(hit.score))
            .or_insert(hit.score);
    }

    let mut out: Vec<LinkSuggestion> = scan
        .pages
        .iter()
        .filter_map(|p| {
            let sim = *best.get(&p.frontmatter.id)?;
            let stem = p.path.file_stem()?.to_string();
            Some(LinkSuggestion {
                already_linked: outgoing.contains(&stem),
                title: p.frontmatter.title.clone(),
                stem,
                score: sim,
            })
        })
        .collect();
    out.sort_by(|a, b| b.score.total_cmp(&a.score));
    out.truncate(top_k);
    Ok(out)
}
