//! Listwise LLM reranking of fused retrieval candidates.
//!
//! Cross-encoder-style precision at the top of the funnel without a new
//! model or service: the already-configured chat provider reads the query
//! and a numbered list of candidate snippets and returns the candidate
//! indices ordered by relevance. One call per query, opt-in via
//! `[search] rerank = true`.
//!
//! Reranking is strictly best-effort: any LLM failure, parse failure, or
//! nonsense permutation leaves the fused order untouched.

use std::fmt::Write as _;

use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use crate::llm::{complete_as, CompletionRequest, LlmProvider, Message, Role};

/// One candidate offered to the reranker.
#[derive(Debug, Clone)]
pub struct RerankCandidate {
    /// Display identifier (page stem) — shown to the model, echoed nowhere.
    pub label: String,
    /// A short excerpt of the page (first few hundred chars, single line).
    pub snippet: String,
}

#[derive(Debug, Deserialize)]
struct RerankResponse {
    ranking: Vec<usize>,
}

/// Ask the LLM to order `candidates` by relevance to `question`.
///
/// Returns a permutation of `0..candidates.len()` (best first), or `None`
/// if the call failed or returned an invalid ranking. Indices the model
/// omitted are appended in their original order, so a partial-but-valid
/// answer still helps.
pub async fn llm_rerank(
    provider: &dyn LlmProvider,
    question: &str,
    candidates: &[RerankCandidate],
) -> Option<Vec<usize>> {
    if candidates.len() < 2 {
        return None;
    }
    let schema = json!({
        "type": "object",
        "properties": {
            "ranking": {
                "type": "array",
                "items": { "type": "integer" },
                "description": "Candidate indices ordered from most to least relevant."
            }
        },
        "required": ["ranking"],
        "additionalProperties": false
    });

    let mut listing = String::new();
    for (i, cand) in candidates.iter().enumerate() {
        let _ = writeln!(listing, "[{i}] {} — {}", cand.label, cand.snippet);
    }
    let req = CompletionRequest {
        system: "You are a retrieval reranker. Given a question and a numbered \
                 list of candidate pages, return JSON with `ranking`: every \
                 candidate index, ordered from most to least relevant to the \
                 question. Judge only by the provided text."
            .to_string(),
        messages: vec![Message {
            role: Role::User,
            content: format!("Question: {question}\n\nCandidates:\n{listing}"),
        }],
        max_tokens: 300,
        temperature: Some(0.0),
        response_schema: Some(schema),
    };

    let parsed: RerankResponse = match complete_as(provider, req).await {
        Ok(p) => p,
        Err(e) => {
            debug!(error = %e, "rerank failed, keeping fused order");
            return None;
        }
    };
    normalize_ranking(&parsed.ranking, candidates.len())
}

/// Validate and complete a model-returned ranking: dedupe, drop
/// out-of-range indices, append anything missing in original order.
/// Returns `None` when the response contains no usable indices.
fn normalize_ranking(raw: &[usize], n: usize) -> Option<Vec<usize>> {
    let mut seen = vec![false; n];
    let mut order = Vec::with_capacity(n);
    for &idx in raw {
        if idx < n && !seen[idx] {
            seen[idx] = true;
            order.push(idx);
        }
    }
    if order.is_empty() {
        return None;
    }
    for (idx, was_seen) in seen.iter().enumerate() {
        if !was_seen {
            order.push(idx);
        }
    }
    Some(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockProvider;

    fn cands(n: usize) -> Vec<RerankCandidate> {
        (0..n)
            .map(|i| RerankCandidate {
                label: format!("page-{i}"),
                snippet: format!("snippet {i}"),
            })
            .collect()
    }

    #[tokio::test]
    async fn valid_ranking_is_applied() {
        let mock = MockProvider::constant(r#"{"ranking":[2,0,1]}"#);
        let order = llm_rerank(&mock, "q", &cands(3)).await;
        assert_eq!(order, Some(vec![2, 0, 1]));
    }

    #[tokio::test]
    async fn partial_ranking_is_completed() {
        let mock = MockProvider::constant(r#"{"ranking":[1]}"#);
        let order = llm_rerank(&mock, "q", &cands(3)).await;
        assert_eq!(order, Some(vec![1, 0, 2]));
    }

    #[tokio::test]
    async fn garbage_returns_none() {
        let mock = MockProvider::constant("not json at all");
        assert_eq!(llm_rerank(&mock, "q", &cands(3)).await, None);
    }

    #[tokio::test]
    async fn out_of_range_and_duplicates_are_dropped() {
        let mock = MockProvider::constant(r#"{"ranking":[9,1,1,0]}"#);
        let order = llm_rerank(&mock, "q", &cands(3)).await;
        assert_eq!(order, Some(vec![1, 0, 2]));
    }

    #[tokio::test]
    async fn single_candidate_skips_the_call() {
        let mock = MockProvider::constant(r#"{"ranking":[0]}"#);
        assert_eq!(llm_rerank(&mock, "q", &cands(1)).await, None);
    }

    #[test]
    fn normalize_rejects_all_invalid() {
        assert_eq!(normalize_ranking(&[7, 8, 9], 3), None);
    }
}
