//! Reference-free context precision: LLM-judged retrieval quality.
//!
//! The labeled benchmark suite needs hand-curated `expected` stems per
//! query. This judge instead grades the retrieved chunks themselves —
//! "is this chunk relevant to this query?" — so *any* query can be
//! scored, and the bench suite can grow straight from real query logs.
//!
//! The score is rank-weighted (RAGAS-style context precision): relevant
//! hits near the top count more than relevant hits near the bottom,
//! `Σ(rel_i · precision@i) / Σ(rel_i)` over the ranked list.

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::embed::SearchHit;
use crate::error::{Error, Result};
use crate::llm::{complete_as, CompletionRequest, LlmProvider, Message, Role};

/// Judged relevance of one retrieval, plus the derived score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextPrecisionReport {
    /// Per-hit relevance verdicts, in rank order.
    pub relevant: Vec<bool>,
    /// Rank-weighted context precision in [0, 1]. `None` when there were
    /// no hits to judge.
    pub score: Option<f32>,
}

#[derive(Debug, Deserialize)]
struct RelevanceResponse {
    relevant_indices: Vec<usize>,
}

/// Judge each hit's relevance to `question` in one listwise LLM call and
/// compute rank-weighted context precision.
pub async fn judge_context_precision(
    provider: &dyn LlmProvider,
    question: &str,
    hits: &[SearchHit],
) -> Result<ContextPrecisionReport> {
    if hits.is_empty() {
        return Ok(ContextPrecisionReport {
            relevant: Vec::new(),
            score: None,
        });
    }
    let schema = json!({
        "type": "object",
        "properties": {
            "relevant_indices": {
                "type": "array",
                "items": {"type": "integer"},
                "description": "Indices of the chunks that contain information relevant to answering the question."
            }
        },
        "required": ["relevant_indices"],
        "additionalProperties": false
    });
    let mut listing = String::new();
    for (i, hit) in hits.iter().enumerate() {
        use std::fmt::Write as _;
        let snippet: String = hit.chunk_text.chars().take(400).collect();
        let _ = writeln!(listing, "[{i}] {snippet}");
    }
    let req = CompletionRequest {
        system: "You judge retrieval quality. Given a question and a numbered \
                 list of retrieved text chunks, return the indices of chunks \
                 that contain information genuinely useful for answering the \
                 question. Judge each chunk on its own text only. Return JSON."
            .to_string(),
        messages: vec![Message {
            role: Role::User,
            content: format!("Question: {question}\n\nChunks:\n{listing}"),
        }],
        // Output is a short index list, but proxies routing Claude through
        // Vertex/Bedrock can burn completion tokens on internal reasoning
        // first — keep real headroom (see eval::faithfulness).
        max_tokens: 4096,
        temperature: Some(0.0),
        response_schema: Some(schema),
    };
    let parsed: RelevanceResponse = complete_as(provider, req)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("context-precision judge: {e}")))?;

    let mut relevant = vec![false; hits.len()];
    for idx in parsed.relevant_indices {
        if idx < relevant.len() {
            relevant[idx] = true;
        }
    }
    Ok(ContextPrecisionReport {
        score: Some(rank_weighted_precision(&relevant)),
        relevant,
    })
}

/// RAGAS-style context precision: `Σ(rel_i · P@i) / Σ(rel_i)`, where
/// `P@i` is the precision of the prefix ending at rank `i`. Returns 0.0
/// when nothing was relevant.
#[allow(clippy::cast_precision_loss)]
fn rank_weighted_precision(relevant: &[bool]) -> f32 {
    let mut hits_so_far = 0usize;
    let mut acc = 0.0f32;
    for (i, &rel) in relevant.iter().enumerate() {
        if rel {
            hits_so_far += 1;
            acc += hits_so_far as f32 / (i + 1) as f32;
        }
    }
    if hits_so_far == 0 {
        0.0
    } else {
        acc / hits_so_far as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockProvider;
    use crate::vault::PageId;

    fn hit(text: &str) -> SearchHit {
        SearchHit {
            page_id: PageId::new(),
            chunk_idx: 0,
            heading: None,
            chunk_text: text.into(),
            score: 1.0,
            page_path: None,
        }
    }

    #[test]
    fn precision_rewards_relevant_at_top() {
        // Relevant first: P@1 = 1.0.
        assert!((rank_weighted_precision(&[true, false]) - 1.0).abs() < f32::EPSILON);
        // Relevant last of two: P@2 = 0.5.
        assert!((rank_weighted_precision(&[false, true]) - 0.5).abs() < f32::EPSILON);
        // Nothing relevant.
        assert!(rank_weighted_precision(&[false, false]).abs() < f32::EPSILON);
        // All relevant = 1.0 regardless of length.
        assert!((rank_weighted_precision(&[true, true, true]) - 1.0).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn judge_maps_indices_and_scores() {
        let mock = MockProvider::constant(r#"{"relevant_indices":[0,2,9]}"#);
        let hits = vec![hit("a"), hit("b"), hit("c")];
        let report = judge_context_precision(&mock, "q", &hits).await.unwrap();
        assert_eq!(report.relevant, vec![true, false, true]);
        // P@1 = 1.0, P@3 = 2/3 → mean = 5/6.
        let score = report.score.unwrap();
        assert!((score - 5.0 / 6.0).abs() < 1e-6, "got {score}");
    }

    #[tokio::test]
    async fn empty_hits_score_none() {
        let mock = MockProvider::constant(r#"{"relevant_indices":[]}"#);
        let report = judge_context_precision(&mock, "q", &[]).await.unwrap();
        assert!(report.score.is_none());
    }
}
