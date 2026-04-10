//! Multi-query expansion via LLM.
//!
//! Generates alternative phrasings of a search query so the retrieval
//! pipeline searches from multiple angles. This improves recall for
//! ambiguous or synonym-heavy queries.
//!
//! The expansion is always non-fatal: if the LLM call fails or returns
//! garbage, the original query is used as-is.
//!
//! Reference: `GBrain`'s `src/core/search/expansion.ts`.

use std::collections::HashSet;

use serde::Deserialize;
use serde_json::json;
use tracing::debug;

use crate::llm::{CompletionRequest, LlmProvider, Message, Role};

/// Minimum word count to attempt expansion. Queries shorter than this are
/// too terse to meaningfully rephrase.
const MIN_WORDS: usize = 3;

/// Generate up to `max_alternatives` alternative phrasings of `question`,
/// returning a vec that always starts with the original query.
///
/// Non-fatal: any failure (LLM error, parse error, empty response) returns
/// `vec![question.to_string()]`.
pub async fn expand_query(
    provider: &dyn LlmProvider,
    question: &str,
    max_alternatives: usize,
) -> Vec<String> {
    if question.split_whitespace().count() < MIN_WORDS {
        return vec![question.to_string()];
    }

    let result = try_expand(provider, question, max_alternatives).await;
    match result {
        Ok(mut variants) => {
            // Always start with the original.
            if variants.is_empty() || variants[0] != question {
                variants.insert(0, question.to_string());
            }
            // Deduplicate case-insensitively, preserving order.
            let mut seen = HashSet::new();
            variants.retain(|q| seen.insert(q.to_lowercase().trim().to_string()));
            // Cap at original + max_alternatives.
            variants.truncate(max_alternatives + 1);
            debug!(
                original = question,
                alternatives = ?&variants[1..],
                "query expanded"
            );
            variants
        }
        Err(e) => {
            debug!(error = %e, "query expansion failed, using original");
            vec![question.to_string()]
        }
    }
}

#[derive(Debug, Deserialize)]
struct ExpansionResponse {
    alternative_queries: Vec<String>,
}

async fn try_expand(
    provider: &dyn LlmProvider,
    question: &str,
    max_alternatives: usize,
) -> Result<Vec<String>, String> {
    let schema = json!({
        "type": "object",
        "properties": {
            "alternative_queries": {
                "type": "array",
                "items": { "type": "string" },
                "description": format!(
                    "{max_alternatives} alternative phrasings of the search query, \
                     each approaching the topic from a different angle or using \
                     different terminology."
                )
            }
        },
        "required": ["alternative_queries"],
        "additionalProperties": false
    });

    let req = CompletionRequest {
        system: "You are a search query expander. Generate alternative search \
                 queries that would find relevant results for the user's question. \
                 Each alternative should approach the topic from a different angle \
                 or use different terminology. Return JSON only."
            .to_string(),
        messages: vec![Message {
            role: Role::User,
            content: format!(
                "Generate {max_alternatives} alternative search queries for: \"{question}\""
            ),
        }],
        max_tokens: 300,
        temperature: Some(0.7),
        response_schema: Some(schema),
    };

    let resp = provider
        .complete(req)
        .await
        .map_err(|e| format!("LLM error: {e}"))?;

    let parsed: ExpansionResponse =
        serde_json::from_str(&resp.text).map_err(|e| format!("parse error: {e}"))?;

    let mut variants: Vec<String> = parsed
        .alternative_queries
        .into_iter()
        .filter(|q| !q.trim().is_empty())
        .take(max_alternatives)
        .collect();
    variants.insert(0, question.to_string());
    Ok(variants)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockProvider;

    #[tokio::test]
    async fn short_query_skips_expansion() {
        let mock = MockProvider::constant("should not be called");
        let result = expand_query(&mock, "two words", 2).await;
        assert_eq!(result, vec!["two words"]);
    }

    #[tokio::test]
    async fn expansion_returns_alternatives() {
        let mock = MockProvider::constant(
            r#"{"alternative_queries": ["how does the ingest pipeline work", "ingest process explanation"]}"#,
        );
        let result = expand_query(&mock, "how does ingest work", 2).await;
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], "how does ingest work");
    }

    #[tokio::test]
    async fn expansion_deduplicates() {
        // Mock returns the original as one of the alternatives.
        let mock = MockProvider::constant(
            r#"{"alternative_queries": ["how does ingest work", "ingest pipeline overview"]}"#,
        );
        let result = expand_query(&mock, "how does ingest work", 2).await;
        // Should deduplicate the original, leaving 2 unique entries.
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "how does ingest work");
    }

    #[tokio::test]
    async fn expansion_failure_returns_original() {
        let mock = MockProvider::constant("this is not valid json at all");
        let result = expand_query(&mock, "how does ingest work", 2).await;
        assert_eq!(result, vec!["how does ingest work"]);
    }
}
