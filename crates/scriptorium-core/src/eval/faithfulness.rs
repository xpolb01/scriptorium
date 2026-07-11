//! Claim-level faithfulness judging.
//!
//! One LLM call decomposes a subject text into atomic factual claims and
//! verifies each strictly against a context. The wrappers below point the
//! same engine at scriptorium's two provenance-critical seams:
//!
//! - [`curation_audit`] — is a curated wiki page supported by the interned
//!   sources it cites? Catches ingest-time hallucination where it enters
//!   the vault.
//! - [`citation_audit`] — is an answer supported by the pages it cites?
//!   Complements the mechanical citation guard, which only checks that a
//!   cited ref *exists* in the retrieved set, not that it *supports* the
//!   claim.

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{Error, Result};
use crate::llm::{complete_as, CompletionRequest, LlmProvider, Message, Role};
use crate::vault::{Page, Vault};

/// Verdict for one atomic claim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimVerdict {
    /// The claim, as extracted from the subject text.
    pub claim: String,
    /// Whether the context supports the claim.
    pub supported: bool,
    /// Short quote or pointer from the context that supports the claim
    /// (empty when unsupported).
    #[serde(default)]
    pub evidence: String,
}

/// Aggregate result of a faithfulness judgement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaithfulnessReport {
    pub claims: Vec<ClaimVerdict>,
    /// `supported / total`, or 1.0 for a subject with no factual claims.
    pub score: f32,
}

impl FaithfulnessReport {
    pub fn supported(&self) -> usize {
        self.claims.iter().filter(|c| c.supported).count()
    }

    pub fn unsupported(&self) -> impl Iterator<Item = &ClaimVerdict> {
        self.claims.iter().filter(|c| !c.supported)
    }
}

#[derive(Debug, Deserialize)]
struct JudgeResponse {
    claims: Vec<ClaimVerdict>,
}

/// Decompose `subject` into atomic factual claims and verify each against
/// `context`. One LLM call; deterministic temperature.
pub async fn judge_faithfulness(
    provider: &dyn LlmProvider,
    subject: &str,
    context: &str,
) -> Result<FaithfulnessReport> {
    let schema = json!({
        "type": "object",
        "properties": {
            "claims": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "claim":     {"type": "string"},
                        "supported": {"type": "boolean"},
                        "evidence":  {"type": "string", "description": "Short supporting quote from the context; empty if unsupported."}
                    },
                    "required": ["claim", "supported"]
                }
            }
        },
        "required": ["claims"],
        "additionalProperties": false
    });
    let req = CompletionRequest {
        system: "You are a strict fact-checking judge. Decompose the SUBJECT \
                 into its atomic factual claims (skip opinions, hedges, and \
                 meta-commentary). For each claim decide whether the CONTEXT \
                 supports it. A claim is supported only if the context states \
                 or directly entails it — general plausibility and outside \
                 knowledge do not count. Quote the minimal supporting span as \
                 evidence. Return JSON only."
            .to_string(),
        messages: vec![Message {
            role: Role::User,
            content: format!("CONTEXT:\n{context}\n\nSUBJECT:\n{subject}"),
        }],
        max_tokens: 2048,
        temperature: Some(0.0),
        response_schema: Some(schema),
    };
    let parsed: JudgeResponse = complete_as(provider, req)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("faithfulness judge: {e}")))?;
    Ok(score_report(parsed.claims))
}

fn score_report(claims: Vec<ClaimVerdict>) -> FaithfulnessReport {
    let score = if claims.is_empty() {
        1.0
    } else {
        #[allow(clippy::cast_precision_loss)]
        let s = claims.iter().filter(|c| c.supported).count() as f32 / claims.len() as f32;
        s
    };
    FaithfulnessReport { claims, score }
}

/// Audit a curated wiki page against its own interned sources: read every
/// file in `frontmatter.sources`, concatenate them as the context, and
/// judge the page body against it.
///
/// Returns the report plus the list of source paths that could not be
/// read (moved/deleted sources make the audit incomplete, not failed).
pub async fn curation_audit(
    vault: &Vault,
    provider: &dyn LlmProvider,
    page: &Page,
) -> Result<(FaithfulnessReport, Vec<String>)> {
    let mut context = String::new();
    let mut missing = Vec::new();
    for source in &page.frontmatter.sources {
        let path = vault.root().join(source);
        match std::fs::read_to_string(path.as_std_path()) {
            Ok(text) => {
                context.push_str(&format!("=== {source} ===\n"));
                context.push_str(&text);
                context.push('\n');
            }
            Err(_) => missing.push(source.clone()),
        }
    }
    if context.trim().is_empty() {
        return Err(Error::Other(anyhow::anyhow!(
            "page cites no readable sources (missing: {missing:?})"
        )));
    }
    let report = judge_faithfulness(provider, &page.body, &context).await?;
    Ok((report, missing))
}

/// Audit answer text against the pages it cites: the context is exactly
/// the cited pages' bodies, so an unsupported claim means a citation that
/// does not actually back its sentence.
pub async fn citation_audit(
    provider: &dyn LlmProvider,
    answer: &str,
    cited_pages: &[&Page],
) -> Result<FaithfulnessReport> {
    let mut context = String::new();
    for page in cited_pages {
        context.push_str(&format!("=== [[{}]] ===\n", page.frontmatter.title));
        context.push_str(&page.body);
        context.push('\n');
    }
    judge_faithfulness(provider, answer, &context).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockProvider;

    #[tokio::test]
    async fn judge_parses_and_scores() {
        let mock = MockProvider::constant(
            r#"{"claims":[
                {"claim":"the sky is blue","supported":true,"evidence":"the sky appears blue"},
                {"claim":"grass is red","supported":false,"evidence":""}
            ]}"#,
        );
        let report = judge_faithfulness(&mock, "subject", "context")
            .await
            .unwrap();
        assert_eq!(report.claims.len(), 2);
        assert_eq!(report.supported(), 1);
        assert!((report.score - 0.5).abs() < f32::EPSILON);
        assert_eq!(report.unsupported().count(), 1);
    }

    #[tokio::test]
    async fn empty_claims_score_perfect() {
        let mock = MockProvider::constant(r#"{"claims":[]}"#);
        let report = judge_faithfulness(&mock, "no facts here", "ctx")
            .await
            .unwrap();
        assert!((report.score - 1.0).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn garbage_response_is_an_error() {
        let mock = MockProvider::constant("not json");
        assert!(judge_faithfulness(&mock, "s", "c").await.is_err());
    }
}
