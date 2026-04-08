//! In-process mock [`LlmProvider`] used by tests and CI.
//!
//! Ships with two construction styles:
//!
//! - [`MockProvider::with_handler`] — a closure is called with each incoming
//!   [`CompletionRequest`] and returns a [`CompletionResponse`]. Use this when
//!   you want dynamic behaviour (e.g. different responses per operation).
//! - [`MockProvider::with_fixtures`] — a map of substring-match keys to
//!   canned response text. Simpler for basic tests: "when the prompt
//!   contains `KEY`, return `VALUE`".
//!
//! Embeddings are derived deterministically from the SHA-256 of the input
//! text, so the same text always produces the same unit vector. This is
//! enough to exercise the cache, similarity ranking, and hybrid search.

use std::sync::Arc;

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use super::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, Usage};

type Handler = dyn Fn(&CompletionRequest) -> Result<CompletionResponse, LlmError> + Send + Sync;

/// Test double for an LLM provider. Cheap to clone (wraps the handler in an
/// `Arc`), so you can share one instance across tasks.
#[derive(Clone)]
pub struct MockProvider {
    embedding_dim: usize,
    context_window: usize,
    handler: Arc<Handler>,
}

impl MockProvider {
    /// Construct a mock from a handler closure.
    pub fn with_handler<F>(handler: F) -> Self
    where
        F: Fn(&CompletionRequest) -> Result<CompletionResponse, LlmError> + Send + Sync + 'static,
    {
        Self {
            embedding_dim: 32,
            context_window: 200_000,
            handler: Arc::new(handler),
        }
    }

    /// Construct a mock from a substring-match table. Each completion request
    /// has its `system` + messages concatenated; the first fixture whose key
    /// is a substring of that text is returned. If no fixture matches, the
    /// mock returns [`LlmError::MockUnhandled`].
    pub fn with_fixtures(fixtures: impl IntoIterator<Item = (String, String)>) -> Self {
        let table: Vec<(String, String)> = fixtures.into_iter().collect();
        Self::with_handler(move |req| {
            let haystack = build_haystack(req);
            for (needle, text) in &table {
                if haystack.contains(needle) {
                    return Ok(CompletionResponse {
                        text: text.clone(),
                        usage: Usage {
                            input_tokens: haystack.len() as u64 / 4,
                            output_tokens: text.len() as u64 / 4,
                        },
                        model: "mock-1".into(),
                    });
                }
            }
            Err(LlmError::MockUnhandled)
        })
    }

    /// Return the same text for every request. Useful for single-call tests.
    pub fn constant(text: impl Into<String>) -> Self {
        let text = text.into();
        Self::with_handler(move |_| {
            Ok(CompletionResponse {
                text: text.clone(),
                usage: Usage::default(),
                model: "mock-1".into(),
            })
        })
    }
}

fn build_haystack(req: &CompletionRequest) -> String {
    let mut s = String::with_capacity(req.system.len() + 64);
    s.push_str(&req.system);
    for msg in &req.messages {
        s.push('\n');
        s.push_str(&msg.content);
    }
    s
}

#[async_trait]
impl LlmProvider for MockProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        (self.handler)(&req)
    }

    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        Ok(texts
            .iter()
            .map(|t| deterministic_embedding(t, self.embedding_dim))
            .collect())
    }

    fn name(&self) -> &'static str {
        "mock"
    }

    fn context_window(&self) -> usize {
        self.context_window
    }

    fn embedding_dim(&self) -> usize {
        self.embedding_dim
    }
}

/// Hash-derived unit vector. Deterministic per-text; good enough for the
/// cache tests but not semantically meaningful. Real tests against retrieval
/// quality use actual providers.
fn deterministic_embedding(text: &str, dim: usize) -> Vec<f32> {
    let mut out = Vec::with_capacity(dim);
    // Chain SHA-256 hashes so we can produce vectors of any dimension without
    // the output being dominated by the first 32 bytes.
    let mut seed = Sha256::digest(text.as_bytes()).to_vec();
    while out.len() < dim {
        for &byte in &seed {
            out.push((f32::from(byte) / 127.5) - 1.0);
            if out.len() == dim {
                break;
            }
        }
        seed = Sha256::digest(&seed).to_vec();
    }
    // Normalize to unit length so cosine similarity == dot product.
    let norm = out.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut out {
            *x /= norm;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(system: &str, user: &str) -> CompletionRequest {
        CompletionRequest::new(system).with_user(user)
    }

    #[tokio::test]
    async fn constant_provider_returns_fixed_text() {
        let mock = MockProvider::constant("hello world");
        let resp = mock.complete(req("system", "anything")).await.unwrap();
        assert_eq!(resp.text, "hello world");
        assert_eq!(mock.name(), "mock");
    }

    #[tokio::test]
    async fn handler_receives_request() {
        let mock = MockProvider::with_handler(|req| {
            Ok(CompletionResponse {
                text: format!("echo:{}", req.messages[0].content),
                usage: Usage::default(),
                model: "mock-1".into(),
            })
        });
        let resp = mock.complete(req("sys", "ping")).await.unwrap();
        assert_eq!(resp.text, "echo:ping");
    }

    #[tokio::test]
    async fn fixtures_match_on_substring() {
        let mock = MockProvider::with_fixtures([
            ("INGEST".to_string(), "ingest-response".to_string()),
            ("QUERY".to_string(), "query-response".to_string()),
        ]);
        let ingest = mock
            .complete(req("you are ingesting", "INGEST something"))
            .await
            .unwrap();
        assert_eq!(ingest.text, "ingest-response");
        let query = mock
            .complete(req("you are querying", "do a QUERY"))
            .await
            .unwrap();
        assert_eq!(query.text, "query-response");
    }

    #[tokio::test]
    async fn fixtures_error_when_no_match() {
        let mock = MockProvider::with_fixtures([("INGEST".to_string(), "x".to_string())]);
        let err = mock
            .complete(req("sys", "nothing relevant"))
            .await
            .unwrap_err();
        matches!(err, LlmError::MockUnhandled);
    }

    #[tokio::test]
    async fn embeddings_are_deterministic_and_unit_length() {
        let mock = MockProvider::constant("");
        let a = mock
            .embed(&["hello".to_string(), "world".to_string()])
            .await
            .unwrap();
        let b = mock
            .embed(&["hello".to_string(), "world".to_string()])
            .await
            .unwrap();
        assert_eq!(a, b);
        for v in &a {
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 1e-5, "norm = {norm}");
            assert_eq!(v.len(), mock.embedding_dim());
        }
        // Different inputs → different vectors.
        assert_ne!(a[0], a[1]);
    }
}
