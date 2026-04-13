//! Live integration tests for the Gemini provider.
//!
//! These tests hit the real Google Generative Language API and are
//! `#[ignore]`-gated so they never run in CI or casual `cargo test`.
//! Run them explicitly:
//!
//! ```sh
//! cargo test --test live_gemini -- --ignored
//! ```
//!
//! Requires `SCRIPTORIUM_GOOGLE_API_KEY` or `SCRIPTORIUM_GEMINI_API_KEY` in env, or macOS keychain
//! service `scriptorium-google`.

use scriptorium_core::llm::{
    CompletionRequest, GeminiConfig, GeminiProvider, IngestPlan, LlmProvider,
};

/// Try to construct a provider; returns `None` (and prints a skip message) if
/// the API key isn't available so the test exits cleanly rather than panicking.
fn provider_or_skip() -> Option<GeminiProvider> {
    let config = match GeminiConfig::from_env() {
        Ok(c) => c,
        Err(_) => {
            eprintln!("SKIP: SCRIPTORIUM_GOOGLE_API_KEY not available, skipping live Gemini test");
            return None;
        }
    };
    Some(GeminiProvider::new(config).expect("GeminiProvider builds from valid config"))
}

#[tokio::test]
#[ignore]
async fn complete_plain_text() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    let req = CompletionRequest::new("You are a helpful assistant.")
        .with_user("What is the capital of France? Reply in one word.")
        .with_max_tokens(256);

    let resp = provider.complete(req).await.expect("API call succeeds");

    assert!(!resp.text.is_empty(), "response text must not be empty");
    assert!(
        resp.text.to_lowercase().contains("paris"),
        "expected 'paris' in response, got: {}",
        resp.text
    );
    assert!(!resp.model.is_empty(), "should report model name");
}

#[tokio::test]
#[ignore]
async fn complete_structured_ingest_plan() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    let schema = IngestPlan::schema();
    let req = CompletionRequest::new(
        "You are a wiki curator. Given a source text, produce a JSON object with exactly these fields:\n\
         - \"summary\": a one-line human summary of what the source contained\n\
         - \"pages\": an array of page objects, each with:\n\
           - \"action\": either \"create\" or \"update\"\n\
           - \"path\": vault-relative path ending in .md, under wiki/concepts/\n\
           - \"title\": human-readable title\n\
           - \"tags\": array of tag strings\n\
           - \"body\": markdown body text\n\
         - \"log_entry\": a single-line log entry\n\n\
         Produce exactly one page to create. No extra fields.",
    )
    .with_user(
        "Source: The Transformer architecture was introduced in \
         'Attention Is All You Need' (Vaswani et al., 2017). It replaces \
         recurrence with multi-head self-attention, enabling parallel \
         processing of sequences.",
    )
    .with_max_tokens(1024)
    .with_response_schema(schema);

    let resp = provider.complete(req).await.expect("API call succeeds");

    let plan: IngestPlan =
        serde_json::from_str(&resp.text).expect("response should parse as IngestPlan");

    assert!(!plan.summary.is_empty(), "summary must not be empty");
    assert!(!plan.pages.is_empty(), "should produce at least one page");
    assert!(!plan.log_entry.is_empty(), "log_entry must not be empty");

    let page = &plan.pages[0];
    assert!(
        page.path.ends_with(".md"),
        "page path should end in .md, got: {}",
        page.path
    );
    assert!(!page.title.is_empty(), "page title must not be empty");
    assert!(!page.body.is_empty(), "page body must not be empty");
}

#[tokio::test]
#[ignore]
async fn embed_returns_correct_dimensions() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    let texts = vec![
        "The Transformer architecture uses self-attention.".to_string(),
        "BERT is a bidirectional encoder.".to_string(),
    ];

    let vectors = provider.embed(&texts).await.expect("embed succeeds");

    assert_eq!(vectors.len(), 2, "should return one vector per input");
    for (i, vec) in vectors.iter().enumerate() {
        assert_eq!(
            vec.len(),
            3072,
            "vector {i} should have 3072 dimensions, got {}",
            vec.len()
        );
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 0.01,
            "vector {i} should be unit-normalized, got norm={norm}"
        );
    }
}

#[tokio::test]
#[ignore]
async fn embed_single_text() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    let vectors = provider
        .embed(&["hello world".to_string()])
        .await
        .expect("embed succeeds");

    assert_eq!(vectors.len(), 1);
    assert_eq!(vectors[0].len(), 3072);
}

#[test]
#[ignore]
fn metadata_is_sane() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    assert_eq!(provider.name(), "gemini");
    assert_eq!(provider.context_window(), 1_048_576);
    assert_eq!(provider.embedding_dim(), 3072);
}
