//! Live integration tests for the Claude provider.
//!
//! These tests hit the real Anthropic API and are `#[ignore]`-gated so they
//! never run in CI or casual `cargo test`. Run them explicitly:
//!
//! ```sh
//! cargo test --test live_claude -- --ignored
//! ```
//!
//! Requires `SCRIPTORIUM_ANTHROPIC_API_KEY` in env or macOS keychain service
//! `scriptorium-anthropic`.

use scriptorium_core::llm::{
    ClaudeConfig, ClaudeProvider, CompletionRequest, IngestPlan, LlmError, LlmProvider,
};

/// Try to construct a provider; returns `None` (and prints a skip message) if
/// the API key isn't available so the test exits cleanly rather than panicking.
fn provider_or_skip() -> Option<ClaudeProvider> {
    let config = match ClaudeConfig::from_env() {
        Ok(c) => c,
        Err(_) => {
            eprintln!(
                "SKIP: SCRIPTORIUM_ANTHROPIC_API_KEY not available, skipping live Claude test"
            );
            return None;
        }
    };
    Some(ClaudeProvider::new(config).expect("ClaudeProvider builds from valid config"))
}

#[tokio::test]
#[ignore]
async fn complete_plain_text() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    let req = CompletionRequest::new("You are a helpful assistant.")
        .with_user("What is the capital of France? Reply in one word.")
        .with_max_tokens(64);

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
        "You are a wiki curator. Given a source text, produce an IngestPlan JSON object. \
         Required fields: \"summary\" (string), \"pages\" (array of page objects), \
         \"log_entry\" (string). Each page object MUST have: \"action\" (\"create\" or \
         \"update\"), \"path\" (vault-relative .md path under wiki/concepts/), \"title\" \
         (string), \"tags\" (array of strings), \"body\" (markdown string). \
         Produce exactly one page with action \"create\".",
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
async fn embed_returns_unsupported() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    let result = provider.embed(&["hello world".to_string()]).await;

    match result {
        Err(LlmError::Unsupported { provider: p, .. }) => {
            assert_eq!(p, "claude", "provider field should be 'claude'");
        }
        other => panic!("expected LlmError::Unsupported, got: {other:?}"),
    }
}

#[test]
#[ignore]
fn metadata_is_sane() {
    let Some(provider) = provider_or_skip() else {
        return;
    };

    assert_eq!(provider.name(), "claude");
    assert_eq!(provider.context_window(), 200_000);
    assert_eq!(provider.embedding_dim(), 0);
}
