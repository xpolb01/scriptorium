//! LLM provider abstraction.
//!
//! [`LlmProvider`] is the only seam between the core engine and an LLM API.
//! Real implementations (`claude`, `openai`, `ollama`) live alongside a
//! [`mock::MockProvider`] that replays fixed responses for tests and CI. Every
//! downstream module (ingest, query, lint-llm) takes `&dyn LlmProvider`, so
//! you can swap providers at runtime via config.
//!
//! Providers with native structured-output support (Anthropic tools, `OpenAI`
//! `response_format: json_schema`) should honour
//! [`CompletionRequest::response_schema`] and return text that already
//! validates against it. Providers without strict mode should pass the schema
//! into the prompt as a best-effort hint.

pub mod claude;
pub mod error;
pub mod gemini;
pub mod mock;
pub mod ollama;
pub mod openai;
pub mod prompts;
pub mod retry;
pub mod usage;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use claude::{ClaudeConfig, ClaudeProvider};
pub use error::LlmError;
pub use gemini::{GeminiConfig, GeminiProvider};
pub use mock::MockProvider;
pub use ollama::{OllamaConfig, OllamaProvider};
pub use openai::{OpenAiConfig, OpenAiProvider};
pub use prompts::{
    ingest_prompt, query_prompt, IngestAction, IngestPageAction, IngestPlan, PromptContext,
    QueryAnswer,
};
pub use usage::{estimate_cost, record_usage, UsageRecord};

/// A single message in a chat-style prompt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: String,
}

impl Message {
    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: text.into(),
        }
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: text.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

/// A request to [`LlmProvider::complete`]. Providers are free to translate the
/// (system, messages) pair into whatever their native API accepts.
#[derive(Debug, Clone)]
pub struct CompletionRequest {
    /// System / developer prompt — schema, rules, voice, output format hints.
    pub system: String,
    /// Conversation so far.
    pub messages: Vec<Message>,
    /// Upper bound on sampled output tokens.
    pub max_tokens: u32,
    /// Sampling temperature. `None` lets the provider choose its default.
    pub temperature: Option<f32>,
    /// Optional JSON Schema the response must validate against. Providers with
    /// strict structured-output enforce this natively; others use it as a
    /// prompt hint and the caller re-validates.
    pub response_schema: Option<serde_json::Value>,
}

impl CompletionRequest {
    pub fn new(system: impl Into<String>) -> Self {
        Self {
            system: system.into(),
            messages: Vec::new(),
            max_tokens: 4096,
            temperature: None,
            response_schema: None,
        }
    }

    #[must_use]
    pub fn with_user(mut self, text: impl Into<String>) -> Self {
        self.messages.push(Message::user(text));
        self
    }

    #[must_use]
    pub fn with_max_tokens(mut self, max_tokens: u32) -> Self {
        self.max_tokens = max_tokens;
        self
    }

    #[must_use]
    pub fn with_response_schema(mut self, schema: serde_json::Value) -> Self {
        self.response_schema = Some(schema);
        self
    }
}

/// A provider's response to a completion request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionResponse {
    /// The model's text output. If `response_schema` was set, this should be a
    /// JSON string that validates against the schema.
    pub text: String,
    pub usage: Usage,
    /// Model name the provider reports having served (may differ from the one
    /// requested if the provider routed to a fallback).
    pub model: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// The provider trait: `Send + Sync` so a single instance can be shared across
/// async tasks; object-safe so callers can hold `Arc<dyn LlmProvider>`.
#[async_trait]
pub trait LlmProvider: Send + Sync {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError>;

    /// Embed one or more texts, returning a unit-normalized vector per input.
    /// Dimensionality is provider-specific; the embeddings cache keys on
    /// `(provider, model, dim)` so callers don't accidentally mix vectors.
    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError>;

    /// Short human-readable name ("claude", "openai", "ollama", "mock").
    fn name(&self) -> &str;

    /// Maximum prompt + completion tokens the model accepts. Used by the
    /// schema renderer to stay under budget when injecting context.
    fn context_window(&self) -> usize;

    /// Dimensionality of vectors returned by [`Self::embed`].
    fn embedding_dim(&self) -> usize;
}

/// Helper: call [`LlmProvider::complete`] and deserialize the response as JSON
/// into `T`. Use this for operations whose prompt forces structured output.
pub async fn complete_as<T>(
    provider: &dyn LlmProvider,
    req: CompletionRequest,
) -> Result<T, LlmError>
where
    T: serde::de::DeserializeOwned,
{
    let resp = provider.complete(req).await?;
    serde_json::from_str(&resp.text)
        .map_err(|e| LlmError::InvalidResponse(format!("json parse: {e}")))
}
