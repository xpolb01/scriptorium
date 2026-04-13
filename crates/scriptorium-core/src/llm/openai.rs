//! `OpenAI` provider.
//!
//! Implements [`LlmProvider`] against the Chat Completions and Embeddings
//! APIs. Structured output uses `OpenAI`'s native
//! `response_format: { type: "json_schema", ... }`, which has strict mode
//! turned on by default so the model's output is guaranteed to validate
//! against the schema scriptorium sent.
//!
//! Embeddings default to `text-embedding-3-small` (1536 dim) — the
//! pragmatic default for cost and speed. A future config knob can change
//! that without touching the code here.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;

use super::retry::{with_retry, Retry};
use super::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, Role, Usage};

const DEFAULT_MODEL: &str = "gpt-4o-mini";
const DEFAULT_EMBED_MODEL: &str = "text-embedding-3-small";
const DEFAULT_EMBED_DIM: usize = 1536;
const DEFAULT_BASE_URL: &str = "https://api.openai.com";
const CONTEXT_WINDOW: usize = 128_000;
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone)]
pub struct OpenAiConfig {
    pub api_key: String,
    pub model: String,
    pub embed_model: String,
    pub embed_dim: usize,
    pub base_url: String,
    pub timeout: Duration,
    pub max_attempts: u32,
}

impl OpenAiConfig {
    pub fn from_env() -> Result<Self, LlmError> {
        let api_key = crate::keychain::resolve_key(
            "OPENAI_API_KEY",
            crate::keychain::services::OPENAI,
        )
        .ok_or_else(|| {
            LlmError::api(
                "openai",
                0,
                "OPENAI_API_KEY not found in env or keychain. \
                 Run `scriptorium setup` to configure.",
            )
        })?;
        Ok(Self {
            api_key,
            model: std::env::var("OPENAI_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string()),
            embed_model: std::env::var("OPENAI_EMBED_MODEL")
                .unwrap_or_else(|_| DEFAULT_EMBED_MODEL.to_string()),
            embed_dim: DEFAULT_EMBED_DIM,
            base_url: std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            timeout: Duration::from_secs(120),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }
}

pub struct OpenAiProvider {
    config: OpenAiConfig,
    client: Client,
}

impl OpenAiProvider {
    pub fn new(config: OpenAiConfig) -> Result<Self, LlmError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| LlmError::network("openai", format!("reqwest builder: {e}")))?;
        Ok(Self { config, client })
    }

    pub fn model(&self) -> &str {
        &self.config.model
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let mut messages = vec![json!({
            "role": "system",
            "content": req.system,
        })];
        for m in &req.messages {
            messages.push(json!({
                "role": match m.role {
                    Role::User => "user",
                    Role::Assistant => "assistant",
                },
                "content": m.content,
            }));
        }
        let mut body = json!({
            "model": self.config.model,
            "messages": messages,
            "max_tokens": req.max_tokens,
        });
        if let Some(temp) = req.temperature {
            body["temperature"] = json!(temp);
        }
        if let Some(schema) = req.response_schema.as_ref() {
            body["response_format"] = json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "structured_response",
                    "strict": true,
                    "schema": schema,
                },
            });
        }

        let url = format!("{}/v1/chat/completions", self.config.base_url);
        let parsed: ChatResponse =
            with_retry(self.config.max_attempts, self.config.timeout, || async {
                let resp = self
                    .client
                    .post(&url)
                    .bearer_auth(&self.config.api_key)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| Retry::Transient(LlmError::network("openai", e.to_string())))?;
                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(classify(
                        status,
                        LlmError::api("openai", status.as_u16(), text),
                    ));
                }
                let parsed: ChatResponse = resp
                    .json()
                    .await
                    .map_err(|e| Retry::Permanent(LlmError::InvalidResponse(e.to_string())))?;
                Ok(parsed)
            })
            .await?;

        let choice = parsed
            .choices
            .into_iter()
            .next()
            .ok_or_else(|| LlmError::InvalidResponse("no choices returned".into()))?;
        Ok(CompletionResponse {
            text: choice.message.content.unwrap_or_default(),
            usage: Usage {
                input_tokens: parsed.usage.as_ref().map_or(0, |u| u.prompt_tokens),
                output_tokens: parsed.usage.as_ref().map_or(0, |u| u.completion_tokens),
            },
            model: parsed.model,
        })
    }

    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let url = format!("{}/v1/embeddings", self.config.base_url);
        let body = json!({
            "model": self.config.embed_model,
            "input": texts,
        });
        let parsed: EmbeddingsResponse =
            with_retry(self.config.max_attempts, self.config.timeout, || async {
                let resp = self
                    .client
                    .post(&url)
                    .bearer_auth(&self.config.api_key)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| Retry::Transient(LlmError::network("openai", e.to_string())))?;
                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(classify(
                        status,
                        LlmError::api("openai", status.as_u16(), text),
                    ));
                }
                let parsed: EmbeddingsResponse = resp
                    .json()
                    .await
                    .map_err(|e| Retry::Permanent(LlmError::InvalidResponse(e.to_string())))?;
                Ok(parsed)
            })
            .await?;
        let vecs: Vec<Vec<f32>> = parsed
            .data
            .into_iter()
            .map(|d| normalize(d.embedding))
            .collect();
        Ok(vecs)
    }

    fn name(&self) -> &'static str {
        "openai"
    }

    fn context_window(&self) -> usize {
        CONTEXT_WINDOW
    }

    fn embedding_dim(&self) -> usize {
        self.config.embed_dim
    }
}

fn classify(status: StatusCode, err: LlmError) -> Retry {
    if status.as_u16() == 429 || status.is_server_error() {
        Retry::Transient(err)
    } else {
        Retry::Permanent(err)
    }
}

fn normalize(mut v: Vec<f32>) -> Vec<f32> {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut v {
            *x /= norm;
        }
    }
    v
}

#[derive(Debug, Deserialize)]
struct ChatResponse {
    model: String,
    choices: Vec<Choice>,
    usage: Option<ChatUsage>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: ChoiceMessage,
}

#[derive(Debug, Deserialize)]
struct ChoiceMessage {
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChatUsage {
    prompt_tokens: u64,
    completion_tokens: u64,
}

#[derive(Debug, Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingDatum>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingDatum {
    embedding: Vec<f32>,
}
