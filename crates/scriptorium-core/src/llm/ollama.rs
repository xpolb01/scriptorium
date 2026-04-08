//! Ollama provider.
//!
//! Talks to a local `ollama serve` HTTP endpoint (default
//! `http://localhost:11434`). Chat requests use the `/api/chat` endpoint
//! with `format: "json"` for structured output — this is a best-effort hint
//! rather than strict-mode enforcement, so the caller still validates the
//! response against the schema on its side.
//!
//! Embeddings use `/api/embeddings`, one text per call. Default embed model
//! is `nomic-embed-text` (768 dim). Make sure it is pulled (`ollama pull
//! nomic-embed-text`) before running any query.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;

use super::retry::{with_retry, Retry};
use super::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, Role, Usage};

const DEFAULT_MODEL: &str = "llama3.1";
const DEFAULT_EMBED_MODEL: &str = "nomic-embed-text";
const DEFAULT_EMBED_DIM: usize = 768;
const DEFAULT_BASE_URL: &str = "http://localhost:11434";
const CONTEXT_WINDOW: usize = 128_000;
const DEFAULT_MAX_ATTEMPTS: u32 = 2;

#[derive(Debug, Clone)]
pub struct OllamaConfig {
    pub model: String,
    pub embed_model: String,
    pub embed_dim: usize,
    pub base_url: String,
    pub timeout: Duration,
    pub max_attempts: u32,
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            model: std::env::var("OLLAMA_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string()),
            embed_model: std::env::var("OLLAMA_EMBED_MODEL")
                .unwrap_or_else(|_| DEFAULT_EMBED_MODEL.to_string()),
            embed_dim: DEFAULT_EMBED_DIM,
            base_url: std::env::var("OLLAMA_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            timeout: Duration::from_secs(120),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        }
    }
}

pub struct OllamaProvider {
    config: OllamaConfig,
    client: Client,
}

impl OllamaProvider {
    pub fn new(config: OllamaConfig) -> Result<Self, LlmError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| LlmError::network("ollama", format!("reqwest builder: {e}")))?;
        Ok(Self { config, client })
    }
}

#[async_trait]
impl LlmProvider for OllamaProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let mut messages = vec![json!({"role": "system", "content": req.system})];
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
            "stream": false,
        });
        if req.response_schema.is_some() {
            // Best-effort structured output. Ollama understands
            // `format: "json"` but does not honour arbitrary JSON schemas.
            body["format"] = json!("json");
        }
        if let Some(temp) = req.temperature {
            body["options"] = json!({ "temperature": temp });
        }

        let url = format!("{}/api/chat", self.config.base_url);
        let parsed: ChatResponse =
            with_retry(self.config.max_attempts, self.config.timeout, || async {
                let resp = self
                    .client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| Retry::Transient(LlmError::network("ollama", e.to_string())))?;
                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(classify(
                        status,
                        LlmError::api("ollama", status.as_u16(), text),
                    ));
                }
                let parsed: ChatResponse = resp
                    .json()
                    .await
                    .map_err(|e| Retry::Permanent(LlmError::InvalidResponse(e.to_string())))?;
                Ok(parsed)
            })
            .await?;

        Ok(CompletionResponse {
            text: parsed.message.content,
            usage: Usage {
                input_tokens: parsed.prompt_eval_count.unwrap_or(0),
                output_tokens: parsed.eval_count.unwrap_or(0),
            },
            model: parsed.model,
        })
    }

    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        let mut out = Vec::with_capacity(texts.len());
        for text in texts {
            let url = format!("{}/api/embeddings", self.config.base_url);
            let body = json!({
                "model": self.config.embed_model,
                "prompt": text,
            });
            let parsed: EmbedResponse =
                with_retry(self.config.max_attempts, self.config.timeout, || async {
                    let resp = self
                        .client
                        .post(&url)
                        .json(&body)
                        .send()
                        .await
                        .map_err(|e| {
                            Retry::Transient(LlmError::network("ollama", e.to_string()))
                        })?;
                    let status = resp.status();
                    if !status.is_success() {
                        let text = resp.text().await.unwrap_or_default();
                        return Err(classify(
                            status,
                            LlmError::api("ollama", status.as_u16(), text),
                        ));
                    }
                    let parsed: EmbedResponse = resp
                        .json()
                        .await
                        .map_err(|e| Retry::Permanent(LlmError::InvalidResponse(e.to_string())))?;
                    Ok(parsed)
                })
                .await?;
            out.push(normalize(parsed.embedding));
        }
        Ok(out)
    }

    fn name(&self) -> &'static str {
        "ollama"
    }

    fn context_window(&self) -> usize {
        CONTEXT_WINDOW
    }

    fn embedding_dim(&self) -> usize {
        self.config.embed_dim
    }
}

fn classify(status: StatusCode, err: LlmError) -> Retry {
    if status.is_server_error() {
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
    message: OllamaMessage,
    prompt_eval_count: Option<u64>,
    eval_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OllamaMessage {
    content: String,
}

#[derive(Debug, Deserialize)]
struct EmbedResponse {
    embedding: Vec<f32>,
}
