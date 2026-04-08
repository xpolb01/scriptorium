//! Anthropic Claude provider.
//!
//! Implements [`LlmProvider`] against the Messages API
//! (<https://docs.anthropic.com/en/api/messages>). The key trick here is
//! **tool-use for structured output**: when the caller supplies a JSON
//! Schema in [`CompletionRequest::response_schema`], we translate it into an
//! Anthropic "tool" with that schema, force the model to call it via
//! `tool_choice`, and return the tool's `input` as JSON text. That gives us
//! the same interface as providers with a native `response_format` field.
//!
//! Embeddings are not supported — Anthropic has no public embeddings API as
//! of this writing — so [`ClaudeProvider::embed`] always returns
//! [`LlmError::Unsupported`]. Callers should wire a separate embeddings
//! provider (`OpenAI` or Ollama) for the vector pipeline.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::retry::{with_retry, Retry};
use super::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, Role, Usage};

const DEFAULT_MODEL: &str = "claude-opus-4-6";
const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";
const API_VERSION: &str = "2023-06-01";
const CONTEXT_WINDOW: usize = 200_000;
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

/// Config for constructing a [`ClaudeProvider`]. All fields are optional and
/// fall back to env vars or sensible defaults.
#[derive(Debug, Clone)]
pub struct ClaudeConfig {
    pub api_key: String,
    pub model: String,
    pub base_url: String,
    pub timeout: Duration,
    pub max_attempts: u32,
}

impl ClaudeConfig {
    /// Read config from environment variables. `ANTHROPIC_API_KEY` is
    /// required; everything else has a default.
    pub fn from_env() -> Result<Self, LlmError> {
        let api_key = std::env::var("ANTHROPIC_API_KEY")
            .map_err(|_| LlmError::api("claude", 0, "ANTHROPIC_API_KEY env var is not set"))?;
        Ok(Self {
            api_key,
            model: std::env::var("ANTHROPIC_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string()),
            base_url: std::env::var("ANTHROPIC_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            timeout: Duration::from_secs(120),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }
}

pub struct ClaudeProvider {
    config: ClaudeConfig,
    client: Client,
}

impl ClaudeProvider {
    pub fn new(config: ClaudeConfig) -> Result<Self, LlmError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| LlmError::network("claude", format!("reqwest builder: {e}")))?;
        Ok(Self { config, client })
    }

    pub fn model(&self) -> &str {
        &self.config.model
    }
}

#[async_trait]
impl LlmProvider for ClaudeProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let messages = req
            .messages
            .iter()
            .map(|m| {
                json!({
                    "role": match m.role {
                        Role::User => "user",
                        Role::Assistant => "assistant",
                    },
                    "content": m.content,
                })
            })
            .collect::<Vec<_>>();

        let mut body = json!({
            "model": self.config.model,
            "max_tokens": req.max_tokens,
            "system": req.system,
            "messages": messages,
        });
        if let Some(temp) = req.temperature {
            body["temperature"] = json!(temp);
        }

        // Structured output: wrap the requested schema in a single "tool" and
        // force the model to invoke it. The tool's `input` then *is* our JSON
        // response.
        if let Some(schema) = req.response_schema.as_ref() {
            let tool_name = "emit_structured_response";
            body["tools"] = json!([{
                "name": tool_name,
                "description": "Return a single structured JSON response.",
                "input_schema": schema,
            }]);
            body["tool_choice"] = json!({ "type": "tool", "name": tool_name });
        }

        let url = format!("{}/v1/messages", self.config.base_url);
        let resp: MessagesResponse =
            with_retry(self.config.max_attempts, self.config.timeout, || async {
                let resp = self
                    .client
                    .post(&url)
                    .header("x-api-key", &self.config.api_key)
                    .header("anthropic-version", API_VERSION)
                    .header("content-type", "application/json")
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| Retry::Transient(LlmError::network("claude", e.to_string())))?;

                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    let err = LlmError::api("claude", status.as_u16(), text);
                    return Err(classify(status, err));
                }
                let parsed: MessagesResponse = resp
                    .json()
                    .await
                    .map_err(|e| Retry::Permanent(LlmError::InvalidResponse(e.to_string())))?;
                Ok(parsed)
            })
            .await?;

        // Extract text: prefer tool_use.input (structured), fall back to text blocks.
        let text = extract_text(&resp)?;
        let usage = Usage {
            input_tokens: resp.usage.input_tokens,
            output_tokens: resp.usage.output_tokens,
        };
        Ok(CompletionResponse {
            text,
            usage,
            model: resp.model,
        })
    }

    async fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        Err(LlmError::unsupported("claude", "embeddings"))
    }

    fn name(&self) -> &'static str {
        "claude"
    }

    fn context_window(&self) -> usize {
        CONTEXT_WINDOW
    }

    fn embedding_dim(&self) -> usize {
        0
    }
}

fn classify(status: StatusCode, err: LlmError) -> Retry {
    if status.as_u16() == 429 || status.is_server_error() {
        Retry::Transient(err)
    } else {
        Retry::Permanent(err)
    }
}

fn extract_text(resp: &MessagesResponse) -> Result<String, LlmError> {
    for block in &resp.content {
        match block {
            ContentBlock::ToolUse { input, .. } => {
                return serde_json::to_string(input).map_err(|e| {
                    LlmError::InvalidResponse(format!("serialize tool_use input: {e}"))
                });
            }
            ContentBlock::Text { text } => {
                // Keep as a fallback if no tool_use block appears.
                return Ok(text.clone());
            }
            ContentBlock::Unknown => {}
        }
    }
    Err(LlmError::InvalidResponse(
        "response contained no text or tool_use block".into(),
    ))
}

#[derive(Debug, Deserialize)]
struct MessagesResponse {
    model: String,
    content: Vec<ContentBlock>,
    usage: ApiUsage,
}

#[derive(Debug, Deserialize)]
struct ApiUsage {
    input_tokens: u64,
    output_tokens: u64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(clippy::enum_variant_names)]
enum ContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    #[serde(rename = "tool_use")]
    ToolUse {
        #[allow(dead_code)]
        id: String,
        #[allow(dead_code)]
        name: String,
        input: serde_json::Value,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)] // documents the request shape for reference
struct MessageRequest<'a> {
    model: &'a str,
    max_tokens: u32,
    system: &'a str,
    messages: &'a [serde_json::Value],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_429_is_transient() {
        let Retry::Transient(_) = classify(
            StatusCode::TOO_MANY_REQUESTS,
            LlmError::api("claude", 429, "rate limit"),
        ) else {
            panic!("expected transient for 429");
        };
    }

    #[test]
    fn classify_5xx_is_transient() {
        let Retry::Transient(_) = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            LlmError::api("claude", 500, "server error"),
        ) else {
            panic!("expected transient for 500");
        };
    }

    #[test]
    fn classify_401_is_permanent() {
        let Retry::Permanent(_) = classify(
            StatusCode::UNAUTHORIZED,
            LlmError::api("claude", 401, "auth"),
        ) else {
            panic!("expected permanent for 401");
        };
    }

    #[test]
    fn extract_prefers_tool_use_block() {
        let resp = MessagesResponse {
            model: "claude".into(),
            content: vec![ContentBlock::ToolUse {
                id: "1".into(),
                name: "emit".into(),
                input: serde_json::json!({"hello": "world"}),
            }],
            usage: ApiUsage {
                input_tokens: 1,
                output_tokens: 1,
            },
        };
        let text = extract_text(&resp).unwrap();
        assert!(text.contains("\"hello\""));
    }

    #[test]
    fn extract_falls_back_to_text_block() {
        let resp = MessagesResponse {
            model: "claude".into(),
            content: vec![ContentBlock::Text {
                text: "plain answer".into(),
            }],
            usage: ApiUsage {
                input_tokens: 1,
                output_tokens: 1,
            },
        };
        assert_eq!(extract_text(&resp).unwrap(), "plain answer");
    }
}
