//! Google Gemini provider.
//!
//! Implements [`LlmProvider`] against Google's Generative Language API:
//! `generateContent` for chat, `batchEmbedContents` (model
//! `gemini-embedding-2-preview`, 3072 dim) for embeddings. Gemini is the first
//! single-provider option for both chat and embeddings — Claude has no
//! embeddings API at all, and `OpenAI` / Ollama work but require a separate
//! API key. One `SCRIPTORIUM_GOOGLE_API_KEY` covers both the ingest loop and the query
//! loop end to end.
//!
//! **Structured output**: Gemini supports `responseSchema` but only a
//! subset of JSON Schema (no `$ref`, no `additionalProperties`, no
//! `$schema` meta-fields). `schemars`-generated schemas include those
//! fields, so sending them raw would fail. Instead we use JSON-mode
//! (`responseMimeType: "application/json"`) and rely on the existing
//! caller-side validation in `ingest::ingest` and `query::query`. If strict
//! enforcement becomes necessary, a schema-sanitizer that strips
//! `$schema`, `$ref`, `additionalProperties`, and `definitions` can be
//! added here later.
//!
//! **Auth**: the API key goes in the query string (`?key=...`), which is
//! how Google's Generative Language API expects it. This is fine for
//! scriptorium's use case (localhost / personal CLI) but you should not
//! proxy scriptorium's requests through a URL logger that captures query
//! strings — rotate the key if you do.

use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;

use super::retry::{with_retry, Retry};
use super::{CompletionRequest, CompletionResponse, LlmError, LlmProvider, Role, Usage};

/// Default chat model. `gemini-2.5-pro` is the latest stable Pro model
/// available via the Generative Language API as of writing — best suited
/// for the quality-sensitive ingest and query paths where we care more
/// about reasoning than latency. Override via `GEMINI_MODEL` env var; for
/// a cheaper/faster option try `gemini-2.5-flash` or `gemini-2.0-flash`.
const DEFAULT_MODEL: &str = "gemini-2.5-pro";
/// Default embedding model. `gemini-embedding-2-preview` is the latest Google
/// embedding model (Matryoshka-style: returns 3072 dimensions by default,
/// can be truncated to 256/512/768/1024/1536 via `outputDimensionality` if
/// smaller vectors are needed). We use the full 3072 for semantic quality.
/// Requires a Tier 3 (paid) API key for adequate rate limits.
const DEFAULT_EMBED_MODEL: &str = "gemini-embedding-2-preview";
const DEFAULT_EMBED_DIM: usize = 3072;
const DEFAULT_BASE_URL: &str = "https://generativelanguage.googleapis.com/v1beta";
// Gemini 2.5 Pro has a 1M-token context window.
const CONTEXT_WINDOW: usize = 1_048_576;
const DEFAULT_MAX_ATTEMPTS: u32 = 3;

#[derive(Debug, Clone)]
pub struct GeminiConfig {
    pub api_key: String,
    pub model: String,
    pub embed_model: String,
    pub embed_dim: usize,
    pub base_url: String,
    pub timeout: Duration,
    pub max_attempts: u32,
}

impl GeminiConfig {
    /// Read config from environment variables, falling back to the macOS
    /// keychain. Checks `SCRIPTORIUM_GOOGLE_API_KEY`, then `SCRIPTORIUM_GEMINI_API_KEY` env vars,
    /// then keychain service `scriptorium-google`.
    pub fn from_env() -> Result<Self, LlmError> {
        let api_key = std::env::var("SCRIPTORIUM_GOOGLE_API_KEY")
            .or_else(|_| std::env::var("SCRIPTORIUM_GEMINI_API_KEY"))
            .ok()
            .filter(|s| !s.is_empty())
            .or_else(|| crate::keychain::get_key(crate::keychain::services::GOOGLE))
            .ok_or_else(|| {
                LlmError::api(
                    "gemini",
                    0,
                    "SCRIPTORIUM_GOOGLE_API_KEY not found in env or keychain. \
                     Run `scriptorium setup` to configure.",
                )
            })?;
        Ok(Self {
            api_key,
            model: std::env::var("GEMINI_MODEL").unwrap_or_else(|_| DEFAULT_MODEL.to_string()),
            embed_model: std::env::var("GEMINI_EMBED_MODEL")
                .unwrap_or_else(|_| DEFAULT_EMBED_MODEL.to_string()),
            embed_dim: DEFAULT_EMBED_DIM,
            base_url: std::env::var("GEMINI_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            timeout: Duration::from_secs(120),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }
}

pub struct GeminiProvider {
    config: GeminiConfig,
    client: Client,
}

impl GeminiProvider {
    pub fn new(config: GeminiConfig) -> Result<Self, LlmError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| LlmError::network("gemini", format!("reqwest builder: {e}")))?;
        Ok(Self { config, client })
    }

    pub fn model(&self) -> &str {
        &self.config.model
    }
}

#[async_trait]
impl LlmProvider for GeminiProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        // Build the `contents` array: user/model turns. Gemini calls
        // assistant messages "model" and system prompts go in a separate
        // top-level `systemInstruction` field.
        let mut contents = Vec::with_capacity(req.messages.len());
        for m in &req.messages {
            contents.push(json!({
                "role": match m.role {
                    Role::User => "user",
                    Role::Assistant => "model",
                },
                "parts": [{"text": m.content}],
            }));
        }

        let mut generation_config = json!({
            "maxOutputTokens": req.max_tokens,
        });
        if let Some(temp) = req.temperature {
            generation_config["temperature"] = json!(temp);
        }
        // JSON mode: request that the response be valid JSON. We don't
        // attempt strict-mode `responseSchema` because schemars-generated
        // schemas include fields Gemini rejects (see the module docs).
        if req.response_schema.is_some() {
            generation_config["responseMimeType"] = json!("application/json");
        }

        let mut body = json!({
            "contents": contents,
            "generationConfig": generation_config,
        });
        if !req.system.is_empty() {
            body["systemInstruction"] = json!({ "parts": [{"text": req.system}] });
        }

        let url = format!(
            "{}/models/{}:generateContent?key={}",
            self.config.base_url, self.config.model, self.config.api_key
        );

        let parsed: GenerateContentResponse =
            with_retry(self.config.max_attempts, self.config.timeout, || async {
                let resp = self
                    .client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| Retry::Transient(LlmError::network("gemini", e.to_string())))?;
                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(classify(
                        status,
                        LlmError::api("gemini", status.as_u16(), text),
                    ));
                }
                let parsed: GenerateContentResponse = resp
                    .json()
                    .await
                    .map_err(|e| Retry::Permanent(LlmError::InvalidResponse(e.to_string())))?;
                Ok(parsed)
            })
            .await?;

        // Extract the text from the first candidate's first text part.
        let text = parsed
            .candidates
            .as_ref()
            .and_then(|c| c.first())
            .and_then(|c| c.content.as_ref())
            .map(|content| {
                content
                    .parts
                    .iter()
                    .filter_map(|p| p.text.as_deref())
                    .collect::<Vec<_>>()
                    .join("")
            })
            .ok_or_else(|| LlmError::InvalidResponse("no text in gemini response".into()))?;

        if text.is_empty() {
            return Err(LlmError::InvalidResponse(
                "gemini returned an empty text payload".into(),
            ));
        }

        let usage = parsed
            .usage_metadata
            .as_ref()
            .map(|u| Usage {
                input_tokens: u.prompt_token_count.unwrap_or(0),
                output_tokens: u.candidates_token_count.unwrap_or(0),
            })
            .unwrap_or_default();
        let model = parsed
            .model_version
            .unwrap_or_else(|| self.config.model.clone());
        Ok(CompletionResponse { text, usage, model })
    }

    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        // Gemini's batchEmbedContents takes one request per text, bundled.
        let model_path = format!("models/{}", self.config.embed_model);
        let requests: Vec<serde_json::Value> = texts
            .iter()
            .map(|t| {
                json!({
                    "model": model_path,
                    "content": {"parts": [{"text": t}]},
                })
            })
            .collect();
        let body = json!({ "requests": requests });
        let url = format!(
            "{}/models/{}:batchEmbedContents?key={}",
            self.config.base_url, self.config.embed_model, self.config.api_key
        );
        let parsed: BatchEmbedResponse =
            with_retry(self.config.max_attempts, self.config.timeout, || async {
                let resp = self
                    .client
                    .post(&url)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|e| Retry::Transient(LlmError::network("gemini", e.to_string())))?;
                let status = resp.status();
                if !status.is_success() {
                    let text = resp.text().await.unwrap_or_default();
                    return Err(classify(
                        status,
                        LlmError::api("gemini", status.as_u16(), text),
                    ));
                }
                let parsed: BatchEmbedResponse = resp
                    .json()
                    .await
                    .map_err(|e| Retry::Permanent(LlmError::InvalidResponse(e.to_string())))?;
                Ok(parsed)
            })
            .await?;
        let vectors = parsed
            .embeddings
            .into_iter()
            .map(|e| normalize(e.values))
            .collect();
        Ok(vectors)
    }

    fn name(&self) -> &'static str {
        "gemini"
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

// ---------- wire types ----------

#[derive(Debug, Deserialize)]
struct GenerateContentResponse {
    candidates: Option<Vec<Candidate>>,
    #[serde(rename = "usageMetadata")]
    usage_metadata: Option<UsageMetadata>,
    #[serde(rename = "modelVersion")]
    model_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Candidate {
    content: Option<CandidateContent>,
    // `finishReason`, `safetyRatings`, etc. exist but aren't surfaced.
}

#[derive(Debug, Deserialize)]
struct CandidateContent {
    #[serde(default)]
    parts: Vec<Part>,
}

#[derive(Debug, Deserialize)]
struct Part {
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct UsageMetadata {
    #[serde(rename = "promptTokenCount")]
    prompt_token_count: Option<u64>,
    #[serde(rename = "candidatesTokenCount")]
    candidates_token_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BatchEmbedResponse {
    embeddings: Vec<EmbedValues>,
}

#[derive(Debug, Deserialize)]
struct EmbedValues {
    values: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_429_is_transient() {
        let Retry::Transient(_) = classify(
            StatusCode::TOO_MANY_REQUESTS,
            LlmError::api("gemini", 429, "rate limit"),
        ) else {
            panic!("expected transient for 429");
        };
    }

    #[test]
    fn classify_5xx_is_transient() {
        let Retry::Transient(_) = classify(
            StatusCode::INTERNAL_SERVER_ERROR,
            LlmError::api("gemini", 500, "boom"),
        ) else {
            panic!("expected transient for 500");
        };
    }

    #[test]
    fn classify_400_is_permanent() {
        let Retry::Permanent(_) = classify(
            StatusCode::BAD_REQUEST,
            LlmError::api("gemini", 400, "bad request"),
        ) else {
            panic!("expected permanent for 400");
        };
    }

    #[test]
    fn normalize_unit_length() {
        let v = normalize(vec![3.0, 4.0]);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    #[test]
    fn normalize_zero_vector_unchanged() {
        let v = normalize(vec![0.0, 0.0, 0.0]);
        assert_eq!(v, vec![0.0, 0.0, 0.0]);
    }
}
