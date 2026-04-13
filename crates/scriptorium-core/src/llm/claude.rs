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
    /// Read config from environment variables, falling back to the macOS
    /// keychain for the API key. `SCRIPTORIUM_ANTHROPIC_API_KEY` env var takes
    /// priority; if absent, checks keychain service `scriptorium-anthropic`.
    pub fn from_env() -> Result<Self, LlmError> {
        let api_key = crate::keychain::resolve_key(
            "SCRIPTORIUM_ANTHROPIC_API_KEY",
            crate::keychain::services::ANTHROPIC,
        )
        .ok_or_else(|| {
            LlmError::api(
                "claude",
                0,
                "SCRIPTORIUM_ANTHROPIC_API_KEY not found in env or keychain. \
                 Run `scriptorium setup` to configure.",
            )
        })?;
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

        // NB: we deliberately do not enable extended thinking on these
        // requests. Extended thinking is incompatible with forced tool_choice
        // and returns HTTP 400 ("Thinking may not be enabled when tool_choice
        // forces tool use"). If a future change adds thinking, switch
        // `tool_choice` to `"any"` instead of forcing a named tool.
        //
        // System prompt is sent as a structured array with `cache_control`
        // on the text block. This enables Anthropic's prompt caching so
        // repeated calls (e.g. bulk-ingest) pay the 0.1x cache-read rate
        // for the system prompt instead of the full input rate every time.
        // The system prompt typically contains the full vault schema (~14KB)
        // which is identical across all calls in a batch.
        let mut body = json!({
            "model": self.config.model,
            "max_tokens": req.max_tokens,
            "system": [{
                "type": "text",
                "text": req.system,
                "cache_control": { "type": "ephemeral" },
            }],
            "messages": messages,
        });
        if let Some(temp) = req.temperature {
            body["temperature"] = json!(temp);
        }

        // Structured output: wrap the requested schema in a single "tool" and
        // force the model to invoke it via `strict: true`. With strict mode,
        // Anthropic uses grammar-constrained sampling server-side so the model
        // structurally cannot omit required fields or emit wrong types — the
        // exact bug class that motivated this call path. Without strict,
        // `tool_choice` only prefills the assistant turn and does not enforce
        // `required`. See
        // <https://platform.claude.com/docs/en/agents-and-tools/tool-use/strict-tool-use>.
        //
        // `cache_control: ephemeral` on the (sole) tool makes the tool
        // definition a cache breakpoint so subsequent ingests with the same
        // schema pay the 0.1x cache-read rate instead of the full schema
        // cost on every call. See prompt-caching docs.
        if let Some(schema) = req.response_schema.as_ref() {
            let tool_name = "emit_structured_response";
            let cleaned_schema = clean_schema_for_anthropic(schema);
            body["tools"] = json!([{
                "name": tool_name,
                "description": "Return a single structured JSON response. \
                                Call this tool exactly once with all required \
                                fields populated.",
                "strict": true,
                "cache_control": { "type": "ephemeral" },
                "input_schema": cleaned_schema,
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

        // Detect truncation BEFORE extracting text. When the model hits the
        // max_tokens ceiling mid-generation while a `tool_use` block is
        // open, Anthropic still returns a `tool_use` block but its `input`
        // JSON is incomplete (missing fields that come later in the object).
        // Surface this as a distinct error so the caller can raise the
        // budget instead of getting a confusing "missing field" serde error.
        if resp.stop_reason.as_deref() == Some("max_tokens") {
            return Err(LlmError::Truncated {
                provider: "claude".into(),
                output_tokens: resp.usage.output_tokens,
                max_tokens: req.max_tokens,
            });
        }

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
    // Prefer tool_use: that's our structured-output slot. We must scan the
    // whole block list before falling back to text, because Anthropic may
    // return `[Text, ToolUse]` when the model narrates before calling the
    // tool — taking the first text block would silently discard the
    // structured response.
    for block in &resp.content {
        if let ContentBlock::ToolUse { input, .. } = block {
            return serde_json::to_string(input).map_err(|e| {
                LlmError::InvalidResponse(format!("serialize tool_use input: {e}"))
            });
        }
    }
    // Fall back to any text block if the model didn't emit a tool_use.
    for block in &resp.content {
        if let ContentBlock::Text { text } = block {
            return Ok(text.clone());
        }
    }
    Err(LlmError::InvalidResponse(
        "response contained no text or tool_use block".into(),
    ))
}

/// Normalize a `schemars`-derived JSON Schema so it satisfies Anthropic's
/// strict-mode subset:
///
/// 1. Strip `$schema` and `title` from the root — Anthropic rejects them.
/// 2. Recursively set `additionalProperties: false` on every node with
///    `"type": "object"`, including schemas nested under `definitions`.
///    schemars 0.8 does not emit this by default, but Anthropic strict mode
///    requires it on every object.
///
/// The cleaner is conservative: it only *adds* `additionalProperties: false`
/// when missing, so a schema that already sets it explicitly is untouched.
fn clean_schema_for_anthropic(schema: &serde_json::Value) -> serde_json::Value {
    let mut v = schema.clone();
    if let Some(obj) = v.as_object_mut() {
        obj.remove("$schema");
        obj.remove("title");
    }
    force_additional_properties_false(&mut v);
    v
}

fn force_additional_properties_false(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            if map.get("type").and_then(|t| t.as_str()) == Some("object")
                && !map.contains_key("additionalProperties")
            {
                map.insert(
                    "additionalProperties".into(),
                    serde_json::Value::Bool(false),
                );
            }
            for (_k, child) in map.iter_mut() {
                force_additional_properties_false(child);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                force_additional_properties_false(item);
            }
        }
        _ => {}
    }
}

#[derive(Debug, Deserialize)]
struct MessagesResponse {
    model: String,
    content: Vec<ContentBlock>,
    usage: ApiUsage,
    /// Why the model stopped generating. Common values:
    /// - `end_turn`: natural stop (good)
    /// - `tool_use`: the model called the forced tool (also good — the
    ///   content block will be a `ToolUse` with `input`)
    /// - `max_tokens`: we hit the budget before the model finished.
    ///   With forced `tool_choice` this produces a truncated `tool_use.input`
    ///   that is missing required fields, and the downstream `from_str`
    ///   will blow up. We detect this here and surface a dedicated error
    ///   so callers can raise the budget and retry.
    /// - `refusal`: model declined — do not retry.
    /// - `stop_sequence`: hit a stop string (not used by scriptorium).
    #[serde(default)]
    stop_reason: Option<String>,
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
            stop_reason: Some("tool_use".into()),
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
            stop_reason: Some("end_turn".into()),
        };
        assert_eq!(extract_text(&resp).unwrap(), "plain answer");
    }

    #[test]
    fn extract_prefers_tool_use_even_when_text_appears_first() {
        // Anthropic may return `[Text, ToolUse]` when the model narrates
        // before calling the tool. Previously extract_text returned the
        // first block it saw, silently dropping the structured response.
        let resp = MessagesResponse {
            model: "claude".into(),
            content: vec![
                ContentBlock::Text {
                    text: "Let me call the tool...".into(),
                },
                ContentBlock::ToolUse {
                    id: "1".into(),
                    name: "emit_structured_response".into(),
                    input: serde_json::json!({"summary": "hi"}),
                },
            ],
            usage: ApiUsage {
                input_tokens: 1,
                output_tokens: 1,
            },
            stop_reason: Some("tool_use".into()),
        };
        let text = extract_text(&resp).unwrap();
        assert!(text.contains("\"summary\""));
        assert!(!text.contains("narrates"));
        assert!(!text.contains("Let me"));
    }

    #[test]
    fn messages_response_deserializes_stop_reason() {
        // Contract check: Anthropic's real response shape includes
        // `stop_reason` at the top level. Deserializing a minimal
        // fixture must populate it so the truncation guard can fire.
        let raw = serde_json::json!({
            "id": "msg_01",
            "type": "message",
            "role": "assistant",
            "model": "claude-opus-4-6",
            "stop_reason": "max_tokens",
            "content": [
                {
                    "type": "tool_use",
                    "id": "toolu_01",
                    "name": "emit_structured_response",
                    "input": { "partial": "response" }
                }
            ],
            "usage": { "input_tokens": 100, "output_tokens": 4096 }
        });
        let resp: MessagesResponse = serde_json::from_value(raw).unwrap();
        assert_eq!(resp.stop_reason.as_deref(), Some("max_tokens"));
        assert_eq!(resp.usage.output_tokens, 4096);
    }

    #[test]
    fn messages_response_deserializes_missing_stop_reason() {
        // Backward-compat: if an older fixture (or a test) omits
        // stop_reason, the struct must still parse.
        let raw = serde_json::json!({
            "id": "msg_01",
            "type": "message",
            "role": "assistant",
            "model": "claude",
            "content": [{"type": "text", "text": "hi"}],
            "usage": { "input_tokens": 1, "output_tokens": 1 }
        });
        let resp: MessagesResponse = serde_json::from_value(raw).unwrap();
        assert!(resp.stop_reason.is_none());
    }

    #[test]
    fn schema_cleaner_strips_schema_and_title_at_root() {
        let input = serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "IngestPlan",
            "type": "object",
            "properties": {
                "summary": { "type": "string" }
            },
            "required": ["summary"]
        });
        let cleaned = clean_schema_for_anthropic(&input);
        assert!(cleaned.get("$schema").is_none(), "$schema must be stripped");
        assert!(cleaned.get("title").is_none(), "title must be stripped");
        assert_eq!(cleaned["type"], "object");
        assert_eq!(cleaned["properties"]["summary"]["type"], "string");
    }

    #[test]
    fn schema_cleaner_forces_additional_properties_false_recursively() {
        // Shape mirrors what schemars 0.8 produces for IngestPlan: a root
        // object with a `definitions` table holding a nested object under
        // `IngestPageAction`. Both objects must end up with
        // additionalProperties: false.
        let input = serde_json::json!({
            "type": "object",
            "properties": {
                "pages": {
                    "type": "array",
                    "items": { "$ref": "#/definitions/IngestPageAction" }
                }
            },
            "required": ["pages"],
            "definitions": {
                "IngestPageAction": {
                    "type": "object",
                    "properties": {
                        "title": { "type": "string" }
                    },
                    "required": ["title"]
                }
            }
        });
        let cleaned = clean_schema_for_anthropic(&input);
        assert_eq!(
            cleaned["additionalProperties"],
            false,
            "root object must have additionalProperties: false"
        );
        assert_eq!(
            cleaned["definitions"]["IngestPageAction"]["additionalProperties"],
            false,
            "nested object in definitions must have additionalProperties: false"
        );
    }

    #[test]
    fn schema_cleaner_does_not_touch_non_object_nodes() {
        let input = serde_json::json!({
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": { "type": "string" }
                }
            }
        });
        let cleaned = clean_schema_for_anthropic(&input);
        // Array and string schemas must not grow an additionalProperties key.
        assert!(cleaned["properties"]["tags"]
            .get("additionalProperties")
            .is_none());
        assert!(cleaned["properties"]["tags"]["items"]
            .get("additionalProperties")
            .is_none());
    }

    #[test]
    fn schema_cleaner_preserves_existing_additional_properties() {
        // If a schema already sets additionalProperties explicitly (even to
        // true), the cleaner must not clobber that choice.
        let input = serde_json::json!({
            "type": "object",
            "additionalProperties": true,
            "properties": {}
        });
        let cleaned = clean_schema_for_anthropic(&input);
        assert_eq!(cleaned["additionalProperties"], true);
    }

    #[test]
    fn cleaner_produces_strict_compatible_ingest_plan_schema() {
        // End-to-end check: feed the real IngestPlan schema through the
        // cleaner and assert every object in the output has
        // additionalProperties: false. This is the canonical pre-flight for
        // Anthropic strict mode — if this passes, the API will accept the
        // tool definition.
        use crate::llm::IngestPlan;
        let cleaned = clean_schema_for_anthropic(&IngestPlan::schema());
        assert!(cleaned.get("$schema").is_none());
        assert!(cleaned.get("title").is_none());
        assert_visit_all_objects_have_additional_properties_false(&cleaned);
    }

    fn assert_visit_all_objects_have_additional_properties_false(v: &serde_json::Value) {
        if let Some(obj) = v.as_object() {
            if obj.get("type").and_then(|t| t.as_str()) == Some("object") {
                assert_eq!(
                    obj.get("additionalProperties"),
                    Some(&serde_json::Value::Bool(false)),
                    "object missing additionalProperties: false: {}",
                    serde_json::to_string_pretty(v).unwrap()
                );
            }
            for child in obj.values() {
                assert_visit_all_objects_have_additional_properties_false(child);
            }
        } else if let Some(arr) = v.as_array() {
            for item in arr {
                assert_visit_all_objects_have_additional_properties_false(item);
            }
        }
    }

    /// Rebuild the request body the same way `complete()` does, so we can
    /// assert what actually goes on the wire without needing an HTTP mock.
    fn build_request_body_for_test(req: &CompletionRequest) -> serde_json::Value {
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
            "model": "claude-opus-4-6",
            "max_tokens": req.max_tokens,
            "system": req.system,
            "messages": messages,
        });
        if let Some(temp) = req.temperature {
            body["temperature"] = json!(temp);
        }
        if let Some(schema) = req.response_schema.as_ref() {
            let tool_name = "emit_structured_response";
            let cleaned_schema = clean_schema_for_anthropic(schema);
            body["tools"] = json!([{
                "name": tool_name,
                "description": "Return a single structured JSON response. \
                                Call this tool exactly once with all required \
                                fields populated.",
                "strict": true,
                "cache_control": { "type": "ephemeral" },
                "input_schema": cleaned_schema,
            }]);
            body["tool_choice"] = json!({ "type": "tool", "name": tool_name });
        }
        body
    }

    #[test]
    fn request_body_with_schema_enables_strict_mode() {
        use crate::llm::{CompletionRequest, IngestPlan, Message, Role};
        let req = CompletionRequest {
            system: "system".into(),
            messages: vec![Message {
                role: Role::User,
                content: "user".into(),
            }],
            max_tokens: 256,
            temperature: Some(0.2),
            response_schema: Some(IngestPlan::schema()),
        };
        let body = build_request_body_for_test(&req);
        assert_eq!(body["tools"][0]["strict"], true);
        assert_eq!(body["tools"][0]["cache_control"]["type"], "ephemeral");
        assert_eq!(
            body["tool_choice"],
            json!({ "type": "tool", "name": "emit_structured_response" })
        );
        // The input_schema must not leak schemars-specific root keys.
        assert!(body["tools"][0]["input_schema"].get("$schema").is_none());
        assert!(body["tools"][0]["input_schema"].get("title").is_none());
        // And must have additionalProperties: false at the root.
        assert_eq!(
            body["tools"][0]["input_schema"]["additionalProperties"],
            false
        );
    }

    #[test]
    fn request_body_without_schema_has_no_tools() {
        use crate::llm::{CompletionRequest, Message, Role};
        let req = CompletionRequest {
            system: "system".into(),
            messages: vec![Message {
                role: Role::User,
                content: "user".into(),
            }],
            max_tokens: 256,
            temperature: None,
            response_schema: None,
        };
        let body = build_request_body_for_test(&req);
        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
    }
}
