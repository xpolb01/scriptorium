//! `OpenAI` provider.
//!
//! Implements [`LlmProvider`] against the Chat Completions and Embeddings
//! APIs. Structured output uses `OpenAI`'s native
//! `response_format: { type: "json_schema", ... }`, which has strict mode
//! turned on by default so the model's output is guaranteed to validate
//! against the schema scriptorium sent.
//!
//! OpenAI-compatible proxies in front of Anthropic models (`LiteLLM` →
//! Vertex) translate `response_format` into a *forced* tool call. Models
//! that refuse forced tool choice (Claude Opus 5.5 answers HTTP 400:
//! `tool_choice: type "tool" and "any" are not supported for this model`)
//! would fail every structured call. On that specific rejection the
//! provider re-sends the request with the JSON Schema written into the
//! system prompt instead, and stays in that mode for the rest of the
//! process. Nothing enforces the shape in that mode, so callers parse the
//! reply with [`super::extract_json_payload`] like every other provider's.
//!
//! Embeddings default to `text-embedding-3-small` (1536 dim) — the
//! pragmatic default for cost and speed. A future config knob can change
//! that without touching the code here.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{json, Value};

use super::retry::{with_retry, with_retry_until, Retry};
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
        let api_key =
            crate::keychain::resolve_key("OPENAI_API_KEY", crate::keychain::services::OPENAI)
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
            embed_dim: std::env::var("OPENAI_EMBED_DIM")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_EMBED_DIM),
            base_url: std::env::var("OPENAI_BASE_URL")
                .unwrap_or_else(|_| DEFAULT_BASE_URL.to_string()),
            timeout: Duration::from_mins(2),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
        })
    }
}

pub struct OpenAiProvider {
    config: OpenAiConfig,
    client: Client,
    /// Set once the backend rejected a forced tool call for
    /// `response_format`; from then on schemas travel in the system prompt.
    schema_in_prompt: AtomicBool,
}

impl OpenAiProvider {
    pub fn new(config: OpenAiConfig) -> Result<Self, LlmError> {
        let client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| LlmError::network("openai", format!("reqwest builder: {e}")))?;
        Ok(Self {
            config,
            client,
            schema_in_prompt: AtomicBool::new(false),
        })
    }

    pub fn model(&self) -> &str {
        &self.config.model
    }

    /// One Chat Completions call with retries. `max_attempts` is what is left
    /// of the caller's attempt allowance and `deadline` the caller's absolute
    /// deadline; every HTTP attempt made is counted in `attempts`.
    async fn send_chat(
        &self,
        req: &CompletionRequest,
        schema_in_prompt: bool,
        max_attempts: u32,
        deadline: tokio::time::Instant,
        attempts: &AtomicU32,
    ) -> Result<CompletionResponse, LlmError> {
        // Serialize once, then measure the budget right before the first
        // attempt so body preparation cannot run past the deadline.
        let body = serde_json::to_vec(&chat_body(&self.config.model, req, schema_in_prompt))
            .map_err(|e| LlmError::InvalidResponse(format!("request body: {e}")))?;
        let url = format!("{}/v1/chat/completions", self.config.base_url);
        if tokio::time::Instant::now() >= deadline {
            return Err(LlmError::Timeout {
                provider: "openai".into(),
                seconds: self.config.timeout.as_secs(),
            });
        }
        let parsed: ChatResponse =
            with_retry_until(max_attempts, deadline, self.config.timeout, || async {
                attempts.fetch_add(1, Ordering::Relaxed);
                let resp = self
                    .client
                    .post(&url)
                    .bearer_auth(&self.config.api_key)
                    .header(reqwest::header::CONTENT_TYPE, "application/json")
                    .body(body.clone())
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
        let output_tokens = parsed.usage.as_ref().map_or(0, |u| u.completion_tokens);
        let mut text = choice.message.content.unwrap_or_default();
        if let (true, Some(schema)) = (schema_in_prompt, req.response_schema.as_ref()) {
            // Nothing upstream enforces the schema in this mode, so enforce
            // what callers cannot check: one complete JSON value with the
            // schema's root type, or an error. Callers never see a fragment.
            // A reply cut off at the token limit is rejected with its text,
            // like any other invalid reply, so callers can record it.
            let checked = if choice.finish_reason.as_deref() == Some("length") {
                Err(format!(
                    "reply cut off at the {}-token limit ({output_tokens} output tokens)",
                    req.max_tokens
                ))
            } else {
                validated_json(&text, schema)
            };
            text = checked.map_err(|reason| LlmError::InvalidStructuredReply {
                reason,
                reply: super::error::RedactedReply::new(text.clone()),
                usage: Usage {
                    input_tokens: parsed.usage.as_ref().map_or(0, |u| u.prompt_tokens),
                    output_tokens,
                },
                model: parsed.model.clone(),
            })?;
        }
        Ok(CompletionResponse {
            text,
            usage: Usage {
                input_tokens: parsed.usage.as_ref().map_or(0, |u| u.prompt_tokens),
                output_tokens,
            },
            model: parsed.model,
        })
    }
}

#[async_trait]
impl LlmProvider for OpenAiProvider {
    async fn complete(&self, req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let structured = req.response_schema.is_some();
        let in_prompt = structured && self.schema_in_prompt.load(Ordering::Relaxed);
        // One deadline and one attempt allowance cover both formats.
        let deadline = tokio::time::Instant::now() + self.config.timeout;
        let attempts = AtomicU32::new(0);
        let first = self
            .send_chat(
                &req,
                in_prompt,
                self.config.max_attempts,
                deadline,
                &attempts,
            )
            .await;
        match first {
            Err(e) if structured && !in_prompt && rejects_forced_tool_choice(&e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                // The switch spends the same attempt allowance as everything
                // else; with none left, the rejection stands.
                let left = self
                    .config
                    .max_attempts
                    .saturating_sub(attempts.load(Ordering::Relaxed));
                if left == 0 {
                    return Err(e);
                }
                let second = self.send_chat(&req, true, left, deadline, &attempts).await;
                // Only a validated reply proves the switch; any error (empty,
                // prose, unfinished or wrong-shape reply) leaves the next
                // call strict.
                if second.is_ok() && !self.schema_in_prompt.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        model = %self.config.model,
                        "backend rejects forced tool choice for response_format; \
                         sending JSON schemas in the system prompt for this process"
                    );
                }
                second
            }
            other => other,
        }
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

/// Build the Chat Completions body. With `schema_in_prompt`, a request's
/// `response_schema` is written into the system prompt instead of being sent
/// as `response_format`.
fn chat_body(model: &str, req: &CompletionRequest, schema_in_prompt: bool) -> Value {
    let schema = req.response_schema.as_ref();
    let system = match schema {
        Some(s) if schema_in_prompt => format!("{}{}", req.system, schema_instruction(s)),
        _ => req.system.clone(),
    };
    let mut messages = vec![json!({
        "role": "system",
        "content": system,
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
        "model": model,
        "messages": messages,
        "max_tokens": req.max_tokens,
    });
    if let Some(temp) = req.temperature {
        body["temperature"] = json!(temp);
    }
    if let (Some(schema), false) = (schema, schema_in_prompt) {
        body["response_format"] = json!({
            "type": "json_schema",
            "json_schema": {
                "name": "structured_response",
                "strict": true,
                "schema": schema,
            },
        });
    }
    body
}

/// The system-prompt suffix that replaces `response_format`. Static per
/// schema, so it stays inside a cached system-prompt prefix.
fn schema_instruction(schema: &Value) -> String {
    let pretty = serde_json::to_string_pretty(schema).unwrap_or_else(|_| schema.to_string());
    format!(
        "\n\n## Output format\n\n\
         Respond with exactly one JSON value that validates against the JSON \
         Schema below. Output only that value: no prose before or after it \
         and no markdown fence.\n\n{pretty}\n"
    )
}

/// True for the backend's rejection of a forced tool call, which is how
/// OpenAI-compatible proxies deliver `response_format` to Anthropic models.
/// The diagnostic must attribute the refusal to `tool_choice` itself (the
/// nested, JSON-escaped `LiteLLM` message included); a 400 about another
/// parameter that merely mentions `tool_choice` elsewhere does not count.
fn rejects_forced_tool_choice(err: &LlmError) -> bool {
    use std::sync::OnceLock;
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let LlmError::Api {
        status: 400,
        message,
        ..
    } = err
    else {
        return false;
    };
    // The diagnostic must *be* the refusal, not merely mention it.
    let re = RE.get_or_init(|| {
        regex::Regex::new(concat!(
            r"^(?:",
            // Anthropic: tool_choice: type "tool" and "any" are not supported ...
            r"tool_choice:? type (?:tool|any)(?: (?:and|or) (?:tool|any))? (?:is|are) (?:not supported|unsupported)",
            // Other wordings: (forced) tool_choice is not supported / unsupported
            r"|(?:forced )?tool_choice (?:is )?(?:not supported|unsupported)",
            // Anthropic with thinking on
            r"|thinking may not be enabled when tool_choice forces tool use",
            r")",
        ))
        .expect("forced tool choice regex is valid")
    });
    let diagnostics = match serde_json::from_str::<Value>(message) {
        Ok(v) => known_diagnostics(&v),
        Err(_) => vec![message.clone()],
    };
    diagnostics.iter().any(|d| {
        let normalized: String = d
            .trim()
            .to_ascii_lowercase()
            .chars()
            .filter(|c| *c != '\\' && *c != '"')
            .collect();
        re.is_match(&normalized)
    })
}

/// `LiteLLM`'s provider-exception envelope: `litellm.BadRequestError:
/// Vertex_aiException BadRequestError - {...}. Received Model Group=...`.
fn litellm_envelope() -> &'static regex::Regex {
    use std::sync::OnceLock;
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(r"(?s)^litellm\.[A-Za-z]+Error: [A-Za-z_]+Exception [A-Za-z]+ - (\{.*)$")
            .expect("litellm envelope regex is valid")
    })
}

/// The diagnostic strings of a JSON error envelope. `LiteLLM` wraps the
/// upstream error as `litellm.<Kind>Error: <Provider>Exception <Kind> - {...}`;
/// that envelope, and only that one, is replaced by the upstream error's own
/// diagnostics, so text echoed beside them never counts.
fn known_diagnostics(v: &Value) -> Vec<String> {
    let mut out = Vec::new();
    for ptr in ["/error/message", "/error", "/message", "/detail"] {
        let Some(text) = v.pointer(ptr).and_then(Value::as_str) else {
            continue;
        };
        // The upstream JSON is followed by LiteLLM's own suffix (". Received
        // Model Group=..."), so parse just the first value.
        let upstream = litellm_envelope()
            .captures(text)
            .and_then(|c| {
                serde_json::Deserializer::from_str(c.get(1)?.as_str())
                    .into_iter::<Value>()
                    .next()?
                    .ok()
            })
            .map(|inner| known_diagnostics(&inner))
            .filter(|d| !d.is_empty());
        match upstream {
            Some(inner) => out.extend(inner),
            None => out.push(text.to_string()),
        }
    }
    out
}

/// The reply as one complete JSON value whose root type (object or array)
/// matches the schema's, or why not. The whole reply must be that value,
/// optionally wrapped in one markdown fence: an opening line of only
/// ```` ``` ```` plus a language tag, and a closing ```` ``` ```` line that
/// ends the reply. Prose around it, a second value, or an unfinished value
/// is rejected rather than mined for fragments. Deeper shape (required keys)
/// is left to callers, as in strict mode: ingest salvages a missing
/// `log_entry`. The reason never quotes the reply, which may quote vault text.
fn validated_json(text: &str, schema: &Value) -> Result<String, String> {
    let t = text.trim();
    let t = if t.starts_with("```") {
        let (open, rest) = t.split_once('\n').ok_or("fence without a body")?;
        let tag = open[3..].trim_end_matches('\r');
        if !tag.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err("text after the opening fence".into());
        }
        let (body, close) = rest
            .rsplit_once('\n')
            .ok_or("fence without a closing line")?;
        if close.trim() != "```" {
            return Err("no closing fence line at the end".into());
        }
        body.trim()
    } else {
        t
    };
    let value: Value = serde_json::from_str(t).map_err(|e| {
        format!(
            "not one complete JSON value ({} bytes; {:?} at line {} column {})",
            t.len(),
            e.classify(),
            e.line(),
            e.column()
        )
    })?;
    if !root_type_matches(&value, t, schema.get("type")) {
        return Err("root type does not match the schema".into());
    }
    Ok(t.to_string())
}

/// Whether `value` (parsed from `token`) has one of the JSON Schema `type`s
/// declared (a name or a list of names); a schema without `type` accepts any
/// value.
fn root_type_matches(value: &Value, token: &str, declared: Option<&Value>) -> bool {
    let is = |name: &str| match name {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.is_i64() || value.is_u64() || integral_literal(token.trim()),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        _ => false,
    };
    match declared {
        None => true,
        Some(Value::String(name)) => is(name),
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).any(is),
        Some(_) => false,
    }
}

/// Whether a JSON number literal denotes an integer, decided on its decimal
/// digits (an `f64` would round `1.0000000000000001` to `1.0`).
fn integral_literal(token: &str) -> bool {
    use std::sync::OnceLock;
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        regex::Regex::new(r"^-?(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$").expect("number regex")
    });
    let Some(c) = re.captures(token) else {
        return false;
    };
    let frac = c.get(2).map_or("", |m| m.as_str());
    let digits = format!("{}{frac}", &c[1]);
    if digits.bytes().all(|b| b == b'0') {
        return true;
    }
    // An exponent beyond i64 keeps its sign, so a huge negative one still
    // leaves a fraction for any nonzero digits.
    let exp: i64 = c.get(3).map_or(0, |m| {
        let e = m.as_str();
        e.parse().unwrap_or(if e.starts_with('-') {
            i64::MIN
        } else {
            i64::MAX
        })
    });
    let frac_len = i64::try_from(frac.len()).unwrap_or(i64::MAX);
    // value = digits × 10^(exp − frac_len): integral when that power is
    // non-negative or the digits end in enough zeros to absorb it.
    let shift = exp.saturating_sub(frac_len);
    let zeros = digits.bytes().rev().take_while(|b| *b == b'0').count();
    shift >= 0 || u64::try_from(zeros).is_ok_and(|z| z >= shift.unsigned_abs())
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
    #[serde(default)]
    finish_reason: Option<String>,
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

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::llm::Message;

    // The 400 LiteLLM relays from Vertex for Claude Opus 5.5, verbatim as
    // captured on 2026-11-23 (request id redacted), trailing suffix included.
    const OPUS_55_REJECTION: &str = r#"{"error":{"message":"litellm.BadRequestError: Vertex_aiException BadRequestError - {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"tool_choice: type \\\"tool\\\" and \\\"any\\\" are not supported for this model.\"},\"request_id\":\"req_vrtx_REDACTED\"}. Received Model Group=claude-opus-5-5\nAvailable Model Group Fallbacks=None","type":null,"param":null,"code":"400"}}"#;
    // Bodies that mention the refusal without being it.
    const MIXED_REJECTION: &str = r#"{"error":{"message":"temperature is not supported for this model","param":"tool_choice","code":"400"}}"#;
    const ECHOED_REJECTION: &str = r#"{"error":{"message":"temperature is not supported for this model"},"request":{"messages":[{"role":"user","content":"Explain when tool_choice forces tool use"}]}}"#;
    const NESTED_ECHO_REJECTION: &str = r#"{"error":{"message":"litellm.BadRequestError: Vertex_aiException BadRequestError - {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"invalid response schema\"},\"request\":{\"messages\":[{\"content\":\"Explain when tool_choice forces tool use\"}]}}","code":"400"}}"#;
    const QUOTED_FIELD_REJECTION: &str = r#"{"error":{"message":"invalid request field: {\"message\":\"tool_choice is unsupported\"}"}}"#;
    const PLAIN_TEXT_WRAPPED_REJECTION: &str =
        r#"BadRequestError - {"error":{"message":"tool_choice is unsupported"}}"#;
    const LITELLM_UNRELATED_REJECTION: &str = r#"{"error":{"message":"litellm.BadRequestError: invalid response schema - {\"message\":\"tool_choice is unsupported\"}"}}"#;
    const NOT_THE_REFUSAL: [&str; 8] = [
        "temperature is not supported for this model",
        MIXED_REJECTION,
        ECHOED_REJECTION,
        NESTED_ECHO_REJECTION,
        QUOTED_FIELD_REJECTION,
        PLAIN_TEXT_WRAPPED_REJECTION,
        LITELLM_UNRELATED_REJECTION,
        r#"{"detail":"invalid temperature","request":{"content":"tool_choice is unsupported"}}"#,
    ];

    fn req(schema: Option<Value>) -> CompletionRequest {
        CompletionRequest {
            system: "SYSTEM".into(),
            messages: vec![Message {
                role: Role::User,
                content: "hi".into(),
            }],
            max_tokens: 64,
            temperature: Some(0.0),
            response_schema: schema,
        }
    }

    fn schema() -> Value {
        json!({"type": "object", "properties": {"a": {"type": "integer"}}, "required": ["a"]})
    }

    #[test]
    fn body_sends_response_format_by_default() {
        let body = chat_body("m", &req(Some(schema())), false);
        assert_eq!(body["response_format"]["type"], "json_schema");
        assert_eq!(body["response_format"]["json_schema"]["schema"], schema());
        assert_eq!(body["messages"][0]["content"], "SYSTEM");
    }

    #[test]
    fn body_moves_schema_into_system_prompt() {
        let body = chat_body("m", &req(Some(schema())), true);
        assert!(body.get("response_format").is_none());
        let system = body["messages"][0]["content"].as_str().unwrap();
        assert!(system.starts_with("SYSTEM\n\n## Output format"));
        assert!(system.contains("exactly one JSON value"));
        assert!(system.contains("\"required\": [\n    \"a\"\n  ]"));
        assert_eq!(body["messages"][1]["content"], "hi");
        assert_eq!(body["temperature"], json!(0.0));
    }

    #[test]
    fn body_without_schema_is_identical_in_both_modes() {
        assert_eq!(
            chat_body("m", &req(None), false),
            chat_body("m", &req(None), true)
        );
        assert!(chat_body("m", &req(None), true)
            .get("response_format")
            .is_none());
    }

    fn api400(text: &str) -> LlmError {
        LlmError::api("openai", 400, text)
    }

    #[test]
    fn detects_only_the_forced_tool_choice_rejection() {
        for text in [
            OPUS_55_REJECTION,
            "forced tool_choice is unsupported for this model",
            "Thinking may not be enabled when tool_choice forces tool use.",
            r#"{"error":{"message":"tool_choice: type \"any\" is not supported"}}"#,
            r#"{"detail":"forced tool_choice is unsupported"}"#,
        ] {
            assert!(rejects_forced_tool_choice(&api400(text)), "{text}");
        }
        for text in NOT_THE_REFUSAL.iter().chain(&[
            r#"{"error":{"message":"\"thinking.type.enabled\" is not supported for this model. Use \"thinking.type.adaptive\""}}"#,
        ]) {
            assert!(!rejects_forced_tool_choice(&api400(text)), "{text}");
        }
        assert!(!rejects_forced_tool_choice(&LlmError::api(
            "openai",
            500,
            OPUS_55_REJECTION
        )));
        assert!(!rejects_forced_tool_choice(&LlmError::network(
            "openai",
            "tool_choice not supported"
        )));
    }

    #[test]
    fn validated_json_accepts_only_one_complete_value_with_the_root_type() {
        let s = schema();
        for (reply, want) in [
            ("{\"a\": 1}", "{\"a\": 1}"),
            ("  {\"a\": 1}\n", "{\"a\": 1}"),
            // Required keys are the caller's business, as in strict mode.
            ("{\"b\": 1}", "{\"b\": 1}"),
            ("```json\n{\"a\": 1}\n```", "{\"a\": 1}"),
            ("```\n{\"a\": 1}\n```", "{\"a\": 1}"),
            ("```json\r\n{\"a\": 1}\r\n```", "{\"a\": 1}"),
            (
                "```json\n{\"a\": 1, \"s\": \"use ``` to fence\"}\n```",
                "{\"a\": 1, \"s\": \"use ``` to fence\"}",
            ),
        ] {
            assert_eq!(validated_json(reply, &s).as_deref(), Ok(want), "{reply}");
        }
        for reply in [
            "",
            "```json\n\n```",
            "Here you go: {\"a\": 1}",
            "{\"a\": 1} done",
            "{\"a\": 1} {\"a\": 2}",
            "Example: {\"a\": 0}\nAnswer: {\"a\": ",
            "[{\"a\": 1},",
            "{\"a\": 1",
            "```json\n{\"a\": 1}",
            "```json\n{\"a\": 1}```",
            "```json {\"a\":1}```\n{\"a\":2}```",
            "```---\n{\"a\": 1}\n```",
            // Wrong root type: what consolidation would mine for its object.
            "[{\"merged_body\":\"draft\"},\"merge incomplete\"]",
            "[{\"a\": 1}]",
        ] {
            assert!(validated_json(reply, &s).is_err(), "{reply}");
        }
        // Declared types beyond object: arrays, scalars, unions.
        let arr = json!({"type": "array"});
        assert!(validated_json("[1]", &arr).is_ok());
        assert!(validated_json("{\"a\": 1}", &arr).is_err());
        let int = json!({"type": "integer"});
        for ok in [
            "1",
            "1.0",
            "1e0",
            "-3",
            "1.5e1",
            "10e-1",
            "0.0e-5",
            "0e-99999999999999999999",
        ] {
            assert!(validated_json(ok, &int).is_ok(), "{ok}");
        }
        for bad in [
            "1.5",
            "1.0000000000000001",
            "1.25e1",
            "1e-1",
            "1e-9223372036854775808",
            "1e-99999999999999999999",
        ] {
            assert!(validated_json(bad, &int).is_err(), "{bad}");
        }
        let boolean = json!({"type": "boolean"});
        assert!(validated_json("true", &boolean).is_ok());
        assert!(validated_json("{\"a\": 1}", &boolean).is_err());
        let nullable = json!({"type": ["object", "null"]});
        assert!(validated_json("null", &nullable).is_ok());
        assert!(validated_json("{\"a\": 1}", &nullable).is_ok());
        assert!(validated_json("[1]", &nullable).is_err());
        assert!(validated_json("[1]", &json!({})).is_ok());
        // The reason never quotes the reply.
        let err = validated_json("SECRET-MARKER then {\"a\": 1}", &s).unwrap_err();
        assert!(!err.contains("SECRET-MARKER"), "{err}");
    }

    /// One-request-per-connection HTTP stub: records each request body and
    /// answers with the next canned `(status, body)`, then with HTTP 418 so
    /// an unexpected extra request is recorded and fails loudly.
    fn stub(responses: Vec<(u16, String)>) -> (String, Arc<Mutex<Vec<Value>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_thread = Arc::clone(&seen);
        std::thread::spawn(move || {
            let mut responses = responses.into_iter();
            loop {
                let (status, body) = responses
                    .next()
                    .unwrap_or_else(|| (418, r#"{"error":"unexpected extra request"}"#.into()));
                let (mut stream, _) = listener.accept().unwrap();
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut len = 0usize;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        len = v.trim().parse().unwrap();
                    }
                }
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf).unwrap();
                seen_thread
                    .lock()
                    .unwrap()
                    .push(serde_json::from_slice(&buf).unwrap());
                let reply = format!(
                    "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(reply.as_bytes()).unwrap();
            }
        });
        (base, seen)
    }

    fn reply(content: Option<&str>, finish_reason: Option<&str>) -> (u16, String) {
        let mut choice = json!({"message": {"content": content}});
        if let Some(f) = finish_reason {
            choice["finish_reason"] = json!(f);
        }
        let body = json!({
            "model": "claude-opus-5-5",
            "choices": [choice],
            "usage": {"prompt_tokens": 10, "completion_tokens": 2},
        });
        (200, body.to_string())
    }

    fn ok(content: &str) -> (u16, String) {
        reply(Some(content), Some("stop"))
    }

    fn rejected() -> (u16, String) {
        (400, OPUS_55_REJECTION.to_string())
    }

    fn provider(base_url: String) -> OpenAiProvider {
        provider_with(base_url, 3)
    }

    fn provider_with(base_url: String, max_attempts: u32) -> OpenAiProvider {
        OpenAiProvider::new(OpenAiConfig {
            api_key: "k".into(),
            model: "claude-opus-5-5".into(),
            embed_model: "e".into(),
            embed_dim: 8,
            base_url,
            timeout: Duration::from_secs(10),
            max_attempts,
        })
        .unwrap()
    }

    fn strict(body: &Value) -> bool {
        body.get("response_format").is_some()
    }

    #[tokio::test]
    async fn falls_back_to_schema_in_prompt_and_stays_there() {
        let (base, seen) = stub(vec![
            rejected(),
            ok("```json\n{\"a\": 1}\n```"),
            ok("{\"a\": 2}"),
            ok("plain text"),
        ]);
        let p = provider(base);

        // Callers get the validated value, unfenced, as strict mode gives it.
        assert_eq!(
            p.complete(req(Some(schema()))).await.unwrap().text,
            "{\"a\": 1}"
        );
        assert!(p.schema_in_prompt.load(Ordering::Relaxed));
        assert_eq!(
            p.complete(req(Some(schema()))).await.unwrap().text,
            "{\"a\": 2}"
        );
        // Unstructured calls are untouched.
        assert_eq!(p.complete(req(None)).await.unwrap().text, "plain text");

        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.len(),
            4,
            "one rejected attempt, then one request per call"
        );
        assert!(strict(&seen[0]));
        for body in &seen[1..3] {
            assert!(!strict(body));
            assert!(body["messages"][0]["content"]
                .as_str()
                .unwrap()
                .contains("## Output format"));
        }
        assert_eq!(seen[3]["messages"][0]["content"], "SYSTEM");
    }

    #[tokio::test]
    async fn other_errors_are_returned_after_one_request_without_a_switch() {
        for body in std::iter::once(r#"{"error":"bad schema"}"#).chain(NOT_THE_REFUSAL) {
            let (base, seen) = stub(vec![(400, body.to_string()), ok("{\"a\": 1}")]);
            let p = provider(base);
            let err = p.complete(req(Some(schema()))).await.unwrap_err();
            assert!(matches!(err, LlmError::Api { status: 400, .. }), "{body}");
            assert_eq!(seen.lock().unwrap().len(), 1, "{body}");
            // The next call is still strict.
            p.complete(req(Some(schema()))).await.unwrap();
            assert!(strict(&seen.lock().unwrap()[1]), "{body}");
            assert!(!p.schema_in_prompt.load(Ordering::Relaxed));
        }
    }

    #[tokio::test]
    async fn the_switch_spends_the_same_attempt_allowance() {
        // Two transient failures and the rejection use all three attempts:
        // no fourth request, the rejection stands.
        let (base, seen) = stub(vec![
            (503, "{}".to_string()),
            (503, "{}".to_string()),
            rejected(),
        ]);
        let err = provider_with(base, 3)
            .complete(req(Some(schema())))
            .await
            .unwrap_err();
        assert!(rejects_forced_tool_choice(&err), "{err}");
        assert_eq!(seen.lock().unwrap().len(), 3);

        // One attempt configured: one request, the original 400.
        let (base, seen) = stub(vec![rejected()]);
        let p = provider_with(base, 1);
        let err = p.complete(req(Some(schema()))).await.unwrap_err();
        assert!(rejects_forced_tool_choice(&err), "{err}");
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(!p.schema_in_prompt.load(Ordering::Relaxed));

        // One attempt left after the rejection: the switch uses it.
        let (base, seen) = stub(vec![rejected(), ok("{\"a\": 1}")]);
        let p = provider_with(base, 2);
        p.complete(req(Some(schema()))).await.unwrap();
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert!(p.schema_in_prompt.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn invalid_prompt_mode_replies_are_errors_and_never_stick() {
        let bad = [
            reply(None, Some("stop")),
            reply(Some("Here you go: {\"a\": 1}"), Some("stop")),
            // No finish_reason, unfinished outer array (what consolidation
            // would otherwise mine for its inner object).
            reply(Some("[{\"merged_body\":\"draft\"},"), None),
            reply(Some("Example: {\"a\": 0}\nAnswer: {\"a\": "), None),
            reply(
                Some("[{\"merged_body\":\"draft\"},\"merge incomplete\"]"),
                Some("stop"),
            ),
            reply(Some("SECRET-MARKER: {\"a\": 1}"), Some("stop")),
        ];
        for bad in bad {
            let (base, seen) = stub(vec![rejected(), bad.clone(), ok("{\"a\": 1}")]);
            let p = provider(base);
            let err = p.complete(req(Some(schema()))).await.unwrap_err();
            assert!(
                matches!(err, LlmError::InvalidStructuredReply { .. }),
                "{err}"
            );
            assert!(!err.to_string().contains("SECRET-MARKER"), "{err}");
            assert!(!format!("{err:?}").contains("SECRET-MARKER"), "{err:?}");
            assert!(!p.schema_in_prompt.load(Ordering::Relaxed), "{bad:?}");
            p.complete(req(Some(schema()))).await.unwrap();
            assert!(strict(&seen.lock().unwrap()[2]), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn a_length_cut_prompt_mode_reply_is_rejected_with_its_text() {
        let (base, _seen) = stub(vec![rejected(), reply(Some("{\"a\": "), Some("length"))]);
        let p = provider(base);
        let err = p.complete(req(Some(schema()))).await.unwrap_err();
        let LlmError::InvalidStructuredReply {
            reason,
            reply,
            usage,
            ..
        } = &err
        else {
            panic!("{err}");
        };
        assert!(reason.contains("token limit"), "{reason}");
        assert_eq!(usage.output_tokens, 2);
        assert_eq!(reply.expose(), "{\"a\": ");
        assert!(!p.schema_in_prompt.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn the_faithfulness_judge_works_in_prompt_mode() {
        let claims = "```json\n{\"claims\": [{\"claim\": \"a\", \"supported\": true}, {\"claim\": \"b\", \"supported\": false}]}\n```";
        let (base, seen) = stub(vec![rejected(), ok(claims), ok(claims)]);
        let p = provider(base);
        for _ in 0..2 {
            let report = crate::eval::judge_faithfulness(&p, "subject", "context")
                .await
                .unwrap();
            assert_eq!(report.claims.len(), 2);
            assert_eq!(report.supported(), 1);
        }
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert!(!strict(&seen[2]), "the second audit starts in prompt mode");
    }

    #[tokio::test]
    async fn no_request_starts_after_the_deadline() {
        let (base, seen) = stub(Vec::new());
        let mut p = provider(base);
        p.config.timeout = Duration::ZERO;
        let err = p.complete(req(Some(schema()))).await.unwrap_err();
        assert!(matches!(err, LlmError::Timeout { .. }), "{err}");
        assert!(seen.lock().unwrap().is_empty());
    }
}
