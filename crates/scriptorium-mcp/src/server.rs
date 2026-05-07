//! JSON-RPC 2.0 loop over stdio.
//!
//! Reads one JSON object per line from stdin, dispatches it to the
//! appropriate MCP method handler, and writes the response (one JSON object
//! per line) to stdout. Notifications (messages without an `id`) produce no
//! response.
//!
//! The loop is single-threaded by design: MCP clients issue requests
//! serially, and serializing the vault lock through the main task is
//! simpler than coordinating concurrent mutating calls.

use std::sync::Arc;

use scriptorium_core::llm::LlmProvider;
use scriptorium_core::telemetry::propagation::TraceContext;
use scriptorium_core::Vault;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tracing::{debug, error, info, warn, Instrument};

use crate::tools::{ToolError, ToolRegistry};

pub const PROTOCOL_VERSION: &str = "2024-11-05";
pub const SERVER_NAME: &str = "scriptorium";
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Shared state every tool handler sees.
///
/// Holds **two** provider references: `llm_provider` handles chat
/// (ingest + query answer generation) and `embed_provider` handles
/// vector embeddings (search + query retrieval). They can be the same
/// underlying provider — `gemini` does both — or different, which is the
/// common Claude-plus-Gemini setup.
#[derive(Clone)]
pub struct ServerContext {
    pub vault: Vault,
    pub llm_provider: Arc<dyn LlmProvider>,
    pub embed_provider: Arc<dyn LlmProvider>,
    pub embeddings_model: String,
}

/// JSON-RPC 2.0 request envelope. `id` is `None` for notifications.
#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[serde(default)]
    #[allow(dead_code)]
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

/// Response envelope. Exactly one of `result` / `error` is set.
#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

/// Standard JSON-RPC error codes used by the server.
mod error_codes {
    pub const PARSE_ERROR: i32 = -32700;
    pub const METHOD_NOT_FOUND: i32 = -32601;
    pub const INVALID_PARAMS: i32 = -32602;
}

/// Run the MCP server on stdio. Returns when stdin closes (EOF) or when a
/// write to stdout fails.
pub async fn serve_stdio(context: ServerContext) -> anyhow::Result<()> {
    info!(
        server = SERVER_NAME,
        version = SERVER_VERSION,
        "mcp stdio: starting"
    );

    let session_id = TraceContext::new_root(None, None);
    let session_span = tracing::info_span!(
        "mcp.session",
        "session.id" = %session_id.trace_id,
        transport = "stdio",
        otel.kind = "server"
    );

    async {
        tracing::info!("mcp.session.start");

        let registry = ToolRegistry::new();
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin).lines();
        let mut stdout = tokio::io::stdout();

        while let Some(line) = reader.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            debug!(raw = %line, "mcp <-");
            let parsed: Result<JsonRpcRequest, _> = serde_json::from_str(&line);
            let response = match parsed {
                Err(e) => {
                    warn!(error = %e, "mcp parse error");
                    Some(error_response(
                        Value::Null,
                        error_codes::PARSE_ERROR,
                        format!("parse error: {e}"),
                    ))
                }
                Ok(req) => handle_request(req, &context, &registry).await,
            };
            if let Some(resp) = response {
                let serialized = serde_json::to_string(&resp)?;
                debug!(raw = %serialized, "mcp ->");
                stdout.write_all(serialized.as_bytes()).await?;
                stdout.write_all(b"\n").await?;
                stdout.flush().await?;
            }
        }
        tracing::info!(otel.status = "ok", "mcp.session.end");
        info!("mcp stdio: stdin closed, exiting");
        Ok(())
    }
    .instrument(session_span)
    .await
}

async fn handle_request(
    req: JsonRpcRequest,
    context: &ServerContext,
    registry: &ToolRegistry,
) -> Option<JsonRpcResponse> {
    let id = req.id.clone();
    let result = dispatch(&req, context, registry).await;
    match (id, result) {
        // Notification (no id): never respond, even on error.
        (None, _) => None,
        (Some(id), Ok(value)) => Some(JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: Some(value),
            error: None,
        }),
        (Some(id), Err(err)) => Some(JsonRpcResponse {
            jsonrpc: "2.0",
            id,
            result: None,
            error: Some(err),
        }),
    }
}

async fn dispatch(
    req: &JsonRpcRequest,
    context: &ServerContext,
    registry: &ToolRegistry,
) -> Result<Value, JsonRpcError> {
    let rpc_method = req.method.as_str();
    let rpc_request_id = match &req.id {
        Some(id) => format!("{}", id),
        None => "null".to_string(),
    };
    let request_span = tracing::info_span!(
        "mcp.request",
        "rpc.method" = %rpc_method,
        "rpc.request_id" = %rpc_request_id,
        otel.kind = "server"
    );

    async {
        let result = match req.method.as_str() {
            "initialize" => Ok(initialize_result()),
            "notifications/initialized" | "initialized" => Ok(Value::Null),
            "ping" => Ok(json!({})),
            "tools/list" => Ok(json!({ "tools": registry.describe_all() })),
            "tools/call" => tools_call(req, context, registry).await,
            other => Err(JsonRpcError {
                code: error_codes::METHOD_NOT_FOUND,
                message: format!("method not found: {other}"),
                data: None,
            }),
        };

        match &result {
            Ok(_) => tracing::info!(otel.status = "ok", "mcp.request.end"),
            Err(e) => tracing::error!(otel.status = "error", error = %e.message, "mcp.request.end"),
        }

        result
    }
    .instrument(request_span)
    .await
}

fn initialize_result() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "serverInfo": {
            "name": SERVER_NAME,
            "version": SERVER_VERSION,
        },
        "capabilities": {
            "tools": {}
        }
    })
}

async fn tools_call(
    req: &JsonRpcRequest,
    context: &ServerContext,
    registry: &ToolRegistry,
) -> Result<Value, JsonRpcError> {
    let name = req
        .params
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| JsonRpcError {
            code: error_codes::INVALID_PARAMS,
            message: "tools/call: missing `name`".into(),
            data: None,
        })?
        .to_string();
    let arguments = req
        .params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let tool_full_name: &str = name.as_str();
    let tool_name: &str = tool_full_name
        .strip_prefix("scriptorium_")
        .unwrap_or(tool_full_name);
    let span = tracing::info_span!(
        "mcp.tool",
        tool_name = %tool_name,
        tool_full_name = %tool_full_name,
        otel.kind = "server",
    );

    async move {
        // Cap the params preview so we don't log unbounded payloads.
        let params_json = serde_json::to_string(&req.params).unwrap_or_default();
        let (params_preview, _trunc_meta) = scriptorium_core::telemetry::payload::cap_body(
            &params_json,
            &scriptorium_core::telemetry::envelope::DEFAULT_PAYLOAD_CAP,
        );
        tracing::info!(params = %params_preview, "mcp.tool.start");

        let t0 = std::time::Instant::now();
        let result = registry.invoke(&name, arguments, context).await;
        let duration_ms = t0.elapsed().as_millis() as u64;

        match &result {
            Ok(text) => {
                tracing::info!(
                    result_size = text.len(),
                    duration_ms = duration_ms,
                    otel.status = "ok",
                    "mcp.tool.end",
                );
            }
            Err(e) => {
                let msg = match e {
                    ToolError::NotFound(n) => format!("not found: {n}"),
                    ToolError::InvalidArgs(m) => format!("invalid args: {m}"),
                    ToolError::Failed(m) => format!("failed: {m}"),
                };
                tracing::error!(
                    error = %msg,
                    duration_ms = duration_ms,
                    otel.status = "error",
                    "mcp.tool.end",
                );
            }
        }

        match result {
            Ok(text) => Ok(json!({
                "content": [{"type": "text", "text": text}],
                "isError": false,
            })),
            Err(ToolError::NotFound(name)) => Err(JsonRpcError {
                code: error_codes::METHOD_NOT_FOUND,
                message: format!("no such tool: {name}"),
                data: None,
            }),
            Err(ToolError::InvalidArgs(msg)) => Err(JsonRpcError {
                code: error_codes::INVALID_PARAMS,
                message: msg,
                data: None,
            }),
            Err(ToolError::Failed(msg)) => {
                // Tool failures are returned as isError content so the client
                // can display them, while non-tool errors (invalid params,
                // method not found) become JSON-RPC errors.
                error!(tool = %name, error = %msg, "tool failed");
                Ok(json!({
                    "content": [{"type": "text", "text": msg}],
                    "isError": true,
                }))
            }
        }
    }
    .instrument(span)
    .await
}

fn error_response(id: Value, code: i32, message: String) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message,
            data: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initialize_result_has_protocol_version_and_capabilities() {
        let v = initialize_result();
        assert_eq!(v["protocolVersion"], PROTOCOL_VERSION);
        assert!(v["capabilities"]["tools"].is_object());
        assert_eq!(v["serverInfo"]["name"], SERVER_NAME);
    }
}
