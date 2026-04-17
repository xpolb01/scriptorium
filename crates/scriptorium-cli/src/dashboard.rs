//! Hooks health dashboard — axum web UI.
//!
//! This module is feature-gated behind `--features dashboard`.
//! When enabled, it provides a localhost-only web server for monitoring
//! hook execution metrics and vault health.

#![cfg(feature = "dashboard")]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::Deserialize;
use std::str::FromStr;
use tower_http::cors::{AllowOrigin, CorsLayer};

use scriptorium_core::hooks_store::HooksStore;
use scriptorium_core::telemetry::{
    store::{SummaryGroupBy, SummaryReport},
    Cursor, LogFilters, Source, SpanFilters, TelemetryStore,
};

// ── App state ────────────────────────────────────────────────────────────

/// Shared application state for all handlers.
///
/// Holds the DB path so each request opens its own [`HooksStore`] connection
/// inside a blocking task. This sidesteps the `!Sync` constraint on
/// `rusqlite::Connection` while letting WAL-mode `SQLite` serve concurrent
/// readers without contention.
#[derive(Clone)]
struct AppState {
    db_path: PathBuf,
    settings_path: PathBuf,
    hooks_dir: PathBuf,
    vault_path: Option<PathBuf>,
    telemetry: Arc<TelemetryStore>,
}

fn cap_limit(raw: Option<u32>) -> u32 {
    raw.unwrap_or(50).clamp(1, 1000)
}

fn parse_sources(csv: Option<&str>) -> Vec<Source> {
    csv.map(|s| {
        s.split(',')
            .filter(|t| !t.is_empty())
            .filter_map(|t| Source::from_str(t.trim()).ok())
            .collect()
    })
    .unwrap_or_default()
}

fn json_err(status: StatusCode, detail: &str) -> (StatusCode, Json<serde_json::Value>) {
    (
        status,
        Json(serde_json::json!({ "error": "internal", "detail": detail })),
    )
}

// ── Query parameter structs ──────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct SummaryParams {
    /// Window in milliseconds (default: 3\_600\_000 = 1 hour).
    window: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct EventsParams {
    /// Maximum number of events to return (default: 50).
    limit: Option<usize>,
    /// ISO-8601 timestamp — only return events after this time.
    since: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorsParams {
    /// Maximum number of error events to return (default: 50).
    limit: Option<usize>,
}

// ── Handlers ─────────────────────────────────────────────────────────────

pub const DASHBOARD_HTML: &str = include_str!("dashboard.html");

/// GET /api/summary?window=ms
///
/// Returns aggregated [`HooksSummary`] for the given time window.
async fn summary_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SummaryParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let window_ms = params.window.unwrap_or(3_600_000);
    let window = Duration::from_millis(window_ms);
    let db_path = state.db_path.clone();

    let summary = tokio::task::spawn_blocking(move || {
        let store = HooksStore::open(&db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        store
            .query_summary(window)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(summary))
}

/// GET /api/events?limit=N&since=iso8601
///
/// Returns recent hook events, most recent first.
async fn events_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<EventsParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let limit = params.limit.unwrap_or(50);

    let since = if let Some(s) = params.since {
        match chrono::DateTime::parse_from_rfc3339(&s) {
            Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": "invalid_since",
                        "detail": format!("`since` must be RFC3339: {e}")
                    })),
                ))
            }
        }
    } else {
        None
    };

    let db_path = state.db_path.clone();

    let events = tokio::task::spawn_blocking(move || {
        let store = HooksStore::open(&db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        store
            .query_events(limit, since)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    })
    .await
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "internal",
                "detail": "events query task panicked",
            })),
        )
    })?
    .map_err(|status_code| {
        (
            status_code,
            Json(serde_json::json!({
                "error": "internal",
                "detail": "failed to query events",
            })),
        )
    })?;

    let events_value = serde_json::to_value(events).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "internal",
                "detail": "failed to serialize events",
            })),
        )
    })?;

    Ok(Json(events_value))
}

/// GET /api/errors?limit=N
///
/// Returns events with `decision = 'veto'` or `privacy_veto IS NOT NULL`.
async fn errors_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ErrorsParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let db_path = state.db_path.clone();

    let errors = tokio::task::spawn_blocking(move || {
        let store = HooksStore::open(&db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        store
            .query_errors(limit)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(errors))
}

/// GET /api/health — Returns Vec<CheckItem> (session + vault merged).
async fn health_handler(
    State(state): State<Arc<AppState>>,
) -> Result<
    Json<Vec<scriptorium_core::hooks_check::CheckItem>>,
    (StatusCode, Json<serde_json::Value>),
> {
    let settings_path = state.settings_path.clone();
    let hooks_dir = state.hooks_dir.clone();
    let vault_path = state.vault_path.clone();

    let items = tokio::task::spawn_blocking(move || {
        use scriptorium_core::hooks_check::{
            check_session_hooks, check_vault_hooks, CheckItem, CheckStatus,
        };

        let mut report = check_session_hooks(&settings_path, &hooks_dir);
        if let Some(ref vp) = vault_path {
            let vault = check_vault_hooks(vp);
            report.items.extend(vault.items);
        } else {
            report.items.push(CheckItem {
                name: "vault_check".to_string(),
                status: CheckStatus::Info,
                message: "no vault configured; pass --vault <PATH> or register a default vault"
                    .to_string(),
            });
        }
        report.items
    })
    .await
    .map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "internal",
                "detail": "health check task panicked",
            })),
        )
    })?;

    Ok(Json(items))
}

/// GET /
///
/// Returns the Scriptorium Hooks Telemetry Dashboard UI.
async fn index_handler() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

// ── Telemetry query params ───────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct LogsParams {
    source: Option<String>,
    severity: Option<u8>,
    search: Option<String>,
    trace_id: Option<String>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SpansParams {
    source: Option<String>,
    name: Option<String>,
    trace_id: Option<String>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TimelineParams {
    sources: Option<String>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SummaryWindowParams {
    window: Option<u64>,
}

async fn logs_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<LogsParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let limit = cap_limit(params.limit);
    let filters = LogFilters {
        sources: parse_sources(params.source.as_deref()),
        min_severity: params.severity,
        search: params.search,
        trace_id: params.trace_id,
        since_unix_nano: None,
    };
    let cursor = params.cursor.as_deref().and_then(Cursor::decode);
    let tele = state.telemetry.clone();

    let result = tokio::task::spawn_blocking(move || tele.query_logs(filters, cursor, limit))
        .await
        .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "logs task panicked"))?
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;

    let (items, next) = result;
    Ok(Json(serde_json::json!({
        "items": items,
        "next_cursor": next.map(|c| c.encode()),
    })))
}

async fn spans_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SpansParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let limit = cap_limit(params.limit);
    let filters = SpanFilters {
        sources: parse_sources(params.source.as_deref()),
        name: params.name,
        trace_id: params.trace_id,
        since_unix_nano: None,
    };
    let cursor = params.cursor.as_deref().and_then(Cursor::decode);
    let tele = state.telemetry.clone();

    let (items, next) =
        tokio::task::spawn_blocking(move || tele.query_spans(filters, cursor, limit))
            .await
            .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "spans task panicked"))?
            .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;

    Ok(Json(serde_json::json!({
        "items": items,
        "next_cursor": next.map(|c| c.encode()),
    })))
}

async fn trace_handler(
    State(state): State<Arc<AppState>>,
    Path(trace_id): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let tele = state.telemetry.clone();
    let tree = tokio::task::spawn_blocking(move || tele.query_trace(&trace_id))
        .await
        .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "trace task panicked"))?
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    Ok(Json(
        serde_json::to_value(tree).unwrap_or(serde_json::json!({})),
    ))
}

async fn timeline_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<TimelineParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let limit = cap_limit(params.limit);
    let sources = parse_sources(params.sources.as_deref());
    let cursor = params.cursor.as_deref().and_then(Cursor::decode);
    let tele = state.telemetry.clone();

    let result = tokio::task::spawn_blocking(move || tele.query_timeline(&sources, cursor, limit))
        .await
        .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "timeline task panicked"))?
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;

    let items: Vec<serde_json::Value> = result
        .items
        .into_iter()
        .map(|e| {
            use scriptorium_core::telemetry::TimelineEntry;
            match e {
                TimelineEntry::Log(l) => serde_json::json!({
                    "timeline_key": format!("log:{}", l.id),
                    "kind": "log",
                    "time_unix_nano": l.time_unix_nano,
                    "source": l.source,
                    "log": l,
                }),
                TimelineEntry::SpanStart(s) => serde_json::json!({
                    "timeline_key": format!("span:{}", s.span_id),
                    "kind": "span_start",
                    "time_unix_nano": s.start_time_unix_nano,
                    "source": s.source,
                    "span": s,
                }),
            }
        })
        .collect();

    Ok(Json(serde_json::json!({
        "items": items,
        "next_cursor": result.next_cursor.map(|c| c.encode()),
    })))
}

fn summary_report(
    state: &Arc<AppState>,
    source: &'static str,
    group_by: SummaryGroupBy<'static>,
    window_ms: u64,
) -> Result<SummaryReport, (StatusCode, Json<serde_json::Value>)> {
    state
        .telemetry
        .span_summary(source, group_by, window_ms)
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))
}

async fn cli_summary_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SummaryWindowParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let window = params.window.unwrap_or(3_600_000);
    let state2 = state.clone();
    let report = tokio::task::spawn_blocking(move || {
        summary_report(&state2, "cli", SummaryGroupBy::SpanAttr("command"), window)
    })
    .await
    .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "cli summary panicked"))??;
    Ok(Json(serde_json::json!({
        "top_commands": report.top,
        "total_invocations": report.total,
        "error_count": report.error_count,
    })))
}

async fn mcp_summary_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SummaryWindowParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let window = params.window.unwrap_or(3_600_000);
    let state2 = state.clone();
    let report = tokio::task::spawn_blocking(move || {
        summary_report(
            &state2,
            "mcp",
            SummaryGroupBy::SpanAttr("tool_name"),
            window,
        )
    })
    .await
    .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "mcp summary panicked"))??;
    Ok(Json(serde_json::json!({
        "top_tools": report.top,
        "total_calls": report.total,
        "error_count": report.error_count,
        "session_count": report.session_count,
    })))
}

async fn hook_summary_handler(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SummaryWindowParams>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<serde_json::Value>)> {
    let window = params.window.unwrap_or(3_600_000);
    let tele = state.telemetry.clone();
    let report = tokio::task::spawn_blocking(move || tele.log_summary_by_body("hook", 40, window))
        .await
        .map_err(|_| json_err(StatusCode::INTERNAL_SERVER_ERROR, "hook summary panicked"))?
        .map_err(|e| json_err(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    Ok(Json(serde_json::json!({
        "top_commands": report.top,
        "total_invocations": report.total,
        "error_count": report.error_count,
    })))
}

// ── Public entry point ───────────────────────────────────────────────────

/// Start the hooks health dashboard web server.
///
/// 1. If `jsonl_path` is provided and exists, imports events first.
/// 2. Opens the [`HooksStore`] at `db_path` to verify accessibility.
/// 3. Builds the axum router with CORS for localhost origins.
/// 4. Binds to `127.0.0.1:{port}` (never `0.0.0.0`).
/// 5. Serves until SIGINT (Ctrl-C).
pub async fn start_dashboard(
    port: u16,
    db_path: PathBuf,
    jsonl_path: Option<PathBuf>,
    settings_path: PathBuf,
    hooks_dir: PathBuf,
    vault_path: Option<PathBuf>,
) -> miette::Result<()> {
    use miette::{miette, IntoDiagnostic};

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).into_diagnostic()?;
    }

    let _ = jsonl_path;

    let _verify =
        HooksStore::open(&db_path).map_err(|e| miette!("cannot open hooks database: {e}"))?;
    let telemetry = Arc::new(
        TelemetryStore::open(&db_path).map_err(|e| miette!("cannot open telemetry store: {e}"))?,
    );

    let state = Arc::new(AppState {
        db_path,
        settings_path,
        hooks_dir,
        vault_path,
        telemetry,
    });

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(
            |origin: &HeaderValue, _parts: &_| {
                origin.as_bytes().starts_with(b"http://127.0.0.1")
                    || origin.as_bytes().starts_with(b"http://localhost")
            },
        ))
        .allow_methods(tower_http::cors::Any)
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/summary", get(summary_handler))
        .route("/api/events", get(events_handler))
        .route("/api/errors", get(errors_handler))
        .route("/api/health", get(health_handler))
        .route("/api/logs", get(logs_handler))
        .route("/api/spans", get(spans_handler))
        .route("/api/traces/:trace_id", get(trace_handler))
        .route("/api/timeline", get(timeline_handler))
        .route("/api/cli/summary", get(cli_summary_handler))
        .route("/api/mcp/summary", get(mcp_summary_handler))
        .route("/api/hook/summary", get(hook_summary_handler))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    eprintln!("Scriptorium Hooks Dashboard: http://127.0.0.1:{port}");

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .into_diagnostic()?;

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .into_diagnostic()?;

    Ok(())
}

/// Wait for SIGINT (Ctrl-C) to trigger graceful shutdown.
async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install CTRL+C signal handler");
    eprintln!("\nShutting down dashboard…");
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use scriptorium_core::hooks_check::{CheckItem, CheckStatus};
    use tower::ServiceExt;

    fn tmp_state(db: &str) -> Arc<AppState> {
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!("scr-dash-{n}-{db}"));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join(db);
        let telemetry = Arc::new(TelemetryStore::open(&db_path).unwrap());
        Arc::new(AppState {
            db_path,
            settings_path: PathBuf::from("/nonexistent/settings.json"),
            hooks_dir: PathBuf::from("/nonexistent/hooks"),
            vault_path: None,
            telemetry,
        })
    }

    #[test]
    fn app_state_carries_new_paths() {
        let state = tmp_state("hooks.sqlite");
        assert!(state.db_path.ends_with("hooks.sqlite"));
        assert_eq!(state.vault_path, None);
    }

    fn build_test_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/health", get(health_handler))
            .with_state(state)
    }

    async fn oneshot_health(state: Arc<AppState>) -> (StatusCode, Vec<u8>) {
        let app = build_test_router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, bytes)
    }

    #[tokio::test]
    async fn health_handler_returns_checkitem_array() {
        let state = tmp_state("health-t1.sqlite");

        let (status, body) = oneshot_health(state).await;
        assert_eq!(status, StatusCode::OK);
        let items: Vec<CheckItem> = serde_json::from_slice(&body)
            .unwrap_or_else(|e| panic!("response body must parse as Vec<CheckItem>: {e}"));
        assert!(
            !items.is_empty(),
            "health endpoint must return at least one CheckItem"
        );
    }

    #[tokio::test]
    async fn health_handler_missing_settings_surfaces_fail_item() {
        let state = tmp_state("health-t2.sqlite");

        let (status, body) = oneshot_health(state).await;
        assert_eq!(status, StatusCode::OK);
        let items: Vec<CheckItem> = serde_json::from_slice(&body).unwrap();
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Fail),
            "missing settings.json must produce at least one Fail item: {items:?}"
        );
    }

    #[tokio::test]
    async fn health_handler_no_vault_surfaces_info_item() {
        let state = tmp_state("health-t3.sqlite");

        let (status, body) = oneshot_health(state).await;
        assert_eq!(status, StatusCode::OK);
        let items: Vec<CheckItem> = serde_json::from_slice(&body).unwrap();
        let vault_check = items
            .iter()
            .find(|i| i.name == "vault_check")
            .expect("no vault → must include a vault_check item");
        assert_eq!(vault_check.status, CheckStatus::Info);
        assert!(
            vault_check.message.contains("no vault configured"),
            "vault_check message must explain absent vault: {}",
            vault_check.message
        );
    }

    fn build_events_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/events", get(events_handler))
            .with_state(state)
    }

    async fn oneshot_events(state: Arc<AppState>, uri: &str) -> (StatusCode, Vec<u8>) {
        let app = build_events_router(state);
        let response = app
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec();
        (status, bytes)
    }

    #[tokio::test]
    async fn events_handler_missing_since_works() {
        let state = tmp_state("events-t1.sqlite");

        let (status, _body) = oneshot_events(state, "/api/events").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "missing since parameter must return 200 OK"
        );
    }

    #[tokio::test]
    async fn events_handler_valid_since_parses() {
        let state = tmp_state("events-t2.sqlite");

        let (status, _body) = oneshot_events(state, "/api/events?since=2026-04-16T08:00:00Z").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "valid RFC3339 since parameter must return 200 OK"
        );
    }

    #[tokio::test]
    async fn events_handler_invalid_since_400() {
        let state = tmp_state("events-t3.sqlite");

        let (status, body) = oneshot_events(state, "/api/events?since=not-a-date").await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "invalid RFC3339 since parameter must return 400 BAD_REQUEST"
        );

        let error_json: serde_json::Value =
            serde_json::from_slice(&body).expect("response body must be valid JSON");
        assert_eq!(
            error_json
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or(""),
            "invalid_since",
            "error response must contain error field with value 'invalid_since'"
        );
        assert!(
            error_json
                .get("detail")
                .and_then(|v| v.as_str())
                .is_some_and(|s| s.contains("RFC3339")),
            "error response detail must mention RFC3339: {error_json:?}"
        );
    }
}
