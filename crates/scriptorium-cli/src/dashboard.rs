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

use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Json};
use axum::routing::get;
use axum::Router;
use serde::{Deserialize, Serialize};
use tower_http::cors::{AllowOrigin, CorsLayer};

use scriptorium_core::hooks_store::HooksStore;

// ── App state ────────────────────────────────────────────────────────────

/// Shared application state for all handlers.
///
/// Holds the DB path so each request opens its own [`HooksStore`] connection
/// inside a blocking task. This sidesteps the `!Sync` constraint on
/// `rusqlite::Connection` while letting WAL-mode SQLite serve concurrent
/// readers without contention.
#[derive(Clone)]
struct AppState {
    db_path: PathBuf,
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

// ── Health response ──────────────────────────────────────────────────────

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    db_path: String,
    db_accessible: bool,
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
) -> Result<impl IntoResponse, StatusCode> {
    let limit = params.limit.unwrap_or(50);
    let since = params.since.and_then(|s| {
        chrono::DateTime::parse_from_rfc3339(&s)
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
    });
    let db_path = state.db_path.clone();

    let events = tokio::task::spawn_blocking(move || {
        let store = HooksStore::open(&db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        store
            .query_events(limit, since)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    })
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    Ok(Json(events))
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

/// GET /api/health
///
/// Returns a health check report including DB accessibility.
async fn health_handler(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let db_path = state.db_path.clone();
    let db_accessible = tokio::task::spawn_blocking(move || HooksStore::open(&db_path).is_ok())
        .await
        .unwrap_or(false);

    Json(HealthResponse {
        status: if db_accessible { "ok" } else { "degraded" },
        db_path: state.db_path.display().to_string(),
        db_accessible,
    })
}

/// GET /
///
/// Returns the Scriptorium Hooks Telemetry Dashboard UI.
async fn index_handler() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
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
) -> miette::Result<()> {
    use miette::{miette, IntoDiagnostic};

    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent).into_diagnostic()?;
    }

    if let Some(ref jsonl) = jsonl_path {
        if jsonl.exists() {
            eprintln!("Importing events from {}…", jsonl.display());
            let store =
                HooksStore::open(&db_path).map_err(|e| miette!("open hooks db: {e}"))?;
            let report = store
                .import_jsonl(jsonl, false)
                .map_err(|e| miette!("import: {e}"))?;
            eprintln!(
                "Import: {} imported, {} duplicates skipped, {} malformed",
                report.imported, report.skipped_duplicate, report.skipped_malformed,
            );
        } else {
            eprintln!(
                "JSONL file not found: {} — skipping import",
                jsonl.display()
            );
        }
    }

    let _verify = HooksStore::open(&db_path)
        .map_err(|e| miette!("cannot open hooks database: {e}"))?;
    drop(_verify);

    let state = Arc::new(AppState { db_path });

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::predicate(
            |origin: &HeaderValue, _parts: &_| {
                origin
                    .as_bytes()
                    .starts_with(b"http://127.0.0.1")
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
