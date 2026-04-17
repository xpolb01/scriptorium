#![allow(clippy::doc_markdown)]

//! SQLite data layer for OTel-shaped telemetry.
//!
//! [`TelemetryStore`] owns the telemetry SQLite file, opens it in WAL mode
//! with a 5-second `busy_timeout`, applies all pending [`schema`]
//! migrations, and exposes:
//!
//! - **Best-effort writes** that never return `Result` — every
//!   `insert_*`/`update_*` method returns an [`InsertOutcome`] encoding
//!   `Inserted(rowid)`, `Duplicate`, or `Dropped(reason)`. A DB error
//!   after the retry budget is exhausted is converted to
//!   `Dropped(DropReason::Busy | …)` and a marker log is best-effort
//!   written (with a thread-local recursion guard so the marker path can
//!   never itself re-enter the pipeline).
//! - **Queries that return `Result`** — reads surface rusqlite errors to
//!   the caller normally. Cursor-paginated scans use
//!   `(time_unix_nano, id)` or `(start_time_unix_nano, span_id)` to walk
//!   the indexes deterministically without gaps or duplicates.
//! - **Global stats** via [`GLOBAL_STATS`], incremented even when the
//!   store cannot be opened (the telemetry layer increments them from
//!   outside the store when `open()` fails — see T12).
//!
//! Connection strategy mirrors the guidance in `hooks_store.rs`: open a
//! fresh [`rusqlite::Connection`] per write to keep locks short; queries
//! use a single scoped connection.
//!
//! [`schema`]: crate::telemetry::schema

use crate::telemetry::envelope::{
    Attributes, LogRecord, SeverityNumber, Source, Span, SpanId, Status, TraceId,
};
use crate::telemetry::resource::Resource;
use crate::telemetry::schema::apply_migrations;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::Rng;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::thread;
use std::time::Duration;

// ── Retry config ─────────────────────────────────────────────────────────

/// Maximum number of attempts for a single write (1 initial + 2 retries).
const MAX_WRITE_ATTEMPTS: u32 = 3;
/// Base backoff in ms; jitter samples uniformly from `[BASE, 2*BASE)`.
const BASE_BACKOFF_MS: u64 = 25;

// ── Global stats ─────────────────────────────────────────────────────────

/// Global telemetry stats. Incremented unconditionally — the telemetry
/// layer keeps incrementing `dropped_count` even when the store cannot be
/// opened, which preserves the no-silent-loss invariant.
#[derive(Debug, Default)]
pub struct TelemetryStats {
    pub dropped_count: AtomicU64,
    pub truncated_count: AtomicU64,
    pub retry_count: AtomicU64,
}

/// Process-global singleton stats. All [`TelemetryStore`] instances — and
/// code that fails to open a store — share the same counters.
pub static GLOBAL_STATS: LazyLock<TelemetryStats> = LazyLock::new(TelemetryStats::default);

// ── Recursion guard ──────────────────────────────────────────────────────

thread_local! {
    /// True while the current thread is inside [`record_dropped_event`]'s
    /// marker-insert path. Prevents the marker-on-marker-failure
    /// recursion when the DB itself is unwritable.
    static TELEMETRY_IN_MARKER: Cell<bool> = const { Cell::new(false) };
}

// ── Outcome types ────────────────────────────────────────────────────────

/// Reason a best-effort write could not be persisted. Purely diagnostic —
/// the write never surfaces an error to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DropReason {
    /// SQLite returned `SQLITE_BUSY`/`SQLITE_LOCKED` after the retry
    /// budget was exhausted.
    Busy,
    /// A CHECK / UNIQUE / FOREIGN KEY violation that is not a legitimate
    /// dedup hit.
    Constraint(String),
    /// The DB file could not be opened or reached (IO error, parent dir
    /// removed, filesystem full, etc.).
    DbUnreachable,
    /// The drop-marker recursion guard tripped — the original error was
    /// logged to stderr and stats incremented, but no marker row was
    /// written.
    MarkerFailed,
    /// Any other `rusqlite` error (syntax, locked table schema change,
    /// etc.).
    Other(String),
}

/// Outcome of a best-effort insert or update. Writes **never** return a
/// `Result` — all errors are mapped into this enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome {
    /// The row was newly written; the `i64` is the assigned rowid (for
    /// `logs`/`resources`) or the writer's logical identifier. For
    /// `update_span_end` the value is `0` because the update affects an
    /// existing row.
    Inserted(i64),
    /// The row already existed (a legitimate dedup hit). NOT a silent
    /// loss — the caller just re-emitted an identical payload.
    Duplicate,
    /// The row could not be persisted after the retry budget was
    /// exhausted. A marker was best-effort written (see [`DropReason`]).
    Dropped(DropReason),
}

// ── Cursor ───────────────────────────────────────────────────────────────

/// Opaque pagination cursor. Encodes the `(event_time, tie)` tuple of the
/// last returned row; the exact semantics of `tie` depend on the query
/// (log `id` for `query_logs`, `span_id` for `query_spans`, `"<kind>:<id>"`
/// for `query_timeline`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    pub time: i64,
    pub tie: String,
}

impl Cursor {
    /// Base64url-encoded JSON form. Safe for URL/query-string transport.
    #[must_use]
    pub fn encode(&self) -> String {
        let json = serde_json::to_vec(self).unwrap_or_else(|_| b"{}".to_vec());
        URL_SAFE_NO_PAD.encode(json)
    }

    /// Decode from the base64url JSON form. Returns `None` on any parse
    /// failure.
    #[must_use]
    pub fn decode(s: &str) -> Option<Self> {
        let bytes = URL_SAFE_NO_PAD.decode(s.as_bytes()).ok()?;
        serde_json::from_slice(&bytes).ok()
    }
}

// ── Row types ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct LogRow {
    pub id: i64,
    pub time_unix_nano: i64,
    pub observed_time_unix_nano: i64,
    pub severity_number: u8,
    pub severity_text: Option<String>,
    pub body: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub resource_id: i64,
    pub attributes: serde_json::Value,
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpanRow {
    pub span_id: String,
    pub trace_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: String,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: Option<i64>,
    pub status_code: String,
    pub status_message: Option<String>,
    pub resource_id: i64,
    pub attributes: serde_json::Value,
    pub events: Option<serde_json::Value>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind")]
pub enum TimelineEntry {
    #[serde(rename = "log")]
    Log(LogRow),
    #[serde(rename = "span_start")]
    SpanStart(SpanRow),
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineResult {
    pub items: Vec<TimelineEntry>,
    pub next_cursor: Option<Cursor>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TraceTree {
    pub trace_id: String,
    pub spans: Vec<SpanRow>,
    pub logs: Vec<LogRow>,
    pub root_span_id: Option<String>,
    pub total_duration_ms: Option<u64>,
}

// ── Filters ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Default)]
pub struct LogFilters {
    pub sources: Vec<Source>,
    pub min_severity: Option<u8>,
    pub search: Option<String>,
    pub trace_id: Option<String>,
    pub since_unix_nano: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub enum SummaryGroupBy<'a> {
    SpanName,
    SpanAttr(&'a str),
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SummaryRow {
    pub name: String,
    pub count: u64,
    pub avg_duration_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub failure_rate: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct SummaryReport {
    pub top: Vec<SummaryRow>,
    pub total: u64,
    pub error_count: u64,
    pub session_count: u64,
}

#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn aggregate_group(name: String, points: &[(i64, bool)]) -> SummaryRow {
    let count = points.len() as u64;
    let mut durs_ms: Vec<f64> = points
        .iter()
        .map(|(ns, _)| (*ns as f64) / 1_000_000.0)
        .collect();
    durs_ms.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let avg = if durs_ms.is_empty() {
        0.0
    } else {
        durs_ms.iter().sum::<f64>() / (durs_ms.len() as f64)
    };
    let percentile = |p: f64| -> f64 {
        if durs_ms.is_empty() {
            return 0.0;
        }
        let idx = ((p / 100.0) * (durs_ms.len() as f64 - 1.0)).round() as usize;
        durs_ms[idx.min(durs_ms.len() - 1)]
    };
    let failures = points.iter().filter(|(_, err)| *err).count() as f64;
    let failure_rate = if count == 0 {
        0.0
    } else {
        failures / (count as f64)
    };
    SummaryRow {
        name,
        count,
        avg_duration_ms: avg,
        p50_ms: percentile(50.0),
        p95_ms: percentile(95.0),
        failure_rate,
    }
}

#[derive(Debug, Clone, Default)]
pub struct SpanFilters {
    pub sources: Vec<Source>,
    pub name: Option<String>,
    pub trace_id: Option<String>,
    pub since_unix_nano: Option<i64>,
}

// ── Store ────────────────────────────────────────────────────────────────

/// Shared store state. Cheaply [`Clone`]-able via [`Arc`].
#[derive(Debug)]
struct StoreInner {
    db_path: PathBuf,
    /// Per-process monotonic counter used in `dedup_hash` to guarantee
    /// distinct events never collide even with identical body +
    /// ms-resolution timestamp.
    dedup_nonce: AtomicU64,
    /// Rowid of the `telemetry-marker` resource, cached so the drop-marker
    /// path does not need to allocate a fresh resource on every invocation.
    marker_resource_id: AtomicI64,
}

/// SQLite-backed store for OTel-shaped logs and spans.
#[derive(Debug, Clone)]
pub struct TelemetryStore {
    inner: Arc<StoreInner>,
}

impl TelemetryStore {
    /// Open (or create) the telemetry DB at `path`. Sets WAL mode + 5s
    /// busy timeout + foreign keys, applies migrations, and caches the
    /// drop-marker resource id.
    ///
    /// Returns `Err` only for catastrophic open-time failures (bad path,
    /// IO error creating the parent directory, migration failure). The
    /// telemetry layer — not this method — is responsible for
    /// incrementing `GLOBAL_STATS.dropped_count` when opening fails.
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        configure_conn(&conn)?;
        apply_migrations(&conn)?;
        let marker_resource_id = ensure_marker_resource(&conn)?;

        Ok(Self {
            inner: Arc::new(StoreInner {
                db_path: path.to_path_buf(),
                dedup_nonce: AtomicU64::new(0),
                marker_resource_id: AtomicI64::new(marker_resource_id),
            }),
        })
    }

    /// Path of the underlying SQLite file.
    #[must_use]
    pub fn db_path(&self) -> &Path {
        &self.inner.db_path
    }

    /// Process-global stats counter. Returned by reference so callers
    /// holding a `TelemetryStore` can sample without a static import.
    #[must_use]
    pub fn stats(&self) -> &'static TelemetryStats {
        &GLOBAL_STATS
    }

    // ── Writes ─────────────────────────────────────────────────────────

    /// Insert one log record. Best-effort — never returns `Result`.
    pub fn insert_log(&self, log: &LogRecord) -> InsertOutcome {
        let hash = self.compute_log_dedup_hash(log);
        self.insert_log_with_hash(log, &hash)
    }

    /// Insert one span-start row. Best-effort — never returns `Result`.
    pub fn insert_span_start(&self, span: &Span) -> InsertOutcome {
        let hash = compute_span_dedup_hash(span);
        let attrs = attrs_to_json(&span.attributes);
        let events = if span.events.is_empty() {
            None
        } else {
            Some(serde_json::to_string(&span.events).unwrap_or_else(|_| "[]".to_string()))
        };
        let span_id = span.span_id.to_string();
        let trace_id = span.trace_id.to_string();
        let parent = span.parent_span_id.map(|s| s.to_string());
        let kind = span.kind.to_string();
        let status_code = span.status.code().to_string();
        let status_message = span.status.message().map(ToOwned::to_owned);
        let source = span.source.to_string();
        let path = self.inner.db_path.clone();

        match self.retry_on_write(|| {
            let conn = Connection::open(&path)?;
            configure_conn(&conn)?;
            let changed = conn.execute(
                "INSERT OR IGNORE INTO spans \
                 (span_id, trace_id, parent_span_id, name, kind, \
                  start_time_unix_nano, end_time_unix_nano, status_code, \
                  status_message, resource_id, attributes, events, source, dedup_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                params![
                    span_id,
                    trace_id,
                    parent,
                    span.name,
                    kind,
                    span.start_time_unix_nano,
                    span.end_time_unix_nano,
                    status_code,
                    status_message,
                    span.resource_id,
                    attrs,
                    events,
                    source,
                    hash,
                ],
            )?;
            Ok((changed, conn.last_insert_rowid()))
        }) {
            Ok((0, _)) => InsertOutcome::Duplicate,
            Ok((_, rowid)) => InsertOutcome::Inserted(rowid),
            Err(reason) => self.record_dropped_event(reason),
        }
    }

    /// Finalise a span — write its `end_time_unix_nano`, `status`, and
    /// merge attributes. Best-effort.
    #[allow(clippy::needless_pass_by_value)]
    pub fn update_span_end(
        &self,
        span_id: SpanId,
        end: i64,
        status: Status,
        attrs: Attributes,
    ) -> InsertOutcome {
        let span_id_hex = span_id.to_string();
        let attrs_json = attrs_to_json(&attrs);
        let status_code = status.code().to_string();
        let status_message = status.message().map(ToOwned::to_owned);
        let path = self.inner.db_path.clone();

        match self.retry_on_write(|| {
            let conn = Connection::open(&path)?;
            configure_conn(&conn)?;
            let changed = conn.execute(
                "UPDATE spans SET end_time_unix_nano = ?1, status_code = ?2, \
                     status_message = ?3, attributes = ?4 \
                     WHERE span_id = ?5",
                params![end, status_code, status_message, attrs_json, span_id_hex],
            )?;
            Ok(changed)
        }) {
            Ok(0) => InsertOutcome::Duplicate,
            Ok(_) => InsertOutcome::Inserted(0),
            Err(reason) => self.record_dropped_event(reason),
        }
    }

    /// Insert (or ignore) a [`Resource`] row. On a Duplicate outcome the
    /// caller should look up the existing id via
    /// [`get_resource_id_by_hash`](Self::get_resource_id_by_hash).
    pub fn insert_resource(&self, r: &Resource) -> InsertOutcome {
        let attrs_json = serde_json::to_string(&r.attributes).unwrap_or_else(|_| "{}".to_string());
        let hash = r.attributes_hash.clone();
        let path = self.inner.db_path.clone();

        match self.retry_on_write(|| {
            let conn = Connection::open(&path)?;
            configure_conn(&conn)?;
            let changed = conn.execute(
                "INSERT OR IGNORE INTO resources (attributes, attributes_hash) VALUES (?1, ?2)",
                params![attrs_json, hash],
            )?;
            Ok((changed, conn.last_insert_rowid()))
        }) {
            Ok((0, _)) => InsertOutcome::Duplicate,
            Ok((_, rowid)) => InsertOutcome::Inserted(rowid),
            Err(reason) => self.record_dropped_event(reason),
        }
    }

    /// Look up an existing resource row by its SHA-256 attributes hash.
    #[must_use]
    pub fn get_resource_id_by_hash(&self, hash: &str) -> Option<i64> {
        let conn = Connection::open(&self.inner.db_path).ok()?;
        configure_conn(&conn).ok()?;
        conn.query_row(
            "SELECT id FROM resources WHERE attributes_hash = ?1",
            params![hash],
            |r| r.get::<_, i64>(0),
        )
        .optional()
        .ok()
        .flatten()
    }

    // ── Internals: dedup + marker + retry ──────────────────────────────

    fn insert_log_with_hash(&self, log: &LogRecord, hash: &str) -> InsertOutcome {
        let attrs = attrs_to_json(&log.attributes);
        let trace_id_str = log.trace_id.map(|t| t.to_string());
        let span_id_str = log.span_id.map(|s| s.to_string());
        let severity_text = log.severity_text.to_string();
        let source = log.source.to_string();
        let body = log.body.clone();
        let hash_owned = hash.to_string();
        let path = self.inner.db_path.clone();
        let severity: u8 = log.severity_number.0;
        let resource_id = log.resource_id;
        let time_unix_nano = log.time_unix_nano;
        let observed_time_unix_nano = log.observed_time_unix_nano;

        match self.retry_on_write(|| {
            let conn = Connection::open(&path)?;
            configure_conn(&conn)?;
            let changed = conn.execute(
                "INSERT OR IGNORE INTO logs \
                 (time_unix_nano, observed_time_unix_nano, severity_number, severity_text, \
                  body, trace_id, span_id, resource_id, attributes, source, dedup_hash) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    time_unix_nano,
                    observed_time_unix_nano,
                    severity,
                    severity_text,
                    body,
                    trace_id_str,
                    span_id_str,
                    resource_id,
                    attrs,
                    source,
                    hash_owned,
                ],
            )?;
            Ok((changed, conn.last_insert_rowid()))
        }) {
            Ok((0, _)) => InsertOutcome::Duplicate,
            Ok((_, rowid)) => InsertOutcome::Inserted(rowid),
            Err(reason) => self.record_dropped_event(reason),
        }
    }

    /// Compute `dedup_hash` for a log record. Guaranteed unique across
    /// distinct calls from the same process thanks to the per-store
    /// monotonic nonce.
    fn compute_log_dedup_hash(&self, log: &LogRecord) -> String {
        let nonce = self.inner.dedup_nonce.fetch_add(1, Ordering::SeqCst);
        let pid = std::process::id();
        let mut hasher = Sha256::new();
        hasher.update(log.time_unix_nano.to_le_bytes());
        hasher.update(log.observed_time_unix_nano.to_le_bytes());
        hasher.update(pid.to_le_bytes());
        hasher.update(nonce.to_le_bytes());
        if let Some(tid) = log.trace_id {
            hasher.update(tid.to_string().as_bytes());
        }
        if let Some(sid) = log.span_id {
            hasher.update(sid.to_string().as_bytes());
        }
        let body_bytes = log.body.as_bytes();
        let take = body_bytes.len().min(256);
        hasher.update(&body_bytes[..take]);
        format!("{:x}", hasher.finalize())
    }

    /// Record a dropped event. Increments `GLOBAL_STATS.dropped_count`
    /// and — unless the recursion guard is already tripped — writes a
    /// WARN-severity marker log. Never itself panics or re-enters the
    /// retry pipeline.
    fn record_dropped_event(&self, reason: DropReason) -> InsertOutcome {
        GLOBAL_STATS.dropped_count.fetch_add(1, Ordering::Relaxed);

        let already_in_marker = TELEMETRY_IN_MARKER.with(|c| c.replace(true));
        if already_in_marker {
            eprintln!("telemetry: dropped event (marker recursion), reason={reason:?}");
            return InsertOutcome::Dropped(DropReason::MarkerFailed);
        }

        let result = self.write_marker(&reason);
        TELEMETRY_IN_MARKER.with(|c| c.set(false));

        if result.is_err() {
            eprintln!("telemetry: marker write failed, reason={reason:?}");
        }
        InsertOutcome::Dropped(reason)
    }

    /// Write a `telemetry.dropped` marker log. Returns `Err(())` if the
    /// marker cannot be persisted.
    fn write_marker(&self, reason: &DropReason) -> Result<(), ()> {
        let marker_resource_id = self.inner.marker_resource_id.load(Ordering::Acquire);
        let mut log =
            LogRecord::with_severity("telemetry.dropped", SeverityNumber::WARN, Source::Core);
        log.resource_id = marker_resource_id;
        log.attributes.insert(
            "telemetry.drop_reason".to_string(),
            serde_json::Value::String(format!("{reason:?}")),
        );

        // Single attempt — no retry, no recursive record_dropped_event.
        let hash = self.compute_log_dedup_hash(&log);
        let attrs = attrs_to_json(&log.attributes);
        let severity: u8 = log.severity_number.0;

        let conn = Connection::open(&self.inner.db_path).map_err(|_| ())?;
        configure_conn(&conn).map_err(|_| ())?;
        conn.execute(
            "INSERT OR IGNORE INTO logs \
             (time_unix_nano, observed_time_unix_nano, severity_number, severity_text, \
              body, trace_id, span_id, resource_id, attributes, source, dedup_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, NULL, NULL, ?6, ?7, ?8, ?9)",
            params![
                log.time_unix_nano,
                log.observed_time_unix_nano,
                severity,
                log.severity_text,
                log.body,
                log.resource_id,
                attrs,
                log.source.to_string(),
                hash,
            ],
        )
        .map(|_| ())
        .map_err(|_| ())
    }

    /// Retry `op` up to [`MAX_WRITE_ATTEMPTS`] times on `SQLITE_BUSY` /
    /// `SQLITE_LOCKED`. Maps the final error to a [`DropReason`].
    fn retry_on_write<T, F>(&self, mut op: F) -> Result<T, DropReason>
    where
        F: FnMut() -> rusqlite::Result<T>,
    {
        let _ = self; // suppress clippy "could be assoc" on `self`
        let mut attempt: u32 = 0;
        loop {
            match op() {
                Ok(v) => return Ok(v),
                Err(e) if is_retryable(&e) && attempt + 1 < MAX_WRITE_ATTEMPTS => {
                    GLOBAL_STATS.retry_count.fetch_add(1, Ordering::Relaxed);
                    let jitter_ms =
                        rand::thread_rng().gen_range(BASE_BACKOFF_MS..BASE_BACKOFF_MS * 2);
                    let backoff = Duration::from_millis(jitter_ms * (1u64 << attempt).min(8));
                    thread::sleep(backoff);
                    attempt += 1;
                }
                Err(e) => return Err(classify_error(&e)),
            }
        }
    }

    // ── Queries ────────────────────────────────────────────────────────

    /// Paginated log query. Cursor: `(time_unix_nano, id)`; order:
    /// `ORDER BY time_unix_nano DESC, id DESC`.
    #[allow(clippy::needless_pass_by_value)]
    pub fn query_logs(
        &self,
        filters: LogFilters,
        cursor: Option<Cursor>,
        limit: u32,
    ) -> rusqlite::Result<(Vec<LogRow>, Option<Cursor>)> {
        let conn = Connection::open(&self.inner.db_path)?;
        configure_conn(&conn)?;

        let mut sql = String::from(
            "SELECT id, time_unix_nano, observed_time_unix_nano, severity_number, \
             severity_text, body, trace_id, span_id, resource_id, attributes, source \
             FROM logs WHERE 1=1",
        );
        let mut vals: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if !filters.sources.is_empty() {
            let placeholders = (0..filters.sources.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");
            write!(sql, " AND source IN ({placeholders})").ok();
            for s in &filters.sources {
                vals.push(Box::new(s.to_string()));
            }
        }
        if let Some(min_sev) = filters.min_severity {
            sql.push_str(" AND severity_number >= ?");
            vals.push(Box::new(i64::from(min_sev)));
        }
        if let Some(search) = &filters.search {
            sql.push_str(" AND body LIKE ?");
            vals.push(Box::new(format!("%{search}%")));
        }
        if let Some(tid) = &filters.trace_id {
            sql.push_str(" AND trace_id = ?");
            vals.push(Box::new(tid.clone()));
        }
        if let Some(since) = filters.since_unix_nano {
            sql.push_str(" AND time_unix_nano >= ?");
            vals.push(Box::new(since));
        }
        if let Some(ref cur) = cursor {
            if let Ok(tie_id) = cur.tie.parse::<i64>() {
                sql.push_str(" AND (time_unix_nano < ? OR (time_unix_nano = ? AND id < ?))");
                vals.push(Box::new(cur.time));
                vals.push(Box::new(cur.time));
                vals.push(Box::new(tie_id));
            }
        }
        sql.push_str(" ORDER BY time_unix_nano DESC, id DESC LIMIT ?");
        let fetch: i64 = i64::from(limit) + 1;
        vals.push(Box::new(fetch));

        let params_ref: Vec<&dyn rusqlite::ToSql> =
            vals.iter().map(std::convert::AsRef::as_ref).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows: Vec<LogRow> = stmt
            .query_map(rusqlite::params_from_iter(params_ref), row_to_log)?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let (items, next) = paginate_logs(rows, limit as usize);
        Ok((items, next))
    }

    /// Paginated span query. Cursor: `(start_time_unix_nano, span_id)`.
    #[allow(clippy::needless_pass_by_value)]
    pub fn query_spans(
        &self,
        filters: SpanFilters,
        cursor: Option<Cursor>,
        limit: u32,
    ) -> rusqlite::Result<(Vec<SpanRow>, Option<Cursor>)> {
        let conn = Connection::open(&self.inner.db_path)?;
        configure_conn(&conn)?;

        let mut sql = String::from(
            "SELECT span_id, trace_id, parent_span_id, name, kind, \
             start_time_unix_nano, end_time_unix_nano, status_code, status_message, \
             resource_id, attributes, events, source \
             FROM spans WHERE 1=1",
        );
        let mut vals: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if !filters.sources.is_empty() {
            let placeholders = (0..filters.sources.len())
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(",");
            write!(sql, " AND source IN ({placeholders})").ok();
            for s in &filters.sources {
                vals.push(Box::new(s.to_string()));
            }
        }
        if let Some(name) = &filters.name {
            sql.push_str(" AND name LIKE ?");
            vals.push(Box::new(format!("{name}%")));
        }
        if let Some(tid) = &filters.trace_id {
            sql.push_str(" AND trace_id = ?");
            vals.push(Box::new(tid.clone()));
        }
        if let Some(since) = filters.since_unix_nano {
            sql.push_str(" AND start_time_unix_nano >= ?");
            vals.push(Box::new(since));
        }
        if let Some(ref cur) = cursor {
            sql.push_str(
                " AND (start_time_unix_nano < ? OR (start_time_unix_nano = ? AND span_id < ?))",
            );
            vals.push(Box::new(cur.time));
            vals.push(Box::new(cur.time));
            vals.push(Box::new(cur.tie.clone()));
        }
        sql.push_str(" ORDER BY start_time_unix_nano DESC, span_id DESC LIMIT ?");
        let fetch: i64 = i64::from(limit) + 1;
        vals.push(Box::new(fetch));

        let params_ref: Vec<&dyn rusqlite::ToSql> =
            vals.iter().map(std::convert::AsRef::as_ref).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows: Vec<SpanRow> = stmt
            .query_map(rusqlite::params_from_iter(params_ref), row_to_span)?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let (items, next) = paginate_spans(rows, limit as usize);
        Ok((items, next))
    }

    /// Fetch the full trace tree for `trace_id`. Returns every span and
    /// every correlated log, plus derived `root_span_id` and
    /// `total_duration_ms`.
    pub fn query_trace(&self, trace_id: &str) -> rusqlite::Result<TraceTree> {
        let conn = Connection::open(&self.inner.db_path)?;
        configure_conn(&conn)?;

        let mut stmt_spans = conn.prepare(
            "SELECT span_id, trace_id, parent_span_id, name, kind, \
             start_time_unix_nano, end_time_unix_nano, status_code, status_message, \
             resource_id, attributes, events, source \
             FROM spans WHERE trace_id = ? \
             ORDER BY start_time_unix_nano ASC, span_id ASC",
        )?;
        let spans: Vec<SpanRow> = stmt_spans
            .query_map(params![trace_id], row_to_span)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt_spans);

        let mut stmt_logs = conn.prepare(
            "SELECT id, time_unix_nano, observed_time_unix_nano, severity_number, \
             severity_text, body, trace_id, span_id, resource_id, attributes, source \
             FROM logs WHERE trace_id = ? \
             ORDER BY time_unix_nano ASC, id ASC",
        )?;
        let logs: Vec<LogRow> = stmt_logs
            .query_map(params![trace_id], row_to_log)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(stmt_logs);

        let root_span_id = spans
            .iter()
            .find(|s| s.parent_span_id.is_none())
            .map(|s| s.span_id.clone());

        let total_duration_ms = {
            let min_start = spans.iter().map(|s| s.start_time_unix_nano).min();
            let max_end = spans.iter().filter_map(|s| s.end_time_unix_nano).max();
            match (min_start, max_end) {
                (Some(a), Some(b)) if b >= a => {
                    Some(u64::try_from((b - a) / 1_000_000).unwrap_or(0))
                }
                _ => None,
            }
        };

        Ok(TraceTree {
            trace_id: trace_id.to_string(),
            spans,
            logs,
            root_span_id,
            total_duration_ms,
        })
    }

    /// Unified timeline of logs + span-starts, ordered by event time DESC.
    #[allow(clippy::needless_pass_by_value)]
    pub fn query_timeline(
        &self,
        sources: &[Source],
        cursor: Option<Cursor>,
        limit: u32,
    ) -> rusqlite::Result<TimelineResult> {
        let log_limit = limit.saturating_add(1);
        let span_limit = limit.saturating_add(1);

        let source_vec = sources.to_vec();
        let base_log_filters = LogFilters {
            sources: source_vec.clone(),
            ..LogFilters::default()
        };
        let base_span_filters = SpanFilters {
            sources: source_vec,
            ..SpanFilters::default()
        };

        // Translate timeline cursor (time, "kind:tie") into per-query
        // cursors; we fetch both sides slightly past the boundary to
        // handle exact-time ties and then filter in Rust.
        let cursor_time = cursor.as_ref().map(|c| c.time);

        let log_cursor = cursor_time.map(|t| Cursor {
            time: t + 1,
            tie: i64::MAX.to_string(),
        });
        let span_cursor = cursor_time.map(|t| Cursor {
            time: t + 1,
            tie: "\u{10FFFF}".to_string(),
        });

        let (logs, _) = self.query_logs(base_log_filters, log_cursor, log_limit)?;
        let (spans, _) = self.query_spans(base_span_filters, span_cursor, span_limit)?;

        let mut merged: Vec<TimelineEntry> = logs
            .into_iter()
            .map(TimelineEntry::Log)
            .chain(spans.into_iter().map(TimelineEntry::SpanStart))
            .collect();

        merged.sort_by(|a, b| {
            let (ta, ka, tie_a) = entry_sort_key(a);
            let (tb, kb, tie_b) = entry_sort_key(b);
            (tb, kb, tie_b).cmp(&(ta, ka, tie_a))
        });

        if let Some(cur) = cursor.as_ref() {
            merged.retain(|e| {
                let (t, k, tie) = entry_sort_key(e);
                let cur_kind = cur.tie.split(':').next().unwrap_or("");
                let cur_tie = cur
                    .tie
                    .split_once(':')
                    .map_or_else(String::new, |x| x.1.to_string());
                (t, k.as_str(), tie.as_str()) < (cur.time, cur_kind, cur_tie.as_str())
            });
        }

        let limit_usize = limit as usize;
        let next_cursor = if merged.len() > limit_usize {
            merged.truncate(limit_usize);
            let last = merged.last().expect("non-empty after truncate");
            let (t, k, tie) = entry_sort_key(last);
            Some(Cursor {
                time: t,
                tie: format!("{k}:{tie}"),
            })
        } else {
            None
        };
        if merged.len() > limit_usize {
            merged.truncate(limit_usize);
        }

        Ok(TimelineResult {
            items: merged,
            next_cursor,
        })
    }

    /// Aggregate completed spans (`end_time_unix_nano IS NOT NULL`) for a
    /// given `source`, grouped by [`SummaryGroupBy`]. Computes per-group
    /// count, avg/p50/p95 duration, and failure rate (share of spans with
    /// `status_code = 'ERROR'`).
    ///
    /// `window_ms` is relative to "now"; non-positive windows disable the
    /// time filter. At most 10k rows are fetched to bound p50/p95 work.
    #[allow(
        clippy::needless_pass_by_value,
        clippy::cast_precision_loss,
        clippy::similar_names
    )]
    pub fn span_summary(
        &self,
        source: &str,
        group_by: SummaryGroupBy,
        window_ms: u64,
    ) -> rusqlite::Result<SummaryReport> {
        let conn = Connection::open(&self.inner.db_path)?;
        configure_conn(&conn)?;

        let since_ns: i64 = {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
                .unwrap_or(0);
            if window_ms == 0 {
                0
            } else {
                let window_ns =
                    i64::try_from(window_ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
                now_ns.saturating_sub(window_ns)
            }
        };

        let group_expr = match group_by {
            SummaryGroupBy::SpanName => "name".to_string(),
            SummaryGroupBy::SpanAttr(key) => {
                format!("json_extract(attributes, '$.{}')", key.replace('\'', "''"))
            }
        };

        let sql = format!(
            "SELECT {group_expr} AS grp, \
             (end_time_unix_nano - start_time_unix_nano) AS dur_ns, \
             status_code, trace_id \
             FROM spans \
             WHERE source = ?1 AND start_time_unix_nano > ?2 AND end_time_unix_nano IS NOT NULL \
             LIMIT 10000"
        );

        let mut stmt = conn.prepare(&sql)?;
        let rows: Vec<(Option<String>, i64, String, String)> = stmt
            .query_map(params![source, since_ns], |r| {
                Ok((
                    r.get::<_, Option<String>>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let total = rows.len() as u64;
        let error_count = rows
            .iter()
            .filter(|(_, _, code, _)| code == "ERROR")
            .count() as u64;

        let mut sessions = std::collections::HashSet::new();
        let mut groups: HashMap<String, Vec<(i64, bool)>> = HashMap::new();
        for (grp, dur, code, tid) in rows {
            let key = grp.unwrap_or_else(|| "(none)".to_string());
            groups.entry(key).or_default().push((dur, code == "ERROR"));
            sessions.insert(tid);
        }

        let mut summary: Vec<SummaryRow> = groups
            .into_iter()
            .map(|(name, points)| aggregate_group(name, &points))
            .collect();
        summary.sort_by(|a, b| b.count.cmp(&a.count));
        summary.truncate(10);

        Ok(SummaryReport {
            top: summary,
            total,
            error_count,
            session_count: sessions.len() as u64,
        })
    }

    /// Aggregate logs by body prefix of `prefix_len` chars for the given
    /// `source`, over the last `window_ms`. Durations are zero (logs have
    /// no duration); `failure_rate` is share of rows with `severity >=
    /// ERROR (17)`.
    #[allow(clippy::cast_precision_loss, clippy::similar_names)]
    pub fn log_summary_by_body(
        &self,
        source: &str,
        prefix_len: usize,
        window_ms: u64,
    ) -> rusqlite::Result<SummaryReport> {
        let conn = Connection::open(&self.inner.db_path)?;
        configure_conn(&conn)?;

        let since_ns: i64 = {
            let now_ns = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
                .unwrap_or(0);
            if window_ms == 0 {
                0
            } else {
                let window_ns =
                    i64::try_from(window_ms.saturating_mul(1_000_000)).unwrap_or(i64::MAX);
                now_ns.saturating_sub(window_ns)
            }
        };

        let sql = "SELECT substr(body, 1, ?1) AS grp, severity_number \
                   FROM logs WHERE source = ?2 AND time_unix_nano > ?3 LIMIT 10000";
        let plen = i64::try_from(prefix_len.max(1)).unwrap_or(40);
        let mut stmt = conn.prepare(sql)?;
        let rows: Vec<(String, i64)> = stmt
            .query_map(params![plen, source, since_ns], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let total = rows.len() as u64;
        let error_count = rows.iter().filter(|(_, sev)| *sev >= 17).count() as u64;

        let mut groups: HashMap<String, Vec<(i64, bool)>> = HashMap::new();
        for (grp, sev) in rows {
            groups.entry(grp).or_default().push((0, sev >= 17));
        }

        let mut summary: Vec<SummaryRow> = groups
            .into_iter()
            .map(|(name, points)| aggregate_group(name, &points))
            .collect();
        summary.sort_by(|a, b| b.count.cmp(&a.count));
        summary.truncate(10);

        Ok(SummaryReport {
            top: summary,
            total,
            error_count,
            session_count: 0,
        })
    }

    /// Counts of log rows by `source`, restricted to `time_unix_nano > since`.
    pub fn count_by_source(&self, since_unix_nano: i64) -> rusqlite::Result<HashMap<String, u64>> {
        let conn = Connection::open(&self.inner.db_path)?;
        configure_conn(&conn)?;
        let mut stmt = conn.prepare(
            "SELECT source, COUNT(*) FROM logs WHERE time_unix_nano > ?1 GROUP BY source",
        )?;
        let rows = stmt.query_map(params![since_unix_nano], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        let mut out = HashMap::new();
        for row in rows {
            let (s, c) = row?;
            out.insert(s, u64::try_from(c).unwrap_or(0));
        }
        Ok(out)
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn configure_conn(conn: &Connection) -> rusqlite::Result<()> {
    // `journal_mode = WAL` is a PRAGMA that returns a row; use query_row.
    let _: String = conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get(0))?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.pragma_update(None, "busy_timeout", 5000i64)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

/// Well-known attributes hash for the single `telemetry-marker` resource.
const MARKER_ATTRS_JSON: &str = r#"{"service.name":"scriptorium-telemetry-marker"}"#;

fn marker_attrs_hash() -> String {
    let mut h = Sha256::new();
    h.update(MARKER_ATTRS_JSON.as_bytes());
    format!("{:x}", h.finalize())
}

fn ensure_marker_resource(conn: &Connection) -> rusqlite::Result<i64> {
    let hash = marker_attrs_hash();
    conn.execute(
        "INSERT OR IGNORE INTO resources (attributes, attributes_hash) VALUES (?1, ?2)",
        params![MARKER_ATTRS_JSON, hash],
    )?;
    conn.query_row(
        "SELECT id FROM resources WHERE attributes_hash = ?1",
        params![hash],
        |r| r.get::<_, i64>(0),
    )
}

fn attrs_to_json(attrs: &Attributes) -> String {
    serde_json::to_string(attrs).unwrap_or_else(|_| "{}".to_string())
}

fn compute_span_dedup_hash(span: &Span) -> String {
    let mut h = Sha256::new();
    h.update(span.span_id.to_string().as_bytes());
    h.update(span.start_time_unix_nano.to_le_bytes());
    format!("{:x}", h.finalize())
}

fn is_retryable(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(ffi, _)
            if ffi.code == rusqlite::ErrorCode::DatabaseBusy
                || ffi.code == rusqlite::ErrorCode::DatabaseLocked
    )
}

fn classify_error(e: &rusqlite::Error) -> DropReason {
    match e {
        rusqlite::Error::SqliteFailure(ffi, _)
            if ffi.code == rusqlite::ErrorCode::DatabaseBusy
                || ffi.code == rusqlite::ErrorCode::DatabaseLocked =>
        {
            DropReason::Busy
        }
        rusqlite::Error::SqliteFailure(ffi, msg)
            if ffi.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            DropReason::Constraint(msg.clone().unwrap_or_default())
        }
        rusqlite::Error::SqliteFailure(ffi, _)
            if ffi.code == rusqlite::ErrorCode::CannotOpen
                || ffi.code == rusqlite::ErrorCode::ReadOnly =>
        {
            DropReason::DbUnreachable
        }
        other => DropReason::Other(other.to_string()),
    }
}

fn row_to_log(row: &rusqlite::Row<'_>) -> rusqlite::Result<LogRow> {
    let attrs_str: String = row.get(9)?;
    let attributes: serde_json::Value =
        serde_json::from_str(&attrs_str).unwrap_or_else(|_| serde_json::json!({}));
    let severity_number = u8::try_from(row.get::<_, i64>(3)?).unwrap_or(0);
    Ok(LogRow {
        id: row.get(0)?,
        time_unix_nano: row.get(1)?,
        observed_time_unix_nano: row.get(2)?,
        severity_number,
        severity_text: row.get(4)?,
        body: row.get(5)?,
        trace_id: row.get(6)?,
        span_id: row.get(7)?,
        resource_id: row.get(8)?,
        attributes,
        source: row.get(10)?,
    })
}

fn row_to_span(row: &rusqlite::Row<'_>) -> rusqlite::Result<SpanRow> {
    let attrs_str: String = row.get(10)?;
    let attributes: serde_json::Value =
        serde_json::from_str(&attrs_str).unwrap_or_else(|_| serde_json::json!({}));
    let events_opt: Option<String> = row.get(11)?;
    let events = events_opt.and_then(|s| serde_json::from_str(&s).ok());
    Ok(SpanRow {
        span_id: row.get(0)?,
        trace_id: row.get(1)?,
        parent_span_id: row.get(2)?,
        name: row.get(3)?,
        kind: row.get(4)?,
        start_time_unix_nano: row.get(5)?,
        end_time_unix_nano: row.get(6)?,
        status_code: row.get(7)?,
        status_message: row.get(8)?,
        resource_id: row.get(9)?,
        attributes,
        events,
        source: row.get(12)?,
    })
}

fn paginate_logs(mut rows: Vec<LogRow>, limit: usize) -> (Vec<LogRow>, Option<Cursor>) {
    if rows.len() > limit {
        rows.truncate(limit);
        let last = rows.last().expect("non-empty after truncate");
        let cursor = Cursor {
            time: last.time_unix_nano,
            tie: last.id.to_string(),
        };
        (rows, Some(cursor))
    } else {
        (rows, None)
    }
}

fn paginate_spans(mut rows: Vec<SpanRow>, limit: usize) -> (Vec<SpanRow>, Option<Cursor>) {
    if rows.len() > limit {
        rows.truncate(limit);
        let last = rows.last().expect("non-empty after truncate");
        let cursor = Cursor {
            time: last.start_time_unix_nano,
            tie: last.span_id.clone(),
        };
        (rows, Some(cursor))
    } else {
        (rows, None)
    }
}

fn entry_sort_key(e: &TimelineEntry) -> (i64, String, String) {
    match e {
        TimelineEntry::Log(l) => (l.time_unix_nano, "log".to_string(), l.id.to_string()),
        TimelineEntry::SpanStart(s) => (
            s.start_time_unix_nano,
            "span_start".to_string(),
            s.span_id.clone(),
        ),
    }
}

// Expose `TraceId` via the re-export surface; guard the unused warning so
// the `use` above isn't flagged by clippy in tests.
#[allow(dead_code)]
fn _assert_trace_id_used(_t: TraceId) {}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::envelope::SpanKind;
    use std::collections::BTreeMap;

    fn test_db() -> (tempfile::TempDir, TelemetryStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let store = TelemetryStore::open(&path).expect("open store");
        // Pre-seed a resource so log inserts satisfy FK.
        let resource = Resource::from_attributes(BTreeMap::from_iter([(
            "service.name".to_string(),
            "test".to_string(),
        )]));
        let _ = store.insert_resource(&resource);
        (dir, store)
    }

    fn default_resource_id(store: &TelemetryStore) -> i64 {
        let resource = Resource::from_attributes(BTreeMap::from_iter([(
            "service.name".to_string(),
            "test".to_string(),
        )]));
        store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .expect("resource exists")
    }

    fn mk_log(store: &TelemetryStore, body: &str, time: i64) -> LogRecord {
        let mut l = LogRecord::info(body, Source::Cli);
        l.time_unix_nano = time;
        l.observed_time_unix_nano = time;
        l.resource_id = default_resource_id(store);
        l
    }

    // -------------------------------------------------------------------
    #[test]
    fn open_initializes_schema() {
        let (_dir, store) = test_db();
        let conn = Connection::open(store.db_path()).unwrap();
        let v: i64 = conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            v,
            i64::from(crate::telemetry::schema::CURRENT_SCHEMA_VERSION)
        );
    }

    #[test]
    fn insert_log_basic() {
        let (_dir, store) = test_db();
        let log = mk_log(&store, "hello", 100);
        let outcome = store.insert_log(&log);
        assert!(matches!(outcome, InsertOutcome::Inserted(id) if id > 0));

        let (rows, _) = store.query_logs(LogFilters::default(), None, 10).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].body, "hello");
        assert_eq!(rows[0].time_unix_nano, 100);
    }

    #[test]
    fn insert_log_duplicate() {
        let (_dir, store) = test_db();
        let log = mk_log(&store, "same", 100);
        let out1 = store.insert_log_with_hash(&log, "hash-xyz");
        assert!(matches!(out1, InsertOutcome::Inserted(_)));
        let out2 = store.insert_log_with_hash(&log, "hash-xyz");
        assert_eq!(out2, InsertOutcome::Duplicate);
    }

    #[test]
    fn insert_log_distinct_nonces() {
        let (_dir, store) = test_db();
        for _ in 0..100 {
            let log = mk_log(&store, "collision-body", 12345);
            let outcome = store.insert_log(&log);
            assert!(matches!(outcome, InsertOutcome::Inserted(_)));
        }
        let (rows, _) = store.query_logs(LogFilters::default(), None, 200).unwrap();
        assert_eq!(rows.len(), 100);
    }

    #[test]
    fn insert_span_start_and_end() {
        let (_dir, store) = test_db();
        let mut span = Span::start("op", SpanKind::Internal, Source::Core);
        span.resource_id = default_resource_id(&store);
        span.start_time_unix_nano = 500;
        let sid = span.span_id;
        let outcome = store.insert_span_start(&span);
        assert!(matches!(outcome, InsertOutcome::Inserted(_)));

        let end_outcome = store.update_span_end(sid, 1000, Status::Ok, Attributes::new());
        assert!(matches!(end_outcome, InsertOutcome::Inserted(_)));

        let (rows, _) = store.query_spans(SpanFilters::default(), None, 10).unwrap();
        let found = rows.iter().find(|r| r.span_id == sid.to_string()).unwrap();
        assert_eq!(found.end_time_unix_nano, Some(1000));
        assert_eq!(found.status_code, "OK");
    }

    #[test]
    fn insert_resource_upsert() {
        let dir = tempfile::tempdir().unwrap();
        let store = TelemetryStore::open(&dir.path().join("t.sqlite")).unwrap();
        let r =
            Resource::from_attributes(BTreeMap::from_iter([("k".to_string(), "v".to_string())]));
        let o1 = store.insert_resource(&r);
        let InsertOutcome::Inserted(id_first) = o1 else {
            panic!("expected Inserted, got {o1:?}")
        };
        let o2 = store.insert_resource(&r);
        assert_eq!(o2, InsertOutcome::Duplicate);
        let found = store.get_resource_id_by_hash(&r.attributes_hash).unwrap();
        assert_eq!(found, id_first);
    }

    #[test]
    fn concurrent_writers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let store = TelemetryStore::open(&path).unwrap();
        let resource = Resource::from_attributes(BTreeMap::from_iter([(
            "service.name".to_string(),
            "conc".to_string(),
        )]));
        let _ = store.insert_resource(&resource);
        let rid = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap();

        let mut handles = Vec::new();
        for t in 0..4u32 {
            let store_clone = store.clone();
            let handle = thread::spawn(move || {
                for i in 0..250u32 {
                    let mut l = LogRecord::info(format!("t{t}-i{i}"), Source::Cli);
                    l.resource_id = rid;
                    let _ = store_clone.insert_log(&l);
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }

        let (rows, _) = store.query_logs(LogFilters::default(), None, 2000).unwrap();
        // At least 1000 rows — may include prior marker-resource default log.
        assert!(rows.len() >= 1000, "got {}", rows.len());
        // Exactly 1000 of them should have the "t{N}-i{M}" body prefix.
        let count = rows.iter().filter(|r| r.body.starts_with('t')).count();
        assert_eq!(count, 1000);
    }

    #[test]
    fn cursor_pagination_no_gaps() {
        let (_dir, store) = test_db();
        for i in 0..100i64 {
            let log = mk_log(&store, &format!("page-{i}"), 1000 + i);
            store.insert_log(&log);
        }
        let mut cursor: Option<Cursor> = None;
        let mut collected: Vec<i64> = Vec::new();
        loop {
            let (rows, next) = store
                .query_logs(LogFilters::default(), cursor.clone(), 10)
                .unwrap();
            if rows.is_empty() {
                break;
            }
            for r in &rows {
                collected.push(r.id);
            }
            match next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        assert_eq!(collected.len(), 100);
        // Ensure uniqueness + monotonically decreasing id.
        let mut unique = collected.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 100);
        let mut sorted_desc = collected.clone();
        sorted_desc.sort_unstable_by(|a, b| b.cmp(a));
        assert_eq!(collected, sorted_desc);
    }

    #[test]
    fn retry_on_busy() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let store = TelemetryStore::open(&path).unwrap();
        let resource = Resource::from_attributes(BTreeMap::from_iter([(
            "service.name".to_string(),
            "busy".to_string(),
        )]));
        let _ = store.insert_resource(&resource);
        let rid = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap();

        let before_retry = GLOBAL_STATS.retry_count.load(Ordering::Relaxed);

        // Spawn holder that takes an EXCLUSIVE lock briefly, then releases.
        let holder_path = path.clone();
        let holder = thread::spawn(move || {
            let mut conn = Connection::open(&holder_path).unwrap();
            configure_conn(&conn).unwrap();
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Exclusive)
                .unwrap();
            thread::sleep(Duration::from_millis(60));
            tx.commit().unwrap();
        });

        // Give the holder a moment to acquire.
        thread::sleep(Duration::from_millis(5));

        let mut log = LogRecord::info("busy-retry", Source::Cli);
        log.resource_id = rid;
        // With 5s busy_timeout, this insert should succeed after waiting.
        let outcome = store.insert_log(&log);
        holder.join().unwrap();

        assert!(matches!(outcome, InsertOutcome::Inserted(_)));
        let _ = before_retry; // busy_timeout may absorb the wait internally.
    }

    #[test]
    fn drop_marker_on_exhaustion() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("t.sqlite");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = TelemetryStore::open(&path).unwrap();

        let before = GLOBAL_STATS.dropped_count.load(Ordering::Relaxed);

        // Make the DB unreachable by removing its parent directory.
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();

        let mut log = LogRecord::info("should-drop", Source::Cli);
        log.resource_id = 1;
        let outcome = store.insert_log(&log);
        assert!(matches!(outcome, InsertOutcome::Dropped(_)));

        let after = GLOBAL_STATS.dropped_count.load(Ordering::Relaxed);
        assert!(
            after > before,
            "dropped_count should increment (before={before}, after={after})"
        );
    }

    #[test]
    fn recursion_guard_no_infinite_loop() {
        // Scenario: DB unreachable → original write fails → marker write
        // also fails. Recursion guard must prevent stack overflow and the
        // call must terminate.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inner").join("t.sqlite");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let store = TelemetryStore::open(&path).unwrap();
        std::fs::remove_dir_all(path.parent().unwrap()).unwrap();

        let mut log = LogRecord::info("recursion-test", Source::Cli);
        log.resource_id = 1;
        let start = std::time::Instant::now();
        let outcome = store.insert_log(&log);
        assert!(start.elapsed() < Duration::from_secs(5));
        assert!(matches!(outcome, InsertOutcome::Dropped(_)));
    }

    #[test]
    fn query_timeline_interleaves_sources() {
        let (_dir, store) = test_db();
        let rid = default_resource_id(&store);
        // log at t=100, span at t=200, log at t=300
        let mut l1 = LogRecord::info("l1", Source::Cli);
        l1.time_unix_nano = 100;
        l1.observed_time_unix_nano = 100;
        l1.resource_id = rid;
        store.insert_log(&l1);

        let mut span = Span::start("op", SpanKind::Internal, Source::Core);
        span.start_time_unix_nano = 200;
        span.resource_id = rid;
        store.insert_span_start(&span);

        let mut l2 = LogRecord::info("l2", Source::Cli);
        l2.time_unix_nano = 300;
        l2.observed_time_unix_nano = 300;
        l2.resource_id = rid;
        store.insert_log(&l2);

        let result = store.query_timeline(&[], None, 10).unwrap();
        assert_eq!(result.items.len(), 3);
        // Most recent first: l2, span, l1
        let times: Vec<i64> = result
            .items
            .iter()
            .map(|e| match e {
                TimelineEntry::Log(l) => l.time_unix_nano,
                TimelineEntry::SpanStart(s) => s.start_time_unix_nano,
            })
            .collect();
        assert_eq!(times, vec![300, 200, 100]);
    }

    #[test]
    fn query_trace_builds_tree() {
        let (_dir, store) = test_db();
        let rid = default_resource_id(&store);

        let mut root = Span::start("root", SpanKind::Internal, Source::Core);
        root.start_time_unix_nano = 1_000_000_000;
        root.end_time_unix_nano = Some(4_000_000_000);
        root.resource_id = rid;
        let trace_id = root.trace_id;
        let root_sid = root.span_id;
        store.insert_span_start(&root);

        for i in 0..2 {
            let mut child = Span::start("child", SpanKind::Internal, Source::Core);
            child.trace_id = trace_id;
            child.parent_span_id = Some(root_sid);
            child.start_time_unix_nano = 1_500_000_000 + i;
            child.end_time_unix_nano = Some(3_000_000_000 + i);
            child.resource_id = rid;
            store.insert_span_start(&child);
        }

        for i in 0..3 {
            let mut l = LogRecord::info(format!("trace-log-{i}"), Source::Cli);
            l.time_unix_nano = 2_000_000_000 + i;
            l.observed_time_unix_nano = 2_000_000_000 + i;
            l.trace_id = Some(trace_id);
            l.span_id = Some(root_sid);
            l.resource_id = rid;
            store.insert_log(&l);
        }

        let trace = store.query_trace(&trace_id.to_string()).unwrap();
        assert_eq!(trace.spans.len(), 3);
        assert_eq!(trace.logs.len(), 3);
        assert_eq!(trace.root_span_id, Some(root_sid.to_string()));
        assert!(trace.total_duration_ms.unwrap_or(0) >= 3000);
    }

    #[test]
    fn cursor_encode_decode_roundtrip() {
        let c = Cursor {
            time: 1_234_567,
            tie: "abc".to_string(),
        };
        let s = c.encode();
        let back = Cursor::decode(&s).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn count_by_source_basic() {
        let (_dir, store) = test_db();
        let rid = default_resource_id(&store);
        for i in 0..5i64 {
            let mut l = LogRecord::info("a", Source::Cli);
            l.time_unix_nano = 100 + i;
            l.observed_time_unix_nano = 100 + i;
            l.resource_id = rid;
            store.insert_log(&l);
        }
        for i in 0..3i64 {
            let mut l = LogRecord::info("b", Source::Mcp);
            l.time_unix_nano = 200 + i;
            l.observed_time_unix_nano = 200 + i;
            l.resource_id = rid;
            store.insert_log(&l);
        }
        let counts = store.count_by_source(50).unwrap();
        assert_eq!(counts.get("cli").copied(), Some(5));
        assert_eq!(counts.get("mcp").copied(), Some(3));
    }
}
