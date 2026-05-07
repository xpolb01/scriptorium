#![allow(clippy::doc_markdown)]

//! Backfill migration: copy `hook_events` rows into the new OTel-shaped
//! `logs` and `spans` tables introduced in migration 001.
//!
//! This module is a one-shot data migration. It is **idempotent** — the
//! deterministic `dedup_hash` (derived from the existing
//! `hook_events.raw_json_hash`) ensures re-running only imports unseen
//! rows. It is also **safe under partial failure** — malformed rows are
//! skipped (not the whole batch) and counters accumulate in a
//! [`BackfillReport`].
//!
//! # Mapping rules
//!
//! - `"stop"` (per-turn) → one `LogRecord` with
//!   `body = "hook.turn_scored"`, severity `INFO`, source `Hook`.
//! - `"subagent-stop"` / `"subagent_stop"` → one `LogRecord` **and** one
//!   instant `Span` (`start=end=time_unix_nano`, `name="subagent"`).
//! - `"session-end"` / `"session_end"` → one `LogRecord` with
//!   `body = "hook.session_end"` and aggregate attrs.
//! - Any other value → one `LogRecord` at severity `WARN` with
//!   `body = "hook.unknown_type"` (never silently dropped).
//!
//! # Why raw SQL instead of `TelemetryStore::insert_log`
//!
//! The store's normal dedup uses a per-process nonce (see
//! `compute_log_dedup_hash` in `store.rs`) which would prevent idempotent
//! re-runs for backfill rows. The backfill therefore writes directly via
//! raw SQL using `sha256("backfill:log:" || raw_json_hash)` as the
//! `dedup_hash`. This bypass is scoped strictly to migration; live
//! telemetry still flows through the store's public API.

use crate::telemetry::envelope::{Attributes, Source, SpanKind};
use crate::telemetry::resource::Resource;
use crate::telemetry::store::{InsertOutcome, TelemetryStore};
use chrono::DateTime;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

/// Report returned by [`backfill_hook_events`]. All counters accumulate
/// across the entire scan and are never reset mid-run.
#[derive(Debug, Clone, Default, Serialize)]
pub struct BackfillReport {
    /// Total rows observed in `hook_events`.
    pub rows_read: u64,
    /// Rows that produced a new `logs` row.
    pub logs_inserted: u64,
    /// Rows that produced a new `spans` row (subagent-stop only).
    pub spans_inserted: u64,
    /// Rows whose `dedup_hash` collided with an existing `logs` row.
    pub skipped_duplicates: u64,
    /// Rows that could not be parsed (missing `ts`, unparseable `ts`,
    /// missing `raw_json_hash`, etc.).
    pub skipped_malformed: u64,
    /// Rows that failed to insert for any other reason (CHECK / FK /
    /// unexpected SQLite error). Never short-circuits the loop.
    pub errors: u64,
    /// Reflects the `dry_run` flag passed to [`backfill_hook_events`].
    pub dry_run: bool,
}

/// Backfill every row in `hook_events` into the new `logs` (and, for
/// subagent-stop, `spans`) tables. Idempotent; safe to re-run.
///
/// When `dry_run` is true, no rows are written; counters report what
/// *would* have been inserted (treated as if every non-malformed row is
/// a new insert — dedup state is not consulted).
///
/// # Errors
///
/// Returns `Err` only if the initial DB connection cannot be opened,
/// PRAGMAs cannot be applied, or the `hook_events` probe statement
/// itself fails. Per-row parse/insert failures are recorded in the
/// report and the scan continues.
pub fn backfill_hook_events(
    store: &TelemetryStore,
    dry_run: bool,
) -> rusqlite::Result<BackfillReport> {
    let mut report = BackfillReport {
        dry_run,
        ..Default::default()
    };

    // Resolve (or create) the Hook resource row once per run.
    let resource = Resource::detect(Source::Hook, None);
    let resource_id = match store.get_resource_id_by_hash(&resource.attributes_hash) {
        Some(id) => id,
        None => match store.insert_resource(&resource) {
            InsertOutcome::Inserted(id) => id,
            InsertOutcome::Duplicate => store
                .get_resource_id_by_hash(&resource.attributes_hash)
                .unwrap_or(0),
            InsertOutcome::Dropped(reason) => {
                eprintln!("backfill: could not allocate hook resource: {reason:?}");
                report.errors += 1;
                return Ok(report);
            }
        },
    };

    let conn = Connection::open(store.db_path())?;
    conn.pragma_update(None, "busy_timeout", 5000i64)?;
    conn.pragma_update(None, "foreign_keys", "ON")?;

    // If `hook_events` does not exist yet, the report is trivially empty.
    let has_table: bool = conn
        .query_row(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='hook_events'",
            [],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .is_some();
    if !has_table {
        return Ok(report);
    }

    let mut stmt = conn.prepare(
        "SELECT hook_type, session_id, ts, raw_json, score, decision, signals, \
         agent_type, cwd, threshold, metrics, privacy_veto, peak_turn_score, \
         session_aggregate_score, final_score, turn_count, subagent_count, \
         raw_json_hash, parent_session_id \
         FROM hook_events ORDER BY ts ASC",
    )?;

    let rows = stmt.query_map([], |r| {
        Ok(RawRow {
            hook_type: r.get(0)?,
            session_id: r.get(1)?,
            ts: r.get(2)?,
            raw_json: r.get(3)?,
            score: r.get(4)?,
            decision: r.get(5)?,
            signals: r.get(6)?,
            agent_type: r.get(7)?,
            cwd: r.get(8)?,
            threshold: r.get(9)?,
            metrics: r.get(10)?,
            privacy_veto: r.get(11)?,
            peak_turn_score: r.get(12)?,
            session_aggregate_score: r.get(13)?,
            final_score: r.get(14)?,
            turn_count: r.get(15)?,
            subagent_count: r.get(16)?,
            raw_json_hash: r.get(17)?,
            parent_session_id: r.get(18)?,
        })
    })?;

    for row_result in rows {
        report.rows_read += 1;
        let row = match row_result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("backfill: skip unreadable row: {e}");
                report.skipped_malformed += 1;
                continue;
            }
        };

        let Some(time_unix_nano) = parse_ts_to_nanos(row.ts.as_deref()) else {
            eprintln!("backfill: skip row with unparseable ts: {:?}", row.ts);
            report.skipped_malformed += 1;
            continue;
        };

        let Some(raw_hash) = row
            .raw_json_hash
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
        else {
            eprintln!("backfill: skip row missing raw_json_hash");
            report.skipped_malformed += 1;
            continue;
        };

        emit_row(
            &conn,
            &row,
            time_unix_nano,
            &raw_hash,
            resource_id,
            &mut report,
        );
    }

    Ok(report)
}

// ── Row helpers ──────────────────────────────────────────────────────────

/// Column tuple read from `hook_events`. Every field is optional — the
/// live schema has many NOT NULL constraints, but we defend against
/// test-seeded NULLs and future schema relaxations.
struct RawRow {
    hook_type: Option<String>,
    session_id: Option<String>,
    ts: Option<String>,
    #[allow(dead_code)]
    raw_json: Option<String>,
    score: Option<i64>,
    decision: Option<String>,
    signals: Option<String>,
    agent_type: Option<String>,
    cwd: Option<String>,
    threshold: Option<i64>,
    metrics: Option<String>,
    privacy_veto: Option<String>,
    peak_turn_score: Option<i64>,
    session_aggregate_score: Option<i64>,
    final_score: Option<i64>,
    turn_count: Option<i64>,
    subagent_count: Option<i64>,
    raw_json_hash: Option<String>,
    parent_session_id: Option<String>,
}

enum HookKind {
    Stop,
    SubagentStop,
    SessionEnd,
    Unknown,
}

fn normalize_hook_type(raw: &str) -> HookKind {
    match raw {
        "stop" => HookKind::Stop,
        "subagent-stop" | "subagent_stop" => HookKind::SubagentStop,
        "session-end" | "session_end" => HookKind::SessionEnd,
        _ => HookKind::Unknown,
    }
}

fn emit_row(
    conn: &Connection,
    row: &RawRow,
    time_unix_nano: i64,
    raw_hash: &str,
    resource_id: i64,
    report: &mut BackfillReport,
) {
    let hook_type_raw = row.hook_type.as_deref().unwrap_or("");
    let kind = normalize_hook_type(hook_type_raw);
    let (trace_id, span_id) = derive_ids(raw_hash);

    let attrs = build_attrs(row, hook_type_raw);
    let attrs_json = serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());

    let (body, severity_number, severity_text, emit_span) = match kind {
        HookKind::Stop => ("hook.turn_scored", 9u8, "INFO", false),
        HookKind::SubagentStop => ("hook.subagent_stop", 9u8, "INFO", true),
        HookKind::SessionEnd => ("hook.session_end", 9u8, "INFO", false),
        HookKind::Unknown => ("hook.unknown_type", 13u8, "WARN", false),
    };

    let log_dedup = deterministic_dedup("log", raw_hash);

    if report.dry_run {
        report.logs_inserted += 1;
    } else {
        match conn.execute(
            "INSERT OR IGNORE INTO logs \
             (time_unix_nano, observed_time_unix_nano, severity_number, severity_text, \
              body, trace_id, span_id, resource_id, attributes, source, dedup_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                time_unix_nano,
                time_unix_nano,
                i64::from(severity_number),
                severity_text,
                body,
                &trace_id,
                &span_id,
                resource_id,
                attrs_json,
                "hook",
                log_dedup,
            ],
        ) {
            Ok(0) => report.skipped_duplicates += 1,
            Ok(_) => report.logs_inserted += 1,
            Err(e) => {
                eprintln!("backfill: log insert failed (hook_type={hook_type_raw}): {e}");
                report.errors += 1;
            }
        }
    }

    if emit_span {
        emit_span_row(
            conn,
            row,
            time_unix_nano,
            raw_hash,
            resource_id,
            &trace_id,
            &span_id,
            report,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_span_row(
    conn: &Connection,
    row: &RawRow,
    time_unix_nano: i64,
    raw_hash: &str,
    resource_id: i64,
    trace_id: &str,
    span_id: &str,
    report: &mut BackfillReport,
) {
    let mut attrs = Attributes::new();
    if let Some(a) = &row.agent_type {
        attrs.insert("agent.type".to_string(), Value::String(a.clone()));
    }
    if let Some(sid) = &row.session_id {
        attrs.insert("session.id".to_string(), Value::String(sid.clone()));
    }
    attrs.insert("telemetry.backfilled".to_string(), Value::Bool(true));
    let span_attrs_json = serde_json::to_string(&attrs).unwrap_or_else(|_| "{}".to_string());
    let span_dedup = deterministic_dedup("span", raw_hash);

    if report.dry_run {
        report.spans_inserted += 1;
        return;
    }

    match conn.execute(
        "INSERT OR IGNORE INTO spans \
         (span_id, trace_id, parent_span_id, name, kind, \
          start_time_unix_nano, end_time_unix_nano, status_code, \
          status_message, resource_id, attributes, events, source, dedup_hash) \
         VALUES (?1, ?2, NULL, ?3, ?4, ?5, ?6, ?7, NULL, ?8, ?9, NULL, ?10, ?11)",
        params![
            span_id,
            trace_id,
            "subagent",
            SpanKind::Internal.to_string(),
            time_unix_nano,
            time_unix_nano,
            "OK",
            resource_id,
            span_attrs_json,
            "hook",
            span_dedup,
        ],
    ) {
        Ok(0) => { /* span dup — log dedup already recorded the outcome */ }
        Ok(_) => report.spans_inserted += 1,
        Err(e) => {
            eprintln!("backfill: span insert failed: {e}");
            report.errors += 1;
        }
    }
}

fn build_attrs(row: &RawRow, hook_type_raw: &str) -> Attributes {
    let mut a = Attributes::new();
    if let Some(v) = &row.session_id {
        a.insert("session.id".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = &row.parent_session_id {
        a.insert("session.parent_id".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = &row.cwd {
        a.insert("cwd".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = &row.agent_type {
        a.insert("agent.type".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = row.score {
        a.insert("classifier.score".to_string(), Value::from(v));
    }
    if let Some(v) = row.threshold {
        a.insert("classifier.threshold".to_string(), Value::from(v));
    }
    if let Some(v) = &row.decision {
        a.insert("classifier.decision".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = &row.signals {
        a.insert("classifier.signals".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = &row.metrics {
        a.insert("classifier.metrics".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = &row.privacy_veto {
        a.insert("privacy.veto".to_string(), Value::String(v.clone()));
    }
    if let Some(v) = row.peak_turn_score {
        a.insert("session.peak_turn_score".to_string(), Value::from(v));
    }
    if let Some(v) = row.session_aggregate_score {
        a.insert("session.aggregate_score".to_string(), Value::from(v));
    }
    if let Some(v) = row.final_score {
        a.insert("session.final_score".to_string(), Value::from(v));
    }
    if let Some(v) = row.turn_count {
        a.insert("session.turn_count".to_string(), Value::from(v));
    }
    if let Some(v) = row.subagent_count {
        a.insert("session.subagent_count".to_string(), Value::from(v));
    }
    a.insert("telemetry.backfilled".to_string(), Value::Bool(true));
    a.insert(
        "telemetry.source_hook_type".to_string(),
        Value::String(hook_type_raw.to_string()),
    );
    a
}

/// SHA-256 of `"backfill:<kind>:<raw_json_hash>"`. Produces a stable
/// 64-char lowercase hex string suitable for the `logs.dedup_hash` or
/// `spans.dedup_hash` column.
fn deterministic_dedup(kind: &str, raw_json_hash: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"backfill:");
    h.update(kind.as_bytes());
    h.update(b":");
    h.update(raw_json_hash.as_bytes());
    format!("{:x}", h.finalize())
}

/// Derive deterministic OTel-shaped `(trace_id, span_id)` from the
/// existing `raw_json_hash`. Since `raw_json_hash` is a 64-char lowercase
/// hex sha256, the first 32 chars become the trace id and the next 16
/// become the span id. Short inputs are right-padded with `'0'` for
/// robustness.
fn derive_ids(raw_json_hash: &str) -> (String, String) {
    let mut src = raw_json_hash.to_string();
    while src.len() < 48 {
        src.push('0');
    }
    (src[..32].to_string(), src[32..48].to_string())
}

/// Parse the `hook_events.ts` column to nanoseconds since Unix epoch.
///
/// Accepts RFC-3339 / ISO-8601 (the format `HooksStore::insert_event`
/// writes). Falls back to bare integer epoch milliseconds. Returns
/// `None` on any parse failure — the caller bumps `skipped_malformed`.
fn parse_ts_to_nanos(ts: Option<&str>) -> Option<i64> {
    let s = ts?.trim();
    if s.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_nanos_opt();
    }
    // Tolerate epoch-ms fallback (bare integer).
    if let Ok(ms) = s.parse::<i64>() {
        return ms.checked_mul(1_000_000);
    }
    None
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use tempfile::TempDir;

    /// DDL mirror of `hooks_store.rs` — every column and constraint EXCEPT
    /// the `NOT NULL` on `ts` (so malformed-row tests can exercise the
    /// parse-failure path via invalid strings without having to disable
    /// the constraint mid-test). The live schema has `ts TEXT NOT NULL`;
    /// the backfill code treats an unparseable `ts` as malformed either
    /// way.
    const HOOK_EVENTS_DDL: &str = "CREATE TABLE IF NOT EXISTS hook_events (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        ts                      TEXT,
        session_id              TEXT NOT NULL,
        parent_session_id       TEXT,
        hook_type               TEXT NOT NULL DEFAULT 'stop',
        source                  TEXT,
        cwd                     TEXT,
        score                   INTEGER,
        threshold               INTEGER DEFAULT 6,
        signals                 TEXT,
        metrics                 TEXT,
        agent_type              TEXT,
        privacy_veto            TEXT,
        decision                TEXT,
        peak_turn_score         INTEGER,
        session_aggregate_score INTEGER,
        final_score             INTEGER,
        turn_count              INTEGER,
        subagent_count          INTEGER,
        raw_json                TEXT,
        raw_json_hash           TEXT NOT NULL UNIQUE
    );";

    fn fresh_db() -> (TempDir, TelemetryStore) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let store = TelemetryStore::open(&path).expect("open store");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "DROP VIEW IF EXISTS hook_events; DROP TRIGGER IF EXISTS hook_events_insert;",
        )
        .unwrap();
        conn.execute_batch(HOOK_EVENTS_DDL).unwrap();
        (dir, store)
    }

    #[allow(clippy::too_many_arguments)]
    fn seed(
        store: &TelemetryStore,
        hook_type: &str,
        ts: Option<&str>,
        hash: &str,
        session_id: &str,
        agent_type: Option<&str>,
        score: Option<i64>,
        signals: Option<&str>,
    ) {
        let conn = Connection::open(store.db_path()).unwrap();
        conn.execute(
            "INSERT INTO hook_events (ts, session_id, hook_type, agent_type, score, signals, \
             raw_json, raw_json_hash) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![ts, session_id, hook_type, agent_type, score, signals, "{}", hash,],
        )
        .unwrap();
    }

    fn log_count(store: &TelemetryStore) -> i64 {
        let conn = Connection::open(store.db_path()).unwrap();
        conn.query_row("SELECT COUNT(*) FROM logs", [], |r| r.get(0))
            .unwrap()
    }

    fn span_count(store: &TelemetryStore) -> i64 {
        let conn = Connection::open(store.db_path()).unwrap();
        conn.query_row("SELECT COUNT(*) FROM spans", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn backfill_row_types() {
        let (_dir, store) = fresh_db();
        seed(
            &store,
            "stop",
            Some("2026-04-17T10:00:00Z"),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
            "sess-a",
            None,
            Some(7),
            Some(r#"["long_output"]"#),
        );
        seed(
            &store,
            "subagent-stop",
            Some("2026-04-17T10:01:00Z"),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2",
            "sess-a",
            Some("explore"),
            None,
            None,
        );
        seed(
            &store,
            "session-end",
            Some("2026-04-17T10:02:00Z"),
            "ccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc3",
            "sess-a",
            None,
            None,
            None,
        );

        let report = backfill_hook_events(&store, false).unwrap();
        assert_eq!(report.rows_read, 3, "report={report:?}");
        assert_eq!(report.logs_inserted, 3, "report={report:?}");
        assert_eq!(report.spans_inserted, 1, "report={report:?}");
        assert_eq!(report.skipped_duplicates, 0);
        assert_eq!(report.skipped_malformed, 0);
        assert_eq!(report.errors, 0);
        assert!(!report.dry_run);
        assert_eq!(log_count(&store), 3);
        assert_eq!(span_count(&store), 1);
    }

    #[test]
    fn idempotent() {
        let (_dir, store) = fresh_db();
        seed(
            &store,
            "stop",
            Some("2026-04-17T10:00:00Z"),
            "1111111111111111111111111111111111111111111111111111111111111111",
            "s1",
            None,
            Some(5),
            None,
        );
        seed(
            &store,
            "subagent-stop",
            Some("2026-04-17T10:00:01Z"),
            "2222222222222222222222222222222222222222222222222222222222222222",
            "s1",
            Some("librarian"),
            None,
            None,
        );

        let first = backfill_hook_events(&store, false).unwrap();
        assert_eq!(first.logs_inserted, 2);
        assert_eq!(first.spans_inserted, 1);
        assert_eq!(first.skipped_duplicates, 0);

        let second = backfill_hook_events(&store, false).unwrap();
        assert_eq!(second.logs_inserted, 0, "report={second:?}");
        assert_eq!(second.spans_inserted, 0);
        assert_eq!(second.skipped_duplicates, 2);
        assert_eq!(log_count(&store), 2);
        assert_eq!(span_count(&store), 1);
    }

    #[test]
    fn malformed_row_skipped() {
        let (_dir, store) = fresh_db();
        seed(
            &store,
            "stop",
            Some("2026-04-17T10:00:00Z"),
            "aaaa111111111111111111111111111111111111111111111111111111111111",
            "s",
            None,
            Some(3),
            None,
        );
        // Invalid ts string — parser returns None → counted as malformed.
        seed(
            &store,
            "stop",
            Some("not-a-real-date"),
            "bbbb222222222222222222222222222222222222222222222222222222222222",
            "s",
            None,
            None,
            None,
        );
        // NULL ts — same outcome.
        seed(
            &store,
            "stop",
            None,
            "cccc333333333333333333333333333333333333333333333333333333333333",
            "s",
            None,
            None,
            None,
        );

        let report = backfill_hook_events(&store, false).unwrap();
        assert_eq!(report.rows_read, 3);
        assert_eq!(report.logs_inserted, 1);
        assert_eq!(report.skipped_malformed, 2);
        assert_eq!(report.errors, 0);
        // Migration completed despite malformed rows.
        assert_eq!(log_count(&store), 1);
    }

    #[test]
    fn unknown_hook_type() {
        let (_dir, store) = fresh_db();
        seed(
            &store,
            "unknown-future-type",
            Some("2026-04-17T10:00:00Z"),
            "dddd444444444444444444444444444444444444444444444444444444444444",
            "s",
            None,
            None,
            None,
        );

        let report = backfill_hook_events(&store, false).unwrap();
        assert_eq!(report.rows_read, 1);
        assert_eq!(report.logs_inserted, 1);
        assert_eq!(report.skipped_malformed, 0);

        let conn = Connection::open(store.db_path()).unwrap();
        let (body, severity_number, severity_text): (String, i64, String) = conn
            .query_row(
                "SELECT body, severity_number, severity_text FROM logs LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(body, "hook.unknown_type");
        assert_eq!(severity_number, 13);
        assert_eq!(severity_text, "WARN");
    }

    #[test]
    fn dry_run_no_writes() {
        let (_dir, store) = fresh_db();
        seed(
            &store,
            "stop",
            Some("2026-04-17T10:00:00Z"),
            "eeee555555555555555555555555555555555555555555555555555555555555",
            "s",
            None,
            Some(7),
            None,
        );
        seed(
            &store,
            "subagent-stop",
            Some("2026-04-17T10:00:01Z"),
            "ffff666666666666666666666666666666666666666666666666666666666666",
            "s",
            Some("metis"),
            None,
            None,
        );

        let report = backfill_hook_events(&store, true).unwrap();
        assert!(report.dry_run);
        assert_eq!(report.rows_read, 2);
        assert_eq!(report.logs_inserted, 2);
        assert_eq!(report.spans_inserted, 1);
        assert_eq!(log_count(&store), 0, "dry_run must not write logs");
        assert_eq!(span_count(&store), 0, "dry_run must not write spans");
    }

    #[test]
    fn subagent_stop_both_variants() {
        let (_dir, store) = fresh_db();
        seed(
            &store,
            "subagent-stop",
            Some("2026-04-17T10:00:00Z"),
            "1010101010101010101010101010101010101010101010101010101010101010",
            "s",
            Some("explore"),
            None,
            None,
        );
        seed(
            &store,
            "subagent_stop",
            Some("2026-04-17T10:00:01Z"),
            "2020202020202020202020202020202020202020202020202020202020202020",
            "s",
            Some("librarian"),
            None,
            None,
        );

        let report = backfill_hook_events(&store, false).unwrap();
        assert_eq!(report.rows_read, 2);
        assert_eq!(report.logs_inserted, 2, "report={report:?}");
        assert_eq!(report.spans_inserted, 2, "report={report:?}");
        assert_eq!(log_count(&store), 2);
        assert_eq!(span_count(&store), 2);
    }

    #[test]
    fn no_hook_events_table() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        let store = TelemetryStore::open(&path).unwrap();
        // Intentionally do NOT create `hook_events`.
        let report = backfill_hook_events(&store, false).unwrap();
        assert_eq!(report.rows_read, 0);
        assert_eq!(report.logs_inserted, 0);
        assert_eq!(report.errors, 0);
    }

    #[test]
    fn derive_ids_deterministic() {
        let (t1, s1) =
            derive_ids("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let (t2, s2) =
            derive_ids("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(t1, t2);
        assert_eq!(s1, s2);
        assert_eq!(t1.len(), 32);
        assert_eq!(s1.len(), 16);
    }

    #[test]
    fn parse_ts_rfc3339() {
        let n = parse_ts_to_nanos(Some("2026-04-17T10:00:00Z")).unwrap();
        assert!(n > 0);
    }

    #[test]
    fn parse_ts_fallback_epoch_ms() {
        let n = parse_ts_to_nanos(Some("1700000000000")).unwrap();
        assert_eq!(n, 1_700_000_000_000 * 1_000_000);
    }

    #[test]
    fn parse_ts_rejects_garbage() {
        assert_eq!(parse_ts_to_nanos(Some("garbage")), None);
        assert_eq!(parse_ts_to_nanos(Some("")), None);
        assert_eq!(parse_ts_to_nanos(None), None);
    }
}
