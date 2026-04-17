#![allow(clippy::doc_markdown)]

//! `scriptorium log` subcommands: `emit` (stdin → SQLite) and `tail`
//! (filtered live stream).
//!
//! `emit` is the bash-accessible telemetry writer used by hook scripts —
//! it is designed to be forgiving of malformed input and **always exits 0**
//! (hook reliability invariant). Validation failures are converted into
//! marker logs so the caller still sees evidence of the miss.

use std::io::Read;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::{Args, Subcommand};
use miette::{miette, Result};
use scriptorium_core::telemetry::envelope::SpanId;
use scriptorium_core::telemetry::{
    Attributes, InsertOutcome, LogFilters, LogRecord, LogRow, Resource, SeverityNumber, Source,
    TelemetryStore, TraceContext, TraceId,
};
use serde_json::Value;

#[derive(Debug, Args)]
pub struct LogCommand {
    #[command(subcommand)]
    pub action: LogAction,
}

#[derive(Debug, Subcommand)]
pub enum LogAction {
    /// Emit an OTel-shaped log entry from stdin JSON. ALWAYS exits 0.
    Emit,

    /// Tail telemetry logs with filters.
    Tail {
        /// Filter by source (hook|cli|mcp|core). Repeatable.
        #[arg(long)]
        source: Vec<String>,
        /// Minimum OTel severity number (1..=24).
        #[arg(long)]
        severity: Option<u8>,
        /// Lower bound (ISO-8601). Logs strictly after this time.
        #[arg(long)]
        since: Option<String>,
        /// Max rows per poll (default 50, max 1000).
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// Poll every 500ms and stream new rows (`tail -f` semantics).
        #[arg(long)]
        follow: bool,
        /// Filter by `attributes.session_id`.
        #[arg(long)]
        session: Option<String>,
        /// Emit a single JSON aggregate line instead of per-row output.
        #[arg(long)]
        aggregate: bool,
        /// Structured JSON output (one `LogRow` per line).
        #[arg(long)]
        json: bool,
    },
}

/// Entry point for the `log` subtree.
pub fn run(cmd: LogCommand, db_path: PathBuf) -> Result<()> {
    match cmd.action {
        LogAction::Emit => {
            emit(&db_path);
            Ok(())
        }
        LogAction::Tail {
            source,
            severity,
            since,
            limit,
            follow,
            session,
            aggregate,
            json,
        } => tail(
            &db_path,
            &source,
            severity,
            since.as_deref(),
            limit,
            follow,
            session.as_deref(),
            aggregate,
            json,
        ),
    }
}

// ── emit ────────────────────────────────────────────────────────────────

/// Consume stdin JSON, write a log row, exit 0.
///
/// Errors (bad JSON, invalid source, write failure) are reported to
/// stderr; a marker log is written for certain classes of bad input.
fn emit(db_path: &PathBuf) {
    let mut raw = String::new();
    if let Err(e) = std::io::stdin().read_to_string(&mut raw) {
        eprintln!("scriptorium log emit: stdin read: {e}");
        return;
    }
    if raw.trim().is_empty() {
        return;
    }

    let parsed: Value = match serde_json::from_str(&raw) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("scriptorium log emit: invalid JSON: {e}");
            return;
        }
    };

    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let store = match TelemetryStore::open(db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scriptorium log emit: open store: {e}");
            return;
        }
    };

    let raw_source = parsed
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let source = match Source::from_str(&raw_source) {
        Ok(s) => s,
        Err(_) => {
            eprintln!("scriptorium log emit: invalid source: {raw_source:?}");
            emit_invalid_source_marker(&store, &raw_source);
            return;
        }
    };

    let body = parsed
        .get("body")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if body.is_empty() {
        eprintln!("scriptorium log emit: missing required field: body");
        return;
    }

    let severity = parsed
        .get("severity")
        .map_or(SeverityNumber::INFO, parse_severity);

    let mut attributes = attrs_from_value(parsed.get("attributes"));

    // Trace/span id normalization: invalid hex → NULL + invalid-id marker.
    let mut invalid_trace_hit = false;
    let trace_id_stdin = parsed.get("trace_id").and_then(Value::as_str);
    let mut trace_id: Option<TraceId> = None;
    if let Some(raw_tid) = trace_id_stdin {
        match normalize_hex(raw_tid, 32).and_then(|h| TraceId::from_str(&h).ok()) {
            Some(t) => trace_id = Some(t),
            None => {
                invalid_trace_hit = true;
                attributes.insert(
                    "telemetry.invalid_trace_id".to_string(),
                    Value::String(raw_tid.to_string()),
                );
            }
        }
    }

    let span_id_stdin = parsed.get("span_id").and_then(Value::as_str);
    let mut span_id: Option<SpanId> = None;
    if let Some(raw_sid) = span_id_stdin {
        match normalize_hex(raw_sid, 16).and_then(|h| SpanId::from_str(&h).ok()) {
            Some(s) => span_id = Some(s),
            None => {
                attributes.insert(
                    "telemetry.invalid_span_id".to_string(),
                    Value::String(raw_sid.to_string()),
                );
            }
        }
    }

    // Env correlation. If stdin didn't provide a trace_id, inherit from
    // SCRIPTORIUM_TRACEPARENT — including the env span_id unless stdin
    // explicitly set span_id (stdin wins). If both env and stdin are
    // missing, synthesize a new root.
    if trace_id.is_none() {
        if let Some(ctx) = TraceContext::from_env() {
            trace_id = Some(ctx.trace_id);
            if span_id.is_none() {
                span_id = Some(ctx.span_id);
            }
        } else {
            let ctx = TraceContext::new_root(None, None);
            trace_id = Some(ctx.trace_id);
            if span_id.is_none() {
                span_id = Some(ctx.span_id);
            }
        }
    }

    let resource = Resource::detect(source, None);
    let _ = store.insert_resource(&resource);
    let resource_id = store
        .get_resource_id_by_hash(&resource.attributes_hash)
        .unwrap_or(1);

    let mut log = LogRecord::with_severity(body, severity, source);
    log.attributes = attributes;
    log.trace_id = trace_id;
    log.span_id = span_id;
    log.resource_id = resource_id;
    if let Some(t) = parsed.get("time_unix_nano").and_then(Value::as_i64) {
        log.time_unix_nano = t;
        log.observed_time_unix_nano = t;
    }

    match store.insert_log(&log) {
        InsertOutcome::Inserted(_) | InsertOutcome::Duplicate => {}
        InsertOutcome::Dropped(reason) => {
            eprintln!("scriptorium log emit: dropped: {reason:?}");
        }
    }

    if invalid_trace_hit {
        let mut marker =
            LogRecord::with_severity("telemetry.invalid_id", SeverityNumber::WARN, Source::Core);
        marker.resource_id = resource_id;
        let _ = store.insert_log(&marker);
    }
}

fn emit_invalid_source_marker(store: &TelemetryStore, original: &str) {
    let resource = Resource::detect(Source::Core, None);
    let _ = store.insert_resource(&resource);
    let resource_id = store
        .get_resource_id_by_hash(&resource.attributes_hash)
        .unwrap_or(1);
    let mut marker = LogRecord::with_severity(
        "telemetry.invalid_source",
        SeverityNumber::WARN,
        Source::Core,
    );
    marker.attributes.insert(
        "original_source".to_string(),
        Value::String(original.to_string()),
    );
    marker.resource_id = resource_id;
    let _ = store.insert_log(&marker);
}

// ── tail ────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
fn tail(
    db_path: &PathBuf,
    sources: &[String],
    severity: Option<u8>,
    since: Option<&str>,
    limit: u32,
    follow: bool,
    session: Option<&str>,
    aggregate: bool,
    json: bool,
) -> Result<()> {
    let limit = limit.clamp(1, 1000);
    let store = TelemetryStore::open(db_path).map_err(|e| miette!("open store: {e}"))?;

    let mut parsed_sources = Vec::new();
    for s in sources {
        parsed_sources.push(Source::from_str(s).map_err(|_| miette!("invalid source: {s}"))?);
    }

    let since_nanos = since.and_then(parse_iso_to_nanos);

    if aggregate {
        let filters = LogFilters {
            sources: parsed_sources.clone(),
            min_severity: severity,
            search: None,
            trace_id: None,
            since_unix_nano: since_nanos,
        };
        let (rows, _) = store
            .query_logs(filters, None, limit)
            .map_err(|e| miette!("query: {e}"))?;
        let filtered = filter_session(rows, session);
        let count = filtered.len();
        let sum_score: i64 = filtered
            .iter()
            .filter_map(|r| r.attributes.get("score").and_then(Value::as_i64))
            .sum();
        println!(r#"{{"count":{count},"sum_score":{sum_score}}}"#);
        return Ok(());
    }

    let mut watermark: Option<i64> = since_nanos;
    loop {
        let filters = LogFilters {
            sources: parsed_sources.clone(),
            min_severity: severity,
            search: None,
            trace_id: None,
            since_unix_nano: watermark,
        };
        let (mut rows, _) = store
            .query_logs(filters, None, limit)
            .map_err(|e| miette!("query: {e}"))?;
        rows = filter_session(rows, session);
        // Query returns DESC; display ascending.
        rows.sort_by_key(|r| r.time_unix_nano);
        for row in &rows {
            if json {
                let s = serde_json::to_string(row).unwrap_or_default();
                println!("{s}");
            } else {
                let ts_ms = row.time_unix_nano / 1_000_000;
                let sev = row.severity_text.as_deref().unwrap_or("?");
                println!("{ts_ms}ms [{}] [{sev}] {}", row.source, row.body);
            }
            watermark = Some(row.time_unix_nano + 1);
        }
        if !follow {
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Ok(())
}

fn filter_session(rows: Vec<LogRow>, session: Option<&str>) -> Vec<LogRow> {
    match session {
        Some(want) => rows
            .into_iter()
            .filter(|r| {
                r.attributes
                    .get("session_id")
                    .and_then(Value::as_str)
                    .is_some_and(|s| s == want)
            })
            .collect(),
        None => rows,
    }
}

// ── helpers ─────────────────────────────────────────────────────────────

fn parse_severity(v: &Value) -> SeverityNumber {
    match v {
        Value::Number(n) => {
            let n = n.as_u64().unwrap_or(u64::from(SeverityNumber::INFO.0));
            SeverityNumber(u8::try_from(n).unwrap_or(SeverityNumber::INFO.0))
        }
        Value::String(s) => match s.to_ascii_uppercase().as_str() {
            "TRACE" => SeverityNumber::TRACE,
            "DEBUG" => SeverityNumber::DEBUG,
            "INFO" => SeverityNumber::INFO,
            "WARN" | "WARNING" => SeverityNumber::WARN,
            "ERROR" => SeverityNumber::ERROR,
            "FATAL" => SeverityNumber::FATAL,
            _ => SeverityNumber::INFO,
        },
        _ => SeverityNumber::INFO,
    }
}

fn attrs_from_value(v: Option<&Value>) -> Attributes {
    let mut out = Attributes::new();
    if let Some(Value::Object(map)) = v {
        for (k, val) in map {
            out.insert(k.clone(), val.clone());
        }
    }
    out
}

fn normalize_hex(s: &str, len: usize) -> Option<String> {
    let lower = s.to_ascii_lowercase();
    if lower.len() == len && lower.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(lower)
    } else {
        None
    }
}

fn parse_iso_to_nanos(s: &str) -> Option<i64> {
    let dt = chrono::DateTime::parse_from_rfc3339(s).ok()?;
    let secs = dt.timestamp();
    let nanos = i64::from(dt.timestamp_subsec_nanos());
    Some(secs.checked_mul(1_000_000_000)?.checked_add(nanos)?)
}

// ── tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_db() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        (dir, path)
    }

    #[test]
    fn emit_round_trip_direct() {
        // Drives the internal logic without spawning a binary: simulate
        // by feeding parsed JSON through the helpers used by `emit`.
        let (_dir, path) = tmp_db();
        let store = TelemetryStore::open(&path).unwrap();

        let parsed: Value = serde_json::from_str(
            r#"{"severity":"INFO","body":"hello","source":"cli","attributes":{}}"#,
        )
        .unwrap();

        let source = Source::from_str(parsed.get("source").unwrap().as_str().unwrap()).unwrap();
        let body = parsed.get("body").unwrap().as_str().unwrap().to_string();
        let severity = parse_severity(parsed.get("severity").unwrap());

        let resource = Resource::detect(source, None);
        let _ = store.insert_resource(&resource);
        let rid = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap();
        let mut log = LogRecord::with_severity(body, severity, source);
        log.resource_id = rid;
        let out = store.insert_log(&log);
        assert!(matches!(out, InsertOutcome::Inserted(_)));

        let (rows, _) = store.query_logs(LogFilters::default(), None, 10).unwrap();
        assert!(rows.iter().any(|r| r.body == "hello" && r.source == "cli"));
    }

    #[test]
    fn emit_invalid_source_writes_marker() {
        let (_dir, path) = tmp_db();
        let store = TelemetryStore::open(&path).unwrap();
        emit_invalid_source_marker(&store, "invalid");

        let (rows, _) = store.query_logs(LogFilters::default(), None, 50).unwrap();
        let marker = rows
            .iter()
            .find(|r| r.body == "telemetry.invalid_source")
            .expect("marker present");
        assert_eq!(marker.source, "core");
        assert_eq!(
            marker
                .attributes
                .get("original_source")
                .and_then(Value::as_str),
            Some("invalid")
        );
    }

    #[test]
    fn emit_invalid_trace_id_stored_null_plus_marker() {
        let (_dir, path) = tmp_db();
        let store = TelemetryStore::open(&path).unwrap();

        // Simulate: invalid hex in trace_id
        let raw_tid = "abc";
        let valid = normalize_hex(raw_tid, 32).and_then(|h| TraceId::from_str(&h).ok());
        assert!(valid.is_none(), "sanity: 'abc' is not 32 hex");

        let mut attrs = Attributes::new();
        attrs.insert(
            "telemetry.invalid_trace_id".to_string(),
            Value::String(raw_tid.to_string()),
        );
        let mut log = LogRecord::with_severity("bad trace", SeverityNumber::INFO, Source::Cli);
        log.attributes = attrs;
        log.trace_id = None;
        let resource = Resource::detect(Source::Cli, None);
        let _ = store.insert_resource(&resource);
        log.resource_id = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap();
        let _ = store.insert_log(&log);

        let mut marker =
            LogRecord::with_severity("telemetry.invalid_id", SeverityNumber::WARN, Source::Core);
        marker.resource_id = log.resource_id;
        let _ = store.insert_log(&marker);

        let (rows, _) = store.query_logs(LogFilters::default(), None, 50).unwrap();
        let bad = rows.iter().find(|r| r.body == "bad trace").unwrap();
        assert!(bad.trace_id.is_none());
        assert_eq!(
            bad.attributes
                .get("telemetry.invalid_trace_id")
                .and_then(Value::as_str),
            Some("abc")
        );
        assert!(rows.iter().any(|r| r.body == "telemetry.invalid_id"));
    }

    #[test]
    fn parse_severity_by_name() {
        assert_eq!(
            parse_severity(&Value::String("INFO".into())),
            SeverityNumber::INFO
        );
        assert_eq!(
            parse_severity(&Value::String("warn".into())),
            SeverityNumber::WARN
        );
        assert_eq!(
            parse_severity(&serde_json::json!(17)),
            SeverityNumber::ERROR
        );
        assert_eq!(parse_severity(&Value::Null), SeverityNumber::INFO);
    }

    #[test]
    fn normalize_hex_accepts_uppercase() {
        assert_eq!(
            normalize_hex("AABBCCDDEEFF0011", 16).as_deref(),
            Some("aabbccddeeff0011")
        );
        assert!(normalize_hex("abc", 32).is_none());
        assert!(normalize_hex("zz", 2).is_none());
    }

    #[test]
    fn parse_iso_round_trip() {
        let n = parse_iso_to_nanos("2026-04-17T00:00:00Z").unwrap();
        assert!(n > 1_700_000_000_000_000_000);
    }
}
