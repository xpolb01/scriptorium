#![allow(clippy::doc_markdown)]

//! `scriptorium trace` subcommands: `inspect`, `new-root`, `span start`,
//! `span end`.
//!
//! The span subcommands are bash-accessible span emitters used by
//! hook scripts (T16). They **always exit 0** (hook reliability invariant).
//!
//! **`trace new-root` is a developer helper only.** Hooks MUST NOT call
//! it — they rely on internal root generation inside `scriptorium log emit`
//! and `scriptorium span start` when `SCRIPTORIUM_TRACEPARENT` is absent.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;

use clap::{Args, Subcommand};
use miette::{miette, Result};
use scriptorium_core::telemetry::{
    Attributes, Resource, Source, Span, SpanEvent, SpanId, SpanKind, SpanRow, Status,
    TelemetryStore, TraceContext, TraceId,
};
use serde_json::Value;

#[derive(Debug, Args)]
pub struct TraceCommand {
    #[command(subcommand)]
    pub action: TraceAction,
}

#[derive(Debug, Subcommand)]
pub enum TraceAction {
    /// Render a trace tree by trace_id.
    Inspect {
        trace_id: String,
        #[arg(long)]
        json: bool,
    },

    /// Generate a new W3C traceparent. Developer helper ONLY.
    /// Hooks MUST NOT call this — they rely on internal root generation.
    NewRoot {
        #[arg(long)]
        session_id: Option<String>,
        #[arg(long)]
        turn_id: Option<String>,
    },

    /// Emit a span-start record. Prints `{"trace_id":"...","span_id":"..."}`
    /// on stdout so bash can capture the ids. ALWAYS exits 0.
    SpanStart {
        #[arg(long)]
        name: String,
        #[arg(long, default_value = "INTERNAL")]
        kind: String,
        #[arg(long, default_value = "{}")]
        attributes: String,
        #[arg(long)]
        source: String,
        #[arg(long)]
        trace_id: Option<String>,
        #[arg(long)]
        parent_span_id: Option<String>,
    },

    /// Close an in-flight span. ALWAYS exits 0.
    SpanEnd {
        #[arg(long)]
        span_id: String,
        #[arg(long, default_value = "OK")]
        status: String,
        #[arg(long)]
        status_message: Option<String>,
        #[arg(long, default_value = "{}")]
        attributes: String,
    },
}

/// Entry point for the `trace` subtree.
pub fn run(cmd: TraceCommand, db_path: PathBuf) -> Result<()> {
    match cmd.action {
        TraceAction::Inspect { trace_id, json } => inspect(&db_path, &trace_id, json),
        TraceAction::NewRoot {
            session_id,
            turn_id,
        } => {
            let ctx = TraceContext::new_root(session_id, turn_id);
            println!("{}", ctx.to_traceparent());
            Ok(())
        }
        TraceAction::SpanStart {
            name,
            kind,
            attributes,
            source,
            trace_id,
            parent_span_id,
        } => {
            span_start(
                &db_path,
                name,
                &kind,
                &attributes,
                &source,
                trace_id.as_deref(),
                parent_span_id.as_deref(),
            );
            Ok(())
        }
        TraceAction::SpanEnd {
            span_id,
            status,
            status_message,
            attributes,
        } => {
            span_end(&db_path, &span_id, &status, status_message, &attributes);
            Ok(())
        }
    }
}

// ── span start ─────────────────────────────────────────────────────────

fn span_start(
    db_path: &PathBuf,
    name: String,
    kind: &str,
    attrs_json: &str,
    source_str: &str,
    trace_id_arg: Option<&str>,
    parent_span_id_arg: Option<&str>,
) {
    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let source = match Source::from_str(source_str) {
        Ok(s) => s,
        Err(_) => {
            eprintln!("scriptorium span start: invalid source: {source_str:?}");
            return;
        }
    };
    let store = match TelemetryStore::open(db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scriptorium span start: open store: {e}");
            return;
        }
    };

    // Trace id precedence: --trace-id arg → SCRIPTORIUM_TRACEPARENT env → new random.
    let env_ctx = TraceContext::from_env();
    let trace = trace_id_arg
        .and_then(|s| normalize_hex(s, 32))
        .and_then(|s| TraceId::from_str(&s).ok())
        .or_else(|| env_ctx.as_ref().map(|c| c.trace_id))
        .unwrap_or_else(TraceId::new_random);

    // Parent span id: explicit arg takes precedence. If not provided and we
    // inherited the trace from env, use the env span as parent.
    let parent = parent_span_id_arg
        .and_then(|s| normalize_hex(s, 16))
        .and_then(|s| SpanId::from_str(&s).ok())
        .or_else(|| {
            if trace_id_arg.is_none() {
                env_ctx.as_ref().map(|c| c.span_id)
            } else {
                None
            }
        });

    let span_kind = parse_kind(kind);
    let attrs = parse_attributes(attrs_json);

    let resource = Resource::detect(source, None);
    let _ = store.insert_resource(&resource);
    let resource_id = store
        .get_resource_id_by_hash(&resource.attributes_hash)
        .unwrap_or(1);

    let new_span_id = SpanId::new_random();
    let span = Span {
        span_id: new_span_id,
        trace_id: trace,
        parent_span_id: parent,
        name,
        kind: span_kind,
        start_time_unix_nano: now_unix_nanos(),
        end_time_unix_nano: None,
        status: Status::Unset,
        attributes: attrs,
        events: Vec::<SpanEvent>::new(),
        source,
        resource_id,
    };
    let _ = store.insert_span_start(&span);

    println!(r#"{{"trace_id":"{trace}","span_id":"{new_span_id}"}}"#);
}

// ── span end ───────────────────────────────────────────────────────────

fn span_end(
    db_path: &PathBuf,
    span_id: &str,
    status: &str,
    status_message: Option<String>,
    attrs_json: &str,
) {
    let sid = match normalize_hex(span_id, 16).and_then(|s| SpanId::from_str(&s).ok()) {
        Some(s) => s,
        None => {
            eprintln!("scriptorium span end: invalid span_id: {span_id:?}");
            return;
        }
    };
    let store = match TelemetryStore::open(db_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("scriptorium span end: open store: {e}");
            return;
        }
    };
    let status_val = match status.to_ascii_uppercase().as_str() {
        "OK" => Status::Ok,
        "ERROR" => Status::Error(status_message.unwrap_or_default()),
        _ => Status::Unset,
    };
    let attrs = parse_attributes(attrs_json);
    let end = now_unix_nanos();
    let _ = store.update_span_end(sid, end, status_val, attrs);
}

// ── inspect ────────────────────────────────────────────────────────────

fn inspect(db_path: &PathBuf, trace_id: &str, json: bool) -> Result<()> {
    let store = TelemetryStore::open(db_path).map_err(|e| miette!("open store: {e}"))?;
    let tree = store
        .query_trace(trace_id)
        .map_err(|e| miette!("query: {e}"))?;

    if json {
        let s = serde_json::to_string_pretty(&tree).map_err(|e| miette!("json: {e}"))?;
        println!("{s}");
        return Ok(());
    }

    let dur = tree
        .total_duration_ms
        .map_or_else(|| "???ms".to_string(), |d| format!("{d}ms"));
    println!("trace: {}  duration: {dur}", tree.trace_id);

    if tree.spans.is_empty() && tree.logs.is_empty() {
        println!("  (no spans or logs)");
        return Ok(());
    }

    // parent_span_id → [children] index for O(1) lookup during DFS.
    let mut children: BTreeMap<Option<String>, Vec<&SpanRow>> = BTreeMap::new();
    for s in &tree.spans {
        children
            .entry(s.parent_span_id.clone())
            .or_default()
            .push(s);
    }

    for root in tree.spans.iter().filter(|s| s.parent_span_id.is_none()) {
        print_span(root, &children, 1);
    }

    // Logs not attached to any known span (orphans).
    let known_spans: std::collections::BTreeSet<&str> =
        tree.spans.iter().map(|s| s.span_id.as_str()).collect();
    for log in &tree.logs {
        if let Some(sid) = &log.span_id {
            if known_spans.contains(sid.as_str()) {
                continue;
            }
        }
        let sev = log.severity_text.as_deref().unwrap_or("?");
        println!("  [{sev}] log: {:?}", log.body);
    }

    Ok(())
}

fn print_span(
    span: &SpanRow,
    children_map: &BTreeMap<Option<String>, Vec<&SpanRow>>,
    depth: usize,
) {
    let indent = "  ".repeat(depth);
    let dur = match span.end_time_unix_nano {
        Some(e) => format!("{}ms", (e - span.start_time_unix_nano) / 1_000_000),
        None => "???ms".to_string(),
    };
    let attrs_str = match &span.attributes {
        Value::Object(map) if !map.is_empty() => {
            format!("  {}", serde_json::to_string(map).unwrap_or_default())
        }
        _ => String::new(),
    };
    println!(
        "{indent}[{} {}] {} {}{}",
        span.kind, span.status_code, span.name, dur, attrs_str
    );
    if let Some(kids) = children_map.get(&Some(span.span_id.clone())) {
        for c in kids {
            print_span(c, children_map, depth + 1);
        }
    }
}

// ── helpers ────────────────────────────────────────────────────────────

fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn parse_kind(s: &str) -> SpanKind {
    match s.to_ascii_uppercase().as_str() {
        "SERVER" => SpanKind::Server,
        "CLIENT" => SpanKind::Client,
        "PRODUCER" => SpanKind::Producer,
        "CONSUMER" => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}

fn parse_attributes(s: &str) -> Attributes {
    let mut out = Attributes::new();
    if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(s) {
        for (k, v) in map {
            out.insert(k, v);
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

// ── tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use scriptorium_core::telemetry::TelemetryStore;

    fn tmp_db() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.sqlite");
        (dir, path)
    }

    #[test]
    fn new_root_format() {
        let ctx = TraceContext::new_root(None, None);
        let tp = ctx.to_traceparent();
        assert_eq!(tp.len(), 55);
        assert!(tp.starts_with("00-"));
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0].len(), 2);
        assert_eq!(parts[1].len(), 32);
        assert_eq!(parts[2].len(), 16);
        assert_eq!(parts[3].len(), 2);
        for p in parts {
            assert!(p.bytes().all(|b| b.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn parse_kind_defaults_to_internal() {
        assert_eq!(parse_kind("SERVER"), SpanKind::Server);
        assert_eq!(parse_kind("client"), SpanKind::Client);
        assert_eq!(parse_kind("NONSENSE"), SpanKind::Internal);
    }

    #[test]
    fn parse_attributes_tolerates_bad_json() {
        assert!(parse_attributes("not json").is_empty());
        assert!(parse_attributes("{}").is_empty());
        let attrs = parse_attributes(r#"{"k":"v","n":1}"#);
        assert_eq!(attrs.get("k").and_then(Value::as_str), Some("v"));
        assert_eq!(attrs.get("n").and_then(Value::as_i64), Some(1));
    }

    #[test]
    fn normalize_hex_rejects_wrong_length() {
        assert!(normalize_hex("deadbeef", 16).is_none());
        assert_eq!(
            normalize_hex("0123456789abcdef", 16).as_deref(),
            Some("0123456789abcdef")
        );
    }

    #[test]
    fn span_start_end_round_trip_direct() {
        // Exercise insert_span_start → update_span_end → query_trace.
        let (_dir, path) = tmp_db();
        let store = TelemetryStore::open(&path).unwrap();
        let resource = Resource::detect(Source::Cli, None);
        let _ = store.insert_resource(&resource);
        let rid = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap();

        let tid = TraceId::new_random();
        let sid = SpanId::new_random();
        let span = Span {
            span_id: sid,
            trace_id: tid,
            parent_span_id: None,
            name: "foo".into(),
            kind: SpanKind::Internal,
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: None,
            status: Status::Unset,
            attributes: Attributes::new(),
            events: Vec::new(),
            source: Source::Cli,
            resource_id: rid,
        };
        let _ = store.insert_span_start(&span);
        let _ = store.update_span_end(sid, 2_000_000_000, Status::Ok, Attributes::new());

        let tree = store.query_trace(&tid.to_string()).unwrap();
        assert_eq!(tree.spans.len(), 1);
        assert_eq!(tree.spans[0].name, "foo");
        assert_eq!(tree.spans[0].status_code, "OK");
    }

    #[test]
    fn inspect_renders_tree() {
        let (_dir, path) = tmp_db();
        let store = TelemetryStore::open(&path).unwrap();
        let resource = Resource::detect(Source::Cli, None);
        let _ = store.insert_resource(&resource);
        let rid = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap();

        let tid = TraceId::new_random();
        let root = SpanId::new_random();
        let child = SpanId::new_random();
        let s_root = Span {
            span_id: root,
            trace_id: tid,
            parent_span_id: None,
            name: "root-op".into(),
            kind: SpanKind::Server,
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: Some(2_000_000_000),
            status: Status::Ok,
            attributes: Attributes::new(),
            events: Vec::new(),
            source: Source::Cli,
            resource_id: rid,
        };
        let s_child = Span {
            span_id: child,
            trace_id: tid,
            parent_span_id: Some(root),
            name: "child-op".into(),
            kind: SpanKind::Internal,
            start_time_unix_nano: 1_500_000_000,
            end_time_unix_nano: Some(1_900_000_000),
            status: Status::Ok,
            attributes: Attributes::new(),
            events: Vec::new(),
            source: Source::Cli,
            resource_id: rid,
        };
        let _ = store.insert_span_start(&s_root);
        let _ = store.update_span_end(root, 2_000_000_000, Status::Ok, Attributes::new());
        let _ = store.insert_span_start(&s_child);
        let _ = store.update_span_end(child, 1_900_000_000, Status::Ok, Attributes::new());

        let result = inspect(&path, &tid.to_string(), false);
        assert!(result.is_ok());
    }
}
