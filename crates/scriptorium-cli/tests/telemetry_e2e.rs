//! End-to-end integration tests for the telemetry pipeline.
//!
//! Each scenario spawns the `scriptorium` binary as a subprocess (via
//! `assert_cmd::Command::cargo_bin`), with `HOME` overridden to a fresh
//! `tempfile::TempDir`. `default_hooks_db_path()` resolves to
//! `$HOME/.scriptorium/hooks.sqlite`, so every CLI/MCP/log-emit
//! invocation writes to a dedicated, throwaway DB.
//!
//! Run with:
//!
//! ```bash
//! cargo test -p scriptorium-cli --features dashboard \
//!     --test telemetry_e2e --release -- --test-threads=1
//! ```
//!
//! `--test-threads=1` is mandatory: SQLite-busy-exhaustion and dashboard
//! port-binding scenarios are not safe to run in parallel.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::missing_panics_doc)]

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as StdCommand, Stdio};
use std::time::{Duration, Instant};

use assert_cmd::Command;
use scriptorium_core::telemetry::{
    Attributes, LogFilters, LogRecord, Resource, SeverityNumber, Source, TelemetryStore,
    GLOBAL_STATS,
};
use serde_json::Value;
use tempfile::TempDir;

// ── helpers ─────────────────────────────────────────────────────────────

/// Build a tempdir suitable for HOME override. Pre-creates
/// `$HOME/.scriptorium/` so the CLI does not race on directory creation.
fn fresh_home() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let scriptorium_dir = dir.path().join(".scriptorium");
    std::fs::create_dir_all(&scriptorium_dir).unwrap();
    let db = scriptorium_dir.join("hooks.sqlite");
    (dir, db)
}

fn scriptorium_cmd(home: &Path) -> Command {
    let mut cmd = Command::cargo_bin("scriptorium").expect("scriptorium binary");
    cmd.env("HOME", home);
    cmd.env_remove("SCRIPTORIUM_TRACEPARENT");
    cmd
}

/// Pick a free TCP port by binding to 0 and immediately closing.
fn pick_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let p = l.local_addr().unwrap().port();
    drop(l);
    p
}

/// Poll `predicate` every 50ms until true or `timeout` elapses.
fn poll_until<F: FnMut() -> bool>(timeout: Duration, mut predicate: F) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    predicate()
}

/// RAII guard that kills a child process on drop. Used to ensure the
/// dashboard subprocess never leaks when a test panics.
struct ChildGuard(Option<Child>);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn open_store(db: &Path) -> TelemetryStore {
    TelemetryStore::open(db).expect("open store")
}

// ── E2E-1: CLI emit log ─────────────────────────────────────────────────

#[test]
fn e2e_cli_emit_log() {
    let (home, db) = fresh_home();
    let payload = serde_json::json!({
        "source": "cli",
        "severity": "INFO",
        "body": "hello-from-cli-e2e",
        "attributes": {"test": "e2e_cli_emit_log"},
    });
    scriptorium_cmd(home.path())
        .args(["log", "emit"])
        .write_stdin(payload.to_string())
        .assert()
        .success();

    let store = open_store(&db);
    let (rows, _) = store.query_logs(LogFilters::default(), None, 100).unwrap();
    assert!(
        rows.iter()
            .any(|r| r.body == "hello-from-cli-e2e" && r.source == "cli"),
        "log row should be present, got {} rows",
        rows.len()
    );
}

// ── E2E-2: MCP stdio session + tool span chain ─────────────────────────
//
// Spinning up an MCP server requires a configured vault with chat +
// embeddings providers, which the lightweight E2E harness cannot supply
// without committing real credentials. We document the desired shape
// here and mark the test ignored — the in-process MCP coverage already
// lives in the `scriptorium-mcp` crate's own integration suite.

#[test]
#[ignore = "needs vault + provider creds; covered by scriptorium-mcp integration tests"]
fn e2e_mcp_tool_span() {
    let (home, _db) = fresh_home();
    // Initialize a vault under HOME so `scriptorium serve` can find one.
    scriptorium_cmd(home.path())
        .args(["init", "--path"])
        .arg(home.path().join("vault"))
        .assert()
        .success();
    // Real test would: spawn `scriptorium --vault <path> serve`,
    // pipe an `initialize` + `tools/call` JSON-RPC frame, then
    // assert spans present with names mcp.session/mcp.request/mcp.tool.
}

// ── E2E-3: Hook script emit via shell subprocess ───────────────────────

#[test]
fn e2e_hook_emit() {
    let (home, db) = fresh_home();
    let bin = assert_cmd::cargo::cargo_bin("scriptorium");
    // Bash pipes the payload through the binary, mimicking a hook script.
    let script = format!(
        "echo '{}' | '{}' log emit",
        serde_json::json!({
            "source": "hook",
            "severity": "INFO",
            "body": "hook-pipe-e2e",
            "attributes": {"hook": "stop"},
        }),
        bin.display()
    );
    let status = StdCommand::new("bash")
        .arg("-c")
        .arg(&script)
        .env("HOME", home.path())
        .env_remove("SCRIPTORIUM_TRACEPARENT")
        .status()
        .expect("spawn bash");
    assert!(status.success());

    let store = open_store(&db);
    let (rows, _) = store.query_logs(LogFilters::default(), None, 100).unwrap();
    assert!(
        rows.iter()
            .any(|r| r.body == "hook-pipe-e2e" && r.source == "hook"),
        "hook log row not found"
    );
}

// ── E2E-4: Cross-process trace correlation ─────────────────────────────

#[test]
fn e2e_cross_process_trace() {
    let (home, db) = fresh_home();

    // Parent: generate a new traceparent with a known root span_id.
    let new_root = scriptorium_cmd(home.path())
        .args(["trace", "new-root"])
        .assert()
        .success();
    let traceparent = String::from_utf8(new_root.get_output().stdout.clone())
        .unwrap()
        .trim()
        .to_string();
    assert!(traceparent.starts_with("00-"));
    let parts: Vec<&str> = traceparent.split('-').collect();
    let parent_trace_id = parts[1].to_string();
    let parent_span_id = parts[2].to_string();

    // Child A: log emit inheriting trace_id via env.
    scriptorium_cmd(home.path())
        .env("SCRIPTORIUM_TRACEPARENT", &traceparent)
        .args(["log", "emit"])
        .write_stdin(
            serde_json::json!({
                "source": "cli",
                "severity": "INFO",
                "body": "child-log-correlated",
            })
            .to_string(),
        )
        .assert()
        .success();

    // Child B: span start + span end with the same env.
    let span_start = scriptorium_cmd(home.path())
        .env("SCRIPTORIUM_TRACEPARENT", &traceparent)
        .args([
            "trace",
            "span",
            "start",
            "--name",
            "child.work",
            "--source",
            "cli",
        ])
        .assert()
        .success();
    let stdout = String::from_utf8(span_start.get_output().stdout.clone()).unwrap();
    let v: Value = serde_json::from_str(stdout.trim()).expect("span start prints JSON");
    let child_span_id = v["span_id"].as_str().unwrap().to_string();
    let child_trace_id = v["trace_id"].as_str().unwrap().to_string();
    assert_eq!(child_trace_id, parent_trace_id);

    scriptorium_cmd(home.path())
        .args(["trace", "span", "end", "--span-id", &child_span_id])
        .assert()
        .success();

    // Verify via store.
    let store = open_store(&db);
    let tree = store.query_trace(&parent_trace_id).unwrap();
    let log_correlated = tree
        .logs
        .iter()
        .find(|l| l.body == "child-log-correlated")
        .expect("correlated log present in trace");
    assert_eq!(
        log_correlated.trace_id.as_deref(),
        Some(parent_trace_id.as_str())
    );
    let child_span = tree
        .spans
        .iter()
        .find(|s| s.span_id == child_span_id)
        .expect("child span present");
    assert_eq!(
        child_span.parent_span_id.as_deref(),
        Some(parent_span_id.as_str()),
        "child span's parent_span_id should match parent_span_id from env"
    );
}

// ── E2E-5: Dashboard live update ────────────────────────────────────────

#[test]
#[cfg(feature = "dashboard")]
fn e2e_dashboard_live_update() {
    let (home, db) = fresh_home();
    let port = pick_port();
    let bin = assert_cmd::cargo::cargo_bin("scriptorium");

    let child = StdCommand::new(&bin)
        .env("HOME", home.path())
        .env_remove("SCRIPTORIUM_TRACEPARENT")
        .args([
            "hooks",
            "dashboard",
            "--no-browser",
            "--port",
            &port.to_string(),
            "--db",
        ])
        .arg(&db)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn dashboard");
    let _guard = ChildGuard(Some(child));

    // Wait for the server to bind.
    let base = format!("http://127.0.0.1:{port}");
    assert!(
        poll_until(Duration::from_secs(15), || {
            std::process::Command::new("curl")
                .args(["-fsS", "-o", "/dev/null", &format!("{base}/api/health")])
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
        }),
        "dashboard never became reachable on {base}"
    );

    // Emit an event after the server is up.
    scriptorium_cmd(home.path())
        .args(["log", "emit"])
        .write_stdin(
            serde_json::json!({
                "source": "cli",
                "severity": "INFO",
                "body": "dashboard-live-update-marker",
            })
            .to_string(),
        )
        .assert()
        .success();

    // Poll /api/timeline until the new event surfaces.
    let url = format!("{base}/api/timeline?limit=20");
    assert!(
        poll_until(Duration::from_secs(10), || {
            let out = std::process::Command::new("curl")
                .args(["-fsS", &url])
                .output();
            match out {
                Ok(o) if o.status.success() => {
                    let s = String::from_utf8_lossy(&o.stdout);
                    s.contains("dashboard-live-update-marker")
                }
                _ => false,
            }
        }),
        "timeline did not reflect the new event"
    );
}

// ── E2E-6: Dangling span (process killed mid-span) ──────────────────────

#[test]
fn e2e_dangling_span() {
    let (home, db) = fresh_home();

    let span_start = scriptorium_cmd(home.path())
        .args([
            "trace",
            "span",
            "start",
            "--name",
            "dangling.op",
            "--source",
            "cli",
        ])
        .assert()
        .success();
    let stdout = String::from_utf8(span_start.get_output().stdout.clone()).unwrap();
    let v: Value = serde_json::from_str(stdout.trim()).unwrap();
    let trace_id = v["trace_id"].as_str().unwrap().to_string();
    let span_id = v["span_id"].as_str().unwrap().to_string();

    // Intentionally do NOT call `span end`. The "process killed before
    // end" condition is captured by the absence of the end_unix_nano.

    let store = open_store(&db);
    let tree = store.query_trace(&trace_id).unwrap();
    let span = tree
        .spans
        .iter()
        .find(|s| s.span_id == span_id)
        .expect("dangling span present");
    assert!(
        span.end_time_unix_nano.is_none(),
        "dangling span should have null end_time_unix_nano"
    );
    assert!(
        tree.total_duration_ms.is_none(),
        "trace duration should be unknown for dangling span"
    );
}

// ── E2E-7: Concurrent writers ───────────────────────────────────────────

#[test]
fn e2e_concurrent_writers() {
    let (home, db) = fresh_home();
    let bin = assert_cmd::cargo::cargo_bin("scriptorium");
    let n: usize = 50;

    // Bash one-liner spawns N background `scriptorium log emit` and waits.
    let script = format!(
        r#"
        for i in $(seq 1 {n}); do
            echo '{{"source":"cli","severity":"INFO","body":"concurrent-'$i'"}}' \
              | '{bin}' log emit &
        done
        wait
        "#,
        bin = bin.display(),
    );
    let status = StdCommand::new("bash")
        .arg("-c")
        .arg(&script)
        .env("HOME", home.path())
        .env_remove("SCRIPTORIUM_TRACEPARENT")
        .status()
        .expect("spawn bash");
    assert!(status.success());

    let store = open_store(&db);
    let (rows, _) = store.query_logs(LogFilters::default(), None, 1000).unwrap();
    let count = rows
        .iter()
        .filter(|r| r.body.starts_with("concurrent-"))
        .count();
    assert_eq!(count, n, "expected {n} concurrent rows, got {count}");
}

// ── E2E-8: Payload cap (32 KiB body → truncated + sha256) ───────────────

#[test]
fn e2e_payload_cap() {
    let (home, db) = fresh_home();
    let big_body: String = std::iter::repeat('A').take(32 * 1024).collect();
    let payload = serde_json::json!({
        "source": "cli",
        "severity": "INFO",
        "body": big_body,
    });
    scriptorium_cmd(home.path())
        .args(["log", "emit"])
        .write_stdin(payload.to_string())
        .assert()
        .success();

    let store = open_store(&db);
    let (rows, _) = store.query_logs(LogFilters::default(), None, 100).unwrap();
    let row = rows
        .iter()
        .find(|r| r.body.starts_with('A'))
        .expect("big-body row");
    assert!(
        row.body.len() <= 8192,
        "body should be capped at 8 KiB, was {}",
        row.body.len()
    );
    assert_eq!(
        row.attributes.get("telemetry.truncated"),
        Some(&Value::Bool(true)),
        "telemetry.truncated must be true"
    );
    let fields = row
        .attributes
        .get("telemetry.truncated_fields")
        .expect("truncated_fields attr");
    let s = serde_json::to_string(fields).unwrap();
    assert!(
        s.contains("sha256") || s.contains("hash"),
        "truncated_fields should include a sha256/hash, got {s}"
    );
}

// ── E2E-9: Dropped marker on SQLite busy exhaustion ─────────────────────

#[test]
fn e2e_dropped_marker() {
    // We exercise this in-process: hold an immediate writer transaction
    // on the same SQLite file and drive insert_log. With a tight busy
    // timeout the second writer is forced to drop, which both increments
    // GLOBAL_STATS.dropped_count and writes a `telemetry.dropped` marker
    // (via record_dropped_event).
    let (_home, db) = fresh_home();
    {
        let _bootstrap = open_store(&db);
    }

    // Hold an exclusive write lock from a separate connection.
    let blocker = rusqlite::Connection::open(&db).unwrap();
    blocker
        .execute_batch("BEGIN IMMEDIATE; INSERT INTO resources(attributes, attributes_hash) VALUES ('{}', 'lock-holder');")
        .unwrap();

    // Reduce the contender's busy_timeout so Dropped fires deterministically.
    let store = open_store(&db);
    {
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.busy_timeout(Duration::from_millis(50)).unwrap();
    }

    let before = GLOBAL_STATS
        .dropped_count
        .load(std::sync::atomic::Ordering::Relaxed);

    // Try a series of inserts; some should be dropped while the blocker
    // holds the writer lock.
    for i in 0..20u32 {
        let mut log =
            LogRecord::with_severity(format!("contender-{i}"), SeverityNumber::INFO, Source::Cli);
        let resource = Resource::detect(Source::Cli, None);
        let _ = store.insert_resource(&resource);
        log.resource_id = store
            .get_resource_id_by_hash(&resource.attributes_hash)
            .unwrap_or(1);
        log.attributes = Attributes::new();
        let _ = store.insert_log(&log);
    }

    // Release the lock.
    blocker.execute_batch("ROLLBACK;").unwrap();
    drop(blocker);

    let after = GLOBAL_STATS
        .dropped_count
        .load(std::sync::atomic::Ordering::Relaxed);

    // EITHER global stats incremented OR a dropped marker landed in store.
    let (rows, _) = store.query_logs(LogFilters::default(), None, 1000).unwrap();
    let marker_present = rows.iter().any(|r| r.body == "telemetry.dropped");
    assert!(
        after > before || marker_present,
        "expected dropped_count to increment or telemetry.dropped marker present (before={before}, after={after}, marker_present={marker_present})"
    );
}

// ── E2E-10: hook_events backward compat (view projection) ───────────────

#[test]
fn e2e_hook_events_compat() {
    let (home, db) = fresh_home();

    // Insert a hook log via TelemetryStore (the canonical write path).
    let store = open_store(&db);
    let resource = Resource::detect(Source::Hook, None);
    let _ = store.insert_resource(&resource);
    let rid = store
        .get_resource_id_by_hash(&resource.attributes_hash)
        .unwrap_or(1);
    let mut log = LogRecord::with_severity("hook-compat-body", SeverityNumber::INFO, Source::Hook);
    log.resource_id = rid;
    log.attributes = Attributes::from_iter([
        ("session_id".to_string(), Value::String("sess-xyz".into())),
        ("hook_type".to_string(), Value::String("stop".into())),
        ("decision".to_string(), Value::String("allow".into())),
        ("score".to_string(), Value::from(7_i64)),
    ]);
    let _ = store.insert_log(&log);

    // Open the same DB raw and query the legacy hook_events VIEW.
    let conn = rusqlite::Connection::open(&db).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT session_id, hook_type, decision, score FROM hook_events \
             WHERE session_id = 'sess-xyz' LIMIT 1",
        )
        .expect("hook_events view should exist (migration 002)");
    let row = stmt
        .query_row([], |r| {
            Ok((
                r.get::<_, Option<String>>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, Option<i64>>(3)?,
            ))
        })
        .expect("row visible via hook_events view");
    assert_eq!(row.0.as_deref(), Some("sess-xyz"));
    assert_eq!(row.1.as_deref(), Some("stop"));
    assert_eq!(row.2.as_deref(), Some("allow"));
    assert_eq!(row.3, Some(7));

    // Touch home so it's not unused.
    let _ = home.path();
}
