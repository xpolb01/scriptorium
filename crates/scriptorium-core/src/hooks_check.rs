//! Static health checker for Claude Code session hooks.
//!
//! [`check_session_hooks`] validates hook registrations, file existence,
//! timeouts, dependencies, guard patterns, error logs, and JSONL freshness
//! **without executing** any hooks. This is the static counterpart to
//! dynamic hook validation (which actually runs the hooks).
//!
//! Parallels [`crate::doctor`]'s `DoctorReport`/`DoctorCheck` pattern.

use std::collections::HashSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::config::Config;

// ── Report types ─────────────────────────────────────────────────────────

/// Status of a single check item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CheckStatus {
    Pass,
    Warn,
    Fail,
    Info,
}

/// One check result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckItem {
    pub name: String,
    pub status: CheckStatus,
    pub message: String,
}

impl CheckItem {
    fn pass(name: &str, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Pass,
            message: message.into(),
        }
    }

    fn warn(name: &str, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Warn,
            message: message.into(),
        }
    }

    fn fail(name: &str, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Fail,
            message: message.into(),
        }
    }

    fn info(name: &str, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: CheckStatus::Info,
            message: message.into(),
        }
    }
}

/// Aggregated hooks health report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HooksCheckReport {
    pub items: Vec<CheckItem>,
}

impl HooksCheckReport {
    /// True if any check has `Fail` status.
    pub fn has_failures(&self) -> bool {
        self.items.iter().any(|c| c.status == CheckStatus::Fail)
    }

    /// True if any check has `Warn` status.
    pub fn has_warnings(&self) -> bool {
        self.items.iter().any(|c| c.status == CheckStatus::Warn)
    }

    /// Exit code: 2 = failures present, 1 = warnings only, 0 = clean.
    pub fn exit_code(&self) -> i32 {
        if self.has_failures() {
            2
        } else {
            i32::from(self.has_warnings())
        }
    }
}

// ── Settings JSON types ──────────────────────────────────────────────────

/// Minimal parse of Claude Code `settings.json` — only the `hooks` section.
#[derive(Debug, Deserialize)]
struct SettingsJson {
    #[serde(default)]
    hooks: std::collections::HashMap<String, Vec<HookMatcher>>,
}

/// One matcher block inside a hook event type.
#[derive(Debug, Deserialize)]
struct HookMatcher {
    #[allow(dead_code)]
    matcher: Option<String>,
    #[serde(default)]
    hooks: Vec<HookEntry>,
}

/// A single hook registration.
#[derive(Debug, Deserialize)]
struct HookEntry {
    #[allow(dead_code)]
    #[serde(rename = "type")]
    hook_type: Option<String>,
    command: Option<String>,
    timeout: Option<u64>,
}

// ── Main entry point ─────────────────────────────────────────────────────

/// Run all static hook health checks.
///
/// - `settings_path`: path to Claude Code `settings.json`
/// - `hooks_dir`: directory containing `scriptorium-*.sh` hook scripts
///
/// This function never executes hooks — it only inspects files and config.
#[allow(clippy::too_many_lines)]
pub fn check_session_hooks(settings_path: &Path, hooks_dir: &Path) -> HooksCheckReport {
    let mut items = Vec::new();

    // Parse settings.json once; bail early with a FAIL if unreadable.
    let settings = match load_settings(settings_path) {
        Ok(s) => Some(s),
        Err(msg) => {
            items.push(CheckItem::fail("registration", msg));
            None
        }
    };

    // Collect registered commands for later checks.
    let registered_commands: Vec<RegisteredHook> = settings
        .as_ref()
        .map(collect_registered_hooks)
        .unwrap_or_default();

    // 1. Registration check
    if settings.is_some() {
        items.extend(check_registration(&registered_commands));
    }

    // 2. Unregistered scripts check
    items.extend(check_unregistered_scripts(hooks_dir, &registered_commands));

    // 3. Timeout reasonableness
    items.extend(check_timeouts(&registered_commands));

    // 4. Dependency check
    items.extend(check_dependencies());

    // 5. Guard pattern check
    items.extend(check_guard_patterns(&registered_commands));

    // 6. Error log check
    items.extend(check_error_logs());

    // 7. JSONL freshness
    items.extend(check_jsonl_freshness());

    // 8. SQLite freshness
    items.extend(check_sqlite_freshness());

    HooksCheckReport { items }
}

// ── Internal types ───────────────────────────────────────────────────────

/// A hook command extracted from settings.json.
struct RegisteredHook {
    event_type: String,
    command: String,
    timeout: Option<u64>,
}

/// Collect all registered hooks from parsed settings.
fn collect_registered_hooks(settings: &SettingsJson) -> Vec<RegisteredHook> {
    let mut cmds = Vec::new();
    for (event_type, matchers) in &settings.hooks {
        for matcher in matchers {
            for hook in &matcher.hooks {
                if let Some(cmd) = &hook.command {
                    cmds.push(RegisteredHook {
                        event_type: event_type.clone(),
                        command: cmd.clone(),
                        timeout: hook.timeout,
                    });
                }
            }
        }
    }
    cmds
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Load and parse settings.json.
fn load_settings(path: &Path) -> std::result::Result<SettingsJson, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Cannot read `settings.json` at {}: {e}", path.display()))?;
    serde_json::from_str::<SettingsJson>(&content)
        .map_err(|e| format!("Cannot parse `settings.json`: {e}"))
}

/// Expand `~` prefix to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    }
    PathBuf::from(path)
}

/// Home directory lookup via `$HOME`.
fn home_dir() -> Option<PathBuf> {
    std::env::var("HOME").ok().map(PathBuf::from)
}

/// Default artifacts directory: `~/.claude/artifacts/`.
fn artifacts_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".claude").join("artifacts"))
}

/// Default scriptorium data directory: `~/.scriptorium/`.
fn scriptorium_data_dir() -> Option<PathBuf> {
    home_dir().map(|h| h.join(".scriptorium"))
}

// ── Individual checks ────────────────────────────────────────────────────

/// Check 1: For each registered hook, verify the script file exists and
/// has the executable bit set.
fn check_registration(commands: &[RegisteredHook]) -> Vec<CheckItem> {
    let mut items = Vec::new();
    if commands.is_empty() {
        items.push(CheckItem::warn(
            "registration",
            "No hooks registered in `settings.json`",
        ));
        return items;
    }

    for hook in commands {
        let resolved = expand_tilde(&hook.command);
        if resolved.exists() {
            match fs::metadata(&resolved) {
                Ok(m) => {
                    let mode = m.permissions().mode();
                    if mode & 0o111 == 0 {
                        items.push(CheckItem::warn(
                            "registration",
                            format!(
                                "[{}] Script not executable: {} (mode {mode:o})",
                                hook.event_type, hook.command
                            ),
                        ));
                    } else {
                        items.push(CheckItem::pass(
                            "registration",
                            format!(
                                "[{}] {} — exists, executable",
                                hook.event_type, hook.command
                            ),
                        ));
                    }
                }
                Err(e) => {
                    items.push(CheckItem::warn(
                        "registration",
                        format!("[{}] Cannot stat {}: {e}", hook.event_type, hook.command),
                    ));
                }
            }
        } else {
            items.push(CheckItem::fail(
                "registration",
                format!(
                    "[{}] Script not found: {} (resolved: {})",
                    hook.event_type,
                    hook.command,
                    resolved.display()
                ),
            ));
        }
    }
    items
}

/// Check 2: Scan `hooks_dir` for `scriptorium-*.sh` files not registered
/// in settings.json. These may be intentionally dormant — report as INFO.
fn check_unregistered_scripts(hooks_dir: &Path, commands: &[RegisteredHook]) -> Vec<CheckItem> {
    let mut items = Vec::new();

    let entries = match fs::read_dir(hooks_dir) {
        Ok(e) => e,
        Err(e) => {
            items.push(CheckItem::warn(
                "unregistered_scripts",
                format!("Cannot read hooks dir {}: {e}", hooks_dir.display()),
            ));
            return items;
        }
    };

    // Build a set of resolved registered paths for comparison.
    let registered_paths: HashSet<PathBuf> =
        commands.iter().map(|h| expand_tilde(&h.command)).collect();

    let mut unregistered_count = 0u32;
    for entry in entries.flatten() {
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();
        if name.starts_with("scriptorium-") && name.ends_with(".sh") {
            let abs_path = entry.path().canonicalize().unwrap_or_else(|_| entry.path());
            let is_registered = registered_paths
                .iter()
                .any(|rp| rp.canonicalize().unwrap_or_else(|_| rp.clone()) == abs_path);
            if !is_registered {
                items.push(CheckItem::info(
                    "unregistered_scripts",
                    format!("On-disk but not registered: {name} (may be intentionally dormant)"),
                ));
                unregistered_count += 1;
            }
        }
    }

    if unregistered_count == 0 {
        items.push(CheckItem::pass(
            "unregistered_scripts",
            "All `scriptorium-*.sh` scripts in hooks dir are registered",
        ));
    }

    items
}

/// Check 3: `SessionEnd` timeout reasonableness.
/// < 5s → FAIL, < 10s → WARN, otherwise PASS.
fn check_timeouts(commands: &[RegisteredHook]) -> Vec<CheckItem> {
    let mut items = Vec::new();
    let session_end_hooks: Vec<_> = commands
        .iter()
        .filter(|h| h.event_type == "SessionEnd")
        .collect();

    if session_end_hooks.is_empty() {
        items.push(CheckItem::info(
            "timeout",
            "No SessionEnd hooks registered (skipped)",
        ));
        return items;
    }

    for hook in &session_end_hooks {
        match hook.timeout {
            Some(t) if t < 5 => {
                items.push(CheckItem::fail(
                    "timeout",
                    format!(
                        "SessionEnd hook `{}` has timeout {t}s (< 5s — likely too short to finish)",
                        hook.command
                    ),
                ));
            }
            Some(t) if t < 10 => {
                items.push(CheckItem::warn(
                    "timeout",
                    format!(
                        "SessionEnd hook `{}` has timeout {t}s (< 10s — may be tight)",
                        hook.command
                    ),
                ));
            }
            Some(t) => {
                items.push(CheckItem::pass(
                    "timeout",
                    format!("SessionEnd hook `{}` timeout {t}s — OK", hook.command),
                ));
            }
            None => {
                items.push(CheckItem::warn(
                    "timeout",
                    format!(
                        "SessionEnd hook `{}` has no explicit timeout (default may be too short)",
                        hook.command
                    ),
                ));
            }
        }
    }

    items
}

/// Check 4: Verify critical dependencies (`jq` and `scriptorium`) are in PATH.
fn check_dependencies() -> Vec<CheckItem> {
    let mut items = Vec::new();

    for dep in &["jq", "scriptorium"] {
        let found = std::process::Command::new("which")
            .arg(dep)
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);
        if found {
            items.push(CheckItem::pass(
                "dependency",
                format!("`{dep}` found in PATH"),
            ));
        } else {
            items.push(CheckItem::fail(
                "dependency",
                format!("`{dep}` NOT found in PATH — hooks will fail at runtime"),
            ));
        }
    }

    items
}

/// Check 5: For each registered hook, read the script and verify it
/// contains `set +e` and ends with `exit 0` (guard patterns that prevent
/// hook failures from killing the Claude Code session).
fn check_guard_patterns(commands: &[RegisteredHook]) -> Vec<CheckItem> {
    let mut items = Vec::new();

    for hook in commands {
        let resolved = expand_tilde(&hook.command);
        let Ok(content) = fs::read_to_string(&resolved) else {
            continue;
        };

        let has_set_plus_e = content.contains("set +e");
        let ends_exit_0 = content
            .trim_end()
            .lines()
            .last()
            .is_some_and(|l| l.trim() == "exit 0");

        if has_set_plus_e && ends_exit_0 {
            items.push(CheckItem::pass(
                "guard_pattern",
                format!(
                    "[{}] {} — has `set +e` and ends with `exit 0`",
                    hook.event_type, hook.command
                ),
            ));
        } else {
            let mut missing = Vec::new();
            if !has_set_plus_e {
                missing.push("`set +e`");
            }
            if !ends_exit_0 {
                missing.push("trailing `exit 0`");
            }
            items.push(CheckItem::warn(
                "guard_pattern",
                format!(
                    "[{}] {} — missing {}",
                    hook.event_type,
                    hook.command,
                    missing.join(" and ")
                ),
            ));
        }
    }

    if commands.is_empty() {
        items.push(CheckItem::info(
            "guard_pattern",
            "No hooks to check (skipped)",
        ));
    }

    items
}

/// Check 6: Inspect `~/.claude/artifacts/scriptorium-*-errors.log` files.
/// Empty → PASS, non-empty → WARN with last 3 lines.
fn check_error_logs() -> Vec<CheckItem> {
    check_error_logs_at(artifacts_dir())
}

/// Testable inner implementation.
fn check_error_logs_at(artifacts: Option<PathBuf>) -> Vec<CheckItem> {
    let mut items = Vec::new();

    let dir = match artifacts {
        Some(d) if d.is_dir() => d,
        Some(d) => {
            items.push(CheckItem::info(
                "error_logs",
                format!("Artifacts dir not found: {} (skipped)", d.display()),
            ));
            return items;
        }
        None => {
            items.push(CheckItem::warn(
                "error_logs",
                "Cannot determine HOME directory",
            ));
            return items;
        }
    };

    let entries = match fs::read_dir(&dir) {
        Ok(e) => e,
        Err(e) => {
            items.push(CheckItem::warn(
                "error_logs",
                format!("Cannot read artifacts dir: {e}"),
            ));
            return items;
        }
    };

    let mut found_any = false;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("scriptorium-") && name_str.ends_with("-errors.log") {
            found_any = true;
            match fs::read_to_string(entry.path()) {
                Ok(content) if content.trim().is_empty() => {
                    items.push(CheckItem::pass(
                        "error_logs",
                        format!("{name_str} — empty (no errors)"),
                    ));
                }
                Ok(content) => {
                    let last_lines: Vec<&str> = content
                        .trim_end()
                        .lines()
                        .rev()
                        .take(3)
                        .collect::<Vec<_>>()
                        .into_iter()
                        .rev()
                        .collect();
                    items.push(CheckItem::warn(
                        "error_logs",
                        format!(
                            "{name_str} — has errors. Last 3 lines:\n{}",
                            last_lines.join("\n")
                        ),
                    ));
                }
                Err(e) => {
                    items.push(CheckItem::warn(
                        "error_logs",
                        format!("{name_str} — cannot read: {e}"),
                    ));
                }
            }
        }
    }

    if !found_any {
        items.push(CheckItem::pass(
            "error_logs",
            "No `scriptorium-*-errors.log` files found",
        ));
    }

    items
}

/// Check 7: JSONL freshness — check last entry timestamp in the classifier
/// output. Older than 24h → WARN, missing → FAIL.
fn check_jsonl_freshness() -> Vec<CheckItem> {
    check_jsonl_freshness_at(artifacts_dir())
}

/// Testable inner implementation that accepts the artifacts dir.
fn check_jsonl_freshness_at(artifacts: Option<PathBuf>) -> Vec<CheckItem> {
    let mut items = Vec::new();

    let dir = match artifacts {
        Some(d) if d.is_dir() => d,
        _ => {
            items.push(CheckItem::fail(
                "jsonl_freshness",
                "Artifacts dir not found — cannot check JSONL freshness",
            ));
            return items;
        }
    };

    let jsonl_path = dir.join("scriptorium-classifier.jsonl");
    if !jsonl_path.exists() {
        items.push(CheckItem::fail(
            "jsonl_freshness",
            format!(
                "`scriptorium-classifier.jsonl` not found at {}",
                jsonl_path.display()
            ),
        ));
        return items;
    }

    match fs::read_to_string(&jsonl_path) {
        Ok(content) => {
            let last_line = content.trim_end().lines().last();
            match last_line {
                Some(line) => match extract_ts_from_json(line) {
                    Some(ts) => {
                        let age = Utc::now().signed_duration_since(ts);
                        let hours = age.num_hours();
                        if hours > 24 {
                            items.push(CheckItem::warn(
                                "jsonl_freshness",
                                format!(
                                    "Last JSONL entry is {hours}h old — hooks may not be firing"
                                ),
                            ));
                        } else {
                            items.push(CheckItem::pass(
                                "jsonl_freshness",
                                format!("Last JSONL entry is {hours}h old — fresh"),
                            ));
                        }
                    }
                    None => {
                        items.push(CheckItem::warn(
                            "jsonl_freshness",
                            "Cannot parse `ts` from last JSONL line",
                        ));
                    }
                },
                None => {
                    items.push(CheckItem::warn(
                        "jsonl_freshness",
                        "`scriptorium-classifier.jsonl` exists but is empty",
                    ));
                }
            }
        }
        Err(e) => {
            items.push(CheckItem::fail(
                "jsonl_freshness",
                format!("Cannot read JSONL file: {e}"),
            ));
        }
    }

    items
}

/// Check 8: `SQLite` freshness — if `~/.scriptorium/hooks.sqlite` exists,
/// compare its latest entry timestamp to detect ingest lag.
fn check_sqlite_freshness() -> Vec<CheckItem> {
    check_sqlite_freshness_at(scriptorium_data_dir())
}

/// Testable inner implementation.
fn check_sqlite_freshness_at(data_dir: Option<PathBuf>) -> Vec<CheckItem> {
    let mut items = Vec::new();

    let Some(dir) = data_dir else {
        items.push(CheckItem::info(
            "sqlite_freshness",
            "Cannot determine data directory (skipped)",
        ));
        return items;
    };

    let db_path = dir.join("hooks.sqlite");
    if !db_path.exists() {
        items.push(CheckItem::info(
            "sqlite_freshness",
            "No `hooks.sqlite` found (skipped — not yet created)",
        ));
        return items;
    }

    match rusqlite::Connection::open_with_flags(
        &db_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(conn) => {
            let latest: std::result::Result<Option<String>, _> = conn
                .query_row("SELECT MAX(ts) FROM hook_events", [], |row| row.get(0))
                .map(Some)
                .or_else(|e| match e {
                    rusqlite::Error::QueryReturnedNoRows => Ok(None),
                    other => Err(other),
                });
            match latest {
                Ok(Some(ts_str)) => match parse_ts(&ts_str) {
                    Some(ts) => {
                        let age = Utc::now().signed_duration_since(ts);
                        let hours = age.num_hours();
                        if hours > 48 {
                            items.push(CheckItem::warn(
                                "sqlite_freshness",
                                format!(
                                    "Latest SQLite entry is {hours}h old — JSONL→SQLite sync may be stale"
                                ),
                            ));
                        } else {
                            items.push(CheckItem::pass(
                                "sqlite_freshness",
                                format!("Latest SQLite entry is {hours}h old"),
                            ));
                        }
                    }
                    None => {
                        items.push(CheckItem::warn(
                            "sqlite_freshness",
                            format!("Cannot parse latest ts from SQLite: {ts_str}"),
                        ));
                    }
                },
                Ok(None) => {
                    items.push(CheckItem::info(
                        "sqlite_freshness",
                        "`hooks.sqlite` exists but has no events",
                    ));
                }
                Err(e) => {
                    items.push(CheckItem::warn(
                        "sqlite_freshness",
                        format!("Cannot query `hooks.sqlite`: {e}"),
                    ));
                }
            }
        }
        Err(e) => {
            items.push(CheckItem::warn(
                "sqlite_freshness",
                format!("Cannot open `hooks.sqlite`: {e}"),
            ));
        }
    }

    items
}

/// Extract an ISO-8601 `ts` field from a JSON line.
fn extract_ts_from_json(line: &str) -> Option<chrono::DateTime<Utc>> {
    let val: serde_json::Value = serde_json::from_str(line).ok()?;
    let ts_str = val.get("ts")?.as_str()?;
    parse_ts(ts_str)
}

/// Parse an ISO-8601 timestamp string (with or without timezone).
fn parse_ts(s: &str) -> Option<chrono::DateTime<Utc>> {
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f") {
        return Some(dt.and_utc());
    }
    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.and_utc());
    }
    None
}

// ── Dynamic hook validation ──────────────────────────────────────────────

fn synthetic_payload(event_type: &str) -> Option<&'static str> {
    match event_type {
        "Stop" => Some(
            r#"{"session_id":"health-check-test","transcript_path":"/dev/null","stop_hook_active":false,"cwd":"/tmp"}"#,
        ),
        "SessionEnd" => Some(r#"{"session_id":"health-check-test","transcript_path":"/dev/null"}"#),
        "SubagentStart" => Some(
            r#"{"session_id":"health-check-test","parent_session_id":"parent-test","agent_type":"test"}"#,
        ),
        "SubagentStop" => Some(
            r#"{"session_id":"health-check-test","parent_session_id":"parent-test","transcript_path":"/dev/null","agent_type":"test"}"#,
        ),
        "UserPromptSubmit" => Some(r#"{"session_id":"health-check-test","user_prompt":"test"}"#),
        _ => None,
    }
}

fn execute_hook_check(
    event_type: &str,
    command: &str,
    resolved: &Path,
    payload: &str,
) -> CheckItem {
    use std::io::{Read, Write};
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};

    let timeout = Duration::from_secs(2);
    let start = Instant::now();

    let mut child = match Command::new(resolved)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .env("SCRIPTORIUM_DRY_RUN", "1")
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            return CheckItem::fail(
                "dynamic",
                format!("[{event_type}] {command} — failed to spawn: {e}"),
            );
        }
    };

    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(payload.as_bytes());
    }

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let elapsed = start.elapsed();
                if status.success() {
                    return CheckItem::pass(
                        "dynamic",
                        format!(
                            "[{event_type}] {command} — exit 0 in {:.2}s",
                            elapsed.as_secs_f64()
                        ),
                    );
                }
                let code = status.code().unwrap_or(-1);
                let mut stderr_out = String::new();
                if let Some(mut stderr) = child.stderr.take() {
                    let _ = stderr.read_to_string(&mut stderr_out);
                }
                let snippet = stderr_out
                    .trim()
                    .lines()
                    .last()
                    .filter(|l| !l.is_empty())
                    .map(|l| format!(" | stderr: {l}"))
                    .unwrap_or_default();
                return CheckItem::fail(
                    "dynamic",
                    format!(
                        "[{event_type}] {command} — exit {code} in {:.2}s{snippet}",
                        elapsed.as_secs_f64()
                    ),
                );
            }
            Ok(None) => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return CheckItem::warn(
                        "dynamic",
                        format!(
                            "[{event_type}] {command} — killed after {:.0}s timeout",
                            timeout.as_secs_f64()
                        ),
                    );
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                return CheckItem::fail(
                    "dynamic",
                    format!("[{event_type}] {command} — wait error: {e}"),
                );
            }
        }
    }
}

/// Execute each registered hook with a synthetic JSON payload and
/// `SCRIPTORIUM_DRY_RUN=1` to verify hooks run without side effects.
///
/// Reports per-hook: PASS (exit 0, <2s), WARN (killed after 2s timeout),
/// FAIL (non-zero exit or spawn error).
pub fn check_dynamic(settings_path: &Path, _hooks_dir: &Path) -> HooksCheckReport {
    let mut items = Vec::new();

    let settings = match load_settings(settings_path) {
        Ok(s) => s,
        Err(msg) => {
            items.push(CheckItem::fail("dynamic", msg));
            return HooksCheckReport { items };
        }
    };

    let hooks = collect_registered_hooks(&settings);
    if hooks.is_empty() {
        items.push(CheckItem::info(
            "dynamic",
            "No hooks registered — nothing to test dynamically",
        ));
        return HooksCheckReport { items };
    }

    for hook in &hooks {
        let Some(payload) = synthetic_payload(&hook.event_type) else {
            items.push(CheckItem::info(
                "dynamic",
                format!(
                    "[{}] {} — no synthetic payload for event type (skipped)",
                    hook.event_type, hook.command
                ),
            ));
            continue;
        };

        let resolved = expand_tilde(&hook.command);
        if !resolved.exists() {
            items.push(CheckItem::fail(
                "dynamic",
                format!(
                    "[{}] {} — script not found (skipped execution)",
                    hook.event_type, hook.command
                ),
            ));
            continue;
        }

        items.push(execute_hook_check(
            &hook.event_type,
            &hook.command,
            &resolved,
            payload,
        ));
    }

    HooksCheckReport { items }
}

// ── Vault hooks check ────────────────────────────────────────────────────

/// Run static health checks on a scriptorium **vault**.
///
/// - `vault_path`: root of the scriptorium vault (contains `.scriptorium/`,
///   `wiki/`, `CLAUDE.md`, `log.md`).
///
/// Checks performed:
/// 1. `.scriptorium/config.toml` exists and parses into [`Config`].
/// 2. For each configured vault hook (`pre_ingest`, `post_ingest`,
///    `post_maintain`, `on_watch_trigger`): the command binary is
///    resolvable.
/// 3. Vault structure: `wiki/` directory, `CLAUDE.md`, and `log.md` exist.
/// 4. If no vault hooks are configured at all: report as INFO.
///
/// This function never executes hooks — static inspection only.
pub fn check_vault_hooks(vault_path: &Path) -> HooksCheckReport {
    let mut items = Vec::new();

    // 1. Config check — parse .scriptorium/config.toml
    let config_path = vault_path.join(".scriptorium").join("config.toml");
    let config = match load_vault_config(&config_path) {
        Ok(c) => {
            items.push(CheckItem::pass(
                "vault_config",
                format!("config.toml parsed OK at {}", config_path.display()),
            ));
            Some(c)
        }
        Err(msg) => {
            items.push(CheckItem::fail("vault_config", msg));
            None
        }
    };

    // 2. Vault hook commands — verify each configured hook's binary exists
    if let Some(cfg) = &config {
        items.extend(check_vault_hook_commands(&cfg.hooks));
    }

    // 3. Vault structure — wiki/, CLAUDE.md, log.md
    items.extend(check_vault_structure(vault_path));

    HooksCheckReport { items }
}

/// Load and parse `.scriptorium/config.toml`.
fn load_vault_config(path: &Path) -> std::result::Result<Config, String> {
    let content = fs::read_to_string(path)
        .map_err(|e| format!("Cannot read `config.toml` at {}: {e}", path.display()))?;
    toml::from_str::<Config>(&content).map_err(|e| format!("Cannot parse `config.toml`: {e}"))
}

/// For each configured vault hook, extract the first token (the binary)
/// and verify it can be found via `which` or as an absolute/relative path.
fn check_vault_hook_commands(hooks: &crate::hooks::HooksConfig) -> Vec<CheckItem> {
    let mut items = Vec::new();

    let hook_fields: [(&str, &Option<String>); 4] = [
        ("pre_ingest", &hooks.pre_ingest),
        ("post_ingest", &hooks.post_ingest),
        ("post_maintain", &hooks.post_maintain),
        ("on_watch_trigger", &hooks.on_watch_trigger),
    ];

    let mut any_configured = false;
    for (name, value) in &hook_fields {
        if let Some(cmd) = value {
            any_configured = true;
            let binary = cmd.split_whitespace().next().unwrap_or(cmd);
            let resolved = expand_tilde(binary);
            if resolved.exists() {
                items.push(CheckItem::pass(
                    "vault_hook_command",
                    format!(
                        "[{name}] binary `{binary}` — found at {}",
                        resolved.display()
                    ),
                ));
            } else {
                let in_path = std::process::Command::new("which")
                    .arg(binary)
                    .output()
                    .map(|o| o.status.success())
                    .unwrap_or(false);
                if in_path {
                    items.push(CheckItem::pass(
                        "vault_hook_command",
                        format!("[{name}] binary `{binary}` — found in PATH"),
                    ));
                } else {
                    items.push(CheckItem::fail(
                        "vault_hook_command",
                        format!(
                            "[{name}] binary `{binary}` not found (resolved: {})",
                            resolved.display()
                        ),
                    ));
                }
            }
        }
    }

    if !any_configured {
        items.push(CheckItem::info(
            "vault_hook_command",
            "No vault hooks configured (all None) — optional",
        ));
    }

    items
}

/// Check vault directory structure: `wiki/`, `CLAUDE.md`, `log.md`.
fn check_vault_structure(vault_path: &Path) -> Vec<CheckItem> {
    let mut items = Vec::new();

    let wiki_dir = vault_path.join("wiki");
    if wiki_dir.is_dir() {
        items.push(CheckItem::pass("vault_structure", "wiki/ directory exists"));
    } else {
        items.push(CheckItem::fail(
            "vault_structure",
            format!("wiki/ directory missing at {}", wiki_dir.display()),
        ));
    }

    let claude_md = vault_path.join("CLAUDE.md");
    if claude_md.is_file() {
        items.push(CheckItem::pass("vault_structure", "CLAUDE.md exists"));
    } else {
        items.push(CheckItem::fail(
            "vault_structure",
            format!("CLAUDE.md missing at {}", claude_md.display()),
        ));
    }

    let log_md = vault_path.join("log.md");
    if log_md.is_file() {
        items.push(CheckItem::pass("vault_structure", "log.md exists"));
    } else {
        items.push(CheckItem::fail(
            "vault_structure",
            format!("log.md missing at {}", log_md.display()),
        ));
    }

    items
}

// ── List hooks ───────────────────────────────────────────────────────────

/// One row in the `hooks list` table output.
#[derive(Debug, Clone, Serialize)]
pub struct HookListEntry {
    pub name: String,
    pub event_type: String,
    pub registered: bool,
    pub on_disk: bool,
    pub executable: bool,
    pub timeout: Option<u64>,
}

/// Build a unified list of hooks from settings.json registrations and
/// on-disk `scriptorium-*.sh` scripts. Each entry indicates whether the
/// hook is registered, present on disk, executable, and its timeout.
pub fn list_hooks(settings_path: &Path, hooks_dir: &Path) -> Vec<HookListEntry> {
    let mut entries = Vec::new();
    let mut seen_paths: HashSet<PathBuf> = HashSet::new();

    // 1. Registered hooks from settings.json
    if let Ok(settings) = load_settings(settings_path) {
        let hooks = collect_registered_hooks(&settings);
        for hook in &hooks {
            let resolved = expand_tilde(&hook.command);
            let on_disk = resolved.exists();
            let executable = on_disk
                && fs::metadata(&resolved)
                    .map(|m| m.permissions().mode() & 0o111 != 0)
                    .unwrap_or(false);
            let name = resolved
                .file_name()
                .map_or_else(|| hook.command.clone(), |n| n.to_string_lossy().to_string());

            seen_paths.insert(resolved.canonicalize().unwrap_or_else(|_| resolved.clone()));

            entries.push(HookListEntry {
                name,
                event_type: hook.event_type.clone(),
                registered: true,
                on_disk,
                executable,
                timeout: hook.timeout,
            });
        }
    }

    // 2. On-disk scripts not in settings.json
    if let Ok(dir_entries) = fs::read_dir(hooks_dir) {
        for entry in dir_entries.flatten() {
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy().to_string();
            if name_str.starts_with("scriptorium-")
                && std::path::Path::new(&name_str)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("sh"))
            {
                let abs = entry.path().canonicalize().unwrap_or_else(|_| entry.path());
                if !seen_paths.contains(&abs) {
                    let executable = fs::metadata(&abs)
                        .map(|m| m.permissions().mode() & 0o111 != 0)
                        .unwrap_or(false);
                    entries.push(HookListEntry {
                        name: name_str,
                        event_type: "\u{2014}".to_string(), // em-dash
                        registered: false,
                        on_disk: true,
                        executable,
                        timeout: None,
                    });
                }
            }
        }
    }

    entries
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    /// Helper: create a minimal settings.json with the given hooks block.
    fn write_settings(dir: &Path, hooks_json: &str) -> PathBuf {
        let path = dir.join("settings.json");
        let content = format!(r#"{{ "hooks": {hooks_json} }}"#);
        fs::write(&path, content).unwrap();
        path
    }

    /// Helper: create a hook script with optional guard patterns.
    fn write_hook_script(path: &Path, has_set_plus_e: bool, has_exit_0: bool) {
        let mut lines = vec!["#!/bin/bash"];
        if has_set_plus_e {
            lines.push("set +e");
        }
        lines.push("echo 'doing work'");
        if has_exit_0 {
            lines.push("exit 0");
        }
        fs::write(path, lines.join("\n")).unwrap();
        let mut perms = fs::metadata(path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).unwrap();
    }

    // ── Registration check tests ─────────────────────────────────────

    #[test]
    fn registration_healthy_hooks() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-test.sh");
        write_hook_script(&script, true, true);

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "Stop": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 5 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_session_hooks(&settings, &hooks_dir);
        let reg_items: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "registration")
            .collect();
        assert!(
            reg_items.iter().all(|i| i.status == CheckStatus::Pass),
            "Expected all registration checks to pass: {reg_items:?}"
        );
    }

    #[test]
    fn registration_missing_script_fails() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let settings = write_settings(
            dir.path(),
            r#"{ "Stop": [{ "matcher": "", "hooks": [{ "type": "command", "command": "/nonexistent/script.sh", "timeout": 5 }] }] }"#,
        );

        let report = check_session_hooks(&settings, &hooks_dir);
        assert!(report.has_failures(), "Missing script should cause failure");
        assert_eq!(report.exit_code(), 2);
    }

    #[test]
    fn registration_non_executable_warns() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-test.sh");
        fs::write(&script, "#!/bin/bash\nexit 0").unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o644);
        fs::set_permissions(&script, perms).unwrap();

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "Stop": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 5 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_session_hooks(&settings, &hooks_dir);
        assert!(report.has_warnings(), "Non-executable script should warn");
    }

    // ── Unregistered scripts test ────────────────────────────────────

    #[test]
    fn unregistered_scripts_detected() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        write_hook_script(&hooks_dir.join("scriptorium-orphan.sh"), true, true);

        let settings = write_settings(dir.path(), "{}");
        let report = check_session_hooks(&settings, &hooks_dir);
        let unreg: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "unregistered_scripts" && i.status == CheckStatus::Info)
            .collect();
        assert!(
            !unreg.is_empty(),
            "Should detect unregistered script: {:?}",
            report.items
        );
    }

    // ── Timeout check tests ──────────────────────────────────────────

    #[test]
    fn session_end_short_timeout_fails() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-end.sh");
        write_hook_script(&script, true, true);

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "SessionEnd": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 3 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_session_hooks(&settings, &hooks_dir);
        let timeout_fails: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "timeout" && i.status == CheckStatus::Fail)
            .collect();
        assert!(
            !timeout_fails.is_empty(),
            "SessionEnd timeout < 5s should FAIL"
        );
    }

    #[test]
    fn session_end_moderate_timeout_warns() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-end.sh");
        write_hook_script(&script, true, true);

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "SessionEnd": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 7 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_session_hooks(&settings, &hooks_dir);
        let timeout_warns: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "timeout" && i.status == CheckStatus::Warn)
            .collect();
        assert!(
            !timeout_warns.is_empty(),
            "SessionEnd timeout 5-10s should WARN"
        );
    }

    // ── Guard pattern tests ──────────────────────────────────────────

    #[test]
    fn guard_pattern_missing_warns() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-bad.sh");
        write_hook_script(&script, false, false);

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "Stop": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 10 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_session_hooks(&settings, &hooks_dir);
        let guard_warns: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "guard_pattern" && i.status == CheckStatus::Warn)
            .collect();
        assert!(
            !guard_warns.is_empty(),
            "Missing guard patterns should warn"
        );
    }

    // ── Error log tests ──────────────────────────────────────────────

    #[test]
    fn error_logs_empty_passes() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("scriptorium-classifier-errors.log");
        fs::write(&log, "").unwrap();

        let items = check_error_logs_at(Some(dir.path().to_path_buf()));
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Pass),
            "Empty error log should PASS: {items:?}"
        );
    }

    #[test]
    fn error_logs_non_empty_warns() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("scriptorium-classifier-errors.log");
        fs::write(&log, "line 1\nline 2\nline 3\nline 4\n").unwrap();

        let items = check_error_logs_at(Some(dir.path().to_path_buf()));
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Warn),
            "Non-empty error log should WARN: {items:?}"
        );
    }

    // ── JSONL freshness tests ────────────────────────────────────────

    #[test]
    fn jsonl_freshness_missing_fails() {
        let dir = tempfile::tempdir().unwrap();
        let items = check_jsonl_freshness_at(Some(dir.path().to_path_buf()));
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Fail),
            "Missing JSONL should FAIL"
        );
    }

    #[test]
    fn jsonl_freshness_recent_passes() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl = dir.path().join("scriptorium-classifier.jsonl");
        let now = Utc::now().to_rfc3339();
        let line = format!(r#"{{"ts": "{now}", "session_id": "test"}}"#);
        fs::write(&jsonl, format!("{line}\n")).unwrap();

        let items = check_jsonl_freshness_at(Some(dir.path().to_path_buf()));
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Pass),
            "Recent JSONL should PASS: {items:?}"
        );
    }

    #[test]
    fn jsonl_freshness_stale_warns() {
        let dir = tempfile::tempdir().unwrap();
        let jsonl = dir.path().join("scriptorium-classifier.jsonl");
        let old = Utc::now() - chrono::Duration::hours(48);
        let line = format!(r#"{{"ts": "{}", "session_id": "test"}}"#, old.to_rfc3339());
        fs::write(&jsonl, format!("{line}\n")).unwrap();

        let items = check_jsonl_freshness_at(Some(dir.path().to_path_buf()));
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Warn),
            "Stale JSONL should WARN: {items:?}"
        );
    }

    // ── SQLite freshness tests ───────────────────────────────────────

    #[test]
    fn sqlite_freshness_missing_is_info() {
        let dir = tempfile::tempdir().unwrap();
        let items = check_sqlite_freshness_at(Some(dir.path().to_path_buf()));
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Info),
            "Missing SQLite should be INFO"
        );
    }

    #[test]
    fn sqlite_freshness_recent_passes() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("hooks.sqlite");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS hook_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                session_id TEXT NOT NULL,
                raw_json_hash TEXT NOT NULL
            );",
        )
        .unwrap();
        let now = Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO hook_events (ts, session_id, raw_json_hash) VALUES (?1, ?2, ?3)",
            rusqlite::params![now, "test-session", "abc123"],
        )
        .unwrap();
        drop(conn);

        let items = check_sqlite_freshness_at(Some(dir.path().to_path_buf()));
        assert!(
            items.iter().any(|i| i.status == CheckStatus::Pass),
            "Recent SQLite should PASS: {items:?}"
        );
    }

    // ── Report methods ───────────────────────────────────────────────

    #[test]
    fn report_exit_codes() {
        let clean = HooksCheckReport {
            items: vec![CheckItem::pass("test", "ok")],
        };
        assert_eq!(clean.exit_code(), 0);
        assert!(!clean.has_failures());
        assert!(!clean.has_warnings());

        let warned = HooksCheckReport {
            items: vec![CheckItem::pass("a", "ok"), CheckItem::warn("b", "hmm")],
        };
        assert_eq!(warned.exit_code(), 1);
        assert!(!warned.has_failures());
        assert!(warned.has_warnings());

        let failed = HooksCheckReport {
            items: vec![CheckItem::pass("a", "ok"), CheckItem::fail("b", "bad")],
        };
        assert_eq!(failed.exit_code(), 2);
        assert!(failed.has_failures());
    }

    // ── Timestamp parsing ────────────────────────────────────────────

    #[test]
    fn parse_ts_various_formats() {
        assert!(parse_ts("2025-06-15T10:30:00Z").is_some());
        assert!(parse_ts("2025-06-15T10:30:00+00:00").is_some());
        assert!(parse_ts("2025-06-15T10:30:00.123456").is_some());
        assert!(parse_ts("2025-06-15T10:30:00").is_some());
        assert!(parse_ts("not-a-date").is_none());
    }

    // ── Settings parse test ──────────────────────────────────────────

    #[test]
    fn invalid_settings_json_fails() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("settings.json");
        fs::write(&path, "NOT JSON").unwrap();

        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let report = check_session_hooks(&path, &hooks_dir);
        assert!(report.has_failures(), "Invalid JSON should cause a failure");
    }

    #[test]
    fn missing_settings_file_fails() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let report = check_session_hooks(&dir.path().join("nonexistent.json"), &hooks_dir);
        assert!(
            report.has_failures(),
            "Missing settings file should cause a failure"
        );
    }

    // ── Vault hooks check tests ──────────────────────────────────────

    fn create_valid_vault(dir: &Path) {
        let scriptorium_dir = dir.join(".scriptorium");
        fs::create_dir_all(&scriptorium_dir).unwrap();
        fs::write(
            scriptorium_dir.join("config.toml"),
            "[llm]\nprovider = \"mock\"\nmodel = \"fixture\"\n",
        )
        .unwrap();
        fs::create_dir_all(dir.join("wiki")).unwrap();
        fs::write(dir.join("CLAUDE.md"), "# Vault").unwrap();
        fs::write(dir.join("log.md"), "# Log").unwrap();
    }

    #[test]
    fn vault_healthy_no_hooks() {
        let dir = tempfile::tempdir().unwrap();
        create_valid_vault(dir.path());

        let report = check_vault_hooks(dir.path());
        assert!(
            !report.has_failures(),
            "Healthy vault should not fail: {:#?}",
            report.items
        );
        assert_eq!(report.exit_code(), 0);

        let config_pass = report
            .items
            .iter()
            .any(|i| i.name == "vault_config" && i.status == CheckStatus::Pass);
        assert!(config_pass, "config.toml should pass");

        let info = report
            .items
            .iter()
            .any(|i| i.name == "vault_hook_command" && i.status == CheckStatus::Info);
        assert!(info, "No hooks configured should be INFO");
    }

    #[test]
    fn vault_missing_config_fails() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join("wiki")).unwrap();
        fs::write(dir.path().join("CLAUDE.md"), "# Vault").unwrap();
        fs::write(dir.path().join("log.md"), "# Log").unwrap();

        let report = check_vault_hooks(dir.path());
        assert!(report.has_failures(), "Missing config.toml should fail");
        let fail = report
            .items
            .iter()
            .any(|i| i.name == "vault_config" && i.status == CheckStatus::Fail);
        assert!(fail, "vault_config should FAIL");
    }

    #[test]
    fn vault_invalid_config_fails() {
        let dir = tempfile::tempdir().unwrap();
        let scriptorium_dir = dir.path().join(".scriptorium");
        fs::create_dir_all(&scriptorium_dir).unwrap();
        fs::write(scriptorium_dir.join("config.toml"), "NOT VALID TOML {{{{").unwrap();
        fs::create_dir_all(dir.path().join("wiki")).unwrap();
        fs::write(dir.path().join("CLAUDE.md"), "# Vault").unwrap();
        fs::write(dir.path().join("log.md"), "# Log").unwrap();

        let report = check_vault_hooks(dir.path());
        assert!(report.has_failures(), "Invalid config.toml should fail");
    }

    #[test]
    fn vault_missing_wiki_fails() {
        let dir = tempfile::tempdir().unwrap();
        create_valid_vault(dir.path());
        fs::remove_dir_all(dir.path().join("wiki")).unwrap();

        let report = check_vault_hooks(dir.path());
        assert!(report.has_failures(), "Missing wiki/ should fail");
        let fail = report.items.iter().any(|i| {
            i.name == "vault_structure"
                && i.status == CheckStatus::Fail
                && i.message.contains("wiki/")
        });
        assert!(fail, "Should have vault_structure FAIL for wiki/");
    }

    #[test]
    fn vault_missing_claude_md_fails() {
        let dir = tempfile::tempdir().unwrap();
        create_valid_vault(dir.path());
        fs::remove_file(dir.path().join("CLAUDE.md")).unwrap();

        let report = check_vault_hooks(dir.path());
        assert!(report.has_failures(), "Missing CLAUDE.md should fail");
    }

    #[test]
    fn vault_missing_log_md_fails() {
        let dir = tempfile::tempdir().unwrap();
        create_valid_vault(dir.path());
        fs::remove_file(dir.path().join("log.md")).unwrap();

        let report = check_vault_hooks(dir.path());
        assert!(report.has_failures(), "Missing log.md should fail");
    }

    #[test]
    fn vault_hook_with_valid_binary_passes() {
        let dir = tempfile::tempdir().unwrap();
        let scriptorium_dir = dir.path().join(".scriptorium");
        fs::create_dir_all(&scriptorium_dir).unwrap();
        fs::write(
            scriptorium_dir.join("config.toml"),
            "[llm]\nprovider = \"mock\"\nmodel = \"fixture\"\n\n[hooks]\npre_ingest = \"echo hello\"\n",
        )
        .unwrap();
        fs::create_dir_all(dir.path().join("wiki")).unwrap();
        fs::write(dir.path().join("CLAUDE.md"), "# Vault").unwrap();
        fs::write(dir.path().join("log.md"), "# Log").unwrap();

        let report = check_vault_hooks(dir.path());
        assert!(
            !report.has_failures(),
            "echo should be found in PATH: {:#?}",
            report.items
        );
        let pass = report.items.iter().any(|i| {
            i.name == "vault_hook_command"
                && i.status == CheckStatus::Pass
                && i.message.contains("pre_ingest")
        });
        assert!(pass, "pre_ingest with `echo` should PASS");
    }

    #[test]
    fn vault_hook_with_missing_binary_fails() {
        let dir = tempfile::tempdir().unwrap();
        let scriptorium_dir = dir.path().join(".scriptorium");
        fs::create_dir_all(&scriptorium_dir).unwrap();
        fs::write(
            scriptorium_dir.join("config.toml"),
            "[llm]\nprovider = \"mock\"\nmodel = \"fixture\"\n\n[hooks]\npost_ingest = \"/nonexistent/binary --flag\"\n",
        )
        .unwrap();
        fs::create_dir_all(dir.path().join("wiki")).unwrap();
        fs::write(dir.path().join("CLAUDE.md"), "# Vault").unwrap();
        fs::write(dir.path().join("log.md"), "# Log").unwrap();

        let report = check_vault_hooks(dir.path());
        assert!(report.has_failures(), "Missing binary should fail");
        let fail = report.items.iter().any(|i| {
            i.name == "vault_hook_command"
                && i.status == CheckStatus::Fail
                && i.message.contains("post_ingest")
        });
        assert!(fail, "post_ingest with missing binary should FAIL");
    }

    // ── Dynamic hook validation tests ────────────────────────────────

    #[test]
    fn dynamic_hook_pass_on_exit_zero() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-ok.sh");
        fs::write(&script, "#!/bin/bash\ncat > /dev/null\nexit 0\n").unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "Stop": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 5 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_dynamic(&settings, &hooks_dir);
        let dyn_items: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "dynamic")
            .collect();
        assert!(
            dyn_items.iter().any(|i| i.status == CheckStatus::Pass),
            "Hook exiting 0 should PASS: {dyn_items:?}"
        );
        assert!(!report.has_failures());
    }

    #[test]
    fn dynamic_hook_fail_on_nonzero_exit() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-bad.sh");
        fs::write(&script, "#!/bin/bash\ncat > /dev/null\nexit 1\n").unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "Stop": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 5 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_dynamic(&settings, &hooks_dir);
        assert!(report.has_failures(), "Hook exiting 1 should FAIL");
    }

    #[test]
    fn dynamic_no_hooks_is_info() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let settings = write_settings(dir.path(), "{}");
        let report = check_dynamic(&settings, &hooks_dir);
        let info_items: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "dynamic" && i.status == CheckStatus::Info)
            .collect();
        assert!(
            !info_items.is_empty(),
            "No hooks should be INFO: {info_items:?}"
        );
    }

    #[test]
    fn dynamic_unknown_event_type_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-custom.sh");
        fs::write(&script, "#!/bin/bash\nexit 0\n").unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "CustomEvent": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 5 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_dynamic(&settings, &hooks_dir);
        let info_items: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "dynamic" && i.status == CheckStatus::Info)
            .collect();
        assert!(
            !info_items.is_empty(),
            "Unknown event type should be INFO (skipped)"
        );
    }

    #[test]
    fn dynamic_missing_script_fails() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let settings = write_settings(
            dir.path(),
            r#"{ "Stop": [{ "matcher": "", "hooks": [{ "type": "command", "command": "/nonexistent/script.sh", "timeout": 5 }] }] }"#,
        );

        let report = check_dynamic(&settings, &hooks_dir);
        assert!(report.has_failures(), "Missing script should cause failure");
    }

    #[test]
    fn dynamic_dry_run_env_is_set() {
        let dir = tempfile::tempdir().unwrap();
        let hooks_dir = dir.path().join("hooks");
        fs::create_dir_all(&hooks_dir).unwrap();

        let script = hooks_dir.join("scriptorium-envcheck.sh");
        fs::write(
            &script,
            "#!/bin/bash\ncat > /dev/null\n[ \"$SCRIPTORIUM_DRY_RUN\" = \"1\" ] && exit 0 || exit 1\n",
        )
        .unwrap();
        let mut perms = fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&script, perms).unwrap();

        let settings = write_settings(
            dir.path(),
            &format!(
                r#"{{ "UserPromptSubmit": [{{ "matcher": "", "hooks": [{{ "type": "command", "command": "{}", "timeout": 5 }}] }}] }}"#,
                script.display()
            ),
        );

        let report = check_dynamic(&settings, &hooks_dir);
        assert!(
            !report.has_failures(),
            "SCRIPTORIUM_DRY_RUN=1 should be set: {:?}",
            report.items
        );
    }

    #[test]
    fn vault_structure_all_missing_fails() {
        let dir = tempfile::tempdir().unwrap();
        create_valid_vault(dir.path());
        fs::remove_dir_all(dir.path().join("wiki")).unwrap();
        fs::remove_file(dir.path().join("CLAUDE.md")).unwrap();
        fs::remove_file(dir.path().join("log.md")).unwrap();

        let report = check_vault_hooks(dir.path());
        let structure_fails: Vec<_> = report
            .items
            .iter()
            .filter(|i| i.name == "vault_structure" && i.status == CheckStatus::Fail)
            .collect();
        assert_eq!(
            structure_fails.len(),
            3,
            "All 3 structure checks should fail: {structure_fails:#?}"
        );
    }
}
