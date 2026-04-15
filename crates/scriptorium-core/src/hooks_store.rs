//! SQLite-backed store for Claude Code hook events.
//!
//! This is the data layer for the hooks health dashboard. It ingests
//! JSONL events emitted by the scriptorium classifier hook and provides
//! query methods for dashboards, session drill-down, and error triage.
//!
//! ## Schema
//!
//! ```sql
//! CREATE TABLE IF NOT EXISTS hook_events (
//!   id INTEGER PRIMARY KEY AUTOINCREMENT,
//!   ts TEXT NOT NULL,
//!   session_id TEXT NOT NULL,
//!   parent_session_id TEXT,
//!   hook_type TEXT NOT NULL DEFAULT 'stop',
//!   source TEXT,
//!   cwd TEXT,
//!   score INTEGER,
//!   threshold INTEGER DEFAULT 6,
//!   signals TEXT,           -- JSON array
//!   metrics TEXT,           -- JSON object
//!   agent_type TEXT,
//!   privacy_veto TEXT,
//!   decision TEXT,
//!   peak_turn_score INTEGER,
//!   session_aggregate_score INTEGER,
//!   final_score INTEGER,
//!   turn_count INTEGER,
//!   subagent_count INTEGER,
//!   raw_json TEXT,
//!   raw_json_hash TEXT NOT NULL
//! );
//! ```
//!
//! Dedup is via `raw_json_hash` (SHA-256 of exact raw bytes). The JSONL
//! file is append-only, so re-ingesting the same file is safe — duplicate
//! rows are silently ignored via `INSERT OR IGNORE` on the unique index.
//!
//! The DB lives at `~/.scriptorium/hooks.sqlite` at runtime, but
//! [`HooksStore::open`] accepts any path for testability.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use regex::Regex;
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{Error, Result};

// ── Error helper ─────────────────────────────────────────────────────────

/// Wrap a `rusqlite::Error` into our crate-level `Error::Other`.
#[allow(clippy::needless_pass_by_value)]
fn wrap_sql(e: rusqlite::Error) -> Error {
    Error::Other(anyhow::anyhow!("hooks_store sqlite: {e}"))
}

// ── Domain types ─────────────────────────────────────────────────────────

/// A single hook event, compatible with the JSONL shape emitted by the
/// scriptorium classifier hook. Accommodates all three entry variants:
/// per-turn, subagent-stop, and session-aggregate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookEvent {
    /// Auto-increment row id (populated on read, ignored on insert).
    #[serde(default)]
    pub id: Option<i64>,
    /// ISO-8601 timestamp of the event.
    pub ts: String,
    /// Claude Code session identifier.
    pub session_id: String,
    /// Parent session id (for subagent events).
    #[serde(default)]
    pub parent_session_id: Option<String>,
    /// Hook type: "stop" (per-turn), "`subagent_stop`", "`session_end`".
    #[serde(default = "default_hook_type")]
    pub hook_type: String,
    /// Source identifier (e.g. hook script path).
    #[serde(default)]
    pub source: Option<String>,
    /// Working directory at event time.
    #[serde(default)]
    pub cwd: Option<String>,
    /// Classifier score for this turn.
    #[serde(default)]
    pub score: Option<i32>,
    /// Score threshold that triggers ingest.
    #[serde(default)]
    pub threshold: Option<i32>,
    /// JSON array of signal names that fired.
    #[serde(default)]
    pub signals: Option<String>,
    /// JSON object of raw metrics.
    #[serde(default)]
    pub metrics: Option<String>,
    /// Agent type (e.g. "build", "oracle", "explore").
    #[serde(default)]
    pub agent_type: Option<String>,
    /// Privacy veto reason, if the event was vetoed.
    #[serde(default)]
    pub privacy_veto: Option<String>,
    /// Decision made: "ingest", "skip", "veto".
    #[serde(default)]
    pub decision: Option<String>,
    /// Highest per-turn score seen in this session.
    #[serde(default)]
    pub peak_turn_score: Option<i32>,
    /// Aggregate score across all turns.
    #[serde(default)]
    pub session_aggregate_score: Option<i32>,
    /// Final computed score (may differ from per-turn score).
    #[serde(default)]
    pub final_score: Option<i32>,
    /// Number of turns in the session.
    #[serde(default)]
    pub turn_count: Option<i32>,
    /// Number of subagent spawns in the session.
    #[serde(default)]
    pub subagent_count: Option<i32>,
    /// Original raw JSON line (for debugging / re-parse).
    #[serde(default)]
    pub raw_json: Option<String>,
    /// SHA-256 hex digest of `raw_json` bytes. Used for dedup.
    pub raw_json_hash: String,
}

fn default_hook_type() -> String {
    "stop".to_string()
}

/// Aggregated summary for the hooks health dashboard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HooksSummary {
    pub total_events: u64,
    pub total_ingests: u64,
    pub total_skips: u64,
    pub total_vetoes: u64,
    pub ingests_per_hour: f64,
    /// Score value → count.
    pub score_distribution: HashMap<i32, u64>,
    /// Signal name → frequency.
    pub signal_frequency: HashMap<String, u64>,
    /// Hook type → count.
    pub by_hook_type: HashMap<String, u64>,
    pub avg_score: f64,
    pub peak_score: i32,
    pub privacy_vetoes: u64,
    /// ISO-8601 timestamp of the last ingest decision.
    pub last_ingest_ts: Option<String>,
    /// ISO-8601 timestamp of the last event overall.
    pub last_event_ts: Option<String>,
}

// ── JSONL Import ─────────────────────────────────────────────────────────

/// Report from a JSONL import operation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImportReport {
    /// Total lines read from the JSONL file.
    pub total_lines: usize,
    /// Successfully imported into the database.
    pub imported: usize,
    /// Skipped because the line was malformed JSON.
    pub skipped_malformed: usize,
    /// Skipped because `raw_json_hash` already existed (duplicate).
    pub skipped_duplicate: usize,
    /// Entries where the privacy guard flagged sensitive content.
    pub privacy_flagged: usize,
    /// Lines that produced unexpected errors during insert.
    pub errors: Vec<String>,
}

/// Intermediate serde model for the `scored` sub-object in per-turn
/// and subagent-stop JSONL entries.
#[derive(Debug, Deserialize)]
struct JsonlScored {
    score: i32,
    #[serde(default)]
    signals: Vec<String>,
    #[serde(default)]
    last_user_prompt: Option<String>,
    #[serde(default)]
    metrics: Option<serde_json::Value>,
}

/// Intermediate serde model covering all three JSONL entry variants.
///
/// Fields are a superset: per-turn has `scored` + `threshold`, subagent-stop
/// adds `parent_session_id`/`agent_type`/`source`, and session-aggregate has
/// `peak_turn`/`session_aggregate`/`final_score`/`action`/`decision`.
#[derive(Debug, Deserialize)]
struct JsonlEntry {
    ts: String,
    session_id: String,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    scored: Option<JsonlScored>,
    #[serde(default)]
    threshold: Option<i32>,
    // ── Subagent-stop fields ──
    #[serde(default)]
    parent_session_id: Option<String>,
    #[serde(default)]
    agent_type: Option<String>,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    privacy_veto: Option<String>,
    // ── Session-aggregate fields ──
    #[serde(default)]
    peak_turn: Option<i32>,
    #[serde(default)]
    session_aggregate: Option<i32>,
    #[serde(default)]
    final_score: Option<i32>,
    #[serde(default)]
    turn_count: Option<i32>,
    #[serde(default)]
    subagent_count: Option<i32>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default)]
    decision: Option<String>,
    #[serde(default)]
    veto_reason: Option<String>,
}

/// Privacy guard that detects sensitive content via compiled regex patterns.
///
/// Ports the 7 pattern groups from `privacy-guard.sh`:
///
/// 1. Anthropic API keys (`sk-ant-`)
/// 2. `OpenAI` API keys (`sk-proj-`)
/// 3. AWS access key IDs (`AKIA...`)
/// 4. GitHub tokens (`ghp_`, `gho_`)
/// 5. Bearer auth headers
/// 6. PEM private keys
/// 7. JWTs (`eyJ...eyJ...`)
pub struct PrivacyGuard {
    patterns: Vec<(Regex, &'static str)>,
}

impl PrivacyGuard {
    /// Compile all 7 privacy-pattern groups (8 regexes total).
    ///
    /// # Panics
    ///
    /// Panics if any regex fails to compile. All patterns are compile-time
    /// constants, so this is unreachable in practice.
    #[must_use]
    pub fn new() -> Self {
        let patterns = vec![
            (
                Regex::new(r"sk-ant-[a-zA-Z0-9_-]{20,}").expect("valid regex"),
                "Anthropic API key",
            ),
            (
                Regex::new(r"sk-proj-[a-zA-Z0-9_-]{20,}").expect("valid regex"),
                "OpenAI API key",
            ),
            (
                Regex::new(r"AKIA[0-9A-Z]{16}").expect("valid regex"),
                "AWS access key",
            ),
            (
                Regex::new(r"ghp_[a-zA-Z0-9]{36}").expect("valid regex"),
                "GitHub personal access token",
            ),
            (
                Regex::new(r"gho_[a-zA-Z0-9]{36}").expect("valid regex"),
                "GitHub OAuth token",
            ),
            (
                Regex::new(r"Bearer [A-Za-z0-9._-]{20,}").expect("valid regex"),
                "Bearer auth header",
            ),
            (
                Regex::new(r"-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----")
                    .expect("valid regex"),
                "private key material",
            ),
            (
                Regex::new(r"eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+").expect("valid regex"),
                "JWT token",
            ),
        ];
        Self { patterns }
    }

    /// Check `content` against all patterns. Returns `Some(reason)` on
    /// first match, `None` if clean.
    #[must_use]
    pub fn check(&self, content: &str) -> Option<String> {
        for (re, label) in &self.patterns {
            if re.is_match(content) {
                return Some(format!("PRIVACY: {label} detected"));
            }
        }
        None
    }
}

impl Default for PrivacyGuard {
    fn default() -> Self {
        Self::new()
    }
}

// ── Store ────────────────────────────────────────────────────────────────

/// Maximum retries for `SQLITE_BUSY` errors on insert.
const BUSY_RETRIES: u32 = 3;
/// Backoff between `SQLITE_BUSY` retries.
const BUSY_BACKOFF: Duration = Duration::from_millis(100);

/// SQLite-backed hook event store.
pub struct HooksStore {
    conn: Connection,
}

impl HooksStore {
    /// Open (or create) a hooks store at the given path.
    ///
    /// Sets WAL mode and busy timeout, then runs migrations.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).map_err(wrap_sql)?;
        Self::init(conn)
    }

    /// In-memory store for tests.
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(wrap_sql)?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            r"
            PRAGMA journal_mode = WAL;
            PRAGMA busy_timeout = 5000;

            CREATE TABLE IF NOT EXISTS hook_events (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                ts                      TEXT    NOT NULL,
                session_id              TEXT    NOT NULL,
                parent_session_id       TEXT,
                hook_type               TEXT    NOT NULL DEFAULT 'stop',
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
                raw_json_hash           TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_hook_events_session
                ON hook_events(session_id);
            CREATE INDEX IF NOT EXISTS idx_hook_events_ts
                ON hook_events(ts);
            CREATE INDEX IF NOT EXISTS idx_hook_events_parent
                ON hook_events(parent_session_id) WHERE parent_session_id IS NOT NULL;
            CREATE UNIQUE INDEX IF NOT EXISTS idx_hook_events_dedup
                ON hook_events(raw_json_hash);
            ",
        )
        .map_err(wrap_sql)?;

        Ok(Self { conn })
    }

    /// Compute the SHA-256 hex digest of raw JSON bytes.
    pub fn hash_raw(raw: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(raw.as_bytes());
        format!("{:x}", hasher.finalize())
    }

    /// Insert a hook event with `SQLITE_BUSY` retry (3 retries, 100ms backoff).
    ///
    /// Fails on duplicate `raw_json_hash` (use [`insert_event_idempotent`]
    /// for silent dedup).
    pub fn insert_event(&self, event: &HookEvent) -> Result<()> {
        let mut last_err = None;
        for attempt in 0..=BUSY_RETRIES {
            match self.do_insert(event) {
                Ok(()) => return Ok(()),
                Err(e) => {
                    // Check if this is a BUSY error worth retrying.
                    let is_busy = format!("{e}").contains("database is locked");
                    if is_busy && attempt < BUSY_RETRIES {
                        thread::sleep(BUSY_BACKOFF);
                        last_err = Some(e);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(last_err
            .unwrap_or_else(|| Error::Other(anyhow::anyhow!("insert_event: exhausted retries"))))
    }

    fn do_insert(&self, event: &HookEvent) -> Result<()> {
        self.conn
            .execute(
                r"INSERT INTO hook_events (
                    ts, session_id, parent_session_id, hook_type, source, cwd,
                    score, threshold, signals, metrics, agent_type, privacy_veto,
                    decision, peak_turn_score, session_aggregate_score, final_score,
                    turn_count, subagent_count, raw_json, raw_json_hash
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15, ?16,
                    ?17, ?18, ?19, ?20
                )",
                params![
                    event.ts,
                    event.session_id,
                    event.parent_session_id,
                    event.hook_type,
                    event.source,
                    event.cwd,
                    event.score,
                    event.threshold,
                    event.signals,
                    event.metrics,
                    event.agent_type,
                    event.privacy_veto,
                    event.decision,
                    event.peak_turn_score,
                    event.session_aggregate_score,
                    event.final_score,
                    event.turn_count,
                    event.subagent_count,
                    event.raw_json,
                    event.raw_json_hash,
                ],
            )
            .map_err(wrap_sql)?;
        Ok(())
    }

    /// Insert or silently ignore if `raw_json_hash` already exists.
    pub fn insert_event_idempotent(&self, event: &HookEvent) -> Result<()> {
        self.conn
            .execute(
                r"INSERT OR IGNORE INTO hook_events (
                    ts, session_id, parent_session_id, hook_type, source, cwd,
                    score, threshold, signals, metrics, agent_type, privacy_veto,
                    decision, peak_turn_score, session_aggregate_score, final_score,
                    turn_count, subagent_count, raw_json, raw_json_hash
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    ?7, ?8, ?9, ?10, ?11, ?12,
                    ?13, ?14, ?15, ?16,
                    ?17, ?18, ?19, ?20
                )",
                params![
                    event.ts,
                    event.session_id,
                    event.parent_session_id,
                    event.hook_type,
                    event.source,
                    event.cwd,
                    event.score,
                    event.threshold,
                    event.signals,
                    event.metrics,
                    event.agent_type,
                    event.privacy_veto,
                    event.decision,
                    event.peak_turn_score,
                    event.session_aggregate_score,
                    event.final_score,
                    event.turn_count,
                    event.subagent_count,
                    event.raw_json,
                    event.raw_json_hash,
                ],
            )
            .map_err(wrap_sql)?;
        Ok(())
    }

    /// All events for a given session, ordered by timestamp.
    pub fn query_session(&self, session_id: &str) -> Result<Vec<HookEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                r"SELECT
                    id, ts, session_id, parent_session_id, hook_type, source, cwd,
                    score, threshold, signals, metrics, agent_type, privacy_veto,
                    decision, peak_turn_score, session_aggregate_score, final_score,
                    turn_count, subagent_count, raw_json, raw_json_hash
                FROM hook_events
                WHERE session_id = ?1
                ORDER BY ts ASC",
            )
            .map_err(wrap_sql)?;

        let rows = stmt
            .query_map(params![session_id], row_to_event)
            .map_err(wrap_sql)?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row.map_err(wrap_sql)?);
        }
        Ok(events)
    }

    #[allow(clippy::too_many_lines)]
    pub fn query_summary(&self, window: Duration) -> Result<HooksSummary> {
        let since = Utc::now()
            - chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::days(365));
        let since_str = since.to_rfc3339();

        // Basic counts.
        let total_events: u64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM hook_events WHERE ts >= ?1",
                params![since_str],
                |r| r.get(0),
            )
            .map_err(wrap_sql)?;

        let total_ingests: u64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM hook_events WHERE ts >= ?1 AND decision = 'ingest'",
                params![since_str],
                |r| r.get(0),
            )
            .map_err(wrap_sql)?;

        let total_skips: u64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM hook_events WHERE ts >= ?1 AND decision = 'skip'",
                params![since_str],
                |r| r.get(0),
            )
            .map_err(wrap_sql)?;

        let total_vetoes: u64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM hook_events WHERE ts >= ?1 AND decision = 'veto'",
                params![since_str],
                |r| r.get(0),
            )
            .map_err(wrap_sql)?;

        let privacy_vetoes: u64 = self
            .conn
            .query_row(
                "SELECT COUNT(*) FROM hook_events WHERE ts >= ?1 AND privacy_veto IS NOT NULL",
                params![since_str],
                |r| r.get(0),
            )
            .map_err(wrap_sql)?;

        // Score stats.
        let avg_score: f64 = self
            .conn
            .query_row(
                "SELECT COALESCE(AVG(CAST(score AS REAL)), 0.0) FROM hook_events WHERE ts >= ?1 AND score IS NOT NULL",
                params![since_str],
                |r| r.get(0),
            )
            .map_err(wrap_sql)?;

        let peak_score: i32 = self
            .conn
            .query_row(
                "SELECT COALESCE(MAX(score), 0) FROM hook_events WHERE ts >= ?1",
                params![since_str],
                |r| r.get(0),
            )
            .map_err(wrap_sql)?;

        // Ingests per hour.
        let hours = window.as_secs_f64() / 3600.0;
        #[allow(clippy::cast_precision_loss)]
        let ingests_per_hour = if hours > 0.0 {
            total_ingests as f64 / hours
        } else {
            0.0
        };

        // Score distribution.
        let mut score_distribution = HashMap::new();
        {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT score, COUNT(*) FROM hook_events WHERE ts >= ?1 AND score IS NOT NULL GROUP BY score",
                )
                .map_err(wrap_sql)?;
            let rows = stmt
                .query_map(params![since_str], |row| {
                    Ok((row.get::<_, i32>(0)?, row.get::<_, u64>(1)?))
                })
                .map_err(wrap_sql)?;
            for row in rows {
                let (score, count) = row.map_err(wrap_sql)?;
                score_distribution.insert(score, count);
            }
        }

        // Signal frequency — signals are stored as JSON arrays like '["long-response","markdown-synthesis"]'.
        let mut signal_frequency = HashMap::new();
        {
            let mut stmt = self
                .conn
                .prepare("SELECT signals FROM hook_events WHERE ts >= ?1 AND signals IS NOT NULL")
                .map_err(wrap_sql)?;
            let rows = stmt
                .query_map(params![since_str], |row| row.get::<_, String>(0))
                .map_err(wrap_sql)?;
            for row in rows {
                let signals_json = row.map_err(wrap_sql)?;
                if let Ok(signals) = serde_json::from_str::<Vec<String>>(&signals_json) {
                    for signal in signals {
                        *signal_frequency.entry(signal).or_insert(0u64) += 1;
                    }
                }
            }
        }

        // By hook type.
        let mut by_hook_type = HashMap::new();
        {
            let mut stmt = self
                .conn
                .prepare(
                    "SELECT hook_type, COUNT(*) FROM hook_events WHERE ts >= ?1 GROUP BY hook_type",
                )
                .map_err(wrap_sql)?;
            let rows = stmt
                .query_map(params![since_str], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
                })
                .map_err(wrap_sql)?;
            for row in rows {
                let (ht, count) = row.map_err(wrap_sql)?;
                by_hook_type.insert(ht, count);
            }
        }

        // Last timestamps.
        let last_ingest_ts: Option<String> = self
            .conn
            .query_row(
                "SELECT ts FROM hook_events WHERE ts >= ?1 AND decision = 'ingest' ORDER BY ts DESC LIMIT 1",
                params![since_str],
                |r| r.get(0),
            )
            .optional()
            .map_err(wrap_sql)?;

        let last_event_ts: Option<String> = self
            .conn
            .query_row(
                "SELECT ts FROM hook_events WHERE ts >= ?1 ORDER BY ts DESC LIMIT 1",
                params![since_str],
                |r| r.get(0),
            )
            .optional()
            .map_err(wrap_sql)?;

        Ok(HooksSummary {
            total_events,
            total_ingests,
            total_skips,
            total_vetoes,
            ingests_per_hour,
            score_distribution,
            signal_frequency,
            by_hook_type,
            avg_score,
            peak_score,
            privacy_vetoes,
            last_ingest_ts,
            last_event_ts,
        })
    }

    #[allow(clippy::cast_possible_wrap)]
    pub fn query_events(
        &self,
        limit: usize,
        since: Option<DateTime<Utc>>,
    ) -> Result<Vec<HookEvent>> {
        let (sql, since_str);
        if let Some(dt) = since {
            since_str = dt.to_rfc3339();
            sql = r"SELECT
                    id, ts, session_id, parent_session_id, hook_type, source, cwd,
                    score, threshold, signals, metrics, agent_type, privacy_veto,
                    decision, peak_turn_score, session_aggregate_score, final_score,
                    turn_count, subagent_count, raw_json, raw_json_hash
                FROM hook_events
                WHERE ts >= ?1
                ORDER BY ts DESC
                LIMIT ?2";
        } else {
            since_str = String::new();
            sql = r"SELECT
                    id, ts, session_id, parent_session_id, hook_type, source, cwd,
                    score, threshold, signals, metrics, agent_type, privacy_veto,
                    decision, peak_turn_score, session_aggregate_score, final_score,
                    turn_count, subagent_count, raw_json, raw_json_hash
                FROM hook_events
                ORDER BY ts DESC
                LIMIT ?1";
        }

        let mut stmt = self.conn.prepare(sql).map_err(wrap_sql)?;
        let rows = if since.is_some() {
            stmt.query_map(params![since_str, limit as i64], row_to_event)
                .map_err(wrap_sql)?
        } else {
            stmt.query_map(params![limit as i64], row_to_event)
                .map_err(wrap_sql)?
        };

        let mut events = Vec::new();
        for row in rows {
            events.push(row.map_err(wrap_sql)?);
        }
        Ok(events)
    }

    #[allow(clippy::cast_possible_wrap)]
    pub fn query_errors(&self, limit: usize) -> Result<Vec<HookEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                r"SELECT
                    id, ts, session_id, parent_session_id, hook_type, source, cwd,
                    score, threshold, signals, metrics, agent_type, privacy_veto,
                    decision, peak_turn_score, session_aggregate_score, final_score,
                    turn_count, subagent_count, raw_json, raw_json_hash
                FROM hook_events
                WHERE decision = 'veto' OR privacy_veto IS NOT NULL
                ORDER BY ts DESC
                LIMIT ?1",
            )
            .map_err(wrap_sql)?;

        let rows = stmt
            .query_map(params![limit as i64], row_to_event)
            .map_err(wrap_sql)?;

        let mut events = Vec::new();
        for row in rows {
            events.push(row.map_err(wrap_sql)?);
        }
        Ok(events)
    }

    /// Check if the database is in WAL mode.
    pub fn is_wal_mode(&self) -> Result<bool> {
        let mode: String = self
            .conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .map_err(wrap_sql)?;
        Ok(mode.eq_ignore_ascii_case("wal"))
    }

    /// Import events from a JSONL file into the database.
    ///
    /// Each line is parsed independently — malformed lines are skipped
    /// with a warning in the report. Dedup is via `INSERT OR IGNORE` on
    /// `raw_json_hash` (SHA-256 of exact line bytes, NOT re-serialized).
    ///
    /// When `dry_run` is true the file is fully parsed and validated but
    /// nothing is written to the database.
    #[allow(clippy::too_many_lines)]
    pub fn import_jsonl(&self, jsonl_path: &Path, dry_run: bool) -> Result<ImportReport> {
        let guard = PrivacyGuard::new();
        let mut report = ImportReport::default();

        let file = File::open(jsonl_path).map_err(|e| {
            Error::Other(anyhow::anyhow!(
                "import_jsonl: cannot open {}: {e}",
                jsonl_path.display()
            ))
        })?;
        let reader = BufReader::new(file);

        if !dry_run {
            self.conn
                .execute_batch("BEGIN TRANSACTION")
                .map_err(wrap_sql)?;
        }

        Self::do_import_lines(self, reader, &guard, &mut report, dry_run);

        if !dry_run {
            self.conn.execute_batch("COMMIT").map_err(wrap_sql)?;
        }

        Ok(report)
    }

    #[allow(clippy::too_many_lines)]
    fn do_import_lines(
        &self,
        reader: BufReader<File>,
        guard: &PrivacyGuard,
        report: &mut ImportReport,
        dry_run: bool,
    ) {
        for raw_line_result in reader.lines() {
            let raw_line = match raw_line_result {
                Ok(l) => l,
                Err(e) => {
                    report.skipped_malformed += 1;
                    report.errors.push(format!("IO error reading line: {e}"));
                    continue;
                }
            };

            report.total_lines += 1;

            let trimmed = raw_line.trim();
            if trimmed.is_empty() {
                report.skipped_malformed += 1;
                continue;
            }

            let entry: JsonlEntry = match serde_json::from_str(trimmed) {
                Ok(e) => e,
                Err(e) => {
                    report.skipped_malformed += 1;
                    report.errors.push(format!(
                        "malformed JSON at line {}: {e}",
                        report.total_lines
                    ));
                    continue;
                }
            };

            let raw_json_hash = Self::hash_raw(trimmed);

            let hook_type = if entry.source.as_deref() == Some("subagent-stop") {
                "subagent-stop"
            } else if entry.action.as_deref() == Some("session-aggregate") {
                "session-end"
            } else {
                "stop"
            };

            let (score, signals, metrics, last_user_prompt) = if let Some(scored) = entry.scored {
                (
                    Some(scored.score),
                    Some(serde_json::to_string(&scored.signals).unwrap_or_default()),
                    scored
                        .metrics
                        .map(|m| serde_json::to_string(&m).unwrap_or_default()),
                    scored.last_user_prompt,
                )
            } else {
                (None, None, None, None)
            };

            let guard_result = last_user_prompt.as_deref().and_then(|p| guard.check(p));
            let privacy_veto = guard_result.or(entry.privacy_veto).or(entry.veto_reason);

            if privacy_veto.is_some() {
                report.privacy_flagged += 1;
            }

            let event = HookEvent {
                id: None,
                ts: entry.ts,
                session_id: entry.session_id,
                parent_session_id: entry.parent_session_id,
                hook_type: hook_type.to_string(),
                source: entry.source,
                cwd: entry.cwd,
                score,
                threshold: entry.threshold,
                signals,
                metrics,
                agent_type: entry.agent_type,
                privacy_veto,
                decision: entry.decision,
                peak_turn_score: entry.peak_turn,
                session_aggregate_score: entry.session_aggregate,
                final_score: entry.final_score,
                turn_count: entry.turn_count,
                subagent_count: entry.subagent_count,
                raw_json: Some(trimmed.to_string()),
                raw_json_hash,
            };

            if dry_run {
                report.imported += 1;
                continue;
            }

            match self.insert_event_idempotent(&event) {
                Ok(()) => {
                    if self.conn.changes() > 0 {
                        report.imported += 1;
                    } else {
                        report.skipped_duplicate += 1;
                    }
                }
                Err(e) => {
                    report
                        .errors
                        .push(format!("insert error at line {}: {e}", report.total_lines));
                }
            }
        }
    }
}

// ── Row mapper ───────────────────────────────────────────────────────────

fn row_to_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<HookEvent> {
    Ok(HookEvent {
        id: row.get(0)?,
        ts: row.get(1)?,
        session_id: row.get(2)?,
        parent_session_id: row.get(3)?,
        hook_type: row.get(4)?,
        source: row.get(5)?,
        cwd: row.get(6)?,
        score: row.get(7)?,
        threshold: row.get(8)?,
        signals: row.get(9)?,
        metrics: row.get(10)?,
        agent_type: row.get(11)?,
        privacy_veto: row.get(12)?,
        decision: row.get(13)?,
        peak_turn_score: row.get(14)?,
        session_aggregate_score: row.get(15)?,
        final_score: row.get(16)?,
        turn_count: row.get(17)?,
        subagent_count: row.get(18)?,
        raw_json: row.get(19)?,
        raw_json_hash: row.get(20)?,
    })
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal test event with sensible defaults.
    fn test_event(session_id: &str, decision: &str, score: i32) -> HookEvent {
        let raw = format!(
            r#"{{"ts":"2026-04-15T10:00:00Z","session_id":"{session_id}","score":{score}}}"#
        );
        HookEvent {
            id: None,
            ts: "2026-04-15T10:00:00Z".to_string(),
            session_id: session_id.to_string(),
            parent_session_id: None,
            hook_type: "stop".to_string(),
            source: None,
            cwd: Some("/tmp".to_string()),
            score: Some(score),
            threshold: Some(6),
            signals: Some(r#"["long-response","markdown-synthesis"]"#.to_string()),
            metrics: Some(r#"{"files_read":5,"citations":3}"#.to_string()),
            agent_type: None,
            privacy_veto: None,
            decision: Some(decision.to_string()),
            peak_turn_score: None,
            session_aggregate_score: None,
            final_score: None,
            turn_count: None,
            subagent_count: None,
            raw_json: Some(raw.clone()),
            raw_json_hash: HooksStore::hash_raw(&raw),
        }
    }

    #[test]
    fn create_insert_and_query_back() {
        let store = HooksStore::in_memory().unwrap();
        let event = test_event("sess-1", "ingest", 7);
        store.insert_event(&event).unwrap();

        let events = store.query_session("sess-1").unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session_id, "sess-1");
        assert_eq!(events[0].score, Some(7));
        assert_eq!(events[0].decision.as_deref(), Some("ingest"));
        assert!(events[0].id.is_some(), "id should be populated on read");
    }

    #[test]
    fn idempotent_insert_rejects_duplicate() {
        let store = HooksStore::in_memory().unwrap();
        let event = test_event("sess-2", "skip", 2);

        store.insert_event_idempotent(&event).unwrap();
        // Second insert with same raw_json_hash should be silently ignored.
        store.insert_event_idempotent(&event).unwrap();

        let events = store.query_session("sess-2").unwrap();
        assert_eq!(events.len(), 1, "duplicate should have been ignored");
    }

    #[test]
    fn non_idempotent_insert_fails_on_duplicate() {
        let store = HooksStore::in_memory().unwrap();
        let event = test_event("sess-3", "skip", 1);

        store.insert_event(&event).unwrap();
        // Second insert should fail due to unique constraint.
        let result = store.insert_event(&event);
        assert!(result.is_err(), "duplicate insert should fail");
    }

    #[test]
    fn wal_mode_is_active() {
        let _mem_store = HooksStore::in_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_hooks.sqlite");
        let file_store = HooksStore::open(&db_path).unwrap();
        assert!(
            file_store.is_wal_mode().unwrap(),
            "file-backed store should use WAL mode"
        );
    }

    #[test]
    fn query_summary_returns_correct_aggregates() {
        let store = HooksStore::in_memory().unwrap();

        // Insert a mix of events.
        let mut e1 = test_event("sess-a", "ingest", 8);
        e1.ts = Utc::now().to_rfc3339();
        e1.raw_json_hash = HooksStore::hash_raw("unique-1");
        store.insert_event(&e1).unwrap();

        let mut e2 = test_event("sess-a", "skip", 2);
        e2.ts = Utc::now().to_rfc3339();
        e2.raw_json_hash = HooksStore::hash_raw("unique-2");
        store.insert_event(&e2).unwrap();

        let mut e3 = test_event("sess-b", "veto", 0);
        e3.ts = Utc::now().to_rfc3339();
        e3.privacy_veto = Some("contains API key".to_string());
        e3.raw_json_hash = HooksStore::hash_raw("unique-3");
        store.insert_event(&e3).unwrap();

        let summary = store.query_summary(Duration::from_secs(3600)).unwrap();
        assert_eq!(summary.total_events, 3);
        assert_eq!(summary.total_ingests, 1);
        assert_eq!(summary.total_skips, 1);
        assert_eq!(summary.total_vetoes, 1);
        assert_eq!(summary.privacy_vetoes, 1);
        assert_eq!(summary.peak_score, 8);
        assert!(summary.avg_score > 0.0);
        assert!(summary.by_hook_type.contains_key("stop"));

        // Signal frequency: each event has 2 signals, so each should appear 3 times.
        assert_eq!(
            *summary.signal_frequency.get("long-response").unwrap_or(&0),
            3
        );
        assert_eq!(
            *summary
                .signal_frequency
                .get("markdown-synthesis")
                .unwrap_or(&0),
            3
        );
    }

    #[test]
    fn query_errors_filters_correctly() {
        let store = HooksStore::in_memory().unwrap();

        // Normal event — should NOT appear in errors.
        let mut e1 = test_event("sess-ok", "ingest", 8);
        e1.raw_json_hash = HooksStore::hash_raw("ok-event");
        store.insert_event(&e1).unwrap();

        // Veto event — SHOULD appear.
        let mut e2 = test_event("sess-veto", "veto", 0);
        e2.raw_json_hash = HooksStore::hash_raw("veto-event");
        store.insert_event(&e2).unwrap();

        // Privacy veto (decision may be skip but privacy_veto is set) — SHOULD appear.
        let mut e3 = test_event("sess-privacy", "skip", 3);
        e3.privacy_veto = Some("detected PHI".to_string());
        e3.raw_json_hash = HooksStore::hash_raw("privacy-event");
        store.insert_event(&e3).unwrap();

        let errors = store.query_errors(10).unwrap();
        assert_eq!(errors.len(), 2, "should have 2 error events");
        let session_ids: Vec<&str> = errors.iter().map(|e| e.session_id.as_str()).collect();
        assert!(session_ids.contains(&"sess-veto"));
        assert!(session_ids.contains(&"sess-privacy"));
        assert!(!session_ids.contains(&"sess-ok"));
    }

    #[test]
    fn query_events_pagination() {
        let store = HooksStore::in_memory().unwrap();

        for i in 0..10 {
            let mut event = test_event(&format!("sess-{i}"), "skip", i);
            event.ts = format!("2026-04-15T10:{i:02}:00Z");
            event.raw_json_hash = HooksStore::hash_raw(&format!("page-{i}"));
            store.insert_event(&event).unwrap();
        }

        // Limit to 5.
        let page = store.query_events(5, None).unwrap();
        assert_eq!(page.len(), 5);
        // Most recent first.
        assert!(page[0].ts > page[4].ts);
    }

    #[test]
    fn hash_raw_is_deterministic() {
        let h1 = HooksStore::hash_raw("hello world");
        let h2 = HooksStore::hash_raw("hello world");
        let h3 = HooksStore::hash_raw("hello world!");
        assert_eq!(h1, h2);
        assert_ne!(h1, h3);
        assert_eq!(h1.len(), 64, "SHA-256 hex digest should be 64 chars");
    }

    #[test]
    fn query_events_with_since_filter() {
        let store = HooksStore::in_memory().unwrap();

        let mut old = test_event("sess-old", "skip", 1);
        old.ts = "2020-01-01T00:00:00Z".to_string();
        old.raw_json_hash = HooksStore::hash_raw("old-event");
        store.insert_event(&old).unwrap();

        let mut recent = test_event("sess-new", "ingest", 9);
        recent.ts = Utc::now().to_rfc3339();
        recent.raw_json_hash = HooksStore::hash_raw("new-event");
        store.insert_event(&recent).unwrap();

        let since = Utc::now() - chrono::Duration::hours(1);
        let events = store.query_events(100, Some(since)).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].session_id, "sess-new");
    }

    #[test]
    fn file_backed_store_persists() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("persist_test.sqlite");

        // Insert, close, reopen, verify.
        {
            let store = HooksStore::open(&db_path).unwrap();
            let event = test_event("persist-sess", "ingest", 5);
            store.insert_event(&event).unwrap();
        }
        {
            let store = HooksStore::open(&db_path).unwrap();
            let events = store.query_session("persist-sess").unwrap();
            assert_eq!(events.len(), 1);
        }
    }

    // ── JSONL import tests ──────────────────────────────────────────

    fn write_jsonl(dir: &tempfile::TempDir, lines: &[&str]) -> std::path::PathBuf {
        let path = dir.path().join("test.jsonl");
        std::fs::write(&path, lines.join("\n")).unwrap();
        path
    }

    #[test]
    fn import_jsonl_malformed_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_jsonl(
            &dir,
            &[
                "not json at all",
                "",
                r#"{"ts":"2026-04-15T10:00:00Z","session_id":"s1","scored":{"score":7,"signals":[],"metrics":{}},"threshold":6}"#,
                "{incomplete",
            ],
        );

        let store = HooksStore::in_memory().unwrap();
        let report = store.import_jsonl(&path, false).unwrap();

        assert_eq!(report.total_lines, 4);
        assert_eq!(report.imported, 1);
        assert_eq!(report.skipped_malformed, 3);
    }

    #[test]
    fn import_jsonl_idempotent_reimport() {
        let dir = tempfile::tempdir().unwrap();
        let line = r#"{"ts":"2026-04-15T10:00:00Z","session_id":"s1","scored":{"score":7,"signals":["test"],"metrics":{}},"threshold":6}"#;
        let path = write_jsonl(&dir, &[line]);

        let store = HooksStore::in_memory().unwrap();
        let r1 = store.import_jsonl(&path, false).unwrap();
        assert_eq!(r1.imported, 1);
        assert_eq!(r1.skipped_duplicate, 0);

        let r2 = store.import_jsonl(&path, false).unwrap();
        assert_eq!(r2.imported, 0);
        assert_eq!(r2.skipped_duplicate, 1);

        let events = store.query_session("s1").unwrap();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn import_jsonl_dry_run_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let line = r#"{"ts":"2026-04-15T10:00:00Z","session_id":"s1","scored":{"score":7,"signals":["test"],"metrics":{}},"threshold":6}"#;
        let path = write_jsonl(&dir, &[line]);

        let store = HooksStore::in_memory().unwrap();
        let report = store.import_jsonl(&path, true).unwrap();
        assert_eq!(report.imported, 1);

        let events = store.query_session("s1").unwrap();
        assert_eq!(events.len(), 0, "dry_run should not write to DB");
    }

    #[test]
    fn import_jsonl_privacy_flag_sets_veto() {
        let dir = tempfile::tempdir().unwrap();
        let line = r#"{"ts":"2026-04-15T10:00:00Z","session_id":"s1","scored":{"score":7,"signals":["test"],"last_user_prompt":"my key is sk-ant-aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT","metrics":{}},"threshold":6}"#;
        let path = write_jsonl(&dir, &[line]);

        let store = HooksStore::in_memory().unwrap();
        let report = store.import_jsonl(&path, false).unwrap();

        assert_eq!(report.imported, 1);
        assert_eq!(report.privacy_flagged, 1);

        let events = store.query_session("s1").unwrap();
        assert_eq!(events.len(), 1);
        assert!(events[0].privacy_veto.is_some());
        assert!(events[0]
            .privacy_veto
            .as_ref()
            .unwrap()
            .contains("Anthropic"));
    }

    #[test]
    fn privacy_guard_catches_all_seven_patterns() {
        let guard = PrivacyGuard::new();

        // 1: Anthropic API key
        assert!(guard.check("sk-ant-aB3cD4eF5gH6iJ7kL8mN9oP0").is_some());
        // 2: OpenAI API key
        assert!(guard.check("sk-proj-aB3cD4eF5gH6iJ7kL8mN9oP").is_some());
        // 3: AWS access key ID
        assert!(guard.check("AKIAIOSFODNN7EXAMPLE").is_some());
        // 4a: GitHub PAT
        assert!(guard
            .check("ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij")
            .is_some());
        // 4b: GitHub OAuth
        assert!(guard
            .check("gho_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghij")
            .is_some());
        // 5: Bearer auth header
        assert!(guard.check("Bearer eyJhbGciOiJSUzI1NiIs.token").is_some());
        // 6: PEM private keys (all variants)
        assert!(guard.check("-----BEGIN PRIVATE KEY-----").is_some());
        assert!(guard.check("-----BEGIN RSA PRIVATE KEY-----").is_some());
        assert!(guard.check("-----BEGIN EC PRIVATE KEY-----").is_some());
        assert!(guard.check("-----BEGIN OPENSSH PRIVATE KEY-----").is_some());
        // 7: JWT
        assert!(guard
            .check("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkw")
            .is_some());

        // Clean content should pass
        assert!(guard.check("just a normal message").is_none());
        assert!(guard.check("").is_none());
        assert!(guard.check("sk-ant-short").is_none());
    }

    #[test]
    fn import_jsonl_three_variants() {
        // Tests all 3 JSONL entry types: per-turn (stop), subagent-stop, session-aggregate.
        let dir = tempfile::tempdir().unwrap();

        let per_turn = r#"{"ts":"2026-04-15T10:00:00Z","session_id":"s1","cwd":"/tmp","scored":{"score":7,"signals":["long-response"],"metrics":{"files_read":3}},"threshold":6,"decision":"ingest"}"#;
        let subagent_stop = r#"{"ts":"2026-04-15T10:01:00Z","session_id":"s2","parent_session_id":"s1","source":"subagent-stop","agent_type":"explore","cwd":"/tmp","scored":{"score":4,"signals":["code-nav"],"metrics":{}},"threshold":6,"decision":"skip"}"#;
        let session_aggregate = r#"{"ts":"2026-04-15T10:02:00Z","session_id":"s1","action":"session-aggregate","peak_turn":9,"session_aggregate":15,"final_score":8,"turn_count":5,"subagent_count":2,"decision":"ingest"}"#;

        let path = write_jsonl(&dir, &[per_turn, subagent_stop, session_aggregate]);

        let store = HooksStore::in_memory().unwrap();
        let report = store.import_jsonl(&path, false).unwrap();

        assert_eq!(report.total_lines, 3);
        assert_eq!(report.imported, 3);
        assert_eq!(report.skipped_malformed, 0);
        assert_eq!(report.skipped_duplicate, 0);

        // Verify per-turn event
        let events_s1 = store.query_session("s1").unwrap();
        let per_turn_ev = events_s1.iter().find(|e| e.hook_type == "stop").unwrap();
        assert_eq!(per_turn_ev.score, Some(7));
        assert_eq!(per_turn_ev.decision.as_deref(), Some("ingest"));
        assert!(per_turn_ev
            .signals
            .as_ref()
            .unwrap()
            .contains("long-response"));

        // Verify subagent-stop event
        let events_s2 = store.query_session("s2").unwrap();
        assert_eq!(events_s2.len(), 1);
        assert_eq!(events_s2[0].hook_type, "subagent-stop");
        assert_eq!(events_s2[0].parent_session_id.as_deref(), Some("s1"));
        assert_eq!(events_s2[0].agent_type.as_deref(), Some("explore"));

        // Verify session-aggregate event
        let agg_ev = events_s1
            .iter()
            .find(|e| e.hook_type == "session-end")
            .unwrap();
        assert_eq!(agg_ev.peak_turn_score, Some(9));
        assert_eq!(agg_ev.session_aggregate_score, Some(15));
        assert_eq!(agg_ev.final_score, Some(8));
        assert_eq!(agg_ev.turn_count, Some(5));
        assert_eq!(agg_ev.subagent_count, Some(2));
    }

    #[test]
    fn concurrent_writes_no_data_loss() {
        // Multiple threads writing unique events concurrently — all should succeed.
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("concurrent_test.sqlite");
        let db_path = Arc::new(db_path);

        // Pre-create the DB so all threads use the same schema.
        {
            let _store = HooksStore::open(&db_path).unwrap();
        }

        let num_threads = 4;
        let events_per_thread = 10;
        let mut handles = Vec::new();

        for t in 0..num_threads {
            let path = Arc::clone(&db_path);
            handles.push(thread::spawn(move || {
                let store = HooksStore::open(&path).unwrap();
                for i in 0..events_per_thread {
                    let raw = format!(r#"{{"thread":{t},"idx":{i},"unique":true}}"#);
                    let event = HookEvent {
                        id: None,
                        ts: format!("2026-04-15T10:{t:02}:{i:02}Z"),
                        session_id: format!("sess-t{t}"),
                        parent_session_id: None,
                        hook_type: "stop".to_string(),
                        source: None,
                        cwd: None,
                        score: Some(i),
                        threshold: Some(6),
                        signals: None,
                        metrics: None,
                        agent_type: None,
                        privacy_veto: None,
                        decision: Some("skip".to_string()),
                        peak_turn_score: None,
                        session_aggregate_score: None,
                        final_score: None,
                        turn_count: None,
                        subagent_count: None,
                        raw_json: Some(raw.clone()),
                        raw_json_hash: HooksStore::hash_raw(&raw),
                    };
                    store.insert_event(&event).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().expect("thread panicked");
        }

        // Verify all events landed.
        let store = HooksStore::open(&db_path).unwrap();
        let total_expected = num_threads * events_per_thread;
        let all_events = store.query_events(1000, None).unwrap();
        assert_eq!(
            all_events.len(),
            total_expected as usize,
            "Expected {total_expected} events from {num_threads} threads × {events_per_thread} each, got {}",
            all_events.len()
        );
    }

    #[test]
    fn import_jsonl_raw_hash_uses_exact_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let raw = r#"{"ts":"2026-04-15T10:00:00Z","session_id":"s1","scored":{"score":7,"signals":[],"metrics":{}},"threshold":6}"#;
        let path = write_jsonl(&dir, &[raw]);

        let store = HooksStore::in_memory().unwrap();
        store.import_jsonl(&path, false).unwrap();

        let events = store.query_session("s1").unwrap();
        assert_eq!(events[0].raw_json_hash, HooksStore::hash_raw(raw));
        assert_eq!(events[0].raw_json.as_deref(), Some(raw));
    }
}
