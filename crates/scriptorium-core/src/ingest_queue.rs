//! Decoupled enqueue + drain pipeline.
//!
//! Automation sources (session-end hook, watchers) call [`enqueue`] for an
//! O(filesystem) marker write with no LLM call. A separate [`drain`] (driven
//! by launchd) takes a non-blocking lock, dedups markers by canonical content
//! hash, and ingests only the survivors.
//!
//! See `.sisyphus/plans/ingest-queue-drain.md` for the full design.

use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use fd_lock::RwLock as FdRwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::doctor::DoctorCheck;
use crate::error::{Error, Result};
use crate::llm::LlmProvider;
use crate::vault::Vault;

const QUEUE_SUBDIR: &str = "ingest-queue";
const DRAIN_LOCK_FILE: &str = "drain.lock";
const DRAIN_PID_FILE: &str = "drain.pid";
const DRAIN_STATS_FILE: &str = "drain-stats.json";
const HASH_STORE_FILE: &str = "source-hashes.txt";
const MARKER_SCHEMA_VERSION: u32 = 1;
const DRAIN_STATS_MAX_RUNS: usize = 100;

const CANONICAL_STRIP_PREFIXES: &[&str] = &[
    "Session ID:",
    "Peak Turn Score:",
    "Session Aggregate Score:",
    "Final Score:",
    "Turn Count:",
    "Subagent Count:",
];

/// Queue marker on disk: `<vault>/.scriptorium/ingest-queue/<unix-ts>-<8char>.json`.
///
/// Serialized via `serde_json`. `version` lets future incompatible schema
/// changes be detected; unknown versions cause [`drain`] to log + skip.
/// `source` is stored absolute (the session-end hook produces files outside
/// the vault).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueMarker {
    /// Schema version (currently 1).
    pub version: u32,
    /// Absolute path at enqueue time.
    pub source: PathBuf,
    /// Optional Claude Code session id for traceability.
    pub session_id: Option<String>,
    /// RFC3339 enqueue timestamp.
    pub enqueued_at: DateTime<Utc>,
}

/// Tunable parameters for [`drain`].
#[derive(Debug, Clone)]
pub struct DrainConfig {
    /// Minimum age before a marker is eligible. Default 120s.
    pub debounce: Duration,
    /// Cap on markers processed per drain run. `None` = unlimited.
    pub max_per_run: Option<usize>,
    /// Skip ingest, just dedup-log.
    pub dry_run: bool,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            debounce: Duration::from_secs(120),
            max_per_run: None,
            dry_run: false,
        }
    }
}

/// Outcome of a single [`drain`] invocation.
#[derive(Debug, Clone, Serialize)]
pub struct DrainReport {
    /// Markers visible at scan time.
    pub considered: usize,
    /// Markers younger than `debounce`, left on disk.
    pub skipped_young: usize,
    /// Hash already in `source-hashes.txt`.
    pub skipped_dup: usize,
    /// Source file no longer exists.
    pub skipped_missing: usize,
    /// Marker schema version unknown to this binary.
    pub skipped_unknown_version: usize,
    /// Committed (includes redundant LLM skips).
    pub ingested: usize,
    /// LLM said `redundant: true`.
    pub redundant_skips: usize,
    /// Per-marker failures; the marker stays on disk for retry.
    pub failures: Vec<DrainFailure>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
}

/// Per-marker failure entry recorded in [`DrainReport::failures`].
#[derive(Debug, Clone, Serialize)]
pub struct DrainFailure {
    pub marker: PathBuf,
    pub error: String,
}

/// Cheap status snapshot returned by [`queue_stats`].
#[derive(Debug, Clone, Serialize)]
pub struct QueueStats {
    pub pending: usize,
    pub oldest_age_secs: Option<u64>,
    pub newest_age_secs: Option<u64>,
    pub drain_lock_held: bool,
    pub drain_lock_pid: Option<u32>,
}

/// Append a marker for `source`. No LLM, no lock. O(filesystem).
///
/// `source` must exist and be readable; we only stat it, not read it.
/// Marker name = `<unix_secs>-<8 lowercase hex chars>.json`. The 8-char
/// suffix is `rand::random::<u32>()` rendered hex; collision probability
/// at 1k/sec enqueue is ~1e-5 per second, treated as fatal-rare (we
/// retry once with a fresh suffix on EEXIST, then bail).
///
/// Atomic publish: write to `<name>.tmp`, fsync file, rename to final.
pub fn enqueue(vault: &Vault, source: &Path, session_id: Option<&str>) -> Result<QueueMarker> {
    let abs_source = source.canonicalize().map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            Error::Other(anyhow::anyhow!("source not found: {}", source.display()))
        } else {
            Error::io(source, e)
        }
    })?;

    let queue_dir = queue_dir_path(vault);
    fs::create_dir_all(&queue_dir).map_err(|e| Error::io(&queue_dir, e))?;

    let marker = QueueMarker {
        version: MARKER_SCHEMA_VERSION,
        source: abs_source,
        session_id: session_id.map(str::to_owned),
        enqueued_at: Utc::now(),
    };
    let payload = serde_json::to_vec_pretty(&marker)
        .map_err(|e| Error::Other(anyhow::anyhow!("serialize marker: {e}")))?;

    let unix_secs: u64 = marker.enqueued_at.timestamp().try_into().unwrap_or(0);

    for attempt in 0..2 {
        let suffix = format!("{:08x}", rand::random::<u32>());
        let final_name = format!("{unix_secs}-{suffix}.json");
        let final_path = queue_dir.join(&final_name);
        if final_path.exists() {
            if attempt == 1 {
                return Err(Error::Other(anyhow::anyhow!(
                    "queue marker filename collision after retry: {final_name}"
                )));
            }
            continue;
        }
        let tmp_path = queue_dir.join(format!("{final_name}.tmp"));
        let mut f = fs::File::create(&tmp_path).map_err(|e| Error::io(&tmp_path, e))?;
        f.write_all(&payload).map_err(|e| Error::io(&tmp_path, e))?;
        f.sync_all().map_err(|e| Error::io(&tmp_path, e))?;
        drop(f);
        fs::rename(&tmp_path, &final_path).map_err(|e| Error::io(&final_path, e))?;
        return Ok(marker);
    }
    unreachable!("loop exits via return")
}

fn queue_dir_path(vault: &Vault) -> PathBuf {
    vault.meta_dir().join(QUEUE_SUBDIR).into_std_path_buf()
}

/// Drain the queue. Acquires `<vault>/.scriptorium/drain.lock` via
/// `try_lock_write`; if another drainer holds it, returns Ok with
/// `DrainReport { considered: 0, .. }` and logs at info.
///
/// Markers younger than `cfg.debounce` are skipped (left on disk). For
/// each survivor: (1) read marker, (2) read source file, (3) canonicalize
/// and hash, (4) check `source-hashes.txt` — if hit, delete marker,
/// increment `skipped_dup`, continue; (5) call `ingest_with_retrieval`,
/// (6) on success append hash, delete marker, (7) on failure leave
/// marker in place (next drain retries) and record a [`DrainFailure`].
///
/// Errors short-circuit only on lock-acquisition or fatal IO; per-marker
/// errors accumulate in `DrainFailure`.
#[allow(clippy::too_many_lines)]
pub async fn drain(
    vault: &Vault,
    provider: &dyn LlmProvider,
    cfg: DrainConfig,
) -> Result<DrainReport> {
    let started_at = Utc::now();
    let meta = vault.meta_dir().into_std_path_buf();
    fs::create_dir_all(&meta).map_err(|e| Error::io(&meta, e))?;

    let lock_path = meta.join(DRAIN_LOCK_FILE);
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| Error::io(&lock_path, e))?;
    let mut rwlock = FdRwLock::new(lock_file);
    let Ok(_guard) = rwlock.try_write() else {
        return Ok(DrainReport {
            considered: 0,
            skipped_young: 0,
            skipped_dup: 0,
            skipped_missing: 0,
            skipped_unknown_version: 0,
            ingested: 0,
            redundant_skips: 0,
            failures: Vec::new(),
            started_at,
            finished_at: Utc::now(),
        });
    };

    let _ = write_drain_pidfile(&meta);

    let mut report = DrainReport {
        considered: 0,
        skipped_young: 0,
        skipped_dup: 0,
        skipped_missing: 0,
        skipped_unknown_version: 0,
        ingested: 0,
        redundant_skips: 0,
        failures: Vec::new(),
        started_at,
        finished_at: started_at,
    };

    let marker_paths = list_marker_paths(vault)?;
    let mut sorted = marker_paths;
    sorted.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    if let Some(cap) = cfg.max_per_run {
        sorted.truncate(cap);
    }
    report.considered = sorted.len();

    for marker_path in sorted {
        let bytes = match fs::read(&marker_path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => {
                report.failures.push(DrainFailure {
                    marker: marker_path.clone(),
                    error: format!("read marker: {e}"),
                });
                continue;
            }
        };

        let marker: QueueMarker = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                report.failures.push(DrainFailure {
                    marker: marker_path.clone(),
                    error: format!("parse marker: {e}"),
                });
                continue;
            }
        };

        if marker.version != MARKER_SCHEMA_VERSION {
            report.skipped_unknown_version += 1;
            report.failures.push(DrainFailure {
                marker: marker_path.clone(),
                error: format!("unknown marker version {}", marker.version),
            });
            continue;
        }

        let age = Utc::now()
            .signed_duration_since(marker.enqueued_at)
            .to_std()
            .unwrap_or_default();
        if age < cfg.debounce {
            report.skipped_young += 1;
            continue;
        }

        if !marker.source.exists() {
            report.skipped_missing += 1;
            let _ = fs::remove_file(&marker_path);
            continue;
        }

        let raw = match fs::read(&marker.source) {
            Ok(b) => b,
            Err(e) => {
                report.failures.push(DrainFailure {
                    marker: marker_path.clone(),
                    error: format!("read source: {e}"),
                });
                continue;
            }
        };
        let text = match std::str::from_utf8(&raw) {
            Ok(s) => s.to_owned(),
            Err(e) => {
                report.failures.push(DrainFailure {
                    marker: marker_path.clone(),
                    error: format!("source not UTF-8: {e}"),
                });
                continue;
            }
        };

        let hash = canonical_hash(&text);
        match hash_store_contains(vault, &hash) {
            Ok(true) => {
                report.skipped_dup += 1;
                let _ = fs::remove_file(&marker_path);
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                report.failures.push(DrainFailure {
                    marker: marker_path.clone(),
                    error: format!("hash-store read: {e}"),
                });
                continue;
            }
        }

        if cfg.dry_run {
            let _ = fs::remove_file(&marker_path);
            continue;
        }

        match crate::ingest::ingest_with_retrieval(
            vault,
            provider,
            &marker.source,
            crate::ingest::IngestOptions::default(),
            None,
            None,
            None,
        )
        .await
        {
            Ok(ingest_report) => {
                if let Err(e) = hash_store_append(vault, &hash) {
                    report.failures.push(DrainFailure {
                        marker: marker_path.clone(),
                        error: format!("hash-store append: {e}"),
                    });
                    continue;
                }
                let _ = fs::remove_file(&marker_path);
                report.ingested += 1;
                if ingest_report.redundant {
                    report.redundant_skips += 1;
                }
            }
            Err(e) => {
                report.failures.push(DrainFailure {
                    marker: marker_path.clone(),
                    error: format!("ingest: {e}"),
                });
            }
        }
    }

    report.finished_at = Utc::now();
    let _ = record_drain_stats(vault, &report);
    Ok(report)
}

/// Read all marker files (regardless of age). Sorted oldest first.
/// Used by `ingest-queue --list` and `--stats`.
pub fn list_queued(vault: &Vault) -> Result<Vec<QueueMarker>> {
    let mut entries = list_marker_paths(vault)?;
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let mut markers = Vec::with_capacity(entries.len());
    for path in entries {
        match fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<QueueMarker>(&bytes) {
                Ok(m) => markers.push(m),
                Err(e) => eprintln!(
                    "scriptorium: skipping malformed queue marker {}: {e}",
                    path.display()
                ),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::io(&path, e)),
        }
    }
    Ok(markers)
}

fn list_marker_paths(vault: &Vault) -> Result<Vec<PathBuf>> {
    let dir = queue_dir_path(vault);
    let read = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::io(&dir, e)),
    };
    let mut out = Vec::new();
    for entry in read {
        let entry = entry.map_err(|e| Error::io(&dir, e))?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            out.push(path);
        }
    }
    Ok(out)
}

/// Delete every marker file. Does NOT touch `source-hashes.txt` or
/// `drain.lock`. Used by `ingest-queue --clear`. Ignores missing files
/// but propagates other IO errors. Returns count deleted.
pub fn clear_queue(vault: &Vault) -> Result<usize> {
    let dir = queue_dir_path(vault);
    let read = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(Error::io(&dir, e)),
    };
    let mut count = 0usize;
    for entry in read {
        let entry = entry.map_err(|e| Error::io(&dir, e))?;
        let path = entry.path();
        if !entry
            .file_type()
            .map_err(|e| Error::io(&path, e))?
            .is_file()
        {
            continue;
        }
        match fs::remove_file(&path) {
            Ok(()) => count += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(Error::io(&path, e)),
        }
    }
    Ok(count)
}

/// Cheap stats: count + oldest/newest age + `drain.lock` state.
pub fn queue_stats(vault: &Vault) -> Result<QueueStats> {
    let paths = list_marker_paths(vault)?;
    let pending = paths.len();

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);

    let mut min_ts: Option<u64> = None;
    let mut max_ts: Option<u64> = None;
    for p in &paths {
        if let Some(ts) = parse_marker_timestamp(p) {
            min_ts = Some(min_ts.map_or(ts, |m| m.min(ts)));
            max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
        }
    }

    let oldest_age_secs = min_ts.map(|t| now_secs.saturating_sub(t));
    let newest_age_secs = max_ts.map(|t| now_secs.saturating_sub(t));

    let meta = vault.meta_dir();
    let lock_path = meta.join(DRAIN_LOCK_FILE);
    let pid_path = meta.join(DRAIN_PID_FILE);
    let drain_lock_held = lock_path.exists();
    let drain_lock_pid = match fs::read_to_string(pid_path.as_std_path()) {
        Ok(s) => s.trim().parse::<u32>().ok(),
        Err(_) => None,
    };

    Ok(QueueStats {
        pending,
        oldest_age_secs,
        newest_age_secs,
        drain_lock_held,
        drain_lock_pid,
    })
}

fn parse_marker_timestamp(path: &Path) -> Option<u64> {
    let stem = path.file_stem()?.to_str()?;
    let (secs, _) = stem.split_once('-')?;
    secs.parse::<u64>().ok()
}

/// Doctor check: WARN if pending > 1000 or lock-pid stale across reboot,
/// FAIL if any marker older than `stuck_threshold` (default 3600s) or
/// `drain.lock` pid is alive but no progress in dedup-stats. Returns a
/// [`DoctorCheck`] named `queue_health`.
pub fn queue_health_check(vault: &Vault, stuck_threshold: Duration) -> DoctorCheck {
    use crate::doctor::CheckStatus;

    let mut messages: Vec<String> = Vec::new();
    let mut status = CheckStatus::Ok;

    let paths = match list_marker_paths(vault) {
        Ok(p) => p,
        Err(e) => {
            return DoctorCheck {
                name: "queue_health".into(),
                status: CheckStatus::Warn,
                message: format!("could not list ingest-queue: {e}"),
            };
        }
    };
    let pending = paths.len();

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let threshold_secs = stuck_threshold.as_secs();

    if pending > 1000 {
        status = CheckStatus::Warn;
        messages.push(format!("queue has {pending} pending markers (>1000)"));
    }

    for path in &paths {
        if let Some(ts) = parse_marker_timestamp(path) {
            let age = now_secs.saturating_sub(ts);
            if age > threshold_secs {
                status = CheckStatus::Fail;
                let name = path
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or("<unknown>");
                messages.push(format!("marker {name} is {age}s old (> {threshold_secs}s)"));
            }
        }
    }

    let meta = vault.meta_dir().into_std_path_buf();
    let lock_held = meta.join(DRAIN_LOCK_FILE).exists();
    let pid = read_drain_pidfile(&meta).ok().flatten();
    if lock_held {
        match pid {
            Some(p) if !pid_alive(p) => {
                status = CheckStatus::Fail;
                messages.push(format!("drain.lock present but pid {p} is not alive"));
            }
            Some(p) => {
                if let Some(last_run) = last_drain_stats_ts(vault) {
                    let age: u64 = (Utc::now() - last_run)
                        .num_seconds()
                        .try_into()
                        .unwrap_or(0);
                    if age > threshold_secs {
                        status = CheckStatus::Fail;
                        messages.push(format!(
                            "drain pid {p} alive but last drain-stats entry is {age}s old"
                        ));
                    }
                } else if status == CheckStatus::Ok {
                    status = CheckStatus::Warn;
                    messages.push(format!("drain.lock held by pid {p}, no drain-stats yet"));
                }
            }
            None => {
                if status == CheckStatus::Ok {
                    status = CheckStatus::Warn;
                    messages.push("drain.lock present without drain.pid".into());
                }
            }
        }
    }

    let message = if messages.is_empty() {
        format!("{pending} pending markers, drain idle")
    } else {
        messages.join("; ")
    };
    DoctorCheck {
        name: "queue_health".into(),
        status,
        message,
    }
}

fn last_drain_stats_ts(vault: &Vault) -> Option<DateTime<Utc>> {
    let path = vault.meta_dir().join(DRAIN_STATS_FILE).into_std_path_buf();
    let bytes = fs::read(&path).ok()?;
    let stats: DrainStatsFile = serde_json::from_slice(&bytes).ok()?;
    stats.runs.last().map(|r| r.ts)
}

fn canonical_hash(text: &str) -> String {
    let mut buf = String::with_capacity(text.len());
    let mut first = true;
    for line in text.split('\n') {
        let trimmed_start = line.trim_start();
        if CANONICAL_STRIP_PREFIXES
            .iter()
            .any(|p| trimmed_start.starts_with(p))
        {
            continue;
        }
        if !first {
            buf.push('\n');
        }
        first = false;
        buf.push_str(line.trim_end());
    }
    let mut hasher = Sha256::new();
    hasher.update(buf.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn hash_store_path(vault: &Vault) -> PathBuf {
    vault.meta_dir().join(HASH_STORE_FILE).into_std_path_buf()
}

fn hash_store_contains(vault: &Vault, h: &str) -> Result<bool> {
    let path = hash_store_path(vault);
    let file = match fs::File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(Error::io(&path, e)),
    };
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|e| Error::io(&path, e))?;
        if line.trim() == h {
            return Ok(true);
        }
    }
    Ok(false)
}

fn hash_store_append(vault: &Vault, h: &str) -> Result<()> {
    let path = hash_store_path(vault);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| Error::io(parent, e))?;
    }
    let mut f = OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .map_err(|e| Error::io(&path, e))?;
    f.write_all(format!("{h}\n").as_bytes())
        .map_err(|e| Error::io(&path, e))?;
    f.sync_all().map_err(|e| Error::io(&path, e))?;
    Ok(())
}

#[allow(dead_code)]
fn try_drain_lock<T, F: FnOnce() -> Result<T>>(meta: &Path, f: F) -> Result<Option<T>> {
    fs::create_dir_all(meta).map_err(|e| Error::io(meta, e))?;
    let lock_path = meta.join(DRAIN_LOCK_FILE);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| Error::io(&lock_path, e))?;
    let mut rwlock = FdRwLock::new(file);
    let outcome = match rwlock.try_write() {
        Ok(_guard) => {
            let _ = write_drain_pidfile(meta);
            let result = f()?;
            Ok(Some(result))
        }
        Err(_) => Ok(None),
    };
    outcome
}

fn write_drain_pidfile(meta: &Path) -> Result<()> {
    let path = meta.join(DRAIN_PID_FILE);
    let tmp = meta.join(format!("{DRAIN_PID_FILE}.tmp"));
    let content = format!("{}\n", std::process::id());
    fs::write(&tmp, content.as_bytes()).map_err(|e| Error::io(&tmp, e))?;
    fs::rename(&tmp, &path).map_err(|e| Error::io(&path, e))?;
    Ok(())
}

fn read_drain_pidfile(meta: &Path) -> Result<Option<u32>> {
    let path = meta.join(DRAIN_PID_FILE);
    match fs::read_to_string(&path) {
        Ok(s) => Ok(s.trim().parse::<u32>().ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(Error::io(&path, e)),
    }
}

fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DrainStatsFile {
    runs: Vec<DrainStatsEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DrainStatsEntry {
    ts: DateTime<Utc>,
    considered: usize,
    ingested: usize,
    dup: usize,
    redundant: usize,
}

fn record_drain_stats(vault: &Vault, report: &DrainReport) -> Result<()> {
    let path = vault.meta_dir().join(DRAIN_STATS_FILE).into_std_path_buf();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| Error::io(parent, e))?;
    }
    let mut stats: DrainStatsFile = match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_default(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DrainStatsFile::default(),
        Err(e) => return Err(Error::io(&path, e)),
    };
    stats.runs.push(DrainStatsEntry {
        ts: report.finished_at,
        considered: report.considered,
        ingested: report.ingested,
        dup: report.skipped_dup,
        redundant: report.redundant_skips,
    });
    let drop = stats.runs.len().saturating_sub(DRAIN_STATS_MAX_RUNS);
    if drop > 0 {
        stats.runs.drain(..drop);
    }
    let payload = serde_json::to_vec_pretty(&stats)
        .map_err(|e| Error::Other(anyhow::anyhow!("serialize drain stats: {e}")))?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, &payload).map_err(|e| Error::io(&tmp, e))?;
    fs::rename(&tmp, &path).map_err(|e| Error::io(&path, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_test_vault() -> (TempDir, Vault) {
        let dir = tempfile::tempdir().expect("tempdir");
        let vault = Vault::open(dir.path()).expect("open vault");
        (dir, vault)
    }

    fn write_source(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn marker_roundtrip() {
        let original = QueueMarker {
            version: 1,
            source: PathBuf::from("/tmp/example/source.md"),
            session_id: Some("abc-123".to_string()),
            enqueued_at: Utc::now(),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let parsed: QueueMarker = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed, original);
    }

    #[test]
    fn enqueue_writes_marker() {
        let (dir, vault) = make_test_vault();
        let src = write_source(dir.path(), "src.md", "hello");

        let marker = enqueue(&vault, &src, Some("sess-1")).expect("enqueue");
        assert_eq!(marker.version, 1);
        assert_eq!(marker.session_id.as_deref(), Some("sess-1"));
        assert!(marker.source.is_absolute());
        assert_eq!(marker.source, src.canonicalize().unwrap());

        let listed = list_queued(&vault).expect("list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0], marker);
    }

    #[test]
    fn enqueue_creates_meta_subdirs() {
        let (dir, vault) = make_test_vault();
        let queue = vault.meta_dir().join(QUEUE_SUBDIR).into_std_path_buf();
        assert!(!queue.exists());
        let src = write_source(dir.path(), "src.md", "x");
        enqueue(&vault, &src, None).expect("enqueue");
        assert!(queue.is_dir());
    }

    #[test]
    fn enqueue_missing_source() {
        let (dir, vault) = make_test_vault();
        let missing = dir.path().join("nope.md");
        let err = enqueue(&vault, &missing, None).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("source not found"), "got: {msg}");
    }

    #[test]
    fn enqueue_unique_filenames_at_same_second() {
        let (dir, vault) = make_test_vault();
        let src = write_source(dir.path(), "src.md", "x");
        for _ in 0..50 {
            enqueue(&vault, &src, None).expect("enqueue");
        }
        let listed = list_queued(&vault).expect("list");
        assert_eq!(listed.len(), 50);
    }

    #[test]
    fn enqueue_does_not_publish_tmp() {
        let (dir, vault) = make_test_vault();
        let src = write_source(dir.path(), "src.md", "x");
        enqueue(&vault, &src, None).expect("enqueue");
        let queue = vault.meta_dir().join(QUEUE_SUBDIR).into_std_path_buf();
        let names: Vec<String> = fs::read_dir(&queue)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 1);
        let p = Path::new(&names[0]);
        assert_eq!(p.extension().and_then(|s| s.to_str()), Some("json"));
    }

    #[test]
    fn list_queued_sorts_oldest_first() {
        let (_dir, vault) = make_test_vault();
        let queue = vault.meta_dir().join(QUEUE_SUBDIR).into_std_path_buf();
        fs::create_dir_all(&queue).unwrap();
        let body = serde_json::to_vec(&QueueMarker {
            version: 1,
            source: PathBuf::from("/tmp/x"),
            session_id: None,
            enqueued_at: Utc::now(),
        })
        .unwrap();
        for name in [
            "200-bbbbbbbb.json",
            "100-aaaaaaaa.json",
            "150-cccccccc.json",
        ] {
            fs::write(queue.join(name), &body).unwrap();
        }
        let listed = list_queued(&vault).expect("list");
        assert_eq!(listed.len(), 3);
    }

    #[test]
    fn clear_queue_removes_all_then_returns_count() {
        let (dir, vault) = make_test_vault();
        let src = write_source(dir.path(), "src.md", "x");
        for _ in 0..3 {
            enqueue(&vault, &src, None).unwrap();
        }
        let removed = clear_queue(&vault).expect("clear");
        assert_eq!(removed, 3);
        assert!(list_queued(&vault).unwrap().is_empty());
    }

    #[test]
    fn canonical_hash_strips_session_id() {
        let a = "Session ID: foo\nbody line\n";
        let b = "Session ID: bar\nbody line\n";
        assert_eq!(canonical_hash(a), canonical_hash(b));
    }

    #[test]
    fn canonical_hash_preserves_blank_lines() {
        assert_ne!(canonical_hash("a\n\nb"), canonical_hash("a\nb"));
    }

    fn write_marker_file(vault: &Vault, name: &str, marker: &QueueMarker) {
        let dir = vault.meta_dir().join(QUEUE_SUBDIR).into_std_path_buf();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(name), serde_json::to_vec(marker).unwrap()).unwrap();
    }

    fn dummy_provider() -> crate::llm::MockProvider {
        crate::llm::MockProvider::constant("{}")
    }

    #[tokio::test]
    async fn drain_no_lock_contention_empty_queue() {
        let (_dir, vault) = make_test_vault();
        let cfg = DrainConfig {
            debounce: Duration::from_secs(0),
            ..Default::default()
        };
        let report = drain(&vault, &dummy_provider(), cfg).await.expect("drain");
        assert_eq!(report.considered, 0);
        assert_eq!(report.ingested, 0);
        assert!(report.failures.is_empty());
    }

    #[tokio::test]
    async fn drain_skips_young_markers() {
        let (dir, vault) = make_test_vault();
        let src = write_source(dir.path(), "src.md", "hello");
        enqueue(&vault, &src, None).unwrap();
        let cfg = DrainConfig {
            debounce: Duration::from_secs(120),
            ..Default::default()
        };
        let report = drain(&vault, &dummy_provider(), cfg).await.unwrap();
        assert_eq!(report.considered, 1);
        assert_eq!(report.skipped_young, 1);
        assert_eq!(report.ingested, 0);
        assert_eq!(list_queued(&vault).unwrap().len(), 1);
    }

    fn make_ingest_vault() -> (TempDir, Vault) {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::create_dir_all(dir.path().join("wiki/concepts")).unwrap();
        fs::create_dir_all(dir.path().join("sources/articles")).unwrap();
        fs::write(
            dir.path().join("CLAUDE.md"),
            "# Vault Rules\n\nBe concise.\n",
        )
        .unwrap();
        let vault = Vault::open(dir.path()).expect("open");
        (dir, vault)
    }

    fn ingest_plan_mock() -> crate::llm::MockProvider {
        use crate::llm::prompts::{IngestAction, IngestPageAction, IngestPlan};
        let plan = IngestPlan {
            summary: "drain dedup test".into(),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/concepts/dedup-target.md".into(),
                title: "Dedup Target".into(),
                tags: vec![],
                body: "Body without wikilinks.\n".into(),
            }],
            log_entry: "drain ingest".into(),
            redundant: false,
        };
        crate::llm::MockProvider::constant(serde_json::to_string(&plan).unwrap())
    }

    #[tokio::test]
    async fn drain_dedups_by_canonical_hash() {
        let (dir, vault) = make_ingest_vault();
        let s1 = write_source(
            dir.path().join("sources/articles").as_path(),
            "s1.md",
            "Session ID: A\nbody\n",
        );
        let s2 = write_source(
            dir.path().join("sources/articles").as_path(),
            "s2.md",
            "Session ID: B\nbody\n",
        );
        let s3 = write_source(
            dir.path().join("sources/articles").as_path(),
            "s3.md",
            "Peak Turn Score: 99\nbody\n",
        );

        for s in [&s1, &s2, &s3] {
            enqueue(&vault, s, None).unwrap();
        }
        let cfg = DrainConfig {
            debounce: Duration::from_secs(0),
            ..Default::default()
        };
        let report = drain(&vault, &ingest_plan_mock(), cfg).await.unwrap();

        assert_eq!(report.considered, 3);
        assert_eq!(report.ingested, 1);
        assert_eq!(report.skipped_dup, 2);
        assert_eq!(report.redundant_skips, 0);
        assert!(list_queued(&vault).unwrap().is_empty());

        let h = canonical_hash("body\n");
        let lines: Vec<String> = fs::read_to_string(hash_store_path(&vault))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(lines, vec![h]);

        let repo = git2::Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        assert!(head.message().unwrap().contains("drain dedup test"));
    }

    #[tokio::test]
    async fn drain_skips_unknown_version() {
        let (_dir, vault) = make_test_vault();
        let bad = QueueMarker {
            version: 999,
            source: PathBuf::from("/tmp/does-not-matter"),
            session_id: None,
            enqueued_at: Utc::now() - chrono::Duration::seconds(10),
        };
        write_marker_file(&vault, "100-deadbeef.json", &bad);
        let cfg = DrainConfig {
            debounce: Duration::from_secs(0),
            ..Default::default()
        };
        let report = drain(&vault, &dummy_provider(), cfg).await.unwrap();
        assert_eq!(report.skipped_unknown_version, 1);
        assert_eq!(report.failures.len(), 1);
        assert!(vault
            .meta_dir()
            .join(QUEUE_SUBDIR)
            .join("100-deadbeef.json")
            .exists());
    }

    #[tokio::test]
    async fn drain_skips_missing_source() {
        let (dir, vault) = make_test_vault();
        let src = write_source(dir.path(), "src.md", "x");
        enqueue(&vault, &src, None).unwrap();
        fs::remove_file(&src).unwrap();
        let cfg = DrainConfig {
            debounce: Duration::from_secs(0),
            ..Default::default()
        };
        let report = drain(&vault, &dummy_provider(), cfg).await.unwrap();
        assert_eq!(report.considered, 1);
        assert_eq!(report.skipped_missing, 1);
        assert!(report.failures.is_empty());
        assert!(list_queued(&vault).unwrap().is_empty());
    }

    #[tokio::test]
    async fn drain_lock_contention_returns_empty() {
        let (_dir, vault) = make_test_vault();
        let meta = vault.meta_dir().into_std_path_buf();
        fs::create_dir_all(&meta).unwrap();
        let lock_path = meta.join(DRAIN_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        let mut held = FdRwLock::new(file);
        let _guard = held.try_write().unwrap();

        let cfg = DrainConfig {
            debounce: Duration::from_secs(0),
            ..Default::default()
        };
        let report = drain(&vault, &dummy_provider(), cfg).await.unwrap();
        assert_eq!(report.considered, 0);
        assert!(report.failures.is_empty());
    }

    #[tokio::test]
    async fn drain_per_marker_failure_isolation() {
        let (dir, vault) = make_test_vault();
        let good_a = write_source(dir.path(), "a.md", "alpha\n");
        let good_b = write_source(dir.path(), "b.md", "beta\n");
        let bad = dir.path().join("bad.bin");
        fs::write(&bad, [0xFF, 0xFE, 0xFD]).unwrap();

        hash_store_append(&vault, &canonical_hash("alpha\n")).unwrap();
        hash_store_append(&vault, &canonical_hash("beta\n")).unwrap();

        for s in [&good_a, &good_b, &bad] {
            enqueue(&vault, s, None).unwrap();
        }
        let cfg = DrainConfig {
            debounce: Duration::from_secs(0),
            ..Default::default()
        };
        let report = drain(&vault, &dummy_provider(), cfg).await.unwrap();
        assert_eq!(report.considered, 3);
        assert_eq!(report.skipped_dup, 2);
        assert_eq!(report.failures.len(), 1);
        assert!(report.failures[0].error.contains("UTF-8"));
        let remaining = list_queued(&vault).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].source, bad.canonicalize().unwrap());
    }

    #[test]
    fn pid_alive_returns_false_for_unlikely_pid() {
        assert!(!pid_alive(u32::MAX));
        assert!(!pid_alive(0));
    }

    #[test]
    fn queue_health_ok_when_empty() {
        let (_dir, vault) = make_test_vault();
        let check = queue_health_check(&vault, Duration::from_secs(3600));
        assert_eq!(check.name, "queue_health");
        assert_eq!(check.status, crate::doctor::CheckStatus::Ok);
    }

    #[test]
    fn queue_health_warn_when_count_over_1000() {
        let (_dir, vault) = make_test_vault();
        let dir = vault.meta_dir().join(QUEUE_SUBDIR).into_std_path_buf();
        fs::create_dir_all(&dir).unwrap();
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let body = serde_json::to_vec(&QueueMarker {
            version: 1,
            source: PathBuf::from("/tmp/x"),
            session_id: None,
            enqueued_at: Utc::now(),
        })
        .unwrap();
        for i in 0..1001 {
            let name = format!("{now_secs}-{i:08x}.json");
            fs::write(dir.join(name), &body).unwrap();
        }
        let check = queue_health_check(&vault, Duration::from_secs(3600));
        assert_eq!(check.status, crate::doctor::CheckStatus::Warn);
        assert!(check.message.contains("1001"));
    }

    #[test]
    fn queue_health_fail_on_old_marker() {
        let (_dir, vault) = make_test_vault();
        let dir = vault.meta_dir().join(QUEUE_SUBDIR).into_std_path_buf();
        fs::create_dir_all(&dir).unwrap();
        let body = serde_json::to_vec(&QueueMarker {
            version: 1,
            source: PathBuf::from("/tmp/x"),
            session_id: None,
            enqueued_at: Utc::now(),
        })
        .unwrap();
        fs::write(dir.join("1000-deadbeef.json"), &body).unwrap();
        let check = queue_health_check(&vault, Duration::from_secs(60));
        assert_eq!(check.status, crate::doctor::CheckStatus::Fail);
        assert!(check.message.contains("1000-deadbeef.json"));
    }

    #[test]
    fn queue_health_fail_on_dead_pidfile() {
        let (_dir, vault) = make_test_vault();
        let meta = vault.meta_dir().into_std_path_buf();
        fs::create_dir_all(&meta).unwrap();
        fs::write(meta.join(DRAIN_LOCK_FILE), b"").unwrap();
        fs::write(meta.join(DRAIN_PID_FILE), format!("{}\n", u32::MAX)).unwrap();
        let check = queue_health_check(&vault, Duration::from_secs(3600));
        assert_eq!(check.status, crate::doctor::CheckStatus::Fail);
        assert!(check.message.contains("not alive"));
    }
}
