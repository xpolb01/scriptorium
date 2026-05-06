//! Decoupled enqueue + drain pipeline.
//!
//! Automation sources (session-end hook, watchers) call [`enqueue`] for an
//! O(filesystem) marker write with no LLM call. A separate [`drain`] (driven
//! by launchd) takes a non-blocking lock, dedups markers by canonical content
//! hash, and ingests only the survivors.
//!
//! See `.sisyphus/plans/ingest-queue-drain.md` for the full design.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::doctor::DoctorCheck;
use crate::error::{Error, Result};
use crate::llm::LlmProvider;
use crate::vault::Vault;

const QUEUE_SUBDIR: &str = "ingest-queue";
const DRAIN_LOCK_FILE: &str = "drain.lock";
const DRAIN_PID_FILE: &str = "drain.pid";
const MARKER_SCHEMA_VERSION: u32 = 1;

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
#[allow(clippy::unused_async)]
pub async fn drain(
    _vault: &Vault,
    _provider: &dyn LlmProvider,
    _cfg: DrainConfig,
) -> Result<DrainReport> {
    unimplemented!("commit 3")
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
pub fn queue_health_check(_vault: &Vault, _stuck_threshold: Duration) -> DoctorCheck {
    unimplemented!("commit 5")
}

#[allow(dead_code)]
fn canonical_hash(_text: &str) -> String {
    unimplemented!("commit 3")
}

#[allow(dead_code)]
fn hash_store_contains(_vault: &Vault, _h: &str) -> Result<bool> {
    unimplemented!("commit 3")
}

#[allow(dead_code)]
fn hash_store_append(_vault: &Vault, _h: &str) -> Result<()> {
    unimplemented!("commit 3")
}

#[allow(dead_code)]
fn try_drain_lock<T, F: FnOnce() -> Result<T>>(_meta: &Path, _f: F) -> Result<Option<T>> {
    unimplemented!("commit 3")
}

#[allow(dead_code)]
fn write_drain_pidfile(_meta: &Path) -> Result<()> {
    unimplemented!("commit 3")
}

#[allow(dead_code)]
fn read_drain_pidfile(_meta: &Path) -> Result<Option<u32>> {
    unimplemented!("commit 3")
}

#[allow(dead_code)]
fn pid_alive(_pid: u32) -> bool {
    unimplemented!("commit 3")
}

#[allow(dead_code)]
fn record_drain_stats(_vault: &Vault, _report: &DrainReport) -> Result<()> {
    unimplemented!("commit 3")
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
}
