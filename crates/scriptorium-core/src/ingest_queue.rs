//! Decoupled enqueue + drain pipeline.
//!
//! Automation sources (session-end hook, watchers) call [`enqueue`] for an
//! O(filesystem) marker write with no LLM call. A separate [`drain`] (driven
//! by launchd) takes a non-blocking lock, dedups markers by canonical content
//! hash, and ingests only the survivors.
//!
//! See `.sisyphus/plans/ingest-queue-drain.md` for the full design.

use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::doctor::DoctorCheck;
use crate::error::Result;
use crate::llm::LlmProvider;
use crate::vault::Vault;

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
pub fn enqueue(_vault: &Vault, _source: &Path, _session_id: Option<&str>) -> Result<QueueMarker> {
    unimplemented!("commit 2")
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
pub fn list_queued(_vault: &Vault) -> Result<Vec<QueueMarker>> {
    unimplemented!("commit 2")
}

/// Delete every marker file. Does NOT touch `source-hashes.txt` or
/// `drain.lock`. Used by `ingest-queue --clear`. Ignores missing files
/// but propagates other IO errors. Returns count deleted.
pub fn clear_queue(_vault: &Vault) -> Result<usize> {
    unimplemented!("commit 2")
}

/// Cheap stats: count + oldest/newest age + `drain.lock` state.
pub fn queue_stats(_vault: &Vault) -> Result<QueueStats> {
    unimplemented!("commit 2")
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
}
