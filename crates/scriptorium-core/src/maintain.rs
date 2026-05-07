//! Dream cycle / autonomous maintenance command.
//!
//! Runs all mechanical maintenance tasks in a single pass and returns a
//! structured report. Optionally auto-fixes safe issues (re-embed stale
//! pages, fix bad timestamps).
//!
//! Designed to be triggered by cron, Claude Code hooks, or the MCP
//! `scriptorium_maintain` tool. No LLM required for reporting; the embed
//! provider is only needed when `fix = true` and there are stale embeddings.
//!
//! Reference: `GBrain`'s `skills/maintain/SKILL.md`.

use serde::{Deserialize, Serialize};

use crate::embed::{self, EmbeddingsStore};
use crate::error::Result;
use crate::lint::{self, stale, LintReport, Severity};
use crate::llm::LlmProvider;
use crate::telemetry::TelemetryStore;
use crate::vault::Vault;

/// Options for [`maintain`].
#[derive(Debug, Clone, Default)]
pub struct MaintainOptions {
    /// Auto-fix safe issues: re-embed stale pages, fix bad timestamps.
    pub fix: bool,
}

/// Report from a [`maintain`] run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintainReport {
    /// Full lint report (broken links, orphans, frontmatter, stale pages).
    pub lint: LintReport,
    /// Pages with stale embeddings (content changed since last embed).
    pub stale_embedding_pages: Vec<String>,
    /// Embedding coverage: `(embedded_count, total_count)`.
    pub embedding_coverage: (usize, usize),
    /// Number of issues auto-fixed (0 if `fix = false`).
    pub auto_fixed: usize,
    /// Number of chunks re-embedded (0 if `fix = false` or no stale).
    pub chunks_reembedded: usize,
}

impl MaintainReport {
    /// Summary counts for display.
    pub fn summary(&self) -> MaintainSummary {
        MaintainSummary {
            errors: self.lint.count_by_severity(Severity::Error),
            warnings: self.lint.count_by_severity(Severity::Warning),
            stale_pages: self
                .lint
                .issues
                .iter()
                .filter(|i| i.rule == stale::STALE_PAGE)
                .count(),
            stale_embeddings: self.stale_embedding_pages.len(),
            embedded: self.embedding_coverage.0,
            total_pages: self.embedding_coverage.1,
            auto_fixed: self.auto_fixed,
            chunks_reembedded: self.chunks_reembedded,
        }
    }
}

/// Condensed summary for CLI output.
#[derive(Debug)]
pub struct MaintainSummary {
    pub errors: usize,
    pub warnings: usize,
    pub stale_pages: usize,
    pub stale_embeddings: usize,
    pub embedded: usize,
    pub total_pages: usize,
    pub auto_fixed: usize,
    pub chunks_reembedded: usize,
}

/// Run all maintenance tasks.
///
/// `embed_provider` is only needed when `options.fix` is true and there are
/// stale embeddings to refresh. Pass `None` for report-only mode.
pub async fn maintain(
    vault: &Vault,
    store: &EmbeddingsStore,
    embed_provider: Option<&dyn LlmProvider>,
    embed_model: &str,
    options: &MaintainOptions,
) -> Result<MaintainReport> {
    // 1. Run full lint (includes stale page detection from Phase 5).
    let lint_report = lint::run(vault)?;

    // 2. Detect stale embeddings.
    let scan = vault.scan()?;
    let total_pages = scan.pages.len();
    let embedded = store.distinct_page_count().unwrap_or(0);

    let mut stale_embedding_pages = Vec::new();
    for page in &scan.pages {
        let Ok(hash) = page.content_hash() else {
            continue;
        };
        // A page is "stale-embedded" if no provider/model combo has the
        // current content hash. We check all providers by looking for the
        // hash in ANY row for this page.
        let has_current: bool = store
            .has_any_version(page.frontmatter.id, &hash)
            .unwrap_or(false);
        if !has_current {
            stale_embedding_pages.push(page.path.to_string());
        }
    }

    // 3. Auto-fix if requested.
    let mut auto_fixed = 0usize;
    let mut chunks_reembedded = 0usize;

    if options.fix {
        // Fix bad timestamps via lint::fix.
        if let Ok(fix_report) = lint::fix::run(vault) {
            auto_fixed = fix_report.fixed.len();
        }

        // Re-embed stale pages.
        if !stale_embedding_pages.is_empty() {
            if let Some(provider) = embed_provider {
                match embed::reindex(vault, store, provider, embed_model).await {
                    Ok(n) => chunks_reembedded = n,
                    Err(e) => {
                        tracing::warn!(error = %e, "re-embed during maintain failed");
                    }
                }
            }
        }
    }

    Ok(MaintainReport {
        lint: lint_report,
        stale_embedding_pages,
        embedding_coverage: (embedded, total_pages),
        auto_fixed,
        chunks_reembedded,
    })
}

// ── Telemetry retention / prune ─────────────────────────────────────────

/// Report from [`prune_telemetry`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PruneReport {
    pub deleted_logs: u64,
    pub deleted_spans: u64,
    pub deleted_resources: u64,
    pub freed_bytes_estimate: u64,
    pub dry_run: bool,
}

/// Delete telemetry rows older than `older_than` and any resulting orphan
/// resources. Explicit-only — never called from startup or background.
///
/// `logs` rows with `time_unix_nano < cutoff` and `spans` rows with
/// `start_time_unix_nano < cutoff` are deleted, then any resource row no
/// longer referenced by either table is removed.
///
/// `dry_run = true` wraps all deletes in a SAVEPOINT that is rolled back,
/// so the caller sees the would-be counts without mutating the DB.
///
/// `freed_bytes_estimate` compares `PRAGMA page_count * PRAGMA page_size`
/// before and after; on dry-run it is `0`. This is approximate because
/// SQLite does not reclaim pages without `VACUUM`.
///
/// # Errors
///
/// Returns `rusqlite::Error` if the underlying DB cannot be opened or any
/// DELETE statement fails.
pub fn prune_telemetry(
    store: &TelemetryStore,
    older_than: std::time::Duration,
    dry_run: bool,
) -> rusqlite::Result<PruneReport> {
    let now_ns: i64 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let older_ns: i64 = i64::try_from(older_than.as_nanos()).unwrap_or(i64::MAX);
    let cutoff_ns = now_ns.saturating_sub(older_ns);

    let mut conn = rusqlite::Connection::open(store.db_path())?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    conn.pragma_update(None, "busy_timeout", 5000i64)?;

    let size_before = db_size_bytes(&conn).unwrap_or(0);

    let tx = conn.transaction()?;
    let deleted_logs = u64::try_from(tx.execute(
        "DELETE FROM logs WHERE time_unix_nano < ?1",
        rusqlite::params![cutoff_ns],
    )?)
    .unwrap_or(0);
    let deleted_spans = u64::try_from(tx.execute(
        "DELETE FROM spans WHERE start_time_unix_nano < ?1",
        rusqlite::params![cutoff_ns],
    )?)
    .unwrap_or(0);
    let deleted_resources = u64::try_from(tx.execute(
        "DELETE FROM resources WHERE \
         NOT EXISTS (SELECT 1 FROM logs WHERE logs.resource_id = resources.id) \
         AND NOT EXISTS (SELECT 1 FROM spans WHERE spans.resource_id = resources.id)",
        [],
    )?)
    .unwrap_or(0);

    if dry_run {
        tx.rollback()?;
    } else {
        tx.commit()?;
    }

    let freed_bytes_estimate = if dry_run {
        0
    } else {
        let size_after = db_size_bytes(&conn).unwrap_or(size_before);
        size_before.saturating_sub(size_after)
    };

    Ok(PruneReport {
        deleted_logs,
        deleted_spans,
        deleted_resources,
        freed_bytes_estimate,
        dry_run,
    })
}

fn db_size_bytes(conn: &rusqlite::Connection) -> rusqlite::Result<u64> {
    let page_count: i64 = conn.query_row("PRAGMA page_count", [], |r| r.get(0))?;
    let page_size: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
    let total = page_count.saturating_mul(page_size).max(0);
    Ok(u64::try_from(total).unwrap_or(0))
}

/// Parse a retention duration like `30d`, `12h`, `60m`, `90s`. Bare digit
/// strings are interpreted as seconds.
///
/// # Errors
///
/// Returns `Err` for empty input, unknown suffix, or non-numeric prefix.
pub fn parse_older_than(s: &str) -> std::result::Result<std::time::Duration, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("empty duration".to_string());
    }
    let last = s.chars().last().unwrap();
    let (num_str, mult) = if last.is_ascii_digit() {
        (s, 1u64)
    } else {
        let head = &s[..s.len() - last.len_utf8()];
        let mult = match last {
            's' => 1u64,
            'm' => 60,
            'h' => 3600,
            'd' => 86_400,
            other => return Err(format!("unknown suffix '{other}' (use s|m|h|d)")),
        };
        (head, mult)
    };
    let n: u64 = num_str
        .parse()
        .map_err(|e: std::num::ParseIntError| format!("invalid number '{num_str}': {e}"))?;
    Ok(std::time::Duration::from_secs(n.saturating_mul(mult)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embed::EmbeddingsStore;
    use crate::vault::Vault;

    #[tokio::test]
    async fn maintain_reports_on_empty_vault() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        git2::Repository::init(dir.path()).unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        let store = EmbeddingsStore::in_memory().unwrap();

        let report = maintain(&vault, &store, None, "mock-1", &MaintainOptions::default())
            .await
            .unwrap();

        assert!(report.lint.is_clean());
        assert!(report.stale_embedding_pages.is_empty());
        assert_eq!(report.embedding_coverage, (0, 0));
    }

    mod prune_telemetry {
        use super::super::{parse_older_than, prune_telemetry};
        use crate::telemetry::TelemetryStore;
        use rusqlite::{params, Connection};
        use std::time::Duration;

        fn open_store() -> (tempfile::TempDir, TelemetryStore, std::path::PathBuf) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("t.sqlite");
            let store = TelemetryStore::open(&path).expect("open store");
            (dir, store, path)
        }

        fn now_ns() -> i64 {
            i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            )
            .unwrap()
        }

        fn seed_resource(path: &std::path::Path, attrs: &str, hash: &str) -> i64 {
            let conn = Connection::open(path).unwrap();
            conn.execute(
                "INSERT OR IGNORE INTO resources (attributes, attributes_hash) VALUES (?1, ?2)",
                params![attrs, hash],
            )
            .unwrap();
            conn.query_row(
                "SELECT id FROM resources WHERE attributes_hash = ?1",
                params![hash],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        }

        fn seed_log(path: &std::path::Path, time: i64, resource_id: i64, dedup: &str) {
            let conn = Connection::open(path).unwrap();
            conn.execute(
                "INSERT INTO logs \
                 (time_unix_nano, observed_time_unix_nano, severity_number, severity_text, \
                  body, trace_id, span_id, resource_id, attributes, source, dedup_hash) \
                 VALUES (?1, ?1, 9, 'INFO', 'b', NULL, NULL, ?2, '{}', 'cli', ?3)",
                params![time, resource_id, dedup],
            )
            .unwrap();
        }

        fn seed_span(path: &std::path::Path, start: i64, resource_id: i64, span_id: &str) {
            let conn = Connection::open(path).unwrap();
            conn.execute(
                "INSERT INTO spans \
                 (span_id, trace_id, parent_span_id, name, kind, \
                  start_time_unix_nano, end_time_unix_nano, status_code, status_message, \
                  resource_id, attributes, events, source, dedup_hash) \
                 VALUES (?1, 'tr', NULL, 'n', 'INTERNAL', ?2, NULL, 'UNSET', NULL, \
                  ?3, '{}', NULL, 'cli', ?1)",
                params![span_id, start, resource_id],
            )
            .unwrap();
        }

        fn count(path: &std::path::Path, sql: &str) -> i64 {
            let conn = Connection::open(path).unwrap();
            conn.query_row(sql, [], |r| r.get(0)).unwrap()
        }

        #[test]
        fn retention_boundary_keeps_newer_prunes_older() {
            let (_d, store, path) = open_store();
            let rid = seed_resource(&path, r#"{"x":"y"}"#, "h1");
            let now = now_ns();
            let hour_ns: i64 = 3_600_000_000_000;

            seed_log(&path, now - 2 * hour_ns, rid, "old-log");
            seed_log(&path, now + hour_ns, rid, "new-log");
            seed_span(&path, now - 2 * hour_ns, rid, "old-span");
            seed_span(&path, now + hour_ns, rid, "new-span");

            let report =
                prune_telemetry(&store, Duration::from_secs(3600), false).expect("prune ok");
            assert_eq!(report.deleted_logs, 1);
            assert_eq!(report.deleted_spans, 1);
            assert!(!report.dry_run);

            let remaining_logs = count(&path, "SELECT COUNT(*) FROM logs WHERE body='b'");
            assert_eq!(remaining_logs, 1);
            let remaining_spans = count(&path, "SELECT COUNT(*) FROM spans");
            assert_eq!(remaining_spans, 1);
        }

        #[test]
        fn orphan_resources_removed() {
            let (_d, store, path) = open_store();
            let orphan = seed_resource(&path, r#"{"o":"1"}"#, "orphan-hash");
            let _live = seed_resource(&path, r#"{"l":"1"}"#, "live-hash");

            let now = now_ns();
            let hour_ns: i64 = 3_600_000_000_000;
            seed_log(&path, now - 2 * hour_ns, orphan, "old-o");
            let live_id = count(
                &path,
                "SELECT id FROM resources WHERE attributes_hash='live-hash'",
            );
            seed_log(&path, now, live_id, "new-l");

            let report =
                prune_telemetry(&store, Duration::from_secs(3600), false).expect("prune ok");
            assert_eq!(report.deleted_logs, 1);
            assert!(report.deleted_resources >= 1);

            let orphan_still = count(
                &path,
                "SELECT COUNT(*) FROM resources WHERE attributes_hash='orphan-hash'",
            );
            assert_eq!(orphan_still, 0);
            let live_still = count(
                &path,
                "SELECT COUNT(*) FROM resources WHERE attributes_hash='live-hash'",
            );
            assert_eq!(live_still, 1);
        }

        #[test]
        fn dry_run_does_not_mutate() {
            let (_d, store, path) = open_store();
            let rid = seed_resource(&path, r#"{"d":"1"}"#, "dh");
            let now = now_ns();
            let hour_ns: i64 = 3_600_000_000_000;
            seed_log(&path, now - 2 * hour_ns, rid, "old");
            seed_log(&path, now - 2 * hour_ns, rid, "old2");

            let before_logs = count(&path, "SELECT COUNT(*) FROM logs WHERE body='b'");
            let report =
                prune_telemetry(&store, Duration::from_secs(3600), true).expect("prune ok");
            assert!(report.dry_run);
            assert_eq!(report.deleted_logs, 2);
            assert_eq!(report.freed_bytes_estimate, 0);

            let after_logs = count(&path, "SELECT COUNT(*) FROM logs WHERE body='b'");
            assert_eq!(before_logs, after_logs);
        }

        #[test]
        fn empty_db_zero_report() {
            let (_d, store, _path) = open_store();
            let report = prune_telemetry(&store, Duration::from_secs(60), false).expect("prune ok");
            assert_eq!(report.deleted_logs, 0);
            assert_eq!(report.deleted_spans, 0);
        }

        #[test]
        fn parse_older_than_formats() {
            assert_eq!(parse_older_than("30d").unwrap().as_secs(), 30 * 86_400);
            assert_eq!(parse_older_than("12h").unwrap().as_secs(), 12 * 3600);
            assert_eq!(parse_older_than("60m").unwrap().as_secs(), 3600);
            assert_eq!(parse_older_than("90s").unwrap().as_secs(), 90);
            assert_eq!(parse_older_than("45").unwrap().as_secs(), 45);
            assert!(parse_older_than("").is_err());
            assert!(parse_older_than("10x").is_err());
            assert!(parse_older_than("abc").is_err());
        }
    }
}
