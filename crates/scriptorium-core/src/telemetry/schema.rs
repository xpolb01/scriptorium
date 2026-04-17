#![allow(clippy::doc_markdown)]

//! Telemetry schema migrations.
//!
//! Migration 001 creates the four OTel-shaped tables (`schema_version`,
//! `resources`, `spans`, `logs`) plus seven dashboard-critical indexes.
//! The SQL is embedded via [`include_str!`] from
//! `crates/scriptorium-core/migrations/001_telemetry_initial.sql` — that
//! `.sql` file is the single source of truth; this module only tracks
//! version numbers and orchestrates application.
//!
//! [`apply_migrations`] is idempotent: re-applying against a DB already at
//! the target version is a no-op. All DDL is wrapped in one transaction
//! per migration for atomicity.

use rusqlite::{Connection, OptionalExtension};

/// Migration 001 source (embedded at compile time).
pub const MIGRATION_001: &str = include_str!("../../migrations/001_telemetry_initial.sql");

/// The highest schema version this build knows how to produce.
pub const CURRENT_SCHEMA_VERSION: u32 = 1;

/// Ordered registry of all known migrations. Version numbers MUST be
/// strictly increasing and contiguous starting at 1.
const MIGRATIONS: &[(u32, &str)] = &[(1, MIGRATION_001)];

/// Apply every migration whose version is greater than the DB's current
/// recorded version. Returns the final schema version.
///
/// Idempotent: calling twice on the same connection returns the same
/// version on the second call without re-running any DDL against a
/// fully-migrated DB. Each migration runs inside its own transaction; a
/// failing statement rolls back the whole migration leaving the previous
/// version intact.
///
/// # Errors
///
/// Returns [`rusqlite::Error`] if any DDL statement fails (for example a
/// CHECK or FOREIGN KEY constraint rejecting the statement at parse
/// time), or if the transaction cannot be committed.
pub fn apply_migrations(conn: &Connection) -> rusqlite::Result<u32> {
    let mut current = current_version(conn)?;

    for &(version, sql) in MIGRATIONS {
        if version > current {
            conn.execute_batch(&format!("BEGIN;\n{sql}\nCOMMIT;"))?;
            current = version;
        }
    }

    Ok(current)
}

/// Read the highest applied version from `schema_version`. Returns `0`
/// when the table does not yet exist (a fresh database).
fn current_version(conn: &Connection) -> rusqlite::Result<u32> {
    let row: Option<Option<i64>> = conn
        .query_row("SELECT MAX(version) FROM schema_version", [], |r| {
            r.get::<_, Option<i64>>(0)
        })
        .optional()
        .or_else(|e| match e {
            rusqlite::Error::SqliteFailure(_, Some(ref msg)) if msg.contains("no such table") => {
                Ok(None)
            }
            rusqlite::Error::SqlInputError { ref msg, .. } if msg.contains("no such table") => {
                Ok(None)
            }
            other => Err(other),
        })?;

    let raw = row.flatten().unwrap_or(0).max(0);
    Ok(u32::try_from(raw).unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("open in-memory db");
        conn.execute_batch("PRAGMA foreign_keys = ON;")
            .expect("enable fk");
        conn
    }

    #[test]
    fn fresh_db_applies_to_v1() {
        let conn = fresh_conn();
        let v = apply_migrations(&conn).expect("apply");
        assert_eq!(v, CURRENT_SCHEMA_VERSION);

        let tables: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(
            tables,
            vec![
                "logs".to_string(),
                "resources".to_string(),
                "schema_version".to_string(),
                "spans".to_string(),
            ]
        );

        let indexes: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='index' \
                 AND name NOT LIKE 'sqlite_%' ORDER BY name",
            )
            .unwrap()
            .query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        for expected in [
            "idx_logs_severity",
            "idx_logs_source",
            "idx_logs_time",
            "idx_logs_trace",
            "idx_spans_source",
            "idx_spans_start",
            "idx_spans_trace",
        ] {
            assert!(
                indexes.iter().any(|n| n == expected),
                "missing index {expected}; got {indexes:?}"
            );
        }
    }

    #[test]
    fn idempotent_reapply() {
        let conn = fresh_conn();
        apply_migrations(&conn).unwrap();
        apply_migrations(&conn).unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM schema_version WHERE version = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn json_valid_check_fires() {
        let conn = fresh_conn();
        apply_migrations(&conn).unwrap();

        conn.execute(
            "INSERT INTO resources (attributes, attributes_hash) VALUES ('{}', 'r1')",
            [],
        )
        .unwrap();

        let err = conn.execute(
            "INSERT INTO logs \
             (time_unix_nano, observed_time_unix_nano, severity_number, body, \
              resource_id, attributes, source, dedup_hash) \
             VALUES (1, 1, 9, 'x', 1, 'not-json', 'cli', 'd1')",
            [],
        );
        assert!(err.is_err(), "CHECK(json_valid(attributes)) must reject");
    }

    #[test]
    fn foreign_key_enforced() {
        let conn = fresh_conn();
        apply_migrations(&conn).unwrap();

        let err = conn.execute(
            "INSERT INTO spans \
             (span_id, trace_id, name, kind, start_time_unix_nano, status_code, \
              resource_id, attributes, source) \
             VALUES ('s1', 't1', 'x', 'INTERNAL', 1, 'UNSET', 999, '{}', 'cli')",
            [],
        );
        assert!(err.is_err(), "FOREIGN KEY resource_id=999 must be rejected");
    }

    #[test]
    fn partial_apply_recovery() {
        let conn = fresh_conn();
        conn.execute_batch(
            "CREATE TABLE schema_version (\
               version INTEGER PRIMARY KEY, \
               applied_at_unix_nano INTEGER NOT NULL, \
               note TEXT);",
        )
        .unwrap();

        let v = apply_migrations(&conn).unwrap();
        assert_eq!(v, 1);

        let tables: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master \
                 WHERE type='table' AND name IN ('logs','resources','spans')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(tables, 3);
    }
}
