//! Shared `SQLite` connection setup for the file-backed stores.
//!
//! Several processes open the same database files at once: hooks run in
//! parallel, and the drain, the MCP server and the CLI share one vault.
//! Each store opens its connections through [`open_wal`] and runs its
//! multi-statement writes through [`with_write_txn`].

use std::thread;
use std::time::{Duration, Instant};

use rand::RngExt as _;
use rusqlite::Connection;

/// How long a statement waits for a lock, and how long [`open_wal`]
/// retries the switch to WAL.
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);

/// Set the busy timeout, then switch the database to WAL mode.
///
/// The busy timeout comes first, so the statements after it wait for a
/// lock instead of failing at once with "database is locked". Converting
/// a new database to WAL needs an exclusive lock, and while another
/// connection converts it `SQLite` returns `SQLITE_BUSY` at once, without
/// calling the busy handler. So the switch is retried here, within the
/// same budget.
pub(crate) fn open_wal(conn: &Connection) -> rusqlite::Result<()> {
    conn.busy_timeout(BUSY_TIMEOUT)?;
    let deadline = Instant::now() + BUSY_TIMEOUT;
    loop {
        // `journal_mode = WAL` is a PRAGMA that returns a row; use query_row.
        match conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get::<_, String>(0)) {
            Ok(_) => return Ok(()),
            Err(e) if is_busy(&e) && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(rand::rng().random_range(25..50)));
            }
            Err(e) => return Err(e),
        }
    }
}

/// Run `f` in a `BEGIN IMMEDIATE` transaction. Commit if it succeeds and
/// roll back if it fails. Because the write lock is taken first, no other
/// connection can write between the reads and the writes inside `f`.
pub(crate) fn with_write_txn<T>(
    conn: &Connection,
    f: impl FnOnce() -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    conn.execute_batch("BEGIN IMMEDIATE")?;
    let result = f().and_then(|v| conn.execute_batch("COMMIT").map(|()| v));
    if result.is_err() {
        let _ = conn.execute_batch("ROLLBACK");
    }
    result
}

fn is_busy(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(ffi, _)
            if ffi.code == rusqlite::ErrorCode::DatabaseBusy
                || ffi.code == rusqlite::ErrorCode::DatabaseLocked
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_write_txn_commits_on_success_and_rolls_back_on_error() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE t (x INTEGER)").unwrap();

        with_write_txn(&conn, || conn.execute("INSERT INTO t VALUES (1)", [])).unwrap();
        let failed = with_write_txn(&conn, || {
            conn.execute("INSERT INTO t VALUES (2)", [])?;
            conn.execute("INSERT INTO missing VALUES (3)", [])
        });
        assert!(failed.is_err());

        let rows: Vec<i64> = conn
            .prepare("SELECT x FROM t ORDER BY x")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(rows, vec![1], "the failed transaction must leave no row");
        assert!(conn.is_autocommit(), "no transaction may stay open");
    }
}
