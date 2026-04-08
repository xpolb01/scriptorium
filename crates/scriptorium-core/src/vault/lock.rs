//! Advisory file lock held for the duration of a mutating vault operation.
//!
//! Location: `<vault>/.scriptorium/vault.lock`. Writers (CLI ingest, MCP write
//! tools, watch-mode auto-ingest) all call [`with_lock`] before touching the
//! vault; readers (scan, lint, query) acquire nothing.
//!
//! The lock is implemented with `fd-lock`, which uses `flock(2)` on Unix and
//! `LockFileEx` on Windows. It is cross-process: two `scriptorium` binaries
//! running against the same vault will serialize correctly.
//!
//! We take the lock as a scoped closure rather than a long-lived RAII guard
//! to avoid a self-referential `{RwLock, Guard}` pair. In practice scriptorium
//! mutations are always short-lived (stage in memory → acquire → fsync →
//! commit → release), so a scope-based API matches the use case.

use std::fs::OpenOptions;
use std::path::Path;

use fd_lock::RwLock;

use crate::error::{Error, Result};

/// Run `f` while holding the exclusive vault lock. Blocks until the lock is
/// available; the lock is released when `f` returns (whether `Ok` or `Err`).
///
/// Creates `<meta_dir>` and the lockfile inside it if they do not exist yet.
pub fn with_lock<T, F>(meta_dir: &Path, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    std::fs::create_dir_all(meta_dir).map_err(|e| Error::io(meta_dir.to_path_buf(), e))?;
    let lock_path = meta_dir.join("vault.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| Error::io(lock_path.clone(), e))?;
    let mut rwlock = RwLock::new(file);
    let _guard = rwlock
        .write()
        .map_err(|e| Error::io(lock_path.clone(), e))?;
    f()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;

    #[test]
    fn with_lock_creates_the_lockfile() {
        let dir = TempDir::new().unwrap();
        let meta = dir.path().join(".scriptorium");
        with_lock(&meta, || Ok(())).unwrap();
        assert!(meta.join("vault.lock").exists());
    }

    #[test]
    fn runs_callers_sequentially_across_threads() {
        let dir = TempDir::new().unwrap();
        let meta = dir.path().join(".scriptorium");
        let barrier = Arc::new(Barrier::new(2));

        let meta1 = meta.clone();
        let barrier1 = barrier.clone();
        let t1 = thread::spawn(move || {
            let start = Instant::now();
            with_lock(&meta1, || {
                barrier1.wait(); // signal t2 that we're holding the lock
                thread::sleep(Duration::from_millis(100));
                Ok(())
            })
            .unwrap();
            start.elapsed()
        });

        let meta2 = meta.clone();
        let barrier2 = barrier.clone();
        let t2 = thread::spawn(move || {
            barrier2.wait(); // wait for t1 to take the lock
            let start = Instant::now();
            with_lock(&meta2, || Ok(())).unwrap();
            start.elapsed()
        });

        t1.join().unwrap();
        let t2_elapsed = t2.join().unwrap();
        assert!(
            t2_elapsed >= Duration::from_millis(50),
            "second call should have blocked while t1 held the lock, \
             elapsed = {t2_elapsed:?}"
        );
    }
}
