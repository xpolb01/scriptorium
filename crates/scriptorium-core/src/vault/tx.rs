//! `VaultTx` — staged, validated, atomic writes to the vault.
//!
//! Every mutating operation (ingest, query `--file`, MCP `write_page`, manual
//! edit via the CLI) goes through a transaction:
//!
//! 1. `vault.begin()` → a [`VaultTx`]
//! 2. Call [`VaultTx::write_page`], [`VaultTx::put_file`],
//!    [`VaultTx::append`], or [`VaultTx::delete`] to stage changes. Nothing
//!    touches disk yet.
//! 3. [`VaultTx::validate`] runs mechanical lint against the staged state
//!    (current vault + pending writes overlaid in memory). Any
//!    [`Severity::Error`](crate::lint::Severity::Error) aborts the commit.
//! 4. [`VaultTx::diff`] previews the change set without writing — used for
//!    `--dry-run` and MCP tool responses.
//! 5. [`VaultTx::commit`] takes the vault lock, fsyncs every staged file,
//!    creates a single git commit for the batch, and releases the lock.
//!    Returns the commit id.
//!
//! If the `VaultTx` is dropped without calling `commit`, nothing is written
//! to disk. Git is the undo log for anything that *was* committed:
//! `scriptorium undo` is just `git revert HEAD`.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};

use super::page::Page;
use super::Vault;
use crate::error::{Error, Result};
use crate::git;
use crate::lint::{self, LintReport, Severity};

/// What a single pending write does to its target path.
#[derive(Debug, Clone)]
enum Pending {
    /// Replace (or create) the file with the given full content.
    Put(String),
    /// Append the given suffix to an existing file, creating it if missing.
    Append(String),
    /// Remove the file from disk and from the git index.
    Delete,
}

/// A preview of a single pending change. Returned by [`VaultTx::diff`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeSummary {
    pub path: Utf8PathBuf,
    pub action: Action,
    /// Number of bytes the write would produce, for the human-readable output.
    pub bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Create,
    Update,
    Append,
    Delete,
}

/// A staged batch of vault mutations.
///
/// Hold a `VaultTx` only as long as needed to stage the batch; the
/// transaction itself does not take the vault lock. The lock is acquired
/// briefly inside [`Self::commit`] and released as soon as the commit is done.
pub struct VaultTx<'v> {
    vault: &'v Vault,
    pending: BTreeMap<Utf8PathBuf, Pending>,
}

impl Vault {
    /// Start a new transaction against this vault. Cheap — no IO.
    pub fn begin(&self) -> VaultTx<'_> {
        VaultTx {
            vault: self,
            pending: BTreeMap::new(),
        }
    }
}

impl VaultTx<'_> {
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Stage a page write. The path is `page.path` (vault-relative) and the
    /// content is the page rendered back to markdown.
    pub fn write_page(&mut self, page: &Page) -> Result<()> {
        let rendered = page.to_markdown()?;
        self.put_file(&page.path, rendered)
    }

    /// Stage an arbitrary file write, replacing any previous contents.
    ///
    /// Rejects paths that escape the vault root (absolute, `..` components).
    pub fn put_file(&mut self, rel: &Utf8Path, content: String) -> Result<()> {
        let key = self.canonical_key(rel)?;
        self.pending.insert(key, Pending::Put(content));
        Ok(())
    }

    /// Stage an append to `rel`. Appends accumulate if called multiple times
    /// for the same path.
    pub fn append(&mut self, rel: &Utf8Path, suffix: &str) -> Result<()> {
        let key = self.canonical_key(rel)?;
        match self.pending.get_mut(&key) {
            Some(Pending::Put(content)) => {
                content.push_str(suffix);
            }
            Some(Pending::Append(existing)) => {
                existing.push_str(suffix);
            }
            Some(Pending::Delete) => {
                // Delete+append → recreate with just the suffix.
                self.pending.insert(key, Pending::Put(suffix.to_string()));
            }
            None => {
                self.pending
                    .insert(key, Pending::Append(suffix.to_string()));
            }
        }
        Ok(())
    }

    /// Stage a deletion. The file is removed on commit; no-op if it never
    /// existed.
    pub fn delete(&mut self, rel: &Utf8Path) -> Result<()> {
        let key = self.canonical_key(rel)?;
        self.pending.insert(key, Pending::Delete);
        Ok(())
    }

    fn canonical_key(&self, rel: &Utf8Path) -> Result<Utf8PathBuf> {
        // Resolve just to validate — we only need the relative path as the
        // map key so the git2 commit code can derive absolute paths.
        self.vault.resolve(rel)?;
        Ok(rel.to_path_buf())
    }

    /// Preview the change set. Does not touch disk.
    pub fn diff(&self) -> Vec<ChangeSummary> {
        let mut out = Vec::with_capacity(self.pending.len());
        for (path, pending) in &self.pending {
            let abs = self.vault.root().join(path);
            let exists_on_disk = abs.as_std_path().exists();
            let (action, bytes) = match pending {
                Pending::Put(content) => {
                    let action = if exists_on_disk {
                        Action::Update
                    } else {
                        Action::Create
                    };
                    (action, content.len())
                }
                Pending::Append(suffix) => (Action::Append, suffix.len()),
                Pending::Delete => (Action::Delete, 0),
            };
            out.push(ChangeSummary {
                path: path.clone(),
                action,
                bytes,
            });
        }
        out
    }

    /// Run mechanical lint against the *staged* state — the current vault
    /// contents with the pending writes overlaid. This is a convenience
    /// wrapper around [`validate_pending`] for callers that want to preview
    /// validation outside a commit.
    ///
    /// **Note**: the actual pre-commit validation in [`Self::commit`] happens
    /// *inside* the vault lock, not via this method. Calling `validate()`
    /// on its own races with concurrent writers — it's only safe as a
    /// read-only preview.
    pub fn validate(&self) -> Result<LintReport> {
        validate_pending(self.vault, &self.pending)
    }

    /// Acquire the vault lock, validate against the locked state, fsync every
    /// staged write, create a git commit for the batch, and release the lock.
    /// Returns the new commit id.
    ///
    /// Aborts (without writing anything) if validation produces any
    /// error-level lint issues. Callers that want to bypass validation (e.g.
    /// for a targeted recovery commit) should use
    /// [`Self::commit_without_validation`].
    ///
    /// **Race-free**: the lint validation runs under the same lock as the
    /// write, so a concurrent writer cannot invalidate the check between
    /// `validate` and the filesystem mutations.
    pub fn commit(self, message: &str) -> Result<String> {
        if self.pending.is_empty() {
            return Err(Error::Other(anyhow::anyhow!("nothing to commit")));
        }
        let meta = self.vault.meta_dir();
        let vault = self.vault;
        let pending = self.pending;
        super::lock::with_lock(meta.as_std_path(), move || {
            // Validate under the lock: scan the current on-disk state, overlay
            // the pending writes in memory, run mechanical lint. Any
            // Error-severity issue aborts the commit before we touch disk.
            let report = validate_pending(vault, &pending)?;
            if report.has_errors() {
                let messages: Vec<String> = report
                    .errors()
                    .map(|i| format!("{}: {}", i.rule, i.message))
                    .collect();
                return Err(Error::Other(anyhow::anyhow!(
                    "vault tx validation failed: {}",
                    messages.join("; ")
                )));
            }
            apply_and_commit(vault.root().as_std_path(), &pending, message)
        })
    }

    /// Commit without running validation. Use sparingly — validation exists
    /// specifically to prevent landing broken links and malformed frontmatter.
    /// This exists for tests and emergency recovery. It still takes the vault
    /// lock, fsync's every write, and makes a single git commit.
    pub fn commit_without_validation(self, message: &str) -> Result<String> {
        if self.pending.is_empty() {
            return Err(Error::Other(anyhow::anyhow!("nothing to commit")));
        }
        let meta = self.vault.meta_dir();
        let vault = self.vault;
        let pending = self.pending;
        super::lock::with_lock(meta.as_std_path(), move || {
            apply_and_commit(vault.root().as_std_path(), &pending, message)
        })
    }
}

/// Free helper that performs mechanical lint against the current vault on
/// disk with the pending writes overlaid in memory. Extracted so
/// [`VaultTx::commit`] can call it from inside the lock closure without
/// borrow-checker gymnastics.
fn validate_pending(vault: &Vault, pending: &BTreeMap<Utf8PathBuf, Pending>) -> Result<LintReport> {
    let mut scan = vault.scan()?;
    // Overlay the pending writes on top of the scan. Only `Put` entries under
    // `wiki/` are page candidates and participate in link-graph validation;
    // everything else (log.md, raw sources under `sources/`, top-level
    // derived files like index.md) is excluded — sources are immutable
    // inputs by definition and a source's own frontmatter or wikilinks must
    // never fail a wiki-page validation.
    for (path, op) in pending {
        match op {
            Pending::Put(content) => {
                if !path.starts_with("wiki/") {
                    // Non-wiki put: not a wiki page, skip parse + overlay.
                    continue;
                }
                scan.pages.retain(|p| &p.path != path);
                if let Ok(page) = Page::parse(path.clone(), content) {
                    scan.pages.push(page);
                }
            }
            Pending::Delete => {
                scan.pages.retain(|p| &p.path != path);
            }
            Pending::Append(_) => {
                // Appends to wiki pages would corrupt frontmatter and are
                // disallowed by convention; callers use `append` only for
                // log.md and similar. Do not reinject into scan.
            }
        }
    }
    let graph = super::LinkGraph::build(&scan.pages);
    let mut issues = Vec::new();
    for err in &scan.errors {
        issues.push(crate::lint::LintIssue {
            severity: Severity::Error,
            rule: "scan".into(),
            page: None,
            path: Utf8PathBuf::from_path_buf(err.path.clone()).ok(),
            message: err.message.clone(),
        });
    }
    issues.extend(lint::frontmatter::check(&scan.pages));
    issues.extend(lint::frontmatter::check_duplicate_stems(&scan.pages));
    issues.extend(lint::broken_links::check(&graph));
    issues.extend(lint::orphans::check(&graph));
    Ok(LintReport { issues })
}

/// Free helper that performs the actual write + git commit for a pending set.
/// Called from inside [`super::lock::with_lock`] by both `commit` and
/// `commit_without_validation`.
fn apply_and_commit(
    root: &std::path::Path,
    pending: &BTreeMap<Utf8PathBuf, Pending>,
    message: &str,
) -> Result<String> {
    let mut changed_paths: Vec<PathBuf> = Vec::with_capacity(pending.len());
    for (rel, op) in pending {
        let abs = root.join(rel.as_std_path());
        match op {
            Pending::Put(content) => {
                write_with_fsync(&abs, content.as_bytes())?;
            }
            Pending::Append(suffix) => {
                append_with_fsync(&abs, suffix.as_bytes())?;
            }
            Pending::Delete => match std::fs::remove_file(&abs) {
                Ok(()) => {
                    // Deleting a file also changes the parent directory's
                    // dirent, so sync the parent for durability.
                    if let Some(parent) = abs.parent() {
                        sync_dir(parent)?;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::io(abs.clone(), e)),
            },
        }
        changed_paths.push(abs);
    }
    git::commit_paths(root, &changed_paths, message)
}

/// Write `bytes` to `path` durably: ensure the parent exists, open with
/// truncate, write, fsync the file, then fsync the parent directory so the
/// dirent is also durable. Without the directory fsync, a crash between
/// file-data-durable and dirent-durable would leave the tree committed in
/// git but the file missing from the filesystem on restart.
fn write_with_fsync(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.to_path_buf(), e))?;
    }
    {
        let mut file = File::create(path).map_err(|e| Error::io(path.to_path_buf(), e))?;
        file.write_all(bytes)
            .map_err(|e| Error::io(path.to_path_buf(), e))?;
        file.sync_all()
            .map_err(|e| Error::io(path.to_path_buf(), e))?;
    }
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn append_with_fsync(path: &std::path::Path, bytes: &[u8]) -> Result<()> {
    let creating = !path.exists();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| Error::io(parent.to_path_buf(), e))?;
    }
    {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| Error::io(path.to_path_buf(), e))?;
        file.write_all(bytes)
            .map_err(|e| Error::io(path.to_path_buf(), e))?;
        file.sync_all()
            .map_err(|e| Error::io(path.to_path_buf(), e))?;
    }
    // If we just created the file, the parent directory's dirent is new and
    // needs to be synced too. For pure appends to an existing file, the
    // dirent hasn't changed so the parent fsync is unnecessary.
    if creating {
        if let Some(parent) = path.parent() {
            sync_dir(parent)?;
        }
    }
    Ok(())
}

/// Fsync a directory so its dirent changes (created/deleted entries) are
/// durable. On Unix this is `open(dir, O_RDONLY) + fsync(fd)`; on Windows
/// rename/create are already durable to the volume without an explicit
/// directory fsync, so this is a no-op.
#[cfg(unix)]
fn sync_dir(path: &std::path::Path) -> Result<()> {
    let dir = File::open(path).map_err(|e| Error::io(path.to_path_buf(), e))?;
    dir.sync_all()
        .map_err(|e| Error::io(path.to_path_buf(), e))?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_path: &std::path::Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, PageId};
    use chrono::TimeZone;
    use tempfile::TempDir;

    fn fresh_vault() -> (TempDir, Vault) {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        (dir, vault)
    }

    fn sample_page(stem: &str, title: &str, body: &str) -> Page {
        let now = chrono::Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from(format!("wiki/{stem}.md")),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: title.into(),
                created: now,
                updated: now,
                sources: vec![],
                tags: vec![],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: body.into(),
        }
    }

    #[test]
    fn empty_tx_commit_errors() {
        let (_dir, vault) = fresh_vault();
        let tx = vault.begin();
        let err = tx.commit("empty").unwrap_err();
        assert!(err.to_string().contains("nothing to commit"));
    }

    #[test]
    fn put_file_writes_and_commits() {
        let (_dir, vault) = fresh_vault();
        let mut tx = vault.begin();
        let page = sample_page("foo", "Foo", "hello\n");
        tx.write_page(&page).unwrap();
        let diff = tx.diff();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].action, Action::Create);
        let oid = tx.commit("add foo").unwrap();
        assert_eq!(oid.len(), 40);
        let read_back =
            std::fs::read_to_string(vault.root().join("wiki/foo.md").as_std_path()).unwrap();
        assert!(read_back.contains("title: Foo"));
    }

    #[test]
    fn update_existing_page_reports_as_update() {
        let (_dir, vault) = fresh_vault();
        let mut tx = vault.begin();
        let mut page = sample_page("foo", "Foo", "first\n");
        tx.write_page(&page).unwrap();
        tx.commit("initial").unwrap();

        page.body = "second\n".into();
        let mut tx2 = vault.begin();
        tx2.write_page(&page).unwrap();
        let diff = tx2.diff();
        assert_eq!(diff[0].action, Action::Update);
        tx2.commit("update").unwrap();
    }

    #[test]
    fn append_accumulates_then_writes() {
        let (_dir, vault) = fresh_vault();
        let mut tx = vault.begin();
        tx.append(Utf8Path::new("log.md"), "line 1\n").unwrap();
        tx.append(Utf8Path::new("log.md"), "line 2\n").unwrap();
        tx.commit("log").unwrap();
        let text = std::fs::read_to_string(vault.root().join("log.md").as_std_path()).unwrap();
        assert_eq!(text, "line 1\nline 2\n");

        // Second commit appends to the existing file.
        let mut tx2 = vault.begin();
        tx2.append(Utf8Path::new("log.md"), "line 3\n").unwrap();
        tx2.commit("log2").unwrap();
        let text = std::fs::read_to_string(vault.root().join("log.md").as_std_path()).unwrap();
        assert_eq!(text, "line 1\nline 2\nline 3\n");
    }

    #[test]
    fn delete_removes_file_and_stages_in_git() {
        let (_dir, vault) = fresh_vault();
        let mut tx = vault.begin();
        let page = sample_page("foo", "Foo", "body\n");
        tx.write_page(&page).unwrap();
        tx.commit("add").unwrap();
        assert!(vault.root().join("wiki/foo.md").as_std_path().exists());

        let mut tx2 = vault.begin();
        tx2.delete(Utf8Path::new("wiki/foo.md")).unwrap();
        tx2.commit("rm").unwrap();
        assert!(!vault.root().join("wiki/foo.md").as_std_path().exists());
    }

    #[test]
    fn validate_blocks_commit_on_broken_links() {
        let (_dir, vault) = fresh_vault();
        let mut tx = vault.begin();
        let page = sample_page("foo", "Foo", "links to [[ghost]]\n");
        tx.write_page(&page).unwrap();
        let report = tx.validate().unwrap();
        assert!(report.has_errors());
        let err = tx.commit("try").unwrap_err();
        assert!(err.to_string().contains("broken wikilink"));
        // Nothing landed on disk.
        assert!(!vault.root().join("wiki/foo.md").as_std_path().exists());
    }

    #[test]
    fn rollback_on_drop_does_not_write() {
        let (_dir, vault) = fresh_vault();
        {
            let mut tx = vault.begin();
            let page = sample_page("foo", "Foo", "body\n");
            tx.write_page(&page).unwrap();
            // drop without commit
        }
        assert!(!vault.root().join("wiki/foo.md").as_std_path().exists());
    }

    #[test]
    fn path_escape_is_rejected() {
        let (_dir, vault) = fresh_vault();
        let mut tx = vault.begin();
        let err = tx
            .put_file(Utf8Path::new("../../etc/passwd"), String::new())
            .unwrap_err();
        assert!(matches!(err, Error::PathEscape(_)));
    }

    #[test]
    fn commit_validates_under_lock() {
        // Regression test for the TOCTOU race: validation must run against
        // the same locked state as the write. We can't trivially construct a
        // race between processes in a unit test, but we can verify the
        // behavioral contract: a tx whose staged state passes validation
        // commits, and a tx whose staged state introduces a broken link is
        // rejected *at commit time* (not just at validate time).
        let (_dir, vault) = fresh_vault();

        // Good path: a self-contained page commits cleanly.
        let mut good = vault.begin();
        good.write_page(&sample_page("standalone", "Standalone", "body\n"))
            .unwrap();
        good.commit("good").unwrap();

        // Bad path: a page with a broken wikilink is rejected by commit().
        let mut bad = vault.begin();
        bad.write_page(&sample_page("bad", "Bad", "linking to [[ghost]]\n"))
            .unwrap();
        let err = bad.commit("should fail").unwrap_err();
        assert!(
            err.to_string().contains("broken wikilink"),
            "expected broken-link error, got: {err}"
        );
        // Nothing landed on disk.
        assert!(!vault.root().join("wiki/bad.md").as_std_path().exists());
    }

    #[test]
    fn commit_fsyncs_parent_directory() {
        // We can't observe the fsync directly, but we can verify the parent
        // directory exists and the file is present after commit — and that
        // the commit does not error on the sync_dir call path. This is a
        // smoke test: the path has to compile and run on Unix and Windows
        // alike without panicking.
        let (dir, vault) = fresh_vault();
        let mut tx = vault.begin();
        tx.write_page(&sample_page("foo", "Foo", "body\n")).unwrap();
        tx.commit("add").unwrap();
        assert!(dir.path().join("wiki/foo.md").exists());
        // A second tx with a deletion also exercises the dir-sync path.
        let mut tx2 = vault.begin();
        tx2.delete(Utf8Path::new("wiki/foo.md")).unwrap();
        tx2.commit("rm").unwrap();
        assert!(!dir.path().join("wiki/foo.md").exists());
    }
}
