//! Thin wrapper around `git2` for the operations scriptorium needs.
//!
//! We use git as the durable undo log for every mutating operation: the entry
//! point is [`commit_paths`], which stages the given paths in the working
//! tree, creates a commit on the current branch (initializing the repo first
//! if it does not exist), and returns the new commit id.
//!
//! Scope is intentionally minimal. No push/pull, no branches, no merges.
//! Scriptorium's job is to keep the vault consistent; anything fancier (e.g.
//! syncing to a remote) is left to the user running `git` themselves.

use std::path::{Path, PathBuf};

use git2::{IndexAddOption, Repository, RepositoryInitOptions, Signature};

use crate::error::{Error, Result};

/// Open the repository at `root`, or initialize a fresh one if none exists.
///
/// New repositories use `main` as the initial branch name (git2's default can
/// otherwise drift with the host git version).
pub fn open_or_init(root: &Path) -> Result<Repository> {
    if let Ok(repo) = Repository::open(root) {
        return Ok(repo);
    }
    let mut opts = RepositoryInitOptions::new();
    opts.initial_head("main");
    Repository::init_opts(root, &opts).map_err(git_err)
}

/// Stage the given vault-relative paths and create a single commit.
///
/// `paths` may include files that no longer exist on disk (deletions).
/// Returns the new commit id as a hex string.
///
/// The author/committer signature is derived from the user's git config if
/// available, otherwise falls back to `scriptorium <noreply@scriptorium>`.
pub fn commit_paths(root: &Path, paths: &[PathBuf], message: &str) -> Result<String> {
    let repo = open_or_init(root)?;
    stage_paths(&repo, paths)?;
    let sig = signature(&repo)?;
    let mut index = repo.index().map_err(git_err)?;
    let tree_oid = index.write_tree().map_err(git_err)?;
    let tree = repo.find_tree(tree_oid).map_err(git_err)?;

    // Parent: whatever HEAD currently points to. If this is the first commit
    // in the repo (HEAD points at an unborn branch), there is no parent.
    let parent_commit = match repo.head() {
        Ok(head) => head.peel_to_commit().ok(),
        Err(_) => None,
    };
    let parents: Vec<&git2::Commit> = parent_commit.iter().collect();

    let oid = repo
        .commit(Some("HEAD"), &sig, &sig, message, &tree, &parents)
        .map_err(git_err)?;
    Ok(oid.to_string())
}

fn stage_paths(repo: &Repository, paths: &[PathBuf]) -> Result<()> {
    let raw_workdir = repo
        .workdir()
        .ok_or_else(|| Error::Other(anyhow::anyhow!("repository is bare")))?
        .to_path_buf();
    // Canonicalize the workdir so prefix-stripping is symlink-safe (macOS
    // `/var/folders/...` is a symlink to `/private/var/folders/...` and the
    // two forms never share a literal prefix).
    let workdir = raw_workdir
        .canonicalize()
        .unwrap_or_else(|_| raw_workdir.clone());
    let mut index = repo.index().map_err(git_err)?;
    let mut relatives: Vec<PathBuf> = Vec::with_capacity(paths.len());
    for p in paths {
        let rel = if p.is_absolute() {
            let canon = canonicalize_possibly_missing(p);
            canon
                .strip_prefix(&workdir)
                .map_err(|_| {
                    Error::Other(anyhow::anyhow!(
                        "path `{}` (canonical `{}`) is outside the repo at `{}`",
                        p.display(),
                        canon.display(),
                        workdir.display()
                    ))
                })?
                .to_path_buf()
        } else {
            p.clone()
        };
        relatives.push(rel);
    }

    let (present, missing): (Vec<_>, Vec<_>) = relatives
        .into_iter()
        .partition(|rel| workdir.join(rel).exists());

    if !present.is_empty() {
        let refs: Vec<&Path> = present.iter().map(PathBuf::as_path).collect();
        index
            .add_all(refs, IndexAddOption::DEFAULT, None)
            .map_err(git_err)?;
    }
    for rel in &missing {
        // remove_path ignores missing entries gracefully; we still call it so
        // deletions get staged.
        let _ = index.remove_path(rel);
    }
    index.write().map_err(git_err)?;
    Ok(())
}

fn signature(repo: &Repository) -> Result<Signature<'static>> {
    if let Ok(sig) = repo.signature() {
        // `repo.signature()` returns a Signature borrowed from repo config;
        // clone into 'static by rebuilding from the name/email/time.
        let name = sig.name().unwrap_or("scriptorium").to_string();
        let email = sig.email().unwrap_or("noreply@scriptorium").to_string();
        return Signature::now(&name, &email).map_err(git_err);
    }
    Signature::now("scriptorium", "noreply@scriptorium").map_err(git_err)
}

// `map_err` passes errors by value, so taking &git2::Error here would force
// every call site into `|e| git_err(&e)`. Prefer the shorter call sites.
#[allow(clippy::needless_pass_by_value)]
fn git_err(e: git2::Error) -> Error {
    Error::Other(anyhow::anyhow!("git: {e}"))
}

/// Canonicalize a path that may not exist (e.g. staged deletions). Falls back
/// to canonicalizing the parent and rejoining the file name, then to the raw
/// path if even that fails.
fn canonicalize_possibly_missing(p: &Path) -> PathBuf {
    if let Ok(c) = p.canonicalize() {
        return c;
    }
    if let (Some(parent), Some(name)) = (p.parent(), p.file_name()) {
        if let Ok(c) = parent.canonicalize() {
            return c.join(name);
        }
    }
    p.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn auto_inits_repository_on_first_commit() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("hello.md");
        std::fs::write(&file, "hello\n").unwrap();
        let oid = commit_paths(dir.path(), &[file], "init").unwrap();
        assert_eq!(oid.len(), 40, "git oid is a 40-char hex string");
        // Repo exists now.
        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap();
        assert!(head.is_branch());
        assert_eq!(head.shorthand(), Some("main"));
    }

    #[test]
    fn stacks_multiple_commits() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("a.md");
        std::fs::write(&file, "one\n").unwrap();
        let first = commit_paths(dir.path(), std::slice::from_ref(&file), "first").unwrap();

        std::fs::write(&file, "two\n").unwrap();
        let second = commit_paths(dir.path(), &[file], "second").unwrap();
        assert_ne!(first, second);

        let repo = Repository::open(dir.path()).unwrap();
        let head_commit = repo.head().unwrap().peel_to_commit().unwrap();
        assert_eq!(head_commit.parent_count(), 1);
    }

    #[test]
    fn stages_deletions() {
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("doomed.md");
        std::fs::write(&file, "gone\n").unwrap();
        commit_paths(dir.path(), std::slice::from_ref(&file), "add").unwrap();
        std::fs::remove_file(&file).unwrap();
        commit_paths(dir.path(), &[file], "rm").unwrap();
        let repo = Repository::open(dir.path()).unwrap();
        let head = repo.head().unwrap().peel_to_tree().unwrap();
        assert!(head.get_name("doomed.md").is_none());
    }
}
