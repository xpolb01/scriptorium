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

use git2::{Repository, RepositoryInitOptions, Signature};

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
        // Normalize to an absolute canonical path regardless of whether
        // the caller passed relative or absolute. This is critical: the
        // old fork passed relative paths through unchanged, which meant
        // `./log.md` (produced by `root.join("log.md")` when `root = "."`,
        // as in `scriptorium ingest` run from inside the vault) flowed
        // all the way to libgit2 as the pathspec `./log.md` — which does
        // NOT match the index entry `log.md` and silently stages nothing,
        // producing empty commits.
        let absolute = if p.is_absolute() {
            p.clone()
        } else {
            workdir.join(p)
        };
        let canon = canonicalize_possibly_missing(&absolute);
        let rel = canon
            .strip_prefix(&workdir)
            .map_err(|_| {
                Error::Other(anyhow::anyhow!(
                    "path `{}` (canonical `{}`) is outside the repo at `{}`",
                    p.display(),
                    canon.display(),
                    workdir.display()
                ))
            })?
            .to_path_buf();
        relatives.push(rel);
    }

    let (present, missing): (Vec<_>, Vec<_>) = relatives
        .into_iter()
        .partition(|rel| workdir.join(rel).exists());

    // Use `add_path` per file instead of `add_all` with pathspec matching.
    // `add_path` takes a literal file path and directly stages its current
    // on-disk content — no glob/pathspec interpretation that could silently
    // drop files. Defense-in-depth against the class of bug that produced
    // empty commits in scriptorium-vault.
    for rel in &present {
        index.add_path(rel).map_err(git_err)?;
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

    /// Regression test for the "empty commit on new subdirectory" bug.
    ///
    /// VaultTx::commit was producing git commits with the correct message
    /// but ZERO file changes when an ingest wrote pages into a subdirectory
    /// that did not exist in the index yet (e.g. `wiki/notes/`,
    /// `wiki/patterns/` on a vault whose index only knew about
    /// `wiki/entities/`). The root cause: `index.add_all` with the default
    /// flags treats its input as pathspec glob patterns, and for literal
    /// paths into brand-new subdirectories the match silently returns
    /// nothing. `add_path` (which takes a literal file path) works
    /// correctly.
    ///
    /// This test creates a repo with an initial commit at the root, then
    /// tries to commit a new file in a brand-new `nested/deep/` subtree.
    /// The commit MUST contain the file — an empty commit fails the test.
    #[test]
    fn commits_files_in_brand_new_subdirectory() {
        let dir = TempDir::new().unwrap();
        // Initial commit at the root so we have a parent for the next one.
        let root_file = dir.path().join("root.md");
        std::fs::write(&root_file, "root\n").unwrap();
        commit_paths(dir.path(), std::slice::from_ref(&root_file), "init").unwrap();

        // Write a file into a brand-new subdirectory. The subdirectory has
        // never been seen by the index before. `write_with_fsync`-style
        // callers do `create_dir_all` first, so by commit time the dir
        // exists on disk.
        let nested_dir = dir.path().join("nested").join("deep");
        std::fs::create_dir_all(&nested_dir).unwrap();
        let nested_file = nested_dir.join("page.md");
        std::fs::write(&nested_file, "nested body\n").unwrap();

        let oid = commit_paths(dir.path(), &[nested_file.clone()], "add nested").unwrap();

        // Inspect the resulting commit's tree. It MUST contain the new
        // file at `nested/deep/page.md`; an empty commit fails the test.
        let repo = Repository::open(dir.path()).unwrap();
        let commit = repo
            .find_commit(git2::Oid::from_str(&oid).unwrap())
            .unwrap();
        let tree = commit.tree().unwrap();

        // Walk the tree down to nested/deep/page.md.
        let nested_entry = tree
            .get_name("nested")
            .expect("commit tree should contain `nested/` subtree after staging a file in it");
        let nested_tree = nested_entry
            .to_object(&repo)
            .unwrap()
            .peel_to_tree()
            .unwrap();
        let deep_entry = nested_tree
            .get_name("deep")
            .expect("commit tree should contain `nested/deep/` subtree");
        let deep_tree = deep_entry.to_object(&repo).unwrap().peel_to_tree().unwrap();
        assert!(
            deep_tree.get_name("page.md").is_some(),
            "commit tree must contain nested/deep/page.md — if this fails, \
             stage_paths is producing empty commits for files in new subdirs"
        );

        // Also assert the commit is not empty by comparing its tree to
        // the parent's tree. A non-empty commit has a different tree oid.
        let parent = commit.parent(0).unwrap();
        assert_ne!(
            commit.tree_id(),
            parent.tree_id(),
            "commit tree matches parent tree — commit is empty (the bug)"
        );
    }

    /// Same regression, but for multiple distinct brand-new subdirectories
    /// in one commit. This mirrors the real ingest pattern where a single
    /// plan creates pages across several new folders simultaneously
    /// (wiki/notes/a.md + wiki/patterns/b.md + sources/articles/c.md).
    #[test]
    fn commits_files_in_multiple_brand_new_subdirectories() {
        let dir = TempDir::new().unwrap();
        let root_file = dir.path().join("root.md");
        std::fs::write(&root_file, "root\n").unwrap();
        commit_paths(dir.path(), std::slice::from_ref(&root_file), "init").unwrap();

        // Three brand-new subdirectories, one file each.
        let targets = [
            ("wiki/notes", "a.md", "a body\n"),
            ("wiki/patterns", "b.md", "b body\n"),
            ("sources/articles", "c.md", "c body\n"),
        ];
        let mut paths = Vec::new();
        for (subdir, name, body) in &targets {
            let d = dir.path().join(subdir);
            std::fs::create_dir_all(&d).unwrap();
            let f = d.join(name);
            std::fs::write(&f, body).unwrap();
            paths.push(f);
        }

        let oid = commit_paths(dir.path(), &paths, "add three").unwrap();
        let repo = Repository::open(dir.path()).unwrap();
        let commit = repo
            .find_commit(git2::Oid::from_str(&oid).unwrap())
            .unwrap();

        // Every file must appear in the commit tree.
        for (subdir, name, _) in &targets {
            let mut current = commit.tree().unwrap();
            for part in subdir.split('/') {
                let next_oid = {
                    let entry = current
                        .get_name(part)
                        .unwrap_or_else(|| panic!("missing subtree `{part}` (parent of `{name}`)"));
                    entry.id()
                };
                current = repo.find_tree(next_oid).unwrap();
            }
            assert!(
                current.get_name(name).is_some(),
                "commit tree missing `{subdir}/{name}` — bug reproduced"
            );
        }
    }

    /// The bug reproduces only when the working tree has **uncommitted
    /// modifications to unrelated files at commit time** — the exact
    /// state of the real vault when the dropped commits 85c5d8d and
    /// 211df25 were made. The schema update to CLAUDE.md sat in the
    /// working tree (not staged, not committed), and then a subsequent
    /// ingest tried to commit brand-new pages in a brand-new
    /// subdirectory. The cleaner reproducer above passes because it
    /// doesn't exercise this interaction.
    ///
    /// Setup mirrors the real failure:
    /// 1. Initial commit with CLAUDE.md + wiki/entities/foo.md.
    /// 2. Modify CLAUDE.md in the working tree (do NOT stage, do NOT
    ///    commit) — this simulates the schema update I made directly
    ///    before the ingests.
    /// 3. Write a new page at wiki/notes/bar.md (brand-new subdir).
    /// 4. Call commit_paths with ONLY [wiki/notes/bar.md].
    /// 5. Assert the resulting commit tree contains bar.md.
    #[test]
    fn commits_new_subdir_file_when_working_tree_has_dirty_unrelated_files() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();

        // Step 1: initial commit with CLAUDE.md + wiki/entities/foo.md.
        let claude_md = root.join("CLAUDE.md");
        std::fs::write(&claude_md, "# Vault Rules\n\nInitial schema.\n").unwrap();
        let entities_dir = root.join("wiki").join("entities");
        std::fs::create_dir_all(&entities_dir).unwrap();
        let foo = entities_dir.join("foo.md");
        std::fs::write(&foo, "initial foo\n").unwrap();
        commit_paths(root, &[claude_md.clone(), foo], "init").unwrap();

        // Step 2: modify CLAUDE.md in the working tree, do NOT stage it.
        std::fs::write(
            &claude_md,
            "# Vault Rules\n\nInitial schema.\n\nSchema update added later.\n",
        )
        .unwrap();

        // Step 3: brand-new subdir + brand-new file.
        let notes_dir = root.join("wiki").join("notes");
        std::fs::create_dir_all(&notes_dir).unwrap();
        let bar = notes_dir.join("bar.md");
        std::fs::write(&bar, "bar body\n").unwrap();

        // Step 4: commit ONLY the new file — not CLAUDE.md.
        let oid = commit_paths(root, &[bar.clone()], "ingest bar").unwrap();

        // Step 5: the resulting commit tree MUST contain wiki/notes/bar.md.
        let repo = Repository::open(root).unwrap();
        let commit = repo
            .find_commit(git2::Oid::from_str(&oid).unwrap())
            .unwrap();
        let parent = commit.parent(0).unwrap();
        assert_ne!(
            commit.tree_id(),
            parent.tree_id(),
            "commit tree matches parent tree — commit is empty (the bug reproduces here)"
        );
        let tree = commit.tree().unwrap();
        let wiki_oid = tree.get_name("wiki").expect("wiki/ subtree").id();
        let wiki_tree = repo.find_tree(wiki_oid).unwrap();
        let notes_oid = wiki_tree
            .get_name("notes")
            .expect("wiki/notes/ subtree")
            .id();
        let notes_tree = repo.find_tree(notes_oid).unwrap();
        assert!(
            notes_tree.get_name("bar.md").is_some(),
            "commit tree missing wiki/notes/bar.md — BUG REPRODUCED"
        );

        // Extra safety: CLAUDE.md's tree entry in the new commit must be
        // unchanged (same oid) from the parent — we did NOT ask to commit
        // it, and its working-tree modification must not leak in.
        let parent_tree = parent.tree().unwrap();
        let new_claude_oid = commit.tree().unwrap().get_name("CLAUDE.md").unwrap().id();
        let parent_claude_oid = parent_tree.get_name("CLAUDE.md").unwrap().id();
        assert_eq!(
            new_claude_oid, parent_claude_oid,
            "unstaged CLAUDE.md modification leaked into a commit that didn't ask for it"
        );
    }

    /// THE regression test for the production bug. When `scriptorium
    /// ingest` is run from inside the vault directory without `-C`, the
    /// CLI opens the vault at path `"."`. `apply_and_commit` then does
    /// `root.join("log.md")` which produces the relative path
    /// `./log.md`. That leading `./` flows all the way down to libgit2
    /// as a pathspec, and libgit2's pathspec matcher does NOT treat
    /// `./log.md` as equivalent to the index entry `log.md`. The result
    /// is a silent no-op — `index.add_all` returns success, the index
    /// is unchanged, and the commit is empty.
    ///
    /// This test passes RELATIVE paths (some with leading `./`) to
    /// `commit_paths` and asserts the resulting commits are non-empty.
    /// It reproduces the bug and pins the fix.
    #[test]
    fn commits_with_relative_path_and_dot_slash_prefix() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().canonicalize().unwrap();

        // Initial commit with log.md via a plain relative path.
        std::fs::write(root.join("log.md"), "# Log\n\nInitial.\n").unwrap();
        commit_paths(&root, &[PathBuf::from("log.md")], "init").unwrap();

        // Append to log.md on disk.
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(root.join("log.md"))
                .unwrap();
            f.write_all(b"\n## entry\n\nAppended.\n").unwrap();
            f.sync_all().unwrap();
        }

        // Commit using a `./`-prefixed RELATIVE path — exactly what the
        // real apply_and_commit produces when `root = Path::new(".")`
        // and we call `root.join("log.md")`.
        let oid = commit_paths(&root, &[PathBuf::from("./log.md")], "append log").unwrap();

        let repo = Repository::open(&root).unwrap();
        let commit = repo
            .find_commit(git2::Oid::from_str(&oid).unwrap())
            .unwrap();
        let parent = commit.parent(0).unwrap();
        assert_ne!(
            commit.tree_id(),
            parent.tree_id(),
            "commit tree matches parent tree — the `./log.md` pathspec failed \
             to stage the modification. This is the empty-commit bug."
        );
        let tree = commit.tree().unwrap();
        let blob = repo
            .find_blob(tree.get_name("log.md").expect("log.md").id())
            .unwrap();
        assert!(std::str::from_utf8(blob.content())
            .unwrap()
            .contains("Appended"));
    }

    /// Same bug, same call site, but with a brand-new file under a
    /// brand-new subdirectory — the case that matches the original
    /// persistence-pipeline and smoke-test empty commits (files in
    /// `wiki/notes/`, `wiki/patterns/`, `sources/articles/`).
    #[test]
    fn commits_new_nested_file_with_relative_dot_slash_prefix() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().canonicalize().unwrap();

        // Initial commit with a root file so we have a parent.
        std::fs::write(root.join("README.md"), "# Repo\n").unwrap();
        commit_paths(&root, &[PathBuf::from("README.md")], "init").unwrap();

        // Create a brand-new nested file.
        let nested = root.join("wiki").join("notes");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("new-page.md"), "body\n").unwrap();

        // Commit using `./wiki/notes/new-page.md` (the exact form
        // produced by `Path::new(".").join("wiki/notes/new-page.md")`).
        let oid = commit_paths(
            &root,
            &[PathBuf::from("./wiki/notes/new-page.md")],
            "add nested",
        )
        .unwrap();

        let repo = Repository::open(&root).unwrap();
        let commit = repo
            .find_commit(git2::Oid::from_str(&oid).unwrap())
            .unwrap();
        assert_ne!(
            commit.tree_id(),
            commit.parent(0).unwrap().tree_id(),
            "commit is empty — the `./` prefix caused add_all/add_path to drop the file"
        );
        let tree = commit.tree().unwrap();
        let wiki_oid = tree.get_name("wiki").expect("wiki/").id();
        let wiki_tree = repo.find_tree(wiki_oid).unwrap();
        let notes_oid = wiki_tree.get_name("notes").expect("wiki/notes/").id();
        let notes_tree = repo.find_tree(notes_oid).unwrap();
        assert!(notes_tree.get_name("new-page.md").is_some());
    }

    /// The minimal reproducer of the actual production bug observed in
    /// the scriptorium-vault repo: append to an already-tracked file
    /// (`log.md`), then `commit_paths([log.md])`, and the resulting
    /// commit is empty (same tree oid as its parent).
    ///
    /// This is the code path exercised by every ingest — every ingest
    /// appends to log.md. The scriptorium-vault repo showed three empty
    /// commits in a row (85c5d8d, 211df25, 055e003) all following this
    /// pattern. Until this test fails, the fix is wrong.
    #[test]
    fn commits_modification_to_tracked_root_file() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();

        // Initial commit contains log.md with some initial content.
        let log_md = root.join("log.md");
        std::fs::write(&log_md, "# Log\n\nInitial entry.\n").unwrap();
        commit_paths(root, std::slice::from_ref(&log_md), "init").unwrap();

        // Append to log.md on disk (OS-level, no git involvement).
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&log_md)
                .unwrap();
            f.write_all(b"\n## appended\n\nNew entry appended after init.\n")
                .unwrap();
            f.sync_all().unwrap();
        }

        // Create an untracked file in a new subdirectory — this mirrors
        // the real ingest flow where `intern_source` writes a source
        // file to sources/articles/... outside the vault transaction.
        let articles_dir = root.join("sources").join("articles");
        std::fs::create_dir_all(&articles_dir).unwrap();
        std::fs::write(articles_dir.join("untracked.md"), "untracked\n").unwrap();

        // Commit ONLY log.md. The untracked sources/articles file is
        // deliberately NOT in the pathspec — it should stay untracked.
        let oid = commit_paths(root, &[log_md.clone()], "append to log").unwrap();

        let repo = Repository::open(root).unwrap();
        let commit = repo
            .find_commit(git2::Oid::from_str(&oid).unwrap())
            .unwrap();
        let parent = commit.parent(0).unwrap();

        assert_ne!(
            commit.tree_id(),
            parent.tree_id(),
            "commit tree matches parent tree — the log.md append was not staged \
             (empty commit). THIS IS THE BUG: index.add_all is not picking up \
             modifications to already-tracked files in this flow."
        );

        // The committed log.md must have the appended content.
        let tree = commit.tree().unwrap();
        let log_entry = tree.get_name("log.md").expect("log.md in tree");
        let blob = repo.find_blob(log_entry.id()).unwrap();
        let body = std::str::from_utf8(blob.content()).unwrap();
        assert!(
            body.contains("appended"),
            "committed log.md missing the appended content: {body:?}"
        );
    }
}
