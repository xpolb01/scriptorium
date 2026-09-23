//! Read-only views of the repository: one git revision, or the working tree.
//!
//! Every ontology operation reads through a [`Snapshot`]. The git
//! implementation reads blobs straight from the object database, so it sees
//! exactly what was committed at that revision — independent of the working
//! tree, the index, or whichever branch is checked out. The worktree
//! implementation exists for the edit loop (`--worktree`), before changes
//! are committed.
//!
//! Paths are repository-relative and `/`-separated.

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use git2::{ErrorCode, ObjectType, Oid, Repository, StatusOptions};

use crate::error::{Error, Result};

/// One entry of a directory listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
    /// A symlink, whatever it points at (never followed).
    pub is_symlink: bool,
}

/// A read-only view of repository contents.
pub trait Snapshot: Send + Sync {
    /// Read a file. `Ok(None)` when it does not exist (or is a directory).
    fn read(&self, path: &str) -> Result<Option<Vec<u8>>>;

    /// List the immediate children of a directory, sorted by name. Empty
    /// when the directory does not exist.
    fn list(&self, dir: &str) -> Result<Vec<DirEntry>>;

    /// Human-readable description: `HEAD@1a2b3c4` or `worktree`.
    fn label(&self) -> String;

    /// The commit this snapshot reads, when it reads one.
    fn commit(&self) -> Option<String>;

    /// Read a file as UTF-8 text.
    fn read_string(&self, path: &str) -> Result<Option<String>> {
        match self.read(path)? {
            None => Ok(None),
            Some(bytes) => String::from_utf8(bytes)
                .map(Some)
                .map_err(|e| Error::Invalid {
                    what: "utf-8 file",
                    path: path.to_string(),
                    message: e.to_string(),
                }),
        }
    }
}

/// Which view of the repository to read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotSpec {
    /// A git revision: branch, tag, sha, `HEAD~2`, …
    Rev(String),
    /// Uncommitted files in the working tree.
    Worktree,
}

impl SnapshotSpec {
    /// Open the snapshot for the repository at `repo_root`.
    pub fn open(&self, repo_root: &Path) -> Result<Box<dyn Snapshot>> {
        Ok(match self {
            Self::Rev(rev) => Box::new(GitSnapshot::open(repo_root, rev)?),
            Self::Worktree => Box::new(WorktreeSnapshot::new(repo_root)),
        })
    }
}

/// Files as committed at one git revision.
pub struct GitSnapshot {
    repo: Mutex<Repository>,
    commit: Oid,
    tree: Oid,
    rev: String,
}

impl GitSnapshot {
    /// Resolve `rev` in the repository whose working directory is
    /// `repo_root` (the ontology paths are relative to it).
    pub fn open(repo_root: &Path, rev: &str) -> Result<Self> {
        let repo = Repository::open(repo_root)?;
        let object = repo.revparse_single(rev).map_err(|e| Error::Revision {
            rev: rev.to_string(),
            message: e.message().to_string(),
        })?;
        let commit = object.peel_to_commit().map_err(|e| Error::Revision {
            rev: rev.to_string(),
            message: e.message().to_string(),
        })?;
        let (commit_id, tree_id) = (commit.id(), commit.tree_id());
        drop(commit);
        drop(object);
        Ok(Self {
            repo: Mutex::new(repo),
            commit: commit_id,
            tree: tree_id,
            rev: rev.to_string(),
        })
    }

    fn with_repo<T>(&self, f: impl FnOnce(&Repository) -> Result<T>) -> Result<T> {
        let repo = self
            .repo
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&repo)
    }
}

impl Snapshot for GitSnapshot {
    fn read(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.with_repo(|repo| {
            let tree = repo.find_tree(self.tree)?;
            let entry = match tree.get_path(Path::new(path)) {
                Ok(entry) => entry,
                Err(e) if e.code() == ErrorCode::NotFound => return Ok(None),
                Err(e) => return Err(e.into()),
            };
            if entry.kind() != Some(ObjectType::Blob) {
                return Ok(None);
            }
            // A symlink's blob is its target path, not the file it names.
            if entry.filemode() == 0o120_000 {
                return Err(linked(path));
            }
            Ok(Some(repo.find_blob(entry.id())?.content().to_vec()))
        })
    }

    fn list(&self, dir: &str) -> Result<Vec<DirEntry>> {
        self.with_repo(|repo| {
            let root = repo.find_tree(self.tree)?;
            let tree = if dir.trim_matches('/').is_empty() {
                root
            } else {
                match root.get_path(Path::new(dir.trim_matches('/'))) {
                    Ok(entry) if entry.kind() == Some(ObjectType::Tree) => {
                        repo.find_tree(entry.id())?
                    }
                    Ok(_) => return Ok(Vec::new()),
                    Err(e) if e.code() == ErrorCode::NotFound => return Ok(Vec::new()),
                    Err(e) => return Err(e.into()),
                }
            };
            let mut entries = Vec::new();
            for entry in &tree {
                // Paths are text everywhere else, so a name that is not
                // cannot be skipped quietly: its record would vanish.
                let Ok(name) = entry.name() else {
                    return Err(unnamed(dir, &String::from_utf8_lossy(entry.name_bytes())));
                };
                entries.push(DirEntry {
                    name: name.to_string(),
                    is_dir: entry.kind() == Some(ObjectType::Tree),
                    is_symlink: entry.filemode() == 0o120_000,
                });
            }
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(entries)
        })
    }

    fn label(&self) -> String {
        let sha = self.commit.to_string();
        format!("{}@{}", self.rev, &sha[..7.min(sha.len())])
    }

    fn commit(&self) -> Option<String> {
        Some(self.commit.to_string())
    }
}

/// Files as they are on disk right now, committed or not.
///
/// Reads are confined to the repository: a path that resolves outside it —
/// lexically (`..`) or through a symlink — is an error, so a symlinked
/// schema or record cannot expose files from elsewhere on the machine.
pub struct WorktreeSnapshot {
    root: PathBuf,
}

impl WorktreeSnapshot {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn resolve(&self, path: &str) -> Result<PathBuf> {
        let escapes = |message: &str| Error::Invalid {
            what: "path",
            path: path.to_string(),
            message: message.to_string(),
        };
        let rel = Path::new(path.trim_matches('/'));
        if rel.is_absolute()
            || rel
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Err(escapes("must be relative and stay inside the repository"));
        }
        let full = self.root.join(rel);
        match full.canonicalize() {
            Ok(resolved) => {
                let root = self
                    .root
                    .canonicalize()
                    .map_err(|e| Error::io(self.root.display().to_string(), e))?;
                if resolved.starts_with(&root) {
                    Ok(resolved)
                } else {
                    Err(escapes(
                        "resolves (through a symlink) outside the repository",
                    ))
                }
            }
            // Missing paths read as absent.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(full),
            Err(e) => Err(Error::io(full.display().to_string(), e)),
        }
    }
}

impl Snapshot for WorktreeSnapshot {
    fn read(&self, path: &str) -> Result<Option<Vec<u8>>> {
        // A commit stores a symlink, not what it points at: reading through
        // one here would check what `HEAD` would not serve.
        let mut prefix = self.root.clone();
        for part in path.split('/').filter(|p| !p.is_empty()) {
            prefix.push(part);
            if prefix
                .symlink_metadata()
                .is_ok_and(|meta| meta.file_type().is_symlink())
            {
                return Err(linked(path));
            }
        }
        let full = self.resolve(path)?;
        if full.is_dir() {
            return Ok(None);
        }
        match std::fs::read(&full) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::io(full.display().to_string(), e)),
        }
    }

    fn list(&self, dir: &str) -> Result<Vec<DirEntry>> {
        let full = self.resolve(dir)?;
        let read = match std::fs::read_dir(&full) {
            Ok(read) => read,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::io(full.display().to_string(), e)),
        };
        let mut entries = Vec::new();
        for entry in read {
            let entry = entry.map_err(|e| Error::io(full.display().to_string(), e))?;
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(raw) => return Err(unnamed(dir, &raw.to_string_lossy())),
            };
            // Mirror git: never descend into the repository metadata.
            if name == ".git" {
                continue;
            }
            let file_type = entry.file_type().ok();
            entries.push(DirEntry {
                name,
                is_dir: file_type.is_some_and(|t| t.is_dir()),
                is_symlink: file_type.is_some_and(|t| t.is_symlink()),
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(entries)
    }

    fn label(&self) -> String {
        "worktree".to_string()
    }

    fn commit(&self) -> Option<String> {
        None
    }
}

/// The error for reading through a symlink.
fn linked(path: &str) -> Error {
    Error::Invalid {
        what: "file",
        path: path.to_string(),
        message: "is (or is under) a symlink; git stores the link, not the file, so a commit would read the link in its place; replace it with the file"
            .to_string(),
    }
}

/// The error for a file name that is not UTF-8 (shown lossily).
fn unnamed(dir: &str, lossy: &str) -> Error {
    Error::Invalid {
        what: "file name",
        path: crate::layout::join(dir, lossy),
        message: "is not valid UTF-8, so no record or schema can be read under it; rename it"
            .to_string(),
    }
}

/// True when `dir` has uncommitted changes (modified, staged, or untracked
/// files) relative to `HEAD`. Used to warn that a revision read does not
/// include work in progress.
pub fn has_uncommitted_changes(repo_root: &Path, dir: &str) -> Result<bool> {
    let repo = Repository::open(repo_root)?;
    let mut opts = StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .pathspec(dir);
    let dirty = !repo.statuses(Some(&mut opts))?.is_empty();
    Ok(dirty)
}

/// The working-directory root of the git repository containing `start`,
/// if there is one.
pub fn discover_repo_root(start: &Path) -> Option<PathBuf> {
    Repository::discover(start)
        .ok()
        .and_then(|repo| repo.workdir().map(Path::to_path_buf))
}

/// Stage `paths` (repository-relative files) and commit them on the current
/// branch, initializing the repository (branch `main`) first if needed.
/// Returns the new commit id. The commit contains the whole index, so
/// callers check [`has_staged_changes`] before calling this.
pub fn commit_paths(repo_root: &Path, paths: &[PathBuf], message: &str) -> Result<String> {
    let repo = open_or_init(repo_root)?;
    let parent = repo.head().ok().and_then(|head| head.peel_to_commit().ok());
    commit_index(&repo, paths, message, parent.as_ref())
}

/// [`commit_paths`], but only onto `parent`: the commit (`None`: no commit
/// yet) the caller checked its change against. If HEAD is anything else by
/// then, nothing is committed; the ref update itself also requires `parent`
/// to still be HEAD.
pub fn commit_paths_onto(
    repo_root: &Path,
    paths: &[PathBuf],
    message: &str,
    parent: Option<&str>,
) -> Result<String> {
    let repo = open_or_init(repo_root)?;
    let parent = parent
        .map(|id| repo.find_commit(Oid::from_str(id)?))
        .transpose()?;
    let head = repo
        .head()
        .ok()
        .and_then(|head| head.peel_to_commit().ok())
        .map(|commit| commit.id());
    if head != parent.as_ref().map(git2::Commit::id) {
        let short = |id: Option<Oid>| {
            id.map_or_else(
                || "(no commit)".to_string(),
                |id| id.to_string()[..7].to_string(),
            )
        };
        return Err(git2::Error::from_str(&format!(
            "HEAD moved to {} since {}; nothing committed",
            short(head),
            short(parent.as_ref().map(git2::Commit::id))
        ))
        .into());
    }
    commit_index(&repo, paths, message, parent.as_ref())
}

fn open_or_init(repo_root: &Path) -> Result<Repository> {
    match Repository::open(repo_root) {
        Ok(repo) => Ok(repo),
        Err(e) if e.code() == ErrorCode::NotFound => {
            let mut opts = git2::RepositoryInitOptions::new();
            opts.initial_head("main");
            Ok(Repository::init_opts(repo_root, &opts)?)
        }
        Err(e) => Err(e.into()),
    }
}

/// Stage `paths` and commit the index on top of `parent`, moving HEAD.
fn commit_index(
    repo: &Repository,
    paths: &[PathBuf],
    message: &str,
    parent: Option<&git2::Commit<'_>>,
) -> Result<String> {
    let mut index = repo.index()?;
    for path in paths {
        index.add_path(path)?;
    }
    index.write()?;
    let tree = repo.find_tree(index.write_tree()?)?;
    let signature = repo
        .signature()
        .or_else(|_| git2::Signature::now("scriptorium", "noreply@scriptorium"))?;
    let parents: Vec<&git2::Commit<'_>> = parent.into_iter().collect();
    let oid = repo.commit(
        Some("HEAD"),
        &signature,
        &signature,
        message,
        &tree,
        &parents,
    )?;
    Ok(oid.to_string())
}

/// Reset the index entries for `paths` to HEAD: undoes a [`commit_paths`]
/// that staged them but could not commit.
pub fn unstage_paths(repo_root: &Path, paths: &[PathBuf]) -> Result<()> {
    let repo = Repository::open(repo_root)?;
    let head = repo.head()?.peel(ObjectType::Commit)?;
    repo.reset_default(Some(&head), paths.iter().map(PathBuf::as_path))?;
    Ok(())
}

/// True when the index holds staged changes. A commit made now would
/// include them, so callers that commit their own paths check this first.
/// A directory that is not a repository has nothing staged.
pub fn has_staged_changes(repo_root: &Path) -> Result<bool> {
    let repo = match Repository::open(repo_root) {
        Ok(repo) => repo,
        Err(e) if e.code() == ErrorCode::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let staged = git2::Status::INDEX_NEW
        | git2::Status::INDEX_MODIFIED
        | git2::Status::INDEX_DELETED
        | git2::Status::INDEX_RENAMED
        | git2::Status::INDEX_TYPECHANGE;
    let any = repo
        .statuses(None)?
        .iter()
        .any(|entry| entry.status().intersects(staged));
    Ok(any)
}

#[cfg(test)]
pub(crate) mod test_support {
    //! Build throwaway git repositories for tests.

    use std::path::Path;

    use git2::{Repository, Signature};

    /// Write `files` into the working tree of `repo_root` and commit them all.
    /// Returns the new commit id.
    pub fn commit_files(repo_root: &Path, files: &[(&str, &str)], message: &str) -> String {
        let repo = Repository::open(repo_root).unwrap_or_else(|_| {
            let mut opts = git2::RepositoryInitOptions::new();
            opts.initial_head("main");
            Repository::init_opts(repo_root, &opts).unwrap()
        });
        for (path, contents) in files {
            let full = repo_root.join(path);
            std::fs::create_dir_all(full.parent().unwrap()).unwrap();
            std::fs::write(&full, contents).unwrap();
        }
        let mut index = repo.index().unwrap();
        index
            .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
            .unwrap();
        index.update_all(["*"].iter(), None).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = Signature::now("test", "test@example.com").unwrap();
        let parent = repo.head().ok().and_then(|h| h.peel_to_commit().ok());
        let parents: Vec<&git2::Commit> = parent.iter().collect();
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &parents)
            .unwrap()
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::commit_files;
    use super::*;

    #[cfg(unix)]
    #[test]
    fn names_that_are_not_utf8_are_errors_not_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        commit_files(
            root,
            &[
                ("ontology/core/subgraph.toml", "[storage]\nkind = \"git\"\n"),
                (
                    "ontology/core/schema.graphql",
                    "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\"])\ntype Person @key(fields: \"id\") { id: ID! name: String }\ntype Query { person(id: ID!): Person }\n",
                ),
                ("ontology/core/data/Person/ok.yaml", ""),
            ],
            "init",
        );
        // Git allows any bytes in a name; commit one that is not UTF-8.
        let repo = Repository::open(root).unwrap();
        let mut index = repo.index().unwrap();
        let entry = git2::IndexEntry {
            ctime: git2::IndexTime::new(0, 0),
            mtime: git2::IndexTime::new(0, 0),
            dev: 0,
            ino: 0,
            mode: 0o100_644,
            uid: 0,
            gid: 0,
            file_size: 0,
            id: Oid::ZERO_SHA1,
            flags: 0,
            flags_extended: 0,
            path: b"ontology/core/data/Person/\xff.yaml".to_vec(),
        };
        index.add_frombuffer(&entry, b"name: X\n").unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "odd name", &tree, &[&head])
            .unwrap();

        let snapshot = GitSnapshot::open(root, "HEAD").unwrap();
        let err = snapshot
            .list("ontology/core/data/Person")
            .unwrap_err()
            .to_string();
        assert!(err.contains("not valid UTF-8"), "{err}");
        // `check` (and so `serve`, which needs a clean check) refuses it.
        let err = crate::check::check(&snapshot, "ontology")
            .map(|report| format!("{:?}", report.issues))
            .unwrap_err()
            .to_string();
        assert!(err.contains("not valid UTF-8"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_files_are_errors_in_both_views() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        commit_files(root, &[("a.txt", "real"), ("d/x.txt", "inside")], "init");
        std::os::unix::fs::symlink("a.txt", root.join("b.txt")).unwrap();
        std::os::unix::fs::symlink("d", root.join("e")).unwrap();
        let worktree = WorktreeSnapshot::new(root);
        assert_eq!(worktree.read("a.txt").unwrap().unwrap(), b"real");
        for path in ["b.txt", "e/x.txt"] {
            let err = worktree.read(path).unwrap_err().to_string();
            assert!(err.contains("symlink"), "{path}: {err}");
        }
        // Committed, the link is a blob holding its target's path.
        commit_files(root, &[], "links");
        let repo = Repository::open(root).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("b.txt")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "link", &tree, &[&parent])
            .unwrap();
        let head = GitSnapshot::open(root, "HEAD").unwrap();
        let err = head.read("b.txt").unwrap_err().to_string();
        assert!(err.contains("symlink"), "{err}");
        assert_eq!(head.read("a.txt").unwrap().unwrap(), b"real");
    }

    #[test]
    fn commits_land_only_on_the_expected_parent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let first = commit_files(root, &[("a.txt", "1")], "first");
        let second = commit_files(root, &[("a.txt", "2")], "second");
        std::fs::write(root.join("b.txt"), "new").unwrap();
        let paths = [PathBuf::from("b.txt")];

        // Checked against `first`, but HEAD moved on: nothing committed or staged.
        let err = commit_paths_onto(root, &paths, "late", Some(&first)).unwrap_err();
        assert!(err.to_string().contains("HEAD moved"), "{err}");
        let repo = Repository::open(root).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        assert_eq!(head.id().to_string(), second);
        assert!(
            repo.index()
                .unwrap()
                .get_path(Path::new("b.txt"), 0)
                .is_none(),
            "the index is untouched"
        );

        let third = commit_paths_onto(root, &paths, "on time", Some(&second)).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        assert_eq!(head.id().to_string(), third);
        assert_eq!(head.parent_id(0).unwrap().to_string(), second);
    }

    #[test]
    fn git_snapshot_reads_committed_state_not_worktree() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        commit_files(root, &[("ontology/core/schema.graphql", "v1")], "v1");
        commit_files(root, &[("ontology/core/schema.graphql", "v2")], "v2");
        std::fs::write(root.join("ontology/core/schema.graphql"), "uncommitted").unwrap();

        let head = GitSnapshot::open(root, "HEAD").unwrap();
        let prev = GitSnapshot::open(root, "HEAD~1").unwrap();
        let wt = WorktreeSnapshot::new(root);

        let path = "ontology/core/schema.graphql";
        assert_eq!(head.read_string(path).unwrap().as_deref(), Some("v2"));
        assert_eq!(prev.read_string(path).unwrap().as_deref(), Some("v1"));
        assert_eq!(
            wt.read_string(path).unwrap().as_deref(),
            Some("uncommitted")
        );
        assert!(head.label().starts_with("HEAD@"));
        assert!(has_uncommitted_changes(root, "ontology").unwrap());
    }

    #[test]
    fn listing_and_missing_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        commit_files(
            root,
            &[
                ("ontology/core/schema.graphql", "x"),
                ("ontology/research/schema.graphql", "y"),
                ("ontology/ontology.toml", ""),
            ],
            "init",
        );
        for snap in [
            Box::new(GitSnapshot::open(root, "HEAD").unwrap()) as Box<dyn Snapshot>,
            Box::new(WorktreeSnapshot::new(root)),
        ] {
            let names: Vec<(String, bool)> = snap
                .list("ontology")
                .unwrap()
                .into_iter()
                .map(|e| (e.name, e.is_dir))
                .collect();
            assert_eq!(
                names,
                vec![
                    ("core".to_string(), true),
                    ("ontology.toml".to_string(), false),
                    ("research".to_string(), true),
                ]
            );
            assert!(snap.list("nope").unwrap().is_empty());
            assert!(snap.read("ontology/missing.graphql").unwrap().is_none());
            assert!(
                snap.read("ontology/core").unwrap().is_none(),
                "dir is not a file"
            );
        }
    }

    #[test]
    fn unknown_revision_is_a_revision_error() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(dir.path(), &[("a.txt", "a")], "init");
        let err = GitSnapshot::open(dir.path(), "no-such-branch")
            .err()
            .unwrap();
        assert!(matches!(err, Error::Revision { .. }), "got {err:?}");
    }

    #[test]
    fn worktree_rejects_escaping_paths() {
        let snap = WorktreeSnapshot::new("/tmp");
        assert!(snap.read("../etc/passwd").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn worktree_reads_refuse_symlinks() {
        let repo = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.txt"), "secret").unwrap();
        std::fs::create_dir_all(repo.path().join("ontology/core")).unwrap();
        std::fs::write(repo.path().join("ontology/core/inside.txt"), "ok").unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("secret.txt"),
            repo.path().join("ontology/core/schema.graphql"),
        )
        .unwrap();
        std::os::unix::fs::symlink(
            repo.path().join("ontology/core/inside.txt"),
            repo.path().join("ontology/core/alias.txt"),
        )
        .unwrap();
        std::os::unix::fs::symlink(outside.path(), repo.path().join("ontology/elsewhere")).unwrap();

        let snap = WorktreeSnapshot::new(repo.path());
        let err = snap
            .read("ontology/core/schema.graphql")
            .unwrap_err()
            .to_string();
        assert!(err.contains("symlink"), "{err}");
        assert!(snap.list("ontology/elsewhere").is_err());
        // Inside the repository too: a commit would store the link instead.
        let err = snap
            .read("ontology/core/alias.txt")
            .unwrap_err()
            .to_string();
        assert!(err.contains("symlink"), "{err}");
        assert_eq!(
            snap.read_string("ontology/core/inside.txt")
                .unwrap()
                .as_deref(),
            Some("ok")
        );
    }

    #[test]
    fn discovers_the_enclosing_repository() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(dir.path(), &[("a/b/c.txt", "x")], "init");
        let root = discover_repo_root(&dir.path().join("a/b")).unwrap();
        assert_eq!(
            root.canonicalize().unwrap(),
            dir.path().canonicalize().unwrap()
        );
    }
}
