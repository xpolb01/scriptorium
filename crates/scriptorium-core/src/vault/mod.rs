//! Vault layout, scanning, and (in later phases) transactional writes.
//!
//! A vault is an Obsidian-compatible directory:
//!
//! ```text
//! my-vault/
//! ├── .obsidian/        # Obsidian app data — never touched by us
//! ├── .scriptorium/     # our metadata (config, embeddings, lock, version)
//! ├── CLAUDE.md         # the schema
//! ├── index.md          # generated content catalog
//! ├── log.md            # append-only timeline
//! ├── sources/          # raw, immutable inputs
//! └── wiki/             # LLM-maintained markdown pages
//! ```

pub mod graph;
pub mod lock;
pub mod page;
pub mod patch;
pub mod tx;
pub mod wikilink;

use std::path::{Path, PathBuf};

use camino::{Utf8Path, Utf8PathBuf};

use crate::error::{Error, Result};

pub use graph::{LinkGraph, LinkRef, PageInfo, Resolution};
pub use page::{Frontmatter, Page, PageId, SCHEMA_VERSION};
pub use patch::{Patch, PatchError, PatchOp, PatchOutcome};
pub use tx::{Action, ChangeSummary, VaultTx};
pub use wikilink::Wikilink;

/// Default subdirectory names. Configurable later via [`crate::config::PathsConfig`].
pub mod dirs {
    pub const WIKI: &str = "wiki";
    pub const SOURCES: &str = "sources";
    pub const META: &str = ".scriptorium";
    pub const OBSIDIAN: &str = ".obsidian";
}

/// Standard top-level filenames.
pub mod files {
    pub const SCHEMA: &str = "CLAUDE.md";
    pub const INDEX: &str = "index.md";
    pub const LOG: &str = "log.md";
}

/// Handle to an on-disk vault. Cheap to clone (just a `Utf8PathBuf` underneath).
#[derive(Debug, Clone)]
pub struct Vault {
    root: Utf8PathBuf,
}

impl Vault {
    /// Open an existing vault. Errors if the directory does not exist.
    /// Does not create any files; use `init` (later) to scaffold.
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root_buf = root.as_ref().to_path_buf();
        let root = Utf8PathBuf::from_path_buf(root_buf.clone()).map_err(|_| non_utf8(&root_buf))?;
        if !root.is_dir() {
            return Err(Error::VaultMissing(root.into_std_path_buf()));
        }
        Ok(Self { root })
    }

    pub fn root(&self) -> &Utf8Path {
        &self.root
    }

    pub fn wiki_dir(&self) -> Utf8PathBuf {
        self.root.join(dirs::WIKI)
    }

    pub fn sources_dir(&self) -> Utf8PathBuf {
        self.root.join(dirs::SOURCES)
    }

    pub fn meta_dir(&self) -> Utf8PathBuf {
        self.root.join(dirs::META)
    }

    pub fn schema_path(&self) -> Utf8PathBuf {
        self.root.join(files::SCHEMA)
    }

    pub fn index_path(&self) -> Utf8PathBuf {
        self.root.join(files::INDEX)
    }

    pub fn log_path(&self) -> Utf8PathBuf {
        self.root.join(files::LOG)
    }

    /// Resolve a vault-relative path to its absolute on-disk location, ensuring
    /// the result still lives inside the vault root after normalization.
    /// Rejects absolute paths and `..` components.
    pub fn resolve(&self, rel: &Utf8Path) -> Result<Utf8PathBuf> {
        if rel.is_absolute() {
            return Err(Error::PathEscape(rel.as_std_path().to_path_buf()));
        }
        for component in rel.components() {
            if matches!(component, camino::Utf8Component::ParentDir) {
                return Err(Error::PathEscape(rel.as_std_path().to_path_buf()));
            }
        }
        Ok(self.root.join(rel))
    }

    /// Walk `wiki/` and parse every `.md` page. Pages that fail to parse are
    /// returned in [`ScanReport::errors`] rather than aborting the scan.
    pub fn scan(&self) -> Result<ScanReport> {
        let wiki = self.wiki_dir();
        let mut report = ScanReport::default();
        if !wiki.exists() {
            return Ok(report);
        }
        let walker = ignore::WalkBuilder::new(&wiki)
            .hidden(false)
            .git_ignore(true)
            .git_global(false)
            .git_exclude(false)
            .build();
        for entry in walker {
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    report.errors.push(ScanError {
                        path: wiki.clone().into_std_path_buf(),
                        message: e.to_string(),
                    });
                    continue;
                }
            };
            let Some(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_file() {
                continue;
            }
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("md") {
                continue;
            }
            match Page::read(&self.root, path) {
                Ok(page) => report.pages.push(page),
                Err(err) => report.errors.push(ScanError {
                    path: path.to_path_buf(),
                    message: err.to_string(),
                }),
            }
        }
        // Sort by relative path for deterministic ordering — important for
        // tests, log entries, and any consumer that diffs scans.
        report.pages.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(report)
    }
}

fn non_utf8(p: &Path) -> Error {
    Error::Io {
        path: p.to_path_buf(),
        source: std::io::Error::new(std::io::ErrorKind::InvalidData, "non-UTF-8 path"),
    }
}

/// Result of [`Vault::scan`]: parsed pages plus per-file parse errors.
#[derive(Debug, Default)]
pub struct ScanReport {
    pub pages: Vec<Page>,
    pub errors: Vec<ScanError>,
}

impl ScanReport {
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty() && self.errors.is_empty()
    }

    pub fn page_count(&self) -> usize {
        self.pages.len()
    }
}

#[derive(Debug)]
pub struct ScanError {
    pub path: PathBuf,
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(dir: &Path, rel: &str, content: &str) {
        let path = dir.join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, content).unwrap();
    }

    fn sample_page(title: &str) -> String {
        format!(
            r"---
id: 01ARZ3NDEKTSV4RRFFQ69G5FAV
title: {title}
created: 2026-04-06T12:00:00Z
updated: 2026-04-06T12:00:00Z
---
# {title}

Body.
"
        )
    }

    #[test]
    fn open_errors_when_missing() {
        let err = Vault::open("/nonexistent/scriptorium-test").unwrap_err();
        assert!(matches!(err, Error::VaultMissing(_)));
    }

    #[test]
    fn open_succeeds_for_existing_directory() {
        let dir = TempDir::new().unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        assert_eq!(vault.root().as_std_path(), dir.path());
    }

    #[test]
    fn scan_returns_empty_for_fresh_vault() {
        let dir = TempDir::new().unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        let report = vault.scan().unwrap();
        assert!(report.is_empty());
    }

    #[test]
    fn scan_walks_wiki_subdirectories() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "wiki/concepts/foo.md", &sample_page("Foo"));
        write(dir.path(), "wiki/entities/bar.md", &sample_page("Bar"));
        write(
            dir.path(),
            "wiki/topics/nested/deep.md",
            &sample_page("Deep"),
        );
        // Non-markdown should be ignored.
        write(dir.path(), "wiki/concepts/notes.txt", "ignore me");

        let vault = Vault::open(dir.path()).unwrap();
        let report = vault.scan().unwrap();
        assert_eq!(report.page_count(), 3);
        assert!(report.errors.is_empty());
        // Sorted ordering.
        let paths: Vec<_> = report.pages.iter().map(|p| p.path.as_str()).collect();
        assert_eq!(
            paths,
            vec![
                "wiki/concepts/foo.md",
                "wiki/entities/bar.md",
                "wiki/topics/nested/deep.md",
            ]
        );
    }

    #[test]
    fn scan_collects_parse_errors_without_aborting() {
        let dir = TempDir::new().unwrap();
        write(dir.path(), "wiki/good.md", &sample_page("Good"));
        write(dir.path(), "wiki/bad.md", "no frontmatter here\n");
        let vault = Vault::open(dir.path()).unwrap();
        let report = vault.scan().unwrap();
        assert_eq!(report.page_count(), 1);
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.pages[0].frontmatter.title, "Good");
    }

    #[test]
    fn resolve_rejects_parent_dir_escape() {
        let dir = TempDir::new().unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        let err = vault
            .resolve(Utf8Path::new("../../etc/passwd"))
            .unwrap_err();
        assert!(matches!(err, Error::PathEscape(_)));
    }

    #[test]
    fn resolve_rejects_absolute_path() {
        let dir = TempDir::new().unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        let err = vault.resolve(Utf8Path::new("/etc/passwd")).unwrap_err();
        assert!(matches!(err, Error::PathEscape(_)));
    }

    #[test]
    fn resolve_joins_relative_path() {
        let dir = TempDir::new().unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        let resolved = vault
            .resolve(Utf8Path::new("wiki/concepts/foo.md"))
            .unwrap();
        assert_eq!(
            resolved.as_std_path(),
            dir.path().join("wiki/concepts/foo.md").as_path()
        );
    }
}
