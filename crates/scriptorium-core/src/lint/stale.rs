//! Stale page detection: flags wiki pages whose source material has been
//! updated more recently than the page itself.
//!
//! A page is "stale" when any file listed in `frontmatter.sources` has a
//! filesystem modification time newer than `frontmatter.updated`. This
//! signals that new evidence exists but hasn't been synthesized into the
//! page yet — a maintenance action is needed.
//!
//! Reference: `GBrain`'s "stale alert" system flags search results where
//! `compiled_truth` is older than the latest `timeline_entry`.

use std::time::SystemTime;

use chrono::{DateTime, Utc};

use super::LintIssue;
use crate::vault::{Page, Vault};

pub const STALE_PAGE: &str = "stale_page";

/// Check all pages for staleness against their source files.
pub fn check(vault: &Vault, pages: &[Page]) -> Vec<LintIssue> {
    let mut issues = Vec::new();
    for page in pages {
        if let Some(newest_source) = newest_source_mtime(vault, page) {
            if newest_source > page.frontmatter.updated {
                issues.push(
                    LintIssue::warning(
                        STALE_PAGE,
                        format!(
                            "page was last updated {} but has a source modified {}; \
                             the page may need re-ingestion to incorporate new material",
                            page.frontmatter.updated.format("%Y-%m-%d"),
                            newest_source.format("%Y-%m-%d %H:%M"),
                        ),
                    )
                    .at(page.frontmatter.id, page.path.clone()),
                );
            }
        }
    }
    issues
}

/// Check whether a single page is stale. Used by MCP tools and query
/// results to annotate individual hits without running the full lint.
pub fn is_stale(vault: &Vault, page: &Page) -> bool {
    newest_source_mtime(vault, page)
        .is_some_and(|source_time| source_time > page.frontmatter.updated)
}

/// Find the most recent modification time among a page's sources.
/// Returns `None` if the page has no sources, or if none of them can be
/// stat'd (missing file, permission error, platform limitation).
fn newest_source_mtime(vault: &Vault, page: &Page) -> Option<DateTime<Utc>> {
    page.frontmatter
        .sources
        .iter()
        .filter_map(|source_ref| {
            let path = vault.root().join(source_ref);
            let meta = std::fs::metadata(path.as_std_path()).ok()?;
            let mtime: SystemTime = meta.modified().ok()?;
            let dt: DateTime<Utc> = mtime.into();
            Some(dt)
        })
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lint::Severity;
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    fn make_page(stem: &str, updated: DateTime<Utc>, sources: Vec<String>) -> Page {
        Page {
            path: Utf8PathBuf::from(format!("wiki/{stem}.md")),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: stem.into(),
                created: Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
                updated,
                sources,
                tags: vec![],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: "body".into(),
        }
    }

    #[test]
    fn stale_detects_newer_source() {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::open(dir.path()).unwrap();

        // Create a source file (its mtime is "now").
        let sources_dir = dir.path().join("sources");
        std::fs::create_dir_all(&sources_dir).unwrap();
        let source_path = sources_dir.join("article.md");
        std::fs::write(&source_path, "new content").unwrap();

        // Page was "updated" in the past.
        let old_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let page = make_page("test", old_time, vec!["sources/article.md".into()]);

        assert!(is_stale(&vault, &page), "page should be stale");
        let issues = check(&vault, &[page]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].rule, STALE_PAGE);
        assert!(matches!(issues[0].severity, Severity::Warning));
    }

    #[test]
    fn not_stale_when_source_older() {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::open(dir.path()).unwrap();

        let sources_dir = dir.path().join("sources");
        std::fs::create_dir_all(&sources_dir).unwrap();
        std::fs::write(sources_dir.join("old.md"), "old").unwrap();

        // Page updated "now" — newer than the source file.
        let page = make_page("test", Utc::now(), vec!["sources/old.md".into()]);
        assert!(!is_stale(&vault, &page));
    }

    #[test]
    fn not_stale_when_no_sources() {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        let page = make_page("test", Utc::now(), vec![]);
        assert!(!is_stale(&vault, &page));
    }

    #[test]
    fn stale_graceful_on_missing_source_file() {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        // Source doesn't exist on disk — should not crash, not flag as stale.
        let page = make_page(
            "test",
            Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            vec!["sources/nonexistent.md".into()],
        );
        assert!(!is_stale(&vault, &page));
    }

    #[test]
    fn stale_lint_issue_severity_is_warning() {
        let dir = tempfile::tempdir().unwrap();
        let vault = Vault::open(dir.path()).unwrap();

        let sources_dir = dir.path().join("sources");
        std::fs::create_dir_all(&sources_dir).unwrap();
        std::fs::write(sources_dir.join("new.md"), "content").unwrap();

        let page = make_page(
            "test",
            Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
            vec!["sources/new.md".into()],
        );
        let issues = check(&vault, &[page]);
        assert_eq!(issues.len(), 1);
        assert!(matches!(issues[0].severity, Severity::Warning));
    }
}
