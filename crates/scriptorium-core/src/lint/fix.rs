//! Auto-fix mode for the mechanical lint rules.
//!
//! Most lint issues require human or LLM judgment to resolve — broken links
//! could be typos or planned pages, orphans might be intentional, duplicate
//! IDs need a human to decide which is canonical. Only one rule has an
//! unambiguously safe mechanical fix:
//!
//! - **`frontmatter.bad_timestamps`** — set `updated = max(updated, created)`.
//!   Bumps `updated` forward to satisfy `updated >= created` without
//!   pretending the page was edited more recently than it actually was.
//!
//! Everything else is reported as "skipped" with a one-line reason so the
//! caller can show the user what wasn't touched and why.
//!
//! The fix runs as a single [`VaultTx`](crate::vault::VaultTx) commit so
//! either all fixes land or none do. The commit message names the fix mode
//! and the count.

use camino::Utf8Path;
use chrono::DateTime;

use super::{LintIssue, LintReport};
use crate::error::Result;
use crate::vault::Vault;

/// Outcome of a [`run`] invocation.
#[derive(Debug)]
pub struct FixReport {
    /// Issues this run resolved. Each one is the original [`LintIssue`]
    /// from the input report, copied verbatim.
    pub fixed: Vec<LintIssue>,
    /// Issues this run did not resolve, paired with a one-line reason
    /// (e.g. `"rule not auto-fixable"`, `"page not found in scan"`).
    pub skipped: Vec<(LintIssue, &'static str)>,
    /// Git commit id for the fix commit, or `None` if nothing was fixed
    /// (no commit is created on a no-op run).
    pub commit_id: Option<String>,
}

impl FixReport {
    #[must_use]
    pub fn is_noop(&self) -> bool {
        self.fixed.is_empty()
    }
}

/// Walk a fresh lint report, apply safe mechanical fixes inside a single
/// [`VaultTx`](crate::vault::VaultTx), and return the [`FixReport`].
///
/// The function runs `lint::run` itself rather than taking a precomputed
/// report so the caller can't accidentally pass a stale one whose pages
/// have already drifted on disk.
pub fn run(vault: &Vault) -> Result<FixReport> {
    let report = super::run(vault)?;
    let scan = vault.scan()?;

    let mut fixed: Vec<LintIssue> = Vec::new();
    let mut skipped: Vec<(LintIssue, &'static str)> = Vec::new();
    let mut tx = vault.begin();

    for issue in &report.issues {
        match issue.rule.as_str() {
            super::frontmatter::BAD_TIMESTAMPS => {
                let Some(path) = &issue.path else {
                    skipped.push((issue.clone(), "issue has no associated path"));
                    continue;
                };
                let Some(page) = scan.pages.iter().find(|p| &p.path == path) else {
                    skipped.push((issue.clone(), "page not found in fresh scan"));
                    continue;
                };
                let mut next = page.clone();
                let bumped: DateTime<_> =
                    next.frontmatter.created.max(next.frontmatter.updated);
                next.frontmatter.updated = bumped;
                tx.write_page(&next)?;
                fixed.push(issue.clone());
            }
            _ => {
                skipped.push((issue.clone(), "rule not auto-fixable"));
            }
        }
    }

    let commit_id = if tx.is_empty() {
        None
    } else {
        let msg = format!("[lint --fix] fixed {} issue(s)", fixed.len());
        Some(tx.commit(&msg)?)
    };

    Ok(FixReport {
        fixed,
        skipped,
        commit_id,
    })
}

/// Convenience: take an already-built [`LintReport`] and report what would
/// be fixable without touching disk. Used by callers that want to preview
/// a fix before invoking [`run`].
#[must_use]
pub fn classify(report: &LintReport) -> (usize, usize) {
    let mut fixable = 0usize;
    let mut unfixable = 0usize;
    for issue in &report.issues {
        if issue.rule == super::frontmatter::BAD_TIMESTAMPS && issue.path.is_some() {
            fixable += 1;
        } else {
            unfixable += 1;
        }
    }
    (fixable, unfixable)
}

// Suppress an unused-import warning for `Utf8Path` — we don't reference it
// directly here, but downstream test code (in the tests module below) needs
// it via the wildcard re-export. Keeping the import in the parent scope is
// simpler than gating the test imports.
#[allow(dead_code)]
fn _force_path_import(_p: &Utf8Path) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, Page, PageId};
    use camino::Utf8PathBuf;
    use chrono::{Duration, TimeZone, Utc};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn fresh_vault() -> (TempDir, Vault) {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        (dir, vault)
    }

    fn make_page(stem: &str, title: &str) -> Page {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from(format!("wiki/{stem}.md")),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: title.into(),
                created: now,
                updated: now,
                sources: vec![],
                tags: vec!["entity".into()],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: "self-contained body without wikilinks\n".into(),
        }
    }

    #[test]
    fn run_fixes_bad_timestamps() {
        let (_dir, vault) = fresh_vault();
        // Stage a page with `updated < created`. The lint rule reports it
        // as a warning (not an error), so commit() is allowed to land it.
        let mut page = make_page("foo", "Foo");
        let created = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        page.frontmatter.created = created;
        page.frontmatter.updated = created - Duration::days(1);
        let mut tx = vault.begin();
        tx.write_page(&page).unwrap();
        tx.commit("seed").unwrap();

        // Sanity: the lint report sees the bad timestamp.
        let pre = super::super::run(&vault).unwrap();
        assert!(pre
            .issues
            .iter()
            .any(|i| i.rule == super::super::frontmatter::BAD_TIMESTAMPS));

        let report = run(&vault).unwrap();
        assert_eq!(report.fixed.len(), 1);
        assert!(report.commit_id.is_some());

        // Re-scan: the page now has updated == created.
        let scan = vault.scan().unwrap();
        let p = scan
            .pages
            .iter()
            .find(|p| p.path == page.path)
            .expect("page should still exist");
        assert_eq!(p.frontmatter.updated, p.frontmatter.created);

        // The bad-timestamps issue is gone from a fresh lint.
        let post = super::super::run(&vault).unwrap();
        assert!(!post
            .issues
            .iter()
            .any(|i| i.rule == super::super::frontmatter::BAD_TIMESTAMPS));
    }

    #[test]
    fn run_skips_unfixable_rules() {
        let (_dir, vault) = fresh_vault();
        // A page with a broken wikilink — the broken_link rule fires but
        // there's no auto-fix for it.
        let mut page = make_page("foo", "Foo");
        page.body = "links to [[ghost]]\n".into();
        let mut tx = vault.begin();
        tx.write_page(&page).unwrap();
        // commit() would block on the broken link error; bypass validation.
        tx.commit_without_validation("seed broken link").unwrap();

        let report = run(&vault).unwrap();
        assert!(report.is_noop(), "no fixable issues, expected no-op");
        assert!(report.commit_id.is_none(), "no commit on no-op");
        assert!(!report.skipped.is_empty(), "the broken link should appear as skipped");
        assert!(report
            .skipped
            .iter()
            .any(|(i, _)| i.rule == super::super::broken_links::BROKEN));
    }

    #[test]
    fn run_on_clean_vault_is_noop() {
        let (_dir, vault) = fresh_vault();
        let report = run(&vault).unwrap();
        assert!(report.is_noop());
        assert!(report.skipped.is_empty());
        assert!(report.commit_id.is_none());
    }

    #[test]
    fn classify_counts_fixable_vs_unfixable() {
        let report = LintReport {
            issues: vec![
                LintIssue {
                    severity: super::super::Severity::Warning,
                    rule: super::super::frontmatter::BAD_TIMESTAMPS.into(),
                    page: None,
                    path: Some(Utf8PathBuf::from("wiki/foo.md")),
                    message: "bad".into(),
                },
                LintIssue {
                    severity: super::super::Severity::Error,
                    rule: super::super::broken_links::BROKEN.into(),
                    page: None,
                    path: Some(Utf8PathBuf::from("wiki/bar.md")),
                    message: "bad link".into(),
                },
            ],
        };
        let (fixable, unfixable) = classify(&report);
        assert_eq!(fixable, 1);
        assert_eq!(unfixable, 1);
    }
}
