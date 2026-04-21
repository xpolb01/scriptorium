//! Lint rule: compiled truth + timeline page structure.
//!
//! The recommended page structure has a "compiled truth" section above a
//! horizontal rule (`---`) and a `## Timeline` section below it. This rule
//! is advisory (Info severity) — it does not block commits and is
//! backward-compatible with existing pages.

use super::{LintIssue, Severity};
use crate::vault::Page;

pub const MISSING_TIMELINE: &str = "page_structure.missing_timeline";

/// Check all pages for the compiled-truth + timeline structure.
pub fn check(pages: &[Page]) -> Vec<LintIssue> {
    pages
        .iter()
        .filter(|p| !has_timeline_section(&p.body))
        .map(|p| LintIssue {
            severity: Severity::Info,
            rule: MISSING_TIMELINE.into(),
            page: Some(p.frontmatter.id),
            path: Some(p.path.clone()),
            message: "page lacks a ## Timeline section below a --- separator; \
                          consider adding one for provenance tracking"
                .into(),
        })
        .collect()
}

/// Check if the body contains a horizontal rule followed by a `## Timeline`
/// heading (anywhere after the rule).
fn has_timeline_section(body: &str) -> bool {
    // Look for a standalone `---` line (horizontal rule) in the body.
    let mut found_rule = false;
    for line in body.lines() {
        let trimmed = line.trim();
        if !found_rule {
            // A horizontal rule is a line with only 3+ hyphens (and optional spaces).
            if trimmed.len() >= 3 && trimmed.chars().all(|c| c == '-') {
                found_rule = true;
            }
        } else if trimmed.starts_with("## Timeline") {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn make_page(body: &str) -> Page {
        Page {
            path: Utf8PathBuf::from("wiki/test.md"),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: "Test".into(),
                created: Utc::now(),
                updated: Utc::now(),
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
    fn detects_missing_timeline() {
        let page = make_page("Some compiled truth content.\n\nMore content.\n");
        let issues = check(&[page]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].rule, MISSING_TIMELINE);
    }

    #[test]
    fn passes_with_timeline() {
        let page = make_page(
            "Compiled truth here.\n\n---\n\n## Timeline\n\n### 2026-04-10\n- Evidence.\n",
        );
        let issues = check(&[page]);
        assert!(issues.is_empty());
    }

    #[test]
    fn info_severity() {
        let page = make_page("No timeline.\n");
        let issues = check(&[page]);
        assert_eq!(issues.len(), 1);
        assert!(
            matches!(issues[0].severity, Severity::Info),
            "should be Info, not Warning or Error"
        );
    }
}
