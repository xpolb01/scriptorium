//! Frontmatter validity rules. Checks that complement the type system — the
//! struct guarantees every required field is present, but it cannot enforce
//! things like "title is non-empty" or "`schema_version` is supported".

use std::collections::HashMap;

use super::{LintIssue, Severity};
use crate::vault::{stem::normalize_stem, Page, SCHEMA_VERSION};

pub const EMPTY_TITLE: &str = "frontmatter.empty_title";
pub const BAD_TIMESTAMPS: &str = "frontmatter.bad_timestamps";
pub const UNKNOWN_SCHEMA_VERSION: &str = "frontmatter.unknown_schema_version";
pub const DUPLICATE_ID: &str = "frontmatter.duplicate_id";
pub const DUPLICATE_STEM: &str = "frontmatter.duplicate_stem";

pub fn check(pages: &[Page]) -> Vec<LintIssue> {
    let mut issues = Vec::new();
    let mut seen_ids: std::collections::HashMap<_, Vec<&Page>> = std::collections::HashMap::new();

    for page in pages {
        let fm = &page.frontmatter;
        if fm.title.trim().is_empty() {
            issues.push(
                LintIssue {
                    severity: Severity::Error,
                    rule: EMPTY_TITLE.into(),
                    page: None,
                    path: None,
                    message: "frontmatter `title` is empty".into(),
                }
                .at(fm.id, page.path.clone()),
            );
        }
        if fm.updated < fm.created {
            issues.push(
                LintIssue {
                    severity: Severity::Warning,
                    rule: BAD_TIMESTAMPS.into(),
                    page: None,
                    path: None,
                    message: format!(
                        "`updated` ({}) is earlier than `created` ({})",
                        fm.updated, fm.created
                    ),
                }
                .at(fm.id, page.path.clone()),
            );
        }
        if fm.schema_version > SCHEMA_VERSION {
            issues.push(
                LintIssue {
                    severity: Severity::Warning,
                    rule: UNKNOWN_SCHEMA_VERSION.into(),
                    page: None,
                    path: None,
                    message: format!(
                        "frontmatter `schema_version = {}` is newer than the engine ({SCHEMA_VERSION})",
                        fm.schema_version
                    ),
                }
                .at(fm.id, page.path.clone()),
            );
        }
        seen_ids.entry(fm.id).or_default().push(page);
    }

    for (id, group) in seen_ids {
        if group.len() <= 1 {
            continue;
        }
        // Report each duplicate except the first, citing the winner's path.
        let winner = group[0];
        for dup in &group[1..] {
            issues.push(
                LintIssue {
                    severity: Severity::Error,
                    rule: DUPLICATE_ID.into(),
                    page: None,
                    path: None,
                    message: format!("page id `{id}` is also used by `{}`", winner.path),
                }
                .at(dup.frontmatter.id, dup.path.clone()),
            );
        }
    }
    issues
}

pub fn check_duplicate_stems(pages: &[Page]) -> Vec<LintIssue> {
    let mut stems: HashMap<String, Vec<&Page>> = HashMap::new();

    for page in pages {
        if !page.path.as_str().starts_with("wiki/") {
            continue;
        }
        let stem = normalize_stem(&page.path);
        stems.entry(stem).or_default().push(page);
    }

    let mut issues = Vec::new();
    for (stem, group) in &stems {
        if group.len() <= 1 {
            continue;
        }
        let mut paths: Vec<&str> = group.iter().map(|p| p.path.as_str()).collect();
        paths.sort_unstable();
        let path_list = paths.join(", ");
        let first = group[0];
        issues.push(
            LintIssue {
                severity: Severity::Error,
                rule: DUPLICATE_STEM.into(),
                page: None,
                path: None,
                message: format!("duplicate stem '{stem}': {path_list}"),
            }
            .at(first.frontmatter.id, first.path.clone()),
        );
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    fn make_page(path: &str, title: &str) -> Page {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from(path),
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
            body: String::new(),
        }
    }

    #[test]
    fn duplicate_stems_clean_when_no_dupes() {
        let pages = vec![
            make_page("wiki/concepts/foo.md", "Foo"),
            make_page("wiki/topics/bar.md", "Bar"),
        ];
        let issues = check_duplicate_stems(&pages);
        assert!(issues.is_empty());
    }

    #[test]
    fn duplicate_stems_detects_two_collisions() {
        let pages = vec![
            make_page("wiki/concepts/Foo.md", "Foo Concept"),
            make_page("wiki/topics/foo.md", "Foo Topic"),
        ];
        let issues = check_duplicate_stems(&pages);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].rule, DUPLICATE_STEM);
        assert!(issues[0].message.contains("foo"));
        assert!(issues[0].message.contains("wiki/concepts/Foo.md"));
        assert!(issues[0].message.contains("wiki/topics/foo.md"));
    }

    #[test]
    fn duplicate_stems_detects_three_way_collision() {
        let pages = vec![
            make_page("wiki/concepts/Foo.md", "Foo A"),
            make_page("wiki/topics/foo.md", "Foo B"),
            make_page("wiki/entities/FOO.md", "Foo C"),
        ];
        let issues = check_duplicate_stems(&pages);
        assert_eq!(issues.len(), 1);
        let msg = &issues[0].message;
        assert!(msg.contains("wiki/concepts/Foo.md"));
        assert!(msg.contains("wiki/topics/foo.md"));
        assert!(msg.contains("wiki/entities/FOO.md"));
    }

    #[test]
    fn duplicate_stems_excludes_non_wiki_paths() {
        let pages = vec![
            make_page("sources/articles/foo.md", "Source Foo"),
            make_page("sources/pdfs/foo.md", "PDF Foo"),
        ];
        let issues = check_duplicate_stems(&pages);
        assert!(
            issues.is_empty(),
            "sources/ pages must not trigger duplicate_stem"
        );
    }

    #[test]
    fn duplicate_stems_does_not_interfere_with_duplicate_id() {
        let mut p1 = make_page("wiki/concepts/alpha.md", "Alpha");
        let mut p2 = make_page("wiki/topics/beta.md", "Beta");
        let shared_id = PageId::new();
        p1.frontmatter.id = shared_id;
        p2.frontmatter.id = shared_id;

        let stem_issues = check_duplicate_stems(&[p1.clone(), p2.clone()]);
        assert!(
            stem_issues.is_empty(),
            "different stems must not trigger duplicate_stem"
        );

        let id_issues = check(&[p1, p2]);
        let id_rules: Vec<&str> = id_issues.iter().map(|i| i.rule.as_str()).collect();
        assert!(id_rules.contains(&DUPLICATE_ID));
        assert!(!id_rules.contains(&DUPLICATE_STEM));
    }
}
