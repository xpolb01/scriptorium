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
        paths.sort();
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
