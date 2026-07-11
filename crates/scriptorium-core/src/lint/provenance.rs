//! Provenance and lifecycle metadata checks.
//!
//! Validates the optional frontmatter conventions added for span-level
//! provenance and page lifecycle:
//!
//! - `source_quotes` — map of source path → verbatim quote. Each key must
//!   be listed in `sources`, and each quote must actually appear in the
//!   source file (whitespace-normalized). A quote that no longer matches
//!   is exactly the drift this rule exists to catch.
//! - `supersedes` / `superseded_by` — stems must resolve to existing
//!   pages; a page must never supersede itself.
//! - `valid_from` / `valid_to` — must parse as ISO dates when present,
//!   and `valid_from` must not be after `valid_to`.
//! - `relations` — list of `{type, target}`; type must be in the small
//!   vocabulary, target must resolve.
//!
//! Everything here is a **warning**, not an error: these conventions are
//! optional metadata, and an unreadable source (e.g. a binary PDF) makes
//! a quote unverifiable, not wrong.

use std::collections::HashSet;

use super::{LintIssue, Severity};
use crate::vault::{Page, Vault};

/// Allowed `relations[].type` values.
pub const RELATION_TYPES: &[&str] = &[
    "supports",
    "contradicts",
    "supersedes",
    "part_of",
    "see_also",
];

pub fn check(vault: &Vault, pages: &[Page]) -> Vec<LintIssue> {
    let stems: HashSet<&str> = pages.iter().filter_map(|p| p.path.file_stem()).collect();
    let mut issues = Vec::new();
    for page in pages {
        check_source_quotes(vault, page, &mut issues);
        check_lifecycle(page, &stems, &mut issues);
        check_relations(page, &stems, &mut issues);
    }
    issues
}

fn warn(page: &Page, message: String) -> LintIssue {
    LintIssue {
        severity: Severity::Warning,
        rule: "provenance".into(),
        page: Some(page.frontmatter.id),
        path: Some(page.path.clone()),
        message,
    }
}

fn check_source_quotes(vault: &Vault, page: &Page, issues: &mut Vec<LintIssue>) {
    let Some(serde_yml::Value::Mapping(map)) = page.frontmatter.extra.get("source_quotes") else {
        return;
    };
    for (key, value) in map {
        let (Some(source), Some(quote)) = (key.as_str(), value.as_str()) else {
            issues.push(warn(
                page,
                "source_quotes must map strings to strings".into(),
            ));
            continue;
        };
        if !page.frontmatter.sources.iter().any(|s| s == source) {
            issues.push(warn(
                page,
                format!("source_quotes references '{source}' which is not in sources"),
            ));
            continue;
        }
        // Verify the quote appears in the source (or its extracted sibling
        // for binary formats). Unreadable → skip silently; that's the
        // stale-source rule's territory.
        let root = vault.root();
        let text = std::fs::read_to_string(root.join(source).as_std_path())
            .or_else(|_| {
                std::fs::read_to_string(root.join(format!("{source}.extracted.md")).as_std_path())
            })
            .ok();
        if let Some(text) = text {
            if !normalized_contains(&text, quote) {
                issues.push(warn(
                    page,
                    format!("source_quote not found in '{source}': \"{quote}\""),
                ));
            }
        }
    }
}

fn check_lifecycle(page: &Page, stems: &HashSet<&str>, issues: &mut Vec<LintIssue>) {
    let own_stem = page.path.file_stem().unwrap_or_default();
    if let Some(value) = page.frontmatter.extra.get("supersedes") {
        for stem in yaml_str_list(value) {
            if stem == own_stem {
                issues.push(warn(page, "page supersedes itself".into()));
            } else if !stems.contains(stem.as_str()) {
                issues.push(warn(
                    page,
                    format!("supersedes target '{stem}' does not resolve to a page"),
                ));
            }
        }
    }
    if let Some(value) = page.frontmatter.extra.get("superseded_by") {
        if let Some(stem) = value.as_str() {
            if !stems.contains(stem) {
                issues.push(warn(
                    page,
                    format!("superseded_by target '{stem}' does not resolve to a page"),
                ));
            }
        }
    }
    let parse_date = |v: &serde_yml::Value| -> Option<chrono::NaiveDate> {
        v.as_str()
            .and_then(|s| chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
    };
    let from = page.frontmatter.extra.get("valid_from");
    let to = page.frontmatter.extra.get("valid_to");
    let from_date = from.and_then(&parse_date);
    let to_date = to.and_then(&parse_date);
    if from.is_some() && from_date.is_none() {
        issues.push(warn(
            page,
            "valid_from is not an ISO date (YYYY-MM-DD)".into(),
        ));
    }
    if to.is_some() && to_date.is_none() {
        issues.push(warn(
            page,
            "valid_to is not an ISO date (YYYY-MM-DD)".into(),
        ));
    }
    if let (Some(f), Some(t)) = (from_date, to_date) {
        if f > t {
            issues.push(warn(page, "valid_from is after valid_to".into()));
        }
    }
}

fn check_relations(page: &Page, stems: &HashSet<&str>, issues: &mut Vec<LintIssue>) {
    let Some(serde_yml::Value::Sequence(items)) = page.frontmatter.extra.get("relations") else {
        return;
    };
    for item in items {
        let serde_yml::Value::Mapping(map) = item else {
            issues.push(warn(
                page,
                "relations entries must be {type, target} maps".into(),
            ));
            continue;
        };
        let rel_type = map
            .get(serde_yml::Value::String("type".into()))
            .and_then(|v| v.as_str());
        let target = map
            .get(serde_yml::Value::String("target".into()))
            .and_then(|v| v.as_str());
        match (rel_type, target) {
            (Some(t), Some(target_stem)) => {
                if !RELATION_TYPES.contains(&t) {
                    issues.push(warn(
                        page,
                        format!("relation type '{t}' not in vocabulary {RELATION_TYPES:?}"),
                    ));
                }
                if !stems.contains(target_stem) {
                    issues.push(warn(
                        page,
                        format!("relation target '{target_stem}' does not resolve to a page"),
                    ));
                }
            }
            _ => issues.push(warn(page, "relation entry missing type or target".into())),
        }
    }
}

/// Extract a list of strings from a YAML value (sequence of strings, or a
/// single string treated as a one-element list).
fn yaml_str_list(value: &serde_yml::Value) -> Vec<String> {
    match value {
        serde_yml::Value::Sequence(items) => items
            .iter()
            .filter_map(|v| v.as_str().map(ToString::to_string))
            .collect(),
        serde_yml::Value::String(s) => vec![s.clone()],
        _ => Vec::new(),
    }
}

/// Whitespace-insensitive containment: both haystack and needle collapse
/// runs of whitespace to single spaces before matching.
fn normalized_contains(haystack: &str, needle: &str) -> bool {
    let h: String = haystack.split_whitespace().collect::<Vec<_>>().join(" ");
    let n: String = needle.split_whitespace().collect::<Vec<_>>().join(" ");
    !n.is_empty() && h.contains(&n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_contains_ignores_whitespace_shape() {
        assert!(normalized_contains("a  b\nc d", "b c"));
        assert!(!normalized_contains("a b c", "x y"));
        assert!(!normalized_contains("anything", ""));
    }
}
