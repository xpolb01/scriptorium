//! Lint rule: pages missing a type tag.
//!
//! Every wiki page should have at least one "type" tag from the controlled
//! vocabulary (`concept`, `entity`, `topic`, `pattern`, `playbook`,
//! `reference`, `paper`, `talk`, `note`). Pages without one are harder to
//! categorize in search and dedup.

use super::LintIssue;
use crate::vault::Page;

pub const MISSING_TYPE_TAG: &str = "missing_type_tag";

/// Known page type tags (must match the vault schema in CLAUDE.md).
const TYPE_TAGS: &[&str] = &[
    "concept",
    "entity",
    "topic",
    "pattern",
    "playbook",
    "reference",
    "paper",
    "talk",
    "note",
];

/// Check all pages for a type tag.
pub fn check(pages: &[Page]) -> Vec<LintIssue> {
    pages
        .iter()
        .filter(|p| !has_type_tag(p))
        .map(|p| {
            LintIssue::warning(
                MISSING_TYPE_TAG,
                "page has no type tag (concept, entity, topic, etc.)",
            )
            .at(p.frontmatter.id, p.path.clone())
        })
        .collect()
}

fn has_type_tag(page: &Page) -> bool {
    page.frontmatter
        .tags
        .iter()
        .any(|t| TYPE_TAGS.contains(&t.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn make_page(tags: Vec<&str>) -> Page {
        Page {
            path: Utf8PathBuf::from("wiki/test.md"),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: "Test".into(),
                created: Utc::now(),
                updated: Utc::now(),
                sources: vec![],
                tags: tags.into_iter().map(String::from).collect(),
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: "body".into(),
        }
    }

    #[test]
    fn flags_page_without_type_tag() {
        let page = make_page(vec!["domain/rust"]);
        let issues = check(&[page]);
        assert_eq!(issues.len(), 1);
        assert_eq!(issues[0].rule, MISSING_TYPE_TAG);
    }

    #[test]
    fn passes_page_with_type_tag() {
        let page = make_page(vec!["concept", "domain/rust"]);
        let issues = check(&[page]);
        assert!(issues.is_empty());
    }

    #[test]
    fn flags_page_with_empty_tags() {
        let page = make_page(vec![]);
        let issues = check(&[page]);
        assert_eq!(issues.len(), 1);
    }
}
