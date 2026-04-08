//! Section-level patches applied to `Page` bodies.
//!
//! The LLM returns a plan describing what each page should look like, but we
//! do not trust it to hand-write whole markdown files without drift. Instead
//! we translate the plan into a list of [`PatchOp`]s against existing pages
//! and apply them deterministically:
//!
//! - [`PatchOp::CreatePage`] — for pages the plan marks as new
//! - [`PatchOp::ReplaceSection`] — rewrites a specific `## Heading` body
//! - [`PatchOp::AppendSection`] — adds a new section to the end of the page
//! - [`PatchOp::InsertSectionAfter`] — new section after a named existing one
//! - [`PatchOp::SetFrontmatterField`] — mutate a single frontmatter key
//! - [`PatchOp::DeletePage`] — remove the page entirely
//!
//! Two safety nets run before any apply:
//!
//! 1. **Stale-page check**: each non-create op carries the expected content
//!    hash of the page at the time the LLM saw it. If the on-disk page hash
//!    differs, the whole [`Patch`] is rejected so the engine can re-fetch and
//!    retry with fresh context.
//! 2. **Conflict check**: two ops in the same `Patch` that touch the same
//!    section heading are rejected as a conflict rather than silently
//!    overwriting each other.
//!
//! Both checks live in [`Patch::apply`], so any caller — ingest, watch mode,
//! MCP `write_page` — gets the same guarantees.

use std::collections::HashSet;
use std::fmt::Write;

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use super::page::{Frontmatter, Page, PageId};
use crate::error::{Error, Result};

/// A batch of operations that must apply atomically to one or more pages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Patch {
    pub ops: Vec<PatchOp>,
}

impl Patch {
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    pub fn push(&mut self, op: PatchOp) {
        self.ops.push(op);
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

impl Default for Patch {
    fn default() -> Self {
        Self::new()
    }
}

/// A single operation against a page. All ops that target an existing page
/// carry a `expected_hash`: the SHA-256 of that page's rendered markdown at
/// the time the LLM read it. If the live page hash differs at apply time, the
/// entire patch is rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PatchOp {
    CreatePage {
        path: Utf8PathBuf,
        frontmatter: Frontmatter,
        body: String,
    },
    ReplaceSection {
        page_id: PageId,
        expected_hash: String,
        /// ATX heading text without the `##` prefix, e.g. `"Overview"`.
        heading: String,
        /// New body for the section, without the heading line itself.
        new_content: String,
    },
    AppendSection {
        page_id: PageId,
        expected_hash: String,
        /// Heading text of the new section.
        heading: String,
        /// Level of the new heading (2 for `## Foo`, 3 for `### Foo`, etc.).
        level: u8,
        content: String,
    },
    InsertSectionAfter {
        page_id: PageId,
        expected_hash: String,
        after_heading: String,
        heading: String,
        level: u8,
        content: String,
    },
    SetFrontmatterField {
        page_id: PageId,
        expected_hash: String,
        field: String,
        value: serde_yml::Value,
    },
    DeletePage {
        page_id: PageId,
        expected_hash: String,
    },
}

/// Result of applying a patch: which pages were created, updated, or deleted.
#[derive(Debug, Clone, Default)]
pub struct PatchOutcome {
    pub created: Vec<Page>,
    pub updated: Vec<Page>,
    pub deleted: Vec<PageId>,
}

/// Errors specific to patch application.
#[derive(Debug, thiserror::Error)]
pub enum PatchError {
    #[error("patch targets page id `{0}` but no such page exists in the vault")]
    UnknownPage(PageId),

    #[error("page `{page_id}` has changed since the LLM read it (expected hash `{expected}`, got `{actual}`)")]
    Stale {
        page_id: PageId,
        expected: String,
        actual: String,
    },

    #[error("patch touches the same section `{heading}` of page `{page_id}` more than once")]
    Conflict { page_id: PageId, heading: String },

    #[error("section `{heading}` not found in page `{page_id}`")]
    SectionNotFound { page_id: PageId, heading: String },

    #[error("patch targets `{0}` for an existing-page op but it looks like a new-page path")]
    PathCollision(Utf8PathBuf),
}

impl From<PatchError> for Error {
    fn from(value: PatchError) -> Self {
        Error::Other(anyhow::anyhow!(value))
    }
}

impl Patch {
    /// Apply the patch against the current state of the vault (the pages
    /// passed in). Returns a [`PatchOutcome`] describing the result; on any
    /// failure nothing has been mutated and the caller should re-fetch context.
    #[allow(clippy::too_many_lines)] // the big match is clearer flat than split
    pub fn apply(&self, current_pages: &[Page]) -> Result<PatchOutcome, PatchError> {
        // 1. Conflict check — no two ops against the same (page_id, section).
        self.check_conflicts()?;

        // 2. Build an index of current pages by id for stale-checks and
        //    section rewrites. We clone so apply() can mutate without
        //    affecting the caller's view.
        let mut by_id: std::collections::BTreeMap<PageId, Page> = current_pages
            .iter()
            .map(|p| (p.frontmatter.id, p.clone()))
            .collect();

        let mut created: Vec<Page> = Vec::new();
        let mut updated_ids: HashSet<PageId> = HashSet::new();
        let mut deleted: Vec<PageId> = Vec::new();

        for op in &self.ops {
            match op {
                PatchOp::CreatePage {
                    path,
                    frontmatter,
                    body,
                } => {
                    // Reject if a page with that ID already exists — the
                    // plan should have used an update op instead.
                    if by_id.contains_key(&frontmatter.id) {
                        return Err(PatchError::PathCollision(path.clone()));
                    }
                    let new_page = Page {
                        path: path.clone(),
                        frontmatter: frontmatter.clone(),
                        body: body.clone(),
                    };
                    created.push(new_page);
                }
                PatchOp::ReplaceSection {
                    page_id,
                    expected_hash,
                    heading,
                    new_content,
                } => {
                    let page = by_id
                        .get_mut(page_id)
                        .ok_or(PatchError::UnknownPage(*page_id))?;
                    check_hash(page, page_id, expected_hash)?;
                    page.body =
                        replace_section(&page.body, heading, new_content).ok_or_else(|| {
                            PatchError::SectionNotFound {
                                page_id: *page_id,
                                heading: heading.clone(),
                            }
                        })?;
                    updated_ids.insert(*page_id);
                }
                PatchOp::AppendSection {
                    page_id,
                    expected_hash,
                    heading,
                    level,
                    content,
                } => {
                    let page = by_id
                        .get_mut(page_id)
                        .ok_or(PatchError::UnknownPage(*page_id))?;
                    check_hash(page, page_id, expected_hash)?;
                    page.body = append_section(&page.body, *level, heading, content);
                    updated_ids.insert(*page_id);
                }
                PatchOp::InsertSectionAfter {
                    page_id,
                    expected_hash,
                    after_heading,
                    heading,
                    level,
                    content,
                } => {
                    let page = by_id
                        .get_mut(page_id)
                        .ok_or(PatchError::UnknownPage(*page_id))?;
                    check_hash(page, page_id, expected_hash)?;
                    page.body =
                        insert_section_after(&page.body, after_heading, *level, heading, content)
                            .ok_or_else(|| PatchError::SectionNotFound {
                            page_id: *page_id,
                            heading: after_heading.clone(),
                        })?;
                    updated_ids.insert(*page_id);
                }
                PatchOp::SetFrontmatterField {
                    page_id,
                    expected_hash,
                    field,
                    value,
                } => {
                    let page = by_id
                        .get_mut(page_id)
                        .ok_or(PatchError::UnknownPage(*page_id))?;
                    check_hash(page, page_id, expected_hash)?;
                    set_frontmatter_field(&mut page.frontmatter, field, value.clone());
                    updated_ids.insert(*page_id);
                }
                PatchOp::DeletePage {
                    page_id,
                    expected_hash,
                } => {
                    let page = by_id
                        .remove(page_id)
                        .ok_or(PatchError::UnknownPage(*page_id))?;
                    let actual = page.content_hash().map_err(|_| PatchError::Stale {
                        page_id: *page_id,
                        expected: expected_hash.clone(),
                        actual: "<hash-error>".into(),
                    })?;
                    if &actual != expected_hash {
                        return Err(PatchError::Stale {
                            page_id: *page_id,
                            expected: expected_hash.clone(),
                            actual,
                        });
                    }
                    deleted.push(*page_id);
                }
            }
        }

        // Collect updated pages (those we mutated and did not delete).
        let updated: Vec<Page> = updated_ids
            .into_iter()
            .filter_map(|id| by_id.get(&id).cloned())
            .collect();

        Ok(PatchOutcome {
            created,
            updated,
            deleted,
        })
    }

    fn check_conflicts(&self) -> Result<(), PatchError> {
        let mut seen: HashSet<(PageId, String)> = HashSet::new();
        for op in &self.ops {
            let key = match op {
                PatchOp::ReplaceSection {
                    page_id, heading, ..
                }
                | PatchOp::AppendSection {
                    page_id, heading, ..
                }
                | PatchOp::InsertSectionAfter {
                    page_id, heading, ..
                } => Some((*page_id, heading.clone())),
                _ => None,
            };
            if let Some(k) = key {
                if !seen.insert(k.clone()) {
                    return Err(PatchError::Conflict {
                        page_id: k.0,
                        heading: k.1,
                    });
                }
            }
        }
        Ok(())
    }
}

fn check_hash(page: &Page, page_id: &PageId, expected: &str) -> Result<(), PatchError> {
    let actual = page.content_hash().map_err(|_| PatchError::Stale {
        page_id: *page_id,
        expected: expected.into(),
        actual: "<hash-error>".into(),
    })?;
    if actual != expected {
        return Err(PatchError::Stale {
            page_id: *page_id,
            expected: expected.into(),
            actual,
        });
    }
    Ok(())
}

/// Find `## heading` in `body` and replace its content (everything until the
/// next same-or-higher-level heading) with `new_content`. Preserves the
/// heading line itself. Returns `None` if the heading is not found.
fn replace_section(body: &str, heading: &str, new_content: &str) -> Option<String> {
    let (level, heading_line, start, end) = find_section(body, heading)?;
    // `start` is the byte index just after the heading line's newline.
    // `end` is the byte index where the next heading of level <= `level`
    // begins, or body.len() if this was the last section.
    let _ = level;
    let mut out = String::with_capacity(body.len());
    out.push_str(&body[..start - heading_line.len()]);
    out.push_str(&heading_line);
    let trimmed = new_content.trim_end_matches('\n');
    out.push_str(trimmed);
    if !trimmed.is_empty() {
        out.push('\n');
    }
    if end < body.len() {
        // Preserve a blank line between the replaced section and the next.
        if !out.ends_with("\n\n") {
            out.push('\n');
        }
        out.push_str(&body[end..]);
    }
    Some(out)
}

/// Append a new section to the end of `body`.
fn append_section(body: &str, level: u8, heading: &str, content: &str) -> String {
    let mut out = body.to_string();
    if !out.ends_with('\n') {
        out.push('\n');
    }
    if !out.ends_with("\n\n") && !out.is_empty() {
        out.push('\n');
    }
    let hashes = "#".repeat(level.into());
    let _ = writeln!(out, "{hashes} {heading}\n");
    out.push_str(content.trim_end_matches('\n'));
    out.push('\n');
    out
}

/// Insert a new section after an existing one.
fn insert_section_after(
    body: &str,
    after_heading: &str,
    level: u8,
    heading: &str,
    content: &str,
) -> Option<String> {
    let (_, _, _, end) = find_section(body, after_heading)?;
    let mut out = String::with_capacity(body.len() + content.len());
    out.push_str(&body[..end]);
    if !out.ends_with("\n\n") {
        out.push('\n');
    }
    let hashes = "#".repeat(level.into());
    let _ = writeln!(out, "{hashes} {heading}\n");
    out.push_str(content.trim_end_matches('\n'));
    out.push('\n');
    if end < body.len() {
        out.push('\n');
        out.push_str(&body[end..]);
    }
    Some(out)
}

/// Find the ATX heading with text `heading` (any level) in `body`.
/// Returns `(level, heading_line_with_newline, content_start, content_end)`.
fn find_section(body: &str, heading: &str) -> Option<(u8, String, usize, usize)> {
    let lines: Vec<(usize, &str)> = body
        .split_inclusive('\n')
        .scan(0usize, |pos, line| {
            let start = *pos;
            *pos += line.len();
            Some((start, line))
        })
        .collect();

    for (i, (start, line)) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let level = trimmed.chars().take_while(|&c| c == '#').count();
        if level == 0 || level > 6 {
            continue;
        }
        let after_hashes = &trimmed[level..];
        if !after_hashes.starts_with(' ') {
            continue;
        }
        let title = after_hashes.trim().trim_end_matches('#').trim();
        if title != heading {
            continue;
        }
        // Found it. Compute content start + end.
        let content_start = start + line.len();
        // Walk forward until we hit a heading of level <= this one.
        let mut content_end = body.len();
        for (j, (next_start, next_line)) in lines[i + 1..].iter().enumerate() {
            let _ = j;
            let next_trimmed = next_line.trim_start();
            let next_level = next_trimmed.chars().take_while(|&c| c == '#').count();
            if next_level > 0
                && next_level <= level
                && next_trimmed
                    .get(next_level..)
                    .is_some_and(|rest| rest.starts_with(' '))
            {
                content_end = *next_start;
                break;
            }
        }
        // `level` is known to be 1..=6 because we bail out above otherwise.
        let level_u8 = u8::try_from(level).unwrap_or(6);
        return Some(((level_u8), (*line).to_string(), content_start, content_end));
    }
    None
}

fn set_frontmatter_field(fm: &mut Frontmatter, field: &str, value: serde_yml::Value) {
    // Known fields get first-class handling so the struct types are honoured
    // (title stays a String, etc.). Unknown fields flow into `extra`.
    match field {
        "title" => {
            if let serde_yml::Value::String(s) = value {
                fm.title = s;
            }
        }
        "tags" => {
            if let serde_yml::Value::Sequence(seq) = value {
                fm.tags = seq
                    .into_iter()
                    .filter_map(|v| match v {
                        serde_yml::Value::String(s) => Some(s),
                        _ => None,
                    })
                    .collect();
            }
        }
        "aliases" => {
            if let serde_yml::Value::Sequence(seq) = value {
                fm.aliases = seq
                    .into_iter()
                    .filter_map(|v| match v {
                        serde_yml::Value::String(s) => Some(s),
                        _ => None,
                    })
                    .collect();
            }
        }
        other => {
            fm.extra.insert(other.to_string(), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, PageId};
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    fn sample_page(id: PageId, body: &str) -> Page {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from("wiki/foo.md"),
            frontmatter: Frontmatter {
                id,
                title: "Foo".into(),
                created: now,
                updated: now,
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
    fn replace_section_swaps_matching_heading() {
        let body = "## Overview\n\nOld.\n\n## Details\n\nKeep.\n";
        let out = replace_section(body, "Overview", "New overview.").unwrap();
        assert!(out.contains("## Overview"));
        assert!(out.contains("New overview."));
        assert!(!out.contains("Old."));
        assert!(out.contains("## Details"));
        assert!(out.contains("Keep."));
    }

    #[test]
    fn replace_section_returns_none_when_heading_missing() {
        let body = "## Details\n\nKeep.\n";
        assert!(replace_section(body, "Overview", "x").is_none());
    }

    #[test]
    fn append_section_adds_to_end() {
        let body = "## Foo\n\nBody.\n";
        let out = append_section(body, 2, "Bar", "Bar body.");
        assert!(out.ends_with("## Bar\n\nBar body.\n"));
    }

    #[test]
    fn insert_section_after_places_between() {
        let body = "## A\n\nA body.\n\n## C\n\nC body.\n";
        let out = insert_section_after(body, "A", 2, "B", "B body.").unwrap();
        let a_pos = out.find("## A").unwrap();
        let b_pos = out.find("## B").unwrap();
        let c_pos = out.find("## C").unwrap();
        assert!(a_pos < b_pos && b_pos < c_pos);
    }

    #[test]
    fn patch_apply_replaces_section_with_valid_hash() {
        let id = PageId::new();
        let page = sample_page(id, "## Overview\n\nOld.\n\n## Details\n\nKeep.\n");
        let hash = page.content_hash().unwrap();
        let patch = Patch {
            ops: vec![PatchOp::ReplaceSection {
                page_id: id,
                expected_hash: hash,
                heading: "Overview".into(),
                new_content: "New.".into(),
            }],
        };
        let outcome = patch.apply(&[page]).unwrap();
        assert_eq!(outcome.updated.len(), 1);
        assert!(outcome.updated[0].body.contains("New."));
    }

    #[test]
    fn patch_apply_rejects_stale_hash() {
        let id = PageId::new();
        let page = sample_page(id, "## Overview\n\nOld.\n");
        let patch = Patch {
            ops: vec![PatchOp::ReplaceSection {
                page_id: id,
                expected_hash: "wronghash".into(),
                heading: "Overview".into(),
                new_content: "New.".into(),
            }],
        };
        let err = patch.apply(&[page]).unwrap_err();
        assert!(matches!(err, PatchError::Stale { .. }));
    }

    #[test]
    fn patch_apply_rejects_conflicting_ops_on_same_section() {
        let id = PageId::new();
        let page = sample_page(id, "## Overview\n\nOld.\n");
        let hash = page.content_hash().unwrap();
        let patch = Patch {
            ops: vec![
                PatchOp::ReplaceSection {
                    page_id: id,
                    expected_hash: hash.clone(),
                    heading: "Overview".into(),
                    new_content: "A".into(),
                },
                PatchOp::ReplaceSection {
                    page_id: id,
                    expected_hash: hash,
                    heading: "Overview".into(),
                    new_content: "B".into(),
                },
            ],
        };
        let err = patch.apply(&[page]).unwrap_err();
        assert!(matches!(err, PatchError::Conflict { .. }));
    }

    #[test]
    fn patch_apply_creates_new_page() {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        let fm = Frontmatter {
            id: PageId::new(),
            title: "New".into(),
            created: now,
            updated: now,
            sources: vec![],
            tags: vec![],
            aliases: vec![],
            schema_version: 1,
            extra: BTreeMap::new(),
        };
        let patch = Patch {
            ops: vec![PatchOp::CreatePage {
                path: Utf8PathBuf::from("wiki/new.md"),
                frontmatter: fm,
                body: "body\n".into(),
            }],
        };
        let outcome = patch.apply(&[]).unwrap();
        assert_eq!(outcome.created.len(), 1);
        assert_eq!(outcome.created[0].path, "wiki/new.md");
    }

    #[test]
    fn patch_apply_set_frontmatter_field() {
        let id = PageId::new();
        let page = sample_page(id, "body\n");
        let hash = page.content_hash().unwrap();
        let patch = Patch {
            ops: vec![PatchOp::SetFrontmatterField {
                page_id: id,
                expected_hash: hash,
                field: "title".into(),
                value: serde_yml::Value::String("Renamed".into()),
            }],
        };
        let outcome = patch.apply(&[page]).unwrap();
        assert_eq!(outcome.updated[0].frontmatter.title, "Renamed");
    }

    #[test]
    fn patch_apply_delete_page() {
        let id = PageId::new();
        let page = sample_page(id, "body\n");
        let hash = page.content_hash().unwrap();
        let patch = Patch {
            ops: vec![PatchOp::DeletePage {
                page_id: id,
                expected_hash: hash,
            }],
        };
        let outcome = patch.apply(&[page]).unwrap();
        assert_eq!(outcome.deleted, vec![id]);
    }
}
