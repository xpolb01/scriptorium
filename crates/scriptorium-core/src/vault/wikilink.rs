//! Wikilink extraction.
//!
//! Supports the Obsidian-compatible subset:
//!
//! - `[[Page]]`
//! - `[[Page|Display alias]]`
//! - `[[Page#Heading]]`
//! - `[[Page#Heading|Display alias]]`
//! - `[[#Heading]]` (within-page heading link)
//!
//! Block references (`[[Page^block-id]]`) are not supported in v1; the parser
//! will see them as part of the target string.
//!
//! Wikilinks inside code spans, code blocks, or HTML blocks are skipped — they
//! are usually examples in documentation, not real references. This is
//! enforced by walking the document with `pulldown-cmark` and only running the
//! regex over text events outside code contexts.

use std::ops::Range;
use std::sync::OnceLock;

use pulldown_cmark::{Event, Parser, Tag, TagEnd};
use regex::Regex;
use serde::{Deserialize, Serialize};

/// A parsed wikilink.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Wikilink {
    /// The link target — typically the file stem of the destination page.
    /// Empty when the link is a within-page heading reference (`[[#Heading]]`).
    pub target: String,
    /// Optional `#Heading` portion.
    pub heading: Option<String>,
    /// Optional `|Alias` portion.
    pub alias: Option<String>,
}

impl Wikilink {
    /// True if this wikilink is just a within-page heading reference.
    pub fn is_self_link(&self) -> bool {
        self.target.is_empty() && self.heading.is_some()
    }
}

fn wikilink_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        // [[ TARGET (#HEADING)? (|ALIAS)? ]]
        // - TARGET: anything but [, ], |, # (may be empty for [[#heading]])
        // - HEADING: anything but [, ], |
        // - ALIAS: anything but [, ]
        Regex::new(r"\[\[([^\[\]\|#]*)(?:#([^\[\]\|]+))?(?:\|([^\[\]]+))?\]\]")
            .expect("wikilink regex compiles")
    })
}

/// Extract wikilinks from a plain text string. Does not understand markdown
/// structure — use [`parse_markdown`] for full pages.
pub fn parse_text(text: &str) -> Vec<Wikilink> {
    wikilink_regex()
        .captures_iter(text)
        .filter_map(|cap| {
            let target = cap
                .get(1)
                .map(|m| m.as_str().trim().to_string())
                .unwrap_or_default();
            let heading = cap.get(2).map(|m| m.as_str().trim().to_string());
            let alias = cap.get(3).map(|m| m.as_str().trim().to_string());
            // Skip empty links: [[]] or [[|alias]] with no target and no heading.
            if target.is_empty() && heading.is_none() {
                return None;
            }
            Some(Wikilink {
                target,
                heading,
                alias,
            })
        })
        .collect()
}

/// Walk a markdown document, extracting wikilinks from text outside code
/// spans, code blocks, and HTML blocks. Returns links in document order; the
/// same `Wikilink` may appear multiple times if the page references the same
/// target more than once.
///
/// Implementation note: we cannot feed `Text` events to the wikilink regex,
/// because `pulldown-cmark` parses `[[foo]]` as an opening `[` followed by a
/// `[foo]` reference link followed by `]`, fragmenting the text before we see
/// it. Instead, we use `pulldown-cmark` only to discover the byte ranges of
/// code / HTML regions in the original source, then run the regex over the
/// complementary ranges of the untouched source.
pub fn parse_markdown(md: &str) -> Vec<Wikilink> {
    let excluded = excluded_ranges(md);
    let mut links = Vec::new();
    let mut pos = 0;
    for range in &excluded {
        if range.start > pos {
            links.extend(parse_text(&md[pos..range.start]));
        }
        pos = range.end.max(pos);
    }
    if pos < md.len() {
        links.extend(parse_text(&md[pos..]));
    }
    links
}

/// Byte ranges in `md` that should be skipped when hunting for wikilinks:
/// fenced/indented code blocks, inline code spans, and raw HTML.
fn excluded_ranges(md: &str) -> Vec<Range<usize>> {
    let mut ranges: Vec<Range<usize>> = Vec::new();
    let parser = Parser::new(md).into_offset_iter();
    let mut code_block_start: Option<usize> = None;
    for (event, range) in parser {
        match event {
            Event::Start(Tag::CodeBlock(_)) => {
                code_block_start = Some(range.start);
            }
            Event::End(TagEnd::CodeBlock) => {
                if let Some(start) = code_block_start.take() {
                    ranges.push(start..range.end);
                }
            }
            Event::Code(_) | Event::Html(_) | Event::InlineHtml(_) => {
                ranges.push(range);
            }
            _ => {}
        }
    }
    // Sort and merge overlapping ranges so the walk in `parse_markdown` is
    // monotonic.
    ranges.sort_by_key(|r| r.start);
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(ranges.len());
    for r in ranges {
        match merged.last_mut() {
            Some(last) if r.start <= last.end => {
                last.end = last.end.max(r.end);
            }
            _ => merged.push(r),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_link() {
        let links = parse_text("see [[foo]] for details");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "foo");
        assert!(links[0].heading.is_none());
        assert!(links[0].alias.is_none());
    }

    #[test]
    fn parses_link_with_alias() {
        let links = parse_text("see [[foo|the foo page]]");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "foo");
        assert_eq!(links[0].alias.as_deref(), Some("the foo page"));
    }

    #[test]
    fn parses_link_with_heading() {
        let links = parse_text("see [[foo#Section A]]");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "foo");
        assert_eq!(links[0].heading.as_deref(), Some("Section A"));
    }

    #[test]
    fn parses_link_with_heading_and_alias() {
        let links = parse_text("see [[foo#Section A|alias]]");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].target, "foo");
        assert_eq!(links[0].heading.as_deref(), Some("Section A"));
        assert_eq!(links[0].alias.as_deref(), Some("alias"));
    }

    #[test]
    fn parses_self_heading_link() {
        let links = parse_text("see [[#Heading]] above");
        assert_eq!(links.len(), 1);
        assert!(links[0].is_self_link());
        assert_eq!(links[0].heading.as_deref(), Some("Heading"));
    }

    #[test]
    fn parses_multiple_links_in_one_string() {
        let links = parse_text("[[a]] and [[b]] and [[c|sea]]");
        assert_eq!(links.len(), 3);
        assert_eq!(links[0].target, "a");
        assert_eq!(links[1].target, "b");
        assert_eq!(links[2].target, "c");
        assert_eq!(links[2].alias.as_deref(), Some("sea"));
    }

    #[test]
    fn ignores_empty_links() {
        let links = parse_text("[[]] should be ignored");
        assert!(links.is_empty());
    }

    #[test]
    fn skips_links_inside_fenced_code_block() {
        let md = "Real [[outside]]\n\n```\nfake [[inside]]\n```\n\nMore [[after]]";
        let links = parse_markdown(md);
        let targets: Vec<_> = links.iter().map(|l| l.target.as_str()).collect();
        assert_eq!(targets, vec!["outside", "after"]);
    }

    #[test]
    fn skips_links_inside_inline_code() {
        let md = "Real [[outside]] but not `[[inside]]`";
        let links = parse_markdown(md);
        let targets: Vec<_> = links.iter().map(|l| l.target.as_str()).collect();
        assert_eq!(targets, vec!["outside"]);
    }

    #[test]
    fn handles_links_across_paragraphs() {
        let md = "First paragraph [[a]].\n\nSecond paragraph [[b]] and [[c]].\n";
        let links = parse_markdown(md);
        let targets: Vec<_> = links.iter().map(|l| l.target.as_str()).collect();
        assert_eq!(targets, vec!["a", "b", "c"]);
    }
}
