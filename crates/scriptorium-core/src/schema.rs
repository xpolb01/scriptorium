//! The vault's `CLAUDE.md` schema: the contract between the user and the LLM.
//!
//! Scriptorium reads `<vault>/CLAUDE.md` (optionally falling back to
//! `AGENTS.md`) before every ingest / query / lint-llm prompt and injects it
//! as the system prompt. The schema describes:
//!
//! - what kinds of pages live in `wiki/`
//! - naming and linking conventions
//! - citation format for query answers
//! - required frontmatter fields
//! - "do not" rules (e.g. "never rewrite sources")
//!
//! Because the schema grows unboundedly over a vault's lifetime, every
//! injection passes through [`Schema::render`] with a token budget. The
//! rendered form keeps the top-level heading and the first paragraph of each
//! subsection, truncating further detail with `…`. A rough `chars / 4`
//! heuristic stands in for a real tokenizer; it is conservative enough for
//! the 200k+ context windows we target.

use std::fmt::Write;
use std::fs;
use std::path::PathBuf;

use pulldown_cmark::{Event, HeadingLevel, Parser, Tag, TagEnd};

use crate::error::{Error, Result};
use crate::vault::Vault;

pub const SCHEMA_FILENAME: &str = "CLAUDE.md";
pub const SCHEMA_FALLBACK: &str = "AGENTS.md";

/// Approximate token count for a piece of text (chars / 4, rounded up).
pub fn approx_tokens(text: &str) -> usize {
    text.chars().count().div_ceil(4)
}

/// A parsed `CLAUDE.md`: the raw content plus a flat list of top-level
/// sections discovered by walking the markdown.
#[derive(Debug, Clone)]
pub struct Schema {
    pub raw: String,
    /// Top-level sections in document order.
    pub sections: Vec<Section>,
}

#[derive(Debug, Clone)]
pub struct Section {
    pub level: u8,
    pub heading: String,
    /// Body of the section, excluding the heading line. May contain
    /// subsections.
    pub body: String,
}

impl Schema {
    /// Empty schema, useful as a default when there is no `CLAUDE.md` yet.
    pub fn empty() -> Self {
        Self {
            raw: String::new(),
            sections: Vec::new(),
        }
    }

    /// Load `CLAUDE.md` (or `AGENTS.md`) from the vault root. Missing files
    /// produce an empty schema rather than an error — the engine should still
    /// work in a skeleton vault.
    pub fn load(vault: &Vault) -> Result<Self> {
        let primary: PathBuf = vault.root().join(SCHEMA_FILENAME).into_std_path_buf();
        if let Ok(text) = fs::read_to_string(&primary) {
            return Ok(Self::parse(&text));
        }
        let fallback: PathBuf = vault.root().join(SCHEMA_FALLBACK).into_std_path_buf();
        match fs::read_to_string(&fallback) {
            Ok(text) => Ok(Self::parse(&text)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(e) => Err(Error::io(fallback, e)),
        }
    }

    /// Parse a schema from raw text.
    pub fn parse(input: &str) -> Self {
        let sections = split_sections(input);
        Self {
            raw: input.to_string(),
            sections,
        }
    }

    /// Render the schema so that its approximate token count fits under
    /// `budget`. If the full schema is already within budget, returns it
    /// unchanged.
    ///
    /// The shrink strategy, in order:
    /// 1. Full raw text
    /// 2. Each top-level section's heading + first paragraph
    /// 3. Just the headings
    /// 4. A single "schema truncated" marker if even the headings overflow
    pub fn render(&self, budget: usize) -> String {
        if self.raw.is_empty() {
            return String::new();
        }
        if approx_tokens(&self.raw) <= budget {
            return self.raw.clone();
        }
        let condensed = self.render_condensed();
        if approx_tokens(&condensed) <= budget {
            return condensed;
        }
        let headings_only = self.render_headings_only();
        if approx_tokens(&headings_only) <= budget {
            return headings_only;
        }
        format!("[schema truncated to fit {budget}-token budget]\n")
    }

    fn render_condensed(&self) -> String {
        let mut out = String::new();
        for section in &self.sections {
            let hashes = "#".repeat(section.level.into());
            let _ = writeln!(out, "{hashes} {}\n", section.heading);
            if let Some(first_para) = first_paragraph(&section.body) {
                out.push_str(first_para.trim_end());
                out.push_str("\n\n");
            }
        }
        out
    }

    fn render_headings_only(&self) -> String {
        let mut out = String::new();
        for section in &self.sections {
            let hashes = "#".repeat(section.level.into());
            let _ = writeln!(out, "{hashes} {}", section.heading);
        }
        out
    }
}

fn first_paragraph(body: &str) -> Option<&str> {
    let trimmed = body.trim_start_matches('\n');
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.find("\n\n") {
        Some(end) => Some(&trimmed[..end]),
        None => Some(trimmed),
    }
}

/// Walk the document with `pulldown-cmark`, collecting top-level sections
/// (ATX headings) and the text that follows them until the next same-or-
/// higher-level heading.
fn split_sections(input: &str) -> Vec<Section> {
    let mut sections: Vec<Section> = Vec::new();
    let parser = Parser::new(input).into_offset_iter();
    let mut current: Option<(u8, String, usize, usize)> = None; // (level, heading, body_start, body_end)
    let mut in_heading: Option<(u8, String, usize)> = None;
    let mut heading_buffer = String::new();
    for (event, range) in parser {
        match event {
            Event::Start(Tag::Heading { level, .. }) => {
                // Close the previous section.
                if let Some((lvl, title, start, _)) = current.take() {
                    let body = input.get(start..range.start).unwrap_or("").to_string();
                    sections.push(Section {
                        level: lvl,
                        heading: title,
                        body,
                    });
                }
                heading_buffer.clear();
                in_heading = Some((heading_level_to_u8(level), String::new(), range.end));
            }
            Event::Text(text) if in_heading.is_some() => {
                heading_buffer.push_str(&text);
            }
            Event::Code(text) if in_heading.is_some() => {
                heading_buffer.push_str(&text);
            }
            Event::End(TagEnd::Heading(_)) => {
                if let Some((level, _, body_start)) = in_heading.take() {
                    let title = std::mem::take(&mut heading_buffer);
                    current = Some((level, title, body_start, body_start));
                }
            }
            _ => {}
        }
    }
    // Close the final section.
    if let Some((level, title, start, _)) = current {
        let body = input.get(start..).unwrap_or("").to_string();
        sections.push(Section {
            level,
            heading: title,
            body,
        });
    }
    sections
}

const fn heading_level_to_u8(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_schema_renders_empty() {
        assert_eq!(Schema::empty().render(1000), "");
    }

    #[test]
    fn parses_top_level_sections() {
        let input = "# One\n\nAlpha.\n\n# Two\n\nBeta.\n";
        let schema = Schema::parse(input);
        assert_eq!(schema.sections.len(), 2);
        assert_eq!(schema.sections[0].heading, "One");
        assert_eq!(schema.sections[1].heading, "Two");
    }

    #[test]
    fn render_returns_full_text_when_under_budget() {
        let input = "# Rules\n\nBe nice.\n";
        let schema = Schema::parse(input);
        assert_eq!(schema.render(1000), input);
    }

    #[test]
    fn render_condenses_when_over_budget() {
        let mut input = String::from("# Rules\n\nBe nice.\n\n## Details\n\n");
        for i in 0..500 {
            let _ = writeln!(input, "Detail line {i}.");
        }
        let schema = Schema::parse(&input);
        let rendered = schema.render(100);
        // Condensed output keeps the top-level heading but drops most content.
        assert!(rendered.contains("# Rules"));
        assert!(rendered.len() < input.len());
    }

    #[test]
    fn render_falls_back_to_headings_only_when_still_too_long() {
        let mut input = String::from("# Rules\n\n");
        for _ in 0..1000 {
            input.push_str("Very long first paragraph continues and continues. ");
        }
        let schema = Schema::parse(&input);
        let rendered = schema.render(10);
        // Very tight budget; should end up at headings-only or the truncated marker.
        assert!(
            rendered.contains("# Rules") || rendered.contains("schema truncated"),
            "got: {rendered}"
        );
    }

    #[test]
    fn approx_tokens_is_chars_over_four() {
        assert_eq!(approx_tokens(""), 0);
        assert_eq!(approx_tokens("abcd"), 1);
        assert_eq!(approx_tokens("abcde"), 2);
        assert_eq!(approx_tokens("aaaaaaaa"), 2);
    }
}
