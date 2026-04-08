//! Heading-aware page chunker.
//!
//! Splits a page body into chunks small enough to fit an embedding model,
//! preferring boundaries at H2/H3 headings so chunks carry semantic context.
//! Sections that still exceed the byte budget are split further by paragraph.
//!
//! The chunker is deterministic: the same body produces the same sequence of
//! chunks every time. That matters because chunks are stored keyed by
//! `chunk_idx` — if two runs disagreed on indices, the embeddings cache would
//! become inconsistent.

use serde::{Deserialize, Serialize};

/// One indexed piece of a page, suitable for embedding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    /// 0-based index of this chunk within its page. Stable for a given
    /// `(body, max_chars)` pair.
    pub idx: usize,
    /// The nearest enclosing heading (H2/H3), if any. Used as lightweight
    /// metadata in the store and as display text in search results.
    pub heading: Option<String>,
    /// Raw text of the chunk, including the heading line if one started it.
    pub text: String,
}

/// Split `body` into chunks of at most `max_chars` characters each,
/// preferring H2/H3 heading boundaries. Empty bodies produce no chunks.
pub fn chunk_page(body: &str, max_chars: usize) -> Vec<Chunk> {
    assert!(max_chars > 0, "max_chars must be positive");
    let mut sections: Vec<Section> = Vec::new();
    let mut current_heading: Option<String> = None;
    let mut current_text = String::new();

    for line in body.split_inclusive('\n') {
        if let Some(new_heading) = heading_text(line) {
            if current_text.trim().is_empty() {
                current_text.clear();
            } else {
                sections.push(Section {
                    heading: current_heading.clone(),
                    text: std::mem::take(&mut current_text),
                });
            }
            current_heading = Some(new_heading);
        }
        current_text.push_str(line);
    }
    if !current_text.trim().is_empty() {
        sections.push(Section {
            heading: current_heading,
            text: current_text,
        });
    }

    // Split any oversize sections by paragraph, assigning consecutive indices.
    let mut out = Vec::with_capacity(sections.len());
    let mut idx = 0usize;
    for section in sections {
        for sub in split_by_paragraph(&section.text, max_chars) {
            out.push(Chunk {
                idx,
                heading: section.heading.clone(),
                text: sub,
            });
            idx += 1;
        }
    }
    out
}

struct Section {
    heading: Option<String>,
    text: String,
}

fn heading_text(line: &str) -> Option<String> {
    // Only H2 and H3 become chunk boundaries. H1 is usually the page title
    // (duplicated from frontmatter) and H4+ is too fine-grained for
    // retrieval.
    let trimmed = line.trim_start();
    let hash_count = trimmed.chars().take_while(|&c| c == '#').count();
    if !(2..=3).contains(&hash_count) {
        return None;
    }
    let after = trimmed.get(hash_count..)?;
    if !after.starts_with(' ') {
        return None;
    }
    let title = after.trim().trim_end_matches('#').trim();
    if title.is_empty() {
        None
    } else {
        Some(title.to_string())
    }
}

fn split_by_paragraph(text: &str, max_chars: usize) -> Vec<String> {
    if text.chars().count() <= max_chars {
        return vec![text.to_string()];
    }
    let mut out: Vec<String> = Vec::new();
    let mut buf = String::new();
    for para in text.split("\n\n") {
        let para_with_sep = if buf.is_empty() {
            para.to_string()
        } else {
            format!("\n\n{para}")
        };
        if buf.chars().count() + para_with_sep.chars().count() > max_chars && !buf.is_empty() {
            out.push(std::mem::take(&mut buf));
        }
        buf.push_str(&para_with_sep);
        if buf.chars().count() > max_chars {
            // A single paragraph larger than the budget: split on chars.
            for window in split_by_chars(&buf, max_chars) {
                out.push(window);
            }
            buf.clear();
        }
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

fn split_by_chars(text: &str, max_chars: usize) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    chars
        .chunks(max_chars)
        .map(|c| c.iter().collect::<String>())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;

    #[test]
    fn body_without_headings_is_one_chunk() {
        let body = "Just some text.\n\nAnother paragraph.\n";
        let chunks = chunk_page(body, 1000);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].idx, 0);
        assert!(chunks[0].heading.is_none());
    }

    #[test]
    fn empty_body_produces_no_chunks() {
        let chunks = chunk_page("", 1000);
        assert!(chunks.is_empty());
        let chunks = chunk_page("   \n\n  \n", 1000);
        assert!(chunks.is_empty());
    }

    #[test]
    fn headings_create_chunk_boundaries() {
        let body = "Intro paragraph.\n\n## First\n\nA body.\n\n## Second\n\nB body.\n";
        let chunks = chunk_page(body, 1000);
        assert_eq!(chunks.len(), 3);
        assert!(chunks[0].heading.is_none());
        assert_eq!(chunks[1].heading.as_deref(), Some("First"));
        assert_eq!(chunks[2].heading.as_deref(), Some("Second"));
        // Indices are consecutive.
        assert_eq!(chunks[0].idx, 0);
        assert_eq!(chunks[1].idx, 1);
        assert_eq!(chunks[2].idx, 2);
    }

    #[test]
    fn h3_also_creates_a_chunk_boundary() {
        let body = "### Sub A\n\naaa\n\n### Sub B\n\nbbb\n";
        let chunks = chunk_page(body, 1000);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].heading.as_deref(), Some("Sub A"));
        assert_eq!(chunks[1].heading.as_deref(), Some("Sub B"));
    }

    #[test]
    fn oversize_section_is_split_by_paragraph() {
        let mut body = String::from("## Big\n\n");
        for i in 0..20 {
            let _ = writeln!(body, "Paragraph {i} has some words in it.\n");
        }
        let chunks = chunk_page(&body, 80);
        assert!(chunks.len() > 1, "expected split; got {chunks:?}");
        // Every chunk keeps the same heading.
        for chunk in &chunks {
            assert_eq!(chunk.heading.as_deref(), Some("Big"));
        }
    }

    #[test]
    fn h1_and_h4_are_not_boundaries() {
        let body = "# Title\n\nIntro.\n\n#### Deep\n\nstill grouped.\n";
        let chunks = chunk_page(body, 1000);
        assert_eq!(chunks.len(), 1, "no H2/H3 → one chunk");
    }
}
