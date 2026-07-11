//! Atomic-span detection for markdown-aware chunking.
//!
//! Fenced/indented code blocks and GFM tables lose their meaning when a
//! chunk boundary lands inside them: half a table embeds as pipe-noise and
//! half a code block embeds as syntax soup. This module finds the byte
//! ranges of those blocks with a real markdown parse (pulldown-cmark) so
//! every chunker can treat them as indivisible units.
//!
//! The contract: [`segment_atomic`] returns a contiguous cover of the
//! input — concatenating the segment texts reproduces the input exactly.
//! Chunkers split freely inside non-atomic segments and never inside
//! atomic ones.

use std::ops::Range;

use pulldown_cmark::{Event, Options, Parser, Tag};

/// One contiguous piece of a text: either protected markdown structure
/// (code block / table) or ordinary prose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment<'a> {
    pub text: &'a str,
    pub atomic: bool,
}

/// Byte ranges of atomic blocks (code blocks and tables), sorted and merged.
pub fn atomic_spans(text: &str) -> Vec<Range<usize>> {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    let mut spans: Vec<Range<usize>> = Parser::new_ext(text, opts)
        .into_offset_iter()
        .filter_map(|(event, range)| match event {
            // For container tags the Start event's range covers the whole
            // element, fences/delimiters included.
            Event::Start(Tag::CodeBlock(_) | Tag::Table(_)) => Some(range),
            _ => None,
        })
        .collect();
    spans.sort_by_key(|r| (r.start, r.end));
    merge_overlapping(spans)
}

/// Split `text` into a contiguous cover of atomic and non-atomic segments.
/// Concatenating `segment.text` in order reproduces `text` byte-for-byte.
pub fn segment_atomic(text: &str) -> Vec<Segment<'_>> {
    if text.is_empty() {
        return Vec::new();
    }
    let spans = atomic_spans(text);
    if spans.is_empty() {
        return vec![Segment {
            text,
            atomic: false,
        }];
    }
    let mut segments = Vec::with_capacity(spans.len() * 2 + 1);
    let mut pos = 0usize;
    for span in spans {
        let start = span.start.min(text.len());
        let end = span.end.min(text.len());
        if start > pos {
            segments.push(Segment {
                text: &text[pos..start],
                atomic: false,
            });
        }
        if end > start {
            segments.push(Segment {
                text: &text[start..end],
                atomic: true,
            });
        }
        pos = pos.max(end);
    }
    if pos < text.len() {
        segments.push(Segment {
            text: &text[pos..],
            atomic: false,
        });
    }
    segments.retain(|s| !s.text.is_empty());
    segments
}

fn merge_overlapping(spans: Vec<Range<usize>>) -> Vec<Range<usize>> {
    let mut merged: Vec<Range<usize>> = Vec::with_capacity(spans.len());
    for span in spans {
        match merged.last_mut() {
            Some(last) if span.start <= last.end => last.end = last.end.max(span.end),
            _ => merged.push(span),
        }
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_is_one_normal_segment() {
        let text = "Just prose.\n\nMore prose.\n";
        let segments = segment_atomic(text);
        assert_eq!(segments.len(), 1);
        assert!(!segments[0].atomic);
        assert_eq!(segments[0].text, text);
    }

    #[test]
    fn fenced_code_block_is_atomic() {
        let text = "Before.\n\n```rust\nlet x = 1;\n\nlet y = 2;\n```\n\nAfter.\n";
        let segments = segment_atomic(text);
        let atomic: Vec<_> = segments.iter().filter(|s| s.atomic).collect();
        assert_eq!(atomic.len(), 1, "one code block: {segments:?}");
        assert!(atomic[0].text.contains("let x = 1;"));
        assert!(atomic[0].text.contains("let y = 2;"));
        assert!(atomic[0].text.starts_with("```"));
    }

    #[test]
    fn gfm_table_is_atomic() {
        let text = "Intro.\n\n| a | b |\n|---|---|\n| 1 | 2 |\n| 3 | 4 |\n\nOutro.\n";
        let segments = segment_atomic(text);
        let atomic: Vec<_> = segments.iter().filter(|s| s.atomic).collect();
        assert_eq!(atomic.len(), 1, "one table: {segments:?}");
        assert!(atomic[0].text.contains("| 1 | 2 |"));
        assert!(atomic[0].text.contains("| 3 | 4 |"));
    }

    #[test]
    fn cover_is_lossless() {
        let text = "A.\n\n```\ncode\n```\n\nB.\n\n| x |\n|---|\n| 1 |\n\nC.\n";
        let segments = segment_atomic(text);
        let rebuilt: String = segments.iter().map(|s| s.text).collect();
        assert_eq!(rebuilt, text, "segments must cover the input exactly");
        assert_eq!(segments.iter().filter(|s| s.atomic).count(), 2);
    }

    #[test]
    fn fake_heading_inside_fence_stays_in_the_block() {
        let text = "Before.\n\n```md\n## not a real heading\n```\n\nAfter.\n";
        let segments = segment_atomic(text);
        let atomic: Vec<_> = segments.iter().filter(|s| s.atomic).collect();
        assert_eq!(atomic.len(), 1);
        assert!(atomic[0].text.contains("## not a real heading"));
    }

    #[test]
    fn empty_and_no_block_inputs_are_safe() {
        assert!(segment_atomic("").is_empty());
        let spans = atomic_spans("no blocks here at all");
        assert!(spans.is_empty());
    }
}
