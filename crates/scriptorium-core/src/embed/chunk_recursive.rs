//! Recursive delimiter-hierarchy chunker.
//!
//! Splits text through five levels of decreasing granularity:
//!
//! | Level | Delimiters |
//! |-------|-----------|
//! | L0 | `\n\n` (paragraphs) |
//! | L1 | `\n` (lines) |
//! | L2 | `. `, `! `, `? `, `.\n`, `!\n`, `?\n` (sentences) |
//! | L3 | `; `, `: `, `, ` (clauses) |
//! | L4 | whitespace (words) |
//!
//! After splitting, small adjacent pieces are greedily merged up to
//! 1.5× the target size. Like the heading chunker, sections are first
//! separated by H2/H3 headings to preserve heading context.
//!
//! Reference: `GBrain`'s `src/core/chunkers/recursive.ts`.

use super::chunk::{split_into_sections, Chunk};

/// Five levels of delimiters, from coarsest to finest.
const LEVELS: &[&[&str]] = &[
    &["\n\n"],                                       // L0: paragraphs
    &["\n"],                                         // L1: lines
    &[". ", "! ", "? ", ".\n", "!\n", "?\n"],       // L2: sentences
    &["; ", ": ", ", "],                             // L3: clauses
    &[],                                             // L4: whitespace (special-cased)
];

/// Split `body` into chunks using the recursive delimiter hierarchy.
/// Preserves heading context from H2/H3 headings.
pub fn chunk_page_recursive(body: &str, max_chars: usize) -> Vec<Chunk> {
    assert!(max_chars > 0, "max_chars must be positive");
    let sections = split_into_sections(body);

    let mut out = Vec::new();
    let mut idx = 0usize;
    for section in sections {
        let pieces = recursive_split(&section.text, 0, max_chars);
        let merged = greedy_merge(&pieces, max_chars * 3 / 2);
        for text in merged {
            if !text.trim().is_empty() {
                out.push(Chunk {
                    idx,
                    heading: section.heading.clone(),
                    text,
                });
                idx += 1;
            }
        }
    }
    out
}

/// Recursively split `text` using delimiters at `level` and deeper.
fn recursive_split(text: &str, level: usize, max_chars: usize) -> Vec<String> {
    if text.chars().count() <= max_chars {
        return vec![text.to_string()];
    }
    if level >= LEVELS.len() {
        // Past L4: hard split by character count as last resort.
        return split_by_chars(text, max_chars);
    }

    let delimiters = LEVELS[level];
    let pieces = if delimiters.is_empty() {
        // L4: split on whitespace.
        split_on_whitespace(text, max_chars)
    } else {
        split_at_delimiters(text, delimiters)
    };

    // If splitting produced only one piece, try the next level.
    if pieces.len() <= 1 {
        return recursive_split(text, level + 1, max_chars);
    }

    // Recurse on any piece that's still too large.
    let mut result = Vec::new();
    for piece in pieces {
        if piece.chars().count() > max_chars {
            result.extend(recursive_split(&piece, level + 1, max_chars));
        } else {
            result.push(piece);
        }
    }
    result
}

/// Split `text` at any of the given delimiters. The delimiter is kept at
/// the end of the preceding piece (lossless).
fn split_at_delimiters(text: &str, delimiters: &[&str]) -> Vec<String> {
    let mut pieces = Vec::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        // Find the earliest delimiter occurrence.
        let mut earliest_end: Option<usize> = None;
        for delim in delimiters {
            if let Some(pos) = remaining.find(delim) {
                let end = pos + delim.len();
                match earliest_end {
                    None => earliest_end = Some(end),
                    Some(prev) if end < prev => earliest_end = Some(end),
                    _ => {}
                }
            }
        }
        if let Some(end) = earliest_end {
            pieces.push(remaining[..end].to_string());
            remaining = &remaining[end..];
        } else {
            pieces.push(remaining.to_string());
            break;
        }
    }
    pieces
}

/// L4: split on whitespace boundaries, targeting `max_chars` per piece.
fn split_on_whitespace(text: &str, max_chars: usize) -> Vec<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.is_empty() {
        return vec![text.to_string()];
    }
    let mut pieces = Vec::new();
    let mut buf = String::new();
    for word in words {
        let needed = if buf.is_empty() {
            word.len()
        } else {
            1 + word.len() // space + word
        };
        if buf.chars().count() + needed > max_chars && !buf.is_empty() {
            pieces.push(std::mem::take(&mut buf));
        }
        if !buf.is_empty() {
            buf.push(' ');
        }
        buf.push_str(word);
    }
    if !buf.is_empty() {
        pieces.push(buf);
    }
    pieces
}

/// Hard split by character count (last resort).
fn split_by_chars(text: &str, max_chars: usize) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    chars
        .chunks(max_chars)
        .map(|c| c.iter().collect::<String>())
        .collect()
}

/// Greedily merge adjacent small pieces as long as the combined size
/// stays under `ceiling`.
fn greedy_merge(pieces: &[String], ceiling: usize) -> Vec<String> {
    let mut merged = Vec::new();
    let mut buf = String::new();
    for piece in pieces {
        if buf.is_empty() {
            piece.clone_into(&mut buf);
        } else if buf.chars().count() + piece.chars().count() <= ceiling {
            buf.push_str(piece);
        } else {
            merged.push(std::mem::take(&mut buf));
            piece.clone_into(&mut buf);
        }
    }
    if !buf.is_empty() {
        merged.push(buf);
    }
    merged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recursive_splits_by_paragraph_first() {
        let body = "Para one.\n\nPara two.\n\nPara three.\n\nPara four.\n";
        let chunks = chunk_page_recursive(body, 30);
        assert!(chunks.len() > 1, "should split into multiple chunks");
        // All text should be preserved (no data loss).
        let total: String = chunks.iter().map(|c| c.text.as_str()).collect();
        assert!(total.contains("Para one"));
        assert!(total.contains("Para four"));
    }

    #[test]
    fn recursive_falls_through_levels() {
        // A single long line with no paragraph breaks → falls to L1 (lines),
        // then L2 (sentences), etc.
        let body = "Sentence one. Sentence two. Sentence three. Sentence four. Sentence five.";
        let chunks = chunk_page_recursive(body, 40);
        assert!(chunks.len() > 1, "should split by sentence");
    }

    #[test]
    fn recursive_greedy_merge_combines_small_pieces() {
        let pieces = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
        ];
        let merged = greedy_merge(&pieces, 5);
        assert_eq!(merged.len(), 1, "three tiny pieces should merge into one");
        assert_eq!(merged[0], "abc");
    }

    #[test]
    fn recursive_preserves_heading_context() {
        let body = "## Section A\n\nContent A.\n\n## Section B\n\nContent B.\n";
        let chunks = chunk_page_recursive(body, 1000);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].heading.as_deref(), Some("Section A"));
        assert_eq!(chunks[1].heading.as_deref(), Some("Section B"));
    }

    #[test]
    fn recursive_matches_heading_chunker_on_simple_input() {
        // For well-headed documents within budget, both chunkers produce
        // the same number of chunks with the same headings.
        let body = "## Alpha\n\nFirst.\n\n## Beta\n\nSecond.\n";
        let heading_chunks = super::super::chunk::chunk_page(body, 1000);
        let recursive_chunks = chunk_page_recursive(body, 1000);
        assert_eq!(heading_chunks.len(), recursive_chunks.len());
        for (h, r) in heading_chunks.iter().zip(recursive_chunks.iter()) {
            assert_eq!(h.heading, r.heading);
        }
    }
}
