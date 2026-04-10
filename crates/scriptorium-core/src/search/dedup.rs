//! Four-layer deduplication pipeline for search results.
//!
//! Runs after RRF fusion to clean up the merged result set:
//!
//! 1. **Merge duplicates** — same `(page_id, chunk_idx)` from different
//!    retrieval paths → keep highest score.
//! 2. **Jaccard text filter** — near-duplicate text across different chunks
//!    → drop the lower-scored one.
//! 3. **Type diversity** — no single page type (derived from wiki directory)
//!    dominates the result set.
//! 4. **Per-page cap** — at most N chunks from the same page.
//!
//! Reference: `GBrain`'s `src/core/search/dedup.ts`.

use std::collections::{HashMap, HashSet};

use crate::embed::SearchHit;
use crate::vault::PageId;

/// Configuration for the dedup pipeline.
#[derive(Debug, Clone)]
pub struct DedupConfig {
    /// Jaccard word-set similarity threshold. Pairs above this are considered
    /// near-duplicates and the lower-scored one is dropped.
    pub jaccard_threshold: f32,
    /// Maximum fraction of results from any single page type. Types are
    /// derived from the wiki directory path (e.g. `wiki/concepts/` →
    /// `"concepts"`). Set to 1.0 to disable.
    pub max_type_ratio: f32,
    /// Maximum chunks per page in the final result set.
    pub max_per_page: usize,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            jaccard_threshold: 0.85,
            max_type_ratio: 0.60,
            max_per_page: 2,
        }
    }
}

/// Run all four dedup layers in sequence. Input should be sorted by score
/// descending (as returned by [`super::fusion::rrf_fuse`]).
pub fn dedup_pipeline(results: Vec<SearchHit>, config: &DedupConfig) -> Vec<SearchHit> {
    let r = merge_duplicates(results);
    let r = filter_jaccard(r, config.jaccard_threshold);
    let r = enforce_type_diversity(r, config.max_type_ratio);
    cap_per_page(r, config.max_per_page)
}

/// Layer 1: merge identical `(page_id, chunk_idx)` pairs, keeping the
/// highest score.
fn merge_duplicates(results: Vec<SearchHit>) -> Vec<SearchHit> {
    let mut best: HashMap<(PageId, usize), SearchHit> = HashMap::new();
    for hit in results {
        let key = (hit.page_id, hit.chunk_idx);
        best.entry(key)
            .and_modify(|existing| {
                if hit.score > existing.score {
                    *existing = hit.clone();
                }
            })
            .or_insert(hit);
    }
    let mut out: Vec<SearchHit> = best.into_values().collect();
    out.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
    out
}

/// Layer 2: greedy Jaccard word-set filter. For each candidate (in score
/// order), skip it if its text is too similar to any already-kept result.
fn filter_jaccard(results: Vec<SearchHit>, threshold: f32) -> Vec<SearchHit> {
    let mut kept: Vec<SearchHit> = Vec::new();
    for hit in results {
        let dominated = kept.iter().any(|k| jaccard(&hit.chunk_text, &k.chunk_text) > threshold);
        if !dominated {
            kept.push(hit);
        }
    }
    kept
}

/// Layer 3: type diversity enforcement. No single page type exceeds
/// `ceil(n * max_type_ratio)` results.
fn enforce_type_diversity(results: Vec<SearchHit>, max_type_ratio: f32) -> Vec<SearchHit> {
    if max_type_ratio >= 1.0 {
        return results;
    }
    let n = results.len();
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let max_per_type = ((n as f32 * max_type_ratio).ceil() as usize).max(1);
    let mut type_counts: HashMap<String, usize> = HashMap::new();
    let mut kept = Vec::new();
    for hit in results {
        let page_type = derive_page_type(&hit);
        let count = type_counts.entry(page_type).or_default();
        if *count < max_per_type {
            *count += 1;
            kept.push(hit);
        }
    }
    kept
}

/// Layer 4: per-page chunk cap.
fn cap_per_page(results: Vec<SearchHit>, max: usize) -> Vec<SearchHit> {
    let mut page_counts: HashMap<PageId, usize> = HashMap::new();
    let mut kept = Vec::new();
    for hit in results {
        let count = page_counts.entry(hit.page_id).or_default();
        if *count < max {
            *count += 1;
            kept.push(hit);
        }
    }
    kept
}

/// Derive the page "type" from a `SearchHit`. Uses the `page_path` field
/// if populated (e.g. `wiki/concepts/foo.md` → `"concepts"`), otherwise
/// falls back to `"unknown"`.
fn derive_page_type(hit: &SearchHit) -> String {
    hit.page_path
        .as_deref()
        .and_then(|p| {
            // Strip leading "wiki/" then take the first directory component.
            let stripped = p.strip_prefix("wiki/").unwrap_or(p);
            stripped.split('/').next().filter(|s| !s.contains('.'))
        })
        .unwrap_or("unknown")
        .to_string()
}

/// Word-set Jaccard similarity: |A ∩ B| / |A ∪ B|.
fn jaccard(a: &str, b: &str) -> f32 {
    let wa: HashSet<&str> = a.split_whitespace().collect();
    let wb: HashSet<&str> = b.split_whitespace().collect();
    let inter = wa.intersection(&wb).count();
    let union = wa.union(&wb).count();
    if union == 0 {
        0.0
    } else {
        #[allow(clippy::cast_precision_loss)]
        let sim = inter as f32 / union as f32;
        sim
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::PageId;

    fn hit(page_id: PageId, idx: usize, text: &str, score: f32) -> SearchHit {
        SearchHit {
            page_id,
            chunk_idx: idx,
            heading: None,
            chunk_text: text.to_string(),
            score,
            page_path: None,
        }
    }

    fn hit_with_path(page_id: PageId, idx: usize, text: &str, score: f32, path: &str) -> SearchHit {
        SearchHit {
            page_id,
            chunk_idx: idx,
            heading: None,
            chunk_text: text.to_string(),
            score,
            page_path: Some(path.to_string()),
        }
    }

    #[test]
    fn merge_keeps_highest_score() {
        let id = PageId::new();
        let results = vec![
            hit(id, 0, "same chunk", 0.5),
            hit(id, 0, "same chunk", 0.9),
        ];
        let merged = merge_duplicates(results);
        assert_eq!(merged.len(), 1);
        assert!((merged[0].score - 0.9).abs() < f32::EPSILON);
    }

    #[test]
    fn jaccard_removes_near_duplicates() {
        let a = PageId::new();
        let b = PageId::new();
        // Use 20-word sentences differing in only 1 word.
        // Shared: 19 unique, union: 20, Jaccard = 19/20 = 0.95 > 0.85.
        let results = vec![
            hit(a, 0, "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra tango", 0.9),
            hit(b, 0, "alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike november oscar papa quebec romeo sierra uniform", 0.5),
        ];
        let filtered = filter_jaccard(results, 0.85);
        assert_eq!(filtered.len(), 1, "near-duplicate should be removed");
        assert_eq!(filtered[0].page_id, a, "higher-scored hit survives");
    }

    #[test]
    fn jaccard_keeps_dissimilar_chunks() {
        let a = PageId::new();
        let b = PageId::new();
        let results = vec![
            hit(a, 0, "photosynthesis converts sunlight to energy", 0.9),
            hit(b, 0, "quantum mechanics describes particle behavior", 0.5),
        ];
        let filtered = filter_jaccard(results, 0.85);
        assert_eq!(filtered.len(), 2, "dissimilar chunks should both survive");
    }

    #[test]
    fn type_diversity_caps_dominant_type() {
        let ids: Vec<PageId> = (0..10).map(|_| PageId::new()).collect();
        let results: Vec<SearchHit> = ids
            .iter()
            .enumerate()
            .map(|(i, &id)| {
                hit_with_path(id, 0, &format!("concept {i}"), 1.0 - i as f32 * 0.05, "wiki/concepts/foo.md")
            })
            .collect();
        let filtered = enforce_type_diversity(results, 0.6);
        // ceil(10 * 0.6) = 6
        assert_eq!(filtered.len(), 6, "should cap at 60% of results for one type");
    }

    #[test]
    fn type_diversity_allows_minority_types() {
        let a = PageId::new();
        let b = PageId::new();
        let results = vec![
            hit_with_path(a, 0, "a concept", 0.9, "wiki/concepts/a.md"),
            hit_with_path(b, 0, "an entity", 0.8, "wiki/entities/b.md"),
        ];
        // ceil(2 * 0.6) = 2 per type — both fit.
        let filtered = enforce_type_diversity(results, 0.6);
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn per_page_cap_limits_chunks() {
        let id = PageId::new();
        let results = vec![
            hit(id, 0, "chunk zero", 0.9),
            hit(id, 1, "chunk one", 0.8),
            hit(id, 2, "chunk two", 0.7),
        ];
        let capped = cap_per_page(results, 2);
        assert_eq!(capped.len(), 2);
    }
}
