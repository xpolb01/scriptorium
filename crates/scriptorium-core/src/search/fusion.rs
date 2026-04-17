//! Reciprocal Rank Fusion (RRF) for merging ranked result lists.
//!
//! RRF is a score-agnostic rank fusion method: each result's contribution
//! from a given list is `1 / (K + rank)`, where `K` is a constant that
//! controls how quickly lower-ranked results lose influence. Results that
//! appear in multiple lists accumulate contributions and naturally rank
//! higher.
//!
//! Reference: Cormack, Clarke & Buettcher, "Reciprocal Rank Fusion
//! outperforms Condorcet and individual Rank Learning Methods" (SIGIR 2009).

use std::collections::HashMap;

use crate::embed::SearchHit;
#[cfg(test)]
use crate::vault::PageId;

/// Smoothing constant. Higher values flatten the rank curve (more weight to
/// lower-ranked results). 60 is the standard value from the original RRF
/// paper and matches `GBrain`'s implementation.
pub const RRF_K: usize = 60;

/// Fuse multiple ranked result lists into a single list ordered by
/// accumulated RRF score.
///
/// Each input list is assumed to be pre-sorted by relevance (best first).
/// The returned list is sorted by fused score descending. The `score` field
/// on each returned `SearchHit` is overwritten with the fused RRF score.
pub fn rrf_fuse(lists: &[Vec<SearchHit>]) -> Vec<SearchHit> {
    if lists.is_empty() {
        return Vec::new();
    }

    // Accumulate RRF scores keyed by a stable identity string.
    let mut scores: HashMap<String, f32> = HashMap::new();
    // Keep the best (first-encountered) SearchHit for each key.
    let mut representatives: HashMap<String, SearchHit> = HashMap::new();

    for list in lists {
        for (rank, hit) in list.iter().enumerate() {
            let key = identity_key(hit);
            #[allow(clippy::cast_precision_loss)]
            let contribution = 1.0 / (RRF_K + rank) as f32;
            *scores.entry(key.clone()).or_default() += contribution;
            representatives.entry(key).or_insert_with(|| hit.clone());
        }
    }

    // Build the fused list, replacing each hit's score with the RRF score.
    let mut fused: Vec<SearchHit> = representatives
        .into_iter()
        .map(|(key, mut hit)| {
            hit.score = scores[&key];
            hit
        })
        .collect();

    fused.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    fused
}

/// Stable identity key for merging results across lists.
/// Uses `page_id` + first 50 chars of `chunk_text` to identify the same
/// logical chunk even when scores differ across retrieval methods.
fn identity_key(hit: &SearchHit) -> String {
    let text_prefix = if hit.chunk_text.len() > 50 {
        &hit.chunk_text[..hit
            .chunk_text
            .char_indices()
            .nth(50)
            .map_or(hit.chunk_text.len(), |(i, _)| i)]
    } else {
        &hit.chunk_text
    };
    format!("{}:{}", hit.page_id, text_prefix)
}

/// Build a `SearchHit` for testing. Not `pub` — only used in this module's
/// tests.
#[cfg(test)]
fn test_hit(page_id: PageId, chunk_idx: usize, text: &str, score: f32) -> SearchHit {
    SearchHit {
        page_id,
        chunk_idx,
        heading: None,
        chunk_text: text.to_string(),
        score,
        page_path: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::PageId;

    #[test]
    fn single_list_preserves_order() {
        let id = PageId::new();
        let list = vec![
            test_hit(id, 0, "first", 0.9),
            test_hit(id, 1, "second", 0.5),
            test_hit(id, 2, "third", 0.1),
        ];
        let fused = rrf_fuse(&[list]);
        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].chunk_text, "first");
        assert_eq!(fused[1].chunk_text, "second");
        assert_eq!(fused[2].chunk_text, "third");
    }

    #[test]
    fn multi_list_boosts_shared_results() {
        let a = PageId::new();
        let b = PageId::new();
        // "shared" appears in both lists; "only_vec" and "only_kw" appear in one each.
        let vec_list = vec![
            test_hit(a, 0, "shared chunk text here", 0.9),
            test_hit(b, 0, "only_vec result", 0.5),
        ];
        let kw_list = vec![
            test_hit(a, 0, "shared chunk text here", 5.0),
            test_hit(b, 1, "only_kw result", 3.0),
        ];
        let fused = rrf_fuse(&[vec_list, kw_list]);
        // "shared" should be first because it gets contributions from both lists.
        assert_eq!(fused[0].chunk_text, "shared chunk text here");
        assert!(fused[0].score > fused[1].score);
    }

    #[test]
    fn disjoint_lists_interleave_by_score() {
        let a = PageId::new();
        let b = PageId::new();
        let list1 = vec![test_hit(a, 0, "alpha", 0.9)];
        let list2 = vec![test_hit(b, 0, "beta", 0.9)];
        let fused = rrf_fuse(&[list1, list2]);
        assert_eq!(fused.len(), 2);
        // Both at rank 0 in their respective lists → same RRF score.
        assert!((fused[0].score - fused[1].score).abs() < f32::EPSILON);
    }

    #[test]
    fn empty_lists_return_empty() {
        assert!(rrf_fuse(&[]).is_empty());
        assert!(rrf_fuse(&[vec![], vec![]]).is_empty());
    }
}
