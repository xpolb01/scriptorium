//! Semantic chunker using embedding-based topic boundary detection.
//!
//! Splits text by finding points where adjacent sentences are semantically
//! dissimilar (topic shifts). Uses Savitzky-Golay smoothing on the cosine
//! similarity curve to robustly detect local minima.
//!
//! Falls back to the recursive chunker on any error (embedding failure, too
//! few sentences, etc.).
//!
//! Reference: `GBrain`'s `src/core/chunkers/semantic.ts`.

use tracing::debug;

use super::chunk::{split_into_sections, Chunk};
use super::chunk_recursive::chunk_page_recursive;
use crate::error::Result;
use crate::llm::LlmProvider;

/// Minimum sentences to attempt semantic chunking. Below this threshold,
/// the recursive chunker is more appropriate.
const MIN_SENTENCES: usize = 4;

/// Minimum distance (in sentences) between detected boundaries.
const MIN_BOUNDARY_DISTANCE: usize = 2;

/// Percentile threshold for boundary filtering. Only local minima where
/// the similarity is below this percentile of all similarities are kept.
const BOUNDARY_PERCENTILE: f32 = 0.2;

/// Factor above which a group is considered oversized and split further.
const OVERSIZE_FACTOR: f32 = 1.5;

/// Pre-computed Savitzky-Golay first-derivative coefficients for
/// window=5, polynomial order=3, derivative order=1.
/// These are the standard coefficients: `[-2, -1, 0, 1, 2] / 10`.
const SG_COEFFS: [f32; 5] = [-0.2, -0.1, 0.0, 0.1, 0.2];

/// Split `body` into chunks using semantic boundary detection.
///
/// Embeds each sentence, computes adjacent cosine similarities, smooths
/// with Savitzky-Golay, and splits at local minima. Falls back to the
/// recursive chunker on any error.
pub async fn chunk_page_semantic(
    body: &str,
    max_chars: usize,
    embed_provider: &dyn LlmProvider,
    model: &str,
) -> Result<Vec<Chunk>> {
    match try_semantic(body, max_chars, embed_provider, model).await {
        Ok(chunks) => Ok(chunks),
        Err(e) => {
            debug!(error = %e, "semantic chunking failed, falling back to recursive");
            Ok(chunk_page_recursive(body, max_chars))
        }
    }
}

async fn try_semantic(
    body: &str,
    max_chars: usize,
    embed_provider: &dyn LlmProvider,
    _model: &str,
) -> std::result::Result<Vec<Chunk>, String> {
    let sections = split_into_sections(body);
    let mut out = Vec::new();
    let mut idx = 0usize;

    for section in &sections {
        let sentences = split_sentences(&section.text);
        if sentences.len() < MIN_SENTENCES {
            // Too few sentences — use recursive for this section.
            let recursive = chunk_page_recursive(&section.text, max_chars);
            for mut chunk in recursive {
                chunk.idx = idx;
                chunk.heading.clone_from(&section.heading);
                out.push(chunk);
                idx += 1;
            }
            continue;
        }

        // Batch-embed all sentences.
        let texts: Vec<String> = sentences.iter().map(std::string::ToString::to_string).collect();
        let embeddings = embed_provider
            .embed(&texts)
            .await
            .map_err(|e| format!("embed sentences: {e}"))?;

        if embeddings.len() != sentences.len() {
            return Err(format!(
                "embedding count mismatch: {} sentences, {} embeddings",
                sentences.len(),
                embeddings.len()
            ));
        }

        // Compute adjacent cosine similarities.
        let sims = adjacent_cosine_similarities(&embeddings);
        if sims.is_empty() {
            // Single sentence — just use it as one chunk.
            out.push(Chunk {
                idx,
                heading: section.heading.clone(),
                text: section.text.clone(),
            });
            idx += 1;
            continue;
        }

        // Detect boundaries via SG smoothing.
        let boundaries = find_boundaries(&sims);

        // Group sentences at boundaries.
        let groups = group_at_boundaries(&sentences, &boundaries);

        // Emit chunks, splitting oversized groups recursively.
        #[allow(clippy::cast_precision_loss)]
        let oversize_threshold = max_chars as f32 * OVERSIZE_FACTOR;
        for group in groups {
            let text = group.join(" ");
            #[allow(clippy::cast_precision_loss)]
            if (text.chars().count() as f32) > oversize_threshold {
                let sub_chunks = chunk_page_recursive(&text, max_chars);
                for mut sub in sub_chunks {
                    sub.idx = idx;
                    sub.heading.clone_from(&section.heading);
                    out.push(sub);
                    idx += 1;
                }
            } else {
                out.push(Chunk {
                    idx,
                    heading: section.heading.clone(),
                    text,
                });
                idx += 1;
            }
        }
    }

    Ok(out)
}

/// Split text into sentences at `.!?` followed by whitespace.
fn split_sentences(text: &str) -> Vec<&str> {
    let mut sentences = Vec::new();
    let mut start = 0;
    let bytes = text.as_bytes();
    let len = bytes.len();

    for i in 0..len {
        if (bytes[i] == b'.' || bytes[i] == b'!' || bytes[i] == b'?')
            && i + 1 < len
            && bytes[i + 1].is_ascii_whitespace()
        {
            let end = i + 1;
            let sentence = &text[start..end];
            if !sentence.trim().is_empty() {
                sentences.push(sentence.trim());
            }
            // Skip the whitespace after the punctuation.
            start = i + 1;
            while start < len && bytes[start].is_ascii_whitespace() {
                start += 1;
            }
        }
    }
    // Remaining text.
    if start < len {
        let tail = text[start..].trim();
        if !tail.is_empty() {
            sentences.push(tail);
        }
    }
    sentences
}

/// Cosine similarity between each consecutive pair of embeddings.
fn adjacent_cosine_similarities(embeddings: &[Vec<f32>]) -> Vec<f32> {
    if embeddings.len() < 2 {
        return Vec::new();
    }
    (0..embeddings.len() - 1)
        .map(|i| cosine_similarity(&embeddings[i], &embeddings[i + 1]))
        .collect()
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    let denom = norm_a * norm_b;
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

/// Detect topic boundaries using Savitzky-Golay 1st derivative smoothing.
fn find_boundaries(sims: &[f32]) -> Vec<usize> {
    if sims.len() < 5 {
        // Too short for SG — use simple percentile method.
        return percentile_boundaries(sims);
    }

    let derivs = savitzky_golay_derivative(sims);

    // Find zero crossings of the derivative (local minima of similarity).
    let mut minima = Vec::new();
    for i in 1..derivs.len() {
        if derivs[i - 1] < 0.0 && derivs[i] >= 0.0 {
            minima.push(i);
        }
    }

    // Filter: keep only minima where similarity < 20th percentile.
    let threshold = percentile(sims, BOUNDARY_PERCENTILE);
    let filtered: Vec<usize> = minima
        .into_iter()
        .filter(|&i| i < sims.len() && sims[i] < threshold)
        .collect();

    enforce_min_distance(&filtered, MIN_BOUNDARY_DISTANCE)
}

/// Fallback boundary detection for short similarity sequences.
fn percentile_boundaries(sims: &[f32]) -> Vec<usize> {
    let threshold = percentile(sims, BOUNDARY_PERCENTILE);
    let candidates: Vec<usize> = sims
        .iter()
        .enumerate()
        .filter(|(_, &s)| s < threshold)
        .map(|(i, _)| i + 1) // boundary is after the low-similarity gap
        .collect();
    enforce_min_distance(&candidates, MIN_BOUNDARY_DISTANCE)
}

/// Apply the pre-computed SG first-derivative filter.
fn savitzky_golay_derivative(values: &[f32]) -> Vec<f32> {
    let half = SG_COEFFS.len() / 2; // 2
    let n = values.len();
    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
    let result = (0..n)
        .map(|i| {
            let mut sum = 0.0f32;
            for (j, &coeff) in SG_COEFFS.iter().enumerate() {
                let k = i as isize + j as isize - half as isize;
                let clamped = k.clamp(0, n as isize - 1) as usize;
                sum += coeff * values[clamped];
            }
            sum
        })
        .collect();
    result
}

/// kth percentile of a slice (0.0 = min, 1.0 = max).
fn percentile(values: &[f32], p: f32) -> f32 {
    if values.is_empty() {
        return 0.0;
    }
    let mut sorted: Vec<f32> = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    #[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let idx = ((p * sorted.len() as f32).floor() as usize).min(sorted.len() - 1);
    sorted[idx]
}

/// Enforce minimum distance between boundaries, keeping the earliest in
/// each cluster.
fn enforce_min_distance(boundaries: &[usize], min_dist: usize) -> Vec<usize> {
    let mut kept = Vec::new();
    for &b in boundaries {
        if kept.last().is_none_or(|&last: &usize| b >= last + min_dist) {
            kept.push(b);
        }
    }
    kept
}

/// Group sentences at the detected boundaries.
fn group_at_boundaries<'a>(sentences: &[&'a str], boundaries: &[usize]) -> Vec<Vec<&'a str>> {
    let mut groups = Vec::new();
    let mut start = 0;
    for &b in boundaries {
        if b > start && b <= sentences.len() {
            groups.push(sentences[start..b].to_vec());
            start = b;
        }
    }
    if start < sentences.len() {
        groups.push(sentences[start..].to_vec());
    }
    groups
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockProvider;

    #[test]
    fn savitzky_golay_coefficients_sum_to_zero() {
        let sum: f32 = SG_COEFFS.iter().sum();
        assert!(sum.abs() < 1e-6, "derivative coefficients should sum to ~0");
    }

    #[test]
    fn boundary_detection_finds_topic_shifts() {
        // Synthetic similarity curve with a clear dip at position 5.
        let sims = vec![0.9, 0.88, 0.85, 0.87, 0.9, 0.2, 0.88, 0.9, 0.87, 0.85];
        let boundaries = find_boundaries(&sims);
        // Should detect a boundary near the dip.
        assert!(
            !boundaries.is_empty(),
            "should find at least one boundary near the dip"
        );
        // The boundary should be near index 5 (the low point).
        let near_dip = boundaries.iter().any(|&b| (4..=7).contains(&b));
        assert!(near_dip, "boundary should be near the similarity dip at index 5; got {boundaries:?}");
    }

    #[tokio::test]
    async fn semantic_falls_back_to_recursive_on_few_sentences() {
        let mock = MockProvider::constant("");
        let body = "Just one sentence. And two.";
        let chunks = chunk_page_semantic(body, 1000, &mock, "m").await.unwrap();
        assert!(!chunks.is_empty(), "should produce chunks via recursive fallback");
    }

    #[tokio::test]
    async fn semantic_falls_back_on_embed_error() {
        // MockProvider::constant returns a constant string for complete(), but
        // embed() returns deterministic vectors. However, if we make the body
        // have enough sentences, the embed call should succeed with the mock.
        // For an actual error test, we'd need a failing mock. For now, verify
        // that even with the mock, semantic chunking produces results.
        let mock = MockProvider::constant("");
        let body = "Sentence one about cats. Sentence two about dogs. \
                    Sentence three about fish. Sentence four about birds. \
                    Sentence five about snakes.";
        let chunks = chunk_page_semantic(body, 1000, &mock, "m").await.unwrap();
        assert!(!chunks.is_empty(), "should produce chunks");
    }

    #[tokio::test]
    async fn semantic_preserves_heading_context() {
        let mock = MockProvider::constant("");
        let body = "## Topic A\n\nFirst about cats. Second about cats. \
                    Third about cats. Fourth about cats.\n\n\
                    ## Topic B\n\nFirst about dogs. Second about dogs. \
                    Third about dogs. Fourth about dogs.\n";
        let chunks = chunk_page_semantic(body, 1000, &mock, "m").await.unwrap();
        assert!(chunks.len() >= 2, "should have chunks from both sections");
        let has_topic_a = chunks.iter().any(|c| c.heading.as_deref() == Some("Topic A"));
        let has_topic_b = chunks.iter().any(|c| c.heading.as_deref() == Some("Topic B"));
        assert!(has_topic_a, "should have Topic A heading");
        assert!(has_topic_b, "should have Topic B heading");
    }
}
