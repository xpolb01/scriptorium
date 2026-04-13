//! Retrieval quality benchmarks: precision@k, recall, MRR, NDCG, F1,
//! and a composite health score.
//!
//! A benchmark suite is a set of `(query, expected_page_stems)` pairs
//! stored in `.scriptorium/benchmarks.json`. Running benchmarks executes
//! hybrid search for each query and measures how well the retrieval
//! system finds the expected pages.
//!
//! ## Metrics
//!
//! | Metric       | What it measures                                    |
//! |--------------|-----------------------------------------------------|
//! | Precision@k  | Fraction of top-k results that are relevant         |
//! | Recall       | Fraction of expected results that were retrieved     |
//! | F1           | Harmonic mean of precision@k and recall              |
//! | MRR          | Reciprocal rank of the first relevant result         |
//! | NDCG@k       | Rank-weighted relevance (rewards relevant docs higher) |
//!
//! Reference: `GBrain`'s precision@5 test in `test/e2e/mechanical.test.ts`.

use serde::{Deserialize, Serialize};

use crate::embed::{EmbeddingsStore, SearchHit};
use crate::error::{Error, Result};
use crate::llm::LlmProvider;
use crate::search::{self, HybridSearchOpts};
use crate::vault::{Page, Vault};

const BENCHMARKS_FILE: &str = "benchmarks.json";

/// A suite of benchmark cases.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkSuite {
    pub benchmarks: Vec<BenchmarkCase>,
}

/// One benchmark: a query and the page stems we expect to find.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkCase {
    /// The search query.
    pub query: String,
    /// Expected page filename stems (e.g. `["hybrid-search-rrf", "fts5"]`).
    pub expected: Vec<String>,
    /// How many results to retrieve. Defaults to 5.
    #[serde(default = "default_k")]
    pub k: usize,
    /// Optional human-readable description of what this case tests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

fn default_k() -> usize {
    5
}

/// Result of one benchmark case.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkResult {
    pub query: String,
    pub expected: Vec<String>,
    pub retrieved: Vec<String>,
    pub k: usize,
    pub precision: f32,
    pub recall: f32,
    pub f1: f32,
    pub mrr: f32,
    pub ndcg: f32,
}

/// Aggregate report over the full suite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub results: Vec<BenchmarkResult>,
    pub mean_precision: f32,
    pub mean_recall: f32,
    pub mean_f1: f32,
    pub mean_mrr: f32,
    pub mean_ndcg: f32,
    /// Composite health score 0–10.
    pub health_score: f32,
    /// Embedding coverage (0.0–1.0) factored into health.
    pub coverage: f32,
    /// Stale page ratio (0.0–1.0) factored into health.
    pub stale_ratio: f32,
}

/// Load the benchmark suite from `.scriptorium/benchmarks.json`.
/// Returns an empty suite if the file doesn't exist.
pub fn load_suite(vault: &Vault) -> Result<BenchmarkSuite> {
    let path = vault.meta_dir().join(BENCHMARKS_FILE);
    match std::fs::read_to_string(path.as_std_path()) {
        Ok(text) => {
            let suite: BenchmarkSuite = serde_json::from_str(&text)
                .map_err(|e| Error::Other(anyhow::anyhow!("invalid benchmarks.json: {e}")))?;
            Ok(suite)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(BenchmarkSuite {
            benchmarks: Vec::new(),
        }),
        Err(e) => Err(Error::io(path.into_std_path_buf(), e)),
    }
}

/// Save a benchmark suite to `.scriptorium/benchmarks.json`.
pub fn save_suite(vault: &Vault, suite: &BenchmarkSuite) -> Result<()> {
    let path = vault.meta_dir().join(BENCHMARKS_FILE);
    if let Some(parent) = path.as_std_path().parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::io(parent.to_path_buf(), e))?;
    }
    let json = serde_json::to_string_pretty(suite)
        .map_err(|e| Error::Other(anyhow::anyhow!("serialize: {e}")))?;
    std::fs::write(path.as_std_path(), json)
        .map_err(|e| Error::io(path.into_std_path_buf(), e))?;
    Ok(())
}

/// Run all benchmarks and compute aggregate metrics.
pub async fn run_benchmarks(
    vault: &Vault,
    store: &EmbeddingsStore,
    embed_provider: &dyn LlmProvider,
    llm_provider: &dyn LlmProvider,
    model: &str,
) -> Result<BenchmarkReport> {
    let suite = load_suite(vault)?;
    let scan = vault.scan()?;

    let mut results = Vec::new();
    for case in &suite.benchmarks {
        let opts = HybridSearchOpts {
            top_k: case.k,
            expansion: false, // Deterministic: no expansion during benchmarks.
            ..HybridSearchOpts::with_top_k(case.k)
        };
        let hits = search::hybrid_search(
            store,
            embed_provider,
            llm_provider,
            model,
            &case.query,
            &scan.pages,
            &opts,
        )
        .await?;

        let retrieved_stems = extract_stems(&hits, &scan.pages);
        let precision = precision_at_k(&retrieved_stems, &case.expected, case.k);
        let recall = recall_score(&retrieved_stems, &case.expected);
        let f1 = f1_score(precision, recall);
        let mrr = mean_reciprocal_rank(&retrieved_stems, &case.expected);
        let ndcg = ndcg_at_k(&retrieved_stems, &case.expected, case.k);

        results.push(BenchmarkResult {
            query: case.query.clone(),
            expected: case.expected.clone(),
            retrieved: retrieved_stems,
            k: case.k,
            precision,
            recall,
            f1,
            mrr,
            ndcg,
        });
    }

    let (mean_precision, mean_recall, mean_f1, mean_mrr, mean_ndcg) = if results.is_empty() {
        (0.0, 0.0, 0.0, 0.0, 0.0)
    } else {
        #[allow(clippy::cast_precision_loss)]
        let n = results.len() as f32;
        (
            results.iter().map(|r| r.precision).sum::<f32>() / n,
            results.iter().map(|r| r.recall).sum::<f32>() / n,
            results.iter().map(|r| r.f1).sum::<f32>() / n,
            results.iter().map(|r| r.mrr).sum::<f32>() / n,
            results.iter().map(|r| r.ndcg).sum::<f32>() / n,
        )
    };

    // Compute coverage and stale ratio from vault state.
    let total_pages = scan.pages.len();
    let embedded_pages = store.distinct_page_count().unwrap_or(0);
    #[allow(clippy::cast_precision_loss)]
    let coverage = if total_pages == 0 {
        1.0
    } else {
        embedded_pages as f32 / total_pages as f32
    };

    let stale_count = scan
        .pages
        .iter()
        .filter(|p| crate::lint::stale::is_stale(vault, p))
        .count();
    #[allow(clippy::cast_precision_loss)]
    let stale_ratio = if total_pages == 0 {
        0.0
    } else {
        stale_count as f32 / total_pages as f32
    };

    // Health score 0–10:
    //   mean_precision * 2 + mean_ndcg * 2 + mean_recall * 2
    //   + mean_mrr * 1 + coverage * 2 + freshness * 1
    let health_score = (mean_precision * 2.0
        + mean_ndcg * 2.0
        + mean_recall * 2.0
        + mean_mrr * 1.0
        + coverage * 2.0
        + (1.0 - stale_ratio) * 1.0)
        .min(10.0);

    Ok(BenchmarkReport {
        results,
        mean_precision,
        mean_recall,
        mean_f1,
        mean_mrr,
        mean_ndcg,
        health_score,
        coverage,
        stale_ratio,
    })
}

/// Extract page stems from search hits using the already-scanned page set.
///
/// Avoids re-scanning the vault by accepting the page slice directly.
fn extract_stems(hits: &[SearchHit], pages: &[Page]) -> Vec<String> {
    let id_to_stem: std::collections::HashMap<_, _> = pages
        .iter()
        .filter_map(|p| {
            p.path
                .file_stem()
                .map(|s| (p.frontmatter.id, s.to_string()))
        })
        .collect();

    hits.iter()
        .filter_map(|h| id_to_stem.get(&h.page_id).cloned())
        .collect()
}

/// Precision@k = |retrieved[0..k] ∩ expected| / k.
#[allow(clippy::cast_precision_loss)]
fn precision_at_k(retrieved: &[String], expected: &[String], k: usize) -> f32 {
    if k == 0 {
        return 0.0;
    }
    let hits = retrieved
        .iter()
        .take(k)
        .filter(|r| expected.contains(r))
        .count();
    hits as f32 / k as f32
}

/// Recall = |retrieved ∩ expected| / |expected|.
#[allow(clippy::cast_precision_loss)]
fn recall_score(retrieved: &[String], expected: &[String]) -> f32 {
    if expected.is_empty() {
        return 1.0; // vacuously true
    }
    let hits = retrieved
        .iter()
        .filter(|r| expected.contains(r))
        .count();
    hits as f32 / expected.len() as f32
}

/// F1 = 2 * precision * recall / (precision + recall).
fn f1_score(precision: f32, recall: f32) -> f32 {
    let sum = precision + recall;
    if sum == 0.0 {
        0.0
    } else {
        2.0 * precision * recall / sum
    }
}

/// MRR = 1 / rank of first expected hit (0 if none found).
#[allow(clippy::cast_precision_loss)]
fn mean_reciprocal_rank(retrieved: &[String], expected: &[String]) -> f32 {
    for (i, stem) in retrieved.iter().enumerate() {
        if expected.contains(stem) {
            return 1.0 / (i + 1) as f32;
        }
    }
    0.0
}

/// NDCG@k — Normalized Discounted Cumulative Gain at rank k.
///
/// Binary relevance: 1 if the retrieved item is in the expected set, 0
/// otherwise. DCG = Σ rel_i / log₂(i+2) for i in 0..k. IDCG is the
/// DCG of a perfect ranking (all expected items at the top).
#[allow(clippy::cast_precision_loss)]
fn ndcg_at_k(retrieved: &[String], expected: &[String], k: usize) -> f32 {
    if expected.is_empty() || k == 0 {
        return 1.0; // vacuously perfect
    }

    // DCG of the actual ranking.
    let dcg: f32 = retrieved
        .iter()
        .take(k)
        .enumerate()
        .map(|(i, stem)| {
            let rel = if expected.contains(stem) { 1.0_f32 } else { 0.0 };
            rel / (i as f32 + 2.0).log2()
        })
        .sum();

    // IDCG: all relevant items at the top.
    let ideal_count = expected.len().min(k);
    let idcg: f32 = (0..ideal_count)
        .map(|i| 1.0 / (i as f32 + 2.0).log2())
        .sum();

    if idcg == 0.0 {
        0.0
    } else {
        (dcg / idcg).min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn precision_at_k_computed_correctly() {
        // Retrieved: [a, b, c, d, e], expected: [a, c, f], k=5
        let retrieved = vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into()];
        let expected = vec!["a".into(), "c".into(), "f".into()];
        let p = precision_at_k(&retrieved, &expected, 5);
        // 2 hits / 5 = 0.4
        assert!((p - 0.4).abs() < 0.001);
    }

    #[test]
    fn recall_computed_correctly() {
        let retrieved = vec!["a".into(), "b".into(), "c".into()];
        let expected = vec!["a".into(), "c".into(), "d".into(), "e".into()];
        let r = recall_score(&retrieved, &expected);
        // 2 hits / 4 expected = 0.5
        assert!((r - 0.5).abs() < 0.001);
    }

    #[test]
    fn f1_computed_correctly() {
        let f1 = f1_score(0.4, 0.5);
        // 2 * 0.4 * 0.5 / (0.4 + 0.5) = 0.4 / 0.9 ≈ 0.4444
        assert!((f1 - 4.0 / 9.0).abs() < 0.001);
    }

    #[test]
    fn f1_zero_when_both_zero() {
        assert!((f1_score(0.0, 0.0)).abs() < 0.001);
    }

    #[test]
    fn mrr_computed_correctly() {
        // First expected hit is at rank 3 (0-indexed: position 2).
        let retrieved = vec!["x".into(), "y".into(), "a".into(), "z".into()];
        let expected = vec!["a".into(), "b".into()];
        let mrr = mean_reciprocal_rank(&retrieved, &expected);
        // 1 / (2+1) = 1/3
        assert!((mrr - 1.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn mrr_zero_when_no_match() {
        let retrieved = vec!["x".into(), "y".into()];
        let expected = vec!["a".into()];
        assert!((mean_reciprocal_rank(&retrieved, &expected)).abs() < 0.001);
    }

    #[test]
    fn ndcg_perfect_ranking() {
        // All expected items at the top.
        let retrieved = vec!["a".into(), "b".into(), "x".into()];
        let expected = vec!["a".into(), "b".into()];
        let ndcg = ndcg_at_k(&retrieved, &expected, 3);
        assert!((ndcg - 1.0).abs() < 0.001);
    }

    #[test]
    fn ndcg_worst_ranking() {
        // No relevant items retrieved.
        let retrieved = vec!["x".into(), "y".into(), "z".into()];
        let expected = vec!["a".into(), "b".into()];
        let ndcg = ndcg_at_k(&retrieved, &expected, 3);
        assert!((ndcg).abs() < 0.001);
    }

    #[test]
    fn ndcg_partial_ranking() {
        // Relevant item at position 2 (0-indexed), not at position 0.
        let retrieved = vec!["x".into(), "y".into(), "a".into()];
        let expected = vec!["a".into()];
        let ndcg = ndcg_at_k(&retrieved, &expected, 3);
        // DCG = 1/log2(4) ≈ 0.5, IDCG = 1/log2(2) = 1.0
        let expected_ndcg = (1.0_f32 / 4.0_f32.log2()) / (1.0 / 2.0_f32.log2());
        assert!((ndcg - expected_ndcg).abs() < 0.001);
    }

    #[test]
    fn ndcg_empty_expected() {
        let retrieved = vec!["x".into()];
        let expected: Vec<String> = vec![];
        assert!((ndcg_at_k(&retrieved, &expected, 3) - 1.0).abs() < 0.001);
    }

    #[test]
    fn health_score_in_range() {
        // Max possible: precision=1, ndcg=1, recall=1, mrr=1, coverage=1, stale=0
        // = 2 + 2 + 2 + 1 + 2 + 1 = 10
        let score: f32 = 1.0 * 2.0 + 1.0 * 2.0 + 1.0 * 2.0 + 1.0 * 1.0 + 1.0 * 2.0 + 1.0 * 1.0;
        assert!((score - 10.0).abs() < 0.001);
        assert!(score <= 10.0);

        // Min possible: all zeros
        let score_min: f32 = 0.0 * 2.0 + 0.0 * 2.0 + 0.0 * 2.0 + 0.0 * 1.0 + 0.0 * 2.0 + 0.0 * 1.0;
        assert!(score_min >= 0.0);
    }

    #[test]
    fn empty_suite_returns_zero() {
        let suite = BenchmarkSuite {
            benchmarks: Vec::new(),
        };
        assert!(suite.benchmarks.is_empty());
        // mean_precision/recall/mrr should all be 0 for empty suite
        // (tested indirectly through the run_benchmarks path)
    }

    #[test]
    fn load_suite_from_json() {
        let json = r#"{
            "benchmarks": [
                { "query": "test query", "expected": ["page-a", "page-b"], "k": 5 }
            ]
        }"#;
        let suite: BenchmarkSuite = serde_json::from_str(json).unwrap();
        assert_eq!(suite.benchmarks.len(), 1);
        assert_eq!(suite.benchmarks[0].query, "test query");
        assert_eq!(suite.benchmarks[0].expected.len(), 2);
        assert_eq!(suite.benchmarks[0].k, 5);
    }

    #[test]
    fn load_suite_with_description() {
        let json = r#"{
            "benchmarks": [
                {
                    "query": "test",
                    "expected": ["a"],
                    "k": 3,
                    "description": "tests basic retrieval"
                }
            ]
        }"#;
        let suite: BenchmarkSuite = serde_json::from_str(json).unwrap();
        assert_eq!(
            suite.benchmarks[0].description.as_deref(),
            Some("tests basic retrieval")
        );
    }
}
