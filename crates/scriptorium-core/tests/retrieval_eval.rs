//! Retrieval eval on a synthetic multi-hop vault.
//!
//! Runs [`bench::run_benchmarks`], the code behind `scriptorium bench`, on
//! `tests/fixtures/retrieval-eval/vault` and compares every case's rank with
//! `baseline.json`. Cases come in four kinds: `single` (the answer is on the
//! page the question names), `hop1` and `hop2` (the answer is one or two
//! wikilinks away from it), and `keyword` (plain words that hybrid search
//! must also find with only its keyword list on).
//!
//! The embeddings are frozen. `vectors.f32` holds a [`MODEL`] vector for
//! every text the pipeline embeds, chunks and queries alike, so the run
//! needs no network and gives the same ranks every time. A text without a
//! frozen vector fails the test, and so does any rank change, which lists
//! the cases that moved, with the metrics.
//!
//! - `SCRIPTORIUM_EVAL_UPDATE=1` rewrites `baseline.json` from this run.
//!   Commit it with the ranking change, so the diff shows what moved.
//! - `SCRIPTORIUM_EVAL_RECORD=1` embeds every text with [`MODEL`] through a
//!   local Ollama and rewrites the vectors. Do it after editing the vault or
//!   the cases, or after a chunking change.
//! - `SCRIPTORIUM_EVAL_REPORT=<path>` writes every case's ranked pages.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use scriptorium_core::bench::{self, BenchmarkReport, BenchmarkSuite};
use scriptorium_core::embed::{self, EmbeddingsStore};
use scriptorium_core::llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, OllamaConfig, OllamaProvider,
};
use scriptorium_core::search::{self, DedupConfig, HybridSearchOpts};
use scriptorium_core::vault::Vault;
use serde_json::{json, Value};

/// The embedding model the frozen vectors come from.
const MODEL: &str = "embeddinggemma-2:270m-q8_0-text";
const DIM: usize = 768;
/// Ranks past `K` count as a miss.
const K: usize = 5;
/// The provider name the store tags the frozen rows with.
const PROVIDER: &str = "frozen";

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/retrieval-eval")
}

/// Serves the frozen vectors, or embeds through Ollama and keeps the
/// vectors while recording. Texts with no frozen vector are collected, so
/// the test fails even when the pipeline falls back after the error.
struct FrozenEmbedder {
    rows: HashMap<String, Vec<f32>>,
    live: Option<OllamaProvider>,
    recorded: Mutex<BTreeMap<String, Vec<f32>>>,
    misses: Mutex<Vec<String>>,
}

impl FrozenEmbedder {
    fn load() -> Self {
        let dir = fixture();
        let manifest = read_json(&dir.join("vectors.json"));
        assert_eq!(
            manifest["model"], MODEL,
            "vectors.json was recorded with another model"
        );
        assert_eq!(manifest["dim"], DIM, "vectors.json has another dimension");
        let texts = manifest["texts"]
            .as_array()
            .expect("vectors.json lists texts");
        let bytes = std::fs::read(dir.join("vectors.f32")).expect("read vectors.f32");
        assert_eq!(
            bytes.len(),
            texts.len() * DIM * 4,
            "vectors.f32 must hold one {DIM}-d f32 vector per text"
        );
        let (byte_rows, _) = bytes.as_chunks::<{ DIM * 4 }>();
        let rows = texts
            .iter()
            .zip(byte_rows)
            .map(|(text, row)| {
                let (floats, _) = row.as_chunks::<4>();
                let vector = floats.iter().map(|b| f32::from_le_bytes(*b)).collect();
                (text.as_str().expect("texts are strings").to_owned(), vector)
            })
            .collect();
        Self {
            rows,
            live: None,
            recorded: Mutex::default(),
            misses: Mutex::default(),
        }
    }

    fn recording() -> Self {
        let config = OllamaConfig {
            embed_model: MODEL.to_owned(),
            ..OllamaConfig::default()
        };
        Self {
            rows: HashMap::new(),
            live: Some(OllamaProvider::new(config).expect("ollama provider")),
            recorded: Mutex::default(),
            misses: Mutex::default(),
        }
    }

    fn save(&self) {
        if self.live.is_none() {
            return;
        }
        let recorded = self.recorded.lock().expect("recorded vectors");
        let mut bytes = Vec::with_capacity(recorded.len() * DIM * 4);
        for vector in recorded.values() {
            assert_eq!(
                vector.len(),
                DIM,
                "{MODEL} returned a {}-d vector",
                vector.len()
            );
            bytes.extend(vector.iter().flat_map(|x| x.to_le_bytes()));
        }
        let dir = fixture();
        std::fs::write(dir.join("vectors.f32"), bytes).expect("write vectors.f32");
        let manifest =
            json!({ "model": MODEL, "dim": DIM, "texts": recorded.keys().collect::<Vec<_>>() });
        write_json(&dir.join("vectors.json"), &manifest);
    }
}

#[async_trait]
impl LlmProvider for FrozenEmbedder {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        Err(LlmError::Unsupported {
            provider: PROVIDER.into(),
            operation: "complete".into(),
        })
    }

    async fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        if let Some(provider) = &self.live {
            let vectors = provider.embed(texts).await?;
            let mut recorded = self.recorded.lock().expect("recorded vectors");
            for (text, vector) in texts.iter().zip(&vectors) {
                recorded.insert(text.clone(), vector.clone());
            }
            return Ok(vectors);
        }
        texts
            .iter()
            .map(|text| {
                self.rows.get(text).cloned().ok_or_else(|| {
                    let head: String = text.chars().take(60).collect();
                    self.misses.lock().expect("misses").push(head.clone());
                    LlmError::InvalidResponse(format!("no frozen vector for {head:?}"))
                })
            })
            .collect()
    }

    fn name(&self) -> &'static str {
        PROVIDER
    }

    fn context_window(&self) -> usize {
        8192
    }

    fn embedding_dim(&self) -> usize {
        DIM
    }
}

/// Stands in for the chat model under a name of its own, so code that uses
/// the chat provider where the embedding provider belongs finds no rows.
struct ChatStub;

#[async_trait]
impl LlmProvider for ChatStub {
    async fn complete(&self, _req: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        Err(LlmError::Unsupported {
            provider: self.name().into(),
            operation: "complete".into(),
        })
    }

    async fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, LlmError> {
        Err(LlmError::Unsupported {
            provider: self.name().into(),
            operation: "embed".into(),
        })
    }

    fn name(&self) -> &'static str {
        "chat-stub"
    }

    fn context_window(&self) -> usize {
        8192
    }

    fn embedding_dim(&self) -> usize {
        DIM
    }
}

/// One case's outcome: the rank of its first expected page among the
/// distinct pages retrieved, if that page is in the top `K`.
struct CaseRank {
    id: String,
    kind: String,
    lang: String,
    rank: Option<usize>,
    pages: Vec<String>,
}

fn rank_cases(suite: &BenchmarkSuite, report: &BenchmarkReport) -> Vec<CaseRank> {
    assert_eq!(suite.benchmarks.len(), report.results.len());
    suite
        .benchmarks
        .iter()
        .zip(&report.results)
        .map(|(case, result)| {
            let label = case
                .description
                .as_deref()
                .expect("cases carry 'id kind lang'");
            let mut parts = label.split_whitespace().map(str::to_owned);
            let (id, kind, lang) = (
                parts.next().expect("case id"),
                parts.next().expect("case kind"),
                parts.next().expect("case language"),
            );
            // A page can come back as several chunks; rank distinct pages.
            let mut pages: Vec<String> = Vec::new();
            for stem in &result.retrieved {
                if !pages.contains(stem) {
                    pages.push(stem.clone());
                }
            }
            let rank = pages
                .iter()
                .take(K)
                .position(|p| case.expected.contains(p))
                .map(|i| i + 1);
            CaseRank {
                id,
                kind,
                lang,
                rank,
                pages,
            }
        })
        .collect()
}

/// hit@1, hit@K and MRR@K for the whole set, each kind and each language.
#[allow(clippy::cast_precision_loss)]
fn metrics(cases: &[CaseRank]) -> BTreeMap<String, Value> {
    let mut groups: BTreeMap<String, Vec<Option<usize>>> = BTreeMap::new();
    for case in cases {
        for group in ["all", case.kind.as_str(), case.lang.as_str()] {
            groups.entry(group.to_owned()).or_default().push(case.rank);
        }
    }
    let round = |x: f64| (x * 1000.0).round() / 1000.0;
    groups
        .into_iter()
        .map(|(group, ranks)| {
            let n = ranks.len() as f64;
            let first = ranks.iter().filter(|r| **r == Some(1)).count() as f64 / n;
            let found = ranks.iter().filter(|r| r.is_some()).count() as f64 / n;
            let mrr = ranks.iter().flatten().map(|r| 1.0 / *r as f64).sum::<f64>() / n;
            let mut row = serde_json::Map::new();
            row.insert("n".into(), json!(ranks.len()));
            row.insert("hit@1".into(), json!(round(first)));
            row.insert(format!("hit@{K}"), json!(round(found)));
            row.insert(format!("mrr@{K}"), json!(round(mrr)));
            (group, Value::Object(row))
        })
        .collect()
}

fn read_json(path: &Path) -> Value {
    let text =
        std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn write_json(path: &Path, value: &Value) {
    let text = serde_json::to_string_pretty(value).expect("serialize") + "\n";
    std::fs::write(path, text).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

/// The keys of a JSON object, or an empty set.
fn keys(value: &Value) -> BTreeSet<String> {
    value
        .as_object()
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn retrieval_eval_matches_baseline() {
    let record = std::env::var_os("SCRIPTORIUM_EVAL_RECORD").is_some();
    let embedder = if record {
        FrozenEmbedder::recording()
    } else {
        FrozenEmbedder::load()
    };
    let vault = Vault::open(fixture().join("vault")).expect("open the eval vault");
    let suite = bench::load_suite(&vault).expect("load cases");
    assert!(!suite.benchmarks.is_empty(), "the eval vault has no cases");

    let store = EmbeddingsStore::in_memory().expect("in-memory store");
    embed::reindex(&vault, &store, &embedder, MODEL)
        .await
        .expect("reindex");
    let report = bench::run_benchmarks(&vault, &store, &embedder, &ChatStub, MODEL)
        .await
        .expect("bench");
    embedder.save();
    let misses = embedder.misses.lock().expect("misses").clone();
    assert!(
        misses.is_empty(),
        "texts with no frozen vector (re-record with SCRIPTORIUM_EVAL_RECORD=1): {misses:?}"
    );

    let cases = rank_cases(&suite, &report);
    let ids: BTreeSet<&str> = cases.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(ids.len(), cases.len(), "case ids must be unique");

    // Keyword cases must also come back from hybrid search with only its
    // keyword list on, which keeps the keyword path wired into the fusion.
    let pages = vault.scan().expect("scan the eval vault").pages;
    let stems: HashMap<_, _> = pages
        .iter()
        .filter_map(|p| p.path.file_stem().map(|s| (p.frontmatter.id, s.to_owned())))
        .collect();
    let keyword_only = HybridSearchOpts {
        top_k: K,
        expansion: false,
        hyde: false,
        vector_limit: 0,
        keyword_limit: 20,
        graph_limit: 0,
        dedup: DedupConfig::default(),
    };
    let mut keyword_misses = Vec::new();
    for (case, rank) in suite.benchmarks.iter().zip(&cases) {
        if rank.kind != "keyword" {
            continue;
        }
        let hits = search::hybrid_search(
            &store,
            &embedder,
            &ChatStub,
            MODEL,
            &case.query,
            &pages,
            &keyword_only,
        )
        .await
        .expect("keyword-only hybrid search");
        if !hits.iter().any(|h| {
            stems
                .get(&h.page_id)
                .is_some_and(|s| case.expected.contains(s))
        }) {
            keyword_misses.push(rank.id.clone());
        }
    }
    assert!(
        keyword_misses.is_empty(),
        "keyword-only hybrid search missed the expected page for {keyword_misses:?}"
    );

    let metrics = metrics(&cases);
    for (group, row) in &metrics {
        println!("{group:<7} {row}");
    }
    let ranks: BTreeMap<&str, Option<usize>> =
        cases.iter().map(|c| (c.id.as_str(), c.rank)).collect();

    if let Some(path) = std::env::var_os("SCRIPTORIUM_EVAL_REPORT") {
        let rows: Vec<Value> = cases
            .iter()
            .map(|c| json!({ "id": c.id, "kind": c.kind, "lang": c.lang, "rank": c.rank, "pages": c.pages }))
            .collect();
        write_json(
            Path::new(&path),
            &json!({ "metrics": metrics, "cases": rows }),
        );
    }

    let baseline_path = fixture().join("baseline.json");
    if std::env::var_os("SCRIPTORIUM_EVAL_UPDATE").is_some() {
        write_json(
            &baseline_path,
            &json!({ "model": MODEL, "k": K, "metrics": metrics, "ranks": ranks }),
        );
        return;
    }

    let baseline = read_json(&baseline_path);
    assert_eq!(
        baseline["model"], MODEL,
        "baseline.json is for another model"
    );
    assert_eq!(baseline["k"], K, "baseline.json uses another k");
    let current_ids: BTreeSet<String> = ids.iter().map(|s| (*s).to_owned()).collect();
    assert_eq!(
        keys(&baseline["ranks"]),
        current_ids,
        "the cases differ from baseline.json; rerun with SCRIPTORIUM_EVAL_UPDATE=1"
    );
    assert_eq!(
        keys(&baseline["metrics"]),
        metrics.keys().cloned().collect::<BTreeSet<_>>(),
        "the case groups differ from baseline.json"
    );

    let show = |r: Option<usize>| r.map_or("-".to_owned(), |r| r.to_string());
    let (mut better, mut worse) = (Vec::new(), Vec::new());
    for case in &cases {
        let before = baseline["ranks"][case.id.as_str()]
            .as_u64()
            .and_then(|r| usize::try_from(r).ok());
        if before == case.rank {
            continue;
        }
        let line = format!(
            "{} {}: {} -> {}",
            case.id,
            case.kind,
            show(before),
            show(case.rank)
        );
        // A miss counts as rank K + 1.
        if case.rank.unwrap_or(K + 1) < before.unwrap_or(K + 1) {
            better.push(line);
        } else {
            worse.push(line);
        }
    }
    let changed_metrics: Vec<String> = metrics
        .iter()
        .filter(|(group, row)| baseline["metrics"][group.as_str()] != **row)
        .map(|(group, row)| format!("{group}: {} -> {row}", baseline["metrics"][group.as_str()]))
        .collect();
    assert!(
        better.is_empty() && worse.is_empty() && changed_metrics.is_empty(),
        "retrieval ranks differ from baseline.json\n\
         worse ({}): {}\nbetter ({}): {}\nmetrics:\n  {}\n\
         If the change is intended, rerun with SCRIPTORIUM_EVAL_UPDATE=1 and commit baseline.json.",
        worse.len(),
        worse.join("; "),
        better.len(),
        better.join("; "),
        changed_metrics.join("\n  "),
    );
}
