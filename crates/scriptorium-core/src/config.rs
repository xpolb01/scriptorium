//! Loader for `.scriptorium/config.toml`.
//!
//! Schema is intentionally tiny in v1; it grows as modules need it.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

use crate::hooks::HooksConfig;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub llm: LlmConfig,
    #[serde(default)]
    pub embeddings: EmbeddingsConfig,
    #[serde(default)]
    pub git: GitConfig,
    #[serde(default)]
    pub paths: PathsConfig,
    #[serde(default)]
    pub hooks: HooksConfig,
    #[serde(default)]
    pub meridian: MeridianConfig,
    #[serde(default)]
    pub search: SearchConfig,
}

/// Query-time retrieval options (`[search]` in `config.toml`). Everything
/// here is a ranking/expansion refinement with a safe default; the base
/// hybrid pipeline (vector + FTS5 + RRF) is not configurable away.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    /// Maximal-Marginal-Relevance balance for final result ordering:
    /// `λ·relevance − (1−λ)·redundancy`. `1.0` disables the diversity
    /// reorder entirely.
    #[serde(default = "default_mmr_lambda")]
    pub mmr_lambda: f32,
    /// Half-life in days for recency boosting of retrieved pages. `0`
    /// (default) disables the boost. Pages updated recently rank higher:
    /// `score × (1 + recency_weight · 0.5^(age/half_life))`.
    #[serde(default)]
    pub recency_half_life_days: f32,
    /// Strength of the recency boost at age zero. Ignored while
    /// `recency_half_life_days` is 0.
    #[serde(default = "default_recency_weight")]
    pub recency_weight: f32,
    /// Generate a `HyDE` (hypothetical document) variant during query
    /// expansion: embed a short hypothetical answer alongside the query
    /// rephrasings. Helps semantic alignment on well-covered topics; can
    /// mislead on out-of-vault questions — hence opt-in.
    #[serde(default)]
    pub hyde: bool,
    /// Rerank the fused candidates with one listwise LLM call before
    /// building the prompt context. Uses the configured chat provider —
    /// no extra service. Non-fatal: any failure keeps the fused order.
    #[serde(default)]
    pub rerank: bool,
    /// How many top candidates are offered to the reranker.
    #[serde(default = "default_rerank_top_n")]
    pub rerank_top_n: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            mmr_lambda: default_mmr_lambda(),
            recency_half_life_days: 0.0,
            recency_weight: default_recency_weight(),
            hyde: false,
            rerank: false,
            rerank_top_n: default_rerank_top_n(),
        }
    }
}

fn default_mmr_lambda() -> f32 {
    0.7
}

fn default_recency_weight() -> f32 {
    0.3
}

fn default_rerank_top_n() -> usize {
    12
}

/// Optional local Anthropic-compatible proxy. When `enabled` and
/// `[llm].provider = "claude"`, scriptorium probes the configured URL at
/// startup; if reachable, Claude requests are routed through the proxy via
/// the OpenAI-compatible chat endpoint instead of `api.anthropic.com`.
/// Falls back to direct Anthropic when the probe fails — never fails closed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeridianConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_meridian_url")]
    pub url: String,
}

impl Default for MeridianConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: default_meridian_url(),
        }
    }
}

fn default_meridian_url() -> String {
    "http://127.0.0.1:3456".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmConfig {
    pub provider: String,
    pub model: String,
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            provider: "mock".into(),
            model: "fixture".into(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

fn default_timeout_secs() -> u64 {
    120
}

/// Chunking strategy for the embeddings pipeline.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ChunkStrategy {
    /// Split on H2/H3 headings, then by paragraph. Deterministic, fast,
    /// no API calls. Suitable for well-structured documents with clear
    /// heading hierarchy.
    Heading,
    /// Five-level recursive delimiter hierarchy (paragraphs → lines →
    /// sentences → clauses → words) with greedy merge. Deterministic,
    /// no API calls. Handles headingless content well.
    Recursive,
    /// Embed each sentence, find topic boundaries via Savitzky-Golay
    /// smoothing on adjacent cosine similarities. Falls back to recursive
    /// on error (no API key, embed failure, <4 sentences). ~2x embedding
    /// cost during reindex but produces the highest-quality chunks.
    #[default]
    Semantic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsConfig {
    pub provider: String,
    pub model: String,
    /// Chunking strategy. Defaults to "heading" (H2/H3 boundary splitting).
    #[serde(default)]
    pub chunk_strategy: ChunkStrategy,
}

impl Default for EmbeddingsConfig {
    fn default() -> Self {
        Self {
            provider: "mock".into(),
            model: "fixture".into(),
            chunk_strategy: ChunkStrategy::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitConfig {
    #[serde(default = "default_true")]
    pub auto_commit: bool,
    #[serde(default = "default_true")]
    pub auto_init: bool,
}

impl Default for GitConfig {
    fn default() -> Self {
        Self {
            auto_commit: true,
            auto_init: true,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PathsConfig {
    /// Override the default `wiki/` directory.
    pub wiki: Option<Utf8PathBuf>,
    /// Override the default `sources/` directory.
    pub sources: Option<Utf8PathBuf>,
}
