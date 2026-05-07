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
