//! Loader for `.scriptorium/config.toml`.
//!
//! Schema is intentionally tiny in v1; it grows as modules need it.

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingsConfig {
    pub provider: String,
    pub model: String,
}

impl Default for EmbeddingsConfig {
    fn default() -> Self {
        Self {
            provider: "mock".into(),
            model: "fixture".into(),
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
