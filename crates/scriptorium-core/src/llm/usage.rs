//! Append-only LLM usage log.
//!
//! Every time scriptorium issues a `complete()` or `embed()` call that the
//! caller wants tracked, it emits one JSONL line to
//! `<vault>/.scriptorium/usage.jsonl` via [`record_usage`]. The log is
//! append-only and newline-delimited so you can `tail -f` it during a long
//! ingest, `jq` it after the fact, or pipe it into a spreadsheet.
//!
//! Cost estimation is best-effort. A small hard-coded table of
//! (provider, model) → per-million-token prices lives in [`estimate_cost`];
//! models not in the table get a zero cost and a `price_known: false`
//! marker in the line. Updating the table is a two-line change per model.
//!
//! Why JSONL and not `SQLite`? The usage log is read far less often than
//! the embeddings store and is valuable as a grep-able text file even if
//! the binary crashes mid-write. Append-only line writes are atomic on
//! POSIX for lines under `PIPE_BUF` (4096 bytes), which every line
//! produced here is.

use std::fs::OpenOptions;
use std::io::Write;

use camino::Utf8Path;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::llm::Usage;

/// One usage record, matching one line in `usage.jsonl`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UsageRecord {
    /// UTC ISO-8601 timestamp of the call.
    pub ts: String,
    /// Which scriptorium operation made the call (`ingest`, `query`,
    /// `embed_page`, `reindex`, `watch`, etc.).
    pub op: String,
    /// Provider name as returned by [`LlmProvider::name`](crate::llm::LlmProvider::name).
    pub provider: String,
    /// Model name the provider reported serving.
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Estimated cost in USD. Zero when the (provider, model) pair is not
    /// in the pricing table.
    pub estimated_cost_usd: f64,
    /// `true` if the cost came from the pricing table; `false` if we
    /// didn't know the rate and defaulted to zero.
    pub price_known: bool,
}

/// Append a usage record to `<meta_dir>/usage.jsonl`, creating the file and
/// its parent directory if needed. Errors are returned but callers should
/// usually `let _ =` them — a failure to log usage should never block an
/// actual ingest or query.
pub fn record_usage(
    meta_dir: &Utf8Path,
    op: &str,
    provider: &str,
    model: &str,
    usage: &Usage,
) -> Result<()> {
    let (cost, price_known) =
        estimate_cost(provider, model, usage.input_tokens, usage.output_tokens);
    let record = UsageRecord {
        ts: Utc::now().to_rfc3339(),
        op: op.to_string(),
        provider: provider.to_string(),
        model: model.to_string(),
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        estimated_cost_usd: cost,
        price_known,
    };
    let mut line = serde_json::to_string(&record)
        .map_err(|e| Error::Other(anyhow::anyhow!("serialize usage: {e}")))?;
    line.push('\n');

    std::fs::create_dir_all(meta_dir.as_std_path())
        .map_err(|e| Error::io(meta_dir.as_std_path().to_path_buf(), e))?;
    let path = meta_dir.join("usage.jsonl");
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path.as_std_path())
        .map_err(|e| Error::io(path.clone().into_std_path_buf(), e))?;
    file.write_all(line.as_bytes())
        .map_err(|e| Error::io(path.into_std_path_buf(), e))?;
    Ok(())
}

/// Return `(cost_usd, price_known)` for a given provider+model token count.
///
/// Prices are in $/million tokens and are taken from each provider's public
/// pricing page at the time of writing. When a model is not in the table,
/// the return is `(0.0, false)` and the log line records `price_known:
/// false` so the aggregator can skip it rather than double-counting zero.
///
/// Update this table when prices change or new models appear.
pub fn estimate_cost(provider: &str, model: &str, input: u64, output: u64) -> (f64, bool) {
    let (in_rate, out_rate) = match (provider, model) {
        // Anthropic Claude — https://www.anthropic.com/pricing
        ("claude", m) if m.starts_with("claude-opus-4") => (15.0, 75.0),
        ("claude", m) if m.starts_with("claude-sonnet-4") => (3.0, 15.0),
        ("claude", m) if m.starts_with("claude-haiku-4") => (0.80, 4.0),
        ("claude", m) if m.starts_with("claude-opus-3") => (15.0, 75.0),
        ("claude", m) if m.starts_with("claude-sonnet-3") => (3.0, 15.0),
        ("claude", m) if m.starts_with("claude-haiku-3") => (0.25, 1.25),

        // OpenAI — https://openai.com/api/pricing/
        ("openai", "gpt-4o") => (2.50, 10.0),
        ("openai", "gpt-4o-mini") => (0.15, 0.60),
        ("openai", m) if m.starts_with("text-embedding-3-small") => (0.02, 0.0),
        ("openai", m) if m.starts_with("text-embedding-3-large") => (0.13, 0.0),

        // Google Gemini — https://ai.google.dev/pricing
        ("gemini", m) if m.starts_with("gemini-2.5-pro") => (1.25, 10.0),
        ("gemini", m) if m.starts_with("gemini-2.5-flash") => (0.30, 2.50),
        ("gemini", m) if m.starts_with("gemini-2.0-flash") => (0.10, 0.40),
        ("gemini", m) if m.starts_with("gemini-embedding") => (0.15, 0.0),

        // Ollama runs locally, and the mock provider is explicitly free —
        // zero marginal cost in both cases, and the rate is "known" so
        // aggregators don't mark them as unpriced.
        ("ollama" | "mock", _) => return (0.0, true),

        _ => return (0.0, false),
    };
    #[allow(clippy::cast_precision_loss)] // cost math doesn't need exact u64
    let cost = (input as f64 / 1_000_000.0) * in_rate + (output as f64 / 1_000_000.0) * out_rate;
    (cost, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn estimate_cost_known_model() {
        let (cost, known) = estimate_cost("claude", "claude-opus-4-6", 2_000_000, 500_000);
        assert!(known);
        // 2M in @ $15/M + 0.5M out @ $75/M = 30 + 37.5 = 67.5
        assert!((cost - 67.5).abs() < 0.01);
    }

    #[test]
    fn estimate_cost_unknown_model_returns_zero_and_false() {
        let (cost, known) = estimate_cost("exotic", "mystery", 100, 100);
        assert!(cost.abs() < f64::EPSILON);
        assert!(!known);
    }

    #[test]
    fn estimate_cost_ollama_is_free_and_known() {
        let (cost, known) = estimate_cost("ollama", "llama3.1", 1_000_000, 1_000_000);
        assert!(cost.abs() < f64::EPSILON);
        assert!(known);
    }

    #[test]
    fn record_usage_appends_jsonl() {
        let dir = TempDir::new().unwrap();
        let meta = Utf8Path::from_path(dir.path()).unwrap();
        let usage = Usage {
            input_tokens: 1000,
            output_tokens: 500,
        };
        record_usage(meta, "ingest", "claude", "claude-opus-4-6", &usage).unwrap();
        record_usage(meta, "query", "claude", "claude-opus-4-6", &usage).unwrap();

        let contents = std::fs::read_to_string(dir.path().join("usage.jsonl")).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2);
        for line in lines {
            let rec: UsageRecord = serde_json::from_str(line).unwrap();
            assert_eq!(rec.provider, "claude");
            assert_eq!(rec.input_tokens, 1000);
            assert_eq!(rec.output_tokens, 500);
            assert!(rec.price_known);
            assert!(rec.estimated_cost_usd > 0.0);
        }
    }

    #[test]
    fn record_usage_creates_missing_meta_dir() {
        let dir = TempDir::new().unwrap();
        let meta_path = dir.path().join("does/not/exist/yet");
        let meta = Utf8Path::from_path(&meta_path).unwrap();
        let usage = Usage::default();
        record_usage(meta, "ingest", "mock", "mock-1", &usage).unwrap();
        assert!(meta_path.join("usage.jsonl").exists());
    }
}
