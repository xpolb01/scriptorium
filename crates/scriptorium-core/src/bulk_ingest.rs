//! Bulk import: process an entire directory of source files with checkpoint
//! resume, error isolation, and progress reporting.
//!
//! Each file gets its own [`ingest::ingest`] call (one git commit per file)
//! for clean history. A checkpoint file at `.scriptorium/bulk-ingest-checkpoint.json`
//! tracks progress so interrupted imports resume where they left off.
//!
//! Reference: `GBrain`'s `src/commands/import.ts`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::error::{Error, Result};
use crate::ingest;
use crate::llm::LlmProvider;
use crate::vault::Vault;

/// Maximum file size to ingest (5 MB).
const MAX_FILE_SIZE: u64 = 5 * 1024 * 1024;

/// Extensions eligible for bulk ingest.
const INGESTABLE_EXTENSIONS: &[&str] = &["md", "markdown", "txt", "text"];

/// Options for [`bulk_ingest`].
#[derive(Debug, Clone)]
pub struct BulkIngestOptions {
    /// If true, each file is ingested with `dry_run: true`.
    pub dry_run: bool,
    /// Save checkpoint every N files.
    pub checkpoint_interval: usize,
}

impl Default for BulkIngestOptions {
    fn default() -> Self {
        Self {
            dry_run: false,
            checkpoint_interval: 10,
        }
    }
}

/// Report from a [`bulk_ingest`] run.
#[derive(Debug, Clone)]
pub struct BulkIngestReport {
    /// Total files discovered in the directory.
    pub total_discovered: usize,
    /// Files skipped because they were already interned in `sources/`.
    pub skipped_already_interned: usize,
    /// Files skipped because they were in a previous checkpoint.
    pub skipped_checkpoint: usize,
    /// Files successfully ingested.
    pub ingested: usize,
    /// Files that failed to ingest (error isolated, did not stop the batch).
    pub failed: Vec<BulkIngestError>,
    /// Total elapsed time.
    pub elapsed: Duration,
}

/// One file that failed during bulk ingest.
#[derive(Debug, Clone)]
pub struct BulkIngestError {
    pub path: PathBuf,
    pub error: String,
}

/// Checkpoint for resume support.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Checkpoint {
    /// Canonical path of the source directory.
    dir: String,
    /// Paths of successfully processed files (relative to `dir`).
    processed: Vec<String>,
}

/// Run bulk ingest over all eligible files in `dir`.
///
/// `progress` is called after each file with `(current_index, total, path)`.
pub async fn bulk_ingest(
    vault: &Vault,
    provider: &dyn LlmProvider,
    dir: &Path,
    options: &BulkIngestOptions,
    progress: impl Fn(usize, usize, &Path),
) -> Result<BulkIngestReport> {
    bulk_ingest_with_retrieval(
        vault, provider, dir, options, progress, None, None, None,
    )
    .await
}

/// Like [`bulk_ingest`] but with optional embeddings retrieval.
pub async fn bulk_ingest_with_retrieval(
    vault: &Vault,
    provider: &dyn LlmProvider,
    dir: &Path,
    options: &BulkIngestOptions,
    progress: impl Fn(usize, usize, &Path),
    embed_store: Option<&crate::embed::EmbeddingsStore>,
    embed_provider: Option<&dyn LlmProvider>,
    embed_model: Option<&str>,
) -> Result<BulkIngestReport> {
    let start = Instant::now();

    // 1. Discover eligible files.
    let mut files = discover_files(dir)?;
    files.sort();
    let total_discovered = files.len();

    // 2. Load checkpoint.
    let checkpoint_path = vault.meta_dir().join("bulk-ingest-checkpoint.json");
    let checkpoint = load_checkpoint(checkpoint_path.as_std_path());
    let dir_canonical = dir
        .canonicalize()
        .unwrap_or_else(|_| dir.to_path_buf())
        .to_string_lossy()
        .to_string();

    let checkpoint_set: std::collections::HashSet<String> = checkpoint
        .as_ref()
        .filter(|c| c.dir == dir_canonical)
        .map(|c| c.processed.iter().cloned().collect())
        .unwrap_or_default();
    let skipped_checkpoint = checkpoint_set.len();

    // 3. Process files.
    let mut ingested = 0usize;
    let mut skipped_already_interned = 0usize;
    let mut failed = Vec::new();
    let mut processed_paths: Vec<String> = checkpoint_set.iter().cloned().collect();

    let ingest_opts = ingest::IngestOptions {
        dry_run: options.dry_run,
        hooks: None, // bulk ingest doesn't fire per-file hooks
    };

    for (i, file_path) in files.iter().enumerate() {
        let rel = file_path
            .strip_prefix(dir)
            .unwrap_or(file_path)
            .to_string_lossy()
            .to_string();

        // Skip if in checkpoint.
        if checkpoint_set.contains(&rel) {
            continue;
        }

        // Skip if already interned.
        if is_already_interned(vault, file_path)? {
            skipped_already_interned += 1;
            continue;
        }

        progress(i + 1, total_discovered, file_path);

        // Ingest with error isolation.
        match ingest::ingest_with_retrieval(
            vault, provider, file_path, ingest_opts.clone(),
            embed_store, embed_provider, embed_model,
        ).await {
            Ok(report) => {
                info!(
                    path = %file_path.display(),
                    commit = %report.commit_id,
                    created = report.created,
                    updated = report.updated,
                    "ingested"
                );
                ingested += 1;
                processed_paths.push(rel);
            }
            Err(e) => {
                warn!(path = %file_path.display(), error = %e, "ingest failed");
                failed.push(BulkIngestError {
                    path: file_path.clone(),
                    error: e.to_string(),
                });
            }
        }

        // Save checkpoint periodically.
        if (ingested + failed.len()) % options.checkpoint_interval == 0 {
            save_checkpoint(
                checkpoint_path.as_std_path(),
                &dir_canonical,
                &processed_paths,
            );
        }
    }

    // 4. Final checkpoint save (or cleanup on completion).
    if failed.is_empty() && !options.dry_run {
        // All succeeded — remove the checkpoint file.
        let _ = std::fs::remove_file(checkpoint_path.as_std_path());
    } else {
        save_checkpoint(
            checkpoint_path.as_std_path(),
            &dir_canonical,
            &processed_paths,
        );
    }

    Ok(BulkIngestReport {
        total_discovered,
        skipped_already_interned,
        skipped_checkpoint,
        ingested,
        failed,
        elapsed: start.elapsed(),
    })
}

/// Walk `dir` recursively and collect eligible files.
fn discover_files(dir: &Path) -> Result<Vec<PathBuf>> {
    if !dir.is_dir() {
        return Err(Error::Other(anyhow::anyhow!(
            "not a directory: {}",
            dir.display()
        )));
    }
    let mut files = Vec::new();
    for entry in ignore::WalkBuilder::new(dir)
        .hidden(true)
        .build()
        .flatten()
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        if !INGESTABLE_EXTENSIONS.contains(&ext.as_str()) {
            continue;
        }
        if let Ok(meta) = std::fs::metadata(path) {
            if meta.len() > MAX_FILE_SIZE {
                continue;
            }
        }
        files.push(path.to_path_buf());
    }
    Ok(files)
}

/// Check if a file is already interned in `sources/` by content hash prefix.
fn is_already_interned(vault: &Vault, file_path: &Path) -> Result<bool> {
    let bytes = std::fs::read(file_path).map_err(|e| Error::io(file_path.to_path_buf(), e))?;
    let hash = content_hash_prefix(&bytes);
    let sources_dir = vault.sources_dir();
    if !sources_dir.as_std_path().is_dir() {
        return Ok(false);
    }
    // Walk sources/ looking for any file starting with the hash prefix.
    for entry in ignore::WalkBuilder::new(sources_dir.as_std_path())
        .hidden(false)
        .build()
        .flatten()
    {
        if let Some(name) = entry.path().file_name().and_then(|n| n.to_str()) {
            if name.starts_with(&hash) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// First 12 hex chars of the SHA-256 hash — matches `intern_source`'s naming.
fn content_hash_prefix(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("{digest:x}")[..12].to_string()
}

fn load_checkpoint(path: &Path) -> Option<Checkpoint> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

fn save_checkpoint(path: &Path, dir: &str, processed: &[String]) {
    let cp = Checkpoint {
        dir: dir.to_string(),
        processed: processed.to_vec(),
    };
    if let Ok(json) = serde_json::to_string_pretty(&cp) {
        let _ = std::fs::write(path, json);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_finds_markdown_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.md"), "# A").unwrap();
        std::fs::write(dir.path().join("b.txt"), "B").unwrap();
        std::fs::write(dir.path().join("c.rs"), "fn main() {}").unwrap();
        let files = discover_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2, "should find .md and .txt, not .rs");
    }

    #[test]
    fn discover_skips_oversized_files() {
        let dir = tempfile::tempdir().unwrap();
        // Create a file larger than 5MB.
        let big = vec![b'x'; (MAX_FILE_SIZE + 1) as usize];
        std::fs::write(dir.path().join("big.md"), &big).unwrap();
        std::fs::write(dir.path().join("small.md"), "small").unwrap();
        let files = discover_files(dir.path()).unwrap();
        assert_eq!(files.len(), 1, "should skip the oversized file");
    }

    #[test]
    fn content_hash_prefix_is_12_chars() {
        let prefix = content_hash_prefix(b"hello world");
        assert_eq!(prefix.len(), 12);
        // Deterministic.
        assert_eq!(prefix, content_hash_prefix(b"hello world"));
        // Different content → different prefix.
        assert_ne!(prefix, content_hash_prefix(b"goodbye world"));
    }

    #[test]
    fn checkpoint_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cp.json");
        save_checkpoint(&path, "/some/dir", &["a.md".into(), "b.md".into()]);
        let loaded = load_checkpoint(&path).unwrap();
        assert_eq!(loaded.dir, "/some/dir");
        assert_eq!(loaded.processed.len(), 2);
    }
}
