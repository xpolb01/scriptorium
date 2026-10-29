//! Bulk import: process an entire directory of source files with checkpoint
//! resume, error isolation, and progress reporting.
//!
//! Each file gets its own [`ingest::ingest_with_retrieval`] call (one git
//! commit per file) for clean history. A checkpoint file at
//! `.scriptorium/bulk-ingest-checkpoint.json` tracks progress so interrupted
//! imports resume where they left off. A real run with no failures deletes
//! it at the end; any other run, a dry run included, saves it.
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

/// Extensions eligible for bulk ingest — everything the extract router
/// handles, plus plain-text formats like Gherkin `.feature` files that
/// flow through the UTF-8 path.
const INGESTABLE_EXTENSIONS: &[&str] = &[
    "md", "markdown", "txt", "text", "feature", "html", "htm", "pdf", "docx", "pptx", "xlsx",
    "epub",
];

/// Options for [`bulk_ingest`].
#[derive(Debug, Clone)]
pub struct BulkIngestOptions {
    /// If true, each file is ingested with `dry_run: true`. The checkpoint
    /// is still saved, so a real run of the same directory afterwards
    /// skips those files unless the checkpoint is deleted first (the CLI's
    /// `--fresh`).
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
    /// Files whose ingest committed. A dry run or a near-duplicate skip
    /// counts as ingested but commits nothing.
    pub committed: usize,
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

impl BulkIngestError {
    /// Log that `path` failed at `step`, and describe the failure.
    fn logged(path: &Path, step: &str, error: &Error) -> Self {
        warn!(path = %path.display(), error = %error, "{step} failed");
        Self {
            path: path.to_path_buf(),
            error: error.to_string(),
        }
    }
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
    bulk_ingest_with_retrieval(vault, provider, dir, options, progress, None, None, None).await
}

/// Like [`bulk_ingest`] but with optional embeddings retrieval.
#[allow(clippy::too_many_arguments)]
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
    let dir_canonical = dir
        .canonicalize()
        .unwrap_or_else(|_| dir.to_path_buf())
        .to_string_lossy()
        .to_string();
    let checkpoint_set = checkpointed_files(checkpoint_path.as_std_path(), &dir_canonical);
    let skipped_checkpoint = checkpoint_set.len();

    // 3. Process files.
    let mut ingested = 0usize;
    let mut committed = 0usize;
    let mut skipped_already_interned = 0usize;
    let mut failed = Vec::new();
    let mut processed_paths: Vec<String> = checkpoint_set.iter().cloned().collect();

    let ingest_opts = ingest::IngestOptions {
        dry_run: options.dry_run,
        hooks: None, // bulk ingest doesn't fire per-file hooks
        force: false,
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

        // Skip if already interned. A file that cannot be read for this
        // check fails on its own, as a failed ingest does: ending the run
        // here would lose the report, and with it the count of the files
        // already committed, which the caller embeds.
        match is_already_interned(vault, file_path) {
            Ok(true) => {
                skipped_already_interned += 1;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                failed.push(BulkIngestError::logged(file_path, "interned check", &e));
                continue;
            }
        }

        progress(i + 1, total_discovered, file_path);

        // Ingest with error isolation.
        match ingest::ingest_with_retrieval(
            vault,
            provider,
            file_path,
            ingest_opts.clone(),
            embed_store,
            embed_provider,
            embed_model,
        )
        .await
        {
            Ok(report) => {
                info!(
                    path = %file_path.display(),
                    commit = %report.commit_id,
                    created = report.created,
                    updated = report.updated,
                    "ingested"
                );
                ingested += 1;
                if !options.dry_run && report.commit_id != ingest::NEAR_DUPLICATE_SKIP {
                    committed += 1;
                }
                processed_paths.push(rel);
            }
            Err(e) => failed.push(BulkIngestError::logged(file_path, "ingest", &e)),
        }

        // Save checkpoint periodically.
        if (ingested + failed.len()).is_multiple_of(options.checkpoint_interval) {
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
        committed,
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
    for entry in ignore::WalkBuilder::new(dir).hidden(true).build().flatten() {
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
    crate::hex::lower(&digest)[..12].to_string()
}

/// The files the checkpoint at `path` lists as processed, if it was saved
/// for the directory `dir_canonical`.
fn checkpointed_files(path: &Path, dir_canonical: &str) -> std::collections::HashSet<String> {
    load_checkpoint(path)
        .filter(|c| c.dir == dir_canonical)
        .map(|c| c.processed.into_iter().collect())
        .unwrap_or_default()
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
        let big = vec![b'x'; usize::try_from(MAX_FILE_SIZE + 1).unwrap()];
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

    /// A vault that ingest can write to, and an empty directory of sources
    /// outside it.
    fn bulk_fixture() -> (tempfile::TempDir, Vault, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki/concepts")).unwrap();
        std::fs::create_dir_all(dir.path().join("sources/articles")).unwrap();
        std::fs::write(
            dir.path().join("CLAUDE.md"),
            "# Vault Rules\n\nBe concise.\n",
        )
        .unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        (dir, vault, tempfile::tempdir().unwrap())
    }

    /// A provider that answers every ingest with a plan for one new page.
    fn plan_mock() -> crate::llm::MockProvider {
        use crate::llm::prompts::{IngestAction, IngestPageAction, IngestPlan};
        let plan = IngestPlan {
            summary: "bulk test".into(),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/concepts/bulk-target.md".into(),
                title: "Bulk Target".into(),
                tags: vec![],
                body: "Body without wikilinks.\n".into(),
                source_quote: None,
                supersedes: vec![],
            }],
            log_entry: "bulk ingest".into(),
            redundant: false,
        };
        crate::llm::MockProvider::constant(serde_json::to_string(&plan).unwrap())
    }

    #[tokio::test]
    async fn dry_run_counts_ingested_files_but_no_commits() {
        let (_dir, vault, sources) = bulk_fixture();
        std::fs::write(sources.path().join("a.md"), "# A\n\nAlpha source.\n").unwrap();
        let options = BulkIngestOptions {
            dry_run: true,
            ..Default::default()
        };

        let report = bulk_ingest(&vault, &plan_mock(), sources.path(), &options, |_, _, _| {})
            .await
            .unwrap();

        assert_eq!((report.ingested, report.committed), (1, 0));
    }

    /// A near-duplicate skip counts as ingested but commits nothing.
    #[tokio::test]
    async fn near_duplicate_skip_counts_no_commit() {
        let (_dir, vault, sources) = bulk_fixture();
        let words: Vec<String> = (0..120).map(|i| format!("bulk{i}")).collect();
        let body = format!("{}\n", words.join(" "));
        std::fs::write(sources.path().join("a.md"), &body).unwrap();
        let options = BulkIngestOptions::default();
        let first = bulk_ingest(&vault, &plan_mock(), sources.path(), &options, |_, _, _| {})
            .await
            .unwrap();
        assert_eq!(first.committed, 1, "{:?}", first.failed);

        let again = tempfile::tempdir().unwrap();
        std::fs::write(
            again.path().join("b.md"),
            format!("{body}one closing line\n"),
        )
        .unwrap();
        let no_plan = crate::llm::MockProvider::constant("{}");
        let second = bulk_ingest(&vault, &no_plan, again.path(), &options, |_, _, _| {})
            .await
            .unwrap();

        assert_eq!(
            (second.ingested, second.committed),
            (1, 0),
            "{:?}",
            second.failed
        );
    }

    /// A file that cannot be read fails on its own. The run goes on, and its
    /// report still counts the commit made before it, which the caller
    /// embeds.
    #[cfg(unix)]
    #[tokio::test]
    async fn unreadable_file_fails_alone_and_keeps_the_commits_before_it() {
        use std::os::unix::fs::PermissionsExt;
        let (_dir, vault, sources) = bulk_fixture();
        std::fs::write(sources.path().join("a.md"), "# A\n\nAlpha source.\n").unwrap();
        let unreadable = sources.path().join("b.md");
        std::fs::write(&unreadable, "# B\n\nBeta source.\n").unwrap();
        let set_mode =
            |mode| std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(mode));
        set_mode(0o000).unwrap();
        if std::fs::File::open(&unreadable).is_ok() {
            // The mode bits do not bind root.
            set_mode(0o644).unwrap();
            return;
        }

        let report = bulk_ingest(
            &vault,
            &plan_mock(),
            sources.path(),
            &BulkIngestOptions::default(),
            |_, _, _| {},
        )
        .await;
        set_mode(0o644).unwrap();
        let report = report.expect("one unreadable file does not end the run");

        assert_eq!(
            (report.ingested, report.committed, report.failed.len()),
            (1, 1, 1)
        );
        assert_eq!(report.failed[0].path, unreadable);
    }
}
