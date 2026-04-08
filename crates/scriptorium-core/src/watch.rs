//! File-system watcher that auto-ingests new sources.
//!
//! The watcher is intentionally narrow in v1: it watches `sources/` only,
//! and fires [`ingest::ingest`] for any new `.md` / `.txt` / `.markdown`
//! file that lands there. It does **not** watch `wiki/` — that would create
//! a feedback loop with its own ingest writes, and we'd need a recently-
//! written cache to break it. Embedding refresh on `wiki/` edits can be
//! added in a later pass once we can distinguish our own writes from
//! external ones.
//!
//! The watcher bridges `notify-debouncer-full`'s blocking std-mpsc channel
//! to a tokio channel via a dedicated thread, so the main async task can
//! await events without blocking a tokio worker.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::{new_debouncer, DebouncedEvent};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::error::{Error, Result};
use crate::ingest;
use crate::llm::LlmProvider;
use crate::vault::Vault;

const DEDUPE_WINDOW: Duration = Duration::from_secs(10);
const DEBOUNCE: Duration = Duration::from_secs(2);

/// Run the watcher until the returned task is dropped or the watch loop
/// encounters a fatal error. Blocks the calling task.
pub async fn watch(vault: Vault, provider: Arc<dyn LlmProvider>) -> Result<()> {
    let sources_dir: PathBuf = vault.sources_dir().into_std_path_buf();
    if !sources_dir.is_dir() {
        return Err(Error::VaultMissing(sources_dir));
    }
    let (tokio_tx, mut tokio_rx) = mpsc::unbounded_channel::<PathBuf>();
    let (std_tx, std_rx) = std::sync::mpsc::channel();

    // Bridge thread: reads the blocking notify channel and forwards paths
    // into the async tokio channel.
    std::thread::spawn(move || {
        for result in std_rx {
            match result {
                Ok(events) => {
                    forward_events(events, &tokio_tx);
                }
                Err(errors) => {
                    for e in errors {
                        warn!(error = ?e, "watch: notify error");
                    }
                }
            }
        }
    });

    let mut debouncer = new_debouncer(DEBOUNCE, None, std_tx)
        .map_err(|e| Error::Other(anyhow::anyhow!("watcher setup: {e}")))?;
    debouncer
        .watcher()
        .watch(&sources_dir, RecursiveMode::Recursive)
        .map_err(|e| Error::Other(anyhow::anyhow!("watch {}: {e}", sources_dir.display())))?;
    info!(dir = %sources_dir.display(), "watch: started");

    let mut dedupe: HashMap<PathBuf, Instant> = HashMap::new();
    while let Some(path) = tokio_rx.recv().await {
        dedupe.retain(|_, t| t.elapsed() < DEDUPE_WINDOW);
        if dedupe.contains_key(&path) {
            debug!(path = %path.display(), "watch: skip recently-seen path");
            continue;
        }
        dedupe.insert(path.clone(), Instant::now());

        if !is_ingestable_source(&path) {
            debug!(path = %path.display(), "watch: skip non-source file");
            continue;
        }
        info!(path = %path.display(), "watch: ingesting");
        match ingest::ingest(&vault, provider.as_ref(), &path).await {
            Ok(report) => info!(
                source = %report.source,
                commit = %report.commit_id,
                created = report.created,
                updated = report.updated,
                "watch: ingested"
            ),
            Err(err) => warn!(error = %err, "watch: ingest failed"),
        }
    }

    // The debouncer is kept alive for the life of this function; dropping it
    // (and the spawned bridge thread closes its sender) causes the tokio
    // channel to finish returning None.
    drop(debouncer);
    Ok(())
}

fn forward_events(events: Vec<DebouncedEvent>, tx: &mpsc::UnboundedSender<PathBuf>) {
    for ev in events {
        if !matches!(ev.event.kind, EventKind::Create(_) | EventKind::Modify(_)) {
            continue;
        }
        for path in &ev.event.paths {
            let _ = tx.send(path.clone());
        }
    }
}

fn is_ingestable_source(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "md" | "markdown" | "txt" | "text"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_ingestable_source_accepts_markdown_files() {
        let dir = tempfile::tempdir().unwrap();
        let md = dir.path().join("foo.md");
        std::fs::write(&md, "body").unwrap();
        assert!(is_ingestable_source(&md));

        let txt = dir.path().join("bar.txt");
        std::fs::write(&txt, "body").unwrap();
        assert!(is_ingestable_source(&txt));

        let pdf = dir.path().join("baz.pdf");
        std::fs::write(&pdf, "body").unwrap();
        assert!(!is_ingestable_source(&pdf));

        // Non-existent path is not ingestable.
        assert!(!is_ingestable_source(&dir.path().join("missing.md")));
    }
}
