//! File-system watcher that auto-ingests new sources.
//!
//! The watcher is intentionally narrow in v1: it watches `sources/` only,
//! and fires [`ingest::ingest`] for any new markdown, text, HTML, PDF,
//! Office or EPUB file that lands there (but not the `.extracted.md`
//! siblings that ingest writes). It does **not** watch `wiki/` — that
//! would create a feedback loop with its own ingest writes, and we'd need
//! a recently-written cache to break it. Embedding refresh on `wiki/`
//! edits can be added in a later pass once we can distinguish our own
//! writes from external ones.
//!
//! Each ingest that commits is embedded at once, as `scriptorium ingest`
//! does, so its pages are searchable without a manual `reindex`.
//!
//! The watcher bridges `notify-debouncer-full`'s blocking std-mpsc channel
//! to a tokio channel via a dedicated thread, so the main async task can
//! await events without blocking a tokio worker.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use notify::{EventKind, RecursiveMode};
use notify_debouncer_full::{new_debouncer, DebouncedEvent};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::ChunkStrategy;
use crate::embed::EmbeddingsStore;
use crate::error::{Error, Result};
use crate::hooks::HooksConfig;
use crate::ingest;
use crate::llm::LlmProvider;
use crate::vault::Vault;

const DEDUPE_WINDOW: Duration = Duration::from_secs(10);
const DEBOUNCE: Duration = Duration::from_secs(2);

/// What the watcher needs to embed the pages each ingest commits.
pub struct WatchEmbed<'a> {
    /// The vault's embeddings store.
    pub store: &'a EmbeddingsStore,
    /// The embeddings provider.
    pub provider: &'a dyn LlmProvider,
    /// The embeddings model.
    pub model: &'a str,
    /// Give each chunk of a multi-chunk page a situating header from the
    /// watcher's chat provider, as `[embeddings] contextual` asks.
    pub contextual: bool,
}

/// Run the watcher until the returned task is dropped or the watch loop
/// encounters a fatal error. Blocks the calling task. `on_ready` runs once
/// the watcher is armed: a file that lands in `sources/` after it runs is
/// picked up. The pages of each ingest that commits are embedded through
/// `embed`.
pub async fn watch(
    vault: Vault,
    provider: Arc<dyn LlmProvider>,
    hooks: Option<HooksConfig>,
    embed: WatchEmbed<'_>,
    on_ready: impl FnOnce(),
) -> Result<()> {
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
        .watch(&sources_dir, RecursiveMode::Recursive)
        .map_err(|e| Error::Other(anyhow::anyhow!("watch {}: {e}", sources_dir.display())))?;
    info!(dir = %sources_dir.display(), "watch: started");
    on_ready();

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
        // Fire on_watch_trigger hook (can abort this file's ingest).
        if let Some(ref h) = hooks {
            if let Err(e) = crate::hooks::on_watch_trigger(h, &path).await {
                warn!(path = %path.display(), error = %e, "watch: hook aborted ingest");
                continue;
            }
        }

        ingest_source(&vault, provider.as_ref(), &embed, &path).await;
    }

    // The debouncer is kept alive for the life of this function; dropping it
    // (and the spawned bridge thread closes its sender) causes the tokio
    // channel to finish returning None.
    drop(debouncer);
    Ok(())
}

/// Ingest one source and embed what the ingest committed, so its pages are
/// searchable at once. Failures are logged and the watcher goes on. The
/// next ingest that commits, or `scriptorium reindex`, embeds what a failed
/// embed left out.
async fn ingest_source(
    vault: &Vault,
    provider: &dyn LlmProvider,
    embed: &WatchEmbed<'_>,
    path: &Path,
) {
    info!(path = %path.display(), "watch: ingesting");
    let report = match ingest::ingest(vault, provider, path).await {
        Ok(report) => report,
        Err(err) => {
            warn!(error = %err, "watch: ingest failed");
            return;
        }
    };
    info!(
        source = %report.source,
        commit = %report.commit_id,
        created = report.created,
        updated = report.updated,
        "watch: ingested"
    );
    // A near-duplicate skip commits nothing.
    if report.commit_id == ingest::NEAR_DUPLICATE_SKIP {
        return;
    }
    // Embed as `scriptorium reindex` does. Plain rows would pass for cache
    // hits there and never get their situating headers.
    let contextual_chat = embed.contextual.then_some(provider);
    match crate::embed::reindex_contextual(
        vault,
        embed.store,
        embed.provider,
        embed.model,
        ChunkStrategy::default(),
        contextual_chat,
    )
    .await
    {
        Ok(chunks) => info!(chunks, "watch: embedded"),
        Err(err) => warn!(error = %err, "watch: embedding failed"),
    }
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
    // Everything the extract router can handle (binary formats included) —
    // extraction failures surface as ingest errors, not watcher crashes.
    // `.extracted.md` siblings of converted binaries are our own writes, as
    // are the `<hash>-…` copies ingest interns. Ingesting an interned copy
    // interns it again under a longer name, which lands here again: a
    // source too short for the near-duplicate gate would loop forever.
    if path
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.ends_with(".extracted.md") || ingest::is_interned_name(n))
    {
        return false;
    }
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "md" | "markdown"
            | "txt"
            | "text"
            | "pdf"
            | "docx"
            | "pptx"
            | "xlsx"
            | "epub"
            | "html"
            | "htm"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::MockProvider;

    #[test]
    fn is_ingestable_source_accepts_markdown_files() {
        let dir = tempfile::tempdir().unwrap();
        let md = dir.path().join("foo.md");
        std::fs::write(&md, "body").unwrap();
        assert!(is_ingestable_source(&md));

        let txt = dir.path().join("bar.txt");
        std::fs::write(&txt, "body").unwrap();
        assert!(is_ingestable_source(&txt));

        // Binary formats route through the extract router since the
        // format-router change — the watcher accepts them now.
        let pdf = dir.path().join("baz.pdf");
        std::fs::write(&pdf, "body").unwrap();
        assert!(is_ingestable_source(&pdf));

        // Extracted-text siblings of converted binaries are our own
        // writes and must not re-trigger ingest.
        let sibling = dir.path().join("baz.pdf.extracted.md");
        std::fs::write(&sibling, "body").unwrap();
        assert!(!is_ingestable_source(&sibling));

        // So are the copies ingest interns.
        let interned = dir.path().join("4ed163f8d545-foo.md");
        std::fs::write(&interned, "body").unwrap();
        assert!(!is_ingestable_source(&interned));

        // Unknown binary formats stay rejected.
        let zip = dir.path().join("qux.zip");
        std::fs::write(&zip, "body").unwrap();
        assert!(!is_ingestable_source(&zip));

        // Non-existent path is not ingestable.
        assert!(!is_ingestable_source(&dir.path().join("missing.md")));
    }

    /// A vault that ingest can write to, with `name` in `sources/articles/`
    /// holding `body`. Returns the source's path.
    fn vault_with_source(name: &str, body: &str) -> (tempfile::TempDir, Vault, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki/concepts")).unwrap();
        std::fs::create_dir_all(dir.path().join("sources/articles")).unwrap();
        std::fs::write(
            dir.path().join("CLAUDE.md"),
            "# Vault Rules\n\nBe concise.\n",
        )
        .unwrap();
        let source = dir.path().join("sources/articles").join(name);
        std::fs::write(&source, body).unwrap();
        let vault = Vault::open(dir.path()).unwrap();
        (dir, vault, source)
    }

    /// The plan the chat provider answers an ingest with: one new page,
    /// whose two sections make two chunks.
    fn two_chunk_plan() -> String {
        use crate::llm::prompts::{IngestAction, IngestPageAction, IngestPlan};
        serde_json::to_string(&IngestPlan {
            summary: "watch test".into(),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/concepts/watched.md".into(),
                title: "Watched".into(),
                tags: vec![],
                body: "## Overview\n\nBody for the watched page.\n\n\
                       ## Details\n\nMore about the watched page.\n"
                    .into(),
                source_quote: None,
                supersedes: vec![],
            }],
            log_entry: "watch ingest".into(),
            redundant: false,
        })
        .unwrap()
    }

    /// Chunks in `store` that carry a situating header written by
    /// [`contextual_chat`].
    fn context_hits(store: &EmbeddingsStore) -> usize {
        store
            .keyword_search("zephyrctx", "mock", "mock-1", 10)
            .unwrap()
            .len()
    }

    /// A chat provider that answers a request for situating headers with
    /// one header per chunk of the page in [`two_chunk_plan`], and any other
    /// request with that plan.
    fn contextual_chat() -> MockProvider {
        MockProvider::with_fixtures([
            (
                "situating".to_string(),
                r#"{"contexts": ["zephyrctx overview", "zephyrctx details"]}"#.to_string(),
            ),
            (String::new(), two_chunk_plan()),
        ])
    }

    #[tokio::test]
    async fn ingest_source_embeds_the_pages_its_ingest_commits() {
        let (_dir, vault, source) = vault_with_source("note.md", "# Note\n\nA watched note.\n");
        let store = EmbeddingsStore::in_memory().unwrap();
        let embedder = MockProvider::constant("");
        let embed = WatchEmbed {
            store: &store,
            provider: &embedder,
            model: "mock-1",
            contextual: false,
        };

        ingest_source(&vault, &contextual_chat(), &embed, &source).await;

        assert_eq!(store.distinct_page_count().unwrap(), 1);
        assert_eq!(
            context_hits(&store),
            0,
            "contextual retrieval is off, so no chunk carries a header"
        );
    }

    /// With `[embeddings] contextual`, the watcher embeds as `scriptorium
    /// reindex` does: each chunk gets a situating header from the chat
    /// provider.
    #[tokio::test]
    async fn ingest_source_embeds_contextually_when_asked() {
        let (_dir, vault, source) = vault_with_source("note.md", "# Note\n\nA watched note.\n");
        let store = EmbeddingsStore::in_memory().unwrap();
        let embedder = MockProvider::constant("");
        let embed = WatchEmbed {
            store: &store,
            provider: &embedder,
            model: "mock-1",
            contextual: true,
        };

        ingest_source(&vault, &contextual_chat(), &embed, &source).await;

        assert_eq!(context_hits(&store), 2, "each chunk carries its header");
    }

    /// A near-duplicate skip commits nothing, so nothing is embedded.
    #[tokio::test]
    async fn ingest_source_embeds_nothing_after_a_near_duplicate_skip() {
        let words: Vec<String> = (0..120).map(|i| format!("watch{i}")).collect();
        let body = format!("{}\n", words.join(" "));
        let (dir, vault, first) = vault_with_source("first.md", &body);
        let store = EmbeddingsStore::in_memory().unwrap();
        let embedder = MockProvider::constant("");
        let embed = WatchEmbed {
            store: &store,
            provider: &embedder,
            model: "mock-1",
            contextual: false,
        };
        ingest_source(&vault, &contextual_chat(), &embed, &first).await;
        assert_eq!(store.distinct_page_count().unwrap(), 1);
        // Drop the page's rows, so an embed would show as rows again.
        store.retain_page_versions(&[]).unwrap();

        let again = dir.path().join("sources/articles/again.md");
        std::fs::write(&again, format!("{body}one closing line\n")).unwrap();
        ingest_source(&vault, &MockProvider::constant("{}"), &embed, &again).await;

        assert_eq!(store.len().unwrap(), 0, "a skip must not embed");
    }
}
