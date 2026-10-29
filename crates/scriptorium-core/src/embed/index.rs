//! Helpers that tie chunking, [`EmbeddingsStore`](super::EmbeddingsStore),
//! and an [`LlmProvider`] together so callers don't have to reassemble the
//! pipeline each time.
//!
//! - [`embed_page`] embeds a single page, skipping the work if the store
//!   already has a row for this `(page_id, content_hash, provider, model)`.
//! - [`reindex`] walks the vault and embeds every page it finds.
//! - [`chunk_with_strategy`] dispatches to the configured chunker.
//!
//! Both are intentionally async — the `LlmProvider::embed` call hits the
//! network for real providers. The mock provider returns immediately so the
//! integration tests stay fast.

use std::fs::OpenOptions;
use std::time::Duration;

use tracing::debug;

use super::{
    chunk_page, chunk_page_recursive, chunk_page_semantic, Chunk, EmbeddingRow, EmbeddingsStore,
};
use crate::config::ChunkStrategy;
use crate::error::{Error, Result};
use crate::llm::LlmProvider;
use crate::vault::{Page, PageId, Vault};

/// Default chunk size, in characters. ~4000 chars ≈ ~1000 tokens, which
/// comfortably fits every popular embedding model's per-request limit.
pub const DEFAULT_CHUNK_CHARS: usize = 4000;

/// Split a page body into chunks using the specified strategy.
///
/// `embed_provider` is only needed for [`ChunkStrategy::Semantic`]; pass
/// `None` for heading or recursive strategies.
pub async fn chunk_with_strategy(
    body: &str,
    max_chars: usize,
    strategy: ChunkStrategy,
    embed_provider: Option<(&dyn LlmProvider, &str)>,
) -> Result<Vec<Chunk>> {
    match strategy {
        ChunkStrategy::Heading => Ok(chunk_page(body, max_chars)),
        ChunkStrategy::Recursive => Ok(chunk_page_recursive(body, max_chars)),
        ChunkStrategy::Semantic => {
            let (provider, model) = embed_provider.ok_or_else(|| {
                Error::Other(anyhow::anyhow!(
                    "semantic chunking requires an embedding provider"
                ))
            })?;
            chunk_page_semantic(body, max_chars, provider, model).await
        }
    }
}

/// Embed a single page, inserting one row per chunk. Returns the number of
/// chunks embedded, or `0` if the store already had cached vectors for the
/// current `(page_id, content_hash, provider, model)`.
///
/// Uses the specified `strategy` for chunking. Pass `ChunkStrategy::default()`
/// for the standard recursive chunker.
pub async fn embed_page(
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    page: &Page,
    strategy: ChunkStrategy,
) -> Result<usize> {
    embed_page_contextual(store, provider, model, page, strategy, None).await
}

/// Like [`embed_page`], with optional Anthropic-style contextual
/// augmentation: when `chat` is provided, one LLM call per page writes a
/// short "where this chunk sits in the document" header per chunk, and
/// the header is prepended to the chunk text before embedding *and*
/// before FTS indexing (both retrieval paths benefit).
///
/// Augmentation is strictly best-effort — any failure falls back to the
/// plain chunks. Note the embeddings cache keys on the page content hash,
/// so toggling contextual mode on requires a rebuild (`reindex --rebuild`)
/// to take effect for already-embedded pages.
pub async fn embed_page_contextual(
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    page: &Page,
    strategy: ChunkStrategy,
    chat: Option<&dyn LlmProvider>,
) -> Result<usize> {
    let hash = page.content_hash()?;
    if store.has_page_version(page.frontmatter.id, &hash, provider.name(), model)? {
        debug!(page = %page.path, "embed cache hit");
        return Ok(0);
    }
    let chunks = chunk_with_strategy(
        &page.body,
        DEFAULT_CHUNK_CHARS,
        strategy,
        Some((provider, model)),
    )
    .await?;
    if chunks.is_empty() {
        return Ok(0);
    }

    // Optional contextual headers. Single-chunk pages need no situating.
    let texts: Vec<String> = match chat {
        Some(chat_provider) if chunks.len() >= 2 => {
            match contextualize_chunks(chat_provider, &page.frontmatter.title, &chunks).await {
                Some(contexts) => chunks
                    .iter()
                    .zip(contexts)
                    .map(|(c, ctx)| {
                        if ctx.trim().is_empty() {
                            c.text.clone()
                        } else {
                            format!("{}\n\n{}", ctx.trim(), c.text)
                        }
                    })
                    .collect(),
                None => chunks.iter().map(|c| c.text.clone()).collect(),
            }
        }
        _ => chunks.iter().map(|c| c.text.clone()).collect(),
    };

    let vectors = provider
        .embed(&texts)
        .await
        .map_err(|e| Error::Other(anyhow::anyhow!("embed call: {e}")))?;
    if vectors.len() != chunks.len() {
        return Err(Error::Other(anyhow::anyhow!(
            "provider returned {} vectors for {} chunks",
            vectors.len(),
            chunks.len()
        )));
    }
    for ((chunk, vector), text) in chunks.iter().zip(vectors).zip(texts.iter()) {
        store.upsert(&EmbeddingRow {
            page_id: page.frontmatter.id,
            content_hash: hash.clone(),
            chunk_idx: chunk.idx,
            chunk_text: text.clone(),
            heading: chunk.heading.clone(),
            provider: provider.name().to_string(),
            model: model.to_string(),
            vector,
        })?;
    }
    Ok(chunks.len())
}

/// One LLM call producing a short situating context per chunk (Anthropic
/// contextual-retrieval pattern). Returns `None` on any failure or count
/// mismatch; the caller falls back to plain chunks.
async fn contextualize_chunks(
    chat: &dyn LlmProvider,
    page_title: &str,
    chunks: &[Chunk],
) -> Option<Vec<String>> {
    use crate::llm::{CompletionRequest, Message, Role};
    #[derive(serde::Deserialize)]
    struct Contexts {
        contexts: Vec<String>,
    }
    let schema = serde_json::json!({
        "type": "object",
        "properties": {
            "contexts": {
                "type": "array",
                "items": {"type": "string"},
                "description": "One 1-2 sentence situating context per chunk, same order."
            }
        },
        "required": ["contexts"],
        "additionalProperties": false
    });
    let mut listing = String::new();
    for (i, c) in chunks.iter().enumerate() {
        use std::fmt::Write as _;
        let preview: String = c.text.chars().take(600).collect();
        let _ = writeln!(listing, "--- chunk {i} ---\n{preview}");
    }
    let req = CompletionRequest {
        system: "For each chunk of the document, write a succinct 1-2 sentence \
                 context situating it within the whole document (what it is \
                 about, which entity/section it belongs to). The context is \
                 prepended to the chunk to improve search retrieval. Return \
                 JSON with exactly one context per chunk, in order."
            .to_string(),
        messages: vec![Message {
            role: Role::User,
            content: format!("Document: {page_title}\n\n{listing}"),
        }],
        // Generous budget — reasoning-heavy proxy routes spend completion
        // tokens before the JSON (see eval::faithfulness).
        max_tokens: 4096,
        temperature: Some(0.0),
        response_schema: Some(schema),
    };
    let resp = chat.complete(req).await.ok()?;
    let payload = crate::llm::extract_json_payload(&resp.text);
    let parsed: Contexts = serde_json::from_str(&payload).ok()?;
    if parsed.contexts.len() == chunks.len() {
        debug!(page = page_title, "contextual headers generated");
        Some(parsed.contexts)
    } else {
        debug!(
            got = parsed.contexts.len(),
            want = chunks.len(),
            "contextual header count mismatch, falling back"
        );
        None
    }
}

/// Walk the vault and reconcile the embeddings store with the current
/// page set:
///
/// 1. Embed every page whose `(page_id, content_hash, provider, model)`
///    row isn't already cached (the incremental case — unchanged pages
///    are free).
/// 2. Prune orphan rows from the store — any `(page_id, content_hash)`
///    pair that no longer appears in the current scan. This covers
///    both pages that were removed (delete, `scriptorium undo` /
///    git-revert, rename) and pages whose content has since changed
///    (old hash is no longer current).
///
/// Returns the number of *newly embedded* chunks (the incremental insert
/// count). The pruned-row count is intentionally not part of this
/// return value — callers that want observability on pruning should use
/// [`EmbeddingsStore::retain_page_versions`] directly, or inspect
/// [`EmbeddingsStore::len`] before and after. Most callers just want
/// "how many new chunks did I pay to embed this run", which is `total`.
///
/// Runs take `<vault>/.scriptorium/embed.lock` in turn, across processes,
/// from the scan to the prune. Without it, a run that scanned before
/// another run committed and embedded a page would prune that page's rows.
/// A run waits for as long as another run holds the lock.
pub async fn reindex(
    vault: &Vault,
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
) -> Result<usize> {
    reindex_with_strategy(vault, store, provider, model, ChunkStrategy::default()).await
}

/// Like [`reindex`] but with an explicit chunking strategy.
pub async fn reindex_with_strategy(
    vault: &Vault,
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    strategy: ChunkStrategy,
) -> Result<usize> {
    reindex_contextual(vault, store, provider, model, strategy, None).await
}

/// File, under the vault's meta dir, that reindex runs take in turn across
/// processes. See [`reindex`].
const EMBED_LOCK_FILE: &str = "embed.lock";

/// How long a reindex run waits before it tries [`EMBED_LOCK_FILE`] again.
const EMBED_LOCK_RETRY: Duration = Duration::from_millis(200);

/// Like [`reindex_with_strategy`], with optional contextual chunk
/// augmentation via `chat` (see [`embed_page_contextual`]).
pub async fn reindex_contextual(
    vault: &Vault,
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    strategy: ChunkStrategy,
    chat: Option<&dyn LlmProvider>,
) -> Result<usize> {
    reindex_under_lock(vault, store, provider, model, strategy, chat, false).await
}

/// Like [`reindex_contextual`], but first drops every cached row, so a
/// strategy or contextual-mode change re-embeds every page. The drop
/// happens under the embed lock: a run that held the lock while this one
/// waited would otherwise refill the store, and this run would take those
/// rows for cache hits.
pub async fn rebuild_contextual(
    vault: &Vault,
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    strategy: ChunkStrategy,
    chat: Option<&dyn LlmProvider>,
) -> Result<usize> {
    reindex_under_lock(vault, store, provider, model, strategy, chat, true).await
}

/// Take the embed lock, then run [`reindex_locked`]. See [`reindex`].
async fn reindex_under_lock(
    vault: &Vault,
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    strategy: ChunkStrategy,
    chat: Option<&dyn LlmProvider>,
    rebuild: bool,
) -> Result<usize> {
    let meta = vault.meta_dir().into_std_path_buf();
    std::fs::create_dir_all(&meta).map_err(|e| Error::io(&meta, e))?;
    let lock_path = meta.join(EMBED_LOCK_FILE);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| Error::io(&lock_path, e))?;
    let mut lock = fd_lock::RwLock::new(file);
    // Poll rather than block: a blocking wait would stall the async
    // runtime's thread, and another task in this process may hold the lock.
    loop {
        match lock.try_write() {
            Ok(_guard) => {
                return reindex_locked(vault, store, provider, model, strategy, chat, rebuild)
                    .await;
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(e) => return Err(Error::io(&lock_path, e)),
        }
        tokio::time::sleep(EMBED_LOCK_RETRY).await;
    }
}

/// The body of a reindex run, run while it holds the embed lock. A
/// `rebuild` drops every cached row first.
async fn reindex_locked(
    vault: &Vault,
    store: &EmbeddingsStore,
    provider: &dyn LlmProvider,
    model: &str,
    strategy: ChunkStrategy,
    chat: Option<&dyn LlmProvider>,
    rebuild: bool,
) -> Result<usize> {
    if rebuild {
        store.retain_page_versions(&[])?;
    }
    let scan = vault.scan()?;
    let mut total = 0;

    // Compute each page's current hash once: `embed_page` needs it, and
    // so does the keep-set we build for pruning. Doing it up front lets
    // us hand the same values to both phases.
    let mut current_versions: Vec<(PageId, String)> = Vec::with_capacity(scan.pages.len());
    for page in &scan.pages {
        let hash = page.content_hash()?;
        current_versions.push((page.frontmatter.id, hash));
    }

    // Phase 1: embed pages. `embed_page` computes its own hash inside
    // `has_page_version` to decide cache-hit/miss; for unchanged pages
    // this is an indexed lookup, no embedding work.
    for page in &scan.pages {
        total += embed_page_contextual(store, provider, model, page, strategy, chat).await?;
    }

    // Phase 2: prune orphan rows. Any row whose (page_id, content_hash)
    // isn't in `current_versions` is an orphan — either from a removed
    // page or a superseded content version.
    let keep: Vec<(PageId, &str)> = current_versions
        .iter()
        .map(|(id, hash)| (*id, hash.as_str()))
        .collect();
    store.retain_page_versions(&keep)?;

    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::llm::{CompletionRequest, CompletionResponse, LlmError, MockProvider};
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::{Notify, Semaphore};

    fn make_page(stem: &str, body: &str) -> Page {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from(format!("wiki/{stem}.md")),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: stem.into(),
                created: now,
                updated: now,
                sources: vec![],
                tags: vec![],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: body.into(),
        }
    }

    #[tokio::test]
    async fn embed_page_populates_store_and_is_cached() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let mock = MockProvider::constant("");
        let page = make_page("alpha", "## First\n\nSome content.\n\n## Second\n\nMore.\n");
        // Use Heading strategy for this test since the body has clear H2 sections.
        let inserted = embed_page(&store, &mock, "mock-1", &page, ChunkStrategy::Heading)
            .await
            .unwrap();
        assert_eq!(inserted, 2, "two H2 sections → two chunks");
        assert_eq!(store.len().unwrap(), 2);
        // A second call is a cache hit.
        let again = embed_page(&store, &mock, "mock-1", &page, ChunkStrategy::Heading)
            .await
            .unwrap();
        assert_eq!(again, 0);
        assert_eq!(store.len().unwrap(), 2);
    }

    #[tokio::test]
    async fn embed_page_handles_empty_body() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let mock = MockProvider::constant("");
        let page = make_page("empty", "");
        let inserted = embed_page(&store, &mock, "mock-1", &page, ChunkStrategy::default())
            .await
            .unwrap();
        assert_eq!(inserted, 0);
        assert_eq!(store.len().unwrap(), 0);
    }

    /// Embeds like [`MockProvider`], but each `embed` call first reports
    /// that it started, then waits until the test opens `gate`.
    struct GatedEmbedder {
        inner: MockProvider,
        reached: AtomicBool,
        entered: Notify,
        gate: Semaphore,
    }

    impl GatedEmbedder {
        fn new() -> Self {
            Self {
                inner: MockProvider::constant(""),
                reached: AtomicBool::new(false),
                entered: Notify::new(),
                gate: Semaphore::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl LlmProvider for GatedEmbedder {
        async fn complete(
            &self,
            req: CompletionRequest,
        ) -> std::result::Result<CompletionResponse, LlmError> {
            self.inner.complete(req).await
        }

        async fn embed(&self, texts: &[String]) -> std::result::Result<Vec<Vec<f32>>, LlmError> {
            self.reached.store(true, Ordering::SeqCst);
            self.entered.notify_one();
            let _permit = self.gate.acquire().await.expect("gate is never closed");
            self.inner.embed(texts).await
        }

        fn name(&self) -> &str {
            self.inner.name()
        }

        fn context_window(&self) -> usize {
            self.inner.context_window()
        }

        fn embedding_dim(&self) -> usize {
            self.inner.embedding_dim()
        }
    }

    /// Runs `first`, which must reach `gated`'s provider, and, once it waits
    /// there, `then`. The gate opens 500 ms after `then` starts, so `then`
    /// overlaps the rest of `first`. Fails rather than hangs when a run
    /// stalls or `first` returns before it reaches the provider.
    async fn overlap<A: Future, B: Future>(
        gated: &GatedEmbedder,
        first: A,
        then: B,
    ) -> (A::Output, B::Output)
    where
        A::Output: std::fmt::Debug,
    {
        let run_first = async {
            let out = first.await;
            // Lets `then` start if `first` returned before the provider.
            gated.entered.notify_one();
            out
        };
        let run_then = async {
            gated.entered.notified().await;
            let open_gate_later = async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                gated.gate.add_permits(1);
            };
            tokio::join!(then, open_gate_later).0
        };
        let outputs = tokio::time::timeout(Duration::from_secs(30), async {
            tokio::join!(run_first, run_then)
        })
        .await
        .expect("both runs finish");
        assert!(
            gated.reached.load(Ordering::SeqCst),
            "the first run never reached the provider, so nothing overlapped it: {:?}",
            outputs.0
        );
        outputs
    }

    /// Writes `page` under `root` at its vault path.
    fn write_page(root: &Path, page: &Page) {
        let path = root.join(page.path.as_str());
        std::fs::write(path, page.to_markdown().unwrap()).unwrap();
    }

    /// A vault in a temp dir with one page, `stem`.
    fn vault_with_page(stem: &str) -> (tempfile::TempDir, Vault) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki")).unwrap();
        write_page(dir.path(), &make_page(stem, &format!("{stem} body.\n")));
        let vault = Vault::open(dir.path()).unwrap();
        (dir, vault)
    }

    /// Run A scans, then stalls in the provider. A page is written and run B
    /// indexes it. When A finishes, its prune must keep B's rows for the new
    /// page.
    #[tokio::test]
    async fn concurrent_reindex_keeps_rows_of_a_page_written_after_its_scan() {
        let (dir, vault) = vault_with_page("alpha");
        let store = EmbeddingsStore::in_memory().unwrap();
        let gated = GatedEmbedder::new();
        let plain = MockProvider::constant("");

        let write_beta_then_reindex = async {
            write_page(dir.path(), &make_page("beta", "beta body.\n"));
            reindex(&vault, &store, &plain, "mock-1").await
        };
        let (run_a, run_b) = overlap(
            &gated,
            reindex(&vault, &store, &gated, "mock-1"),
            write_beta_then_reindex,
        )
        .await;
        run_a.unwrap();
        run_b.unwrap();

        let pages = vault.scan().unwrap().pages;
        assert_eq!(pages.len(), 2);
        for page in pages {
            let hash = page.content_hash().unwrap();
            assert!(
                store.has_any_version(page.frontmatter.id, &hash).unwrap(),
                "{} lost its embeddings",
                page.path
            );
        }
    }

    /// A rebuild that waits for the lock must drop the cached rows only once
    /// it holds the lock. Dropped earlier, the rows the lock holder writes
    /// meanwhile would pass for cache hits, and the rebuild would re-embed
    /// nothing.
    #[tokio::test]
    async fn rebuild_waiting_for_the_lock_re_embeds_rows_written_meanwhile() {
        let (_dir, vault) = vault_with_page("alpha");
        let store = EmbeddingsStore::in_memory().unwrap();
        let gated = GatedEmbedder::new();
        let plain = MockProvider::constant("");

        let rebuild_run = rebuild_contextual(
            &vault,
            &store,
            &plain,
            "mock-1",
            ChunkStrategy::default(),
            None,
        );
        let (run_a, re_embedded) = overlap(
            &gated,
            reindex(&vault, &store, &gated, "mock-1"),
            rebuild_run,
        )
        .await;
        let written = run_a.unwrap();
        assert!(written > 0);
        assert_eq!(
            re_embedded.unwrap(),
            written,
            "the rebuild re-embeds every chunk"
        );
    }
}
