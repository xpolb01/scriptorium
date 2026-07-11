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
    for ((chunk, vector), text) in chunks.iter().zip(vectors.into_iter()).zip(texts.iter()) {
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
    #[derive(serde::Deserialize)]
    struct Contexts {
        contexts: Vec<String>,
    }
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
    use crate::llm::MockProvider;
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

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
}
