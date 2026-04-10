//! Embeddings cache and hybrid retrieval.
//!
//! Scriptorium needs a way to "ask the vault what pages are relevant to this
//! question" before it calls the LLM. That's the job of this module:
//!
//! - [`chunk::chunk_page`] breaks each page into heading-scoped chunks small
//!   enough to fit an embedding model's context (roughly 1000 tokens each).
//! - [`store::EmbeddingsStore`] persists one row per chunk in a `SQLite`
//!   database, keyed by `(page_id, content_hash, chunk_idx, provider,
//!   model)`. The `content_hash` in the key means renames are free and
//!   edits never return stale vectors — a re-scan re-embeds only what
//!   actually changed.
//! - [`store::EmbeddingsStore::search`] computes cosine top-k over every
//!   chunk in the store and returns ranked [`SearchHit`]s. It's a linear
//!   scan; good enough for tens of thousands of chunks, and trivially
//!   swappable for an ANN index later.
//!
//! The ingest pipeline in [`crate::ingest`] will grow an
//! `EmbeddingsStore::embed_page` call in phase 10 so the query side has real
//! vectors to search over.

pub mod chunk;
pub mod chunk_recursive;
pub mod chunk_semantic;
pub mod index;
pub mod store;
pub mod vector_index;

pub use chunk::{chunk_page, Chunk};
pub use chunk_recursive::chunk_page_recursive;
pub use chunk_semantic::chunk_page_semantic;
pub use index::{chunk_with_strategy, embed_page, reindex, DEFAULT_CHUNK_CHARS};
pub use store::{EmbeddingRow, EmbeddingsStore, SearchHit};
pub use vector_index::{LinearIndex, VectorIndex};
