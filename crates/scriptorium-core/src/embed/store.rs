//! SQLite-backed embeddings store with cosine top-k search.
//!
//! Schema:
//!
//! ```sql
//! CREATE TABLE embeddings (
//!   page_id       TEXT    NOT NULL,
//!   content_hash  TEXT    NOT NULL,
//!   chunk_idx     INTEGER NOT NULL,
//!   chunk_text    TEXT    NOT NULL,
//!   heading       TEXT,
//!   provider      TEXT    NOT NULL,
//!   model         TEXT    NOT NULL,
//!   dim           INTEGER NOT NULL,
//!   vector        BLOB    NOT NULL,  -- little-endian f32[dim]
//!   PRIMARY KEY (page_id, content_hash, chunk_idx, provider, model)
//! );
//! ```
//!
//! The primary key enforces both identity and cache semantics: once you
//! insert a row, re-inserting the same `(page, hash, chunk, provider, model)`
//! is a no-op, so a re-scan that finds a page unchanged produces zero work.
//! When a page changes, its `content_hash` changes, and the insert lands
//! alongside the old rows (which can be garbage-collected separately).
//!
//! Search is a straight linear scan for the moment. For a vault with tens of
//! thousands of chunks this is still millisecond-scale; upgrading to an ANN
//! index (HNSW, `DiskANN`) is a drop-in replacement of this one module.

use std::path::Path;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::vault::PageId;

/// Errors specific to the embeddings store, wrapped into [`Error::Other`].
// `map_err` consumes the error by value, so taking `&rusqlite::Error` here
// would force every call site into `|e| wrap_sql(&e)`.
#[allow(clippy::needless_pass_by_value)]
fn wrap_sql(e: rusqlite::Error) -> Error {
    Error::Other(anyhow::anyhow!("sqlite: {e}"))
}

/// One row to insert into the store. Consumes nothing — the caller keeps
/// ownership until [`EmbeddingsStore::upsert`] returns.
#[derive(Debug, Clone)]
pub struct EmbeddingRow {
    pub page_id: PageId,
    pub content_hash: String,
    pub chunk_idx: usize,
    pub chunk_text: String,
    pub heading: Option<String>,
    pub provider: String,
    pub model: String,
    pub vector: Vec<f32>,
}

/// One result from [`EmbeddingsStore::search`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHit {
    pub page_id: PageId,
    pub chunk_idx: usize,
    pub heading: Option<String>,
    pub chunk_text: String,
    pub score: f32,
}

pub struct EmbeddingsStore {
    conn: Connection,
}

impl EmbeddingsStore {
    /// Open (or create) a store at the given filesystem path.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path).map_err(wrap_sql)?;
        Self::init(conn)
    }

    /// In-memory store, useful for tests.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(wrap_sql)?;
        Self::init(conn)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            r"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous  = NORMAL;
            CREATE TABLE IF NOT EXISTS embeddings (
                page_id      TEXT    NOT NULL,
                content_hash TEXT    NOT NULL,
                chunk_idx    INTEGER NOT NULL,
                chunk_text   TEXT    NOT NULL,
                heading      TEXT,
                provider     TEXT    NOT NULL,
                model        TEXT    NOT NULL,
                dim          INTEGER NOT NULL,
                vector       BLOB    NOT NULL,
                PRIMARY KEY (page_id, content_hash, chunk_idx, provider, model)
            );
            CREATE INDEX IF NOT EXISTS idx_embeddings_page_hash
                ON embeddings(page_id, content_hash);
            CREATE INDEX IF NOT EXISTS idx_embeddings_provider_model
                ON embeddings(provider, model);
            ",
        )
        .map_err(wrap_sql)?;
        Ok(Self { conn })
    }

    /// Insert or replace a single row.
    pub fn upsert(&self, row: &EmbeddingRow) -> Result<()> {
        let bytes = vec_to_bytes(&row.vector);
        let chunk_idx = i64::try_from(row.chunk_idx)
            .map_err(|_| Error::Other(anyhow::anyhow!("chunk_idx overflow")))?;
        let dim = i64::try_from(row.vector.len())
            .map_err(|_| Error::Other(anyhow::anyhow!("dim overflow")))?;
        self.conn
            .execute(
                r"
                INSERT INTO embeddings
                    (page_id, content_hash, chunk_idx, chunk_text, heading,
                     provider, model, dim, vector)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                ON CONFLICT(page_id, content_hash, chunk_idx, provider, model)
                DO UPDATE SET
                    chunk_text = excluded.chunk_text,
                    heading    = excluded.heading,
                    dim        = excluded.dim,
                    vector     = excluded.vector
                ",
                params![
                    row.page_id.to_string(),
                    row.content_hash,
                    chunk_idx,
                    row.chunk_text,
                    row.heading,
                    row.provider,
                    row.model,
                    dim,
                    bytes,
                ],
            )
            .map_err(wrap_sql)?;
        Ok(())
    }

    /// Do we already have *any* row for this page at this content hash /
    /// provider / model? Used to skip re-embedding unchanged pages during a
    /// scan.
    pub fn has_page_version(
        &self,
        page_id: PageId,
        content_hash: &str,
        provider: &str,
        model: &str,
    ) -> Result<bool> {
        let found: Option<i64> = self
            .conn
            .query_row(
                r"
                SELECT 1 FROM embeddings
                 WHERE page_id = ?1
                   AND content_hash = ?2
                   AND provider = ?3
                   AND model = ?4
                 LIMIT 1
                ",
                params![page_id.to_string(), content_hash, provider, model],
                |row| row.get(0),
            )
            .optional()
            .map_err(wrap_sql)?;
        Ok(found.is_some())
    }

    /// Delete every row belonging to a specific (`page_id`, `content_hash`),
    /// leaving other versions alone.
    pub fn delete_version(&self, page_id: PageId, content_hash: &str) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM embeddings WHERE page_id = ?1 AND content_hash = ?2",
                params![page_id.to_string(), content_hash],
            )
            .map_err(wrap_sql)?;
        Ok(())
    }

    /// Prune the store down to only the rows whose `(page_id, content_hash)`
    /// appears in `keep`. Returns the total number of rows deleted.
    ///
    /// This reconciles the store against a fresh vault scan and handles
    /// **both** classes of orphan:
    ///
    /// 1. **Removed pages**: a page that existed when it was embedded but
    ///    has since been deleted, reverted (`scriptorium undo`), or
    ///    renamed. Its `page_id` is no longer in any scan entry, so it is
    ///    not in `keep`, and every row for that `page_id` is deleted.
    /// 2. **Stale content versions**: a page that is still present in
    ///    the vault but whose body has been edited. The new
    ///    `content_hash` lands alongside the old rows; passing only the
    ///    current `(page_id, current_hash)` in `keep` prunes every row
    ///    whose `content_hash` doesn't match.
    ///
    /// Callers should build `keep` from a fresh `vault.scan()` + per-page
    /// `Page::content_hash()`. The full call sequence lives in
    /// [`crate::embed::reindex`], which already does this.
    ///
    /// A single SQL `DELETE` per surviving stale version is cheap for the
    /// typical vault (tens to a few thousand pages). For vaults with
    /// hundreds of thousands of chunks, a temporary-table JOIN would be
    /// faster but is deliberately not implemented yet — premature.
    pub fn retain_page_versions(&self, keep: &[(PageId, &str)]) -> Result<usize> {
        use std::collections::HashSet;

        // Build a lookup set keyed by "page_id:hash". String keys avoid
        // adding a `Hash` bound on `PageId` for this call site.
        let keep_keys: HashSet<String> = keep
            .iter()
            .map(|(id, hash)| format!("{}:{}", id, hash))
            .collect();

        // Collect the distinct (page_id, content_hash) pairs currently
        // stored. Materialize into a Vec so the read stmt is dropped
        // before we run the delete statements.
        let current: Vec<(String, String)> = {
            let mut stmt = self
                .conn
                .prepare("SELECT DISTINCT page_id, content_hash FROM embeddings")
                .map_err(wrap_sql)?;
            let rows = stmt
                .query_map([], |row| {
                    let pid: String = row.get(0)?;
                    let hash: String = row.get(1)?;
                    Ok((pid, hash))
                })
                .map_err(wrap_sql)?;
            let mut out = Vec::new();
            for r in rows {
                out.push(r.map_err(wrap_sql)?);
            }
            out
        };

        let before = self.len()?;
        for (pid, hash) in current {
            let key = format!("{}:{}", pid, hash);
            if !keep_keys.contains(&key) {
                self.conn
                    .execute(
                        "DELETE FROM embeddings WHERE page_id = ?1 AND content_hash = ?2",
                        params![pid, hash],
                    )
                    .map_err(wrap_sql)?;
            }
        }
        let after = self.len()?;
        Ok(before.saturating_sub(after))
    }

    /// Total number of rows in the store (test / diagnostics helper).
    pub fn len(&self) -> Result<usize> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM embeddings", [], |row| row.get(0))
            .map_err(wrap_sql)?;
        Ok(usize::try_from(count).unwrap_or_default())
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Cosine top-k search over chunks stored for the given provider/model.
    ///
    /// `query` should already be unit-normalized. The store's rows are also
    /// expected to be unit vectors (the [`crate::llm::LlmProvider::embed`]
    /// contract requires it), so cosine reduces to a dot product.
    pub fn search(
        &self,
        query: &[f32],
        provider: &str,
        model: &str,
        top_k: usize,
    ) -> Result<Vec<SearchHit>> {
        if top_k == 0 {
            return Ok(Vec::new());
        }
        let mut stmt = self
            .conn
            .prepare(
                r"
                SELECT page_id, content_hash, chunk_idx, chunk_text, heading, dim, vector
                  FROM embeddings
                 WHERE provider = ?1
                   AND model = ?2
                ",
            )
            .map_err(wrap_sql)?;
        let rows = stmt
            .query_map(params![provider, model], |row| {
                let page_id_str: String = row.get(0)?;
                let _content_hash: String = row.get(1)?;
                let chunk_idx: i64 = row.get(2)?;
                let chunk_text: String = row.get(3)?;
                let heading: Option<String> = row.get(4)?;
                let dim: i64 = row.get(5)?;
                let bytes: Vec<u8> = row.get(6)?;
                Ok((
                    page_id_str,
                    chunk_idx,
                    chunk_text,
                    heading,
                    usize::try_from(dim).unwrap_or(0),
                    bytes,
                ))
            })
            .map_err(wrap_sql)?;

        // Track the top-K hits in a small sorted vec. For N up to ~50k and
        // typical K of 10, this is faster than building a full heap.
        let mut best: Vec<SearchHit> = Vec::with_capacity(top_k + 1);

        for row in rows {
            let (page_id_str, chunk_idx, chunk_text, heading, dim, bytes) =
                row.map_err(wrap_sql)?;
            if dim != query.len() {
                // Dim mismatch — either a stale row or a different model
                // got mislabelled. Skip rather than crash.
                continue;
            }
            let vector = bytes_to_vec(&bytes);
            if vector.len() != query.len() {
                continue;
            }
            let score = dot(query, &vector);
            let Ok(page_id) = PageId::parse(&page_id_str) else {
                continue;
            };
            let hit = SearchHit {
                page_id,
                chunk_idx: usize::try_from(chunk_idx).unwrap_or(0),
                heading,
                chunk_text,
                score,
            };
            insert_sorted(&mut best, hit, top_k);
        }

        Ok(best)
    }
}

fn insert_sorted(best: &mut Vec<SearchHit>, hit: SearchHit, top_k: usize) {
    // Binary search by descending score.
    let pos = best.partition_point(|h| h.score >= hit.score);
    best.insert(pos, hit);
    if best.len() > top_k {
        best.truncate(top_k);
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

fn vec_to_bytes(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

fn bytes_to_vec(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(v: Vec<f32>) -> Vec<f32> {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.into_iter().map(|x| x / norm).collect()
    }

    fn row(page_id: PageId, hash: &str, idx: usize, text: &str, vector: Vec<f32>) -> EmbeddingRow {
        EmbeddingRow {
            page_id,
            content_hash: hash.into(),
            chunk_idx: idx,
            chunk_text: text.into(),
            heading: None,
            provider: "mock".into(),
            model: "mock-1".into(),
            vector,
        }
    }

    #[test]
    fn upsert_and_count() {
        let store = EmbeddingsStore::in_memory().unwrap();
        assert!(store.is_empty().unwrap());
        let id = PageId::new();
        store
            .upsert(&row(id, "hash-1", 0, "chunk a", unit(vec![1.0, 0.0, 0.0])))
            .unwrap();
        store
            .upsert(&row(id, "hash-1", 1, "chunk b", unit(vec![0.0, 1.0, 0.0])))
            .unwrap();
        assert_eq!(store.len().unwrap(), 2);
    }

    #[test]
    fn upsert_is_idempotent_within_same_key() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id = PageId::new();
        let r = row(id, "hash-1", 0, "chunk", unit(vec![1.0, 0.0]));
        store.upsert(&r).unwrap();
        store.upsert(&r).unwrap();
        store.upsert(&r).unwrap();
        assert_eq!(store.len().unwrap(), 1);
    }

    #[test]
    fn has_page_version_reports_cache_hits() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id = PageId::new();
        assert!(!store
            .has_page_version(id, "hash-1", "mock", "mock-1")
            .unwrap());
        store
            .upsert(&row(id, "hash-1", 0, "a", unit(vec![1.0, 0.0])))
            .unwrap();
        assert!(store
            .has_page_version(id, "hash-1", "mock", "mock-1")
            .unwrap());
        // Different hash → still a miss.
        assert!(!store
            .has_page_version(id, "hash-2", "mock", "mock-1")
            .unwrap());
    }

    #[test]
    fn delete_version_removes_only_matching_rows() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id = PageId::new();
        store
            .upsert(&row(id, "hash-1", 0, "v1", unit(vec![1.0, 0.0])))
            .unwrap();
        store
            .upsert(&row(id, "hash-2", 0, "v2", unit(vec![0.0, 1.0])))
            .unwrap();
        assert_eq!(store.len().unwrap(), 2);
        store.delete_version(id, "hash-1").unwrap();
        assert_eq!(store.len().unwrap(), 1);
        assert!(!store
            .has_page_version(id, "hash-1", "mock", "mock-1")
            .unwrap());
        assert!(store
            .has_page_version(id, "hash-2", "mock", "mock-1")
            .unwrap());
    }

    #[test]
    fn search_returns_top_k_by_cosine() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id = PageId::new();
        // Three orthogonal unit vectors.
        store
            .upsert(&row(id, "hash-1", 0, "x-axis", unit(vec![1.0, 0.0, 0.0])))
            .unwrap();
        store
            .upsert(&row(id, "hash-1", 1, "y-axis", unit(vec![0.0, 1.0, 0.0])))
            .unwrap();
        store
            .upsert(&row(id, "hash-1", 2, "z-axis", unit(vec![0.0, 0.0, 1.0])))
            .unwrap();
        // Query parallel to x-axis.
        let q = unit(vec![0.9, 0.1, 0.1]);
        let hits = store.search(&q, "mock", "mock-1", 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].chunk_text, "x-axis");
        assert!(hits[0].score > hits[1].score);
    }

    #[test]
    fn search_ignores_other_providers() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id = PageId::new();
        let mut r = row(id, "hash-1", 0, "alpha", unit(vec![1.0, 0.0]));
        r.provider = "openai".into();
        store.upsert(&r).unwrap();
        let hits = store.search(&[1.0, 0.0], "mock", "mock-1", 10).unwrap();
        assert!(hits.is_empty());
    }

    #[test]
    fn search_top_k_zero_returns_empty() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let id = PageId::new();
        store
            .upsert(&row(id, "hash-1", 0, "a", unit(vec![1.0, 0.0])))
            .unwrap();
        assert!(store
            .search(&[1.0, 0.0], "mock", "mock-1", 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn vector_bytes_round_trip() {
        let v = vec![0.1f32, -0.2, 0.3, -0.4];
        let bytes = vec_to_bytes(&v);
        let back = bytes_to_vec(&bytes);
        assert_eq!(v.len(), back.len());
        for (a, b) in v.iter().zip(back.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }

    /// Retaining [A, B] when the store has A, B, C should delete every
    /// chunk row for C and leave A and B untouched. This is the
    /// "removed page" orphan case — a page that no longer exists in
    /// the vault scan but still has rows in the embeddings store.
    #[test]
    fn retain_page_versions_removes_rows_for_deleted_pages() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let a = PageId::new();
        let b = PageId::new();
        let c = PageId::new();
        // Two chunks per page, all with the same hash per page.
        for (id, hash) in &[(a, "a-hash"), (b, "b-hash"), (c, "c-hash")] {
            store
                .upsert(&row(*id, hash, 0, "c0", unit(vec![1.0, 0.0])))
                .unwrap();
            store
                .upsert(&row(*id, hash, 1, "c1", unit(vec![0.0, 1.0])))
                .unwrap();
        }
        assert_eq!(store.len().unwrap(), 6);

        // Keep only A and B — C is a "deleted page" orphan.
        let keep: Vec<(PageId, &str)> = vec![(a, "a-hash"), (b, "b-hash")];
        let pruned = store.retain_page_versions(&keep).unwrap();
        assert_eq!(pruned, 2, "both C chunks should have been pruned");
        assert_eq!(store.len().unwrap(), 4);
        assert!(store.has_page_version(a, "a-hash", "mock", "mock-1").unwrap());
        assert!(store.has_page_version(b, "b-hash", "mock", "mock-1").unwrap());
        assert!(!store.has_page_version(c, "c-hash", "mock", "mock-1").unwrap());
    }

    /// Retaining [(A, "hash-2")] when the store has rows for A at both
    /// "hash-1" and "hash-2" should delete the hash-1 rows and leave
    /// hash-2 alone. This is the "stale content version" orphan case —
    /// a page that's still in the vault but whose body has been updated,
    /// so the old content_hash rows are superseded.
    #[test]
    fn retain_page_versions_removes_stale_content_versions_for_kept_page() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let a = PageId::new();
        // Two versions of page A.
        store
            .upsert(&row(a, "hash-1", 0, "old c0", unit(vec![1.0, 0.0])))
            .unwrap();
        store
            .upsert(&row(a, "hash-1", 1, "old c1", unit(vec![0.0, 1.0])))
            .unwrap();
        store
            .upsert(&row(a, "hash-2", 0, "new c0", unit(vec![0.0, 1.0])))
            .unwrap();
        assert_eq!(store.len().unwrap(), 3);

        // Keep only the new version.
        let keep: Vec<(PageId, &str)> = vec![(a, "hash-2")];
        let pruned = store.retain_page_versions(&keep).unwrap();
        assert_eq!(pruned, 2, "both hash-1 rows should have been pruned");
        assert_eq!(store.len().unwrap(), 1);
        assert!(!store.has_page_version(a, "hash-1", "mock", "mock-1").unwrap());
        assert!(store.has_page_version(a, "hash-2", "mock", "mock-1").unwrap());
    }

    /// An empty keep set deletes every row. Sanity check for the "vault
    /// wiped" edge case.
    #[test]
    fn retain_page_versions_with_empty_keep_deletes_all() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let a = PageId::new();
        store
            .upsert(&row(a, "h", 0, "c", unit(vec![1.0, 0.0])))
            .unwrap();
        store
            .upsert(&row(a, "h", 1, "c", unit(vec![0.0, 1.0])))
            .unwrap();
        let pruned = store.retain_page_versions(&[]).unwrap();
        assert_eq!(pruned, 2);
        assert!(store.is_empty().unwrap());
    }

    /// Keeping everything that's already in the store is a no-op.
    #[test]
    fn retain_page_versions_is_noop_when_keep_matches_store() {
        let store = EmbeddingsStore::in_memory().unwrap();
        let a = PageId::new();
        let b = PageId::new();
        store
            .upsert(&row(a, "ha", 0, "a0", unit(vec![1.0, 0.0])))
            .unwrap();
        store
            .upsert(&row(b, "hb", 0, "b0", unit(vec![0.0, 1.0])))
            .unwrap();
        let keep: Vec<(PageId, &str)> = vec![(a, "ha"), (b, "hb")];
        let pruned = store.retain_page_versions(&keep).unwrap();
        assert_eq!(pruned, 0);
        assert_eq!(store.len().unwrap(), 2);
    }
}
