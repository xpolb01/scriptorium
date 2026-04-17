//! Pluggable vector index abstraction for nearest-neighbor search.
//!
//! Two implementations:
//!
//! - [`LinearIndex`] — brute-force cosine scan over all vectors. Always
//!   available, no external deps. O(n) per query but fast enough for vaults
//!   up to ~50K chunks.
//!
//! - [`HnswIndex`] — HNSW approximate nearest-neighbor index via the
//!   `usearch` crate (C++ FFI). O(log n) per query, handles millions of
//!   vectors. Requires the `hnsw` cargo feature and a C++ toolchain at
//!   build time.
//!
//! The [`EmbeddingsStore`](super::EmbeddingsStore) holds a `Box<dyn VectorIndex>`
//! and delegates its `search()` method to it. The index is kept in sync
//! during `upsert()` and `retain_page_versions()`.

use std::path::Path;

use crate::error::{Error, Result};

/// A nearest-neighbor search index over f32 vectors.
///
/// All methods take `&self` (interior mutability where needed) so the index
/// can be shared behind `Arc`. The `id` parameter is the `SQLite` rowid of
/// the corresponding `embeddings` row.
pub trait VectorIndex: Send + Sync {
    /// Insert a vector with the given id. If the id already exists, the
    /// behavior is implementation-defined (`LinearIndex` overwrites, HNSW
    /// may create a duplicate that shadows the old entry).
    fn insert(&self, id: u64, vector: &[f32]) -> Result<()>;

    /// Find the `k` nearest neighbors to `query`, returning `(id, distance)`
    /// pairs sorted by ascending distance (lower = more similar for cosine).
    /// For cosine, distance = 1 - similarity.
    fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>>;

    /// Remove a vector by id. No-op if the id doesn't exist.
    fn remove(&self, id: u64) -> Result<()>;

    /// Number of vectors currently in the index.
    fn len(&self) -> usize;

    /// Whether the index is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Persist the index to disk. No-op for implementations that don't
    /// maintain a separate on-disk representation (e.g. `LinearIndex`
    /// lives entirely in the `SQLite` DB).
    fn save(&self, _path: &Path) -> Result<()> {
        Ok(())
    }
}

// ── LinearIndex ──────────────────────────────────────────────────────────

use std::sync::RwLock;

/// Brute-force cosine nearest-neighbor search. Stores all vectors in memory
/// and scans linearly on each query. Good enough for ~50K vectors.
pub struct LinearIndex {
    entries: RwLock<Vec<(u64, Vec<f32>)>>,
}

impl LinearIndex {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
        }
    }
}

impl Default for LinearIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl VectorIndex for LinearIndex {
    fn insert(&self, id: u64, vector: &[f32]) -> Result<()> {
        let mut entries = self
            .entries
            .write()
            .map_err(|e| Error::Other(anyhow::anyhow!("LinearIndex lock poisoned: {e}")))?;
        // Overwrite if exists.
        if let Some(existing) = entries.iter_mut().find(|(eid, _)| *eid == id) {
            existing.1 = vector.to_vec();
        } else {
            entries.push((id, vector.to_vec()));
        }
        Ok(())
    }

    fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>> {
        if k == 0 {
            return Ok(Vec::new());
        }
        let entries = self
            .entries
            .read()
            .map_err(|e| Error::Other(anyhow::anyhow!("LinearIndex lock poisoned: {e}")))?;
        let mut scored: Vec<(u64, f32)> = entries
            .iter()
            .filter(|(_, v)| v.len() == query.len())
            .map(|(id, v)| {
                let sim = dot(query, v);
                // Convert similarity to distance (1 - sim) for consistency
                // with ANN libraries that return distances.
                (*id, 1.0 - sim)
            })
            .collect();
        // Sort by distance ascending (most similar first).
        scored.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        Ok(scored)
    }

    fn remove(&self, id: u64) -> Result<()> {
        let mut entries = self
            .entries
            .write()
            .map_err(|e| Error::Other(anyhow::anyhow!("LinearIndex lock poisoned: {e}")))?;
        entries.retain(|(eid, _)| *eid != id);
        Ok(())
    }

    fn len(&self) -> usize {
        self.entries.read().map(|e| e.len()).unwrap_or(0)
    }
}

fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

// ── HnswIndex (behind feature flag) ─────────────────────────────────────

#[cfg(feature = "hnsw")]
#[allow(unsafe_code)]
mod hnsw_impl {
    use super::*;
    use std::path::Path;

    /// HNSW approximate nearest-neighbor index backed by `usearch`.
    /// O(log n) search, memory-mapped persistence, incremental inserts.
    pub struct HnswIndex {
        inner: usearch::Index,
        count: std::sync::atomic::AtomicUsize,
    }

    impl HnswIndex {
        /// Create a new empty HNSW index for the given dimension.
        pub fn new(dimensions: usize) -> Result<Self> {
            let opts = usearch::IndexOptions {
                dimensions,
                metric: usearch::MetricKind::Cos,
                quantization: usearch::ScalarKind::F32,
                connectivity: 16,
                expansion_add: 128,
                expansion_search: 64,
                ..Default::default()
            };
            let index = usearch::new_index(&opts)
                .map_err(|e| Error::Other(anyhow::anyhow!("usearch init: {e}")))?;
            Ok(Self {
                inner: index,
                count: std::sync::atomic::AtomicUsize::new(0),
            })
        }

        /// Load an existing HNSW index from disk.
        pub fn load(path: &Path, dimensions: usize) -> Result<Self> {
            let idx = Self::new(dimensions)?;
            idx.inner
                .load(path.to_str().unwrap_or(""))
                .map_err(|e| Error::Other(anyhow::anyhow!("usearch load: {e}")))?;
            idx.count
                .store(idx.inner.size(), std::sync::atomic::Ordering::Relaxed);
            Ok(idx)
        }
    }

    impl VectorIndex for HnswIndex {
        fn insert(&self, id: u64, vector: &[f32]) -> Result<()> {
            self.inner
                .add(id, vector)
                .map_err(|e| Error::Other(anyhow::anyhow!("usearch insert: {e}")))?;
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn search(&self, query: &[f32], k: usize) -> Result<Vec<(u64, f32)>> {
            if k == 0 || self.is_empty() {
                return Ok(Vec::new());
            }
            let results = self
                .inner
                .search(query, k)
                .map_err(|e| Error::Other(anyhow::anyhow!("usearch search: {e}")))?;
            Ok(results.keys.into_iter().zip(results.distances).collect())
        }

        fn remove(&self, id: u64) -> Result<()> {
            let _ = self.inner.remove(id);
            self.count
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }

        fn len(&self) -> usize {
            self.count.load(std::sync::atomic::Ordering::Relaxed)
        }

        fn save(&self, path: &Path) -> Result<()> {
            self.inner
                .save(path.to_str().unwrap_or(""))
                .map_err(|e| Error::Other(anyhow::anyhow!("usearch save: {e}")))?;
            Ok(())
        }
    }
}

#[cfg(feature = "hnsw")]
pub use hnsw_impl::HnswIndex;

#[cfg(test)]
mod tests {
    use super::*;

    fn unit(v: Vec<f32>) -> Vec<f32> {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        v.into_iter().map(|x| x / norm).collect()
    }

    #[test]
    fn linear_index_insert_and_search() {
        let idx = LinearIndex::new();
        // Insert 3 unit vectors along the axes.
        idx.insert(1, &unit(vec![1.0, 0.0, 0.0])).unwrap();
        idx.insert(2, &unit(vec![0.0, 1.0, 0.0])).unwrap();
        idx.insert(3, &unit(vec![0.0, 0.0, 1.0])).unwrap();

        // Query near x-axis → id=1 should be closest.
        let results = idx.search(&unit(vec![0.9, 0.1, 0.0]), 2).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 1, "x-axis vector should be nearest");
        assert!(
            results[0].1 < results[1].1,
            "first result should have lower distance"
        );
    }

    #[test]
    fn linear_index_remove() {
        let idx = LinearIndex::new();
        idx.insert(1, &[1.0, 0.0]).unwrap();
        idx.insert(2, &[0.0, 1.0]).unwrap();
        assert_eq!(idx.len(), 2);
        idx.remove(1).unwrap();
        assert_eq!(idx.len(), 1);
        let results = idx.search(&[1.0, 0.0], 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, 2);
    }

    #[test]
    fn linear_index_overwrite() {
        let idx = LinearIndex::new();
        idx.insert(1, &[1.0, 0.0]).unwrap();
        idx.insert(1, &[0.0, 1.0]).unwrap();
        assert_eq!(idx.len(), 1, "overwrite should not increase count");
        // Search near y-axis — the overwritten vector should match.
        let results = idx.search(&[0.0, 1.0], 1).unwrap();
        assert_eq!(results[0].0, 1);
        assert!(
            results[0].1 < 0.01,
            "should be very close to y-axis after overwrite"
        );
    }

    #[test]
    fn linear_index_empty_search() {
        let idx = LinearIndex::new();
        let results = idx.search(&[1.0, 0.0], 5).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn linear_index_k_zero() {
        let idx = LinearIndex::new();
        idx.insert(1, &[1.0, 0.0]).unwrap();
        let results = idx.search(&[1.0, 0.0], 0).unwrap();
        assert!(results.is_empty());
    }
}
