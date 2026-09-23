//! Git-backed records: `<sub-ontology>/<data_dir>/<Type>/<key>.yaml`.
//!
//! Reads go through a [`Snapshot`], so a store opened on a revision serves
//! exactly the records committed there. The key is the file name; a record
//! may repeat it in its key field, and [`GitRecordStore`] adds it when it
//! does not, so every returned record carries its own key.
//!
//! On a commit snapshot (immutable) the store caches parsed records, the
//! key list of each type, and — for inverse relations — a reverse index per
//! `(type, field)` built on first use, so a `references` lookup costs
//! O(matches) instead of a scan. A worktree snapshot caches nothing and
//! scans; it exists for the edit loop, and `scriptorium-ontology serve` refuses it.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;

use super::{EntityStore, ListQuery, Record};
use crate::error::{Error, Result};
use crate::layout::{join, RECORD_EXT};
use crate::snapshot::Snapshot;

pub struct GitRecordStore {
    snapshot: Arc<dyn Snapshot>,
    /// Repository-relative data directory, e.g. `ontology/core/data`.
    data_root: String,
    /// Entity type → key field name.
    key_fields: BTreeMap<String, String>,
    /// Parsed records by path. Only used for commit snapshots, which are
    /// immutable; a worktree snapshot is re-read on every request.
    cache: Option<Mutex<HashMap<String, Option<Arc<Record>>>>>,
    /// Record keys by type, cached on the same terms, so repeated list and
    /// inverse-relation scans do not re-walk the tree.
    key_cache: Option<Mutex<HashMap<String, Arc<Vec<String>>>>>,
    /// `(type, field)` → referenced key → keys of the records of `type`
    /// whose `field` references it. Commit snapshots only.
    reference_index: Option<Mutex<ReferenceIndexes>>,
}

/// Referenced key → keys of the records that reference it.
type ReferenceIndex = HashMap<String, Vec<String>>;
/// One [`ReferenceIndex`] per `(type, field)`, each built exactly once. A
/// failed build is cached too: the snapshot is immutable, so it would fail
/// the same way again.
type ReferenceIndexes =
    HashMap<(String, String), Arc<OnceLock<std::result::Result<Arc<ReferenceIndex>, String>>>>;

impl GitRecordStore {
    pub fn new(
        snapshot: Arc<dyn Snapshot>,
        data_root: String,
        key_fields: BTreeMap<String, String>,
    ) -> Self {
        let immutable = snapshot.commit().is_some();
        Self {
            snapshot,
            data_root,
            key_fields,
            cache: immutable.then(|| Mutex::new(HashMap::new())),
            key_cache: immutable.then(|| Mutex::new(HashMap::new())),
            reference_index: immutable.then(|| Mutex::new(HashMap::new())),
        }
    }

    fn record_path(&self, type_name: &str, key: &str) -> String {
        join(&self.data_root, &format!("{type_name}/{key}.{RECORD_EXT}"))
    }

    fn load(&self, type_name: &str, key: &str) -> Result<Option<Arc<Record>>> {
        if crate::layout::validate_record_key(key).is_err() {
            // Not a valid file name, so it cannot be stored here.
            return Ok(None);
        }
        let path = self.record_path(type_name, key);
        let cached = self.cache.as_ref().and_then(|cache| {
            cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&path)
                .cloned()
        });
        if let Some(hit) = cached {
            return Ok(hit);
        }
        let record = match self.snapshot.read(&path)? {
            None => None,
            Some(bytes) => {
                let mut record = parse_record(&bytes).map_err(|message| Error::Invalid {
                    what: "record",
                    path: path.clone(),
                    message,
                })?;
                if let Some(key_field) = self.key_fields.get(type_name) {
                    record
                        .entry(key_field.clone())
                        .or_insert_with(|| serde_json::Value::String(key.to_string()));
                }
                Some(Arc::new(record))
            }
        };
        if let Some(cache) = &self.cache {
            cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(path, record.clone());
        }
        Ok(record)
    }

    /// The reverse index for `type_name.field`, built on first use. `None`
    /// for worktree snapshots, which are not cached.
    fn references_of(&self, type_name: &str, field: &str) -> Result<Option<Arc<ReferenceIndex>>> {
        let Some(indexes) = &self.reference_index else {
            return Ok(None);
        };
        // Single flight: concurrent first lookups share one build instead of
        // each scanning every record. `serve` builds every index before it
        // accepts requests (see `prepare_references`), so this only waits
        // on a cold path outside the server.
        let slot = Arc::clone(
            indexes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry((type_name.to_string(), field.to_string()))
                .or_default(),
        );
        let built = slot.get_or_init(|| {
            self.build_reference_index(type_name, field)
                .map(Arc::new)
                .map_err(|e| e.to_string())
        });
        match built {
            Ok(index) => Ok(Some(Arc::clone(index))),
            Err(message) => Err(Error::Store {
                store: self.describe(),
                message: format!("indexing {type_name}.{field}: {message}"),
            }),
        }
    }

    fn build_reference_index(&self, type_name: &str, field: &str) -> Result<ReferenceIndex> {
        let mut index = ReferenceIndex::new();
        for key in self.keys(type_name)?.iter() {
            let Some(record) = self.load(type_name, key)? else {
                continue;
            };
            let mut add = |value: &serde_json::Value| {
                let target = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    _ => return,
                };
                let sources = index.entry(target).or_default();
                if sources.last() != Some(key) {
                    sources.push(key.clone());
                }
            };
            match record.get(field) {
                Some(serde_json::Value::Array(items)) => items.iter().for_each(&mut add),
                Some(value) => add(value),
                None => {}
            }
        }
        Ok(index)
    }

    /// Keys of every record of `type_name`, sorted.
    pub fn keys(&self, type_name: &str) -> Result<Arc<Vec<String>>> {
        let lock = |cache: &Mutex<HashMap<String, Arc<Vec<String>>>>| {
            cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(type_name)
                .cloned()
        };
        if let Some(hit) = self.key_cache.as_ref().and_then(lock) {
            return Ok(hit);
        }
        let dir = join(&self.data_root, type_name);
        let suffix = format!(".{RECORD_EXT}");
        let keys: Arc<Vec<String>> = Arc::new(
            self.snapshot
                .list(&dir)?
                .into_iter()
                .filter(|e| !e.is_dir)
                .filter_map(|e| e.name.strip_suffix(&suffix).map(ToString::to_string))
                .collect(),
        );
        if let Some(cache) = &self.key_cache {
            cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(type_name.to_string(), Arc::clone(&keys));
        }
        Ok(keys)
    }
}

#[async_trait]
impl EntityStore for GitRecordStore {
    fn describe(&self) -> String {
        format!("git:{}@{}", self.data_root, self.snapshot.label())
    }

    fn prepare_references(&self, type_name: &str, field: &str) -> Result<()> {
        self.references_of(type_name, field).map(|_| ())
    }

    async fn get(&self, type_name: &str, key: &str) -> Result<Option<Arc<Record>>> {
        self.load(type_name, key)
    }

    async fn list(&self, type_name: &str, query: &ListQuery) -> Result<Vec<Arc<Record>>> {
        if let Some((field, key)) = &query.references {
            if let Some(index) = self.references_of(type_name, field)? {
                let mut out = Vec::new();
                for source in index.get(key).map(Vec::as_slice).unwrap_or_default() {
                    if query.limit.is_some_and(|limit| out.len() >= limit) {
                        break;
                    }
                    if let Some(record) = self.load(type_name, source)? {
                        out.push(record);
                    }
                }
                return Ok(out);
            }
        }
        let mut out = Vec::new();
        for key in self.keys(type_name)?.iter() {
            if query.limit.is_some_and(|limit| out.len() >= limit) {
                break;
            }
            if let Some(record) = self.load(type_name, key)? {
                if query.matches(&record) {
                    out.push(record);
                }
            }
        }
        Ok(out)
    }
}

/// Parse a record file: a YAML mapping (an empty file is an empty record).
pub fn parse_record(bytes: &[u8]) -> std::result::Result<Record, String> {
    let text = std::str::from_utf8(bytes).map_err(|e| format!("not UTF-8: {e}"))?;
    if text.trim().is_empty() {
        return Ok(Record::new());
    }
    // Deserializing straight into JSON keeps the last of two equal keys, so
    // `authors: [a]` then `authors: [b]` would silently lose `a`. serde_yml's
    // own value type rejects duplicates at every level: parse with it first.
    serde_yml::from_str::<serde_yml::Value>(text).map_err(|e| format!("invalid YAML: {e}"))?;
    match serde_yml::from_str::<serde_json::Value>(text) {
        Ok(serde_json::Value::Object(map)) => Ok(map),
        Ok(serde_json::Value::Null) => Ok(Record::new()),
        Ok(other) => Err(format!(
            "expected a mapping of field names to values, got {}",
            json_kind(&other)
        )),
        Err(e) => Err(format!("invalid YAML: {e}")),
    }
}

pub(crate) fn json_kind(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "a boolean",
        serde_json::Value::Number(_) => "a number",
        serde_json::Value::String(_) => "a string",
        serde_json::Value::Array(_) => "a list",
        serde_json::Value::Object(_) => "a mapping",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{test_support::commit_files, GitSnapshot};
    use serde_json::json;

    fn store(root: &std::path::Path, rev: &str) -> GitRecordStore {
        GitRecordStore::new(
            Arc::new(GitSnapshot::open(root, rev).unwrap()),
            "ontology/research/data".into(),
            BTreeMap::from([("Paper".to_string(), "id".to_string())]),
        )
    }

    #[tokio::test]
    async fn records_are_shared_not_copied() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[("ontology/research/data/Paper/moe.yaml", "title: MoE\n")],
            "init",
        );
        let store = store(dir.path(), "HEAD");
        let a = store.get("Paper", "moe").await.unwrap().unwrap();
        let b = store.get("Paper", "moe").await.unwrap().unwrap();
        assert!(
            Arc::ptr_eq(&a, &b),
            "one copy, however often it is asked for"
        );
        let listed = store.list("Paper", &ListQuery::default()).await.unwrap();
        assert!(Arc::ptr_eq(&a, &listed[0]));
    }

    #[tokio::test]
    async fn reads_records_at_a_revision_with_injected_key() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[(
                "ontology/research/data/Paper/attention.yaml",
                "title: Attention\nyear: 2017\n",
            )],
            "v1",
        );
        commit_files(
            dir.path(),
            &[
                (
                    "ontology/research/data/Paper/attention.yaml",
                    "title: Attention Is All You Need\nyear: 2017\nauthors: [vaswani]\n",
                ),
                (
                    "ontology/research/data/Paper/bert.yaml",
                    "title: BERT\nauthors: [devlin]\n",
                ),
            ],
            "v2",
        );
        let head = store(dir.path(), "HEAD");
        let rec = head.get("Paper", "attention").await.unwrap().unwrap();
        assert_eq!(rec["id"], json!("attention"));
        assert_eq!(rec["title"], json!("Attention Is All You Need"));
        assert!(head.get("Paper", "missing").await.unwrap().is_none());
        assert!(head.get("Paper", "../escape").await.unwrap().is_none());

        let all = head.list("Paper", &ListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 2);
        let by_vaswani = head
            .list(
                "Paper",
                &ListQuery {
                    references: Some(("authors".into(), "vaswani".into())),
                    limit: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(by_vaswani.len(), 1);
        let limited = head
            .list(
                "Paper",
                &ListQuery {
                    references: None,
                    limit: Some(1),
                },
            )
            .await
            .unwrap();
        assert_eq!(limited.len(), 1);

        let prev = store(dir.path(), "HEAD~1");
        let old = prev.get("Paper", "attention").await.unwrap().unwrap();
        assert_eq!(old["title"], json!("Attention"), "time travel");
        assert!(prev.get("Paper", "bert").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn reverse_index_matches_scanning() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("ontology/research/data/Paper/a.yaml", "authors: [x, y]\n"),
                ("ontology/research/data/Paper/b.yaml", "authors: [y]\n"),
                ("ontology/research/data/Paper/c.yaml", "authors: 42\n"),
                ("ontology/research/data/Paper/d.yaml", "title: none\n"),
            ],
            "init",
        );
        let indexed = store(dir.path(), "HEAD");
        let scanning = GitRecordStore::new(
            Arc::new(crate::snapshot::WorktreeSnapshot::new(dir.path())),
            "ontology/research/data".into(),
            BTreeMap::from([("Paper".to_string(), "id".to_string())]),
        );
        for key in ["x", "y", "42", "nobody"] {
            let query = ListQuery {
                references: Some(("authors".into(), key.into())),
                limit: None,
            };
            let ids = |records: Vec<Arc<Record>>| -> Vec<String> {
                records
                    .iter()
                    .map(|r| r["id"].as_str().unwrap().to_string())
                    .collect()
            };
            assert_eq!(
                ids(indexed.list("Paper", &query).await.unwrap()),
                ids(scanning.list("Paper", &query).await.unwrap()),
                "key {key}"
            );
        }
        assert!(indexed.references_of("Paper", "authors").unwrap().is_some());
        assert!(scanning
            .references_of("Paper", "authors")
            .unwrap()
            .is_none());
    }

    #[test]
    fn concurrent_cold_lookups_build_the_index_once() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[("ontology/research/data/Paper/a.yaml", "authors: [x]\n")],
            "init",
        );
        let store = Arc::new(store(dir.path(), "HEAD"));
        let indexes: Vec<Arc<ReferenceIndex>> = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    let store = Arc::clone(&store);
                    scope.spawn(move || store.references_of("Paper", "authors").unwrap().unwrap())
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert!(
            indexes.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])),
            "every caller shares one build"
        );
    }

    #[test]
    fn parse_record_shapes() {
        assert!(parse_record(b"").unwrap().is_empty());
        assert!(parse_record(b"~").unwrap().is_empty());
        assert_eq!(parse_record(b"a: 1").unwrap()["a"], json!(1));
        assert!(parse_record(b"- a\n- b").unwrap_err().contains("a list"));
        assert!(parse_record(b"a: [").unwrap_err().contains("invalid YAML"));
    }

    #[test]
    fn duplicate_keys_are_rejected_at_every_level() {
        let err = parse_record(b"authors: [alice]\nauthors: [bob]\n").unwrap_err();
        assert!(
            err.contains("duplicate entry with key \"authors\""),
            "{err}"
        );
        let err = parse_record(b"pages: { first: 1, first: 2 }\n").unwrap_err();
        assert!(err.contains("duplicate entry with key \"first\""), "{err}");
    }
}
