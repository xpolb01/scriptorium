//! Storage adapters for sub-ontology instances.
//!
//! A sub-ontology's schema always lives in git; its *instances* live
//! wherever its `[storage]` says. Adapters implement [`EntityStore`], a
//! deliberately small read interface keyed by entity type and key. A record
//! holds only the fields its own sub-ontology owns — joining an entity's
//! fields across sub-ontologies is the federation gateway's job.

pub mod git;

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::manifest::StorageConfig;
use crate::registry::SubOntology;
use crate::snapshot::Snapshot;

/// One entity's owned fields, as JSON. Relation fields hold target keys.
pub type Record = serde_json::Map<String, serde_json::Value>;

/// Filter for [`EntityStore::list`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListQuery {
    /// Keep records whose `.0` field equals `.1`, or is a list containing it
    /// (how inverse relations find their sources).
    pub references: Option<(String, String)>,
    pub limit: Option<usize>,
}

impl ListQuery {
    /// Whether `record` passes the `references` filter.
    pub fn matches(&self, record: &Record) -> bool {
        let Some((field, key)) = &self.references else {
            return true;
        };
        match record.get(field) {
            Some(serde_json::Value::Array(items)) => items.iter().any(|v| key_matches(v, key)),
            Some(value) => key_matches(value, key),
            None => false,
        }
    }
}

/// Keys are compared as strings: a relation may store `123` or `"123"`.
fn key_matches(value: &serde_json::Value, key: &str) -> bool {
    match value {
        serde_json::Value::String(s) => s == key,
        serde_json::Value::Number(n) => n.to_string() == key,
        _ => false,
    }
}

/// A store with no records. `check` builds a subgraph against it to prove
/// a schema is servable without touching real storage.
pub struct NullStore;

#[async_trait]
impl EntityStore for NullStore {
    fn describe(&self) -> String {
        "null".into()
    }

    async fn get(&self, _type_name: &str, _key: &str) -> Result<Option<Arc<Record>>> {
        Ok(None)
    }

    async fn list(&self, _type_name: &str, _query: &ListQuery) -> Result<Vec<Arc<Record>>> {
        Ok(Vec::new())
    }
}

/// Read access to one sub-ontology's instances.
#[async_trait]
pub trait EntityStore: Send + Sync {
    /// Short description for logs and errors, e.g. `git:ontology/core/data@HEAD@1a2b3c4`.
    fn describe(&self) -> String;

    /// The record for `key`, or `None` when this store has no such entity.
    /// Records are shared, not copied: a batch naming one entity many times
    /// holds it once.
    async fn get(&self, type_name: &str, key: &str) -> Result<Option<Arc<Record>>>;

    /// Records of `type_name`, sorted by key, filtered by `query`.
    async fn list(&self, type_name: &str, query: &ListQuery) -> Result<Vec<Arc<Record>>>;

    /// Get ready to answer `references` queries on `type_name.field`, e.g. by
    /// building an index. `serve` calls this for every inverse relation
    /// before accepting requests, so no request pays for it.
    fn prepare_references(&self, _type_name: &str, _field: &str) -> Result<()> {
        Ok(())
    }
}

/// Open the store a local sub-ontology's manifest asks for.
///
/// `key_fields` maps each entity type to its key field so adapters can
/// return records that include their own key.
pub fn open_store(
    sub: &SubOntology,
    storage: &StorageConfig,
    snapshot: Arc<dyn Snapshot>,
    key_fields: std::collections::BTreeMap<String, String>,
) -> Arc<dyn EntityStore> {
    match storage {
        StorageConfig::Git { data_dir } => Arc::new(git::GitRecordStore::new(
            snapshot,
            crate::layout::join(&sub.dir, data_dir),
            key_fields,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn references_filter_matches_scalar_and_list_fields() {
        let rec = |v: serde_json::Value| v.as_object().unwrap().clone();
        let q = ListQuery {
            references: Some(("authors".into(), "vaswani".into())),
            limit: None,
        };
        assert!(q.matches(&rec(json!({"authors": ["shazeer", "vaswani"]}))));
        assert!(q.matches(&rec(json!({"authors": "vaswani"}))));
        assert!(!q.matches(&rec(json!({"authors": ["shazeer"]}))));
        assert!(!q.matches(&rec(json!({"title": "x"}))));
        assert!(ListQuery::default().matches(&rec(json!({}))));
        let numeric = ListQuery {
            references: Some(("authors".into(), "123".into())),
            limit: None,
        };
        assert!(
            numeric.matches(&rec(json!({"authors": [123]}))),
            "integer keys"
        );
        assert!(numeric.matches(&rec(json!({"authors": 123}))));
    }
}
