//! Storage adapters for sub-ontology instances.
//!
//! A sub-ontology's schema always lives in git; its *instances* live
//! wherever its `[storage]` says. Adapters implement [`EntityStore`], a
//! deliberately small read interface keyed by entity type and key. A record
//! holds only the fields its own sub-ontology owns — joining an entity's
//! fields across sub-ontologies is the federation gateway's job.
//!
//! | `kind` | Instances | Build feature |
//! |---|---|---|
//! | `git` | YAML records in the git snapshot | — |
//! | `markdown` | `.md` notes with YAML frontmatter in the git snapshot | — |
//! | `postgres` | one table per entity type | `postgres` |
//! | `dynamodb` | one table, `pk = "<Type>#<key>"` | `dynamodb` |

#[cfg(feature = "dynamodb")]
pub mod dynamodb;
pub mod git;
pub mod markdown;
#[cfg(feature = "postgres")]
pub mod postgres;

use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;
use crate::manifest::StorageConfig;
use crate::model::SubOntologyModel;
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
pub(crate) fn key_matches(value: &serde_json::Value, key: &str) -> bool {
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

    /// Check the backing store is reachable (connect, describe the table,
    /// …). `serve` calls this before it publishes a supergraph or accepts
    /// requests; `compose` does not, so it needs no database access.
    async fn ready(&self) -> Result<()> {
        Ok(())
    }
}

/// Open the store a local sub-ontology's manifest asks for, for the
/// entity types `model` declares (adapters return records that include
/// their own key).
///
/// Opening never touches the network: database stores connect lazily (see
/// [`EntityStore::ready`]).
pub fn open_store(
    sub: &SubOntology,
    storage: &StorageConfig,
    snapshot: Arc<dyn Snapshot>,
    model: &SubOntologyModel,
) -> Result<Arc<dyn EntityStore>> {
    Ok(match storage {
        StorageConfig::Git { data_dir } => Arc::new(git::GitRecordStore::new(
            snapshot,
            crate::layout::join(&sub.dir, data_dir),
            key_fields(model),
        )),
        StorageConfig::Markdown(config) => Arc::new(markdown::MarkdownStore::new(
            snapshot,
            sub,
            config,
            key_fields(model),
        )?),
        StorageConfig::Postgres(_) | StorageConfig::Dynamodb(_) => {
            open_database_store(sub, storage, model)?
        }
    })
}

/// Check a database-backed sub-ontology's storage settings the way
/// [`open_store`] does, without connecting, so `check` reports what
/// `serve` would refuse. Other storage kinds pass.
pub fn validate_database_storage(
    sub: &SubOntology,
    storage: &StorageConfig,
    model: &SubOntologyModel,
) -> Result<()> {
    match storage {
        StorageConfig::Postgres(_) | StorageConfig::Dynamodb(_) => {
            open_database_store(sub, storage, model).map(drop)
        }
        StorageConfig::Git { .. } | StorageConfig::Markdown(_) => Ok(()),
    }
}

/// Each entity type → its key field.
fn key_fields(model: &SubOntologyModel) -> std::collections::BTreeMap<String, String> {
    model
        .entities()
        .filter_map(|o| o.key_field().map(|k| (o.name.clone(), k.to_string())))
        .collect()
}

fn open_database_store(
    sub: &SubOntology,
    storage: &StorageConfig,
    model: &SubOntologyModel,
) -> Result<Arc<dyn EntityStore>> {
    match storage {
        #[cfg(feature = "postgres")]
        StorageConfig::Postgres(config) => {
            let stored = model.stored_entities().map(|o| o.name.clone()).collect();
            let fields = model
                .stored_entities()
                .map(|o| {
                    let columns = o
                        .fields
                        .iter()
                        .filter(|f| f.is_stored() && f.args.is_empty())
                        .map(|f| postgres::Column {
                            name: f.name.clone(),
                            text: (f.ty.named == "ID"
                                || matches!(
                                    model.kind_of(&f.ty.named),
                                    crate::model::TypeKind::Entity(_)
                                ))
                            .then(|| f.ty.is_list()),
                        })
                        .collect();
                    (o.name.clone(), columns)
                })
                .collect();
            Ok(Arc::new(postgres::PostgresStore::for_types(
                &sub.name,
                config,
                key_fields(model),
                &stored,
                &fields,
            )?))
        }
        #[cfg(not(feature = "postgres"))]
        StorageConfig::Postgres(_) => Err(feature_missing(&sub.name, "postgres")),
        StorageConfig::Dynamodb(config) => {
            reserved_dynamodb_fields(sub, model)?;
            #[cfg(feature = "dynamodb")]
            return Ok(Arc::new(dynamodb::DynamoStore::new(
                &sub.name,
                config,
                key_fields(model),
            )?));
            #[cfg(not(feature = "dynamodb"))]
            {
                let _ = config;
                Err(feature_missing(&sub.name, "dynamodb"))
            }
        }
        StorageConfig::Git { .. } | StorageConfig::Markdown(_) => Err(crate::error::Error::Store {
            store: format!("{}:{}", storage.kind(), sub.name),
            message: "not a database storage".into(),
        }),
    }
}

/// `pk` and `type` hold each `DynamoDB` item's key and entity type, so no
/// stored field (or key) of a `DynamoDB`-backed sub-ontology may use them.
fn reserved_dynamodb_fields(sub: &SubOntology, model: &SubOntologyModel) -> Result<()> {
    for entity in model.entities() {
        let reserved = entity.fields.iter().find(|f| {
            (f.is_stored() || Some(f.name.as_str()) == entity.key_field())
                && matches!(f.name.as_str(), "pk" | "type")
        });
        if let Some(field) = reserved {
            return Err(crate::error::Error::Store {
                store: format!("dynamodb:{}", sub.name),
                message: format!(
                    "`{}.{}`: `pk` and `type` are reserved attribute names in DynamoDB storage; rename the field",
                    entity.name, field.name
                ),
            });
        }
    }
    Ok(())
}

#[cfg(any(not(feature = "postgres"), not(feature = "dynamodb")))]
fn feature_missing(sub: &str, feature: &str) -> crate::error::Error {
    crate::error::Error::Store {
        store: format!("{feature}:{sub}"),
        message: format!(
            "this build has no `{feature}` storage; rebuild with `--features {feature}`"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn dynamodb_reserves_pk_and_type() {
        let sub = |sdl: &str| SubOntology {
            name: "things".into(),
            dir: "ontology/things".into(),
            description: None,
            owners: Vec::new(),
            sdl: sdl.into(),
            source: crate::registry::SubOntologySource::Local(StorageConfig::Dynamodb(
                crate::manifest::DynamoConfig {
                    table: "things".into(),
                    region: None,
                    endpoint_url_env: None,
                },
            )),
        };
        let storage = StorageConfig::Dynamodb(crate::manifest::DynamoConfig {
            table: "things".into(),
            region: None,
            endpoint_url_env: None,
        });
        for field in ["type: String!", "pk: String"] {
            let sdl = format!("type Thing @key(fields: \"id\") {{ id: ID! {field} }}");
            let model = SubOntologyModel::parse(&sdl).unwrap();
            let err = validate_database_storage(&sub(&sdl), &storage, &model)
                .unwrap_err()
                .to_string();
            assert!(err.contains("reserved attribute names"), "{field}: {err}");
        }
        // A derived field of that name stores nothing, so it is fine.
        let sdl = "directive @inverse(of: String!) on FIELD_DEFINITION\ntype Thing @key(fields: \"id\") { id: ID! parent: Thing type: [Thing!]! @inverse(of: \"parent\") }";
        let model = SubOntologyModel::parse(sdl).unwrap();
        let result = validate_database_storage(&sub(sdl), &storage, &model);
        assert!(
            result
                .as_ref()
                .map_or_else(|e| !e.to_string().contains("reserved"), |()| true),
            "{result:?}"
        );
    }

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
