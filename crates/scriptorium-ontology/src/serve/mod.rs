//! Serve local sub-ontologies as Federation v2 subgraphs over HTTP.
//!
//! One process serves every local sub-ontology, each at
//! `POST /<name>/graphql`. The URLs match what [`crate::compose`] writes
//! into the supergraph for the same base URL, so a gateway pointed at that
//! supergraph reaches these subgraphs directly. Remote sub-ontologies are
//! not served here; the gateway calls them at their own URLs.
//!
//! There is no authentication: bind to loopback (the default) or put the
//! server behind something that authenticates.

pub mod schema;

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};

use crate::error::{Error, Result};
use crate::model::SubOntologyModel;
use crate::registry::{Registry, SubOntologySource};
use crate::snapshot::Snapshot;
use crate::store::open_store;

/// A runnable subgraph for one local sub-ontology.
pub struct Subgraph {
    pub name: String,
    /// Where its instances come from; `store.describe()` reads e.g.
    /// `git:ontology/core/data@HEAD@1a2b3c4`.
    pub store: Arc<dyn crate::store::EntityStore>,
    pub schema: schema::SubgraphSchema,
    /// The commit it serves, sent as [`REVISION_HEADER`] with every answer.
    pub revision: String,
}

/// Response header naming the commit a subgraph serves, so a publisher can
/// tell a server pinned to another commit (whose schema may be the same)
/// from one serving what it is about to publish.
pub const REVISION_HEADER: &str = "scriptorium-ontology-revision";

/// Build a subgraph for every local sub-ontology in `registry`, reading
/// git-backed records from `snapshot`.
pub fn build_subgraphs(registry: &Registry, snapshot: &Arc<dyn Snapshot>) -> Result<Vec<Subgraph>> {
    let mut out = Vec::new();
    for sub in registry.local() {
        let SubOntologySource::Local(storage) = &sub.source else {
            continue;
        };
        let model = SubOntologyModel::parse(&sub.sdl)
            .map_err(|e| Error::serve(&sub.name, format!("{}: {e}", sub.schema_path())))?;
        let store = open_store(sub, storage, Arc::clone(snapshot), &model)?;
        // Index every inverse relation up front, before any request arrives.
        for object in model.domain_objects() {
            for field in &object.fields {
                if let Some(of) = &field.inverse_of {
                    store
                        .prepare_references(&field.ty.named, of)
                        .map_err(|e| Error::serve(&sub.name, e.to_string()))?;
                }
            }
        }
        let schema = schema::build_schema(&sub.name, &sub.sdl, Arc::clone(&store))?;
        out.push(Subgraph {
            name: sub.name.clone(),
            store,
            schema,
            revision: registry
                .commit
                .clone()
                .unwrap_or_else(|| registry.source.clone()),
        });
    }
    Ok(out)
}

/// Why `serve` would refuse each local sub-ontology, as `(name, reason)`.
/// Builds each subgraph against an empty store, so nothing is read or
/// indexed. `check` and `compose` use this to refuse what `serve` cannot run.
pub fn unservable(registry: &Registry) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for sub in registry.local() {
        let result = schema::build_schema(&sub.name, &sub.sdl, Arc::new(crate::store::NullStore));
        if let Err(e) = result {
            let reason = match e {
                Error::Serve { message, .. } => message,
                other => other.to_string(),
            };
            out.push((sub.name.clone(), reason));
        }
    }
    out
}

struct AppState {
    /// Name → schema and the revision it serves.
    subgraphs: BTreeMap<String, (schema::SubgraphSchema, String)>,
}

/// HTTP routes: `POST /<name>/graphql`, `GET /` (index), `GET /healthz`.
pub fn router(subgraphs: Vec<Subgraph>) -> Router {
    let state = Arc::new(AppState {
        subgraphs: subgraphs
            .into_iter()
            .map(|s| (s.name, (s.schema, s.revision)))
            .collect(),
    });
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(|| async { "ok" }))
        .route("/{name}/graphql", post(graphql))
        .with_state(state)
}

/// Bind `addr`. Bind before publishing anything that points at the address
/// (a supergraph), so a taken port fails before any side effect.
pub async fn bind(addr: &str) -> std::io::Result<tokio::net::TcpListener> {
    tokio::net::TcpListener::bind(addr).await
}

/// Bind `addr` and serve `subgraphs` until `shutdown` resolves.
pub async fn serve<F>(addr: &str, subgraphs: Vec<Subgraph>, shutdown: F) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    serve_listener(
        tokio::net::TcpListener::bind(addr).await?,
        subgraphs,
        shutdown,
    )
    .await
}

/// Serve `subgraphs` on an already-bound listener (tests bind port 0).
pub async fn serve_listener<F>(
    listener: tokio::net::TcpListener,
    subgraphs: Vec<Subgraph>,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, router(subgraphs))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn index(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let subgraphs: Vec<_> = state
        .subgraphs
        .keys()
        .map(|name| serde_json::json!({ "name": name, "path": format!("/{name}/graphql") }))
        .collect();
    Json(serde_json::json!({ "subgraphs": subgraphs }))
}

async fn graphql(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Json(request): Json<async_graphql::Request>,
) -> Response {
    match state.subgraphs.get(&name) {
        Some((schema, revision)) => (
            [(REVISION_HEADER, revision.clone())],
            Json(schema::execute(schema, request).await),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "errors": [{ "message": format!("no local sub-ontology named `{name}`") }]
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{test_support::commit_files, GitSnapshot};

    #[test]
    fn builds_local_subgraphs_and_skips_remote() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("ontology/core/subgraph.toml", "[storage]\nkind = \"git\"\n"),
                (
                    "ontology/core/schema.graphql",
                    "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\"])\ntype Person @key(fields: \"id\") { id: ID! name: String! }\ntype Query { person(id: ID!): Person }",
                ),
                ("ontology/partner/subgraph.toml", "[remote]\nurl = \"https://p.example/graphql\"\n"),
                ("ontology/partner/schema.graphql", "type Query { ping: Boolean }"),
            ],
            "init",
        );
        let snapshot: Arc<dyn Snapshot> = Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap());
        let (registry, issues) = Registry::load(snapshot.as_ref(), "ontology").unwrap();
        assert!(issues.is_empty());
        let subgraphs = build_subgraphs(&registry, &snapshot).unwrap();
        let names: Vec<_> = subgraphs.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["core"]);
        assert!(subgraphs[0]
            .store
            .describe()
            .starts_with("git:ontology/core/data@HEAD@"));
    }
}
