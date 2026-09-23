//! Git-backed federated ontology.
//!
//! An ontology is a set of **sub-ontologies**, each one a GraphQL
//! Federation v2 subgraph schema stored in git under `ontology/<name>/`.
//! Composition merges them into a supergraph; `@key` types are the
//! ontology's entity classes, and any sub-ontology can extend another's
//! entity with properties and relations of its own.
//!
//! Git is the source of truth for the ontology: every read goes through a
//! [`snapshot::Snapshot`] of one revision (or of the working tree while
//! editing), so any commit can be composed, checked, and served. Instance
//! data is per sub-ontology behind [`store::EntityStore`]; the first
//! adapter keeps YAML records in git next to the schema.
//!
//! - [`registry`] — discover sub-ontologies (`subgraph.toml` + `schema.graphql`)
//! - [`model`] — the subset of a subgraph schema the runtime understands
//! - [`compose`] — federation composition into a supergraph
//! - [`check`] — registry, composition, and record validation
//! - [`store`] — storage adapters for instance data
//! - [`serve`] — HTTP subgraph servers for local sub-ontologies
//! - [`cli`] — the `scriptorium-ontology` command line
//!
//! See `docs/ONTOLOGY.md` for the design.

pub mod check;
pub mod cli;
pub mod compose;
pub mod error;
pub mod layout;
pub mod manifest;
pub mod model;
pub mod registry;
pub mod scaffold;
pub mod serve;
pub mod snapshot;
pub mod store;

pub use error::{Error, Result};
pub use registry::{Registry, SubOntology, SubOntologySource};
pub use snapshot::{Snapshot, SnapshotSpec};
