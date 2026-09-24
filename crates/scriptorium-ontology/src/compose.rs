//! Federation composition: sub-ontologies in, supergraph out.

use graphql_composition::{
    compose as compose_subgraphs, render_api_sdl, render_federated_sdl, Subgraphs,
};

use crate::registry::Registry;

/// The result of composing every sub-ontology in a registry.
#[derive(Debug, Clone, Default)]
pub struct Composition {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    /// Federated SDL (`join__` directives, subgraph URLs) for a gateway.
    pub supergraph: Option<String>,
    /// The client-facing schema: what a query against the gateway sees.
    pub api_schema: Option<String>,
}

impl Composition {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty() && self.supergraph.is_some()
    }
}

/// Compose every sub-ontology in `registry`. Local sub-ontologies are
/// addressed at `<local_base_url>/<name>/graphql`; remote ones at their
/// configured URL.
pub fn compose(registry: &Registry, local_base_url: &str) -> Composition {
    let mut out = Composition::default();
    if registry.sub_ontologies.is_empty() {
        out.errors.push(format!(
            "no sub-ontologies found under `{}/` (a sub-ontology is a directory with subgraph.toml)",
            registry.ontology_dir
        ));
        return out;
    }

    let mut subgraphs = Subgraphs::default();
    for sub in &registry.sub_ontologies {
        let url = sub.subgraph_url(local_base_url);
        if let Err(e) = subgraphs.ingest_str(&sub.sdl, &sub.name, Some(&url)) {
            out.errors
                .push(format!("[{}] {}: {e}", sub.name, sub.schema_path()));
        }
    }
    if !out.errors.is_empty() {
        return out;
    }

    let result = compose_subgraphs(&mut subgraphs);
    let diagnostics = result.diagnostics();
    out.errors
        .extend(diagnostics.iter_errors().map(ToString::to_string));
    out.warnings
        .extend(diagnostics.iter_warnings().map(ToString::to_string));
    let Ok(graph) = result.into_result() else {
        return out;
    };

    match render_federated_sdl(&graph) {
        Ok(sdl) => {
            let header = format!(
                "# Supergraph composed by scriptorium from `{}/` at {}.\n# Generated file: recompose with `scriptorium-ontology compose`.\n\n",
                registry.ontology_dir, registry.source
            );
            out.supergraph = Some(header + &join_root_query(&sdl, registry));
        }
        Err(e) => out.errors.push(format!("rendering supergraph failed: {e}")),
    }
    let api = render_api_sdl(&graph);
    // Composition accepts any root operation; a gateway needs a query.
    if !has_query_fields(&api) {
        out.errors.push(
            "the composed ontology has no Query field, so a gateway has nothing to ask; give some sub-ontology a lookup or list root field".to_string(),
        );
    }
    out.api_schema = Some(api);
    out
}

/// Whether the client-facing schema `api` has a query root with a field.
/// graphql-composition gives the root query type no `@join__type`, and a
/// planner that follows the join spec (Hive Router) then finds no subgraph
/// to ask for any root field. Each subgraph that defines root query fields
/// gets one on the root type, as Apollo's composition writes them; one
/// already there is kept. Everything is read from the parsed supergraph,
/// never matched in its text.
fn join_root_query(supergraph: &str, registry: &Registry) -> String {
    use cynic_parser::type_system::{Definition, TypeDefinition};
    let Ok(doc) = cynic_parser::parse_type_system_document(supergraph) else {
        return supergraph.to_string();
    };
    let root = doc
        .definitions()
        .find_map(|definition| match definition {
            Definition::Schema(schema) | Definition::SchemaExtension(schema) => schema
                .root_operations()
                .find(|root| {
                    matches!(
                        root.operation_type(),
                        cynic_parser::common::OperationType::Query
                    )
                })
                .map(|root| root.named_type()),
            _ => None,
        })
        .unwrap_or("Query");
    // Subgraph name → its `join__Graph` value (`research` → `RESEARCH`).
    let mut graphs = std::collections::HashMap::new();
    let mut query = None;
    for definition in doc.definitions() {
        match definition {
            Definition::Type(TypeDefinition::Enum(graph)) if graph.name() == "join__Graph" => {
                for value in graph.values() {
                    let name = value
                        .directives()
                        .filter(|d| d.name() == "join__graph")
                        .flat_map(|d| d.arguments())
                        .find(|a| a.name() == "name")
                        .and_then(|a| a.value().as_str());
                    if let Some(name) = name {
                        graphs.insert(name, value.value());
                    }
                }
            }
            Definition::Type(TypeDefinition::Object(object)) if object.name() == root => {
                query = Some(object);
            }
            _ => {}
        }
    }
    let Some(query) = query else {
        return supergraph.to_string();
    };
    let joined: std::collections::HashSet<&str> = query
        .directives()
        .filter(|d| d.name() == "join__type")
        .flat_map(|d| d.arguments())
        .filter(|a| a.name() == "graph")
        .filter_map(|a| a.value().as_enum_value())
        .collect();
    let mut owners: Vec<&str> = registry
        .sub_ontologies
        .iter()
        .filter(|sub| defines_root_query_fields(&sub.sdl))
        .filter_map(|sub| graphs.get(sub.name.as_str()).copied())
        .filter(|graph| !joined.contains(graph))
        .collect();
    owners.sort_unstable();
    owners.dedup();
    // Right after the type's name: directive order does not matter, and
    // nothing between a name and its directives can be inside a string.
    let at = query.name_span().end;
    let mut out = String::with_capacity(supergraph.len() + 32 * owners.len());
    out.push_str(&supergraph[..at]);
    for graph in owners {
        out.push_str("\n  @join__type(graph: ");
        out.push_str(graph);
        out.push(')');
    }
    out.push_str(&supergraph[at..]);
    out
}

/// Whether the subgraph `sdl` defines (or extends) its root query type
/// with at least one field. Read as the model reads it (cynic-parser takes
/// descriptions on type extensions, which the scaffold writes).
fn defines_root_query_fields(sdl: &str) -> bool {
    crate::model::SubOntologyModel::parse(sdl).is_ok_and(|model| {
        model
            .query_type
            .as_ref()
            .and_then(|root| model.objects.get(root))
            .is_some_and(|root| !root.fields.is_empty())
    })
}

fn has_query_fields(api: &str) -> bool {
    use async_graphql::parser::types::{TypeKind, TypeSystemDefinition};
    let Ok(doc) = async_graphql::parser::parse_schema(api) else {
        return false;
    };
    let root = doc
        .definitions
        .iter()
        .find_map(|definition| match definition {
            TypeSystemDefinition::Schema(schema) => {
                schema.node.query.as_ref().map(|q| q.node.to_string())
            }
            _ => None,
        })
        .unwrap_or_else(|| "Query".to_string());
    doc.definitions.iter().any(|definition| match definition {
        TypeSystemDefinition::Type(ty) => {
            ty.node.name.node.as_str() == root
                && matches!(&ty.node.kind, TypeKind::Object(object) if !object.fields.is_empty())
        }
        _ => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::StorageConfig;
    use crate::registry::{SubOntology, SubOntologySource};

    fn registry(subs: &[(&str, &str)]) -> Registry {
        Registry {
            ontology_dir: "ontology".into(),
            manifest: crate::manifest::OntologyManifest::default(),
            sub_ontologies: subs
                .iter()
                .map(|(name, sdl)| SubOntology {
                    name: (*name).to_string(),
                    dir: format!("ontology/{name}"),
                    description: None,
                    owners: vec![],
                    sdl: (*sdl).to_string(),
                    source: SubOntologySource::Local(StorageConfig::Git {
                        data_dir: "data".into(),
                    }),
                })
                .collect(),
            source: "HEAD@0000000".into(),
            commit: None,
        }
    }

    const CORE: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Person @key(fields: "id") { id: ID! name: String! }
type Query { person(id: ID!): Person }
"#;

    const RESEARCH: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
directive @inverse(of: String!) on FIELD_DEFINITION
type Paper @key(fields: "id") { id: ID! year: Int authors: [Person!]! }
type Person @key(fields: "id") { id: ID! papers: [Paper!]! @inverse(of: "authors") }
type Query { paper(id: ID!): Paper }
"#;

    #[test]
    fn composes_entity_extension_across_sub_ontologies() {
        let c = compose(
            &registry(&[("core", CORE), ("research", RESEARCH)]),
            "http://h:1",
        );
        assert!(c.is_ok(), "{:?}", c.errors);
        let supergraph = c.supergraph.unwrap();
        assert!(supergraph.contains("url: \"http://h:1/research/graphql\""));
        assert!(supergraph.contains("@join__field(graph: RESEARCH)"));
        let api = c.api_schema.unwrap();
        assert!(
            api.contains("papers: [Paper!]!"),
            "Person gains research's field:\n{api}"
        );
        assert!(!api.contains("inverse"), "ontology directives stay private");
    }

    #[test]
    fn the_root_query_type_joins_every_subgraph_with_root_fields() {
        // Extends Person, and has no root fields of its own.
        const NICKNAMES: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Person @key(fields: "id") { id: ID! nickname: String }
"#;
        // A description on a type extension, as the scaffold writes them.
        const SHELVES: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
"Shelf marks for core's Person."
extend type Person @key(fields: "id") { id: ID! shelf: String }
type Query { shelved: [Person!]! }
"#;
        let c = compose(
            &registry(&[
                ("core", CORE),
                ("nicknames", NICKNAMES),
                ("research", RESEARCH),
                ("shelves", SHELVES),
            ]),
            "http://h:1",
        );
        assert!(c.is_ok(), "{:?}", c.errors);
        let supergraph = c.supergraph.unwrap();
        let head = supergraph
            .split("\ntype Query")
            .nth(1)
            .and_then(|rest| rest.split('{').next())
            .unwrap();
        assert!(head.contains("@join__type(graph: CORE)"), "{head}");
        assert!(head.contains("@join__type(graph: RESEARCH)"), "{head}");
        assert!(head.contains("@join__type(graph: SHELVES)"), "{head}");
        assert!(!head.contains("NICKNAMES"), "{head}");
        // Added once, whatever composition already wrote.
        assert_eq!(
            join_root_query(
                &supergraph,
                &registry(&[("core", CORE), ("research", RESEARCH)])
            ),
            supergraph
        );
    }

    #[test]
    fn root_directives_holding_braces_are_left_whole() {
        let supergraph = r#"schema { query: Query }
enum join__Graph {
  CORE @join__graph(name: "core", url: "http://h:1/core/graphql")
}
type Person @join__type(graph: CORE, key: "id") { id: ID! }
type Query @tag(name: "{internal}")
{
  person(id: ID!): Person @join__field(graph: CORE)
}
"#;
        let joined = join_root_query(supergraph, &registry(&[("core", CORE)]));
        assert!(
            joined.contains("type Query\n  @join__type(graph: CORE) @tag(name: \"{internal}\")\n{"),
            "{joined}"
        );
        cynic_parser::parse_type_system_document(&joined).unwrap();
    }

    #[test]
    fn conflicting_definitions_are_errors() {
        let other = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Paper @key(fields: "id") { id: ID! year: String }
"#;
        let c = compose(
            &registry(&[("core", CORE), ("other", other), ("research", RESEARCH)]),
            "http://h:1",
        );
        assert!(!c.is_ok());
        let all = c.errors.join("\n");
        assert!(all.contains("Paper.year"), "{all}");
    }

    #[test]
    fn syntax_errors_name_the_sub_ontology() {
        let c = compose(&registry(&[("core", "type {")]), "http://h:1");
        assert!(!c.is_ok());
        assert!(
            c.errors[0].starts_with("[core] ontology/core/schema.graphql"),
            "{:?}",
            c.errors
        );
    }

    #[test]
    fn a_graph_without_a_query_field_is_an_error() {
        const PINGS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Mutation { ping: Int }
"#;
        let c = compose(&registry(&[("pings", PINGS)]), "http://h:1");
        assert!(!c.is_ok(), "{:?}", c.errors);
        assert!(
            c.errors.iter().any(|e| e.contains("no Query field")),
            "{:?}",
            c.errors
        );
        assert!(compose(&registry(&[("core", CORE)]), "http://h:1").is_ok());
    }

    #[test]
    fn empty_registry_is_an_error() {
        let c = compose(&registry(&[]), "http://h:1");
        assert!(!c.is_ok());
        assert!(c.errors[0].contains("no sub-ontologies"));
    }
}
