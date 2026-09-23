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
            out.supergraph = Some(header + &sdl);
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
