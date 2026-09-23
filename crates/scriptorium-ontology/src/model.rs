//! The part of a subgraph schema the runtime and the record checker need.
//!
//! Composition validates the full federation semantics; this model only
//! extracts what serving and record validation act on: entity types and
//! their keys, fields and their types, and the federation / ontology
//! directives that decide ownership (`@external`) and derivation
//! (`@inverse`). Constructs the runtime cannot serve yet (interfaces,
//! unions, input objects, nested lists) are recorded in
//! [`SubOntologyModel::unsupported`] instead of failing the parse.
//!
//! Federation directives are recognised the way composition recognises
//! them: by the names `@link(url: ".../federation/...", import: [...])`
//! gives them (including `{ name: "@key", as: "@id" }` renames), or by the
//! link's namespace (`@federation__key`, or `@<as>__key` with `as:`).

use std::collections::{BTreeMap, BTreeSet, HashMap};

use cynic_parser::common::WrappingType;
use cynic_parser::type_system::{
    Definition, Directive, FieldDefinition, ObjectDefinition, TypeDefinition,
};
use cynic_parser::ConstValue;
use serde_json::Value as Json;

/// Built-in GraphQL scalars.
pub const BUILTIN_SCALARS: &[&str] = &["ID", "String", "Int", "Float", "Boolean"];

/// Federation plumbing that may appear in subgraph SDL but is not part of
/// the ontology.
const FEDERATION_TYPES: &[&str] = &["_Any", "_Entity", "_Service", "FieldSet", "_FieldSet"];

/// Prefixes of types defined by linked specs (`link__Import`,
/// `federation__FieldSet`, `join__Graph`, …).
const SPEC_TYPE_PREFIXES: &[&str] = &["link__", "federation__", "join__"];

fn is_federation_plumbing(name: &str) -> bool {
    FEDERATION_TYPES.contains(&name) || SPEC_TYPE_PREFIXES.iter().any(|p| name.starts_with(p))
}

#[derive(Debug, Clone, Default)]
pub struct SubOntologyModel {
    pub objects: BTreeMap<String, ObjectType>,
    pub enums: BTreeMap<String, Vec<String>>,
    /// Custom scalars declared by the schema.
    pub scalars: BTreeSet<String>,
    /// Names of the root operation types present in the schema.
    pub query_type: Option<String>,
    pub mutation_type: Option<String>,
    /// Constructs the runtime cannot serve yet, as human-readable notes.
    pub unsupported: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectType {
    pub name: String,
    pub description: Option<String>,
    /// First `@key`, which is the key records are stored under.
    pub key: Option<EntityKey>,
    /// Further `@key`s. Composition accepts them, but the runtime resolves
    /// entities by the first key only, so local sub-ontologies reject them.
    pub additional_keys: Vec<EntityKey>,
    /// Declared only as `extend type X` (or with `@extends`): this
    /// sub-ontology adds fields to an entity another one owns, so a key
    /// without a local record still names an entity. A plain `type X`
    /// declaration makes this sub-ontology the owner of its records.
    pub extension: bool,
    pub shareable: bool,
    pub fields: Vec<FieldDef>,
}

impl ObjectType {
    pub fn field(&self, name: &str) -> Option<&FieldDef> {
        self.fields.iter().find(|f| f.name == name)
    }

    /// The single key field records are stored under, if the entity has a
    /// single-field key.
    pub fn key_field(&self) -> Option<&str> {
        self.key.as_ref().and_then(EntityKey::single_field)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityKey {
    /// The raw `fields` argument, e.g. `"id"`.
    pub fields: String,
    pub resolvable: bool,
}

impl EntityKey {
    /// `Some("id")` for `@key(fields: "id")`; `None` for compound or nested keys.
    pub fn single_field(&self) -> Option<&str> {
        let trimmed = self.fields.trim();
        let is_single = !trimmed.is_empty()
            && trimmed
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_');
        is_single.then_some(trimmed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldDef {
    pub name: String,
    pub description: Option<String>,
    pub ty: TypeExpr,
    pub args: Vec<ArgDef>,
    /// Declared here but owned (resolved) by another subgraph.
    pub external: bool,
    pub shareable: bool,
    /// `@inverse(of: "field")`: derived from the named field on the target
    /// type, never stored.
    pub inverse_of: Option<String>,
    /// `@override(from: "sub")`: this sub-ontology takes the field over from
    /// another one, which then no longer resolves it…
    pub override_from: Option<String>,
    /// …unless the override is progressive (`label: "percent(10)"`), when
    /// either may.
    pub override_label: Option<String>,
}

impl FieldDef {
    /// Fields whose values this sub-ontology stores in its records.
    pub fn is_stored(&self) -> bool {
        !self.external && self.inverse_of.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArgDef {
    pub name: String,
    pub ty: TypeExpr,
    /// The declared default (`first: Int = 10`), which the server applies.
    pub default: Option<Json>,
}

/// A field or argument type with at most one list level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeExpr {
    /// Innermost named type.
    pub named: String,
    /// Outer non-null (`T!` or `[T]!`).
    pub non_null: bool,
    /// `Some(item_non_null)` for list types.
    pub list: Option<bool>,
}

impl TypeExpr {
    pub fn is_list(&self) -> bool {
        self.list.is_some()
    }
}

impl std::fmt::Display for TypeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bang = |b: bool| if b { "!" } else { "" };
        match self.list {
            Some(item_non_null) => write!(
                f,
                "[{}{}]{}",
                self.named,
                bang(item_non_null),
                bang(self.non_null)
            ),
            None => write!(f, "{}{}", self.named, bang(self.non_null)),
        }
    }
}

/// What a named type refers to within a sub-ontology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeKind<'a> {
    Scalar,
    Enum(&'a [String]),
    /// Object type with a key: a relation target.
    Entity(&'a ObjectType),
    /// Object type without a key: an inline value object.
    Value(&'a ObjectType),
    Unknown,
}

impl SubOntologyModel {
    /// Parse a subgraph schema.
    pub fn parse(sdl: &str) -> std::result::Result<Self, String> {
        let doc = cynic_parser::parse_type_system_document(sdl).map_err(|e| e.to_string())?;
        let mut model = Self::default();
        let mut schema_query = None;
        let mut schema_mutation = None;

        // `@link` can follow the types that use its names, so read it first.
        let mut names = DirectiveNames::default();
        for definition in doc.definitions() {
            if let Definition::Schema(schema) | Definition::SchemaExtension(schema) = definition {
                for directive in schema.directives() {
                    names.add_link(&directive);
                }
            }
        }

        for definition in doc.definitions() {
            match definition {
                Definition::Schema(schema) | Definition::SchemaExtension(schema) => {
                    for root in schema.root_operations() {
                        match root.operation_type() {
                            cynic_parser::common::OperationType::Query => {
                                schema_query = Some(root.named_type().to_string());
                            }
                            cynic_parser::common::OperationType::Mutation => {
                                schema_mutation = Some(root.named_type().to_string());
                            }
                            cynic_parser::common::OperationType::Subscription => model
                                .unsupported
                                .push("subscriptions are not supported".into()),
                        }
                    }
                }
                Definition::Type(ty) => model.add_type(ty, false, &names),
                Definition::TypeExtension(ty) => model.add_type(ty, true, &names),
                Definition::Directive(_) => {}
            }
        }

        model.query_type =
            schema_query.or_else(|| model.objects.contains_key("Query").then(|| "Query".into()));
        model.mutation_type = schema_mutation.or_else(|| {
            model
                .objects
                .contains_key("Mutation")
                .then(|| "Mutation".into())
        });
        Ok(model)
    }

    fn add_type(&mut self, ty: TypeDefinition<'_>, is_extension: bool, names: &DirectiveNames) {
        if is_federation_plumbing(ty.name()) {
            return;
        }
        match ty {
            TypeDefinition::Object(object) => self.add_object(object, is_extension, names),
            TypeDefinition::Enum(e) => {
                let values = self.enums.entry(e.name().to_string()).or_default();
                values.extend(e.values().map(|v| v.value().to_string()));
            }
            TypeDefinition::Scalar(s) => {
                self.scalars.insert(s.name().to_string());
            }
            TypeDefinition::Interface(i) => self.unsupported.push(format!(
                "interface `{}` (interfaces are not served yet)",
                i.name()
            )),
            TypeDefinition::Union(u) => self
                .unsupported
                .push(format!("union `{}` (unions are not served yet)", u.name())),
            TypeDefinition::InputObject(i) => self.unsupported.push(format!(
                "input `{}` (input objects are not served yet)",
                i.name()
            )),
        }
    }

    fn add_object(
        &mut self,
        object: ObjectDefinition<'_>,
        is_extension: bool,
        names: &DirectiveNames,
    ) {
        let name = object.name().to_string();
        let mut keys = Vec::new();
        let mut shareable = false;
        let mut extends = is_extension;
        for directive in object.directives() {
            match names.canonical(directive.name()) {
                Some("extends") => extends = true,
                Some("key") => keys.push(EntityKey {
                    fields: string_arg(&directive, "fields").unwrap_or_default(),
                    resolvable: bool_arg(&directive, "resolvable").unwrap_or(true),
                }),
                Some("shareable") => shareable = true,
                Some(d @ ("interfaceObject" | "context")) => self
                    .unsupported
                    .push(format!("`{name}` uses @{d} (not served yet)")),
                _ => {}
            }
        }

        let mut fields = Vec::new();
        for field in object.fields() {
            match field_def(&name, field, names) {
                Ok(f) => fields.push(f),
                Err(note) => self.unsupported.push(note),
            }
        }

        // `type X` and `extend type X` in the same subgraph merge.
        let entry = self
            .objects
            .entry(name.clone())
            .or_insert_with(|| ObjectType {
                name,
                description: None,
                key: None,
                additional_keys: Vec::new(),
                extension: true,
                shareable: false,
                fields: Vec::new(),
            });
        // Any plain declaration makes this sub-ontology the owner.
        entry.extension &= extends;
        if entry.description.is_none() {
            entry.description = object
                .description()
                .map(|d| d.literal().to_cow().into_owned());
        }
        for key in keys {
            if entry.key.is_none() {
                entry.key = Some(key);
            } else if entry.key.as_ref() != Some(&key) && !entry.additional_keys.contains(&key) {
                entry.additional_keys.push(key);
            }
        }
        entry.shareable |= shareable;
        entry.fields.extend(fields);
    }

    pub fn object(&self, name: &str) -> Option<&ObjectType> {
        self.objects.get(name)
    }

    /// Classify a named type.
    pub fn kind_of(&self, named: &str) -> TypeKind<'_> {
        if BUILTIN_SCALARS.contains(&named) || self.scalars.contains(named) {
            return TypeKind::Scalar;
        }
        if let Some(values) = self.enums.get(named) {
            return TypeKind::Enum(values);
        }
        match self.objects.get(named) {
            Some(o) if o.key.is_some() => TypeKind::Entity(o),
            Some(o) => TypeKind::Value(o),
            None => TypeKind::Unknown,
        }
    }

    /// Object types that are not root operation types.
    pub fn domain_objects(&self) -> impl Iterator<Item = &ObjectType> {
        self.objects.values().filter(|o| {
            Some(o.name.as_str()) != self.query_type.as_deref()
                && Some(o.name.as_str()) != self.mutation_type.as_deref()
        })
    }

    /// Entity types (object types with a key).
    pub fn entities(&self) -> impl Iterator<Item = &ObjectType> {
        self.domain_objects().filter(|o| o.key.is_some())
    }
}

fn field_def(
    parent: &str,
    field: FieldDefinition<'_>,
    names: &DirectiveNames,
) -> std::result::Result<FieldDef, String> {
    let ty = type_expr(field.ty().wrappers(), field.ty().name()).ok_or_else(|| {
        format!(
            "`{parent}.{}`: nested list types are not served yet",
            field.name()
        )
    })?;
    let mut args = Vec::new();
    for arg in field.arguments() {
        let arg_ty = type_expr(arg.ty().wrappers(), arg.ty().name()).ok_or_else(|| {
            format!(
                "`{parent}.{}({}:)`: nested list arguments are not served yet",
                field.name(),
                arg.name()
            )
        })?;
        if let Some(d) = arg
            .directives()
            .find_map(|d| names.canonical(d.name()).filter(|c| *c == "fromContext"))
        {
            return Err(format!(
                "`{parent}.{}({}:)` uses @{d} (not served yet)",
                field.name(),
                arg.name()
            ));
        }
        args.push(ArgDef {
            name: arg.name().to_string(),
            ty: arg_ty,
            default: arg.default_value().map(const_json),
        });
    }
    let mut def = FieldDef {
        name: field.name().to_string(),
        description: field
            .description()
            .map(|d| d.literal().to_cow().into_owned()),
        ty,
        args,
        external: false,
        shareable: false,
        inverse_of: None,
        override_from: None,
        override_label: None,
    };
    for directive in field.directives() {
        match names.canonical(directive.name()) {
            Some("external") => def.external = true,
            Some("shareable") => def.shareable = true,
            Some("override") => {
                def.override_from = string_arg(&directive, "from");
                def.override_label = string_arg(&directive, "label");
            }
            // Both change what the gateway asks this subgraph for; stored
            // fields cannot honour either yet.
            Some(d @ ("requires" | "provides")) => {
                return Err(format!(
                    "`{parent}.{}` uses @{d} (not served yet)",
                    field.name()
                ));
            }
            // Scriptorium's own directive, never linked or renamed.
            _ if directive.name() == "inverse" => def.inverse_of = string_arg(&directive, "of"),
            _ => {}
        }
    }
    Ok(def)
}

fn type_expr(wrappers: impl Iterator<Item = WrappingType>, named: &str) -> Option<TypeExpr> {
    let wrappers: Vec<WrappingType> = wrappers.collect();
    let mut rest = wrappers.as_slice();
    let take_non_null = |rest: &mut &[WrappingType]| {
        if rest.first() == Some(&WrappingType::NonNull) {
            *rest = &rest[1..];
            true
        } else {
            false
        }
    };
    let non_null = take_non_null(&mut rest);
    let list = if rest.first() == Some(&WrappingType::List) {
        rest = &rest[1..];
        Some(take_non_null(&mut rest))
    } else {
        None
    };
    rest.is_empty().then(|| TypeExpr {
        named: named.to_string(),
        non_null,
        list,
    })
}

/// Federation directive names as this schema spells them.
#[derive(Debug, Default)]
struct DirectiveNames {
    /// Whether the schema `@link`s the federation spec at all.
    linked: bool,
    /// Local name (without `@`) → canonical federation name, from `import:`.
    imported: HashMap<String, String>,
    /// Namespace for directives used without importing them (`federation`
    /// by default, or the link's `as:`).
    namespace: Option<String>,
}

impl DirectiveNames {
    fn add_link(&mut self, directive: &Directive<'_>) {
        if directive.name() != "link" {
            return;
        }
        let Some(url) = string_arg(directive, "url") else {
            return;
        };
        if !url.contains("specs.apollo.dev/federation/") {
            return;
        }
        self.linked = true;
        self.namespace = Some(string_arg(directive, "as").unwrap_or_else(|| "federation".into()));
        let Some(ConstValue::List(imports)) = directive
            .arguments()
            .find(|a| a.name() == "import")
            .map(|a| a.value())
        else {
            return;
        };
        for import in imports.items() {
            let (canonical, local) = match import {
                ConstValue::String(s) => (s.value().to_string(), s.value().to_string()),
                ConstValue::Object(o) => {
                    let Some(name) = o.get("name").and_then(|v| v.as_str().map(str::to_string))
                    else {
                        continue;
                    };
                    let local = o
                        .get("as")
                        .and_then(|v| v.as_str().map(str::to_string))
                        .unwrap_or_else(|| name.clone());
                    (name, local)
                }
                _ => continue,
            };
            // Directive imports start with `@`; type imports (e.g. `FieldSet`) do not.
            if let (Some(canonical), Some(local)) =
                (canonical.strip_prefix('@'), local.strip_prefix('@'))
            {
                self.imported
                    .insert(local.to_string(), canonical.to_string());
            }
        }
    }

    /// The canonical federation name of a directive as written, or `None`
    /// when it is not a federation directive in this schema.
    fn canonical<'a>(&'a self, written: &'a str) -> Option<&'a str> {
        if !self.linked {
            // Federation 1 style schemas use the bare names.
            return Some(written.strip_prefix("federation__").unwrap_or(written));
        }
        if let Some(canonical) = self.imported.get(written) {
            return Some(canonical);
        }
        let namespace = self.namespace.as_deref().unwrap_or("federation");
        written
            .strip_prefix(namespace)
            .and_then(|rest| rest.strip_prefix("__"))
    }
}

fn const_json(value: ConstValue<'_>) -> Json {
    match value {
        ConstValue::Int(i) => Json::from(i.value()),
        ConstValue::Float(f) => {
            serde_json::Number::from_f64(f.value()).map_or(Json::Null, Json::Number)
        }
        ConstValue::String(s) => Json::String(s.value().to_string()),
        ConstValue::Boolean(b) => Json::Bool(b.value()),
        ConstValue::Null(_) => Json::Null,
        ConstValue::Enum(e) => Json::String(e.name().to_string()),
        ConstValue::List(items) => Json::Array(items.items().map(const_json).collect()),
        ConstValue::Object(object) => Json::Object(
            object
                .fields()
                .map(|f| (f.name().to_string(), const_json(f.value())))
                .collect(),
        ),
    }
}

fn string_arg(directive: &Directive<'_>, name: &str) -> Option<String> {
    directive
        .arguments()
        .find(|a| a.name() == name)
        .and_then(|a| a.value().as_str().map(ToString::to_string))
}

fn bool_arg(directive: &Directive<'_>, name: &str) -> Option<bool> {
    directive
        .arguments()
        .find(|a| a.name() == name)
        .and_then(|a| match a.value() {
            ConstValue::Boolean(b) => Some(b.value()),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESEARCH: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key", "@shareable", "@external"])
directive @inverse(of: String!) on FIELD_DEFINITION
scalar Date
enum Venue { CONFERENCE JOURNAL PREPRINT }

"A scholarly paper."
type Paper @key(fields: "id") {
  id: ID!
  title: String!
  year: Int
  venue: Venue
  published: Date
  authors: [Person!]!
  pages: PageRange
}

type PageRange @shareable { first: Int! last: Int! }

type Person @key(fields: "id") {
  id: ID!
  name: String! @external
  papers: [Paper!]! @inverse(of: "authors")
}

type Query {
  paper(id: ID!): Paper
  papers(first: Int): [Paper!]!
}
"#;

    #[test]
    fn extracts_entities_fields_and_directives() {
        let m = SubOntologyModel::parse(RESEARCH).unwrap();
        assert_eq!(m.query_type.as_deref(), Some("Query"));
        assert!(m.mutation_type.is_none());
        assert!(m.unsupported.is_empty(), "{:?}", m.unsupported);

        let paper = m.object("Paper").unwrap();
        assert_eq!(paper.key_field(), Some("id"));
        assert_eq!(paper.description.as_deref(), Some("A scholarly paper."));
        let authors = paper.field("authors").unwrap();
        assert_eq!(authors.ty.to_string(), "[Person!]!");
        assert!(authors.is_stored());

        let person = m.object("Person").unwrap();
        assert!(person.field("name").unwrap().external);
        assert_eq!(
            person.field("papers").unwrap().inverse_of.as_deref(),
            Some("authors")
        );
        assert!(!person.field("papers").unwrap().is_stored());

        assert!(matches!(m.kind_of("Person"), TypeKind::Entity(_)));
        assert!(matches!(m.kind_of("PageRange"), TypeKind::Value(_)));
        assert!(matches!(m.kind_of("Venue"), TypeKind::Enum(v) if v.len() == 3));
        assert!(matches!(m.kind_of("Date"), TypeKind::Scalar));
        assert!(matches!(m.kind_of("Nope"), TypeKind::Unknown));

        let entities: Vec<_> = m.entities().map(|o| o.name.as_str()).collect();
        assert_eq!(entities, ["Paper", "Person"]);
        let papers = m.object("Query").unwrap().field("papers").unwrap();
        assert_eq!(papers.args[0].name, "first");
    }

    #[test]
    fn type_expressions() {
        let m = SubOntologyModel::parse(
            "type T { a: String b: String! c: [String] d: [String!] e: [String!]! f: [[String]] }",
        )
        .unwrap();
        let t = m.object("T").unwrap();
        let shown: Vec<String> = t.fields.iter().map(|f| f.ty.to_string()).collect();
        assert_eq!(
            shown,
            ["String", "String!", "[String]", "[String!]", "[String!]!"]
        );
        assert_eq!(m.unsupported.len(), 1, "nested list is noted, not fatal");
    }

    #[test]
    fn keys_resolvable_flag_and_namespaced_directives() {
        let m = SubOntologyModel::parse(
            r#"type A @federation__key(fields: "id", resolvable: false) { id: ID! }
               type B @key(fields: "org { id } slug") { slug: String! }
               extend type C @key(fields: "id") { id: ID! }"#,
        )
        .unwrap();
        let a = m.object("A").unwrap().key.clone().unwrap();
        assert!(!a.resolvable);
        assert_eq!(m.object("B").unwrap().key_field(), None, "compound key");
        assert_eq!(
            m.object("C").unwrap().key_field(),
            Some("id"),
            "extend type"
        );
    }

    #[test]
    fn link_imports_rename_and_namespace_federation_directives() {
        let m = SubOntologyModel::parse(
            r#"extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", as: "fed",
                 import: [{ name: "@key", as: "@id" }, { name: "@external", as: "@ext" }])
               type P @id(fields: "id") {
                 id: ID!
                 name: String! @ext
                 other: String @fed__external
                 plain: String @external
               }"#,
        )
        .unwrap();
        let p = m.object("P").unwrap();
        assert_eq!(p.key_field(), Some("id"), "renamed @key");
        assert!(p.field("name").unwrap().external, "renamed import");
        assert!(p.field("other").unwrap().external, "link namespace");
        assert!(
            !p.field("plain").unwrap().external,
            "bare @external is not federation's once it is imported under another name"
        );
    }

    #[test]
    fn extensions_are_marked_by_extend_type_or_extends() {
        let m = SubOntologyModel::parse(
            r#"extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key", "@extends"])
               type Owned @key(fields: "id") { id: ID! }
               extend type ByExtend @key(fields: "id") { id: ID! }
               type ByDirective @key(fields: "id") @extends { id: ID! }
               type Mixed @key(fields: "id") { id: ID! }
               extend type Mixed @key(fields: "id") { more: Int }"#,
        )
        .unwrap();
        assert!(!m.object("Owned").unwrap().extension);
        assert!(m.object("ByExtend").unwrap().extension);
        assert!(m.object("ByDirective").unwrap().extension);
        assert!(
            !m.object("Mixed").unwrap().extension,
            "a plain declaration owns"
        );
    }

    #[test]
    fn additional_keys_are_recorded() {
        let m = SubOntologyModel::parse(
            r#"type T @key(fields: "id") @key(fields: "slug") { id: ID! slug: String! }"#,
        )
        .unwrap();
        let t = m.object("T").unwrap();
        assert_eq!(t.key_field(), Some("id"));
        assert_eq!(t.additional_keys.len(), 1);
    }

    #[test]
    fn spec_plumbing_types_are_skipped() {
        let m = SubOntologyModel::parse(
            "scalar link__Import\nenum link__Purpose { SECURITY EXECUTION }\nscalar federation__FieldSet\nscalar _Any\ntype Query { a: Int }",
        )
        .unwrap();
        assert!(m.scalars.is_empty(), "{:?}", m.scalars);
        assert!(m.enums.is_empty(), "{:?}", m.enums);
    }

    #[test]
    fn directives_the_server_cannot_honour_are_unsupported() {
        let m = SubOntologyModel::parse(
            r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.8", import: ["@key", "@external", "@requires", "@provides", "@context", "@fromContext"])
type Product @key(fields: "id") @context(name: "ctx") {
  id: ID!
  weight: Int @external
  shipping: Int @requires(fields: "weight")
  maker: Maker @provides(fields: "name")
}
type Maker @key(fields: "id") { id: ID! name: String @external }
type Query { product(id: ID!, lang: String @fromContext(field: "$ctx { lang }")): Product }
"#,
        )
        .unwrap();
        let notes = m.unsupported.join("\n");
        for needle in [
            "`Product` uses @context",
            "`Product.shipping` uses @requires",
            "`Product.maker` uses @provides",
            "`Query.product(lang:)` uses @fromContext",
        ] {
            assert!(notes.contains(needle), "missing `{needle}` in:\n{notes}");
        }
    }

    #[test]
    fn argument_defaults_are_recorded() {
        let m = SubOntologyModel::parse(
            "type T @key(fields: \"id\") { id: ID! }\ntype Query { ts(first: Int = 10): [T!]! }",
        )
        .unwrap();
        let first = &m.object("Query").unwrap().field("ts").unwrap().args[0];
        assert_eq!(first.default, Some(Json::from(10)));
    }

    #[test]
    fn syntax_errors_surface_as_messages() {
        assert!(SubOntologyModel::parse("type {").is_err());
    }
}
