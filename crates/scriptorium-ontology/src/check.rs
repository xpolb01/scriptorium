//! `ontology check`: registry, composition, schema, and record validation.
//!
//! Records are validated against the schema of the sub-ontology that
//! stores them: a record may only hold fields that sub-ontology owns, which
//! is what keeps each entity's data partitioned by owner. References are
//! checked across every git-backed sub-ontology at the same revision.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::Value;

use crate::compose::{compose, Composition};
use crate::error::Result;
use crate::layout::{join, resolve_within, validate_record_key, RECORD_EXT};
use crate::manifest::{MarkdownConfig, StorageConfig};
use crate::model::{FieldDef, ObjectType, SubOntologyModel, TypeExpr, TypeKind};
use crate::registry::{Registry, SubOntology, SubOntologySource};
use crate::serve::unservable;
use crate::snapshot::Snapshot;
use crate::store::git::{json_kind, parse_record};
use crate::store::markdown::load_notes;
use crate::store::Record;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Issue {
    pub severity: Severity,
    /// Stable rule id, e.g. `ontology.record.not_owned`.
    pub rule: &'static str,
    pub sub_ontology: Option<String>,
    /// Repository-relative path the issue is about.
    pub path: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CheckReport {
    /// Where the ontology was read from, e.g. `HEAD@1a2b3c4`.
    pub source: String,
    pub sub_ontologies: usize,
    pub records: usize,
    pub issues: Vec<Issue>,
    #[serde(skip)]
    pub composition: Composition,
}

impl CheckReport {
    pub fn errors(&self) -> usize {
        self.count(Severity::Error)
    }

    pub fn warnings(&self) -> usize {
        self.count(Severity::Warning)
    }

    fn count(&self, severity: Severity) -> usize {
        self.issues
            .iter()
            .filter(|i| i.severity == severity)
            .count()
    }
}

/// Check the ontology under `ontology_dir` in `snapshot`.
pub fn check(snapshot: &dyn Snapshot, ontology_dir: &str) -> Result<CheckReport> {
    let (registry, registry_issues) = Registry::load(snapshot, ontology_dir)?;
    let mut acc = Accumulator::default();

    for issue in registry_issues {
        acc.issues.push(Issue {
            severity: Severity::Error,
            rule: "ontology.registry",
            sub_ontology: None,
            path: Some(issue.path),
            message: issue.message,
        });
    }

    let composition = compose(&registry, &registry.manifest.serve.base_url());
    for message in &composition.errors {
        acc.push(
            Severity::Error,
            "ontology.composition",
            None,
            None,
            message.clone(),
        );
    }
    for message in &composition.warnings {
        acc.push(
            Severity::Warning,
            "ontology.composition",
            None,
            None,
            message.clone(),
        );
    }

    // `check` must fail on anything `serve` would refuse.
    let parses = |name: &str| {
        registry
            .get(name)
            .is_some_and(|s| SubOntologyModel::parse(&s.sdl).is_ok())
    };
    for (name, reason) in unservable(&registry) {
        // Syntax errors are already reported by composition.
        if !parses(&name) {
            continue;
        }
        acc.push(
            Severity::Error,
            "ontology.schema.unservable",
            Some(&name),
            registry.get(&name).map(SubOntology::schema_path),
            format!("`scriptorium-ontology serve` cannot serve this sub-ontology: {reason}"),
        );
    }

    // Every entity type a git- or markdown-backed sub-ontology owns has a
    // complete key set (both are read in full here), even with no records.
    register_owners(&registry, &mut acc);
    acc.moves = override_moves(&registry);

    validate_instances(snapshot, &registry, &mut acc)?;
    acc.check_references();
    acc.check_required_records();
    acc.check_override_coverage();
    check_single_resolvers(&registry, &mut acc);
    check_root_fields(&registry, &mut acc);
    check_extension_descriptions(&registry, &mut acc);

    let mut issues = acc.issues;
    issues.sort_by(|a, b| {
        (a.severity, &a.path, a.rule, &a.message).cmp(&(b.severity, &b.path, b.rule, &b.message))
    });
    Ok(CheckReport {
        source: registry.source.clone(),
        sub_ontologies: registry.sub_ontologies.len(),
        records: acc.records,
        issues,
        composition,
    })
}

/// Whether `check` reads this sub-ontology's instances (git records and
/// markdown notes are read in full).
fn reads_instances(source: &SubOntologySource) -> bool {
    matches!(
        source,
        SubOntologySource::Local(StorageConfig::Git { .. } | StorageConfig::Markdown(_))
    )
}

/// Note which sub-ontologies own each entity type.
fn register_owners(registry: &Registry, acc: &mut Accumulator) {
    for sub in &registry.sub_ontologies {
        if !reads_instances(&sub.source) {
            continue;
        }
        let Ok(model) = SubOntologyModel::parse(&sub.sdl) else {
            continue;
        };
        for entity in model.entities().filter(|o| !o.extension) {
            acc.owners
                .entry(entity.name.clone())
                .or_default()
                .insert(sub.name.clone());
        }
    }
}

/// What `check` can see of a sub-ontology's instances.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    /// A local sub-ontology whose instances `check` reads.
    Read,
    /// A local sub-ontology whose instances `check` does not read (a database).
    Unread,
    /// Served elsewhere.
    Remote,
}

impl Access {
    fn of(source: &SubOntologySource) -> Self {
        match source {
            _ if reads_instances(source) => Self::Read,
            SubOntologySource::Local(_) => Self::Unread,
            SubOntologySource::Remote(_) => Self::Remote,
        }
    }
}

/// A field one sub-ontology takes over from another with an unlabelled
/// `@override(from:)`: the gateway reads it from `to` only.
struct Move {
    type_name: String,
    field: String,
    from: String,
    to: String,
    from_access: Access,
    to_access: Access,
    to_schema: String,
    /// Why check cannot confirm the move keeps the data, if it cannot:
    /// either side derives the field (`@inverse`), or `from` derives
    /// another field from it, which would keep reading its records.
    unverifiable: Option<String>,
    /// The field's type, for what an absent value answers.
    ty: TypeExpr,
    /// The field is a relation: its values are keys.
    relation: bool,
}

impl Move {
    /// What a record without the field answers, in comparable form.
    fn absent(&self) -> Value {
        if self.ty.is_list() {
            Value::Array(Vec::new())
        } else {
            Value::Null
        }
    }
}

fn override_moves(registry: &Registry) -> Vec<Move> {
    let mut moves = Vec::new();
    for sub in &registry.sub_ontologies {
        let Ok(model) = SubOntologyModel::parse(&sub.sdl) else {
            continue;
        };
        for entity in model.entities() {
            for field in &entity.fields {
                let (Some(from), None) = (&field.override_from, &field.override_label) else {
                    continue;
                };
                let Some(source) = registry.get(from) else {
                    continue;
                };
                let source_model = SubOntologyModel::parse(&source.sdl).ok();
                let derived_at_source = source_model.as_ref().is_some_and(|m| {
                    m.object(&entity.name)
                        .and_then(|o| o.field(&field.name))
                        .is_some_and(|f| f.inverse_of.is_some())
                });
                let dependents: Vec<String> = source_model
                    .iter()
                    .flat_map(SubOntologyModel::domain_objects)
                    .flat_map(|o| {
                        o.fields
                            .iter()
                            .filter(|g| {
                                g.inverse_of.as_deref() == Some(field.name.as_str())
                                    && g.ty.named == entity.name
                            })
                            .map(move |g| format!("{}.{}", o.name, g.name))
                    })
                    .collect();
                moves.push(Move {
                    type_name: entity.name.clone(),
                    field: field.name.clone(),
                    from: from.clone(),
                    to: sub.name.clone(),
                    from_access: Access::of(&source.source),
                    to_access: Access::of(&sub.source),
                    to_schema: sub.schema_path(),
                    unverifiable: if derived_at_source || field.inverse_of.is_some() {
                        Some("one of them derives it (@inverse), so check cannot confirm the relations survive the move; keep the derived field in one sub-ontology".to_string())
                    } else if dependents.is_empty() {
                        None
                    } else {
                        Some(format!(
                            "`{from}` derives `{}` from it (@inverse), which would keep reading `{from}`'s records; keep the relation and its inverse in one sub-ontology",
                            dependents.join("`, `")
                        ))
                    },
                    ty: field.ty.clone(),
                    relation: matches!(model.kind_of(&field.ty.named), TypeKind::Entity(_)),
                });
            }
        }
    }
    moves
}

/// Validate the instances `check` can read: git records and markdown notes.
/// Database-backed instances are not read.
fn validate_instances(
    snapshot: &dyn Snapshot,
    registry: &Registry,
    acc: &mut Accumulator,
) -> Result<()> {
    for sub in &registry.sub_ontologies {
        // Syntax errors are already reported by composition.
        let Ok(model) = SubOntologyModel::parse(&sub.sdl) else {
            continue;
        };
        match &sub.source {
            SubOntologySource::Local(StorageConfig::Git { data_dir }) => {
                let records = join(&sub.dir, data_dir);
                acc.require_extension_records(sub, &model, |type_name| join(&records, type_name));
                validate_git_records(snapshot, sub, data_dir, &model, acc)?;
            }
            SubOntologySource::Local(StorageConfig::Markdown(config)) => {
                let notes =
                    resolve_within(&sub.dir, &config.dir).unwrap_or_else(|| sub.dir.clone());
                acc.require_extension_records(sub, &model, |_| notes.clone());
                validate_markdown_notes(snapshot, sub, config, &model, acc);
            }
            // Database instances are not read, but their settings are
            // checked the way `serve` opens them (without connecting).
            SubOntologySource::Local(storage) => {
                if let Err(e) = crate::store::validate_database_storage(sub, storage, &model) {
                    acc.push(
                        Severity::Error,
                        "ontology.storage",
                        Some(&sub.name),
                        Some(join(&sub.dir, crate::layout::SUBGRAPH_MANIFEST)),
                        e.to_string(),
                    );
                }
            }
            SubOntologySource::Remote(_) => {}
        }
    }
    Ok(())
}

#[derive(Default)]
struct Accumulator {
    issues: Vec<Issue>,
    records: usize,
    /// Entity type → sub-ontology → keys it stores.
    stored: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    /// Every stored record, in the order read.
    entries: Vec<Entry>,
    /// Entity type → the git- or markdown-backed sub-ontologies that own it
    /// (declare a plain `type X @key`). An owner answers `null` for a key it has no
    /// record of, so an X exists only when every owner stores it.
    owners: BTreeMap<String, BTreeSet<String>>,
    references: Vec<Reference>,
    /// Required stored fields extensions declare on entities they do not own.
    required: Vec<Required>,
    /// Fields moved between sub-ontologies with `@override`.
    moves: Vec<Move>,
    /// `(type, field)` of a move → sub-ontology (either end) → key → the
    /// value its record answers with (an absent list is `[]`, anything else
    /// absent is `null`), in comparable form.
    moved_values: BTreeMap<(String, String), BTreeMap<String, BTreeMap<String, Value>>>,
}

/// An extension's required stored fields on one entity type.
struct Required {
    sub_ontology: String,
    schema_path: String,
    type_name: String,
    /// `Type.field: T!`, for each required stored field.
    fields: Vec<String>,
    /// Where this sub-ontology keeps its records of the type.
    dir: String,
}

/// A record a sub-ontology stores.
struct Entry {
    sub_ontology: String,
    path: String,
    type_name: String,
    key: String,
}

struct Reference {
    sub_ontology: String,
    path: String,
    at: String,
    target_type: String,
    key: String,
}

impl Accumulator {
    fn push(
        &mut self,
        severity: Severity,
        rule: &'static str,
        sub_ontology: Option<&str>,
        path: Option<String>,
        message: String,
    ) {
        self.issues.push(Issue {
            severity,
            rule,
            sub_ontology: sub_ontology.map(ToString::to_string),
            path,
            message,
        });
    }

    /// Note that `sub` stores the record `type_name:key` at `path`.
    fn store(&mut self, sub: &str, type_name: &str, key: &str, path: &str, record: &Record) {
        for m in self
            .moves
            .iter()
            .filter(|m| m.type_name == type_name && (m.from == sub || m.to == sub))
        {
            let value = record.get(&m.field).cloned().unwrap_or_else(|| m.absent());
            self.moved_values
                .entry((m.type_name.clone(), m.field.clone()))
                .or_default()
                .entry(sub.to_string())
                .or_default()
                .insert(key.to_string(), comparable(&value, m.relation));
        }
        self.stored
            .entry(type_name.to_string())
            .or_default()
            .entry(sub.to_string())
            .or_default()
            .insert(key.to_string());
        self.entries.push(Entry {
            sub_ontology: sub.to_string(),
            path: path.to_string(),
            type_name: type_name.to_string(),
            key: key.to_string(),
        });
    }

    /// An owner of `type_name` that has no record of `key`, if any.
    fn missing_owner(&self, type_name: &str, key: &str) -> Option<&str> {
        let stores = |owner: &String| {
            self.stored
                .get(type_name)
                .and_then(|by_sub| by_sub.get(owner))
                .is_some_and(|keys| keys.contains(key))
        };
        self.owners
            .get(type_name)?
            .iter()
            .find(|owner| !stores(owner))
            .map(String::as_str)
    }

    /// Note the required stored fields `sub` declares on entity types it
    /// extends; `location` names where its records of a type live.
    fn require_extension_records(
        &mut self,
        sub: &SubOntology,
        model: &SubOntologyModel,
        location: impl Fn(&str) -> String,
    ) {
        for entity in model.entities().filter(|o| o.extension) {
            let key_field = entity.key_field();
            let fields: Vec<String> = entity
                .fields
                .iter()
                .filter(|f| {
                    Some(f.name.as_str()) != key_field
                        && f.is_stored()
                        && f.args.is_empty()
                        && f.ty.non_null
                        && !f.ty.is_list()
                })
                .map(|f| format!("{}.{}: {}", entity.name, f.name, f.ty))
                .collect();
            if !fields.is_empty() {
                self.required.push(Required {
                    sub_ontology: sub.name.clone(),
                    schema_path: sub.schema_path(),
                    type_name: entity.name.clone(),
                    fields,
                    dir: location(&entity.name),
                });
            }
        }
    }

    /// The keys of every entity of `type_name`, when git-backed owners
    /// decide them: an entity exists when every owner stores it.
    fn entity_keys(&self, type_name: &str) -> Option<BTreeSet<&str>> {
        let mut owners = self.owners.get(type_name)?.iter();
        let by_sub = self.stored.get(type_name);
        let keys_of = |owner: &String| by_sub.and_then(|m| m.get(owner));
        let mut keys: BTreeSet<&str> = keys_of(owners.next()?)
            .map(|k| k.iter().map(String::as_str).collect())
            .unwrap_or_default();
        for owner in owners {
            let theirs = keys_of(owner);
            keys.retain(|k| theirs.is_some_and(|t| t.contains(*k)));
        }
        Some(keys)
    }

    /// An extension that declares a required stored field (`rating: Int!`)
    /// must keep a record for every entity of its type: without one, the
    /// field errors and `null` propagates through the gateway's response.
    fn check_required_records(&mut self) {
        let mut found = Vec::new();
        for req in &self.required {
            let fields = req.fields.join("`, `");
            let Some(keys) = self.entity_keys(&req.type_name) else {
                found.push(Issue {
                    severity: Severity::Warning,
                    rule: "ontology.schema.required_extension_field",
                    sub_ontology: Some(req.sub_ontology.clone()),
                    path: Some(req.schema_path.clone()),
                    message: format!(
                        "`{fields}` is required, but no git-backed sub-ontology owns {t}, so check cannot tell whether `{sub}` has a record for every {t}; any {t} without one fails at query time. Make the field nullable, or keep a record for every {t}",
                        t = req.type_name,
                        sub = req.sub_ontology,
                    ),
                });
                continue;
            };
            let stored = self
                .stored
                .get(&req.type_name)
                .and_then(|by_sub| by_sub.get(&req.sub_ontology));
            let missing: Vec<&str> = keys
                .into_iter()
                .filter(|k| !stored.is_some_and(|s| s.contains(*k)))
                .collect();
            if missing.is_empty() {
                continue;
            }
            let shown = missing
                .iter()
                .take(5)
                .copied()
                .collect::<Vec<_>>()
                .join(", ");
            let more = missing
                .len()
                .checked_sub(5)
                .filter(|n| *n > 0)
                .map(|n| format!(", and {n} more"))
                .unwrap_or_default();
            found.push(Issue {
                severity: Severity::Error,
                rule: "ontology.record.missing",
                sub_ontology: Some(req.sub_ontology.clone()),
                path: Some(req.dir.clone()),
                message: format!(
                    "`{fields}` is required, but `{}` has no record for {} {} ({shown}{more}); give each one a record, or make the field nullable",
                    req.sub_ontology,
                    missing.len(),
                    req.type_name,
                ),
            });
        }
        self.issues.extend(found);
    }

    /// A field taken over with `@override` is read from the new sub-ontology
    /// only, so for every entity either side has a record of, it must answer
    /// what the old one does, or the gateway's answer changes.
    fn check_override_coverage(&mut self) {
        let found: Vec<Issue> = self
            .moves
            .iter()
            .flat_map(|m| self.override_issues(m))
            .collect();
        self.issues.extend(found);
    }

    fn override_issues(&self, m: &Move) -> Vec<Issue> {
        let what = format!(
            "`{}` takes `{}.{}` over from `{}` (@override)",
            m.to, m.type_name, m.field, m.from
        );
        let issue = |severity, message: String| Issue {
            severity,
            rule: "ontology.record.override_gap",
            sub_ontology: Some(m.to.clone()),
            path: Some(m.to_schema.clone()),
            message: format!("{what}{message}"),
        };
        if let Some(reason) = &m.unverifiable {
            return vec![issue(Severity::Error, format!(", but {reason}"))];
        }
        let none = BTreeMap::new();
        let by_sub = self
            .moved_values
            .get(&(m.type_name.clone(), m.field.clone()));
        let values_of = |sub: &str| by_sub.and_then(|b| b.get(sub)).unwrap_or(&none);
        let (source, moved) = (values_of(&m.from), values_of(&m.to));
        let absent = comparable(&m.absent(), m.relation);
        // An override check cannot verify blocks publication, unless neither
        // side is part of this repository (two remotes moving a field between
        // them is their business).
        let unverified = if m.from_access == Access::Remote && m.to_access == Access::Remote {
            Severity::Warning
        } else {
            Severity::Error
        };
        if m.from_access != Access::Read {
            return vec![issue(
                unverified,
                format!(
                    "; check cannot read `{}`'s values, so it cannot confirm the answers stay the same",
                    m.from
                ),
            )];
        }
        if m.to_access != Access::Read {
            // Even a source without values answers null, which `to` may
            // not: only reading `to` would confirm the answers stay the same.
            let valued = source.values().filter(|v| **v != absent).count();
            let detail = if valued > 0 {
                format!(
                    ", where {valued} {} still hold a value; check cannot confirm `{}` has them",
                    m.type_name, m.to
                )
            } else {
                format!(
                    "; check cannot read `{}`'s values, so it cannot confirm the answers stay the same",
                    m.to
                )
            };
            return vec![issue(unverified, detail)];
        }
        let (mut missing, mut differing) = (Vec::new(), Vec::new());
        // Every key either side has a record of: no record answers as if the
        // field were absent.
        let keys: BTreeSet<&String> = source.keys().chain(moved.keys()).collect();
        for key in keys {
            let old = source.get(key).unwrap_or(&absent);
            match moved.get(key) {
                None if *old != absent => missing.push(key.as_str()),
                Some(new) if new != old => differing.push(key.as_str()),
                None | Some(_) => {}
            }
        }
        let mut out = Vec::new();
        if !missing.is_empty() {
            out.push(issue(
                Severity::Error,
                format!(
                    ", but has no value for {} {} that `{}` still stores ({}); the gateway would answer null for them. Copy the values to `{}`, or remove them from `{}`",
                    missing.len(),
                    m.type_name,
                    m.from,
                    list_keys(&missing),
                    m.to,
                    m.from
                ),
            ));
        }
        if !differing.is_empty() {
            out.push(issue(
                Severity::Error,
                format!(
                    ", but holds a different value than `{}` for {} {} ({}); the gateway would answer `{}`'s. Make them match, or remove the values from `{}`",
                    m.from,
                    differing.len(),
                    m.type_name,
                    list_keys(&differing),
                    m.to,
                    m.from
                ),
            ));
        }
        out
    }

    fn check_references(&mut self) {
        let mut found = Vec::new();
        for r in &self.references {
            // A git-backed owner's records are every entity of its type, so
            // a key an owner lacks is a broken reference; an extension's
            // record for that key does not help, since the owner still
            // answers `null`. Types owned elsewhere (remote, a database) can
            // only be checked when some extension stores records for them,
            // and then only as a warning.
            let (severity, message) =
                if let Some(owner) = self.missing_owner(&r.target_type, &r.key) {
                    (
                        Severity::Error,
                        format!(
                        "`{}` references {}:{}, but `{owner}` (which owns {}) has no such record",
                        r.at, r.target_type, r.key, r.target_type
                    ),
                    )
                } else if self.owners.contains_key(&r.target_type) {
                    continue;
                } else {
                    match self.stored.get(&r.target_type) {
                        Some(by_sub) if !by_sub.values().any(|keys| keys.contains(&r.key)) => (
                            Severity::Warning,
                            format!(
                                "`{}` references {}:{}, which no git-backed sub-ontology stores",
                                r.at, r.target_type, r.key
                            ),
                        ),
                        _ => continue,
                    }
                };
            found.push(Issue {
                severity,
                rule: "ontology.record.dangling_ref",
                sub_ontology: Some(r.sub_ontology.clone()),
                path: Some(r.path.clone()),
                message,
            });
        }
        // A record (an extension's, or a co-owner's) for a key an owner
        // lacks describes an entity that does not exist.
        for e in &self.entries {
            if let Some(owner) = self.missing_owner(&e.type_name, &e.key) {
                found.push(Issue {
                    severity: Severity::Error,
                    rule: "ontology.record.orphan",
                    sub_ontology: Some(e.sub_ontology.clone()),
                    path: Some(e.path.clone()),
                    message: format!(
                        "`{}` stores {}:{}, but `{owner}` (which owns {}) has no such record, so that entity does not exist",
                        e.sub_ontology, e.type_name, e.key, e.type_name
                    ),
                });
            }
        }
        self.issues.extend(found);
    }
}

/// A sub-ontology that resolves an entity field.
struct Resolver {
    sub: String,
    local: bool,
    schema_path: String,
    /// The field is this sub-ontology's key for the type.
    is_key: bool,
}

/// A non-key entity field may be resolved by one sub-ontology only (not
/// `@external`, and not overridden away by `@override(from:)` without a
/// label). Stores cannot be kept in agreement, and the gateway may read a
/// field from any sub-ontology that resolves it, so two local resolvers
/// are an error; a local one beside a remote one, whose values `check`
/// cannot see, is a warning.
/// A root field names no entity whose answers could be compared, so it is
/// resolved by one sub-ontology, and cannot move with `@override` unless
/// neither side is local (two remotes moving one is their business).
/// GraphQL gives descriptions to definitions only: `"…" extend type X` is
/// not GraphQL. graphql-composition and `serve` read it, but Apollo's
/// composition and other strict parsers reject the whole schema, and each
/// subgraph serves its schema verbatim (`_service`), so a gateway that
/// composes from the running subgraphs could not.
fn check_extension_descriptions(registry: &Registry, acc: &mut Accumulator) {
    use cynic_parser::type_system::Definition;
    for sub in &registry.sub_ontologies {
        let Ok(doc) = cynic_parser::parse_type_system_document(&sub.sdl) else {
            continue;
        };
        for definition in doc.definitions() {
            let extended = match definition {
                Definition::TypeExtension(ty) if ty.description().is_some() => {
                    format!("type {}", ty.name())
                }
                Definition::SchemaExtension(schema) if schema.description().is_some() => {
                    "schema".to_string()
                }
                _ => continue,
            };
            acc.push(
                Severity::Warning,
                "ontology.schema.extension_description",
                Some(&sub.name),
                Some(sub.schema_path()),
                format!(
                    "the description on `extend {extended}` is not GraphQL (only definitions take one), so strict parsers such as Apollo's composition reject this schema; make it a `#` comment"
                ),
            );
        }
    }
}

fn check_root_fields(registry: &Registry, acc: &mut Accumulator) {
    let is_remote = |name: &str| {
        registry
            .get(name)
            .is_some_and(|s| matches!(s.source, SubOntologySource::Remote(_)))
    };
    // Keyed by field name alone: composition merges every subgraph's query
    // root into one `Query`, whatever each calls it.
    let mut resolvers: BTreeMap<String, Vec<(&SubOntology, bool)>> = BTreeMap::new();
    let mut moved = BTreeSet::new();
    for sub in &registry.sub_ontologies {
        let Ok(model) = SubOntologyModel::parse(&sub.sdl) else {
            continue;
        };
        let Some(root) = model.query_type.as_deref().and_then(|q| model.object(q)) else {
            continue;
        };
        let remote = is_remote(&sub.name);
        for field in &root.fields {
            let id = field.name.clone();
            resolvers
                .entry(id.clone())
                .or_default()
                .push((sub, !remote));
            let Some(from) = &field.override_from else {
                continue;
            };
            moved.insert(id);
            acc.push(
                if remote && is_remote(from) {
                    Severity::Warning
                } else {
                    Severity::Error
                },
                "ontology.record.override_gap",
                Some(&sub.name),
                Some(sub.schema_path()),
                format!(
                    "`{}` takes root field `{}.{}` over from `{from}` (@override), but a root field names no entity to compare answers by, so check cannot confirm they stay the same; keep it in one sub-ontology",
                    sub.name, root.name, field.name
                ),
            );
        }
    }
    for (field, subs) in resolvers {
        if moved.contains(&field) {
            continue;
        }
        let names = |local: bool| {
            subs.iter()
                .filter(|(_, l)| *l == local)
                .map(|(s, _)| format!("`{}`", s.name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let locals: Vec<&SubOntology> = subs.iter().filter(|(_, l)| *l).map(|(s, _)| *s).collect();
        let (severity, message) = match (locals.len(), subs.len() - locals.len()) {
            (0, _) | (1, 0) => continue,
            (1, _) => (
                Severity::Warning,
                format!(
                    "root field `Query.{field}` is resolved by {} and by remote {}; the gateway may ask either, and check cannot see what the remote answers",
                    names(true),
                    names(false)
                ),
            ),
            _ => (
                Severity::Error,
                format!(
                    "root field `Query.{field}` is resolved by {}; the gateway may ask any of them, and their answers differ with their stores. Keep it in one",
                    names(true)
                ),
            ),
        };
        acc.push(
            severity,
            "ontology.schema.shared_field",
            Some(&locals[0].name),
            Some(locals[0].schema_path()),
            message,
        );
    }
}

fn check_single_resolvers(registry: &Registry, acc: &mut Accumulator) {
    let mut resolvers: BTreeMap<(String, String), Vec<Resolver>> = BTreeMap::new();
    // (type, field, sub-ontology) that an unlabelled `@override` takes away.
    let mut overridden = BTreeSet::new();
    // Entity type → key field → the local sub-ontologies keying it so.
    let mut keyed: BTreeMap<String, BTreeMap<String, Vec<String>>> = BTreeMap::new();
    for sub in &registry.sub_ontologies {
        let Ok(model) = SubOntologyModel::parse(&sub.sdl) else {
            continue;
        };
        let local = matches!(sub.source, SubOntologySource::Local(_));
        for entity in model.entities() {
            let key = entity.key_field();
            if let (true, Some(key)) = (local, key) {
                keyed
                    .entry(entity.name.clone())
                    .or_default()
                    .entry(key.to_string())
                    .or_default()
                    .push(sub.name.clone());
            }
            for field in &entity.fields {
                if field.external {
                    continue;
                }
                resolvers
                    .entry((entity.name.clone(), field.name.clone()))
                    .or_default()
                    .push(Resolver {
                        sub: sub.name.clone(),
                        local,
                        schema_path: sub.schema_path(),
                        is_key: Some(field.name.as_str()) == key,
                    });
                if let (Some(from), None) = (&field.override_from, &field.override_label) {
                    overridden.insert((entity.name.clone(), field.name.clone(), from.clone()));
                }
            }
        }
    }
    let names = |list: &[&Resolver]| {
        list.iter()
            .map(|r| format!("`{}`", r.sub))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // Records are matched across sub-ontologies by key, so local ones must
    // agree on the key field.
    for (type_name, keys) in &keyed {
        if keys.len() < 2 {
            continue;
        }
        let described = keys
            .iter()
            .map(|(key, subs)| format!("`{key}` in {}", subs.join(", ")))
            .collect::<Vec<_>>()
            .join("; ");
        acc.push(
            Severity::Error,
            "ontology.schema.key_mismatch",
            None,
            None,
            format!(
                "`{type_name}` is keyed by different fields ({described}); local sub-ontologies must key an entity by the same field, since their records are matched by key"
            ),
        );
    }
    for ((type_name, field), all) in resolvers {
        // A key everywhere names the entity, and is the same in each.
        if all.iter().all(|r| r.is_key) {
            continue;
        }
        let (locals, remotes): (Vec<&Resolver>, Vec<&Resolver>) = all
            .iter()
            .filter(|r| !overridden.contains(&(type_name.clone(), field.clone(), r.sub.clone())))
            .partition(|r| r.local);
        let (severity, message) = match (locals.len(), remotes.len()) {
            (0, _) | (1, 0) => continue,
            (1, _) => (
                Severity::Warning,
                format!(
                    "`{type_name}.{field}` is resolved by {} and by remote {}; the gateway may read it from either, and check cannot see the remote's values",
                    names(&locals),
                    names(&remotes)
                ),
            ),
            _ => (
                Severity::Error,
                format!(
                    "`{type_name}.{field}` is resolved by {}; the gateway may read it from any of them and their stores cannot be kept in agreement. Keep it in one (mark it `@external` elsewhere), or move it with `@override(from: …)`",
                    names(&locals)
                ),
            ),
        };
        acc.push(
            severity,
            "ontology.schema.shared_field",
            Some(&locals[0].sub),
            Some(locals[0].schema_path.clone()),
            message,
        );
    }
}

/// `value` in the form two sub-ontologies' copies are compared in:
/// relation keys as strings, numbers by value (`1` = `1.0`).
fn comparable(value: &Value, relation: bool) -> Value {
    match value {
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| comparable(v, relation)).collect())
        }
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(k, v)| (k.clone(), comparable(v, false)))
                .collect(),
        ),
        Value::Number(n) if relation => Value::String(n.to_string()),
        Value::Number(n) => match (n.as_i64(), n.as_f64()) {
            (Some(i), _) => Value::from(i),
            #[allow(clippy::cast_possible_truncation, clippy::cast_precision_loss)]
            (None, Some(f)) if f.fract() == 0.0 && f.abs() < 9.0e15 => Value::from(f as i64),
            _ => value.clone(),
        },
        other => other.clone(),
    }
}

/// Up to five keys, and how many more.
fn list_keys(keys: &[&str]) -> String {
    let shown = keys.iter().take(5).copied().collect::<Vec<_>>().join(", ");
    match keys.len().checked_sub(5) {
        Some(more) if more > 0 => format!("{shown}, and {more} more"),
        _ => shown,
    }
}

/// Where a value is being validated, for messages and references.
struct Site<'a> {
    sub: &'a str,
    path: &'a str,
    /// Human-edited notes (markdown) carry keys the schema does not know
    /// about (Obsidian's `tags`, `aliases`, …): unknown fields are ignored and
    /// fields owned elsewhere only warn. Git records stay strict.
    lenient: bool,
}

/// Whether `data_dir` can hold records, reporting why not: a symlink (git
/// reads do not follow one, so its records would read as none) or a file on
/// the way. A data directory that does not exist holds no records.
fn data_root_is_readable(
    snapshot: &dyn Snapshot,
    sub: &SubOntology,
    data_dir: &str,
    acc: &mut Accumulator,
) -> Result<bool> {
    let Some(problem) = crate::snapshot::not_a_directory(snapshot, &sub.dir, data_dir, "records")?
    else {
        return Ok(true);
    };
    acc.push(
        Severity::Error,
        "ontology.storage",
        Some(&sub.name),
        Some(join(&sub.dir, data_dir)),
        problem,
    );
    Ok(false)
}

fn validate_git_records(
    snapshot: &dyn Snapshot,
    sub: &SubOntology,
    data_dir: &str,
    model: &SubOntologyModel,
    acc: &mut Accumulator,
) -> Result<()> {
    let root = join(&sub.dir, data_dir);
    if !data_root_is_readable(snapshot, sub, data_dir, acc)? {
        return Ok(());
    }
    let suffix = format!(".{RECORD_EXT}");
    for type_entry in snapshot.list(&root)? {
        let type_path = join(&root, &type_entry.name);
        if !type_entry.is_dir {
            acc.push(
                Severity::Error,
                "ontology.record.layout",
                Some(&sub.name),
                Some(type_path),
                format!("records live at {data_dir}/<Type>/<key>{suffix}; this file is not inside a type directory"),
            );
            continue;
        }
        let type_name = type_entry.name.as_str();
        let entity = model
            .entities()
            .find(|o| o.name == type_name)
            .and_then(|o| o.key_field().map(|k| (o, k)));
        let Some((object, key_field)) = entity else {
            let reason = match model.object(type_name) {
                None => "is not a type in this sub-ontology's schema",
                Some(o) if o.key.is_none() => {
                    "has no @key, so it is a value type and cannot be stored on its own"
                }
                Some(_) => "has no single-field @key (compound keys cannot name a file yet)",
            };
            acc.push(
                Severity::Error,
                "ontology.record.unknown_type",
                Some(&sub.name),
                Some(type_path),
                format!("`{type_name}` {reason}"),
            );
            continue;
        };

        for file in snapshot.list(&type_path)? {
            let path = join(&type_path, &file.name);
            let key = file.name.strip_suffix(&suffix).filter(|_| !file.is_dir);
            let Some(key) = key else {
                acc.push(
                    Severity::Error,
                    "ontology.record.layout",
                    Some(&sub.name),
                    Some(path),
                    format!("expected a `<key>{suffix}` record file"),
                );
                continue;
            };
            acc.records += 1;
            if let Some(message) = key_problem(object, key_field, key) {
                acc.push(
                    Severity::Error,
                    "ontology.record.key",
                    Some(&sub.name),
                    Some(path),
                    message,
                );
                continue;
            }
            let Some(bytes) = snapshot.read(&path)? else {
                // Listed but not a file: a git link (submodule), which the
                // store cannot read either.
                acc.push(
                    Severity::Error,
                    "ontology.record.parse",
                    Some(&sub.name),
                    Some(path),
                    "is not a file (a git link or submodule?); a record is a YAML file".to_string(),
                );
                continue;
            };
            let record = match parse_record(&bytes) {
                Ok(record) => record,
                Err(message) => {
                    acc.push(
                        Severity::Error,
                        "ontology.record.parse",
                        Some(&sub.name),
                        Some(path),
                        message,
                    );
                    continue;
                }
            };
            acc.store(&sub.name, type_name, key, &path, &record);
            let site = Site {
                sub: &sub.name,
                path: &path,
                lenient: false,
            };
            validate_entity(model, object, key_field, key, &record, &site, acc);
        }
    }
    Ok(())
}

/// `body_field` and `links_field` name fields the store fills: a stored
/// field of the notes' type (of some entity, with `type_field`), of the
/// kind it receives. Otherwise the content lands under a name the schema
/// never asks for, and nothing says so.
fn filled_field_problems(config: &MarkdownConfig, model: &SubOntologyModel) -> Vec<String> {
    let types: Vec<&ObjectType> = model
        .entities()
        .filter(|o| config.type_name.as_ref().is_none_or(|t| *t == o.name))
        .collect();
    if types.is_empty() {
        // A `type` the schema lacks: the store refuses it, once.
        return Vec::new();
    }
    let notes = config
        .type_name
        .as_ref()
        .map_or_else(|| "entity".to_string(), |t| format!("`{t}`"));
    let settings: [(&str, Option<&String>, &str); 2] = [
        ("body_field", config.body_field.as_ref(), "a `String`"),
        (
            "links_field",
            config.links_field.as_ref(),
            "a list of entities, like `[Topic!]!`",
        ),
    ];
    let mut problems = Vec::new();
    for (setting, field, expected) in settings {
        let Some(field) = field else {
            continue;
        };
        let declared: Vec<(&ObjectType, &FieldDef)> = types
            .iter()
            .filter_map(|o| o.field(field).map(|f| (*o, f)))
            .collect();
        if declared.is_empty() {
            problems.push(format!(
                "`{setting} = \"{field}\"`, but no {notes} declares `{field}`: the store would fill a field nothing reads"
            ));
        }
        for (object, f) in declared {
            // The store fills the key last, over whatever else went there.
            if object.key_field() == Some(field.as_str()) {
                problems.push(format!(
                    "`{setting} = \"{field}\"` names `{}`'s key, which the store fills with each note's key: what it receives would be lost",
                    object.name
                ));
                continue;
            }
            let fits = f.is_stored()
                && if setting == "body_field" {
                    f.ty.named == "String" && !f.ty.is_list()
                } else {
                    f.ty.is_list() && matches!(model.kind_of(&f.ty.named), TypeKind::Entity(_))
                };
            if !fits {
                problems.push(format!(
                    "`{setting} = \"{field}\"` fills `{}.{field}`, which must be a stored field of {expected}",
                    object.name
                ));
            }
        }
    }
    problems
}

fn validate_markdown_notes(
    snapshot: &dyn Snapshot,
    sub: &SubOntology,
    config: &MarkdownConfig,
    model: &SubOntologyModel,
    acc: &mut Accumulator,
) {
    for problem in filled_field_problems(config, model) {
        acc.push(
            Severity::Error,
            "ontology.storage",
            Some(&sub.name),
            Some(join(&sub.dir, crate::layout::SUBGRAPH_MANIFEST)),
            problem,
        );
    }
    let schema = crate::store::markdown::NoteTypes::of(model, config);
    let (notes, problems) = match load_notes(snapshot, sub, config, schema) {
        Ok(loaded) => loaded,
        Err(e) => {
            acc.push(
                Severity::Error,
                "ontology.storage",
                Some(&sub.name),
                Some(join(&sub.dir, crate::layout::SUBGRAPH_MANIFEST)),
                e.to_string(),
            );
            return;
        }
    };
    if notes.is_empty() && problems.is_empty() {
        // Only a warning: git keeps no empty directories, so a sub-ontology
        // with no notes yet looks just like a misspelled `dir`.
        let dir = crate::layout::resolve_within(&sub.dir, &config.dir)
            .unwrap_or_else(|| config.dir.clone());
        acc.push(
            Severity::Warning,
            "ontology.storage",
            Some(&sub.name),
            Some(dir.clone()),
            format!(
                "no notes under `{dir}`, so none of this sub-ontology's entities are served; check `dir` if there should be"
            ),
        );
    }
    for problem in problems {
        acc.push(
            Severity::Error,
            "ontology.record.parse",
            Some(&sub.name),
            Some(problem.path),
            problem.message,
        );
    }
    for note in notes {
        acc.records += 1;
        let entity = model
            .entities()
            .find(|o| o.name == note.type_name)
            .and_then(|o| o.key_field().map(|k| (o, k)));
        let Some((object, key_field)) = entity else {
            acc.push(
                Severity::Error,
                "ontology.record.unknown_type",
                Some(&sub.name),
                Some(note.path.clone()),
                format!(
                    "`{}` is not an entity with a single-field @key in this sub-ontology",
                    note.type_name
                ),
            );
            continue;
        };
        if let Some(message) = int_key_problem(object, key_field, &note.key) {
            acc.push(
                Severity::Error,
                "ontology.record.key",
                Some(&sub.name),
                Some(note.path.clone()),
                message,
            );
            continue;
        }
        lost_filled_fields(sub, config, model, object, &note, acc);
        acc.store(
            &sub.name,
            &note.type_name,
            &note.key,
            &note.path,
            &note.record,
        );
        let site = Site {
            sub: &sub.name,
            path: &note.path,
            lenient: true,
        };
        validate_fields(
            model,
            object,
            Some(key_field),
            &note.record,
            &object.name,
            &site,
            acc,
        );
    }
}

/// With `type_field`, a type may lack a field the store fills: a note of
/// it with a body or links would lose them unannounced. (A field no type
/// declares is reported once, by [`filled_field_problems`].)
fn lost_filled_fields(
    sub: &SubOntology,
    config: &MarkdownConfig,
    model: &SubOntologyModel,
    object: &ObjectType,
    note: &crate::store::markdown::Note,
    acc: &mut Accumulator,
) {
    for (field, what) in [(&config.body_field, "body"), (&config.links_field, "links")] {
        let Some(field) = field.as_ref().filter(|f| {
            object.field(f).is_none() && model.entities().any(|o| o.field(f).is_some())
        }) else {
            continue;
        };
        let lost = match note.record.get(field) {
            Some(Value::String(text)) => !text.trim().is_empty(),
            Some(Value::Array(items)) => !items.is_empty(),
            _ => false,
        };
        if lost {
            acc.push(
                Severity::Error,
                "ontology.storage",
                Some(&sub.name),
                Some(note.path.clone()),
                format!(
                    "this `{}` note has {what}, but `{}` declares no `{field}` to serve it in; declare it, or move the {what} out of the note",
                    note.type_name, note.type_name
                ),
            );
        }
    }
}

/// For an `Int` key field, `key` must be a 32-bit integer in canonical form
/// (`042` parses, but lookups by 42 would not find it).
fn int_key_problem(object: &ObjectType, key_field: &str, key: &str) -> Option<String> {
    let key_is_int = object.field(key_field).is_some_and(|f| f.ty.named == "Int");
    let canonical_int = key.parse::<i32>().is_ok_and(|n| n.to_string() == key);
    (key_is_int && !canonical_int).then(|| {
        format!(
            "`{}.{key_field}` is an Int, so the key `{key}` must be a 32-bit integer in canonical form (no leading zeros)",
            object.name
        )
    })
}

/// Why `key` (a record's file name) cannot name an entity of `object`.
fn key_problem(object: &ObjectType, key_field: &str, key: &str) -> Option<String> {
    if let Err(message) = validate_record_key(key) {
        return Some(message);
    }
    int_key_problem(object, key_field, key)
}

fn validate_entity(
    model: &SubOntologyModel,
    object: &ObjectType,
    key_field: &str,
    key: &str,
    record: &serde_json::Map<String, Value>,
    site: &Site<'_>,
    acc: &mut Accumulator,
) {
    if let Some(stored) = record.get(key_field) {
        let matches = match stored {
            Value::String(s) => s == key,
            Value::Number(n) => n.to_string() == key,
            _ => false,
        };
        if !matches {
            acc.push(
                Severity::Error,
                "ontology.record.key",
                Some(site.sub),
                Some(site.path.to_string()),
                format!("`{key_field}: {stored}` does not match the file name key `{key}`"),
            );
        }
        // Spelled right is not enough: a String key stored as 42 is a number.
        if let Some(field) = object.field(key_field) {
            if let Err(expected) = check_scalar(&field.ty.named, stored) {
                acc.push(
                    Severity::Error,
                    "ontology.record.type",
                    Some(site.sub),
                    Some(site.path.to_string()),
                    format!(
                        "`{key_field}: {stored}` must be {expected} (`{}`)",
                        field.ty
                    ),
                );
            }
        }
    }
    validate_fields(
        model,
        object,
        Some(key_field),
        record,
        &object.name,
        site,
        acc,
    );
}

/// Validate the fields of an entity record or an inline value object.
fn validate_fields(
    model: &SubOntologyModel,
    object: &ObjectType,
    key_field: Option<&str>,
    record: &serde_json::Map<String, Value>,
    at: &str,
    site: &Site<'_>,
    acc: &mut Accumulator,
) {
    let error = |acc: &mut Accumulator, rule: &'static str, message: String| {
        acc.push(
            Severity::Error,
            rule,
            Some(site.sub),
            Some(site.path.to_string()),
            message,
        );
    };
    for (name, value) in record {
        if Some(name.as_str()) == key_field {
            continue;
        }
        let Some(field) = object.field(name) else {
            if site.lenient {
                continue;
            }
            error(
                acc,
                "ontology.record.unknown_field",
                format!(
                    "`{at}.{name}` is not a field of `{}` in sub-ontology `{}`",
                    object.name, site.sub
                ),
            );
            continue;
        };
        if field.external || field.inverse_of.is_some() {
            let (rule, why) = if field.external {
                (
                    "ontology.record.not_owned",
                    "is @external here: another sub-ontology owns it".to_string(),
                )
            } else {
                (
                    "ontology.record.derived",
                    format!(
                        "is derived (@inverse(of: \"{}\"))",
                        field.inverse_of.as_deref().unwrap_or_default()
                    ),
                )
            };
            let severity = if site.lenient {
                Severity::Warning
            } else {
                Severity::Error
            };
            let advice = if site.lenient {
                "so this value is ignored"
            } else {
                "so it must not be stored here"
            };
            acc.push(
                severity,
                rule,
                Some(site.sub),
                Some(site.path.to_string()),
                format!("`{at}.{name}` {why}, {advice}"),
            );
            continue;
        }
        if !field.args.is_empty() {
            error(
                acc,
                "ontology.record.not_storable",
                format!("`{at}.{name}` takes arguments, so it cannot be a stored value"),
            );
            continue;
        }
        validate_value(model, &field.ty, value, &format!("{at}.{name}"), site, acc);
    }
    for field in &object.fields {
        let exempt =
            Some(field.name.as_str()) == key_field || !field.is_stored() || !field.args.is_empty();
        if !exempt && field.ty.non_null && !field.ty.is_list() && !record.contains_key(&field.name)
        {
            error(
                acc,
                "ontology.record.missing_field",
                format!(
                    "required field `{at}.{}: {}` is missing",
                    field.name, field.ty
                ),
            );
        }
    }
}

fn validate_value(
    model: &SubOntologyModel,
    ty: &TypeExpr,
    value: &Value,
    at: &str,
    site: &Site<'_>,
    acc: &mut Accumulator,
) {
    let type_error = |acc: &mut Accumulator, message: String| {
        acc.push(
            Severity::Error,
            "ontology.record.type",
            Some(site.sub),
            Some(site.path.to_string()),
            message,
        );
    };
    if value.is_null() {
        if ty.non_null {
            type_error(acc, format!("`{at}` is null but declared `{ty}`"));
        }
        return;
    }
    if let Some(item_non_null) = ty.list {
        validate_list(model, ty, item_non_null, value, at, site, acc);
        return;
    }
    match model.kind_of(&ty.named) {
        TypeKind::Scalar => {
            if let Err(expected) = check_scalar(&ty.named, value) {
                type_error(
                    acc,
                    format!(
                        "`{at}` must be {expected} (`{ty}`), got {}",
                        json_kind(value)
                    ),
                );
            }
        }
        TypeKind::Enum(values) => {
            if !value
                .as_str()
                .is_some_and(|s| values.iter().any(|v| v == s))
            {
                type_error(
                    acc,
                    format!("`{at}` must be one of {values:?} (`{ty}`), got {value}"),
                );
            }
        }
        TypeKind::Entity(target) => {
            let key_type = target
                .key_field()
                .and_then(|k| target.field(k))
                .map(|f| f.ty.named.as_str());
            match relation_key(value, key_type) {
                Ok(key) => acc.references.push(Reference {
                    sub_ontology: site.sub.to_string(),
                    path: site.path.to_string(),
                    at: at.to_string(),
                    target_type: target.name.clone(),
                    key,
                }),
                Err(expected) => type_error(
                    acc,
                    format!(
                        "`{at}` is a relation to `{}`: store the target's key, {expected}; got {value}",
                        target.name
                    ),
                ),
            }
        }
        TypeKind::Value(object) => match value {
            Value::Object(inline) => validate_fields(model, object, None, inline, at, site, acc),
            _ => type_error(
                acc,
                format!(
                    "`{at}` is a `{}` value object: expected a mapping, got {}",
                    object.name,
                    json_kind(value)
                ),
            ),
        },
        TypeKind::Unknown => type_error(
            acc,
            format!(
                "`{at}` has type `{}`, which this sub-ontology does not define",
                ty.named
            ),
        ),
    }
}

/// A list value: a list, of at most what one list answers, each item valid.
fn validate_list(
    model: &SubOntologyModel,
    ty: &TypeExpr,
    item_non_null: bool,
    value: &Value,
    at: &str,
    site: &Site<'_>,
    acc: &mut Accumulator,
) {
    let type_error = |acc: &mut Accumulator, message: String| {
        acc.push(
            Severity::Error,
            "ontology.record.type",
            Some(site.sub),
            Some(site.path.to_string()),
            message,
        );
    };
    let Value::Array(items) = value else {
        type_error(
            acc,
            format!("`{at}` must be a list (`{ty}`), got {}", json_kind(value)),
        );
        return;
    };
    let most = crate::serve::schema::MAX_LIST_LIMIT;
    if items.len() > most {
        type_error(
            acc,
            format!(
                "`{at}` holds {} items; a stored list may hold at most {most}, the most one list answers",
                items.len()
            ),
        );
        return;
    }
    let item_ty = TypeExpr {
        named: ty.named.clone(),
        non_null: item_non_null,
        list: None,
    };
    for (i, item) in items.iter().enumerate() {
        validate_value(model, &item_ty, item, &format!("{at}[{i}]"), site, acc);
    }
}

/// The key a relation value names, or `Err(expected)` when it is not a
/// literal of the target's key scalar: the gateway hands it to the target's
/// owner as one (an `Int` key in canonical form, as record file names are).
fn relation_key(
    value: &Value,
    key_type: Option<&str>,
) -> std::result::Result<String, &'static str> {
    match (key_type, value) {
        (Some("Int"), Value::Number(n)) if n.as_i64().is_some_and(|n| i32::try_from(n).is_ok()) => {
            Ok(n.to_string())
        }
        (Some("Int"), Value::String(s)) if s.parse::<i32>().is_ok_and(|n| n.to_string() == *s) => {
            Ok(s.clone())
        }
        (Some("Int"), _) => Err("a 32-bit integer in canonical form"),
        (_, Value::String(s)) => Ok(s.clone()),
        (_, Value::Number(n)) if n.is_i64() || n.is_u64() => Ok(n.to_string()),
        _ => Err("a string or an integer"),
    }
}

/// `Err(expected)` when `value` is not a valid literal for scalar `named`.
fn check_scalar(named: &str, value: &Value) -> std::result::Result<(), &'static str> {
    let ok = match named {
        "ID" => value.is_string() || value.is_i64() || value.is_u64(),
        "String" => value.is_string(),
        "Int" => value.as_i64().is_some_and(|n| i32::try_from(n).is_ok()),
        "Float" => value.is_number(),
        "Boolean" => value.is_boolean(),
        // Custom scalars accept any literal.
        _ => true,
    };
    if ok {
        return Ok(());
    }
    Err(match named {
        "ID" => "a string or integer",
        "String" => "a string",
        "Int" => "a 32-bit integer",
        "Float" => "a number",
        _ => "a boolean",
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{test_support::commit_files, GitSnapshot};

    const GIT: &str = "[storage]\nkind = \"git\"\n";
    const CORE: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Person @key(fields: "id") { id: ID! name: String! }
type Query { person(id: ID!): Person }
"#;
    const RESEARCH: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key", "@external", "@shareable"])
directive @inverse(of: String!) on FIELD_DEFINITION
enum Venue { CONFERENCE JOURNAL }
type Paper @key(fields: "id") {
  id: ID!
  title: String!
  year: Int
  venue: Venue
  authors: [Person!]!
  pages: PageRange
}
type PageRange @shareable { first: Int! last: Int! }
extend type Person @key(fields: "id") {
  id: ID!
  papers: [Paper!]! @inverse(of: "authors")
}
type Query { paper(id: ID!): Paper }
"#;

    fn base_files() -> Vec<(&'static str, &'static str)> {
        vec![
            ("ontology/core/subgraph.toml", GIT),
            ("ontology/core/schema.graphql", CORE),
            ("ontology/core/data/Person/vaswani.yaml", "name: Ashish Vaswani\n"),
            ("ontology/research/subgraph.toml", GIT),
            ("ontology/research/schema.graphql", RESEARCH),
            (
                "ontology/research/data/Paper/attention.yaml",
                "title: Attention Is All You Need\nyear: 2017\nvenue: CONFERENCE\nauthors: [vaswani]\npages: { first: 1, last: 11 }\n",
            ),
        ]
    }

    fn run(extra: &[(&str, &str)]) -> CheckReport {
        let dir = tempfile::tempdir().unwrap();
        let mut files = base_files();
        files.extend_from_slice(extra);
        commit_files(dir.path(), &files, "init");
        let snap = GitSnapshot::open(dir.path(), "HEAD").unwrap();
        check(&snap, "ontology").unwrap()
    }

    fn rules(report: &CheckReport) -> Vec<&'static str> {
        report.issues.iter().map(|i| i.rule).collect()
    }

    #[test]
    fn clean_ontology_has_no_issues() {
        let report = run(&[]);
        assert!(report.issues.is_empty(), "{:#?}", report.issues);
        assert_eq!(report.sub_ontologies, 2);
        assert_eq!(report.records, 2);
        assert!(report.composition.is_ok());
    }

    #[test]
    fn descriptions_on_extensions_are_warned_about() {
        let research = RESEARCH.replace(
            "extend type Person",
            "\"research adds papers to core's Person.\"\nextend type Person",
        );
        let report = run_with_research(&research);
        assert_eq!(
            rules(&report),
            ["ontology.schema.extension_description"],
            "{:#?}",
            report.issues
        );
        let issue = &report.issues[0];
        assert_eq!(issue.severity, Severity::Warning);
        assert!(
            issue.message.contains("`extend type Person`"),
            "{}",
            issue.message
        );
        // A comment there is fine, and so is a description on a definition.
        let commented = RESEARCH.replace(
            "extend type Person",
            "# research adds papers to core's Person.\nextend type Person",
        );
        let report = run_with_research(&commented);
        assert!(report.issues.is_empty(), "{:#?}", report.issues);
    }

    #[test]
    fn ownership_and_derivation_are_enforced() {
        let report = run(&[
            // research does not own Person.name, and papers is derived
            (
                "ontology/research/data/Person/vaswani.yaml",
                "name: X\npapers: [attention]\n",
            ),
        ]);
        let mut got = rules(&report);
        got.sort_unstable();
        assert_eq!(
            got,
            ["ontology.record.derived", "ontology.record.unknown_field"]
        );
    }

    #[test]
    fn external_fields_cannot_be_stored() {
        let research = RESEARCH.replace(
            "papers: [Paper!]! @inverse(of: \"authors\")",
            "name: String! @external\n  papers: [Paper!]! @inverse(of: \"authors\")",
        );
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/research/schema.graphql")
            .collect();
        files.push(("ontology/research/schema.graphql", &research));
        files.push(("ontology/research/data/Person/vaswani.yaml", "name: X\n"));
        commit_files(dir.path(), &files, "init");
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        assert_eq!(
            rules(&report),
            ["ontology.record.not_owned"],
            "{:#?}",
            report.issues
        );
    }

    #[test]
    fn value_types_required_fields_and_keys() {
        let report = run(&[(
            "ontology/research/data/Paper/bert.yaml",
            "id: not-bert\nyear: \"2018\"\nvenue: BLOG\nauthors: vaswani\npages: { first: 1 }\n",
        )]);
        let messages: Vec<&str> = report.issues.iter().map(|i| i.message.as_str()).collect();
        let joined = messages.join("\n");
        for needle in [
            "does not match the file name key `bert`",
            "`Paper.year` must be a 32-bit integer",
            "`Paper.venue` must be one of",
            "`Paper.authors` must be a list",
            "required field `Paper.pages.last: Int!` is missing",
            "required field `Paper.title: String!` is missing",
        ] {
            assert!(joined.contains(needle), "missing `{needle}` in:\n{joined}");
        }
        assert!(report.issues.iter().all(|i| i.severity == Severity::Error));
    }

    #[test]
    fn references_to_missing_git_owned_entities_are_errors() {
        let report = run(&[(
            "ontology/research/data/Paper/bert.yaml",
            "title: BERT\nauthors: [devlin]\n",
        )]);
        assert_eq!(rules(&report), ["ontology.record.dangling_ref"]);
        assert_eq!(report.issues[0].severity, Severity::Error);
        assert!(report.issues[0].message.contains("Person:devlin"));
        assert!(report.issues[0]
            .message
            .contains("`core` (which owns Person)"));
    }

    #[test]
    fn extension_records_do_not_stand_in_for_missing_owner_records() {
        let report = run(&[
            ("ontology/research/data/Person/devlin.yaml", "{}\n"),
            (
                "ontology/research/data/Paper/bert.yaml",
                "title: BERT\nauthors: [devlin]\n",
            ),
        ]);
        let got: Vec<(&str, Option<&str>)> = report
            .issues
            .iter()
            .map(|i| (i.rule, i.path.as_deref()))
            .collect();
        assert_eq!(
            got,
            [
                (
                    "ontology.record.dangling_ref",
                    Some("ontology/research/data/Paper/bert.yaml")
                ),
                (
                    "ontology.record.orphan",
                    Some("ontology/research/data/Person/devlin.yaml")
                ),
            ],
            "{:#?}",
            report.issues
        );
        assert!(report.issues.iter().all(|i| i.severity == Severity::Error));
        assert!(report.issues[1]
            .message
            .contains("`core` (which owns Person) has no such record"));
    }

    #[test]
    fn every_owner_of_a_type_stores_each_entity() {
        const HR: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Person @key(fields: "id") { id: ID! team: String }
type Query { member(id: ID!): Person }
"#;
        let report = run(&[
            ("ontology/hr/subgraph.toml", GIT),
            ("ontology/hr/schema.graphql", HR),
            ("ontology/hr/data/Person/devlin.yaml", "team: Brain\n"),
        ]);
        let got: Vec<(&str, Option<&str>)> = report
            .issues
            .iter()
            .map(|i| (i.rule, i.path.as_deref()))
            .collect();
        assert_eq!(
            got,
            [
                (
                    "ontology.record.orphan",
                    Some("ontology/core/data/Person/vaswani.yaml")
                ),
                (
                    "ontology.record.orphan",
                    Some("ontology/hr/data/Person/devlin.yaml")
                ),
                (
                    "ontology.record.dangling_ref",
                    Some("ontology/research/data/Paper/attention.yaml")
                ),
            ],
            "{:#?}",
            report.issues
        );
    }

    #[test]
    fn extensions_with_required_fields_store_every_entity() {
        let research = RESEARCH.replace(
            "papers: [Paper!]! @inverse(of: \"authors\")",
            "papers: [Paper!]! @inverse(of: \"authors\")\n  hIndex: Int!",
        );
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/research/schema.graphql")
            .collect();
        files.push(("ontology/research/schema.graphql", &research));
        files.push((
            "ontology/core/data/Person/shazeer.yaml",
            "name: Noam Shazeer\n",
        ));
        files.push(("ontology/research/data/Person/vaswani.yaml", "hIndex: 70\n"));
        commit_files(dir.path(), &files, "init");
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        assert_eq!(
            rules(&report),
            ["ontology.record.missing"],
            "{:#?}",
            report.issues
        );
        let issue = &report.issues[0];
        assert_eq!(issue.severity, Severity::Error);
        assert_eq!(issue.path.as_deref(), Some("ontology/research/data/Person"));
        assert!(
            issue.message.contains("`Person.hIndex: Int!` is required")
                && issue.message.contains("1 Person (shazeer)"),
            "{}",
            issue.message
        );
    }

    #[test]
    fn required_fields_on_entities_owned_outside_git_are_flagged() {
        const RATINGS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
extend type Person @key(fields: "id") { id: ID! rating: Int! }
"#;
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                (
                    "ontology/people/subgraph.toml",
                    "[remote]\nurl = \"https://people.example/graphql\"\n",
                ),
                ("ontology/people/schema.graphql", CORE),
                ("ontology/ratings/subgraph.toml", GIT),
                ("ontology/ratings/schema.graphql", RATINGS),
                ("ontology/ratings/data/Person/vaswani.yaml", "rating: 5\n"),
            ],
            "init",
        );
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        assert_eq!(
            rules(&report),
            ["ontology.schema.required_extension_field"],
            "{:#?}",
            report.issues
        );
        assert_eq!(report.issues[0].severity, Severity::Warning);
    }

    /// `check` over the base files with research's schema replaced.
    fn run_with_research(research: &str) -> CheckReport {
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/research/schema.graphql")
            .collect();
        files.push(("ontology/research/schema.graphql", research));
        commit_files(dir.path(), &files, "init");
        check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap()
    }

    /// `check` over the base files with core's schema replaced, plus `extra`.
    fn run_with_core(core: &str, extra: &[(&str, &str)]) -> CheckReport {
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/core/schema.graphql")
            .collect();
        files.push(("ontology/core/schema.graphql", core));
        files.extend_from_slice(extra);
        commit_files(dir.path(), &files, "init");
        check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap()
    }

    fn shared_field_issues(report: &CheckReport) -> Vec<(Severity, &str)> {
        report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.schema.shared_field")
            .map(|i| (i.severity, i.message.as_str()))
            .collect()
    }

    const CORE_SHAREABLE: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
type Person @key(fields: "id") { id: ID! name: String! @shareable }
type Query { person(id: ID!): Person }
"#;

    const NOTES_TOML: &str = "[storage]\nkind = \"markdown\"\ndir = \"pages\"\ntype = \"Topic\"\nbody_field = \"body\"\nlinks_field = \"related\"\n";
    const NOTES: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
directive @inverse(of: String!) on FIELD_DEFINITION
type Topic @key(fields: "id") {
  id: ID!
  title: String!
  year: Int
  body: String
  papers: [Paper!]!
  related: [Topic!]!
}
extend type Paper @key(fields: "id") { id: ID! topics: [Topic!]! @inverse(of: "papers") }
type Query { topic(id: ID!): Topic }
"#;

    #[test]
    fn database_storage_settings_serve_would_refuse_are_errors() {
        const THINGS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Thing @key(fields: "id") { id: ID! type: String! }
type Query { thing(id: ID!): Thing }
"#;
        let report = run(&[
            (
                "ontology/things/subgraph.toml",
                "[storage]\nkind = \"dynamodb\"\ntable = \"things\"\n",
            ),
            ("ontology/things/schema.graphql", THINGS),
        ]);
        assert_eq!(rules(&report), ["ontology.storage"], "{:#?}", report.issues);
        assert!(
            report.issues[0]
                .message
                .contains("reserved attribute names"),
            "{}",
            report.issues[0].message
        );
        assert_eq!(
            report.issues[0].path.as_deref(),
            Some("ontology/things/subgraph.toml")
        );
    }

    #[test]
    fn markdown_notes_are_checked_leniently() {
        let report = run(&[
            ("ontology/notes/subgraph.toml", NOTES_TOML),
            ("ontology/notes/schema.graphql", NOTES),
            (
                "ontology/notes/pages/attention.md",
                "---\ntitle: Attention\npapers: [\"[[attention]]\"]\ntags: [unknown, ignored]\n---\nSee [[bad]].\n",
            ),
            (
                "ontology/notes/pages/bad.md",
                "---\ntitle: Bad\nyear: soon\npapers: [\"[[no-such-paper]]\"]\n---\n",
            ),
        ]);
        let got: Vec<(&str, Option<&str>)> = report
            .issues
            .iter()
            .map(|i| (i.rule, i.path.as_deref()))
            .collect();
        assert_eq!(
            got,
            [
                (
                    "ontology.record.dangling_ref",
                    Some("ontology/notes/pages/bad.md")
                ),
                ("ontology.record.type", Some("ontology/notes/pages/bad.md")),
            ],
            "{:#?}",
            report.issues
        );
        assert_eq!(report.records, 4, "two YAML records and two notes");
    }

    #[test]
    fn markdown_filled_fields_must_be_declared() {
        let note = (
            "ontology/notes/pages/attention.md",
            "---\ntitle: Attention\n---\nSee [[attention]].\n",
        );
        let problems = |toml: &str| -> Vec<String> {
            run(&[
                ("ontology/notes/subgraph.toml", toml),
                ("ontology/notes/schema.graphql", NOTES),
                note,
            ])
            .issues
            .into_iter()
            .filter(|i| i.rule == "ontology.storage")
            .map(|i| i.message)
            .collect()
        };
        assert_eq!(problems(NOTES_TOML), [] as [String; 0]);
        // A misspelling would fill a field no query reads.
        let found = problems(&NOTES_TOML.replace("\"related\"", "\"reltaed\""));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("no `Topic` declares `reltaed`"),
            "{}",
            found[0]
        );
        // A declared field of the wrong kind cannot hold what is filled in.
        let found = problems(
            &NOTES_TOML
                .replace("\"body\"", "\"title\"")
                .replace("\"related\"", "\"papers\""),
        );
        assert!(
            found.is_empty(),
            "String and a relation list fit: {found:?}"
        );
        let found = problems(&NOTES_TOML.replace("\"body\"", "\"year\""));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("fills `Topic.year`"), "{}", found[0]);
    }

    #[test]
    fn a_markdown_sub_ontology_without_notes_is_a_warning() {
        let warnings = |files: &[(&str, &str)]| -> Vec<String> {
            run(files)
                .issues
                .into_iter()
                .filter(|i| i.severity == Severity::Warning && i.rule == "ontology.storage")
                .map(|i| i.message)
                .collect()
        };
        let found = warnings(&[
            ("ontology/notes/subgraph.toml", NOTES_TOML),
            ("ontology/notes/schema.graphql", NOTES),
        ]);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            found[0].contains("no notes under `ontology/notes/pages`"),
            "{found:?}"
        );
        let found = warnings(&[
            ("ontology/notes/subgraph.toml", NOTES_TOML),
            ("ontology/notes/schema.graphql", NOTES),
            (
                "ontology/notes/pages/attention.md",
                "---\ntitle: Attention\n---\n",
            ),
        ]);
        assert!(found.is_empty(), "{found:?}");
    }

    #[test]
    fn a_markdown_body_field_cannot_be_the_key() {
        const SLUGGED: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Topic @key(fields: "slug") { slug: String! body: String }
type Query { topic(slug: String!): Topic }
"#;
        let problems = |body_field: &str| -> Vec<String> {
            let toml = format!(
                "[storage]\nkind = \"markdown\"\ndir = \"pages\"\ntype = \"Topic\"\nbody_field = \"{body_field}\"\n"
            );
            run(&[
                ("ontology/notes/subgraph.toml", toml.as_str()),
                ("ontology/notes/schema.graphql", SLUGGED),
                ("ontology/notes/pages/a.md", "Text.\n"),
            ])
            .issues
            .into_iter()
            .filter(|i| i.rule == "ontology.storage")
            .map(|i| i.message)
            .collect()
        };
        assert!(problems("body").is_empty(), "{:?}", problems("body"));
        // The key is filled last, so the body would never be read.
        let found = problems("slug");
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("names `Topic`'s key"), "{}", found[0]);
    }

    #[test]
    fn a_markdown_type_the_schema_lacks_is_an_error_without_notes() {
        let misspelled = NOTES_TOML.replace("type = \"Topic\"", "type = \"Topci\"");
        for files in [
            vec![
                ("ontology/notes/subgraph.toml", misspelled.as_str()),
                ("ontology/notes/schema.graphql", NOTES),
            ],
            vec![
                ("ontology/notes/subgraph.toml", misspelled.as_str()),
                ("ontology/notes/schema.graphql", NOTES),
                ("ontology/notes/pages/attention.md", "---\ntitle: A\n---\n"),
            ],
        ] {
            let found: Vec<String> = run(&files)
                .issues
                .into_iter()
                .filter(|i| i.severity == Severity::Error)
                .map(|i| i.message)
                .collect();
            assert_eq!(found.len(), 1, "{found:?}");
            assert!(
                found[0].contains("`type = \"Topci\"` is not an entity"),
                "{found:?}"
            );
        }
    }

    #[test]
    fn notes_of_a_type_without_the_filled_fields_keep_nothing_there() {
        const MIXED: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Topic @key(fields: "id") { id: ID! body: String related: [Topic!]! }
type Person @key(fields: "id") { id: ID! name: String }
type Query { topic(id: ID!): Topic person(id: ID!): Person }
"#;
        let report = run(&[
            (
                "ontology/notes/subgraph.toml",
                "[storage]\nkind = \"markdown\"\ndir = \"pages\"\ntype_field = \"type\"\nbody_field = \"body\"\nlinks_field = \"related\"\n",
            ),
            ("ontology/notes/schema.graphql", MIXED),
            (
                "ontology/notes/pages/topic.md",
                "---\ntype: Topic\n---\nAbout [[person]].\n",
            ),
            (
                "ontology/notes/pages/person.md",
                "---\ntype: Person\nname: Ann\n---\nA biography.\n",
            ),
            (
                "ontology/notes/pages/quiet.md",
                "---\ntype: Person\nname: Bo\n---\n",
            ),
        ]);
        let storage: Vec<(&str, &str)> = report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.storage")
            .map(|i| (i.path.as_deref().unwrap_or_default(), i.message.as_str()))
            .collect();
        assert_eq!(storage.len(), 1, "{:#?}", report.issues);
        assert_eq!(storage[0].0, "ontology/notes/pages/person.md");
        assert!(
            storage[0].1.contains("declares no `body`"),
            "{}",
            storage[0].1
        );
    }

    #[test]
    fn markdown_sub_ontologies_own_their_types() {
        const READING: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type ReadingList @key(fields: "id") { id: ID! topics: [Topic!]! }
extend type Topic @key(fields: "id") { id: ID! }
type Query { list(id: ID!): ReadingList }
"#;
        let report = run(&[
            ("ontology/notes/subgraph.toml", NOTES_TOML),
            ("ontology/notes/schema.graphql", NOTES),
            (
                "ontology/notes/pages/transformers.md",
                "---\ntitle: Transformers\n---\n",
            ),
            ("ontology/reading/subgraph.toml", GIT),
            ("ontology/reading/schema.graphql", READING),
            (
                "ontology/reading/data/ReadingList/week1.yaml",
                "topics: [transformers, ghost]\n",
            ),
        ]);
        assert_eq!(
            rules(&report),
            ["ontology.record.dangling_ref"],
            "{:#?}",
            report.issues
        );
        let issue = &report.issues[0];
        assert_eq!(issue.severity, Severity::Error);
        assert!(
            issue
                .message
                .contains("Topic:ghost, but `notes` (which owns Topic)"),
            "{}",
            issue.message
        );
    }

    #[test]
    fn markdown_extensions_with_required_fields_need_a_note_per_entity() {
        const BIOS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
extend type Person @key(fields: "id") { id: ID! bio: String! }
"#;
        let report = run(&[
            (
                "ontology/core/data/Person/shazeer.yaml",
                "name: Noam Shazeer\n",
            ),
            (
                "ontology/bios/subgraph.toml",
                "[storage]\nkind = \"markdown\"\ndir = \"pages\"\ntype = \"Person\"\n",
            ),
            ("ontology/bios/schema.graphql", BIOS),
            (
                "ontology/bios/pages/vaswani.md",
                "---\nbio: Transformer co-author\n---\n",
            ),
        ]);
        assert_eq!(
            rules(&report),
            ["ontology.record.missing"],
            "{:#?}",
            report.issues
        );
        let issue = &report.issues[0];
        assert_eq!(issue.path.as_deref(), Some("ontology/bios/pages"));
        assert!(
            issue.message.contains("1 Person (shazeer)"),
            "{}",
            issue.message
        );
    }

    #[test]
    fn a_field_has_one_local_resolver() {
        const HR: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
type Person @key(fields: "id") { id: ID! name: String! @shareable }
type Query { member(id: ID!): Person }
"#;
        // Even with equal values: the stores cannot be kept in agreement.
        let report = run_with_core(
            CORE_SHAREABLE,
            &[
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", HR),
                (
                    "ontology/hr/data/Person/vaswani.yaml",
                    "name: Ashish Vaswani\n",
                ),
            ],
        );
        let issues = shared_field_issues(&report);
        assert_eq!(issues.len(), 1, "{:#?}", report.issues);
        assert_eq!(issues[0].0, Severity::Error);
        assert!(
            issues[0]
                .1
                .contains("`Person.name` is resolved by `core`, `hr`"),
            "{}",
            issues[0].1
        );
    }

    #[test]
    fn a_plain_override_moves_the_field_but_a_progressive_one_shares_it() {
        let hr = |directive: &str| {
            format!(
                r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override"])
type Person @key(fields: "id") {{ id: ID! name: String! {directive} }}
type Query {{ member(id: ID!): Person }}
"#
            )
        };
        let moved = hr(r#"@override(from: "core")"#);
        let report = run_with_core(
            CORE,
            &[
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", &moved),
                ("ontology/hr/data/Person/vaswani.yaml", "name: A. Vaswani\n"),
            ],
        );
        assert!(
            shared_field_issues(&report).is_empty(),
            "{:#?}",
            report.issues
        );

        let progressive = hr(r#"@override(from: "core", label: "percent(50)")"#);
        let report = run_with_core(
            CORE,
            &[
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", &progressive),
                ("ontology/hr/data/Person/vaswani.yaml", "name: A. Vaswani\n"),
            ],
        );
        let issues = shared_field_issues(&report);
        assert_eq!(issues.len(), 1, "{:#?}", report.issues);
        assert_eq!(issues[0].0, Severity::Error);
    }

    #[test]
    fn derived_fields_count_as_resolvers_too() {
        const REVIEWS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
directive @inverse(of: String!) on FIELD_DEFINITION
extend type Paper @key(fields: "id") { id: ID! reviewers: [Person!]! }
extend type Person @key(fields: "id") { id: ID! papers: [Paper!]! @shareable @inverse(of: "reviewers") }
"#;
        let research = RESEARCH
            .replace("\"@shareable\"]", "\"@shareable\", \"@override\"]")
            .replace(
                "@inverse(of: \"authors\")",
                "@shareable @inverse(of: \"authors\")",
            );
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/research/schema.graphql")
            .collect();
        files.extend([
            ("ontology/research/schema.graphql", research.as_str()),
            ("ontology/reviews/subgraph.toml", GIT),
            ("ontology/reviews/schema.graphql", REVIEWS),
        ]);
        commit_files(dir.path(), &files, "init");
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        let issues = shared_field_issues(&report);
        assert_eq!(issues.len(), 1, "{:#?}", report.issues);
        assert!(
            issues[0]
                .1
                .contains("`Person.papers` is resolved by `research`, `reviews`"),
            "{}",
            issues[0].1
        );
    }

    #[test]
    fn local_sub_ontologies_key_an_entity_by_the_same_field() {
        const CORE_ALIAS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
type Person @key(fields: "id") { id: ID! name: String! alias: ID @shareable }
type Query { person(id: ID!): Person }
"#;
        const BY_ALIAS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
type Person @key(fields: "alias") { alias: ID! id: ID! @shareable }
type Query { byAlias(alias: ID!): Person }
"#;
        let report = run_with_core(
            CORE_ALIAS,
            &[
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", BY_ALIAS),
            ],
        );
        let got = rules(&report);
        assert!(
            got.contains(&"ontology.schema.key_mismatch"),
            "{:#?}",
            report.issues
        );
        // Each side's key is a plain stored field on the other, so both
        // fields have two resolvers.
        let shared = shared_field_issues(&report);
        assert!(
            shared
                .iter()
                .any(|(_, m)| m.contains("`Person.id` is resolved by"))
                && shared
                    .iter()
                    .any(|(_, m)| m.contains("`Person.alias` is resolved by")),
            "{shared:#?}"
        );
    }

    const CORE_NICK: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key"])
type Person @key(fields: "id") { id: ID! name: String! nickname: String }
type Query { person(id: ID!): Person }
"#;
    const CORE_NICK_RECORD: (&str, &str) = (
        "ontology/core/data/Person/vaswani.yaml",
        "name: Ashish Vaswani\nnickname: ash\n",
    );

    fn override_gaps(report: &CheckReport) -> Vec<(Severity, String)> {
        report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.record.override_gap")
            .map(|i| (i.severity, i.message.clone()))
            .collect()
    }

    #[test]
    fn an_override_must_carry_the_values_it_takes_over() {
        const HR: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override"])
extend type Person @key(fields: "id") { id: ID! nickname: String @override(from: "core") }
"#;
        let report = run_with_core(
            CORE_NICK,
            &[
                CORE_NICK_RECORD,
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", HR),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(found[0].0, Severity::Error);
        assert!(
            found[0]
                .1
                .contains("no value for 1 Person that `core` still stores (vaswani)"),
            "{}",
            found[0].1
        );

        // With the value moved over, the override is complete.
        let report = run_with_core(
            CORE_NICK,
            &[
                CORE_NICK_RECORD,
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", HR),
                ("ontology/hr/data/Person/vaswani.yaml", "nickname: ash\n"),
            ],
        );
        assert!(override_gaps(&report).is_empty(), "{:#?}", report.issues);

        // Present is not enough: the value must be the one moved.
        let report = run_with_core(
            CORE_NICK,
            &[
                CORE_NICK_RECORD,
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", HR),
                ("ontology/hr/data/Person/vaswani.yaml", "nickname: ashish\n"),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert!(
            found[0]
                .1
                .contains("holds a different value than `core` for 1 Person (vaswani)"),
            "{}",
            found[0].1
        );

        // A remote destination cannot be confirmed, so the move is refused.
        let report = run_with_core(
            CORE_NICK,
            &[
                CORE_NICK_RECORD,
                (
                    "ontology/hr/subgraph.toml",
                    "[remote]\nurl = \"https://hr.example/graphql\"\n",
                ),
                ("ontology/hr/schema.graphql", HR),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(found[0].0, Severity::Error);
        // …even when the source holds no value: it answered null, which the
        // remote may not.
        let report = run_with_core(
            CORE_NICK,
            &[
                (
                    "ontology/core/data/Person/vaswani.yaml",
                    "name: Ashish Vaswani\n",
                ),
                (
                    "ontology/hr/subgraph.toml",
                    "[remote]\nurl = \"https://hr.example/graphql\"\n",
                ),
                ("ontology/hr/schema.graphql", HR),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(found[0].0, Severity::Error);
        assert!(
            found[0].1.contains("check cannot read `hr`'s values"),
            "{}",
            found[0].1
        );
    }

    #[test]
    fn an_override_keeps_every_answer() {
        const HR: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override"])
extend type Person @key(fields: "id") { id: ID! nickname: String @override(from: "core") }
"#;
        const PROFILE: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key"])
extend type Person @key(fields: "id") { id: ID! nickname: String }
"#;
        const FROM_PROFILE: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override"])
extend type Person @key(fields: "id") { id: ID! nickname: String @override(from: "profile") }
"#;
        // An explicit null is a value too: the gateway's answer would change.
        let null_source = (
            "ontology/core/data/Person/vaswani.yaml",
            "name: Ashish Vaswani\nnickname: null\n",
        );
        let report = run_with_core(
            CORE_NICK,
            &[
                null_source,
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", HR),
                ("ontology/hr/data/Person/vaswani.yaml", "nickname: ash\n"),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert!(
            found[0].1.contains("holds a different value"),
            "{}",
            found[0].1
        );
        // …while no value on either side agrees.
        let report = run_with_core(
            CORE_NICK,
            &[
                null_source,
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", HR),
            ],
        );
        assert!(override_gaps(&report).is_empty(), "{:#?}", report.issues);

        // A record only the destination has changes the answer too: the
        // source (an extension) has no record of shazeer, so answered null.
        let report = run_with_core(
            CORE,
            &[
                (
                    "ontology/core/data/Person/shazeer.yaml",
                    "name: Noam Shazeer\n",
                ),
                ("ontology/profile/subgraph.toml", GIT),
                ("ontology/profile/schema.graphql", PROFILE),
                (
                    "ontology/profile/data/Person/vaswani.yaml",
                    "nickname: ash\n",
                ),
                ("ontology/hr/subgraph.toml", GIT),
                ("ontology/hr/schema.graphql", FROM_PROFILE),
                ("ontology/hr/data/Person/vaswani.yaml", "nickname: ash\n"),
                ("ontology/hr/data/Person/shazeer.yaml", "nickname: noam\n"),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert!(
            found[0].1.contains("holds a different value") && found[0].1.contains("(shazeer)"),
            "{}",
            found[0].1
        );
    }

    #[test]
    fn derived_fields_and_their_relations_stay_together() {
        // Takes over a field research derives.
        const LIBRARY: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override"])
extend type Paper @key(fields: "id") { id: ID! }
extend type Person @key(fields: "id") { id: ID! papers: [Paper!]! @override(from: "research") }
"#;
        // Takes over the relation research derives `Person.papers` from.
        const AUTHORS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override"])
extend type Paper @key(fields: "id") { id: ID! authors: [Person!]! @override(from: "research") }
extend type Person @key(fields: "id") { id: ID! }
"#;
        // A derived field cannot be moved: nothing is stored to compare.
        let report = run_with_core(
            CORE_NICK,
            &[
                CORE_NICK_RECORD,
                ("ontology/library/subgraph.toml", GIT),
                ("ontology/library/schema.graphql", LIBRARY),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(found[0].0, Severity::Error);
        assert!(
            found[0]
                .1
                .contains("`library` takes `Person.papers` over from `research`")
                && found[0].1.contains("derives it"),
            "{}",
            found[0].1
        );

        // A relation cannot leave the inverse built from it behind.
        let report = run_with_core(
            CORE_NICK,
            &[
                CORE_NICK_RECORD,
                ("ontology/authors/subgraph.toml", GIT),
                ("ontology/authors/schema.graphql", AUTHORS),
                (
                    "ontology/authors/data/Paper/attention.yaml",
                    "authors: [vaswani]\n",
                ),
            ],
        );
        let found = override_gaps(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert!(
            found[0]
                .1
                .contains("`research` derives `Person.papers` from it"),
            "{}",
            found[0].1
        );
    }

    #[test]
    fn keys_stored_in_records_have_the_key_type() {
        const MEMBERS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Member @key(fields: "handle") { handle: String! }
type Query { member(handle: String!): Member }
"#;
        let report = run_with_core(
            CORE,
            &[
                ("ontology/members/subgraph.toml", GIT),
                ("ontology/members/schema.graphql", MEMBERS),
                ("ontology/members/data/Member/42.yaml", "handle: 42\n"),
                ("ontology/members/data/Member/7.yaml", "handle: \"7\"\n"),
            ],
        );
        let found: Vec<(&str, &str)> = report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.record.type")
            .map(|i| (i.path.as_deref().unwrap_or_default(), i.message.as_str()))
            .collect();
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(found[0].0, "ontology/members/data/Member/42.yaml");
        assert!(found[0].1.contains("must be a string"), "{}", found[0].1);
    }

    #[test]
    fn symlinked_data_dirs_are_errors_not_empty() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("ontology/core/subgraph.toml", GIT),
                ("ontology/core/schema.graphql", CORE),
            ],
            "init",
        );
        let repo = git2::Repository::open(dir.path()).unwrap();
        let mut index = repo.index().unwrap();
        index
            .add_frombuffer(
                &git2::IndexEntry {
                    ctime: git2::IndexTime::new(0, 0),
                    mtime: git2::IndexTime::new(0, 0),
                    dev: 0,
                    ino: 0,
                    mode: 0o120_000,
                    uid: 0,
                    gid: 0,
                    file_size: 0,
                    id: git2::Oid::ZERO_SHA1,
                    flags: 0,
                    flags_extended: 0,
                    path: b"ontology/core/data".to_vec(),
                },
                b"../../elsewhere",
            )
            .unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "link", &tree, &[&parent])
            .unwrap();
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        let found: Vec<&Issue> = report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.storage")
            .collect();
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert!(
            found[0].message.contains("is a symlink"),
            "{}",
            found[0].message
        );
    }

    #[test]
    fn git_links_named_like_records_are_errors() {
        let dir = tempfile::tempdir().unwrap();
        let head = commit_files(
            dir.path(),
            &[
                ("ontology/core/subgraph.toml", GIT),
                ("ontology/core/schema.graphql", CORE),
            ],
            "init",
        );
        // A submodule entry (mode 160000) where a record would be.
        let repo = git2::Repository::open(dir.path()).unwrap();
        let mut index = repo.index().unwrap();
        index
            .add(&git2::IndexEntry {
                ctime: git2::IndexTime::new(0, 0),
                mtime: git2::IndexTime::new(0, 0),
                dev: 0,
                ino: 0,
                mode: 0o160_000,
                uid: 0,
                gid: 0,
                file_size: 0,
                id: git2::Oid::from_str(&head).unwrap(),
                flags: 0,
                flags_extended: 0,
                path: b"ontology/core/data/Person/linked.yaml".to_vec(),
            })
            .unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("t", "t@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "link", &tree, &[&parent])
            .unwrap();

        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        let found: Vec<&Issue> = report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.record.parse")
            .collect();
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert!(
            found[0].message.contains("not a file"),
            "{}",
            found[0].message
        );
    }

    #[test]
    fn stored_lists_fit_what_one_list_answers() {
        const NOTES: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Note @key(fields: "id") { id: ID! tags: [String!]! }
type Query { note(id: ID!): Note }
"#;
        let tags = |n: usize| format!("tags: [{}]\n", vec!["t"; n].join(", "));
        let (fits, over) = (tags(1000), tags(1001));
        let report = run_with_core(
            CORE,
            &[
                ("ontology/notes/subgraph.toml", GIT),
                ("ontology/notes/schema.graphql", NOTES),
                ("ontology/notes/data/Note/fits.yaml", &fits),
                ("ontology/notes/data/Note/over.yaml", &over),
            ],
        );
        let found: Vec<(&str, &str)> = report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.record.type")
            .map(|i| (i.path.as_deref().unwrap_or_default(), i.message.as_str()))
            .collect();
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(found[0].0, "ontology/notes/data/Note/over.yaml");
        assert!(found[0].1.contains("holds 1001 items"), "{}", found[0].1);
    }

    #[test]
    fn relation_keys_are_literals_of_the_target_key() {
        const VENUES: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Venue @key(fields: "id") { id: Int! name: String! }
type Query { venue(id: Int!): Venue }
"#;
        const EVENTS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Event @key(fields: "id") { id: ID! venue: Venue host: Person }
extend type Venue @key(fields: "id") { id: Int! }
extend type Person @key(fields: "id") { id: ID! }
type Query { event(id: ID!): Event }
"#;
        let report = run_with_core(
            CORE,
            &[
                (
                    "ontology/venues/subgraph.toml",
                    "[remote]\nurl = \"https://venues.example/graphql\"\n",
                ),
                ("ontology/venues/schema.graphql", VENUES),
                ("ontology/events/subgraph.toml", GIT),
                ("ontology/events/schema.graphql", EVENTS),
                ("ontology/events/data/Event/int.yaml", "venue: 42\n"),
                ("ontology/events/data/Event/text.yaml", "venue: \"42\"\n"),
                ("ontology/events/data/Event/word.yaml", "venue: abc\n"),
                ("ontology/events/data/Event/padded.yaml", "venue: \"042\"\n"),
                (
                    "ontology/events/data/Event/wide.yaml",
                    "venue: 3000000000\n",
                ),
                ("ontology/events/data/Event/half.yaml", "venue: 42.5\n"),
                ("ontology/events/data/Event/idfloat.yaml", "host: 1.5\n"),
            ],
        );
        let mut bad: Vec<&str> = report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.record.type")
            .filter_map(|i| i.path.as_deref())
            .filter_map(|p| p.strip_prefix("ontology/events/data/Event/"))
            .collect();
        bad.sort_unstable();
        assert_eq!(
            bad,
            [
                "half.yaml",
                "idfloat.yaml",
                "padded.yaml",
                "wide.yaml",
                "word.yaml"
            ],
            "{:#?}",
            report.issues
        );
    }

    #[test]
    fn root_fields_stay_in_one_sub_ontology() {
        const LIBRARY: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
type Book @key(fields: "id") { id: ID! title: String }
type Query { books: [Book!]! @shareable }
"#;
        const SHELF: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override"])
type Book @key(fields: "id") { id: ID! }
type Query { books: [Book!]! @override(from: "library") }
"#;
        const PEOPLE: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
extend type Person @key(fields: "id") { id: ID! }
type Query { people: [Person!]! @shareable }
"#;
        let remote = "[remote]\nurl = \"https://library.example/graphql\"\n";
        let root_issues = |report: &CheckReport| -> Vec<(Severity, &'static str, String)> {
            report
                .issues
                .iter()
                .filter(|i| i.message.contains("root field `"))
                .map(|i| (i.severity, i.rule, i.message.clone()))
                .collect()
        };

        // A local sub-ontology taking a remote's root field over: the
        // gateway would answer from the local store instead.
        let report = run_with_core(
            CORE,
            &[
                ("ontology/library/subgraph.toml", remote),
                ("ontology/library/schema.graphql", LIBRARY),
                ("ontology/shelf/subgraph.toml", GIT),
                ("ontology/shelf/schema.graphql", SHELF),
            ],
        );
        let found = root_issues(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(
            (found[0].0, found[0].1),
            (Severity::Error, "ontology.record.override_gap")
        );

        // Differently named query roots compose into one `Query`.
        let root_a = PEOPLE.replace("type Query {", "schema { query: RootA }\ntype RootA {");
        let root_b = PEOPLE.replace("type Query {", "schema { query: RootB }\ntype RootB {");
        let report = run_with_core(
            CORE,
            &[
                ("ontology/a/subgraph.toml", GIT),
                ("ontology/a/schema.graphql", &root_a),
                ("ontology/b/subgraph.toml", GIT),
                ("ontology/b/schema.graphql", &root_b),
            ],
        );
        let found = root_issues(&report);
        assert_eq!(found.len(), 1, "{:#?}", report.issues);
        assert_eq!(
            (found[0].0, found[0].1),
            (Severity::Error, "ontology.schema.shared_field")
        );

        // Two local resolvers of one root field: an error; with a remote,
        // a warning.
        for (other, severity) in [(GIT, Severity::Error), (remote, Severity::Warning)] {
            let report = run_with_core(
                CORE,
                &[
                    ("ontology/a/subgraph.toml", GIT),
                    ("ontology/a/schema.graphql", PEOPLE),
                    ("ontology/b/subgraph.toml", other),
                    ("ontology/b/schema.graphql", PEOPLE),
                ],
            );
            let found = root_issues(&report);
            assert_eq!(found.len(), 1, "{:#?}", report.issues);
            assert_eq!(
                (found[0].0, found[0].1),
                (severity, "ontology.schema.shared_field")
            );
        }
    }

    #[test]
    fn a_field_shared_with_a_remote_is_a_warning() {
        const PARTNER: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@shareable"])
type Person @key(fields: "id") { id: ID! name: String! @shareable }
type Query { partnerPerson(id: ID!): Person }
"#;
        let report = run_with_core(
            CORE_SHAREABLE,
            &[
                (
                    "ontology/partner/subgraph.toml",
                    "[remote]\nurl = \"https://partner.example/graphql\"\n",
                ),
                ("ontology/partner/schema.graphql", PARTNER),
            ],
        );
        let issues = shared_field_issues(&report);
        assert_eq!(issues.len(), 1, "{:#?}", report.issues);
        assert_eq!(issues[0].0, Severity::Warning);
        assert!(
            issues[0].1.contains("by remote `partner`"),
            "{}",
            issues[0].1
        );
    }

    #[test]
    fn an_owned_type_without_records_still_rejects_references() {
        let dir = tempfile::tempdir().unwrap();
        let files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/core/data/Person/vaswani.yaml")
            .collect();
        commit_files(dir.path(), &files, "init");
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        assert_eq!(
            rules(&report),
            ["ontology.record.dangling_ref"],
            "{:#?}",
            report.issues
        );
        assert_eq!(report.issues[0].severity, Severity::Error);
    }

    #[test]
    fn duplicate_record_fields_are_errors_not_overwrites() {
        let report = run(&[(
            "ontology/research/data/Paper/bert.yaml",
            "title: BERT\nauthors: [vaswani]\nauthors: [devlin]\n",
        )]);
        assert_eq!(
            rules(&report),
            ["ontology.record.parse"],
            "{:#?}",
            report.issues
        );
        assert!(
            report.issues[0]
                .message
                .contains("duplicate entry with key \"authors\""),
            "{}",
            report.issues[0].message
        );
    }

    #[test]
    fn layout_and_unknown_types() {
        let report = run(&[
            ("ontology/research/data/stray.yaml", "x: 1\n"),
            ("ontology/research/data/Talk/keynote.yaml", "x: 1\n"),
            (
                "ontology/research/data/PageRange/p.yaml",
                "first: 1\nlast: 2\n",
            ),
            ("ontology/research/data/Paper/notes.txt", "x"),
        ]);
        let mut got = rules(&report);
        got.sort_unstable();
        assert_eq!(
            got,
            [
                "ontology.record.layout",
                "ontology.record.layout",
                "ontology.record.unknown_type",
                "ontology.record.unknown_type",
            ],
            "{:#?}",
            report.issues
        );
    }

    #[test]
    fn unservable_local_schemas_are_errors() {
        let research = RESEARCH.replace(
            "papers: [Paper!]! @inverse(of: \"authors\")",
            "papers: [Paper!]! @inverse(of: \"missing\")",
        );
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/research/schema.graphql")
            .collect();
        files.push(("ontology/research/schema.graphql", &research));
        commit_files(dir.path(), &files, "init");
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        assert!(
            report.composition.is_ok(),
            "composes fine: {:?}",
            report.composition.errors
        );
        assert_eq!(
            rules(&report),
            ["ontology.schema.unservable"],
            "{:#?}",
            report.issues
        );
        assert_eq!(report.issues[0].severity, Severity::Error);
        assert!(report.issues[0].message.contains("has no field `missing`"));
    }

    #[test]
    fn multiple_keys_are_unservable() {
        let core = CORE.replace(
            "type Person @key(fields: \"id\")",
            "type Person @key(fields: \"id\") @key(fields: \"name\")",
        );
        let dir = tempfile::tempdir().unwrap();
        let mut files: Vec<(&str, &str)> = base_files()
            .into_iter()
            .filter(|(p, _)| *p != "ontology/core/schema.graphql")
            .collect();
        files.push(("ontology/core/schema.graphql", &core));
        commit_files(dir.path(), &files, "init");
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        assert!(
            rules(&report).contains(&"ontology.schema.unservable"),
            "{:#?}",
            report.issues
        );
    }

    #[test]
    fn inverse_fields_on_value_types_are_unservable() {
        const SHOP: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key", "@shareable"])
directive @inverse(of: String!) on FIELD_DEFINITION
type Item @key(fields: "id") { id: ID! price: Price }
type Price @shareable { amount: Int matches: [Item!]! @inverse(of: "price") }
type Query { item(id: ID!): Item }
"#;
        let report = run(&[
            ("ontology/shop/subgraph.toml", GIT),
            ("ontology/shop/schema.graphql", SHOP),
            ("ontology/shop/data/Item/mug.yaml", "price: { amount: 3 }\n"),
        ]);
        assert_eq!(
            rules(&report),
            ["ontology.schema.unservable"],
            "{:#?}",
            report.issues
        );
        assert!(
            report.issues[0]
                .message
                .contains("only entities have inverse relations"),
            "{}",
            report.issues[0].message
        );
    }

    #[test]
    fn int_keyed_file_names_must_be_canonical() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("ontology/inv/subgraph.toml", GIT),
                (
                    "ontology/inv/schema.graphql",
                    "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\"])\ntype Item @key(fields: \"id\") { id: Int! }\ntype Query { item(id: Int!): Item }",
                ),
                ("ontology/inv/data/Item/42.yaml", ""),
                ("ontology/inv/data/Item/042.yaml", ""),
                ("ontology/inv/data/Item/x.yaml", ""),
            ],
            "init",
        );
        let report = check(&GitSnapshot::open(dir.path(), "HEAD").unwrap(), "ontology").unwrap();
        let bad: Vec<&str> = report
            .issues
            .iter()
            .filter(|i| i.rule == "ontology.record.key")
            .filter_map(|i| i.path.as_deref())
            .collect();
        assert_eq!(
            bad,
            [
                "ontology/inv/data/Item/042.yaml",
                "ontology/inv/data/Item/x.yaml"
            ],
            "{:#?}",
            report.issues
        );
    }

    #[test]
    fn composition_errors_are_reported() {
        let report = run(&[
            ("ontology/other/subgraph.toml", GIT),
            (
                "ontology/other/schema.graphql",
                "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\"])\ntype Paper @key(fields: \"id\") { id: ID! year: String }",
            ),
        ]);
        assert!(!report.composition.is_ok());
        assert!(report.errors() >= 1);
        assert!(rules(&report).contains(&"ontology.composition"));
    }
}
