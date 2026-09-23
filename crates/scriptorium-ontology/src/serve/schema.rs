//! Build a Federation v2 subgraph (an `async-graphql` dynamic schema) from a
//! sub-ontology's model and its store.
//!
//! Resolution rules:
//!
//! - **Entities** travel as [`Node::Entity`]: a type and a key, with the
//!   record loaded from the store only when a stored field is requested.
//!   Relations and `_entities` representations therefore cost nothing until
//!   a field of *this* sub-ontology is selected; fields owned elsewhere are
//!   the gateway's to fetch.
//! - **Stored fields** read the record; relation values are target keys.
//! - **`@inverse(of: f)` fields** list the target type's records whose `f`
//!   references this entity.
//! - **Root fields** are *lookups* (`t(key: ID!): T`) or *lists*
//!   (`ts(first: Int): [T!]!`); see `docs/ONTOLOGY.md`.
//! - **`_service { sdl }`** answers with the committed schema, verbatim;
//!   introspection (`__schema`, `__type`) is left to the gateway.

use std::sync::Arc;

use async_graphql::dynamic::{
    Enum, Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, Scalar, Schema,
    TypeRef,
};
use async_graphql::parser::types::{
    DocumentOperations, ExecutableDocument, Field as QueryField, OperationDefinition,
    OperationType, Selection, SelectionSet,
};
use async_graphql::{Name, PathSegment, ServerError, Value};
use serde_json::Value as Json;
use tokio::sync::OnceCell;

use crate::error::{Error, Result};
use crate::model::{ArgDef, FieldDef, ObjectType, SubOntologyModel, TypeExpr, TypeKind};
use crate::store::{EntityStore, ListQuery, Record};

type GqlResult<T> = std::result::Result<T, async_graphql::Error>;

/// Deepest selection a subgraph accepts. Gateway entity fetches are shallow;
/// this stops cyclic relations (`cites { citedBy { cites … } }`) from
/// fanning out without bound.
pub const MAX_QUERY_DEPTH: usize = 32;
/// Most fields one request may select.
pub const MAX_QUERY_COMPLEXITY: usize = 5_000;
/// Root lists return this many entities unless `first` asks otherwise…
pub const DEFAULT_LIST_LIMIT: usize = 100;
/// …and never more than this.
pub const MAX_LIST_LIMIT: usize = 1_000;
/// Most entities one inverse relation returns; more is a query error
/// (inverse relations are not paginated yet).
pub const MAX_INVERSE_RESULTS: usize = 1_000;
/// Most `_entities` representations one request may ask for. Complexity
/// counts selected fields, not the size of the `representations` variable.
pub const MAX_REPRESENTATIONS: usize = 10_000;
/// Most field resolutions one request may schedule for list items and
/// `_entities` representations (items × the fields selected under each,
/// nested selections included): a long list or a big batch must select few
/// fields. Complexity counts the query's fields once, not per item.
pub const MAX_WORK: usize = 250_000;
/// Most list items one request may resolve, summed over every list field
/// (root lists, stored relations, inverse relations). Field-count limits do
/// not multiply by list sizes, so nested relations (`cites { cites … }`)
/// need their own bound.
pub const MAX_LIST_ITEMS: usize = 100_000;
/// Most bytes of values one request may answer with, counted as fields
/// resolve (copies of `_service { sdl }` up front): an alias repeats a
/// value, however large, for one unit of complexity.
pub const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// Per-request bounds on what a subgraph will do. The defaults are the
/// `MAX_*` / `DEFAULT_*` constants; tests shrink them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub depth: usize,
    pub complexity: usize,
    pub default_list: usize,
    pub max_list: usize,
    pub max_inverse: usize,
    pub max_representations: usize,
    pub max_work: usize,
    pub max_list_items: usize,
    pub max_response_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            depth: MAX_QUERY_DEPTH,
            complexity: MAX_QUERY_COMPLEXITY,
            default_list: DEFAULT_LIST_LIMIT,
            max_list: MAX_LIST_LIMIT,
            max_inverse: MAX_INVERSE_RESULTS,
            max_representations: MAX_REPRESENTATIONS,
            max_work: MAX_WORK,
            max_list_items: MAX_LIST_ITEMS,
            max_response_bytes: MAX_RESPONSE_BYTES,
        }
    }
}

/// Per-request bookkeeping shared by every `_entities` field of one request
/// (a query may alias `_entities` several times).
#[derive(Default)]
struct RequestState {
    /// Representations asked for so far, across all `_entities` fields.
    representations: std::sync::atomic::AtomicUsize,
    /// Items × selected fields so far, across every list and `_entities` field.
    work: std::sync::atomic::AtomicUsize,
    /// List items resolved so far, across every list field.
    list_items: std::sync::atomic::AtomicUsize,
    /// Bytes of values answered so far (see [`MAX_RESPONSE_BYTES`]).
    bytes: std::sync::atomic::AtomicUsize,
    /// A request limit was exceeded: resolvers stop reading and answer
    /// placeholders, and [`execute`] answers this one error instead.
    exhausted: std::sync::atomic::AtomicBool,
    exceeded: std::sync::Mutex<Option<String>>,
    /// Owned entities with no record, for [`execute`] to null out.
    missing: std::sync::Mutex<Vec<Missing>>,
}

struct Missing {
    /// Response key of the `_entities` field (its alias, if any).
    response_key: String,
    index: usize,
    message: String,
}

impl RequestState {
    /// A request limit is exceeded: the whole answer becomes `message` (the
    /// first one), and resolvers stop working on it.
    fn exceed(&self, message: String) -> async_graphql::Error {
        let mut first = self
            .exceeded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if first.is_none() {
            *first = Some(message.clone());
        }
        self.exhausted
            .store(true, std::sync::atomic::Ordering::SeqCst);
        async_graphql::Error::new(message)
    }

    fn record_missing(&self, missing: Missing) {
        self.missing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(missing);
    }

    fn take_missing(&self) -> Vec<Missing> {
        std::mem::take(
            &mut *self
                .missing
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }
}

/// Charge a list of `items` to the request: the items themselves (see
/// [`MAX_LIST_ITEMS`]) and the fields selected under each (see
/// [`MAX_WORK`]). Without [`execute`]'s request state nothing is charged.
fn charge_list(ctx: &ResolverContext<'_>, rt: &Runtime, items: usize) -> GqlResult<()> {
    let limits = &rt.limits;
    let Some(state) = ctx.data_opt::<Arc<RequestState>>() else {
        return Ok(());
    };
    let total = state
        .list_items
        .fetch_add(items, std::sync::atomic::Ordering::SeqCst)
        .saturating_add(items);
    if total > limits.max_list_items {
        return Err(state.exceed(format!(
            "this request resolves more than {} list items; select fewer or shallower relations",
            limits.max_list_items
        )));
    }
    // Every item repeats the response keys selected under it, aliases as
    // long as the client likes and `__typename` included.
    let keys = ctx
        .field()
        .selection_set()
        .map(|f| key_bytes(f, rt.typename_bytes))
        .sum::<usize>()
        + 2;
    charge_bytes(ctx, limits, items.saturating_mul(keys))?;
    let per_item = ctx.field().selection_set().map(count_fields).sum::<usize>();
    let work = items.saturating_mul(per_item);
    let total_work = state
        .work
        .fetch_add(work, std::sync::atomic::Ordering::SeqCst)
        .saturating_add(work);
    if total_work > limits.max_work {
        return Err(state.exceed(format!(
            "{items} items × {per_item} selected fields exceeds the per-request budget of {}; select fewer fields or items",
            limits.max_work
        )));
    }
    Ok(())
}

/// Charge `bytes` of answer to the request (see [`MAX_RESPONSE_BYTES`]).
/// Without [`execute`]'s request state nothing is charged.
fn charge_bytes(ctx: &ResolverContext<'_>, limits: &Limits, bytes: usize) -> GqlResult<()> {
    let Some(state) = ctx.data_opt::<Arc<RequestState>>() else {
        return Ok(());
    };
    let total = state
        .bytes
        .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst)
        .saturating_add(bytes);
    if total > limits.max_response_bytes {
        return Err(state.exceed(too_large(limits)));
    }
    Ok(())
}

fn too_large(limits: &Limits) -> String {
    format!(
        "this request's answer exceeds {} bytes; select fewer fields or items",
        limits.max_response_bytes
    )
}

/// Whether the request's answer already outgrew its budget: the rest of
/// it is discarded, so nothing more is read, copied, or reported.
fn exhausted(ctx: &ResolverContext<'_>) -> bool {
    ctx.data_opt::<Arc<RequestState>>()
        .is_some_and(|state| state.exhausted.load(std::sync::atomic::Ordering::SeqCst))
}

/// A cheap stand-in of type `ty` for a discarded answer: null where
/// allowed, else an empty or zero value (an error per field would repeat
/// its path, alias included, once per item).
fn placeholder(model: &SubOntologyModel, ty: &TypeExpr) -> Option<FieldValue<'static>> {
    if ty.is_list() {
        return Some(FieldValue::list(Vec::<FieldValue>::new()));
    }
    if !ty.non_null {
        return None;
    }
    Some(match model.kind_of(&ty.named) {
        TypeKind::Scalar if ty.named == "Int" => FieldValue::value(Value::from(0)),
        TypeKind::Scalar if ty.named == "Float" => FieldValue::value(Value::from(0.0)),
        TypeKind::Scalar if ty.named == "Boolean" => FieldValue::value(Value::from(false)),
        TypeKind::Enum(values) => values.first().map_or(FieldValue::NULL, |v| {
            FieldValue::value(Value::Enum(Name::new(v)))
        }),
        TypeKind::Entity(target) => FieldValue::owned_any(Node::Entity(EntityNode::reference(
            &target.name,
            String::new(),
        ))),
        TypeKind::Value(_) => FieldValue::owned_any(Node::Inline(Arc::new(Record::new()))),
        TypeKind::Scalar | TypeKind::Unknown => FieldValue::value(Value::String(String::new())),
    })
}

/// Roughly the bytes `raw` adds to a response as a value of type `named`.
/// A value object counts only its braces: its fields are charged as they
/// resolve, and only the selected ones.
fn answer_bytes(model: &SubOntologyModel, named: &str, raw: &Json) -> usize {
    fn json_bytes(raw: &Json) -> usize {
        match raw {
            Json::Null | Json::Bool(_) => 5,
            // An upper bound: 64-bit integers and shortest-form floats.
            Json::Number(_) => 24,
            Json::String(s) => json_string_bytes(s),
            Json::Array(items) => {
                2 + items.len().saturating_sub(1) + items.iter().map(json_bytes).sum::<usize>()
            }
            Json::Object(map) => {
                2 + map.len().saturating_sub(1)
                    + map
                        .iter()
                        .map(|(k, v)| json_string_bytes(k) + 1 + json_bytes(v))
                        .sum::<usize>()
            }
        }
    }
    let value_object = matches!(model.kind_of(named), TypeKind::Value(_));
    let item = |raw: &Json| if value_object { 2 } else { json_bytes(raw) };
    match raw {
        Json::Array(items) => {
            2 + items.len().saturating_sub(1) + items.iter().map(item).sum::<usize>()
        }
        other => item(other),
    }
}

/// A string's size in the JSON response: quoted, with the escapes
/// `serde_json` writes (`\"`, `\\`, `\n` and its kin, and `\u00XX` for
/// the other control characters).
fn json_string_bytes(s: &str) -> usize {
    2 + s
        .bytes()
        .map(|b| match b {
            b'"' | b'\\' | b'\x08' | b'\t' | b'\n' | b'\x0c' | b'\r' => 2,
            0x00..=0x1f => 6,
            _ => 1,
        })
        .sum::<usize>()
}

/// A subgraph built by [`build_schema`]: the executable schema, the
/// committed SDL it advertises, and the limits it enforces.
#[derive(Clone)]
pub struct SubgraphSchema {
    schema: Schema,
    sdl: Arc<str>,
    /// What one copy of `sdl` adds to a response, escaped.
    sdl_bytes: usize,
    limits: Limits,
}

/// Execute a request against a subgraph built by [`build_schema`].
///
/// `_service { sdl }` answers with the committed `schema.graphql`, not the
/// executable schema's own rendering: a gateway that composes from live
/// subgraphs then sees exactly what `check` and `compose` validated,
/// directives such as `@override`, `@inaccessible`, and `@tag` included.
///
/// Introspection (`__schema`, `__type`) is refused: a gateway composes from
/// `_service`, clients introspect the gateway, and aliases would repeat the
/// schema's descriptions without bound. So is a request selecting more
/// copies of the SDL than [`Limits::max_response_bytes`] holds.
///
/// Federation answers an unresolvable `_entities` representation with a
/// `null` item, but `async-graphql`'s dynamic executor cannot return a null
/// member of the `_Entity` union list. Missing entities are therefore
/// resolved as nodes whose fields error, recorded, and replaced by `null`
/// (with one error each) here, after execution.
pub async fn execute(
    schema: &SubgraphSchema,
    request: async_graphql::Request,
) -> async_graphql::Response {
    let state = Arc::new(RequestState::default());
    // Parsed a second time only when it may select `_service` or
    // introspection; gateway entity fetches do not.
    let doc = mentions_meta_fields(&request.query)
        .then(|| async_graphql::parser::parse_query(&request.query).ok())
        .flatten();
    let op = doc
        .as_ref()
        .and_then(|doc| selected_query(doc, request.operation_name.as_deref()).map(|op| (doc, op)));
    if let Some((doc, op)) = op {
        if let Err(refusal) = admit(schema, doc, op, &state) {
            return async_graphql::Response::from_errors(vec![ServerError::new(refusal, None)]);
        }
    }
    let mut response = schema
        .schema
        .execute(request.data(Arc::clone(&state)))
        .await;
    if state.exhausted.load(std::sync::atomic::Ordering::SeqCst) {
        let message = state
            .exceeded
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .unwrap_or_else(|| too_large(&schema.limits));
        return async_graphql::Response::from_errors(vec![ServerError::new(message, None)]);
    }
    if let Some((doc, op)) = op {
        advertise_committed_sdl(doc, op, &mut response.data, &schema.sdl);
    }
    let missing = state.take_missing();
    if missing.is_empty() {
        return within_budget(response, &schema.limits);
    }
    if let Value::Object(data) = &mut response.data {
        for m in &missing {
            if let Some(Value::List(items)) = data.get_mut(m.response_key.as_str()) {
                if let Some(item) = items.get_mut(m.index) {
                    *item = Value::Null;
                }
            }
        }
    }
    // Field errors from the dynamic executor carry no path, so drop the
    // per-field copies of each "does not exist" error by message as well.
    // Indexed, so cleanup stays linear in errors + missing items.
    let items: std::collections::HashSet<(&str, usize)> = missing
        .iter()
        .map(|m| (m.response_key.as_str(), m.index))
        .collect();
    let messages: std::collections::HashSet<&str> =
        missing.iter().map(|m| m.message.as_str()).collect();
    response.errors.retain(|error| {
        let under_missing_item = matches!(
            error.path.as_slice(),
            [PathSegment::Field(key), PathSegment::Index(i), ..]
                if items.contains(&(key.as_str(), *i))
        );
        !under_missing_item && !messages.contains(error.message.as_str())
    });
    for m in missing {
        let mut error = ServerError::new(m.message, None);
        error.path = vec![
            PathSegment::Field(m.response_key),
            PathSegment::Index(m.index),
        ];
        response.errors.push(error);
    }
    within_budget(response, &schema.limits)
}

/// The answer itself, measured as encoded: what resolvers charge is an
/// estimate, and root metadata (`__typename`) is charged by none.
fn within_budget(response: async_graphql::Response, limits: &Limits) -> async_graphql::Response {
    struct Count(usize);
    impl std::io::Write for Count {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Count(0);
    let _ = serde_json::to_writer(&mut count, &response);
    if count.0 <= limits.max_response_bytes {
        return response;
    }
    async_graphql::Response::from_errors(vec![ServerError::new(too_large(limits), None)])
}

/// Whether `query` may select `_service`, `__schema`, or `__type` (not
/// `__typename`); a cheap test before parsing it.
fn mentions_meta_fields(query: &str) -> bool {
    query.contains("_service")
        || query.contains("__schema")
        || query
            .match_indices("__type")
            .any(|(at, found)| !query[at + found.len()..].starts_with("name"))
}

/// The query operation a request with `operation` runs in `doc`, if any.
fn selected_query<'a>(
    doc: &'a ExecutableDocument,
    operation: Option<&str>,
) -> Option<&'a OperationDefinition> {
    let op = match (&doc.operations, operation) {
        (DocumentOperations::Single(op), _) => &op.node,
        (DocumentOperations::Multiple(ops), Some(name)) => &ops.get(name)?.node,
        (DocumentOperations::Multiple(ops), None) if ops.len() == 1 => &ops.values().next()?.node,
        _ => return None,
    };
    (op.ty == OperationType::Query).then_some(op)
}

/// Refuse what [`execute`] does not answer, and charge the SDL copies
/// `op` selects to the request.
fn admit(
    schema: &SubgraphSchema,
    doc: &ExecutableDocument,
    op: &OperationDefinition,
    state: &RequestState,
) -> std::result::Result<(), String> {
    let root = &op.selection_set.node;
    if ["__schema", "__type"]
        .iter()
        .any(|name| !fields_named(root, name, doc).is_empty())
    {
        return Err(
            "subgraphs do not answer introspection (`__schema`, `__type`); introspect the gateway, or read `_service { sdl }`"
                .to_string(),
        );
    }
    let copies: usize = fields_named(root, "_service", doc)
        .iter()
        .map(|service| fields_named(&service.selection_set.node, "sdl", doc).len())
        .sum();
    let bytes = copies.saturating_mul(schema.sdl_bytes);
    if bytes > schema.limits.max_response_bytes {
        return Err(format!(
            "{copies} copies of `_service {{ sdl }}` exceed the {}-byte answer budget; select it once",
            schema.limits.max_response_bytes
        ));
    }
    state
        .bytes
        .fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

/// Replace the value of every `_service { sdl }` that `op` selects with
/// `sdl`, following aliases and fragments.
fn advertise_committed_sdl(
    doc: &ExecutableDocument,
    op: &OperationDefinition,
    data: &mut Value,
    sdl: &str,
) {
    let Value::Object(data) = data else {
        return;
    };
    for service in fields_named(&op.selection_set.node, "_service", doc) {
        let Some(Value::Object(service_value)) = data.get_mut(service.response_key().node.as_str())
        else {
            continue;
        };
        for field in fields_named(&service.selection_set.node, "sdl", doc) {
            if let Some(value @ Value::String(_)) =
                service_value.get_mut(field.response_key().node.as_str())
            {
                *value = Value::String(sdl.to_string());
            }
        }
    }
}

/// The fields named `name` in `set`, looking through fragments.
fn fields_named<'a>(
    set: &'a SelectionSet,
    name: &str,
    doc: &'a ExecutableDocument,
) -> Vec<&'a QueryField> {
    fn walk<'a>(
        set: &'a SelectionSet,
        name: &str,
        doc: &'a ExecutableDocument,
        seen: &mut std::collections::HashSet<&'a str>,
        out: &mut Vec<&'a QueryField>,
    ) {
        for item in &set.items {
            match &item.node {
                Selection::Field(field) => {
                    if field.node.name.node.as_str() == name {
                        out.push(&field.node);
                    }
                }
                Selection::InlineFragment(fragment) => {
                    walk(&fragment.node.selection_set.node, name, doc, seen, out);
                }
                Selection::FragmentSpread(spread) => {
                    let fragment = &spread.node.fragment_name.node;
                    if seen.insert(fragment.as_str()) {
                        if let Some(definition) = doc.fragments.get(fragment) {
                            walk(&definition.node.selection_set.node, name, doc, seen, out);
                        }
                    }
                }
            }
        }
    }
    let mut out = Vec::new();
    walk(
        set,
        name,
        doc,
        &mut std::collections::HashSet::new(),
        &mut out,
    );
    out
}

/// A value flowing through resolvers.
#[derive(Clone)]
enum Node {
    Entity(EntityNode),
    /// A value object embedded in a record.
    Inline(Arc<Record>),
}

#[derive(Clone)]
struct EntityNode {
    type_name: Arc<str>,
    key: String,
    record: Arc<OnceCell<Option<Arc<Record>>>>,
    /// An `_entities` representation this subgraph must own a record for
    /// but does not: every field, including the key, reports it.
    missing: bool,
}

impl EntityNode {
    fn reference(type_name: &str, key: String) -> Self {
        Self {
            type_name: Arc::from(type_name),
            key,
            record: Arc::new(OnceCell::new()),
            missing: false,
        }
    }

    fn missing(type_name: &str, key: String) -> Self {
        let mut node = Self::reference(type_name, key);
        node.missing = true;
        node
    }

    fn not_found(&self, store: &dyn EntityStore) -> async_graphql::Error {
        async_graphql::Error::new(format!(
            "{}:{} does not exist in {}",
            self.type_name,
            self.key,
            store.describe()
        ))
    }

    fn loaded(type_name: &str, key: String, record: Arc<Record>) -> Self {
        let node = Self::reference(type_name, key);
        let _ = node.record.set(Some(record));
        node
    }

    async fn record(&self, store: &dyn EntityStore) -> GqlResult<Option<Arc<Record>>> {
        let record = self
            .record
            .get_or_try_init(|| async { store.get(&self.type_name, &self.key).await })
            .await
            .map_err(|e| gql_error(&e))?;
        Ok(record.clone())
    }
}

impl Node {
    /// How many items `field` holds when it is a list, read without copying
    /// it.
    async fn list_len(&self, field: &str, store: &dyn EntityStore) -> GqlResult<Option<usize>> {
        let len = |record: &Record| record.get(field).and_then(Json::as_array).map(Vec::len);
        match self {
            Node::Inline(map) => Ok(len(map)),
            Node::Entity(entity) if entity.missing => Err(entity.not_found(store)),
            Node::Entity(entity) => Ok(entity.record(store).await?.and_then(|r| len(&r))),
        }
    }

    /// Raw JSON value of `field`, or `None` when absent.
    async fn field(
        &self,
        field: &str,
        key_field: Option<&str>,
        store: &dyn EntityStore,
    ) -> GqlResult<Option<Json>> {
        match self {
            Node::Inline(map) => Ok(map.get(field).cloned()),
            Node::Entity(entity) => {
                if entity.missing {
                    return Err(entity.not_found(store));
                }
                if key_field == Some(field) {
                    return Ok(Some(Json::String(entity.key.clone())));
                }
                Ok(entity
                    .record(store)
                    .await?
                    .and_then(|record| record.get(field).cloned()))
            }
        }
    }
}

/// Shared by every resolver of one subgraph.
struct Runtime {
    sub: String,
    model: SubOntologyModel,
    store: Arc<dyn EntityStore>,
    limits: Limits,
    /// The longest `__typename` answer an object of this subgraph gives.
    typename_bytes: usize,
}

/// Build the subgraph schema for one local sub-ontology, with default
/// [`Limits`].
pub fn build_schema(sub: &str, sdl: &str, store: Arc<dyn EntityStore>) -> Result<SubgraphSchema> {
    build_schema_with_limits(sub, sdl, store, Limits::default())
}

/// [`build_schema`] with explicit [`Limits`].
pub fn build_schema_with_limits(
    sub: &str,
    sdl: &str,
    store: Arc<dyn EntityStore>,
    limits: Limits,
) -> Result<SubgraphSchema> {
    let model = SubOntologyModel::parse(sdl).map_err(|e| Error::serve(sub, e))?;
    if !model.unsupported.is_empty() {
        return Err(Error::serve(sub, model.unsupported.join("; ")));
    }
    if model.mutation_type.is_some() {
        return Err(Error::serve(sub, "mutations are not served yet"));
    }
    for entity in model.entities() {
        if entity.key_field().is_none() {
            return Err(Error::serve(
                sub,
                format!("`{}` needs a single-field @key to be served", entity.name),
            ));
        }
        let key_type = entity
            .key_field()
            .and_then(|k| entity.field(k))
            .map(|f| f.ty.named.as_str());
        if !matches!(key_type, Some("ID" | "String" | "Int")) {
            return Err(Error::serve(
                sub,
                format!(
                    "`{}` is keyed by a field of type {}; entity keys must be ID, String, or Int",
                    entity.name,
                    key_type.unwrap_or("(undeclared)")
                ),
            ));
        }
        if !entity.additional_keys.is_empty() {
            return Err(Error::serve(
                sub,
                format!(
                    "`{}` declares more than one @key; entities are resolved by their first key only",
                    entity.name
                ),
            ));
        }
    }

    let typename_bytes = model
        .objects
        .keys()
        .map(|name| json_string_bytes(name))
        .max()
        .unwrap_or(0);
    let rt = Arc::new(Runtime {
        sub: sub.to_string(),
        model,
        store,
        limits,
        typename_bytes,
    });
    let query_name = rt
        .model
        .query_type
        .clone()
        .unwrap_or_else(|| "Query".into());
    let mut builder = Schema::build(&query_name, None, None)
        .enable_federation()
        .limit_depth(limits.depth)
        .limit_complexity(limits.complexity);

    for scalar in &rt.model.scalars {
        builder = builder.register(Scalar::new(scalar));
    }
    for (name, values) in &rt.model.enums {
        builder = builder.register(Enum::new(name).items(values.iter().map(String::as_str)));
    }
    for object in rt.model.domain_objects() {
        builder = builder.register(build_object(&rt, object)?);
    }
    builder = builder.register(build_query(&rt, &query_name)?);

    let entity_rt = Arc::clone(&rt);
    builder = builder.entity_resolver(move |ctx| {
        let rt = Arc::clone(&entity_rt);
        FieldFuture::new(async move { resolve_entities(&rt, &ctx).await })
    });

    let schema = builder
        .finish()
        .map_err(|e| Error::serve(sub, format!("schema construction failed: {e}")))?;
    Ok(SubgraphSchema {
        schema,
        sdl: Arc::from(sdl),
        sdl_bytes: json_string_bytes(sdl),
        limits,
    })
}

fn build_object(rt: &Arc<Runtime>, object: &ObjectType) -> Result<Object> {
    let mut obj = Object::new(&object.name);
    if let Some(description) = &object.description {
        obj = obj.description(description);
    }
    if let Some(key) = &object.key {
        obj = if key.resolvable {
            obj.key(&key.fields)
        } else {
            obj.unresolvable(&key.fields)
        };
    }
    if object.shareable {
        obj = obj.shareable();
    }
    for field in &object.fields {
        if !field.args.is_empty() {
            return Err(Error::serve(
                &rt.sub,
                format!(
                    "`{}.{}`: arguments are only supported on root fields",
                    object.name, field.name
                ),
            ));
        }
        obj = obj.field(build_field(rt, object, field)?);
    }
    Ok(obj)
}

fn build_field(rt: &Arc<Runtime>, parent: &ObjectType, field: &FieldDef) -> Result<Field> {
    let ty = type_ref(&field.ty);
    let mut built = if field.external {
        // Owned by another subgraph; without @requires/@provides support the
        // gateway never asks this subgraph for it.
        let name = field.name.clone();
        Field::new(&field.name, ty, move |_| {
            let name = name.clone();
            FieldFuture::new(async move {
                Err::<Option<FieldValue>, _>(async_graphql::Error::new(format!(
                    "`{name}` is external to this subgraph"
                )))
            })
        })
        .external()
    } else if let Some(of) = &field.inverse_of {
        inverse_field(rt, parent, field, of)?
    } else {
        stored_field(rt, parent, field)
    };
    if field.shareable {
        built = built.shareable();
    }
    if let Some(description) = &field.description {
        built = built.description(description);
    }
    Ok(built)
}

fn stored_field(rt: &Arc<Runtime>, parent: &ObjectType, field: &FieldDef) -> Field {
    let rt = Arc::clone(rt);
    let key_field = parent.key_field().map(ToString::to_string);
    let (parent_name, name, ty) = (parent.name.clone(), field.name.clone(), field.ty.clone());
    Field::new(&field.name, type_ref(&field.ty), move |ctx| {
        let (rt, key_field, parent_name, name, ty) = (
            Arc::clone(&rt),
            key_field.clone(),
            parent_name.clone(),
            name.clone(),
            ty.clone(),
        );
        FieldFuture::new(async move {
            if exhausted(&ctx) {
                return Ok(placeholder(&rt.model, &ty));
            }
            let node = ctx.parent_value.try_downcast_ref::<Node>()?;
            // A stored list is capped and charged before it is copied.
            if ty.is_list() {
                if let Some(len) = node.list_len(&name, rt.store.as_ref()).await? {
                    if len > rt.limits.max_list {
                        return Err(async_graphql::Error::new(format!(
                            "`{parent_name}.{name}` holds {len} items; a list answers at most {}",
                            rt.limits.max_list
                        )));
                    }
                    charge_list(&ctx, &rt, len)?;
                }
            }
            let raw = node
                .field(&name, key_field.as_deref(), rt.store.as_ref())
                .await?;
            match raw {
                // An absent list reads as empty; an explicit `null` stays null.
                None if ty.is_list() => Ok(Some(FieldValue::list(Vec::<FieldValue>::new()))),
                None | Some(Json::Null) if ty.non_null => {
                    // Say which entity and store instead of a bare non-null error.
                    let subject = match node {
                        Node::Entity(e) => format!("{}:{}", e.type_name, e.key),
                        Node::Inline(_) => format!("a `{parent_name}` value"),
                    };
                    Err(async_graphql::Error::new(format!(
                        "{subject} has no value for required field `{parent_name}.{name}` in {}",
                        rt.store.describe()
                    )))
                }
                None | Some(Json::Null) => Ok(None),
                Some(value) => {
                    charge_bytes(&ctx, &rt.limits, answer_bytes(&rt.model, &ty.named, &value))?;
                    to_field_value(&rt.model, &ty, value).map(Some)
                }
            }
        })
    })
}

/// `Person.papers: [Paper!]! @inverse(of: "authors")` lists the `Paper`
/// records whose `authors` references the person.
fn inverse_field(
    rt: &Arc<Runtime>,
    parent: &ObjectType,
    field: &FieldDef,
    of: &str,
) -> Result<Field> {
    let target = rt.model.object(&field.ty.named).filter(|o| o.key.is_some());
    let invalid = |why: &str| {
        Err(Error::serve(
            &rt.sub,
            format!(
                "`{}.{} @inverse(of: \"{of}\")`: {why}",
                parent.name, field.name
            ),
        ))
    };
    if parent.key.is_none() {
        return invalid(&format!(
            "`{}` has no @key; only entities have inverse relations",
            parent.name
        ));
    }
    let Some(target) = target else {
        return invalid("the field type must be an entity of this sub-ontology");
    };
    if !field.ty.is_list() {
        return invalid("inverse fields must be lists");
    }
    let Some(source) = target.field(of) else {
        return invalid(&format!("`{}` has no field `{of}`", target.name));
    };
    if source.ty.named != parent.name || !source.is_stored() {
        return invalid(&format!(
            "`{}.{of}` must be a stored relation to `{}`",
            target.name, parent.name
        ));
    }
    let Some(target_key) = target.key_field().map(ToString::to_string) else {
        return invalid("the target entity needs a single-field @key");
    };

    let rt = Arc::clone(rt);
    let (target_name, of) = (target.name.clone(), of.to_string());
    Ok(Field::new(&field.name, type_ref(&field.ty), move |ctx| {
        let (rt, target_name, target_key, of) = (
            Arc::clone(&rt),
            target_name.clone(),
            target_key.clone(),
            of.clone(),
        );
        FieldFuture::new(async move {
            if exhausted(&ctx) {
                return Ok(Some(FieldValue::list(Vec::<FieldValue>::new())));
            }
            let Node::Entity(entity) = ctx.parent_value.try_downcast_ref::<Node>()? else {
                return Err(async_graphql::Error::new(
                    "inverse relations need an entity parent",
                ));
            };
            if entity.missing {
                return Err(entity.not_found(rt.store.as_ref()));
            }
            let query = ListQuery {
                references: Some((of.clone(), entity.key.clone())),
                limit: Some(rt.limits.max_inverse + 1),
            };
            let records = rt
                .store
                .list(&target_name, &query)
                .await
                .map_err(|e| gql_error(&e))?;
            if records.len() > rt.limits.max_inverse {
                return Err(async_graphql::Error::new(format!(
                    "{}:{} has more than {} `{target_name}.{of}` references; inverse relations are not paginated yet",
                    entity.type_name, entity.key, rt.limits.max_inverse
                )));
            }
            charge_list(&ctx, &rt, records.len())?;
            Ok(Some(FieldValue::list(entity_nodes(
                &target_name,
                &target_key,
                records,
            ))))
        })
    }))
}

fn build_query(rt: &Arc<Runtime>, query_name: &str) -> Result<Object> {
    let mut query = Object::new(query_name);
    let Some(object) = rt.model.object(query_name) else {
        // Entity-only subgraph: `_entities` / `_service` are added by federation.
        return Ok(query);
    };
    for field in &object.fields {
        query = query.field(root_field(rt, field)?);
    }
    Ok(query)
}

/// Why a root field is neither a lookup nor a list of an entity.
fn unsupported_root(rt: &Runtime, field: &FieldDef) -> Error {
    Error::serve(
        &rt.sub,
        format!(
            "`Query.{}: {}` is neither a lookup (`{}(key: ID!): T`) nor a list (`{}(first: Int): [T!]!`) of an entity",
            field.name, field.ty, field.name, field.name
        ),
    )
}

/// The entity a root field returns, and its key field: an entity this
/// sub-ontology owns. An extension's records are optional, so its store
/// can neither confirm that an entity exists nor enumerate them.
fn root_entity<'a>(rt: &'a Runtime, field: &FieldDef) -> Result<(&'a ObjectType, String)> {
    let TypeKind::Entity(entity) = rt.model.kind_of(&field.ty.named) else {
        return Err(unsupported_root(rt, field));
    };
    if entity.extension {
        return Err(Error::serve(
            &rt.sub,
            format!(
                "`Query.{}` returns `{}`, which this sub-ontology only extends; root fields belong in the sub-ontology that owns it",
                field.name, entity.name
            ),
        ));
    }
    let key_field = entity
        .key_field()
        .map(ToString::to_string)
        .ok_or_else(|| unsupported_root(rt, field))?;
    Ok((entity, key_field))
}

fn root_field(rt: &Arc<Runtime>, field: &FieldDef) -> Result<Field> {
    let unsupported = || unsupported_root(rt, field);
    let (entity, key_field) = root_entity(rt, field)?;
    let type_name = entity.name.clone();
    let rt_c = Arc::clone(rt);

    let mut built = if field.ty.is_list() {
        if field
            .args
            .iter()
            .any(|a| a.name != "first" || a.ty.named != "Int" || a.ty.is_list())
        {
            return Err(unsupported());
        }
        Field::new(&field.name, type_ref(&field.ty), move |ctx| {
            let (rt, type_name, key_field) =
                (Arc::clone(&rt_c), type_name.clone(), key_field.clone());
            FieldFuture::new(async move {
                if exhausted(&ctx) {
                    return Ok(Some(FieldValue::list(Vec::<FieldValue>::new())));
                }
                let limit = match ctx.args.get("first") {
                    Some(v) if !v.is_null() => usize::try_from(v.i64()?.max(0))
                        .map_or(rt.limits.max_list, |n| n.min(rt.limits.max_list)),
                    _ => rt.limits.default_list,
                };
                let records = rt
                    .store
                    .list(
                        &type_name,
                        &ListQuery {
                            references: None,
                            limit: Some(limit),
                        },
                    )
                    .await
                    .map_err(|e| gql_error(&e))?;
                charge_list(&ctx, &rt, records.len())?;
                Ok(Some(FieldValue::list(entity_nodes(
                    &type_name, &key_field, records,
                ))))
            })
        })
    } else {
        let [arg] = field.args.as_slice() else {
            return Err(unsupported());
        };
        // A key may name nothing: `Person!` would turn one missing entity
        // into an error that nulls the whole answer.
        if field.ty.non_null {
            return Err(Error::serve(
                &rt.sub,
                format!(
                    "`Query.{}` returns `{}`, but a lookup answers null for a key with no entity; declare it `{type_name}`",
                    field.name, field.ty
                ),
            ));
        }
        // The argument is the key: only the key scalars can name one.
        if arg.ty.is_list()
            || !arg.ty.non_null
            || !matches!(arg.ty.named.as_str(), "ID" | "String" | "Int")
        {
            return Err(unsupported());
        }
        // ...and it must spell every key: an Int cannot name the key `alice`.
        let key_type = entity.field(&key_field).map(|f| f.ty.named.as_str());
        if arg.ty.named == "Int" && key_type != Some("Int") {
            return Err(Error::serve(
                &rt.sub,
                format!(
                    "`Query.{}({}: Int!)` cannot look up every `{type_name}`: its key `{key_field}` is {}, so the argument must be ID! or String!",
                    field.name,
                    arg.name,
                    key_type.unwrap_or("not a scalar")
                ),
            ));
        }
        let arg_name = arg.name.clone();
        Field::new(&field.name, type_ref(&field.ty), move |ctx| {
            let (rt, type_name, arg_name) =
                (Arc::clone(&rt_c), type_name.clone(), arg_name.clone());
            FieldFuture::new(async move {
                if exhausted(&ctx) {
                    return Ok(None);
                }
                let key = key_string(ctx.args.try_get(&arg_name)?.as_value())?;
                let record = rt
                    .store
                    .get(&type_name, &key)
                    .await
                    .map_err(|e| gql_error(&e))?;
                Ok(record.map(|r| {
                    FieldValue::owned_any(Node::Entity(EntityNode::loaded(&type_name, key, r)))
                }))
            })
        })
    };
    for arg in &field.args {
        built = built.argument(root_argument(rt, field, arg)?);
    }
    if let Some(description) = &field.description {
        built = built.description(description);
    }
    Ok(built)
}

/// A root field argument, with the default the schema declares.
fn root_argument(rt: &Runtime, field: &FieldDef, arg: &ArgDef) -> Result<InputValue> {
    let input = InputValue::new(&arg.name, type_ref(&arg.ty));
    let Some(default) = &arg.default else {
        return Ok(input);
    };
    let value = Value::from_json(default.clone()).map_err(|e| {
        Error::serve(
            &rt.sub,
            format!("`Query.{}({}:)` default: {e}", field.name, arg.name),
        )
    })?;
    Ok(input.default_value(value))
}

async fn resolve_entities<'a>(
    rt: &Runtime,
    ctx: &ResolverContext<'a>,
) -> GqlResult<Option<FieldValue<'a>>> {
    if exhausted(ctx) {
        return Ok(Some(FieldValue::list(Vec::<FieldValue>::new())));
    }
    let representations = ctx.args.try_get("representations")?.list()?;
    let state = ctx.data_opt::<Arc<RequestState>>();
    charge_entities(rt, ctx, state, representations.len())?;
    let response_key = ctx.item.node.response_key().node.to_string();
    let mut out = Vec::with_capacity(representations.len());
    let mut looked_up: std::collections::HashMap<(String, String), Option<Arc<Record>>> =
        std::collections::HashMap::new();
    for (index, representation) in representations.iter().enumerate() {
        let representation = representation.object()?;
        let type_name = representation.try_get("__typename")?.string()?;
        let object = rt
            .model
            .object(type_name)
            .filter(|o| o.key.is_some())
            .ok_or_else(|| {
                async_graphql::Error::new(format!(
                    "`{type_name}` is not an entity of this subgraph"
                ))
            })?;
        let key_field = object.key_field().ok_or_else(|| {
            async_graphql::Error::new(format!("`{type_name}` has no single-field key"))
        })?;
        let key = key_string(representation.try_get(key_field)?.as_value())?;

        // A sub-ontology that declares a plain `type X @key` owns X's
        // records: a key it has no record for does not exist. An extension
        // (`extend type X` / `@extends`) adds fields to an entity owned
        // elsewhere, so any key names one. Missing owned entities resolve
        // as nodes whose every field errors, and [`execute`] turns them
        // into `null` items.
        // One lookup per entity: a batch may name the same one often.
        let found = if object.extension {
            None
        } else {
            let id = (type_name.to_string(), key.clone());
            if let Some(found) = looked_up.get(&id) {
                found.clone()
            } else {
                let found = rt
                    .store
                    .get(type_name, &key)
                    .await
                    .map_err(|e| gql_error(&e))?;
                looked_up.insert(id, found.clone());
                found
            }
        };
        let node = if object.extension {
            EntityNode::reference(type_name, key)
        } else if let Some(record) = found {
            EntityNode::loaded(type_name, key, record)
        } else {
            let node = EntityNode::missing(type_name, key);
            if let Some(state) = state {
                let message = node.not_found(rt.store.as_ref()).message;
                // Its error names the field by its alias, which is as long
                // as the client likes: charged before it is copied.
                charge_bytes(
                    ctx,
                    &rt.limits,
                    2 * response_key.len() + json_string_bytes(&message) + 64,
                )?;
                state.record_missing(Missing {
                    response_key: response_key.clone(),
                    index,
                    message,
                });
            }
            node
        };
        out.push(FieldValue::owned_any(Node::Entity(node)).with_type(type_name.to_string()));
    }
    Ok(Some(FieldValue::list(out)))
}

/// Charge an `_entities` batch of `count` representations to the request:
/// the representations (see [`MAX_REPRESENTATIONS`]), the fields selected
/// under each (see [`MAX_WORK`]), and the response keys each repeats.
fn charge_entities(
    rt: &Runtime,
    ctx: &ResolverContext<'_>,
    state: Option<&Arc<RequestState>>,
    count: usize,
) -> GqlResult<()> {
    // Count across every `_entities` field of the request, so aliases
    // cannot multiply the budget.
    let total = match state {
        Some(state) => {
            state
                .representations
                .fetch_add(count, std::sync::atomic::Ordering::SeqCst)
                + count
        }
        None => count,
    };
    let exceed = |message: String| match state {
        Some(state) => state.exceed(message),
        None => async_graphql::Error::new(message),
    };
    if total > rt.limits.max_representations {
        return Err(exceed(format!(
            "{total} representations requested; at most {} per request",
            rt.limits.max_representations
        )));
    }
    // Bound representations × selected fields, so a big batch cannot also
    // select (or alias) many fields per entity.
    let per_entity = ctx
        .field()
        .selection_set()
        .map(count_fields)
        .sum::<usize>()
        .max(1);
    let keys = ctx
        .field()
        .selection_set()
        .map(|f| key_bytes(f, rt.typename_bytes))
        .sum::<usize>()
        + 2;
    charge_bytes(ctx, &rt.limits, count.saturating_mul(keys))?;
    let work = count.saturating_mul(per_entity);
    let total_work = match state {
        Some(state) => state
            .work
            .fetch_add(work, std::sync::atomic::Ordering::SeqCst)
            .saturating_add(work),
        None => work,
    };
    if total_work > rt.limits.max_work {
        return Err(exceed(format!(
            "{} representations × {per_entity} selected fields exceeds the per-request budget of {}",
            count,
            rt.limits.max_work
        )));
    }
    Ok(())
}

/// A selected field and everything selected beneath it.
fn count_fields(field: async_graphql::SelectionField<'_>) -> usize {
    1 + field.selection_set().map(count_fields).sum::<usize>()
}

/// The bytes `field` and everything beneath it put in each answer that
/// resolvers do not charge: response keys (its alias or name, quoted, with
/// a colon and a comma), and `__typename`'s value, at most `typename_bytes`.
fn key_bytes(field: async_graphql::SelectionField<'_>, typename_bytes: usize) -> usize {
    let value = if field.name() == "__typename" {
        typename_bytes
    } else {
        0
    };
    field.alias().unwrap_or_else(|| field.name()).len()
        + 4
        + value
        + field
            .selection_set()
            .map(|f| key_bytes(f, typename_bytes))
            .sum::<usize>()
}

fn entity_nodes(
    type_name: &str,
    key_field: &str,
    records: Vec<Arc<Record>>,
) -> Vec<FieldValue<'static>> {
    records
        .into_iter()
        .filter_map(|record| {
            let key = record
                .get(key_field)
                .and_then(|v| key_string_json(v).ok())?;
            Some(FieldValue::owned_any(Node::Entity(EntityNode::loaded(
                type_name, key, record,
            ))))
        })
        .collect()
}

/// Convert a stored JSON value into a resolver result for a field of type `ty`.
/// Convert a stored, non-null JSON value into a resolver result for a
/// field of type `ty`. Absence and explicit `null` are handled by the caller.
fn to_field_value(
    model: &SubOntologyModel,
    ty: &TypeExpr,
    raw: Json,
) -> GqlResult<FieldValue<'static>> {
    if ty.is_list() {
        let Json::Array(items) = raw else {
            return Err(async_graphql::Error::new(format!(
                "expected a list for `{ty}`"
            )));
        };
        let values = items
            .into_iter()
            .map(|item| item_value(model, &ty.named, item))
            .collect::<GqlResult<Vec<_>>>()?;
        return Ok(FieldValue::list(values));
    }
    item_value(model, &ty.named, raw)
}

fn item_value(model: &SubOntologyModel, named: &str, raw: Json) -> GqlResult<FieldValue<'static>> {
    if raw.is_null() {
        return Ok(FieldValue::NULL);
    }
    Ok(match model.kind_of(named) {
        TypeKind::Scalar if named == "ID" => {
            FieldValue::value(Value::String(key_string_json(&raw)?))
        }
        // Entity keys travel as strings (they are file names); an `Int` key
        // field still answers with a number.
        TypeKind::Scalar if named == "Int" && raw.is_string() => {
            let text = raw.as_str().unwrap_or_default();
            let n: i32 = text.parse().map_err(|_| {
                async_graphql::Error::new(format!("key `{text}` is not a valid Int"))
            })?;
            FieldValue::value(Value::from(n))
        }
        // Database stores hand over numbers too wide for a JSON number (a
        // NUMERIC, a 38-digit N) as their exact text.
        TypeKind::Scalar if named == "Float" && raw.is_string() => {
            let text = raw.as_str().unwrap_or_default();
            let n = text
                .parse::<f64>()
                .ok()
                .filter(|n| n.is_finite())
                .ok_or_else(|| {
                    async_graphql::Error::new(format!("`{text}` is not a valid Float"))
                })?;
            FieldValue::value(Value::from(n))
        }
        TypeKind::Scalar => FieldValue::value(
            Value::from_json(raw).map_err(|e| async_graphql::Error::new(e.to_string()))?,
        ),
        TypeKind::Enum(_) => match raw {
            Json::String(s) => FieldValue::value(Value::Enum(Name::new(s))),
            other => {
                return Err(async_graphql::Error::new(format!(
                    "`{named}` value must be a string, got {other}"
                )))
            }
        },
        TypeKind::Entity(target) => FieldValue::owned_any(Node::Entity(EntityNode::reference(
            &target.name,
            key_string_json(&raw)?,
        ))),
        TypeKind::Value(object) => match raw {
            Json::Object(map) => FieldValue::owned_any(Node::Inline(Arc::new(map))),
            other => {
                return Err(async_graphql::Error::new(format!(
                    "`{}` value must be a mapping, got {other}",
                    object.name
                )))
            }
        },
        TypeKind::Unknown => {
            return Err(async_graphql::Error::new(format!("unknown type `{named}`")))
        }
    })
}

fn key_string(value: &Value) -> GqlResult<String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) if n.is_i64() || n.is_u64() => Ok(n.to_string()),
        other => Err(async_graphql::Error::new(format!(
            "entity keys must be strings or integers, got {other}"
        ))),
    }
}

fn key_string_json(value: &Json) -> GqlResult<String> {
    match value {
        Json::String(s) => Ok(s.clone()),
        Json::Number(n) if n.is_i64() || n.is_u64() => Ok(n.to_string()),
        other => Err(async_graphql::Error::new(format!(
            "entity keys must be strings or integers, got {other}"
        ))),
    }
}

fn type_ref(ty: &TypeExpr) -> TypeRef {
    let named = ty.named.as_str();
    match (ty.list, ty.non_null) {
        (None, false) => TypeRef::named(named),
        (None, true) => TypeRef::named_nn(named),
        (Some(false), false) => TypeRef::named_list(named),
        (Some(true), false) => TypeRef::named_nn_list(named),
        (Some(false), true) => TypeRef::named_list_nn(named),
        (Some(true), true) => TypeRef::named_nn_list_nn(named),
    }
}

/// A store failure as a client sees it: that the store could not answer,
/// and a reference to the log line with the details. The details (a
/// database's schema, tables, and own errors) are the operator's.
fn gql_error(e: &Error) -> async_graphql::Error {
    let reference = error_reference();
    tracing::error!(error = %e, %reference, "ontology store error");
    async_graphql::Error::new(format!(
        "the ontology store could not answer (reference {reference}; the server log has the details)"
    ))
}

/// An opaque reference for one logged error: unique within the process,
/// and saying nothing about how many came before it.
fn error_reference() -> String {
    use std::hash::{BuildHasher, Hasher};
    static ERRORS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
    hasher.write_u64(ERRORS.fetch_add(1, std::sync::atomic::Ordering::Relaxed));
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::{test_support::commit_files, GitSnapshot};
    use crate::store::git::GitRecordStore;
    use serde_json::json;
    use std::collections::BTreeMap;

    const RESEARCH: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key", "@shareable"])
directive @inverse(of: String!) on FIELD_DEFINITION
enum Venue { CONFERENCE JOURNAL }
type Paper @key(fields: "id") {
  id: ID!
  title: String!
  year: Int
  venue: Venue
  authors: [Person!]!
  pages: PageRange
  tags: [String!]!
  aliases: [String!]
}
type PageRange @shareable { first: Int! last: Int! }
extend type Person @key(fields: "id") {
  id: ID!
  papers: [Paper!]! @inverse(of: "authors")
}
"Owned, with only a list field: existence still needs a record."
type Collection @key(fields: "id") {
  id: ID!
  members: [Paper!]!
}
type Query {
  paper(id: ID!): Paper
  papers(first: Int): [Paper!]!
}
"#;

    fn schema() -> (tempfile::TempDir, SubgraphSchema) {
        schema_with(Limits::default())
    }

    fn schema_with(limits: Limits) -> (tempfile::TempDir, SubgraphSchema) {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                (
                    "ontology/research/data/Paper/attention.yaml",
                    "title: Attention Is All You Need\nyear: 2017\nvenue: CONFERENCE\nauthors: [vaswani, shazeer]\npages: { first: 1, last: 11 }\n",
                ),
                ("ontology/research/data/Paper/moe.yaml", "title: Outrageously Large Neural Networks\nyear: 2017\nauthors: [shazeer]\naliases: null\n"),
                ("ontology/research/data/Paper/numeric.yaml", "title: Numeric Keys\nauthors: [42]\n"),
            ],
            "init",
        );
        let store = research_store(dir.path());
        let schema = build_schema_with_limits("research", RESEARCH, store, limits).unwrap();
        (dir, schema)
    }

    fn research_store(dir: &std::path::Path) -> Arc<GitRecordStore> {
        let keys = BTreeMap::from([
            ("Paper".to_string(), "id".to_string()),
            ("Person".to_string(), "id".to_string()),
        ]);
        Arc::new(GitRecordStore::new(
            Arc::new(GitSnapshot::open(dir, "HEAD").unwrap()),
            "ontology/research/data".into(),
            keys,
        ))
    }

    async fn run(schema: &SubgraphSchema, query: &str) -> Json {
        let response = execute(schema, query.into()).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        response.data.into_json().unwrap()
    }

    #[tokio::test]
    async fn lookup_list_relations_and_value_objects() {
        let (_dir, schema) = schema();
        let data = run(
            &schema,
            "{ paper(id: \"attention\") { id title year venue authors { id } pages { first last } tags } papers(first: 1) { id } }",
        )
        .await;
        assert_eq!(
            data,
            json!({
                "paper": {
                    "id": "attention", "title": "Attention Is All You Need", "year": 2017,
                    "venue": "CONFERENCE", "authors": [{"id": "vaswani"}, {"id": "shazeer"}],
                    "pages": {"first": 1, "last": 11}, "tags": []
                },
                "papers": [{"id": "attention"}]
            })
        );
        let missing = run(&schema, "{ paper(id: \"nope\") { id } }").await;
        assert_eq!(missing, json!({"paper": null}));
    }

    #[tokio::test]
    async fn entities_resolve_inverse_relations_by_reference() {
        let (_dir, schema) = schema();
        let data = run(
            &schema,
            r#"{ _entities(representations: [
                   {__typename: "Person", id: "shazeer"},
                   {__typename: "Paper", id: "moe"}
                 ]) {
                   ... on Person { id papers { id } }
                   ... on Paper { title }
                 } }"#,
        )
        .await;
        assert_eq!(
            data,
            json!({"_entities": [
                {"id": "shazeer", "papers": [{"id": "attention"}, {"id": "moe"}]},
                {"title": "Outrageously Large Neural Networks"}
            ]})
        );
    }

    #[tokio::test]
    async fn missing_owned_entities_are_null_items_with_one_error() {
        let (_dir, schema) = schema();
        for (typename, selection) in [
            ("Paper", "__typename"),
            ("Paper", "id"),
            ("Paper", "title year"),
            // Owned type whose only stored field is a list.
            ("Collection", "id members { id }"),
        ] {
            let query = format!(
                r#"{{ _entities(representations: [{{__typename: "Paper", id: "moe"}}, {{__typename: "{typename}", id: "missing"}}]) {{ __typename ... on {typename} {{ {selection} }} }} }}"#
            );
            let response = execute(&schema, query.as_str().into()).await;
            let data = response.data.into_json().unwrap();
            assert!(
                data["_entities"][0].is_object(),
                "{typename}/{selection}: existing entity kept: {data}"
            );
            assert_eq!(
                data["_entities"][1],
                Json::Null,
                "{typename}/{selection}: {data}"
            );
            assert_eq!(
                response.errors.len(),
                1,
                "{typename}/{selection}: {:?}",
                response.errors
            );
            let error = &response.errors[0];
            assert!(
                error
                    .message
                    .contains(&format!("{typename}:missing does not exist in git:")),
                "{}",
                error.message
            );
            assert_eq!(
                error.path,
                vec![
                    PathSegment::Field("_entities".into()),
                    PathSegment::Index(1)
                ]
            );
        }
    }

    #[tokio::test]
    async fn aliased_entity_fields_are_patched_at_their_own_path() {
        let (_dir, schema) = schema();
        let response = execute(
            &schema,
            r#"{
                 a: _entities(representations: [{__typename: "Paper", id: "missing"}]) { __typename }
                 _entities(representations: [{__typename: "Paper", id: "moe"}]) { __typename ... on Paper { id } }
               }"#
                .into(),
        )
        .await;
        let data = response.data.into_json().unwrap();
        assert_eq!(data["a"], json!([null]), "{data}");
        assert_eq!(
            data["_entities"],
            json!([{"__typename": "Paper", "id": "moe"}]),
            "{data}"
        );
        assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
        assert_eq!(
            response.errors[0].path,
            vec![PathSegment::Field("a".into()), PathSegment::Index(0)]
        );
    }

    #[tokio::test]
    async fn representation_budget_is_per_request_not_per_alias() {
        let (_dir, schema) = schema_with(Limits {
            max_representations: 2,
            ..Limits::default()
        });
        let response = execute(
            &schema,
            r#"{
                 a: _entities(representations: [{__typename: "Paper", id: "moe"}, {__typename: "Paper", id: "attention"}]) { __typename }
                 b: _entities(representations: [{__typename: "Paper", id: "moe"}, {__typename: "Paper", id: "attention"}]) { __typename }
               }"#
                .into(),
        )
        .await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("4 representations requested; at most 2")),
            "{:?}",
            response.errors
        );
    }

    #[tokio::test]
    async fn many_missing_entities_resolve_to_nulls_with_one_error_each() {
        let (_dir, schema) = schema();
        let representations: Vec<String> = (0..2_000)
            .map(|i| format!(r#"{{__typename: "Paper", id: "gone-{i}"}}"#))
            .collect();
        let query = format!(
            "{{ _entities(representations: [{}]) {{ __typename ... on Paper {{ title year venue }} }} }}",
            representations.join(", ")
        );
        let response = execute(&schema, query.as_str().into()).await;
        let data = response.data.into_json().unwrap();
        let items = data["_entities"].as_array().unwrap();
        assert_eq!(items.len(), 2_000);
        assert!(items.iter().all(Json::is_null));
        assert_eq!(response.errors.len(), 2_000, "one error per missing item");
    }

    #[test]
    fn answers_are_charged_as_encoded() {
        let model = SubOntologyModel::parse(RESEARCH).unwrap();
        for value in [
            json!("plain"),
            json!("a \" quote and a \\ backslash"),
            json!("controls \u{1}\u{7f}\n\t\r\u{8}\u{c}\u{0}\u{1f}"),
            json!("unicode é 🦀"),
            json!([]),
            json!(["a\u{1}", "b\"c", ""]),
        ] {
            assert_eq!(
                answer_bytes(&model, "String", &value),
                serde_json::to_string(&value).unwrap().len(),
                "{value}"
            );
        }
    }

    #[tokio::test]
    async fn aliases_repeated_per_item_are_charged() {
        let (_dir, schema) = schema_with(Limits {
            max_response_bytes: 64 * 1024,
            ..Limits::default()
        });
        let alias = "a".repeat(30 * 1024);
        // Three papers, each answering with the 30 KiB key: over 64 KiB,
        // whether the aliased field is stored or built in.
        for (field, root) in [
            ("title", "papers(first: 3)"),
            ("__typename", "papers(first: 3)"),
            (
                "__typename",
                r#"_entities(representations: [{__typename: "Paper", id: "moe"}, {__typename: "Paper", id: "attention"}, {__typename: "Paper", id: "numeric"}])"#,
            ),
        ] {
            let query = format!("{{ {root} {{ {alias}: {field} }} }}");
            let response = execute(&schema, query.as_str().into()).await;
            assert!(
                response
                    .errors
                    .iter()
                    .any(|e| e.message.contains("answer exceeds")),
                "{field} under {}: {:?}",
                &root[..6],
                response.errors.first()
            );
        }
        // A short alias fits.
        let data = run(&schema, "{ papers(first: 3) { t: title } }").await;
        assert_eq!(data["papers"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn store_failures_reach_clients_without_their_details() {
        struct Failing;
        #[async_trait::async_trait]
        impl EntityStore for Failing {
            fn describe(&self) -> String {
                "postgres:internal_schema@$DB_URL".into()
            }
            async fn get(&self, _: &str, _: &str) -> Result<Option<Arc<Record>>> {
                Err(Error::Store {
                    store: self.describe(),
                    message: "relation \"internal_schema.paper\" does not exist".into(),
                })
            }
            async fn list(&self, _: &str, _: &ListQuery) -> Result<Vec<Arc<Record>>> {
                Err(Error::Store {
                    store: self.describe(),
                    message: "permission denied for table paper".into(),
                })
            }
        }
        let schema = build_schema("research", RESEARCH, Arc::new(Failing)).unwrap();
        let mut references = std::collections::HashSet::new();
        for query in [
            r#"{ paper(id: "x") { title } }"#,
            "{ papers(first: 1) { title } }",
            r#"{ _entities(representations: [{__typename: "Paper", id: "x"}]) { ... on Paper { title } } }"#,
        ] {
            let response = execute(&schema, query.into()).await;
            assert!(!response.errors.is_empty(), "{query}");
            for error in &response.errors {
                let message = &error.message;
                assert!(message.contains("could not answer"), "{query}: {message}");
                for detail in ["internal_schema", "permission", "relation", "DB_URL"] {
                    assert!(!message.contains(detail), "{query}: {message}");
                }
                references.insert(message.clone());
            }
        }
        // Each failure gets its own reference.
        assert_eq!(references.len(), 3, "{references:?}");
    }

    #[tokio::test]
    async fn a_batch_looks_each_entity_up_once() {
        struct Counting {
            inner: Arc<GitRecordStore>,
            gets: std::sync::atomic::AtomicUsize,
        }
        #[async_trait::async_trait]
        impl EntityStore for Counting {
            fn describe(&self) -> String {
                self.inner.describe()
            }
            async fn get(&self, type_name: &str, key: &str) -> Result<Option<Arc<Record>>> {
                self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.inner.get(type_name, key).await
            }
            async fn list(&self, type_name: &str, query: &ListQuery) -> Result<Vec<Arc<Record>>> {
                self.inner.list(type_name, query).await
            }
        }
        let (dir, _) = schema();
        let store = Arc::new(Counting {
            inner: research_store(dir.path()),
            gets: std::sync::atomic::AtomicUsize::new(0),
        });
        let schema = build_schema("research", RESEARCH, Arc::clone(&store) as _).unwrap();
        let representations = vec![r#"{__typename: "Paper", id: "moe"}"#; 1_000].join(", ");
        let query =
            format!("{{ _entities(representations: [{representations}]) {{ __typename }} }}");
        let data = run(&schema, &query).await;
        assert_eq!(data["_entities"].as_array().unwrap().len(), 1_000);
        assert_eq!(store.gets.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn typenames_repeated_per_item_are_charged() {
        // A long (valid) type name, answered by `__typename` for each item.
        let name = format!("T{}", "a".repeat(199));
        let sdl = format!(
            "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\"])\ntype {name} @key(fields: \"id\") {{ id: ID! }}\ntype Query {{ things(first: Int): [{name}!]! }}"
        );
        let dir = tempfile::tempdir().unwrap();
        let records: Vec<(String, &str)> = ["x", "y", "z"]
            .iter()
            .map(|k| (format!("ontology/t/data/{name}/{k}.yaml"), ""))
            .collect();
        let files: Vec<(&str, &str)> = records.iter().map(|(p, c)| (p.as_str(), *c)).collect();
        commit_files(dir.path(), &files, "init");
        let store = Arc::new(GitRecordStore::new(
            Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap()),
            "ontology/t/data".into(),
            BTreeMap::from([(name.clone(), "id".to_string())]),
        ));
        // Three names of about 200 bytes do not fit 400 bytes; the keys do.
        let limits = Limits {
            max_response_bytes: 400,
            ..Limits::default()
        };
        let schema = build_schema_with_limits("t", &sdl, store, limits).unwrap();
        let response = execute(&schema, "{ things(first: 3) { __typename } }".into()).await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("answer exceeds 400 bytes")),
            "{:?}",
            response.errors
        );
        let response = execute(&schema, "{ things(first: 1) { __typename } }".into()).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }

    #[tokio::test]
    async fn an_exhausted_budget_ends_the_answer() {
        const ITEMS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Item @key(fields: "id") { id: ID! text: String refs: [Item!]! }
type Query { item(id: ID!): Item }
"#;
        // One record naming itself 200 times, with 1 KiB of text.
        let record = format!(
            "text: {}\nrefs: [{}]\n",
            "x".repeat(1024),
            vec!["a"; 200].join(", ")
        );
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[("ontology/t/data/Item/a.yaml", &record)],
            "init",
        );
        let store = Arc::new(GitRecordStore::new(
            Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap()),
            "ontology/t/data".into(),
            BTreeMap::from([("Item".to_string(), "id".to_string())]),
        ));
        let limits = Limits {
            max_response_bytes: 64 * 1024,
            ..Limits::default()
        };
        let schema = build_schema_with_limits("t", ITEMS, store, limits).unwrap();
        // Past the budget, every further item would report an error whose
        // path repeats the 1 KiB alias; instead the answer is one error.
        let alias = "b".repeat(1024);
        let query = format!(r#"{{ item(id: "a") {{ refs {{ {alias}: text }} }} }}"#);
        let response = execute(&schema, query.as_str().into()).await;
        assert_eq!(response.errors.len(), 1, "{:?}", response.errors.len());
        assert!(
            response.errors[0].message.contains("answer exceeds"),
            "{}",
            response.errors[0].message
        );
        assert_eq!(response.data, Value::Null);
    }

    #[tokio::test]
    async fn stored_lists_answer_at_most_one_list() {
        // `attention` stores two authors; one list may answer only one.
        let (_dir, schema) = schema_with(Limits {
            max_list: 1,
            ..Limits::default()
        });
        let response = execute(
            &schema,
            r#"{ paper(id: "attention") { authors { id } } }"#.into(),
        )
        .await;
        assert!(
            response.errors.iter().any(|e| e
                .message
                .contains("`Paper.authors` holds 2 items; a list answers at most 1")),
            "{:?}",
            response.errors
        );
    }

    #[tokio::test]
    async fn root_metadata_counts_toward_the_answer() {
        // A long (valid) query root name, answered by every `__typename`.
        let root = format!("R{}", "a".repeat(999));
        let sdl = format!(
            "schema {{ query: {root} }}\ntype Item @key(fields: \"id\") {{ id: ID! }}\ntype {root} {{ item(id: ID!): Item }}"
        );
        let limits = Limits {
            max_response_bytes: 50 * 1024,
            ..Limits::default()
        };
        let schema =
            build_schema_with_limits("t", &sdl, Arc::new(crate::store::NullStore), limits).unwrap();
        let aliases = (0..100)
            .map(|i| format!("a{i}: __typename"))
            .collect::<Vec<_>>()
            .join(" ");
        let response = execute(&schema, format!("{{ {aliases} }}").into()).await;
        assert_eq!(response.errors.len(), 1, "{:?}", response.errors.len());
        assert!(
            response.errors[0].message.contains("answer exceeds"),
            "{}",
            response.errors[0].message
        );
        let response = execute(&schema, "{ a: __typename }".into()).await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
    }

    #[tokio::test]
    async fn missing_entities_charge_their_errors_to_the_answer() {
        // Every missing item's error repeats the field's alias.
        let (_dir, schema) = schema_with(Limits {
            max_response_bytes: 64 * 1024,
            ..Limits::default()
        });
        let alias = "a".repeat(4096);
        let representations = (0..100)
            .map(|i| format!(r#"{{__typename: "Paper", id: "gone-{i}"}}"#))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!(
            "{{ {alias}: _entities(representations: [{representations}]) {{ __typename }} }}"
        );
        let response = execute(&schema, query.as_str().into()).await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("answer exceeds")),
            "{:?}",
            response.errors.first()
        );
        assert!(
            response.errors.len() < 20,
            "{} errors, not one per item",
            response.errors.len()
        );
    }

    #[tokio::test]
    async fn an_exceeded_limit_ends_the_whole_answer() {
        let (_dir, schema) = schema_with(Limits {
            max_list_items: 2,
            ..Limits::default()
        });
        // The first list goes over; nothing of the rest is answered either.
        let response = execute(
            &schema,
            "{ a: papers(first: 3) { id } b: papers(first: 1) { id } }".into(),
        )
        .await;
        assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
        assert!(
            response.errors[0]
                .message
                .contains("more than 2 list items"),
            "{}",
            response.errors[0].message
        );
        assert_eq!(response.data, Value::Null);
    }

    #[tokio::test]
    async fn wide_selections_on_big_batches_exceed_the_work_budget() {
        let (_dir, schema) = schema_with(Limits {
            max_work: 8,
            ..Limits::default()
        });
        // 2 representations × (__typename, title, year, venue, a: title) = 10 > 8
        let response = execute(
            &schema,
            r#"{ _entities(representations: [{__typename: "Paper", id: "moe"}, {__typename: "Paper", id: "attention"}]) {
                   __typename ... on Paper { title year venue a: title }
                 } }"#
                .into(),
        )
        .await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("exceeds the per-request budget of 8")),
            "{:?}",
            response.errors
        );
    }

    #[tokio::test]
    async fn wide_selections_on_long_lists_exceed_the_work_budget() {
        let (_dir, schema) = schema_with(Limits {
            max_work: 8,
            ..Limits::default()
        });
        let over_budget = |response: &async_graphql::Response| {
            response
                .errors
                .iter()
                .any(|e| e.message.contains("exceeds the per-request budget of 8"))
        };
        // 3 papers × 3 aliased fields = 9 > 8.
        let wide = execute(
            &schema,
            "{ papers(first: 10) { a: title b: title c: title } }".into(),
        )
        .await;
        assert!(over_budget(&wide), "{:?}", wide.errors);
        // Nested selections count: 3 × (pages, first, last) = 9 > 8.
        let nested = execute(
            &schema,
            "{ papers(first: 10) { pages { first last } } }".into(),
        )
        .await;
        assert!(over_budget(&nested), "{:?}", nested.errors);
        // Inverse lists are charged too: shazeer's 2 papers × 5 fields = 10 > 8.
        let inverse = execute(
            &schema,
            r#"{ _entities(representations: [{__typename: "Person", id: "shazeer"}]) {
                   ... on Person { papers { a: title b: title c: title d: title e: title } }
                 } }"#
                .into(),
        )
        .await;
        assert!(over_budget(&inverse), "{:?}", inverse.errors);
        // 3 papers × 2 fields = 6 fits.
        let within = execute(&schema, "{ papers(first: 10) { id title } }".into()).await;
        assert!(within.errors.is_empty(), "{:?}", within.errors);
    }

    #[tokio::test]
    async fn nested_relation_lists_are_bounded_per_request() {
        let (_dir, schema) = schema_with(Limits {
            max_list_items: 5,
            ..Limits::default()
        });
        // 3 papers + their authors (2 + 1 + 1) = 7 items > 5.
        let over = execute(
            &schema,
            "{ papers(first: 10) { id authors { id } } }".into(),
        )
        .await;
        assert!(
            over.errors
                .iter()
                .any(|e| e.message.contains("more than 5 list items")),
            "{:?}",
            over.errors
        );
        // 3 papers alone fit.
        let within = execute(&schema, "{ papers(first: 10) { id } }".into()).await;
        assert!(within.errors.is_empty(), "{:?}", within.errors);
    }

    #[test]
    fn keys_are_strings_or_integers() {
        assert_eq!(key_string_json(&json!(42)).unwrap(), "42");
        assert_eq!(key_string_json(&json!("42.5")).unwrap(), "42.5");
        assert!(key_string_json(&json!(42.5)).is_err());
        assert!(key_string(&Value::from(42.5)).is_err());
        assert_eq!(key_string(&Value::from(42)).unwrap(), "42");
    }

    #[tokio::test]
    async fn int_keys_answer_as_numbers() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[
                ("ontology/inv/data/Item/42.yaml", "label: answer\n"),
                ("ontology/inv/data/Item/-7.yaml", "label: below zero\n"),
            ],
            "init",
        );
        let sdl = r#"type Item @key(fields: "id") { id: Int! label: String! }
               type Query { item(id: Int!): Item }"#;
        let store = Arc::new(GitRecordStore::new(
            Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap()),
            "ontology/inv/data".into(),
            BTreeMap::from([("Item".to_string(), "id".to_string())]),
        ));
        let schema = build_schema("inv", sdl, store).unwrap();
        let data = run(&schema, "{ item(id: 42) { id label } }").await;
        assert_eq!(data, json!({"item": {"id": 42, "label": "answer"}}));
        let data = run(
            &schema,
            r#"{ _entities(representations: [{__typename: "Item", id: 42}]) { ... on Item { id } } }"#,
        )
        .await;
        assert_eq!(data, json!({"_entities": [{"id": 42}]}));
        let data = run(&schema, "{ item(id: -7) { id label } }").await;
        assert_eq!(data, json!({"item": {"id": -7, "label": "below zero"}}));
    }

    #[test]
    fn lookup_arguments_must_be_key_scalars() {
        let err = build_schema(
            "x",
            "type T @key(fields: \"id\") { id: ID! }\ntype Query { t(id: Boolean!): T }",
            Arc::new(crate::store::NullStore),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(err.contains("neither a lookup"), "{err}");
    }

    #[test]
    fn lookup_arguments_spell_every_key() {
        let build = |sdl: &str| build_schema("x", sdl, Arc::new(crate::store::NullStore));
        for key in ["ID", "String"] {
            let err = build(&format!(
                "type T @key(fields: \"id\") {{ id: {key}! }}\ntype Query {{ t(id: Int!): T }}"
            ))
            .err()
            .unwrap()
            .to_string();
            assert!(err.contains("must be ID! or String!"), "{key}: {err}");
        }
        // Every key scalar can spell an Int key.
        for arg in ["ID", "String", "Int"] {
            assert!(
                build(&format!(
                    "type T @key(fields: \"id\") {{ id: Int! }}\ntype Query {{ t(id: {arg}!): T }}"
                ))
                .is_ok(),
                "{arg}"
            );
        }
    }

    #[tokio::test]
    async fn string_keys_look_up_by_string() {
        let dir = tempfile::tempdir().unwrap();
        commit_files(
            dir.path(),
            &[("ontology/people/data/Member/alice.yaml", "name: Alice\n")],
            "init",
        );
        let sdl = r#"type Member @key(fields: "handle") { handle: String! name: String! }
               type Query { member(handle: String!): Member }"#;
        let store = Arc::new(GitRecordStore::new(
            Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap()),
            "ontology/people/data".into(),
            BTreeMap::from([("Member".to_string(), "handle".to_string())]),
        ));
        let schema = build_schema("people", sdl, store).unwrap();
        let data = run(&schema, r#"{ member(handle: "alice") { handle name } }"#).await;
        assert_eq!(
            data,
            json!({"member": {"handle": "alice", "name": "Alice"}})
        );
    }

    #[test]
    fn unsupported_key_types_are_unservable() {
        let err = build_schema(
            "x",
            "type T @key(fields: \"k\") { k: Float! }\ntype Query { t(k: Float!): T }",
            Arc::new(crate::store::NullStore),
        )
        .err()
        .unwrap()
        .to_string();
        assert!(
            err.contains("entity keys must be ID, String, or Int"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn oversized_entity_batches_are_rejected() {
        let (_dir, schema) = schema_with(Limits {
            max_representations: 2,
            ..Limits::default()
        });
        let response = execute(
            &schema,
            r#"{ _entities(representations: [
                   {__typename: "Paper", id: "moe"},
                   {__typename: "Paper", id: "attention"},
                   {__typename: "Paper", id: "numeric"}
                 ]) { ... on Paper { id } } }"#
                .into(),
        )
        .await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("3 representations requested; at most 2")),
            "{:?}",
            response.errors
        );
    }

    #[tokio::test]
    async fn inverse_overflow_is_an_error_not_a_partial_list() {
        let (_dir, schema) = schema_with(Limits {
            max_inverse: 1,
            ..Limits::default()
        });
        let response = execute(
            &schema,
            r#"{ _entities(representations: [{__typename: "Person", id: "shazeer"}]) { ... on Person { papers { id } } } }"#
                .into(),
        )
        .await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("more than 1 `Paper.authors` references")),
            "{:?}",
            response.errors
        );
    }

    #[tokio::test]
    async fn extension_entities_exist_by_reference() {
        let (_dir, schema) = schema();
        // research stores no required Person field, so any Person key is
        // valid here and simply has no papers.
        let data = run(
            &schema,
            r#"{ _entities(representations: [{__typename: "Person", id: "nobody"}]) { ... on Person { id papers { id } } } }"#,
        )
        .await;
        assert_eq!(data, json!({"_entities": [{"id": "nobody", "papers": []}]}));
    }

    #[tokio::test]
    async fn absent_lists_are_empty_and_explicit_nulls_stay_null() {
        let (_dir, schema) = schema();
        let data = run(
            &schema,
            r#"{ a: paper(id: "attention") { aliases } m: paper(id: "moe") { aliases } }"#,
        )
        .await;
        assert_eq!(data, json!({"a": {"aliases": []}, "m": {"aliases": null}}));
    }

    #[tokio::test]
    async fn numeric_relation_keys_resolve_inverse_relations() {
        let (_dir, schema) = schema();
        let data = run(
            &schema,
            r#"{ _entities(representations: [{__typename: "Person", id: "42"}]) { ... on Person { papers { id } } } }"#,
        )
        .await;
        assert_eq!(
            data,
            json!({"_entities": [{"papers": [{"id": "numeric"}]}]})
        );
    }

    #[tokio::test]
    async fn deep_queries_are_rejected() {
        let (_dir, schema) = schema();
        let mut query = String::from("{ paper(id: \"attention\") { ");
        for _ in 0..MAX_QUERY_DEPTH {
            query.push_str("authors { papers { ");
        }
        query.push_str("id");
        for _ in 0..MAX_QUERY_DEPTH {
            query.push_str(" } }");
        }
        query.push_str(" } }");
        let response = execute(&schema, query.as_str().into()).await;
        // Rejected before execution, by the depth limit or the parser's
        // recursion limit, whichever trips first.
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("nested too deep")
                    || e.message.contains("recursion limit")),
            "{:?}",
            response.errors
        );
        assert_eq!(response.data, async_graphql::Value::Null);
    }

    #[tokio::test]
    async fn service_sdl_is_the_committed_schema() {
        let (_dir, schema) = schema();
        let data = run(&schema, "{ _service { sdl } }").await;
        assert_eq!(data["_service"]["sdl"], json!(RESEARCH));
        // Through aliases, fragments, and a named operation too.
        let response = execute(
            &schema,
            async_graphql::Request::new(
                "query Other { __typename }
                 query Sdl { a: _service { b: sdl } ...Svc ... on Query { c: _service { sdl } } }
                 fragment Svc on Query { _service { ... on _Service { sdl } } }",
            )
            .operation_name("Sdl"),
        )
        .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let data = response.data.into_json().unwrap();
        assert_eq!(
            data,
            json!({"a": {"b": RESEARCH}, "_service": {"sdl": RESEARCH}, "c": {"sdl": RESEARCH}})
        );
    }

    #[tokio::test]
    async fn introspection_is_left_to_the_gateway() {
        let (_dir, schema) = schema();
        for query in [
            "{ __schema { types { name } } }",
            r#"{ a: __type(name: "Paper") { description } }"#,
            "query Q { ...F } fragment F on Query { __schema { queryType { name } } }",
        ] {
            let response = execute(&schema, query.into()).await;
            assert!(
                response.errors.len() == 1 && response.errors[0].message.contains("introspection"),
                "{query}: {:?}",
                response.errors
            );
        }
        // `__typename` is not introspection.
        let data = run(&schema, "{ __typename }").await;
        assert_eq!(data["__typename"], json!("Query"));
    }

    #[tokio::test]
    async fn aliases_cannot_multiply_the_answer() {
        // Room for one copy of the SDL, not two.
        let (_dir, schema) = schema_with(Limits {
            max_response_bytes: RESEARCH.len() * 3 / 2,
            ..Limits::default()
        });
        let data = run(&schema, "{ _service { sdl } }").await;
        assert_eq!(data["_service"]["sdl"], json!(RESEARCH));
        let response = execute(
            &schema,
            "{ a: _service { sdl } b: _service { sdl } }".into(),
        )
        .await;
        assert!(
            response.errors.len() == 1
                && response.errors[0]
                    .message
                    .contains("2 copies of `_service { sdl }`"),
            "{:?}",
            response.errors
        );

        // Record values are charged as they resolve: one title fits, twenty
        // aliases of it do not.
        let (_dir, schema) = schema_with(Limits {
            max_response_bytes: 200,
            ..Limits::default()
        });
        let data = run(&schema, r#"{ paper(id: "attention") { title } }"#).await;
        assert_eq!(data["paper"]["title"], json!("Attention Is All You Need"));
        let aliases = (0..20)
            .map(|i| format!("t{i}: title"))
            .collect::<Vec<_>>()
            .join(" ");
        let response = execute(
            &schema,
            format!(r#"{{ paper(id: "attention") {{ {aliases} }} }}"#).into(),
        )
        .await;
        assert!(
            response
                .errors
                .iter()
                .any(|e| e.message.contains("answer exceeds 200 bytes")),
            "{:?}",
            response.errors
        );
    }

    #[tokio::test]
    async fn federation_directives_reach_the_gateway() {
        // async-graphql's own rendering drops these; the committed SDL
        // keeps them.
        let sdl = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.7", import: ["@key", "@override", "@inaccessible", "@tag", "@shareable"])
type Paper @key(fields: "id") @tag(name: "public") {
  id: ID!
  title: String! @override(from: "legacy")
  internal: String @inaccessible
}
type Query { paper(id: ID!): Paper @shareable }
"#;
        let schema = build_schema("x", sdl, Arc::new(crate::store::NullStore)).unwrap();
        let data = run(&schema, "{ _service { sdl } }").await;
        assert_eq!(data["_service"]["sdl"], json!(sdl));
    }

    #[tokio::test]
    async fn declared_argument_defaults_apply() {
        let (dir, _) = schema();
        let sdl = RESEARCH.replace("papers(first: Int)", "papers(first: Int = 1)");
        let store = Arc::new(GitRecordStore::new(
            Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap()),
            "ontology/research/data".into(),
            BTreeMap::from([
                ("Paper".to_string(), "id".to_string()),
                ("Person".to_string(), "id".to_string()),
            ]),
        ));
        let schema = build_schema("research", &sdl, store).unwrap();
        let data = run(&schema, "{ papers { id } }").await;
        assert_eq!(data["papers"].as_array().unwrap().len(), 1, "{data}");
    }

    #[test]
    fn rejects_unservable_root_fields_and_inverses() {
        for root in ["person(id: ID!): Person", "people(first: Int): [Person!]!"] {
            let sdl = format!(
                "extend type Person @key(fields: \"id\") {{ id: ID! rating: Int }}\ntype Query {{ {root} }}"
            );
            let err = build_schema("x", &sdl, Arc::new(crate::store::NullStore))
                .err()
                .unwrap()
                .to_string();
            assert!(
                err.contains("returns `Person`, which this sub-ontology only extends"),
                "{root}: {err}"
            );
        }

        // A key may name nothing, which a non-null lookup cannot answer.
        let strict = "type T @key(fields: \"id\") { id: ID! }\ntype Query { t(id: ID!): T! }";
        let err = build_schema("x", strict, Arc::new(crate::store::NullStore))
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("returns `T!`") && err.contains("declare it `T`"),
            "{err}"
        );

        let bad_root = "type T @key(fields: \"id\") { id: ID! }\ntype Query { search(text: String!, limit: Int): [T!]! }";
        let err = build_schema("x", bad_root, Arc::new(crate::store::NullStore))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("neither a lookup"), "{err}");

        let value_inverse = "directive @inverse(of: String!) on FIELD_DEFINITION\ntype Item @key(fields: \"id\") { id: ID! value: Price }\ntype Price { amount: Int matches: [Item!]! @inverse(of: \"value\") }\ntype Query { item(id: ID!): Item }";
        let err = build_schema("x", value_inverse, Arc::new(crate::store::NullStore))
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("`Price` has no @key; only entities have inverse relations"),
            "{err}"
        );

        let bad_inverse = "directive @inverse(of: String!) on FIELD_DEFINITION\ntype A @key(fields: \"id\") { id: ID! bs: [B!]! @inverse(of: \"nope\") }\ntype B @key(fields: \"id\") { id: ID! }\ntype Query { a(id: ID!): A }";
        let err = build_schema("x", bad_inverse, Arc::new(crate::store::NullStore))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("has no field `nope`"), "{err}");
    }
}
