//! `DynamoDB` storage (build feature `dynamodb`): one table holds every
//! entity of a sub-ontology.
//!
//! # Item layout
//!
//! | Attribute | Type | Holds |
//! |---|---|---|
//! | `pk` | S, the table's only key (HASH) | `"<Type>#<key>"` |
//! | `type` | S | the entity type |
//! | any other | any | one stored field |
//!
//! Type names are GraphQL names and never contain `#`, so the first `#`
//! ends the type and keys may contain `#` themselves (`Tag#c#` is the `Tag`
//! keyed `c#`). `pk` and `type` are reserved: records never include them,
//! so no stored field can use either name. Every record carries its key
//! field, taken from `pk` when the item does not store it. Relations hold
//! target keys: an S or N, a list (L) of them, or a set (SS, NS). Keys are
//! strings or integers, so a decimal N is not one: following it is an
//! error, and inverse relations never list it.
//!
//! # Reads
//!
//! - `get` is a `GetItem` by `pk`.
//! - `list` **scans the table**, filtered on `type` by the server and
//!   following every page, then sorts by key and applies the limit.
//! - `references` (inverse relations) scan too, filtered with
//!   `contains(field, key)`. On a string `contains` matches substrings, so
//!   every match is re-checked with [`ListQuery::matches`].
//!
//! Every list reads, and bills, the whole table. That is fine for this
//! experiment; a large table would want a GSI on `type` and adjacency items
//! for references.
//!
//! # Values
//!
//! S → string; N → integer when it fits `i64` or `u64`, else float; BOOL;
//! NULL; L → array; M → object; SS and NS → arrays, sorted (a set has no
//! order); B and BS → base64 strings (standard alphabet, padded, as in
//! `DynamoDB`'s own JSON). [`to_item`] goes the other way, for seeding.
//!
//! # Connection
//!
//! Nothing connects until the first read or [`EntityStore::ready`], which
//! `serve` calls first. The client uses the standard AWS configuration:
//! credentials from the default chain (environment, profile, SSO, web
//! identity, container or instance role), never logged; the manifest's
//! `region` when set; and, when `endpoint_url_env` names a variable that is
//! set and not empty, that URL as the endpoint (`DynamoDB` Local).

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::error::DisplayErrorContext;
use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeyType, ScalarAttributeType, TableDescription,
};
use aws_sdk_dynamodb::Client;
use serde_json::Value as Json;
use tokio::sync::OnceCell;

use super::{EntityStore, ListQuery, Record};
use crate::error::{Error, Result};
use crate::manifest::DynamoConfig;

/// The partition key attribute: `"<Type>#<key>"`.
pub const PK: &str = "pk";
/// The entity type attribute.
pub const TYPE: &str = "type";
/// `DynamoDB`'s limit on a partition key value, in bytes.
const MAX_PK_BYTES: usize = 2048;
/// The `pk` `ready` looks up: no entity has it, since type names cannot
/// contain `-`.
const READY_PROBE: &str = "scriptorium-ontology#ready";

/// A sub-ontology's entities in one `DynamoDB` table.
/// How long connecting to the endpoint may take.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// How long one request to `DynamoDB`, retries included, may take. An
/// endpoint that stops answering must not hold a request, or `serve`'s
/// startup, forever; the SDK bounds only the connect by default.
const OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub struct DynamoStore {
    sub: String,
    table: String,
    region: Option<String>,
    endpoint_url_env: Option<String>,
    /// Entity type → key field name.
    key_fields: BTreeMap<String, String>,
    /// Built on first use, so opening the store needs neither network nor
    /// credentials.
    client: OnceCell<Client>,
    /// [`OPERATION_TIMEOUT`]; shorter in tests.
    operation_timeout: std::time::Duration,
}

impl DynamoStore {
    /// A store over `config.table`. Does no I/O.
    pub fn new(
        sub: &str,
        config: &DynamoConfig,
        key_fields: BTreeMap<String, String>,
    ) -> Result<Self> {
        for (setting, value) in [
            ("table", Some(config.table.as_str())),
            ("region", config.region.as_deref()),
            ("endpoint_url_env", config.endpoint_url_env.as_deref()),
        ] {
            if value.is_some_and(|v| v.trim().is_empty()) {
                return Err(Error::Store {
                    store: format!("dynamodb:{sub}"),
                    message: format!("`{setting}` must not be empty"),
                });
            }
        }
        Ok(Self {
            sub: sub.to_string(),
            table: config.table.clone(),
            region: config.region.clone(),
            endpoint_url_env: config.endpoint_url_env.clone(),
            key_fields,
            client: OnceCell::new(),
            operation_timeout: OPERATION_TIMEOUT,
        })
    }

    fn error(&self, message: impl Into<String>) -> Error {
        Error::Store {
            store: self.describe(),
            message: message.into(),
        }
    }

    fn key_field(&self, type_name: &str) -> Result<&str> {
        self.key_fields
            .get(type_name)
            .map(String::as_str)
            .ok_or_else(|| {
                self.error(format!(
                    "`{type_name}` is not an entity type of sub-ontology `{}`",
                    self.sub
                ))
            })
    }

    async fn client(&self) -> Result<&Client> {
        self.client
            .get_or_try_init(|| async {
                let timeouts = aws_config::timeout::TimeoutConfig::builder()
                    .connect_timeout(CONNECT_TIMEOUT)
                    .operation_timeout(self.operation_timeout)
                    .build();
                let mut loader =
                    aws_config::defaults(BehaviorVersion::latest()).timeout_config(timeouts);
                if let Some(region) = &self.region {
                    loader = loader.region(Region::new(region.clone()));
                }
                if let Some(url) = self.endpoint_override()? {
                    loader = loader.endpoint_url(url);
                }
                Ok::<_, Error>(Client::new(&loader.load().await))
            })
            .await
    }

    /// The URL in the variable `endpoint_url_env` names. Unset or empty
    /// means the standard endpoint, so one manifest serves both `DynamoDB`
    /// Local and AWS.
    fn endpoint_override(&self) -> Result<Option<String>> {
        let Some(var) = &self.endpoint_url_env else {
            return Ok(None);
        };
        match std::env::var(var) {
            Ok(url) if !url.trim().is_empty() => {
                check_endpoint(var, url.trim()).map_err(|m| self.error(m))?;
                tracing::debug!(
                    table = %self.table,
                    endpoint_env = %var,
                    "DynamoDB endpoint override"
                );
                Ok(Some(url.trim().to_string()))
            }
            Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(self.error(format!("`{var}` is not valid UTF-8")))
            }
        }
    }
}

#[async_trait]
impl EntityStore for DynamoStore {
    fn describe(&self) -> String {
        format!("dynamodb:{}", self.table)
    }

    async fn get(&self, type_name: &str, key: &str) -> Result<Option<Arc<Record>>> {
        let key_field = self.key_field(type_name)?;
        let pk = partition_key(type_name, key);
        if pk.len() > MAX_PK_BYTES {
            // Too long for a partition key, so no item has it.
            return Ok(None);
        }
        let output = self
            .client()
            .await?
            .get_item()
            .table_name(&self.table)
            .key(PK, AttributeValue::S(pk.clone()))
            .send()
            .await
            .map_err(|e| self.error(format!("GetItem `{pk}`: {}", DisplayErrorContext(&e))))?;
        let Some(item) = output.item else {
            return Ok(None);
        };
        // An item whose `pk` and `type` disagree is broken, not missing.
        if let Some(found) = type_problem(&item, type_name) {
            return Err(self.error(format!(
                "item `{pk}` has {found}; expected `{TYPE}` \"{type_name}\""
            )));
        }
        item_to_record(item, key_field, key)
            .map(|record| Some(Arc::new(record)))
            .map_err(|message| self.error(format!("item `{pk}`: {message}")))
    }

    async fn list(&self, type_name: &str, query: &ListQuery) -> Result<Vec<Arc<Record>>> {
        let key_field = self.key_field(type_name)?;
        if query.limit == Some(0) {
            return Ok(Vec::new());
        }
        let filter = ScanFilter::new(type_name, query.references.as_ref());
        let mut items = self
            .client()
            .await?
            .scan()
            .table_name(&self.table)
            .filter_expression(filter.expression)
            .set_expression_attribute_names(Some(filter.names))
            .set_expression_attribute_values(Some(filter.values))
            .into_paginator()
            .items()
            .send();
        let prefix = partition_key(type_name, "");
        let mut records = Vec::new();
        while let Some(item) = items.next().await {
            let item =
                item.map_err(|e| self.error(format!("Scan: {}", DisplayErrorContext(&e))))?;
            let Some(AttributeValue::S(pk)) = item.get(PK) else {
                return Err(self.error(format!("a `{type_name}` item has no string `{PK}`")));
            };
            let Some(key) = pk.strip_prefix(&prefix).map(str::to_string) else {
                return Err(self.error(format!(
                    "item `{pk}` has `{TYPE}` \"{type_name}\" but its `{PK}` does not start with `{prefix}`"
                )));
            };
            // The scan also finds items by `pk` prefix, so one whose `type`
            // is wrong or missing surfaces here instead of vanishing.
            if let Some(found) = type_problem(&item, type_name) {
                return Err(self.error(format!(
                    "item `{pk}` has {found}; expected `{TYPE}` \"{type_name}\""
                )));
            }
            let record = item_to_record(item, key_field, &key)
                .map_err(|message| self.error(format!("item `{prefix}{key}`: {message}")))?;
            if query.matches(&record) {
                records.push((key, record));
            }
        }
        records.sort_by(|(a, _), (b, _)| a.cmp(b));
        records.truncate(query.limit.unwrap_or(usize::MAX));
        Ok(records
            .into_iter()
            .map(|(_, record)| Arc::new(record))
            .collect())
    }

    async fn ready(&self) -> Result<()> {
        let client = self.client().await?;
        let output = client
            .describe_table()
            .table_name(&self.table)
            .send()
            .await
            .map_err(|e| {
                if e.as_service_error()
                    .is_some_and(DescribeTableError::is_resource_not_found_exception)
                {
                    let region = client
                        .config()
                        .region()
                        .map(|region| format!(" in region `{region}`"))
                        .unwrap_or_default();
                    self.error(format!("table `{}` does not exist{region}", self.table))
                } else {
                    self.error(format!(
                        "DescribeTable `{}`: {}",
                        self.table,
                        DisplayErrorContext(&e)
                    ))
                }
            })?;
        let table = output.table.ok_or_else(|| {
            self.error(format!("DescribeTable `{}` described no table", self.table))
        })?;
        check_key_schema(&table)
            .map_err(|message| self.error(format!("table `{}` {message}", self.table)))?;
        // DescribeTable shows the table exists, not that these credentials
        // may read it: make the two reads the store makes, once each.
        let unreadable = |operation: &str, detail: String| {
            self.error(format!(
                "cannot read table `{}`: {operation}: {detail}",
                self.table
            ))
        };
        client
            .get_item()
            .table_name(&self.table)
            .key(PK, AttributeValue::S(READY_PROBE.to_string()))
            .send()
            .await
            .map_err(|e| unreadable("GetItem", DisplayErrorContext(&e).to_string()))?;
        client
            .scan()
            .table_name(&self.table)
            .limit(1)
            .send()
            .await
            .map_err(|e| unreadable("Scan", DisplayErrorContext(&e).to_string()))?;
        Ok(())
    }
}

/// `"<Type>#<key>"`.
pub fn partition_key(type_name: &str, key: &str) -> String {
    format!("{type_name}#{key}")
}

/// The item [`DynamoStore`] reads back as `record`: `pk`, `type`, and one
/// attribute per field (see [`to_attribute_value`]). Fields named `pk` or
/// `type` are overwritten by the layout's own.
pub fn to_item(type_name: &str, key: &str, record: &Record) -> HashMap<String, AttributeValue> {
    let mut item: HashMap<String, AttributeValue> = record
        .iter()
        .map(|(name, value)| (name.clone(), to_attribute_value(value)))
        .collect();
    item.insert(
        PK.to_string(),
        AttributeValue::S(partition_key(type_name, key)),
    );
    item.insert(TYPE.to_string(), AttributeValue::S(type_name.to_string()));
    item
}

/// JSON as an attribute value: null → NULL, boolean → BOOL, number → N,
/// string → S, array → L, object → M. Sets and binary have no JSON form;
/// write those attributes directly. `DynamoDB` normalizes numbers, so `1.0`
/// reads back as `1`.
pub fn to_attribute_value(value: &Json) -> AttributeValue {
    match value {
        Json::Null => AttributeValue::Null(true),
        Json::Bool(b) => AttributeValue::Bool(*b),
        Json::Number(n) => AttributeValue::N(n.to_string()),
        Json::String(s) => AttributeValue::S(s.clone()),
        Json::Array(items) => AttributeValue::L(items.iter().map(to_attribute_value).collect()),
        Json::Object(map) => AttributeValue::M(
            map.iter()
                .map(|(name, value)| (name.clone(), to_attribute_value(value)))
                .collect(),
        ),
    }
}

/// An item as a record: every attribute but `pk` and `type`, plus the key
/// field when the item does not store it.
fn item_to_record(
    mut item: HashMap<String, AttributeValue>,
    key_field: &str,
    key: &str,
) -> std::result::Result<Record, String> {
    item.remove(PK);
    item.remove(TYPE);
    let mut record = to_object(item)?;
    // `pk` is the item's identity: a stored key attribute may repeat it,
    // but never name another entity.
    match record.get(key_field) {
        None | Some(Json::Null) => {
            record.insert(key_field.to_string(), Json::String(key.to_string()));
        }
        Some(stored) if super::key_matches(stored, key) => {}
        Some(stored) => {
            return Err(format!(
                "stores `{key_field}` = {stored}, but its `{PK}` names key `{key}`; the key attribute must match the partition key, or be left out"
            ));
        }
    }
    Ok(record)
}

/// Attributes as a JSON object in name order (`serde_json` keeps insertion
/// order in this workspace, and `HashMap` order is random).
fn to_object(attributes: HashMap<String, AttributeValue>) -> std::result::Result<Record, String> {
    let mut object = Record::new();
    for (name, value) in attributes.into_iter().collect::<BTreeMap<_, _>>() {
        let value = to_json(value).map_err(|message| format!("`{name}`: {message}"))?;
        object.insert(name, value);
    }
    Ok(object)
}

/// An attribute value as JSON; see the module docs.
fn to_json(value: AttributeValue) -> std::result::Result<Json, String> {
    Ok(match value {
        AttributeValue::S(s) => Json::String(s),
        AttributeValue::N(n) => number(&n)?,
        AttributeValue::Bool(b) => Json::Bool(b),
        AttributeValue::Null(_) => Json::Null,
        AttributeValue::L(items) => Json::Array(
            items
                .into_iter()
                .map(to_json)
                .collect::<std::result::Result<_, _>>()?,
        ),
        AttributeValue::M(map) => Json::Object(to_object(map)?),
        AttributeValue::Ss(mut items) => {
            items.sort();
            Json::Array(items.into_iter().map(Json::String).collect())
        }
        AttributeValue::Ns(items) => {
            let mut numbers = items
                .iter()
                .map(|n| number(n))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            numbers.sort_by(compare_number_values);
            Json::Array(numbers)
        }
        AttributeValue::B(blob) => Json::String(base64(blob.as_ref())),
        AttributeValue::Bs(blobs) => {
            let mut items: Vec<String> = blobs.iter().map(|b| base64(b.as_ref())).collect();
            items.sort();
            Json::Array(items.into_iter().map(Json::String).collect())
        }
        _ => return Err("an attribute type this SDK version does not know".into()),
    })
}

/// An N value as a JSON number: an integer when it fits `i64` or `u64`,
/// else the nearest float.
/// An N as JSON: an integer that fits 64 bits as a number; a wider one
/// (`DynamoDB` keeps 38 digits) as its exact digits, since a JSON number would
/// round it and keys compare as text anyway; anything else as a 64-bit
/// float.
fn number(n: &str) -> std::result::Result<Json, String> {
    if let Ok(i) = n.parse::<i64>() {
        return Ok(Json::from(i));
    }
    if let Ok(u) = n.parse::<u64>() {
        return Ok(Json::from(u));
    }
    if is_integer(n) {
        return Ok(Json::String(n.to_string()));
    }
    n.parse::<f64>()
        .ok()
        .and_then(serde_json::Number::from_f64)
        .map(Json::Number)
        .ok_or_else(|| format!("`{n}` is not a number"))
}

/// Order the members of an NS by value: integers exactly (one wider than 64
/// bits is kept as its digits), anything else by its 64-bit float value.
fn compare_number_values(a: &Json, b: &Json) -> Ordering {
    if let (Json::Number(x), Json::Number(y)) = (a, b) {
        return compare_numbers(x, y);
    }
    let text = |v: &Json| match v {
        Json::String(s) => s.clone(),
        other => other.to_string(),
    };
    let (x, y) = (text(a), text(b));
    if is_integer(&x) && is_integer(&y) {
        return compare_integers(&x, &y);
    }
    let float = |t: &str| t.parse::<f64>().unwrap_or_default();
    float(&x).total_cmp(&float(&y)).then_with(|| x.cmp(&y))
}

/// Order two canonical integers (see [`is_integer`]) by value, digit by
/// digit: wide ones round to the same float.
fn compare_integers(a: &str, b: &str) -> Ordering {
    let (a_negative, a_digits) = a.strip_prefix('-').map_or((false, a), |d| (true, d));
    let (b_negative, b_digits) = b.strip_prefix('-').map_or((false, b), |d| (true, d));
    let magnitude = a_digits
        .len()
        .cmp(&b_digits.len())
        .then_with(|| a_digits.cmp(b_digits));
    match (a_negative, b_negative) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => magnitude,
        (true, true) => magnitude.reverse(),
    }
}

fn compare_numbers(a: &serde_json::Number, b: &serde_json::Number) -> Ordering {
    if let (Some(a), Some(b)) = (a.as_i64(), b.as_i64()) {
        return a.cmp(&b);
    }
    if let (Some(a), Some(b)) = (a.as_u64(), b.as_u64()) {
        return a.cmp(&b);
    }
    // Every finite JSON number has an f64 value.
    let float = |n: &serde_json::Number| n.as_f64().unwrap_or_default();
    float(a).total_cmp(&float(b))
}

/// Standard base64 with padding (RFC 4648 §4).
fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let byte = |i: usize| u32::from(chunk.get(i).copied().unwrap_or(0));
        let triple = (byte(0) << 16) | (byte(1) << 8) | byte(2);
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(char::from(ALPHABET[((triple >> shift) & 0x3f) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// Why `item` is not an item of `type_name`, if it is not.
fn type_problem(item: &HashMap<String, AttributeValue>, type_name: &str) -> Option<String> {
    match item.get(TYPE) {
        Some(AttributeValue::S(found)) if found == type_name => None,
        Some(AttributeValue::S(found)) => Some(format!("`{TYPE}` \"{found}\"")),
        Some(_) => Some(format!("a non-string `{TYPE}`")),
        None => Some(format!("no `{TYPE}`")),
    }
}

/// A `Scan` filter for one entity type: items of that `type`, or whose
/// `pk` claims that type (so a mislabeled item is read and reported rather
/// than skipped), optionally narrowed to items whose field may reference a
/// key. Names and values travel as placeholders, never inside the
/// expression.
struct ScanFilter {
    expression: String,
    names: HashMap<String, String>,
    values: HashMap<String, AttributeValue>,
}

impl ScanFilter {
    fn new(type_name: &str, references: Option<&(String, String)>) -> Self {
        let mut filter = Self {
            expression: "(#type = :type OR begins_with(#pk, :prefix))".to_string(),
            names: HashMap::from([
                ("#type".to_string(), TYPE.to_string()),
                ("#pk".to_string(), PK.to_string()),
            ]),
            values: HashMap::from([
                (
                    ":type".to_string(),
                    AttributeValue::S(type_name.to_string()),
                ),
                (
                    ":prefix".to_string(),
                    AttributeValue::S(partition_key(type_name, "")),
                ),
            ]),
        };
        let Some((field, key)) = references else {
            return filter;
        };
        filter.names.insert("#field".to_string(), field.clone());
        filter
            .values
            .insert(":key".to_string(), AttributeValue::S(key.clone()));
        // `contains` with a string: substring of an S, member of an SS,
        // element of an L. An integer key may also be stored as a number:
        // an N (equal), a member of an NS, or an element of an L.
        if is_integer(key) {
            filter
                .values
                .insert(":number".to_string(), AttributeValue::N(key.clone()));
            filter.expression.push_str(
                " AND (contains(#field, :key) OR contains(#field, :number) OR #field = :number)",
            );
        } else {
            filter.expression.push_str(" AND contains(#field, :key)");
        }
        filter
    }
}

/// An endpoint override may be https anywhere, but plain http only to this
/// machine (`DynamoDB` Local): requests are signed with the AWS credentials,
/// and a session token would cross the network unencrypted.
fn check_endpoint(var: &str, url: &str) -> std::result::Result<(), String> {
    let parsed = reqwest::Url::parse(url).map_err(|e| format!("`{var}` is not a URL: {e}"))?;
    let host = parsed
        .host_str()
        .unwrap_or_default()
        .trim_start_matches('[')
        .trim_end_matches(']');
    let local = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    match parsed.scheme() {
        "https" => Ok(()),
        "http" if local => Ok(()),
        "http" => Err(format!(
            "`{var}` points `DynamoDB` at plain http on another machine, where signed requests would travel unencrypted; use https, or a loopback address for `DynamoDB` Local"
        )),
        other => Err(format!("`{var}` must be an https:// URL, not {other}://")),
    }
}

/// Whether `key` is an integer written canonically (no leading zeros)
/// within `DynamoDB`'s 38 digits, so an N holding it reads back as the same
/// text and passes [`ListQuery::matches`].
fn is_integer(key: &str) -> bool {
    let digits = key.strip_prefix('-').unwrap_or(key);
    !digits.is_empty()
        && digits.len() <= 38
        && digits.bytes().all(|b| b.is_ascii_digit())
        && (digits == "0" || !digits.starts_with('0'))
        && key != "-0"
}

/// `Ok` when `table` is keyed the way this store reads it: by a single
/// HASH key `pk` of type S.
fn check_key_schema(table: &TableDescription) -> std::result::Result<(), String> {
    let attribute_type = |name: &str| {
        table
            .attribute_definitions()
            .iter()
            .find(|definition| definition.attribute_name() == name)
            .map(AttributeDefinition::attribute_type)
    };
    if let [key] = table.key_schema() {
        if key.attribute_name() == PK
            && *key.key_type() == KeyType::Hash
            && attribute_type(PK) == Some(&ScalarAttributeType::S)
        {
            return Ok(());
        }
    }
    let found: Vec<String> = table
        .key_schema()
        .iter()
        .map(|key| {
            format!(
                "{} `{}` ({})",
                key.key_type().as_str(),
                key.attribute_name(),
                attribute_type(key.attribute_name()).map_or("?", ScalarAttributeType::as_str)
            )
        })
        .collect();
    Err(format!(
        "must have a single HASH key `{PK}` of type S, not {}",
        if found.is_empty() {
            "no key".to_string()
        } else {
            found.join(" + ")
        }
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::KeySchemaElement;
    use serde_json::json;

    #[tokio::test]
    async fn an_endpoint_that_never_answers_is_given_up_on() {
        // It accepts the connection, then says nothing.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let silent = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        // Static credentials, and no config files: the lookup waits on no
        // metadata service. Nothing else in this binary reads these.
        let var = format!(
            "SCRIPTORIUM_ONTOLOGY_TEST_SILENT_ENDPOINT_{}",
            std::process::id()
        );
        let nowhere = std::env::temp_dir().join("scriptorium-ontology-test-no-aws-config");
        std::env::set_var(&var, &url);
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::set_var("AWS_CONFIG_FILE", &nowhere);
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &nowhere);
        let config = DynamoConfig {
            table: "papers".into(),
            region: Some("eu-central-1".into()),
            endpoint_url_env: Some(var),
        };
        let keys = BTreeMap::from([("Paper".to_string(), "id".to_string())]);
        let mut store = DynamoStore::new("research", &config, keys).unwrap();
        store.operation_timeout = std::time::Duration::from_secs(1);
        let started = std::time::Instant::now();
        let err = tokio::time::timeout(std::time::Duration::from_secs(15), store.get("Paper", "a"))
            .await
            .expect("the request ends on its own")
            .unwrap_err()
            .to_string();
        assert!(err.to_lowercase().contains("timeout"), "{err}");
        assert!(started.elapsed() < std::time::Duration::from_secs(8));
        silent.abort();
    }

    fn s(value: &str) -> AttributeValue {
        AttributeValue::S(value.to_string())
    }

    fn n(value: &str) -> AttributeValue {
        AttributeValue::N(value.to_string())
    }

    fn attributes(pairs: Vec<(&str, AttributeValue)>) -> HashMap<String, AttributeValue> {
        pairs
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect()
    }

    #[test]
    fn plain_http_endpoints_stay_on_this_machine() {
        for url in [
            "https://dynamodb.eu-central-1.amazonaws.com",
            "http://localhost:8000",
            "http://127.0.0.1:8000",
            "http://[::1]:8000",
        ] {
            assert!(check_endpoint("E", url).is_ok(), "{url}");
        }
        for url in [
            "http://dynamo.internal:8000",
            "http://10.0.0.5:8000",
            "ftp://localhost",
            "not a url",
        ] {
            assert!(check_endpoint("E", url).is_err(), "{url}");
        }
    }

    #[test]
    fn partition_keys_join_type_and_key() {
        assert_eq!(partition_key("Paper", "attention"), "Paper#attention");
        assert_eq!(partition_key("Tag", ""), "Tag#");
        // Type names never contain `#`, so the first one ends the type.
        assert_eq!(
            partition_key("Tag", "c#").split_once('#'),
            Some(("Tag", "c#"))
        );
    }

    #[test]
    fn attribute_values_become_json() {
        let record = to_object(attributes(vec![
            ("s", s("text")),
            ("int", n("-42")),
            ("u64", n("18446744073709551615")),
            ("float", n("1.5")),
            ("exponent", n("1E+100")),
            ("yes", AttributeValue::Bool(true)),
            ("nothing", AttributeValue::Null(true)),
            (
                "list",
                AttributeValue::L(vec![s("a"), n("1"), AttributeValue::L(vec![])]),
            ),
            (
                "map",
                AttributeValue::M(attributes(vec![("z", n("1")), ("a", s("x"))])),
            ),
            ("ss", AttributeValue::Ss(vec!["b".into(), "a".into()])),
            (
                "ns",
                AttributeValue::Ns(vec!["10".into(), "-1".into(), "9".into(), "2.5".into()]),
            ),
            ("b", AttributeValue::B(Blob::new("foobar"))),
            (
                "bs",
                AttributeValue::Bs(vec![Blob::new("fo"), Blob::new("f")]),
            ),
        ]))
        .unwrap();
        assert_eq!(
            Json::Object(record.clone()),
            json!({
                "s": "text", "int": -42, "u64": 18_446_744_073_709_551_615_u64,
                "float": 1.5, "exponent": 1e100, "yes": true, "nothing": null,
                "list": ["a", 1, []], "map": {"a": "x", "z": 1},
                "ss": ["a", "b"], "ns": [-1, 2.5, 9, 10],
                "b": "Zm9vYmFy", "bs": ["Zg==", "Zm8="]
            })
        );
        assert!(record["int"].is_i64() && record["u64"].is_u64() && record["float"].is_f64());
        assert!(
            record.keys().is_sorted() && record["map"].as_object().unwrap().keys().is_sorted(),
            "fields in name order"
        );

        let err = to_object(attributes(vec![(
            "m",
            AttributeValue::M(attributes(vec![("year", n("MMXVII"))])),
        )]))
        .unwrap_err();
        assert_eq!(err, "`m`: `year`: `MMXVII` is not a number");
    }

    #[test]
    fn binary_is_standard_padded_base64() {
        // RFC 4648 §10.
        for (raw, encoded) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(raw.as_bytes()), encoded, "{raw:?}");
        }
        assert_eq!(base64(&[0xfb, 0xff, 0xbf]), "+/+/");
    }

    #[test]
    fn json_round_trips_through_items() {
        let value = json!({
            "s": "x", "n": 7, "neg": -3, "f": 0.25, "b": false, "null": null,
            "list": [1, "two", [3]], "map": {"k": {"deep": true}}, "empty": [], "obj": {}
        });
        let Json::Object(record) = &value else {
            unreachable!()
        };
        let item = to_item("Paper", "p", record);
        assert_eq!(item[PK], s("Paper#p"));
        assert_eq!(item[TYPE], s("Paper"));
        assert_eq!(
            item["list"],
            AttributeValue::L(vec![n("1"), s("two"), AttributeValue::L(vec![n("3")])])
        );
        let mut expected = record.clone();
        expected.insert("id".into(), json!("p"));
        assert_eq!(item_to_record(item, "id", "p").unwrap(), expected);
    }

    #[test]
    fn records_drop_the_layout_and_carry_their_key() {
        let item = attributes(vec![
            (PK, s("Paper#a")),
            (TYPE, s("Paper")),
            ("title", s("A")),
        ]);
        assert_eq!(
            Json::Object(item_to_record(item, "id", "a").unwrap()),
            json!({"id": "a", "title": "A"})
        );
        // A stored key field is kept as stored…
        let item = attributes(vec![(PK, s("Item#42")), (TYPE, s("Item")), ("id", n("42"))]);
        assert_eq!(
            Json::Object(item_to_record(item, "id", "42").unwrap()),
            json!({"id": 42})
        );
        // …but may not name another entity than `pk` does.
        let item = attributes(vec![(PK, s("Paper#a")), (TYPE, s("Paper")), ("id", s("b"))]);
        let err = item_to_record(item, "id", "a").unwrap_err();
        assert!(err.contains("names key `a`"), "{err}");
    }

    #[test]
    fn scan_filters_keep_names_and_values_out_of_the_expression() {
        let all = ScanFilter::new("Paper", None);
        assert_eq!(
            all.expression,
            "(#type = :type OR begins_with(#pk, :prefix))"
        );
        assert_eq!(
            all.names,
            HashMap::from([("#type".into(), "type".into()), ("#pk".into(), "pk".into())])
        );
        assert_eq!(
            all.values,
            HashMap::from([
                (":type".into(), s("Paper")),
                (":prefix".into(), s("Paper#"))
            ])
        );

        let hostile = "x) OR (#type <> :type";
        let by_key = ScanFilter::new("Paper", Some(&("authors".into(), hostile.into())));
        assert_eq!(
            by_key.expression,
            "(#type = :type OR begins_with(#pk, :prefix)) AND contains(#field, :key)"
        );
        assert_eq!(by_key.names["#field"], "authors");
        assert_eq!(by_key.values[":key"], s(hostile));
        assert!(!by_key.values.contains_key(":number"));

        let numeric = ScanFilter::new("Paper", Some(&("cites".into(), "42".into())));
        assert_eq!(
            numeric.expression,
            "(#type = :type OR begins_with(#pk, :prefix)) AND (contains(#field, :key) OR contains(#field, :number) OR #field = :number)"
        );
        assert_eq!(numeric.values[":key"], s("42"));
        assert_eq!(numeric.values[":number"], n("42"));
    }

    #[test]
    fn integer_keys_are_written_the_way_json_prints_them() {
        for key in [
            "0",
            "42",
            "-7",
            "9223372036854775807",
            "18446744073709551616",
            "12345678901234567890123456789012345678",
        ] {
            assert!(is_integer(key), "{key}");
        }
        for key in [
            "",
            "abc",
            "042",
            "+42",
            "-0",
            "4.2",
            "1e3",
            " 42",
            // More digits than DynamoDB keeps.
            "123456789012345678901234567890123456789",
        ] {
            assert!(!is_integer(key), "{key}");
        }
    }

    #[test]
    fn numbers_sort_by_value() {
        let mut numbers: Vec<Json> = [
            "18446744073709551615",
            "3",
            "123456789012345678901234567890",
            "-2.5",
            "18446744073709551614",
            "-9223372036854775808",
            "-123456789012345678901234567890",
            // Round to the same float as their neighbours above.
            "-123456789012345678901234567891",
            "-9223372036854775809",
            "18446744073709551616",
        ]
        .iter()
        .map(|n| number(n).unwrap())
        .collect();
        numbers.sort_by(compare_number_values);
        let sorted: Vec<String> = numbers
            .iter()
            .map(|v| match v {
                Json::String(s) => s.clone(),
                other => other.to_string(),
            })
            .collect();
        assert_eq!(
            sorted,
            [
                "-123456789012345678901234567891",
                "-123456789012345678901234567890",
                "-9223372036854775809",
                "-9223372036854775808",
                "-2.5",
                "3",
                "18446744073709551614",
                "18446744073709551615",
                "18446744073709551616",
                "123456789012345678901234567890"
            ]
        );
        // Wider than 64 bits: the exact digits, not a rounded float.
        assert_eq!(
            number("123456789012345678901234567890").unwrap(),
            json!("123456789012345678901234567890")
        );
    }

    #[test]
    fn key_schema_must_be_one_string_hash_key_named_pk() {
        fn table(keys: &[(&str, KeyType, ScalarAttributeType)]) -> TableDescription {
            let mut builder = TableDescription::builder();
            for (name, key_type, attribute_type) in keys {
                builder = builder
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name(*name)
                            .key_type(key_type.clone())
                            .build()
                            .unwrap(),
                    )
                    .attribute_definitions(
                        AttributeDefinition::builder()
                            .attribute_name(*name)
                            .attribute_type(attribute_type.clone())
                            .build()
                            .unwrap(),
                    );
            }
            builder.build()
        }
        use KeyType::{Hash, Range};
        use ScalarAttributeType::{N, S};
        assert_eq!(check_key_schema(&table(&[("pk", Hash, S)])), Ok(()));
        for (keys, found) in [
            (vec![("id", Hash, S)], "HASH `id` (S)"),
            (vec![("pk", Hash, N)], "HASH `pk` (N)"),
            (
                vec![("pk", Hash, S), ("sk", Range, S)],
                "HASH `pk` (S) + RANGE `sk` (S)",
            ),
            (vec![], "no key"),
        ] {
            assert_eq!(
                check_key_schema(&table(&keys)),
                Err(format!(
                    "must have a single HASH key `pk` of type S, not {found}"
                ))
            );
        }
    }

    #[tokio::test]
    async fn bad_config_and_unknown_types_fail_without_io() {
        let config = |table: &str, region: Option<&str>| DynamoConfig {
            table: table.into(),
            region: region.map(Into::into),
            endpoint_url_env: None,
        };
        let keys = BTreeMap::from([("Paper".to_string(), "id".to_string())]);
        for (table, region) in [(" ", None), ("t", Some(""))] {
            assert!(
                DynamoStore::new("research", &config(table, region), keys.clone()).is_err(),
                "{table:?} {region:?}"
            );
        }
        let store = DynamoStore::new("research", &config("papers", None), keys).unwrap();
        assert_eq!(store.describe(), "dynamodb:papers");
        let err = store.get("Person", "x").await.unwrap_err().to_string();
        assert_eq!(
            err,
            "store `dynamodb:papers`: `Person` is not an entity type of sub-ontology `research`"
        );
        assert!(store.list("Person", &ListQuery::default()).await.is_err());
        // Answers that need no request make none.
        let too_long = "k".repeat(MAX_PK_BYTES);
        assert!(store.get("Paper", &too_long).await.unwrap().is_none());
        let none = ListQuery {
            references: None,
            limit: Some(0),
        };
        assert!(store.list("Paper", &none).await.unwrap().is_empty());
        assert!(store.client.get().is_none(), "no client was built");
    }
}
