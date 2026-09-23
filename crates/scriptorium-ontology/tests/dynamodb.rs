//! The `dynamodb` store against a real endpoint, normally `DynamoDB` Local:
//!
//! ```sh
//! docker run --rm -d -p 127.0.0.1:8000:8000 amazon/dynamodb-local
//! SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 \
//!   cargo test --manifest-path crates/scriptorium-ontology/Cargo.toml \
//!   --features dynamodb --test dynamodb
//! ```
//!
//! Without the variable every test prints a note and passes. Each test
//! creates its own table and deletes it when it passes.
//!
//! These tests are a binary of their own because they put dummy AWS
//! credentials in the process environment, which is only safe while no
//! other thread reads it.
#![cfg(feature = "dynamodb")]

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Once};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType,
};
use aws_sdk_dynamodb::Client;
use scriptorium_ontology::manifest::{DynamoConfig, SubgraphManifest};
use scriptorium_ontology::model::SubOntologyModel;
use scriptorium_ontology::serve::schema::{build_schema, execute, SubgraphSchema};
use scriptorium_ontology::snapshot::WorktreeSnapshot;
use scriptorium_ontology::store::dynamodb::{to_item, DynamoStore};
use scriptorium_ontology::store::{open_store, EntityStore, ListQuery, Record};
use scriptorium_ontology::{SubOntology, SubOntologySource};
use serde_json::{json, Value};

const ENDPOINT_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT";
const REGION: &str = "us-east-1";
static DUMMY_CREDENTIALS: Once = Once::new();

/// The endpoint under test, or `None` after a note saying why the test
/// skips.
fn endpoint(test: &str) -> Option<String> {
    let Some(url) = std::env::var(ENDPOINT_ENV)
        .ok()
        .filter(|url| !url.trim().is_empty())
    else {
        eprintln!(
            "skipping {test}: set {ENDPOINT_ENV} to a DynamoDB Local URL (e.g. http://127.0.0.1:8000) to run it"
        );
        return None;
    };
    DUMMY_CREDENTIALS.call_once(|| {
        // DynamoDB Local takes any credentials. Dummy ones, with the AWS
        // config files out of reach, keep the default chain from finding
        // real ones. Every test gets here before it builds a client, so
        // nothing else reads the environment while it changes.
        let nowhere = std::env::temp_dir().join("scriptorium-ontology-test-no-aws-config");
        std::env::set_var("AWS_ACCESS_KEY_ID", "test");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
        std::env::remove_var("AWS_SESSION_TOKEN");
        std::env::remove_var("AWS_PROFILE");
        std::env::set_var("AWS_CONFIG_FILE", &nowhere);
        std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &nowhere);
    });
    Some(url)
}

fn unique_name(test: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("ontology-{test}-{}-{nanos}", std::process::id())
}

/// A table of this test's own on the endpoint under test.
struct Table {
    client: Client,
    name: String,
}

impl Table {
    /// Keyed the way the store expects: a HASH key `pk` of type S.
    async fn create(test: &str) -> Option<Self> {
        Self::create_keyed(test, &[("pk", KeyType::Hash, ScalarAttributeType::S)]).await
    }

    async fn create_keyed(
        test: &str,
        keys: &[(&str, KeyType, ScalarAttributeType)],
    ) -> Option<Self> {
        let endpoint = endpoint(test)?;
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(REGION))
            .endpoint_url(endpoint)
            .load()
            .await;
        let client = Client::new(&config);
        let name = unique_name(test);
        let mut request = client
            .create_table()
            .table_name(&name)
            .billing_mode(BillingMode::PayPerRequest);
        for (attribute, key_type, attribute_type) in keys {
            request = request
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(*attribute)
                        .key_type(key_type.clone())
                        .build()
                        .unwrap(),
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(*attribute)
                        .attribute_type(attribute_type.clone())
                        .build()
                        .unwrap(),
                );
        }
        request.send().await.unwrap();
        Some(Self { client, name })
    }

    fn config(&self) -> DynamoConfig {
        DynamoConfig {
            table: self.name.clone(),
            region: Some(REGION.into()),
            endpoint_url_env: Some(ENDPOINT_ENV.into()),
        }
    }

    fn store(&self, key_fields: &[(&str, &str)]) -> DynamoStore {
        let key_fields = key_fields
            .iter()
            .map(|(type_name, field)| ((*type_name).to_string(), (*field).to_string()))
            .collect();
        DynamoStore::new("research", &self.config(), key_fields).unwrap()
    }

    async fn put_item(&self, item: HashMap<String, AttributeValue>) {
        self.client
            .put_item()
            .table_name(&self.name)
            .set_item(Some(item))
            .send()
            .await
            .unwrap();
    }

    /// Store `fields` (JSON) as the `type_name` keyed `key`.
    async fn put(&self, type_name: &str, key: &str, fields: Value) {
        self.put_item(to_item(type_name, key, fields.as_object().unwrap()))
            .await;
    }

    /// Store raw attributes (sets, binary, …) as the `type_name` keyed `key`.
    async fn put_attributes(
        &self,
        type_name: &str,
        key: &str,
        attributes: Vec<(&str, AttributeValue)>,
    ) {
        let mut item = to_item(type_name, key, &Record::new());
        item.extend(
            attributes
                .into_iter()
                .map(|(name, value)| (name.to_string(), value)),
        );
        self.put_item(item).await;
    }

    /// `pk` of every item whose `field` contains `key`, as `DynamoDB` alone
    /// decides it.
    async fn raw_contains(&self, field: &str, key: &str) -> Vec<String> {
        let mut items = self
            .client
            .scan()
            .table_name(&self.name)
            .filter_expression("contains(#field, :key)")
            .expression_attribute_names("#field", field)
            .expression_attribute_values(":key", AttributeValue::S(key.into()))
            .into_paginator()
            .items()
            .send();
        let mut pks = Vec::new();
        while let Some(item) = items.next().await {
            pks.push(item.unwrap()["pk"].as_s().unwrap().clone());
        }
        pks.sort();
        pks
    }

    /// How many pages a full scan of the table takes.
    async fn scan_pages(&self) -> usize {
        let mut pages = self
            .client
            .scan()
            .table_name(&self.name)
            .into_paginator()
            .send();
        let mut count = 0;
        while let Some(page) = pages.next().await {
            page.unwrap();
            count += 1;
        }
        count
    }

    async fn delete(self) {
        self.client
            .delete_table()
            .table_name(&self.name)
            .send()
            .await
            .unwrap();
    }
}

fn s(value: &str) -> AttributeValue {
    AttributeValue::S(value.to_string())
}

fn keys(records: &[std::sync::Arc<Record>]) -> Vec<String> {
    records
        .iter()
        .map(|record| match &record["id"] {
            Value::String(key) => key.clone(),
            other => other.to_string(),
        })
        .collect()
}

fn references(field: &str, key: &str, limit: Option<usize>) -> ListQuery {
    ListQuery {
        references: Some((field.into(), key.into())),
        limit,
    }
}

#[tokio::test]
async fn get_reads_items_as_records() {
    let Some(table) = Table::create("get").await else {
        return;
    };
    table
        .put(
            "Paper",
            "attention",
            json!({
                "title": "Attention Is All You Need", "year": 2017,
                "authors": ["vaswani", "shazeer"], "pages": {"first": 1, "last": 11}
            }),
        )
        .await;
    table
        .put_attributes(
            "Paper",
            "typed",
            vec![
                (
                    "tags",
                    AttributeValue::Ss(vec!["nlp".into(), "attention".into()]),
                ),
                (
                    "scores",
                    AttributeValue::Ns(vec!["10".into(), "9.5".into()]),
                ),
                ("digest", AttributeValue::B(Blob::new("foobar"))),
                ("draft", AttributeValue::Bool(false)),
                ("venue", AttributeValue::Null(true)),
                ("year", AttributeValue::N("2017.0".into())),
            ],
        )
        .await;
    table
        .put("Item", "42", json!({"id": 42, "label": "answer"}))
        .await;
    let store = table.store(&[("Paper", "id"), ("Item", "id"), ("Person", "id")]);
    store.ready().await.unwrap();
    assert_eq!(store.describe(), format!("dynamodb:{}", table.name));

    let attention = store.get("Paper", "attention").await.unwrap().unwrap();
    assert_eq!(
        Value::Object((*attention).clone()),
        json!({
            "id": "attention", "title": "Attention Is All You Need", "year": 2017,
            "authors": ["vaswani", "shazeer"], "pages": {"first": 1, "last": 11}
        }),
        "no `pk` or `type`; the key field comes from `pk`"
    );
    let typed = store.get("Paper", "typed").await.unwrap().unwrap();
    assert_eq!(
        Value::Object((*typed).clone()),
        json!({
            "id": "typed", "tags": ["attention", "nlp"], "scores": [9.5, 10],
            "digest": "Zm9vYmFy", "draft": false, "venue": null, "year": 2017
        }),
        "sets sorted, binary as base64, numbers as DynamoDB normalized them"
    );
    assert_eq!(
        Value::Object((*store.get("Item", "42").await.unwrap().unwrap()).clone()),
        json!({"id": 42, "label": "answer"}),
        "a stored key field is kept as stored"
    );

    assert!(store.get("Paper", "missing").await.unwrap().is_none());
    assert!(
        store.get("Person", "attention").await.unwrap().is_none(),
        "same key, other type"
    );
    table.delete().await;
}

#[tokio::test]
async fn mislabeled_items_and_unknown_types_are_errors() {
    let Some(table) = Table::create("errors").await else {
        return;
    };
    table
        .put_item(HashMap::from([
            ("pk".to_string(), s("Paper#mislabeled")),
            ("type".to_string(), s("Person")),
        ]))
        .await;
    let store = table.store(&[("Paper", "id"), ("Person", "id")]);
    let err = store
        .get("Paper", "mislabeled")
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.ends_with("item `Paper#mislabeled` has `type` \"Person\"; expected `type` \"Paper\""),
        "{err}"
    );
    let err = store
        .list("Person", &ListQuery::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.ends_with("item `Paper#mislabeled` has `type` \"Person\" but its `pk` does not start with `Person#`"),
        "{err}"
    );
    // Listed by `pk`, a mislabeled item is an error, not a silent gap.
    let err = store
        .list("Paper", &ListQuery::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.ends_with("item `Paper#mislabeled` has `type` \"Person\"; expected `type` \"Paper\""),
        "{err}"
    );
    for err in [
        store.get("Venue", "x").await.unwrap_err(),
        store
            .list("Venue", &ListQuery::default())
            .await
            .unwrap_err(),
    ] {
        assert!(
            err.to_string()
                .ends_with("`Venue` is not an entity type of sub-ontology `research`"),
            "{err}"
        );
    }
    table.delete().await;
}

#[tokio::test]
async fn list_sorts_by_key_applies_limits_and_follows_pages() {
    let Some(table) = Table::create("list").await else {
        return;
    };
    for key in ["b", "a", "c#1", "c", "10", "9"] {
        table.put("Paper", key, json!({"title": key})).await;
    }
    // Four ~390 KB papers: more than one 1 MB scan page.
    let blob = "x".repeat(390_000);
    for key in ["big-1", "big-2", "big-3", "big-4"] {
        table
            .put("Paper", key, json!({"title": key, "blob": blob}))
            .await;
    }
    table.put("Person", "a", json!({"name": "A. Person"})).await;
    assert!(
        table.scan_pages().await > 1,
        "the table spans several pages"
    );
    let store = table.store(&[("Paper", "id"), ("Person", "id")]);

    let papers = store.list("Paper", &ListQuery::default()).await.unwrap();
    assert_eq!(
        keys(&papers),
        ["10", "9", "a", "b", "big-1", "big-2", "big-3", "big-4", "c", "c#1"]
    );
    assert!(papers
        .iter()
        .all(|record| !record.contains_key("pk") && !record.contains_key("type")));
    let first = ListQuery {
        references: None,
        limit: Some(3),
    };
    assert_eq!(
        keys(&store.list("Paper", &first).await.unwrap()),
        ["10", "9", "a"]
    );
    let people = store.list("Person", &ListQuery::default()).await.unwrap();
    assert_eq!(
        Value::Array(
            people
                .iter()
                .map(|p| Value::Object((**p).clone()))
                .collect()
        ),
        json!([{"id": "a", "name": "A. Person"}])
    );
    table.delete().await;
}

#[tokio::test]
async fn references_match_strings_lists_and_string_sets_exactly() {
    let Some(table) = Table::create("references").await else {
        return;
    };
    table
        .put("Paper", "scalar", json!({"authors": "vaswani"}))
        .await;
    table
        .put("Paper", "list", json!({"authors": ["shazeer", "vaswani"]}))
        .await;
    table
        .put_attributes(
            "Paper",
            "set",
            vec![(
                "authors",
                AttributeValue::Ss(vec!["parmar".into(), "vaswani".into()]),
            )],
        )
        .await;
    // Substrings: `contains` finds them, the store must not.
    table
        .put("Paper", "substring", json!({"authors": "vaswani-2"}))
        .await;
    table
        .put("Paper", "prefixed", json!({"authors": "not-vaswani"}))
        .await;
    // Not references either.
    table
        .put("Paper", "list-substring", json!({"authors": ["vaswani-2"]}))
        .await;
    table
        .put("Paper", "other-field", json!({"editors": ["vaswani"]}))
        .await;
    table
        .put("Person", "vaswani", json!({"authors": "vaswani"}))
        .await;
    assert_eq!(
        table.raw_contains("authors", "vaswani").await,
        [
            "Paper#list",
            "Paper#prefixed",
            "Paper#scalar",
            "Paper#set",
            "Paper#substring",
            "Person#vaswani"
        ],
        "what DynamoDB's `contains` alone returns"
    );
    let store = table.store(&[("Paper", "id"), ("Person", "id")]);

    let by_vaswani = store
        .list("Paper", &references("authors", "vaswani", None))
        .await
        .unwrap();
    assert_eq!(keys(&by_vaswani), ["list", "scalar", "set"]);
    let first_two = store
        .list("Paper", &references("authors", "vaswani", Some(2)))
        .await
        .unwrap();
    assert_eq!(keys(&first_two), ["list", "scalar"]);
    assert!(store
        .list("Paper", &references("authors", "nobody", None))
        .await
        .unwrap()
        .is_empty());
    table.delete().await;
}

#[tokio::test]
async fn references_match_numeric_keys_in_numbers_sets_and_lists() {
    let Some(table) = Table::create("numeric").await else {
        return;
    };
    let ns = |values: &[&str]| AttributeValue::Ns(values.iter().map(ToString::to_string).collect());
    table.put("Paper", "number", json!({"cites": 42})).await;
    table
        .put_attributes("Paper", "number-set", vec![("cites", ns(&["7", "42"]))])
        .await;
    table
        .put("Paper", "number-list", json!({"cites": [7, 42]}))
        .await;
    table
        .put_attributes(
            "Paper",
            "normalized",
            vec![("cites", AttributeValue::N("42.0".into()))],
        )
        .await;
    table.put("Paper", "string", json!({"cites": "42"})).await;
    table
        .put("Paper", "string-list", json!({"cites": ["42"]}))
        .await;
    // Not references to 42.
    table
        .put("Paper", "substring", json!({"cites": "142"}))
        .await;
    table.put("Paper", "bigger", json!({"cites": 142})).await;
    table
        .put_attributes("Paper", "bigger-set", vec![("cites", ns(&["420"]))])
        .await;
    table.put("Paper", "fraction", json!({"cites": 42.5})).await;
    // A key wider than 64 bits (DynamoDB keeps 38 digits).
    let wide = "123456789012345678901234567890";
    table
        .put_attributes(
            "Paper",
            "wide-number",
            vec![("cites", AttributeValue::N(wide.into()))],
        )
        .await;
    table
        .put_attributes("Paper", "wide-set", vec![("cites", ns(&["7", wide]))])
        .await;
    let store = table.store(&[("Paper", "id")]);
    let citing_wide = store
        .list("Paper", &references("cites", wide, None))
        .await
        .unwrap();
    assert_eq!(keys(&citing_wide), ["wide-number", "wide-set"]);
    let read = store.get("Paper", "wide-number").await.unwrap().unwrap();
    assert_eq!(
        read["cites"],
        json!(wide),
        "exact digits, not a rounded float"
    );

    let citing = store
        .list("Paper", &references("cites", "42", None))
        .await
        .unwrap();
    assert_eq!(
        keys(&citing),
        [
            "normalized",
            "number",
            "number-list",
            "number-set",
            "string",
            "string-list"
        ]
    );
    table.delete().await;
}

#[tokio::test]
async fn ready_checks_the_table_and_its_key_schema() {
    use KeyType::{Hash, Range};
    use ScalarAttributeType::{N, S};
    for (test, keys, found) in [
        ("ready-id", vec![("id", Hash, S)], "HASH `id` (S)"),
        ("ready-number", vec![("pk", Hash, N)], "HASH `pk` (N)"),
        (
            "ready-composite",
            vec![("pk", Hash, S), ("sk", Range, S)],
            "HASH `pk` (S) + RANGE `sk` (S)",
        ),
    ] {
        let Some(table) = Table::create_keyed(test, &keys).await else {
            return;
        };
        let err = table.store(&[]).ready().await.unwrap_err().to_string();
        assert_eq!(
            err,
            format!(
                "store `dynamodb:{0}`: table `{0}` must have a single HASH key `pk` of type S, not {found}",
                table.name
            )
        );
        table.delete().await;
    }

    let Some(table) = Table::create("ready-ok").await else {
        return;
    };
    table.store(&[]).ready().await.unwrap();
    let missing = DynamoConfig {
        table: unique_name("ready-missing"),
        ..table.config()
    };
    let err = DynamoStore::new("research", &missing, BTreeMap::new())
        .unwrap()
        .ready()
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        err,
        format!(
            "store `dynamodb:{0}`: table `{0}` does not exist in region `{REGION}`",
            missing.table
        )
    );
    table.delete().await;
}

const RESEARCH: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
directive @inverse(of: String!) on FIELD_DEFINITION
type Paper @key(fields: "id") {
  id: ID!
  title: String!
  year: Int
  authors: [Person!]!
}
type Person @key(fields: "id") {
  id: ID!
  name: String!
  papers: [Paper!]! @inverse(of: "authors")
}
type Query {
  paper(id: ID!): Paper
  papers(first: Int): [Paper!]!
}
"#;

/// The research subgraph over `table`, built the way `serve` builds it:
/// manifest → `open_store` → `ready` → schema.
async fn research_subgraph(table: &Table) -> SubgraphSchema {
    subgraph(table, "research", RESEARCH).await
}

/// Sub-ontology `name` with schema `sdl` over `table`, built like `serve`.
async fn subgraph(table: &Table, name: &str, sdl: &str) -> SubgraphSchema {
    let manifest: SubgraphManifest = toml::from_str(&format!(
        "[storage]\nkind = \"dynamodb\"\ntable = \"{}\"\nregion = \"{REGION}\"\nendpoint_url_env = \"{ENDPOINT_ENV}\"\n",
        table.name
    ))
    .unwrap();
    let storage = manifest.storage.unwrap();
    let sub = SubOntology {
        name: name.into(),
        dir: format!("ontology/{name}"),
        description: None,
        owners: Vec::new(),
        sdl: sdl.into(),
        source: SubOntologySource::Local(storage.clone()),
    };
    let model = SubOntologyModel::parse(sdl).unwrap();
    // Only git-backed stores read the snapshot.
    let worktree = tempfile::tempdir().unwrap();
    let store = open_store(
        &sub,
        &storage,
        Arc::new(WorktreeSnapshot::new(worktree.path())),
        &model,
    )
    .unwrap();
    store.ready().await.unwrap();
    build_schema(name, sdl, store).unwrap()
}

#[tokio::test]
async fn wide_numbers_answer_as_floats_and_exact_keys() {
    const GAUGES: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
type Gauge @key(fields: "id") { id: ID! reading: Float peers: [Gauge!]! }
type Query { gauge(id: ID!): Gauge }
"#;
    let Some(table) = Table::create("wide").await else {
        return;
    };
    let wide = "123456789012345678901234567890";
    table
        .put_attributes(
            "Gauge",
            "g1",
            vec![
                ("reading", AttributeValue::N("18446744073709551616".into())),
                ("peers", AttributeValue::Ns(vec![wide.into()])),
            ],
        )
        .await;
    let schema = subgraph(&table, "gauges", GAUGES).await;
    let response = execute(
        &schema,
        r#"{ gauge(id: "g1") { reading peers { id } } }"#.into(),
    )
    .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap(),
        json!({"gauge": {"reading": 18_446_744_073_709_551_616.0, "peers": [{"id": wide}]}})
    );
    table.delete().await;
}

#[tokio::test]
async fn decimal_numbers_are_not_keys() {
    const TALKS: &str = r#"
extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])
directive @inverse(of: String!) on FIELD_DEFINITION
type Talk @key(fields: "id") { id: ID! room: Room }
type Room @key(fields: "id") { id: ID! talks: [Talk!]! @inverse(of: "room") }
type Query { talk(id: ID!): Talk room(id: ID!): Room }
"#;
    let Some(table) = Table::create("decimal").await else {
        return;
    };
    table
        .put_attributes("Talk", "text", vec![("room", s("42.5"))])
        .await;
    table
        .put_attributes(
            "Talk",
            "number",
            vec![("room", AttributeValue::N("42.5".into()))],
        )
        .await;
    table.put("Room", "42.5", json!({})).await;
    let schema = subgraph(&table, "talks", TALKS).await;

    // A string key resolves both ways…
    let response = execute(
        &schema,
        r#"{ room(id: "42.5") { talks { id } } talk(id: "text") { room { id } } }"#.into(),
    )
    .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap(),
        json!({"room": {"talks": [{"id": "text"}]}, "talk": {"room": {"id": "42.5"}}})
    );
    // …a decimal N is no key either way: the inverse above left it out, and
    // following it is an error rather than a guess at its text.
    let response = execute(&schema, r#"{ talk(id: "number") { room { id } } }"#.into()).await;
    assert!(
        response.errors.iter().any(|e| e
            .message
            .contains("entity keys must be strings or integers")),
        "{:?}",
        response.errors
    );
    table.delete().await;
}

#[tokio::test]
async fn serves_a_subgraph_from_a_dynamodb_manifest() {
    let Some(table) = Table::create("serve").await else {
        return;
    };
    table
        .put(
            "Paper",
            "attention",
            json!({"title": "Attention Is All You Need", "year": 2017, "authors": ["vaswani", "shazeer"]}),
        )
        .await;
    table
        .put_attributes(
            "Paper",
            "moe",
            vec![
                ("title", s("Outrageously Large Neural Networks")),
                ("authors", AttributeValue::Ss(vec!["shazeer".into()])),
            ],
        )
        .await;
    table
        .put("Person", "vaswani", json!({"name": "Ashish Vaswani"}))
        .await;
    table
        .put("Person", "shazeer", json!({"name": "Noam Shazeer"}))
        .await;
    let schema = research_subgraph(&table).await;

    let response = execute(
        &schema,
        r#"{ paper(id: "attention") { id title year authors { id name } } papers(first: 1) { id } }"#
            .into(),
    )
    .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    assert_eq!(
        response.data.into_json().unwrap(),
        json!({
            "paper": {
                "id": "attention", "title": "Attention Is All You Need", "year": 2017,
                "authors": [
                    {"id": "vaswani", "name": "Ashish Vaswani"},
                    {"id": "shazeer", "name": "Noam Shazeer"}
                ]
            },
            "papers": [{"id": "attention"}]
        })
    );

    let response = execute(
        &schema,
        r#"{ _entities(representations: [
               {__typename: "Person", id: "shazeer"},
               {__typename: "Paper", id: "missing"}
             ]) {
               ... on Person { id name papers { id title } }
               ... on Paper { title }
             } }"#
            .into(),
    )
    .await;
    assert_eq!(
        response.data.into_json().unwrap(),
        json!({"_entities": [
            {
                "id": "shazeer", "name": "Noam Shazeer",
                "papers": [
                    {"id": "attention", "title": "Attention Is All You Need"},
                    {"id": "moe", "title": "Outrageously Large Neural Networks"}
                ]
            },
            null
        ]})
    );
    assert_eq!(response.errors.len(), 1, "{:?}", response.errors);
    assert_eq!(
        response.errors[0].message,
        format!("Paper:missing does not exist in dynamodb:{}", table.name)
    );
    table.delete().await;
}
