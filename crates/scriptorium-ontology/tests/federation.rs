//! End to end over HTTP, without a gateway: scaffold an ontology into a git
//! repo, commit it, serve its subgraphs, and walk the fetches a federation
//! gateway's query plan would make; and check the supergraph carries what
//! gateways need to plan them. `gateway.rs` runs real gateways.

mod gateway_support;

use std::sync::Arc;

use git2::{IndexAddOption, Repository, RepositoryInitOptions, Signature};
use scriptorium_ontology::compose::compose;
use scriptorium_ontology::scaffold::scaffold_example;
use scriptorium_ontology::serve::{build_subgraphs, serve_listener};
use scriptorium_ontology::snapshot::GitSnapshot;
use scriptorium_ontology::{Registry, Snapshot};
use serde_json::{json, Value};

fn commit_all(root: &std::path::Path, message: &str) {
    let repo = Repository::open(root).unwrap_or_else(|_| {
        let mut opts = RepositoryInitOptions::new();
        opts.initial_head("main");
        Repository::init_opts(root, &opts).unwrap()
    });
    let mut index = repo.index().unwrap();
    index
        .add_all(["*"].iter(), IndexAddOption::DEFAULT, None)
        .unwrap();
    index.update_all(["*"].iter(), None).unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = Signature::now("test", "test@example.com").unwrap();
    let parent = repo.head().ok().and_then(|h| h.peel_to_commit().ok());
    let parents: Vec<&git2::Commit> = parent.iter().collect();
    repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &parents)
        .unwrap();
}

async fn post(client: &reqwest::Client, url: &str, query: &str) -> Value {
    let response: Value = client
        .post(url)
        .json(&json!({ "query": query }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(response.get("errors").is_none(), "{url}: {response}");
    response["data"].clone()
}

#[tokio::test]
async fn subgraphs_join_entities_across_sub_ontologies() {
    let dir = tempfile::tempdir().unwrap();
    scaffold_example(dir.path(), "ontology").unwrap();
    commit_all(dir.path(), "scaffold");
    // Uncommitted edits must not leak into what is served from HEAD.
    std::fs::write(
        dir.path().join("ontology/core/data/Person/devlin.yaml"),
        "name: NOT COMMITTED\n",
    )
    .unwrap();

    let snapshot: Arc<dyn Snapshot> = Arc::new(GitSnapshot::open(dir.path(), "HEAD").unwrap());
    let (registry, issues) = Registry::load(snapshot.as_ref(), "ontology").unwrap();
    assert!(issues.is_empty(), "{issues:?}");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let composition = compose(&registry, &base);
    assert!(composition.is_ok(), "{:?}", composition.errors);
    let supergraph = composition.supergraph.unwrap();
    for sub in ["core", "research", "software", "notes"] {
        assert!(
            supergraph.contains(&format!("{base}/{sub}/graphql")),
            "{sub} url in supergraph"
        );
    }

    let subgraphs = build_subgraphs(&registry, &snapshot).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(serve_listener(listener, subgraphs, async {
        let _ = stopped.await;
    }));
    let client = gateway_support::client();

    // 1. Root field in `research`.
    let paper = post(
        &client,
        &format!("{base}/research/graphql"),
        r#"{ paper(id: "bert") { title year kind authors { id } cites { id title } } }"#,
    )
    .await;
    assert_eq!(paper["paper"]["year"], json!(2019));
    assert_eq!(paper["paper"]["kind"], json!("CONFERENCE"));
    assert_eq!(
        paper["paper"]["cites"][0]["id"],
        json!("attention-is-all-you-need")
    );
    let authors: Vec<Value> = paper["paper"]["authors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| json!({ "__typename": "Person", "id": a["id"] }))
        .collect();
    assert_eq!(authors.len(), 4);

    // 2. `core` resolves the authors' names by key.
    let query = format!(
        "{{ _entities(representations: {}) {{ ... on Person {{ id name }} }} }}",
        graphql_literal(&Value::Array(authors))
    );
    let people = post(&client, &format!("{base}/core/graphql"), &query).await;
    assert_eq!(
        people["_entities"][0],
        json!({ "id": "devlin", "name": "Jacob Devlin" })
    );

    // 3. `software` extends research's Paper with implementations.
    let implementations = post(
        &client,
        &format!("{base}/software/graphql"),
        r#"{ _entities(representations: [{__typename: "Paper", id: "bert"}]) { ... on Paper { implementations { url } } } }"#,
    )
    .await;
    assert_eq!(
        implementations["_entities"][0]["implementations"],
        json!([{ "url": "https://github.com/google-research/bert" }])
    );

    // 4. `research` extends core's Person with derived authorship.
    let authored = post(
        &client,
        &format!("{base}/research/graphql"),
        r#"{ _entities(representations: [{__typename: "Person", id: "vaswani"}]) { ... on Person { papers { id citedBy { id } } } } }"#,
    )
    .await;
    assert_eq!(
        authored["_entities"][0]["papers"],
        json!([{ "id": "attention-is-all-you-need", "citedBy": [{ "id": "bert" }] }])
    );

    assert_notes_subgraph(&client, &base).await;

    // 5. Each subgraph advertises its committed schema, verbatim, so a
    // gateway composing from live subgraphs sees what `compose` composed.
    for sub in ["core", "research", "software", "notes"] {
        let data = post(
            &client,
            &format!("{base}/{sub}/graphql"),
            "{ _service { sdl } }",
        )
        .await;
        assert_eq!(
            data["_service"]["sdl"],
            json!(registry.get(sub).unwrap().sdl),
            "{sub}"
        );
    }

    // Unknown sub-ontology → 404 with a GraphQL-shaped error.
    let missing = client
        .post(format!("{base}/nope/graphql"))
        .json(&json!({ "query": "{ __typename }" }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), reqwest::StatusCode::NOT_FOUND);

    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

/// `notes` serves markdown topic notes: frontmatter wikilinks become
/// relations, body wikilinks become `related`, backlinks are inverse, and
/// `Paper` gains the topics that cover it.
async fn assert_notes_subgraph(client: &reqwest::Client, base: &str) {
    let topic = post(
        client,
        &format!("{base}/notes/graphql"),
        r#"{ topic(id: "transformers") { title papers { id } related { id } backlinks { id } body } }"#,
    )
    .await;
    assert_eq!(topic["topic"]["title"], json!("Transformers"));
    assert_eq!(
        topic["topic"]["papers"],
        json!([{ "id": "attention-is-all-you-need" }, { "id": "bert" }])
    );
    assert_eq!(topic["topic"]["related"], json!([{ "id": "pretraining" }]));
    assert_eq!(
        topic["topic"]["backlinks"],
        json!([{ "id": "pretraining" }])
    );
    assert!(topic["topic"]["body"]
        .as_str()
        .unwrap()
        .contains("[[pretraining]]"));

    // 6. `notes` extends research's Paper with the topics that cover it.
    let covered = post(
        client,
        &format!("{base}/notes/graphql"),
        r#"{ _entities(representations: [{__typename: "Paper", id: "bert"}]) { ... on Paper { topics { id } } } }"#,
    )
    .await;
    assert_eq!(
        covered["_entities"][0]["topics"],
        json!([{ "id": "pretraining" }, { "id": "transformers" }])
    );
}

/// Render JSON as a GraphQL input literal (object keys unquoted).
fn graphql_literal(value: &Value) -> String {
    match value {
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(graphql_literal)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(map) => format!(
            "{{{}}}",
            map.iter()
                .map(|(k, v)| format!("{k}: {}", graphql_literal(v)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        other => other.to_string(),
    }
}

/// The `{ ... }` head of `type <name>` in `sdl`: its directives.
fn head<'a>(sdl: &'a str, name: &str) -> &'a str {
    let start = sdl
        .find(&format!("\ntype {name}\n"))
        .or_else(|| sdl.find(&format!("\nenum {name}\n")))
        .unwrap_or_else(|| panic!("no {name} in\n{sdl}"));
    let rest = &sdl[start..];
    &rest[..rest.find('{').unwrap()]
}

#[test]
fn the_supergraph_joins_every_type_to_its_subgraphs() {
    let dir = tempfile::tempdir().unwrap();
    scaffold_example(dir.path(), "ontology").unwrap();
    commit_all(dir.path(), "scaffold");
    let snapshot = GitSnapshot::open(dir.path(), "HEAD").unwrap();
    let (registry, _) = Registry::load(&snapshot, "ontology").unwrap();
    let supergraph = compose(&registry, "http://h:1").supergraph.unwrap();
    // Root fields are reachable only through a `@join__type` on Query.
    let query = head(&supergraph, "Query");
    for graph in ["CORE", "NOTES", "RESEARCH", "SOFTWARE"] {
        assert!(
            query.contains(&format!("@join__type(graph: {graph})")),
            "{query}"
        );
    }
    // An entity is joined, by its key, to every subgraph that defines it.
    for (entity, graphs) in [
        ("Person", &["CORE", "RESEARCH"][..]),
        ("Paper", &["NOTES", "RESEARCH", "SOFTWARE"][..]),
        ("Topic", &["NOTES"][..]),
        ("Repository", &["SOFTWARE"][..]),
    ] {
        let head = head(&supergraph, entity);
        for graph in graphs {
            assert!(
                head.contains(&format!("@join__type(graph: {graph}, key: \"id\")")),
                "{entity}: {head}"
            );
        }
    }
    assert!(head(&supergraph, "PaperKind").contains("@join__type(graph: RESEARCH)"));
}

#[test]
fn override_and_inaccessible_reach_the_supergraph() {
    use gateway_support::{repository, GIT, LINK};
    let library = format!(
        "{LINK}\ntype Book @key(fields: \"id\") {{ id: ID! title: String! blurb: String }}\ntype Query {{ book(id: ID!): Book }}\n"
    );
    let reviews = "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\", \"@override\", \"@inaccessible\"])\nextend type Book @key(fields: \"id\") { id: ID! blurb: String @override(from: \"library\") draft: String @inaccessible }\n";
    let repo = repository(&[
        ("ontology/library/subgraph.toml", GIT),
        ("ontology/library/schema.graphql", &library),
        ("ontology/reviews/subgraph.toml", GIT),
        ("ontology/reviews/schema.graphql", reviews),
    ]);
    let snapshot = GitSnapshot::open(repo.path(), "HEAD").unwrap();
    let (registry, _) = Registry::load(&snapshot, "ontology").unwrap();
    let composition = compose(&registry, "http://h:1");
    assert!(composition.is_ok(), "{:?}", composition.errors);
    let supergraph = composition.supergraph.unwrap();
    assert!(
        supergraph.contains("blurb: String @join__field(graph: REVIEWS, override: \"library\")"),
        "{supergraph}"
    );
    assert!(supergraph.contains("@inaccessible"), "{supergraph}");
    let api = composition.api_schema.unwrap();
    assert!(!api.contains("draft"), "{api}");
    assert!(api.contains("blurb"), "{api}");
}

#[tokio::test]
async fn int_keyed_representations_resolve() {
    use gateway_support::{repository, Served, GIT, LINK};
    let catalog = format!(
        "{LINK}\ntype Item @key(fields: \"sku\") {{ sku: Int! name: String! }}\ntype Query {{ item(sku: Int!): Item }}\n"
    );
    let stock =
        format!("{LINK}\nextend type Item @key(fields: \"sku\") {{ sku: Int! quantity: Int }}\n");
    let repo = repository(&[
        ("ontology/catalog/subgraph.toml", GIT),
        ("ontology/catalog/schema.graphql", &catalog),
        ("ontology/catalog/data/Item/7.yaml", "name: Anvil\n"),
        ("ontology/catalog/data/Item/42.yaml", "name: Crucible\n"),
        ("ontology/stock/subgraph.toml", GIT),
        ("ontology/stock/schema.graphql", &stock),
        ("ontology/stock/data/Item/42.yaml", "quantity: 3\n"),
    ]);
    let served = Served::start(repo.path()).await;
    let client = gateway_support::client();
    // Keys come back as the numbers the owner answered with.
    let data = post(
        &client,
        &format!("{}/stock/graphql", served.base),
        r#"{ _entities(representations: [{__typename: "Item", sku: 42}, {__typename: "Item", sku: 7}]) { ... on Item { sku quantity } } }"#,
    )
    .await;
    assert_eq!(
        data["_entities"],
        json!([{ "sku": 42, "quantity": 3 }, { "sku": 7, "quantity": null }])
    );
    let data = post(
        &client,
        &format!("{}/catalog/graphql", served.base),
        r#"{ _entities(representations: [{__typename: "Item", sku: 7}]) { ... on Item { name } } }"#,
    )
    .await;
    assert_eq!(data["_entities"], json!([{ "name": "Anvil" }]));
}

#[tokio::test]
async fn mixed_entity_batches_answer_in_order() {
    use gateway_support::Served;
    let dir = tempfile::tempdir().unwrap();
    scaffold_example(dir.path(), "ontology").unwrap();
    commit_all(dir.path(), "scaffold");
    let served = Served::start(dir.path()).await;
    // research owns Paper and extends Person: one batch, both kinds, the
    // order a planner sent them in.
    let data = post(
        &gateway_support::client(),
        &format!("{}/research/graphql", served.base),
        r#"{ _entities(representations: [
            {__typename: "Person", id: "devlin"},
            {__typename: "Paper", id: "attention-is-all-you-need"},
            {__typename: "Person", id: "vaswani"}
        ]) {
            __typename
            ... on Person { papers { id } }
            ... on Paper { year }
        } }"#,
    )
    .await;
    assert_eq!(
        data["_entities"],
        json!([
            { "__typename": "Person", "papers": [{ "id": "bert" }] },
            { "__typename": "Paper", "year": 2017 },
            { "__typename": "Person", "papers": [{ "id": "attention-is-all-you-need" }] },
        ])
    );
}
