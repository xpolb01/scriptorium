//! Through real federation gateways. Each test commits an ontology to a
//! scratch git repository, serves its sub-ontologies, composes the
//! supergraph, starts a gateway on it, and queries the gateway: the
//! gateway's own query planner decides which subgraph answers what.
//!
//! `SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY` names the gateway binary; each test
//! skips with a note while it is unset:
//! - Hive Router (Rust): `cargo install --locked hive-router`, then its
//!   `hive_router` binary;
//! - Hive Gateway (JavaScript): `npm install @graphql-hive/gateway`, then
//!   `node_modules/.bin/hive-gateway`;
//! - Apollo Router (Rust): the `router` binary of a release.
//!
//! The binary's name says which it is, or
//! `SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY_KIND` does. Each plans queries with a
//! planner written apart from the others'; run the suite with each.

mod gateway_support;

use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use gateway_support::{
    behind_gateway, example, gateway, repository, scriptorium_ontology, Gateway, Running, Served,
    GIT, LINK,
};
use serde_json::json;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_example_joins_every_sub_ontology_in_one_query() {
    let repo = example();
    let Some((_served, gateway)) = behind_gateway("the_example_joins", repo.path()).await else {
        return;
    };
    let data = gateway
        .data(
            r#"{
              paper(id: "bert") {
                title year kind venue
                authors { id name papers { id } }
                cites { id title authors { name } }
                citedBy { id }
                implementations { url implements { id } }
                topics { id title related { id } }
              }
            }"#,
        )
        .await;
    let person =
        |id: &str, name: &str| json!({ "id": id, "name": name, "papers": [{ "id": "bert" }] });
    assert_eq!(
        data,
        json!({ "paper": {
            "title": "BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding",
            "year": 2019,
            "kind": "CONFERENCE",
            "venue": "NAACL 2019",
            "authors": [
                person("devlin", "Jacob Devlin"),
                person("chang", "Ming-Wei Chang"),
                person("lee", "Kenton Lee"),
                person("toutanova", "Kristina Toutanova"),
            ],
            "cites": [{
                "id": "attention-is-all-you-need",
                "title": "Attention Is All You Need",
                "authors": [
                    { "name": "Ashish Vaswani" }, { "name": "Noam Shazeer" },
                    { "name": "Niki Parmar" }, { "name": "Jakob Uszkoreit" },
                    { "name": "Llion Jones" }, { "name": "Aidan N. Gomez" },
                    { "name": "Łukasz Kaiser" }, { "name": "Illia Polosukhin" },
                ],
            }],
            "citedBy": [],
            "implementations": [{
                "url": "https://github.com/google-research/bert",
                "implements": [{ "id": "bert" }],
            }],
            "topics": [
                { "id": "pretraining", "title": "Pretraining", "related": [{ "id": "transformers" }] },
                { "id": "transformers", "title": "Transformers", "related": [{ "id": "pretraining" }] },
            ],
        }})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plans_hop_back_and_forth_between_sub_ontologies() {
    let repo = example();
    let Some((_served, gateway)) = behind_gateway("plans_hop", repo.path()).await else {
        return;
    };
    // core → research → research → core → research → software and notes.
    let data = gateway
        .data(
            r#"{
              person(id: "vaswani") {
                name
                papers {
                  title
                  citedBy {
                    title
                    authors { name papers { id implementations { url } topics { id } } }
                  }
                }
              }
            }"#,
        )
        .await;
    let author = |name: &str| {
        json!({ "name": name, "papers": [{
            "id": "bert",
            "implementations": [{ "url": "https://github.com/google-research/bert" }],
            "topics": [{ "id": "pretraining" }, { "id": "transformers" }],
        }]})
    };
    assert_eq!(
        data,
        json!({ "person": {
            "name": "Ashish Vaswani",
            "papers": [{
                "title": "Attention Is All You Need",
                "citedBy": [{
                    "title": "BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding",
                    "authors": [
                        author("Jacob Devlin"), author("Ming-Wei Chang"),
                        author("Kenton Lee"), author("Kristina Toutanova"),
                    ],
                }],
            }],
        }})
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_lists_limits_and_missing_keys() {
    let repo = example();
    let Some((_served, gateway)) = behind_gateway("root_lists", repo.path()).await else {
        return;
    };
    let data = gateway
        .data(
            r#"{
              people(first: 3) { id }
              none: people(first: 0) { id }
              papers(first: 1) { id }
              topics(first: 5) { id }
              paper(id: "nope") { title }
              person(id: "nope") { name }
              repository(id: "tensor2tensor") { url }
            }"#,
        )
        .await;
    assert_eq!(
        data,
        json!({
            "people": [{ "id": "chang" }, { "id": "devlin" }, { "id": "gomez" }],
            "none": [],
            "papers": [{ "id": "attention-is-all-you-need" }],
            "topics": [{ "id": "pretraining" }, { "id": "transformers" }],
            "paper": null,
            "person": null,
            "repository": { "url": "https://github.com/tensorflow/tensor2tensor" },
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn variables_aliases_and_fragments_reach_the_subgraphs() {
    let repo = example();
    let Some((_served, gateway)) = behind_gateway("variables_aliases", repo.path()).await else {
        return;
    };
    let response = gateway
        .post(
            r"query Pick($paper: ID!, $person: ID!, $few: Int) {
              first: paper(id: $paper) { ...Bits kind }
              who: person(id: $person) {
                __typename
                name
                ... on Person { wrote: papers { __typename id } }
              }
              some: people(first: $few) { id }
            }
            fragment Bits on Paper { __typename title writers: authors { id } }",
            json!({ "paper": "attention-is-all-you-need", "person": "shazeer", "few": 2 }),
        )
        .await;
    assert!(
        response
            .get("errors")
            .is_none_or(|e| e.as_array().is_some_and(Vec::is_empty)),
        "{response}"
    );
    assert_eq!(
        response["data"],
        json!({
            "first": {
                "__typename": "Paper",
                "title": "Attention Is All You Need",
                "writers": [
                    { "id": "vaswani" }, { "id": "shazeer" }, { "id": "parmar" }, { "id": "uszkoreit" },
                    { "id": "jones" }, { "id": "gomez" }, { "id": "kaiser" }, { "id": "polosukhin" },
                ],
                "kind": "CONFERENCE",
            },
            "who": {
                "__typename": "Person",
                "name": "Noam Shazeer",
                "wrote": [{ "__typename": "Paper", "id": "attention-is-all-you-need" }],
            },
            "some": [{ "id": "chang" }, { "id": "devlin" }],
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn int_keys_cross_sub_ontologies() {
    let catalog = format!(
        "{LINK}\ntype Item @key(fields: \"sku\") {{ sku: Int! name: String! }}\ntype Query {{ item(sku: Int!): Item items(first: Int): [Item!]! }}\n"
    );
    let stock = format!(
        "{LINK}\ndirective @inverse(of: String!) on FIELD_DEFINITION\nextend type Item @key(fields: \"sku\") {{ sku: Int! quantity: Int shelvedIn: [Warehouse!]! @inverse(of: \"items\") }}\ntype Warehouse @key(fields: \"code\") {{ code: String! items: [Item!]! }}\ntype Query {{ warehouse(code: String!): Warehouse }}\n"
    );
    let repo = repository(&[
        ("ontology/catalog/subgraph.toml", GIT),
        ("ontology/catalog/schema.graphql", &catalog),
        ("ontology/catalog/data/Item/1.yaml", "name: Anvil\n"),
        ("ontology/catalog/data/Item/2.yaml", "name: Bellows\n"),
        ("ontology/catalog/data/Item/42.yaml", "name: Crucible\n"),
        ("ontology/stock/subgraph.toml", GIT),
        ("ontology/stock/schema.graphql", &stock),
        ("ontology/stock/data/Item/1.yaml", "quantity: 5\n"),
        ("ontology/stock/data/Item/42.yaml", "quantity: 0\n"),
        (
            "ontology/stock/data/Warehouse/east.yaml",
            "items: [1, 42]\n",
        ),
        ("ontology/stock/data/Warehouse/west.yaml", "items: [2]\n"),
    ]);
    let Some((_served, gateway)) = behind_gateway("int_keys", repo.path()).await else {
        return;
    };
    let data = gateway
        .data(
            r#"{
              items(first: 10) { sku name quantity shelvedIn { code } }
              item(sku: 42) { name quantity }
              warehouse(code: "east") { items { sku name quantity } }
            }"#,
        )
        .await;
    assert_eq!(
        data,
        json!({
            "items": [
                { "sku": 1, "name": "Anvil", "quantity": 5, "shelvedIn": [{ "code": "east" }] },
                { "sku": 2, "name": "Bellows", "quantity": null, "shelvedIn": [{ "code": "west" }] },
                { "sku": 42, "name": "Crucible", "quantity": 0, "shelvedIn": [{ "code": "east" }] },
            ],
            "item": { "name": "Crucible", "quantity": 0 },
            "warehouse": { "items": [
                { "sku": 1, "name": "Anvil", "quantity": 5 },
                { "sku": 42, "name": "Crucible", "quantity": 0 },
            ]},
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_override_moves_the_field_to_its_new_owner() {
    let library = format!(
        "{LINK}\ntype Book @key(fields: \"id\") {{ id: ID! title: String! blurb: String }}\ntype Query {{ book(id: ID!): Book books(first: Int): [Book!]! }}\n"
    );
    let reviews = "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\", \"@override\"])\nextend type Book @key(fields: \"id\") { id: ID! blurb: String @override(from: \"library\") stars: Int }\n";
    let repo = repository(&[
        ("ontology/library/subgraph.toml", GIT),
        ("ontology/library/schema.graphql", &library),
        (
            "ontology/library/data/Book/b1.yaml",
            "title: Dune\nblurb: Spice.\n",
        ),
        ("ontology/library/data/Book/b2.yaml", "title: Emma\n"),
        ("ontology/reviews/subgraph.toml", GIT),
        ("ontology/reviews/schema.graphql", reviews),
        (
            "ontology/reviews/data/Book/b1.yaml",
            "blurb: Spice.\nstars: 5\n",
        ),
        ("ontology/reviews/data/Book/b2.yaml", "stars: 3\n"),
    ]);
    let Some((served, gateway)) = behind_gateway("an_override_moves", repo.path()).await else {
        return;
    };
    assert!(
        served
            .supergraph()
            .contains("@join__field(graph: REVIEWS, override: \"library\")"),
        "the supergraph routes `blurb` to reviews"
    );
    let data = gateway
        .data(r#"{ books(first: 5) { id title blurb stars } book(id: "b1") { blurb } }"#)
        .await;
    assert_eq!(
        data,
        json!({
            "books": [
                { "id": "b1", "title": "Dune", "blurb": "Spice.", "stars": 5 },
                { "id": "b2", "title": "Emma", "blurb": null, "stars": 3 },
            ],
            "book": { "blurb": "Spice." },
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inaccessible_fields_stay_out_of_the_api() {
    let vault = "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\", \"@inaccessible\"])\ntype Note @key(fields: \"id\") { id: ID! text: String! draft: String @inaccessible }\ntype Query { note(id: ID!): Note }\n";
    let repo = repository(&[
        ("ontology/vault/subgraph.toml", GIT),
        ("ontology/vault/schema.graphql", vault),
        (
            "ontology/vault/data/Note/n1.yaml",
            "text: Public text.\ndraft: Hidden draft.\n",
        ),
    ]);
    let Some((_served, gateway)) = behind_gateway("inaccessible_fields", repo.path()).await else {
        return;
    };
    assert_eq!(
        gateway.data(r#"{ note(id: "n1") { id text } }"#).await,
        json!({ "note": { "id": "n1", "text": "Public text." } })
    );
    let hidden = gateway
        .post(r#"{ note(id: "n1") { draft } }"#, json!({}))
        .await;
    assert!(
        hidden["errors"].as_array().is_some_and(|e| !e.is_empty()),
        "{hidden}"
    );
    assert!(!hidden.to_string().contains("Hidden draft"), "{hidden}");
    let fields = gateway
        .data(r#"{ __type(name: "Note") { fields { name } } }"#)
        .await;
    let mut names: Vec<&str> = fields["__type"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|f| f["name"].as_str())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["id", "text"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_sub_ontology_is_asked_at_its_own_url() {
    if gateway("a_remote_sub_ontology").is_none() {
        return;
    }
    let venues = format!(
        "{LINK}\ntype Venue @key(fields: \"id\") {{ id: ID! name: String! city: String }}\ntype Query {{ venue(id: ID!): Venue }}\n"
    );
    // Another repository serves `venues`; this one pins its schema.
    let elsewhere = repository(&[
        ("ontology/venues/subgraph.toml", GIT),
        ("ontology/venues/schema.graphql", &venues),
        (
            "ontology/venues/data/Venue/neurips.yaml",
            "name: NeurIPS\ncity: Vancouver\n",
        ),
        (
            "ontology/venues/data/Venue/naacl.yaml",
            "name: NAACL\ncity: Minneapolis\n",
        ),
    ]);
    let remote = Served::start(elsewhere.path()).await;
    let pin = format!("[remote]\nurl = \"{}/venues/graphql\"\n", remote.base);
    let talks = format!(
        "{LINK}\ndirective @inverse(of: String!) on FIELD_DEFINITION\ntype Talk @key(fields: \"id\") {{ id: ID! title: String! venue: Venue! }}\nextend type Venue @key(fields: \"id\") {{ id: ID! talks: [Talk!]! @inverse(of: \"venue\") }}\ntype Query {{ talk(id: ID!): Talk talks(first: Int): [Talk!]! }}\n"
    );
    let repo = repository(&[
        ("ontology/venues/subgraph.toml", &pin),
        ("ontology/venues/schema.graphql", &venues),
        ("ontology/talks/subgraph.toml", GIT),
        ("ontology/talks/schema.graphql", &talks),
        (
            "ontology/talks/data/Talk/t1.yaml",
            "title: Attention\nvenue: neurips\n",
        ),
        (
            "ontology/talks/data/Talk/t2.yaml",
            "title: BERT\nvenue: naacl\n",
        ),
        (
            "ontology/talks/data/Talk/t3.yaml",
            "title: Scaling\nvenue: neurips\n",
        ),
    ]);
    let Some((served, gateway)) = behind_gateway("a_remote_sub_ontology", repo.path()).await else {
        return;
    };
    assert!(
        served
            .supergraph()
            .contains(&format!("url: \"{}/venues/graphql\"", remote.base)),
        "the gateway reaches the remote at its own URL"
    );
    let neurips = json!({ "name": "NeurIPS", "city": "Vancouver", "talks": [{ "id": "t1" }, { "id": "t3" }] });
    let data = gateway
        .data(
            r#"{
              talks(first: 10) { id title venue { name city talks { id } } }
              venue(id: "naacl") { name talks { title } }
            }"#,
        )
        .await;
    assert_eq!(
        data,
        json!({
            "talks": [
                { "id": "t1", "title": "Attention", "venue": neurips },
                { "id": "t2", "title": "BERT", "venue": { "name": "NAACL", "city": "Minneapolis", "talks": [{ "id": "t2" }] } },
                { "id": "t3", "title": "Scaling", "venue": neurips },
            ],
            "venue": { "name": "NAACL", "talks": [{ "title": "BERT" }] },
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_published_supergraph_is_what_the_gateway_runs() {
    let Some((binary, kind)) = gateway("the_published_supergraph") else {
        return;
    };
    let repo = example();
    let out = tempfile::tempdir().unwrap();
    let published = out.path().join("supergraph.graphql");
    // On a port the OS picks: the supergraph names the one it bound.
    let serve = Running(
        scriptorium_ontology()
            .args(["-C"])
            .arg(repo.path())
            .args(["serve", "--addr", "127.0.0.1:0", "--supergraph"])
            .arg(&published)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_mins(1);
    while !std::fs::metadata(&published).is_ok_and(|m| m.len() > 0) {
        assert!(Instant::now() < deadline, "no supergraph was published");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let gateway =
        Gateway::start(&binary, kind, &std::fs::read_to_string(&published).unwrap()).await;
    let data = gateway
        .data(r#"{ person(id: "devlin") { name papers { id implementations { url } } } }"#)
        .await;
    assert_eq!(
        data,
        json!({ "person": { "name": "Jacob Devlin", "papers": [{
            "id": "bert",
            "implementations": [{ "url": "https://github.com/google-research/bert" }],
        }]}})
    );
    drop(serve);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_plans_answer_alike() {
    let repo = example();
    let Some((_served, gateway)) = behind_gateway("concurrent_plans", repo.path()).await else {
        return;
    };
    let query = r#"{
      papers(first: 10) { id authors { name papers { implementations { url } topics { id } } } }
      person(id: "shazeer") { papers { citedBy { authors { name } } } }
    }"#;
    let expected = gateway.data(query).await;
    // Many plans at once: every fetch of every one gets its own answer.
    let gateway = Arc::new(gateway);
    let answers = answers_in_order(
        (0..32)
            .map(|_| {
                let gateway = Arc::clone(&gateway);
                tokio::spawn(async move { gateway.data(query).await })
            })
            .collect(),
    )
    .await;
    for answer in answers {
        assert_eq!(answer, expected);
    }
}

/// Await every task, in order.
async fn answers_in_order(
    tasks: Vec<tokio::task::JoinHandle<serde_json::Value>>,
) -> Vec<serde_json::Value> {
    let mut out = Vec::with_capacity(tasks.len());
    for task in tasks {
        out.push(task.await.unwrap());
    }
    out
}
