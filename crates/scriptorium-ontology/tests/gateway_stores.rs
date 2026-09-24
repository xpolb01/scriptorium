//! One dataset, every store, through a real gateway: people and papers
//! served from git records, markdown notes, Postgres rows, and `DynamoDB`
//! items answer a battery of federated queries identically.
//!
//! Needs a gateway (`SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY`, see `gateway.rs`).
//! Postgres joins with `--features postgres` and
//! `SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL`; `DynamoDB` with
//! `--features dynamodb` and `SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT`
//! (`DynamoDB` Local). A binary of its own: the `DynamoDB` leg puts dummy AWS
//! credentials in the process environment.

mod gateway_support;

use std::path::Path;

use gateway_support::{gateway, repository, Gateway, Kind, Served, GIT};
use serde_json::Value;

const CORE_SCHEMA: &str = include_str!("../../../templates/ontology/core/schema.graphql");
const RESEARCH_SCHEMA: &str = include_str!("../../../templates/ontology/research/schema.graphql");

struct Person {
    id: String,
    name: String,
}

struct Paper {
    id: String,
    title: String,
    year: i32,
    kind: &'static str,
    venue: Option<String>,
    authors: Vec<String>,
    cites: Vec<String>,
}

struct Dataset {
    people: Vec<Person>,
    papers: Vec<Paper>,
}

/// A small deterministic generator, so every store gets the same rows.
struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        usize::try_from(self.0 >> 33).unwrap() % n
    }

    /// `count` distinct items of `from`, sorted.
    fn pick(&mut self, from: &[String], count: usize) -> Vec<String> {
        let mut chosen = std::collections::BTreeSet::new();
        while chosen.len() < count.min(from.len()) {
            chosen.insert(from[self.below(from.len())].clone());
        }
        chosen.into_iter().collect()
    }
}

/// Keys sort alike in every collation; some names and titles need quoting
/// or escaping in YAML, SQL, and JSON alike.
fn dataset() -> Dataset {
    let mut rng = Rng(0x2026_0924);
    let people: Vec<Person> = (1..=24)
        .map(|i| Person {
            id: format!("p{i:02}"),
            name: match i {
                3 => "Zoë O'Brien".to_string(),
                7 => "Łukasz \"Luke\" Kaiser".to_string(),
                11 => "Name: with a colon".to_string(),
                _ => format!("Person {i:02}"),
            },
        })
        .collect();
    let person_ids: Vec<String> = people.iter().map(|p| p.id.clone()).collect();
    let mut papers: Vec<Paper> = Vec::new();
    for i in 1..=80 {
        let earlier: Vec<String> = papers.iter().map(|p| p.id.clone()).collect();
        let authors = 1 + rng.below(4);
        let cites = rng.below(4);
        papers.push(Paper {
            id: format!("w{i:03}"),
            title: match i % 20 {
                3 => format!("Paper {i:03}: “quoted”, a colon: and ünïcode"),
                11 => format!("Paper {i:03} with 'single' and \"double\" quotes"),
                _ => format!("Paper {i:03}"),
            },
            year: 2000 + i32::try_from(rng.below(26)).unwrap(),
            kind: ["CONFERENCE", "JOURNAL", "PREPRINT"][rng.below(3)],
            venue: (rng.below(5) != 0).then(|| format!("Venue {}", rng.below(12) + 1)),
            authors: rng.pick(&person_ids, authors),
            cites: rng.pick(&earlier, cites),
        });
    }
    Dataset { people, papers }
}

/// A JSON string, which YAML reads as the same string.
fn quoted(text: &str) -> String {
    serde_json::to_string(text).unwrap()
}

fn paper_fields(paper: &Paper) -> String {
    let venue = paper
        .venue
        .as_ref()
        .map_or(String::new(), |venue| format!("venue: {}\n", quoted(venue)));
    format!(
        "title: {}\nyear: {}\nkind: {}\n{venue}authors: [{}]\ncites: [{}]\n",
        quoted(&paper.title),
        paper.year,
        paper.kind,
        paper.authors.join(", "),
        paper.cites.join(", ")
    )
}

/// core and research, with `storage` as each one's `subgraph.toml`.
fn schemas(core: &str, research: &str) -> Vec<(String, String)> {
    vec![
        ("ontology/core/subgraph.toml".into(), core.into()),
        ("ontology/core/schema.graphql".into(), CORE_SCHEMA.into()),
        ("ontology/research/subgraph.toml".into(), research.into()),
        (
            "ontology/research/schema.graphql".into(),
            RESEARCH_SCHEMA.into(),
        ),
    ]
}

fn git_files(data: &Dataset) -> Vec<(String, String)> {
    let mut files = schemas(GIT, GIT);
    for person in &data.people {
        files.push((
            format!("ontology/core/data/Person/{}.yaml", person.id),
            format!("name: {}\n", quoted(&person.name)),
        ));
    }
    for paper in &data.papers {
        files.push((
            format!("ontology/research/data/Paper/{}.yaml", paper.id),
            paper_fields(paper),
        ));
    }
    files
}

fn markdown_files(data: &Dataset) -> Vec<(String, String)> {
    let notes =
        |ty: &str| format!("[storage]\nkind = \"markdown\"\ndir = \"pages\"\ntype = \"{ty}\"\n");
    let mut files = schemas(&notes("Person"), &notes("Paper"));
    for person in &data.people {
        files.push((
            format!("ontology/core/pages/{}.md", person.id),
            format!(
                "---\nname: {}\n---\nAbout {}.\n",
                quoted(&person.name),
                person.id
            ),
        ));
    }
    for paper in &data.papers {
        files.push((
            format!("ontology/research/pages/{}.md", paper.id),
            format!("---\n{}---\n", paper_fields(paper)),
        ));
    }
    files
}

fn committed(files: &[(String, String)]) -> tempfile::TempDir {
    let refs: Vec<(&str, &str)> = files
        .iter()
        .map(|(path, contents)| (path.as_str(), contents.as_str()))
        .collect();
    repository(&refs)
}

const BATTERY: &[&str] = &[
    "{ papers(first: 1000) { id title year kind venue } }",
    "{ papers(first: 1000) { id authors { id name } cites { id } citedBy { id } } }",
    "{ people(first: 1000) { id name papers { id year } } }",
    r#"{ paper(id: "w007") { title authors { name papers { id citedBy { id } } } } missing: paper(id: "nope") { id } }"#,
    r#"{ papers(first: 5) { id } none: people(first: 0) { id } person(id: "p03") { name papers { title } } }"#,
];

/// The battery's answers from the repository at `root`, with its served
/// sub-ontologies and gateway, still running for further questions.
async fn answers(binary: &Path, kind: Kind, root: &Path) -> (Vec<Value>, Served, Gateway) {
    let served = Served::start(root).await;
    let gateway = Gateway::start(binary, kind, &served.supergraph()).await;
    let mut all = Vec::new();
    for query in BATTERY {
        all.push(gateway.data(query).await);
    }
    (all, served, gateway)
}

fn assert_alike(store: &str, got: &[Value], expected: &[Value]) {
    for ((query, got), expected) in BATTERY.iter().zip(got).zip(expected) {
        assert_eq!(got, expected, "{store}: {query}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_store_answers_alike_through_the_gateway() {
    let Some((binary, kind)) = gateway("every_store_answers_alike") else {
        return;
    };
    let data = dataset();
    let git = committed(&git_files(&data));
    let (expected, served, gateway) = answers(&binary, kind, git.path()).await;
    drop((served, gateway));
    // Nothing trivial: every paper, every author link, the unicode title.
    assert_eq!(expected[0]["papers"].as_array().unwrap().len(), 80);
    assert_eq!(expected[2]["people"].as_array().unwrap().len(), 24);
    assert!(expected[0].to_string().contains("ünïcode"));

    let markdown = committed(&markdown_files(&data));
    let (got, served, gateway) = answers(&binary, kind, markdown.path()).await;
    assert_alike("markdown", &got, &expected);
    drop((served, gateway));

    #[cfg(feature = "postgres")]
    if let Some(rows) = rows::Rows::load(&data).await {
        let repo = committed(&rows.files());
        let (got, served, gateway) = answers(&binary, kind, repo.path()).await;
        assert_alike("postgres", &got, &expected);
        rows.missing_table_stays_private(&gateway).await;
        drop((served, gateway));
        rows.remove().await;
    }

    #[cfg(feature = "dynamodb")]
    if let Some(items) = items::Items::load(&data).await {
        let repo = committed(&items.files());
        let (got, served, gateway) = answers(&binary, kind, repo.path()).await;
        assert_alike("dynamodb", &got, &expected);
        drop((served, gateway));
        items.remove().await;
    }
}

/// A name no other run uses.
#[cfg(any(feature = "postgres", feature = "dynamodb"))]
fn unique(base: &str) -> String {
    let at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() % 1_000_000_000);
    format!("{base}_{}_{at}", std::process::id())
}

#[cfg(feature = "postgres")]
mod rows {
    use super::{schemas, unique, Dataset};
    use crate::gateway_support::Gateway;

    const URL_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL";

    /// The dataset as rows, in schemas of this run's own.
    pub struct Rows {
        client: tokio_postgres::Client,
        core: String,
        research: String,
    }

    impl Rows {
        pub async fn load(data: &Dataset) -> Option<Self> {
            let Ok(url) = std::env::var(URL_ENV) else {
                eprintln!("note: {URL_ENV} is not set; skipping the Postgres leg");
                return None;
            };
            let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                .await
                .unwrap();
            tokio::spawn(async move {
                let _ = connection.await;
            });
            let rows = Self {
                client,
                core: unique("gateway_core"),
                research: unique("gateway_research"),
            };
            let (core, research) = (&rows.core, &rows.research);
            rows.client
                .batch_execute(&format!(
                    "CREATE SCHEMA {core}; CREATE SCHEMA {research};
                     CREATE TABLE {core}.person (id text PRIMARY KEY, name text NOT NULL);
                     CREATE TABLE {research}.paper (id text PRIMARY KEY, title text NOT NULL,
                       year integer NOT NULL, kind text NOT NULL, venue text,
                       authors text[] NOT NULL, cites text[] NOT NULL);"
                ))
                .await
                .unwrap();
            for person in &data.people {
                rows.client
                    .execute(
                        &format!("INSERT INTO {core}.person VALUES ($1, $2)"),
                        &[&person.id, &person.name],
                    )
                    .await
                    .unwrap();
            }
            for paper in &data.papers {
                rows.client
                    .execute(
                        &format!(
                            "INSERT INTO {research}.paper VALUES ($1, $2, $3, $4, $5, $6, $7)"
                        ),
                        &[
                            &paper.id,
                            &paper.title,
                            &paper.year,
                            &paper.kind,
                            &paper.venue,
                            &paper.authors,
                            &paper.cites,
                        ],
                    )
                    .await
                    .unwrap();
            }
            Some(rows)
        }

        pub fn files(&self) -> Vec<(String, String)> {
            let storage = |schema: &str| {
                format!("[storage]\nkind = \"postgres\"\nurl_env = \"{URL_ENV}\"\nschema = \"{schema}\"\n")
            };
            schemas(&storage(&self.core), &storage(&self.research))
        }

        /// With the table gone under a running `serve`, the gateway's client
        /// gets an error that names neither schema nor table.
        pub async fn missing_table_stays_private(&self, gateway: &Gateway) {
            let research = &self.research;
            self.client
                .batch_execute(&format!(
                    "ALTER TABLE {research}.paper RENAME TO paper_moved"
                ))
                .await
                .unwrap();
            let answer = gateway
                .post(r#"{ paper(id: "w001") { title } }"#, serde_json::json!({}))
                .await;
            self.client
                .batch_execute(&format!(
                    "ALTER TABLE {research}.paper_moved RENAME TO paper"
                ))
                .await
                .unwrap();
            assert!(
                answer["errors"].as_array().is_some_and(|e| !e.is_empty()),
                "{answer}"
            );
            let text = answer.to_string();
            assert!(
                !text.contains(research) && !text.contains("paper_moved"),
                "{text}"
            );
        }

        pub async fn remove(self) {
            self.client
                .batch_execute(&format!(
                    "DROP SCHEMA {} CASCADE; DROP SCHEMA {} CASCADE",
                    self.core, self.research
                ))
                .await
                .unwrap();
        }
    }
}

#[cfg(feature = "dynamodb")]
mod items {
    use super::{schemas, unique, Dataset};
    use aws_config::{BehaviorVersion, Region};
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    };
    use aws_sdk_dynamodb::Client;
    use std::collections::HashMap;

    const ENDPOINT_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT";
    const REGION: &str = "eu-central-1";

    /// The dataset as items, in tables of this run's own.
    pub struct Items {
        client: Client,
        core: String,
        research: String,
    }

    impl Items {
        pub async fn load(data: &Dataset) -> Option<Self> {
            let Some(endpoint) = std::env::var(ENDPOINT_ENV)
                .ok()
                .filter(|e| !e.trim().is_empty())
            else {
                eprintln!("note: {ENDPOINT_ENV} is not set; skipping the DynamoDB leg");
                return None;
            };
            // DynamoDB Local takes any credentials; with the AWS config files
            // out of reach, the default chain finds only these. Nothing else
            // in this binary reads the environment meanwhile.
            let nowhere = std::env::temp_dir().join("scriptorium-ontology-test-no-aws-config");
            std::env::set_var("AWS_ACCESS_KEY_ID", "test");
            std::env::set_var("AWS_SECRET_ACCESS_KEY", "test");
            std::env::remove_var("AWS_SESSION_TOKEN");
            std::env::remove_var("AWS_PROFILE");
            std::env::set_var("AWS_CONFIG_FILE", &nowhere);
            std::env::set_var("AWS_SHARED_CREDENTIALS_FILE", &nowhere);
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(REGION))
                .endpoint_url(endpoint)
                .load()
                .await;
            let items = Self {
                client: Client::new(&config),
                core: unique("gateway-core").replace('_', "-"),
                research: unique("gateway-research").replace('_', "-"),
            };
            for table in [&items.core, &items.research] {
                items
                    .client
                    .create_table()
                    .table_name(table)
                    .billing_mode(BillingMode::PayPerRequest)
                    .key_schema(
                        KeySchemaElement::builder()
                            .attribute_name("pk")
                            .key_type(KeyType::Hash)
                            .build()
                            .unwrap(),
                    )
                    .attribute_definitions(
                        AttributeDefinition::builder()
                            .attribute_name("pk")
                            .attribute_type(ScalarAttributeType::S)
                            .build()
                            .unwrap(),
                    )
                    .send()
                    .await
                    .unwrap();
            }
            let s = |text: &str| AttributeValue::S(text.to_string());
            let keys = |keys: &[String]| AttributeValue::L(keys.iter().map(|k| s(k)).collect());
            for person in &data.people {
                let item = HashMap::from([
                    ("pk".to_string(), s(&format!("Person#{}", person.id))),
                    ("type".to_string(), s("Person")),
                    ("name".to_string(), s(&person.name)),
                ]);
                items.put(&items.core, item).await;
            }
            for paper in &data.papers {
                let mut item = HashMap::from([
                    ("pk".to_string(), s(&format!("Paper#{}", paper.id))),
                    ("type".to_string(), s("Paper")),
                    ("title".to_string(), s(&paper.title)),
                    (
                        "year".to_string(),
                        AttributeValue::N(paper.year.to_string()),
                    ),
                    ("kind".to_string(), s(paper.kind)),
                    ("authors".to_string(), keys(&paper.authors)),
                    ("cites".to_string(), keys(&paper.cites)),
                ]);
                if let Some(venue) = &paper.venue {
                    item.insert("venue".to_string(), s(venue));
                }
                items.put(&items.research, item).await;
            }
            Some(items)
        }

        async fn put(&self, table: &str, item: HashMap<String, AttributeValue>) {
            self.client
                .put_item()
                .table_name(table)
                .set_item(Some(item))
                .send()
                .await
                .unwrap();
        }

        pub fn files(&self) -> Vec<(String, String)> {
            let storage = |table: &str| {
                format!(
                    "[storage]\nkind = \"dynamodb\"\ntable = \"{table}\"\nregion = \"{REGION}\"\nendpoint_url_env = \"{ENDPOINT_ENV}\"\n"
                )
            };
            schemas(&storage(&self.core), &storage(&self.research))
        }

        pub async fn remove(self) {
            for table in [&self.core, &self.research] {
                self.client
                    .delete_table()
                    .table_name(table)
                    .send()
                    .await
                    .unwrap();
            }
        }
    }
}
