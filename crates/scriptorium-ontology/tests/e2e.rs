//! The whole system, end to end: the `scriptorium-ontology` binary, git
//! repositories, every store, another repository's subgraph, and a real
//! gateway, driven only as a person or an operator drives them: commands,
//! files, commits, and HTTP. Each test takes a claim `docs/ONTOLOGY.md`
//! makes and shows that it holds.
//!
//! Most tests need a gateway (`SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY`, see
//! `gateway.rs`). The store tests also need `--features postgres,dynamodb`,
//! `SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL`, and
//! `SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT` (`DynamoDB` Local). The
//! comparison with Apollo's composition needs `SCRIPTORIUM_ONTOLOGY_TEST_ROVER`
//! (a `rover` binary) and `APOLLO_ELV2_LICENSE=accept`: Apollo's composition
//! plugin is under the Elastic License 2.0, which whoever runs the test
//! accepts, never the test itself. A test skips, with a note, when what it
//! needs is missing.

mod gateway_support;

use std::path::Path;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

use gateway_support::{client, gateway, scriptorium_ontology, Gateway, Kind, Running, GIT};
use serde_json::{json, Value};

/// How long one command may run, and `serve` may take to start.
const COMMAND_TIMEOUT: Duration = Duration::from_mins(1);

/// What the example's paper lookup answers: `research` resolves `paper` and
/// `cites`, `core` the names, `software` the implementations.
fn bert() -> Value {
    json!({ "paper": {
        "title": "BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding",
        "authors": [
            { "name": "Jacob Devlin" },
            { "name": "Ming-Wei Chang" },
            { "name": "Kenton Lee" },
            { "name": "Kristina Toutanova" },
        ],
        "cites": [{
            "title": "Attention Is All You Need",
            "implementations": [{ "url": "https://github.com/tensorflow/tensor2tensor" }],
        }],
    }})
}

/// Questions over every sub-ontology of the example: lookups, lists,
/// inverses, markdown links, and a key that names nothing.
const BATTERY: &[&str] = &[
    r#"{ paper(id: "bert") { title authors { name } cites { title implementations { url } } } }"#,
    "{ people(first: 100) { id name papers { id title } } }",
    "{ papers(first: 100) { id title year kind venue authors { id name } cites { id } citedBy { id } implementations { id url } topics { id } } }",
    "{ topics(first: 100) { id title body papers { id authors { name } } related { id } backlinks { id } } }",
    r#"{ person(id: "vaswani") { name papers { id } } repository(id: "tensor2tensor") { url implements { id title } } topic(id: "pretraining") { title papers { id } } nobody: person(id: "nobody") { id } }"#,
];

// ---------------------------------------------------------------------------
// Commands, repositories, and servers, as a person runs them.

/// Variables for a child process.
type Env = Vec<(String, String)>;

/// `command` run to completion; one still running after `limit` is killed
/// and fails the test.
async fn finish(mut command: Command, limit: Duration) -> Output {
    let out = tempfile::tempdir().unwrap();
    let (stdout, stderr) = (out.path().join("stdout"), out.path().join("stderr"));
    command
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(&stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap());
    let mut child = Running(command.spawn().unwrap());
    let deadline = Instant::now() + limit;
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "still running after {limit:?}: {command:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    Output {
        status,
        stdout: std::fs::read(&stdout).unwrap(),
        stderr: std::fs::read(&stderr).unwrap(),
    }
}

/// The binary with `args` on `repo`, run to completion within
/// [`COMMAND_TIMEOUT`].
async fn run(repo: &Path, args: &[&str], env: &Env) -> Output {
    let mut command = scriptorium_ontology();
    command
        .arg("-C")
        .arg(repo)
        .args(args)
        .envs(env.iter().map(|(k, v)| (k, v)));
    finish(command, COMMAND_TIMEOUT).await
}

/// [`run`], which must succeed; its standard output.
async fn ok(repo: &Path, args: &[&str], env: &Env) -> String {
    let out = run(repo, args, env).await;
    assert!(out.status.success(), "{args:?} failed:\n{}", text(&out));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Both streams of `out`, for a failure message.
fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// `git` in `repo`, which must succeed; its standard output.
fn git(repo: &Path, args: &[&str]) -> String {
    let mut command = Command::new("git");
    for var in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_PREFIX",
    ] {
        command.env_remove(var);
    }
    let out = command
        .args(["-c", "user.name=test", "-c", "user.email=test@example.com"])
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(out.status.success(), "git {args:?}:\n{}", text(&out));
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn write(repo: &Path, files: &[(&str, &str)]) {
    for (path, contents) in files {
        let full = repo.join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, contents).unwrap();
    }
}

/// A new git repository with the example ontology `init` scaffolds and
/// commits.
async fn example() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    ok(repo.path(), &["init"], &Env::new()).await;
    repo
}

fn head(repo: &Path) -> String {
    git(repo, &["rev-parse", "HEAD"]).trim().to_string()
}

/// `scriptorium-ontology serve` on a repository, killed when dropped.
struct Serving {
    process: Running,
    /// Where it serves, `http://127.0.0.1:<port>`.
    base: String,
    dir: tempfile::TempDir,
}

impl Serving {
    /// `serve` on `repo` with `args`, on `addr`, once its banner says it
    /// serves (after publishing, given `--supergraph`).
    async fn start(repo: &Path, addr: &str, args: &[&str], env: &Env) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let log = std::fs::File::create(dir.path().join("serve.log")).unwrap();
        let mut command = scriptorium_ontology();
        command
            .arg("-C")
            .arg(repo)
            .args(["serve", "--addr", addr])
            .args(args)
            .envs(env.iter().map(|(k, v)| (k, v)))
            .stdin(Stdio::null())
            .stdout(log.try_clone().unwrap())
            .stderr(log);
        let mut serving = Self {
            process: Running(command.spawn().unwrap()),
            base: String::new(),
            dir,
        };
        let deadline = Instant::now() + COMMAND_TIMEOUT;
        loop {
            if let Some(base) = banner_base(&serving.log()) {
                serving.base = base;
                return serving;
            }
            if let Ok(Some(status)) = serving.process.0.try_wait() {
                panic!("serve exited ({status}):\n{}", serving.log());
            }
            assert!(
                Instant::now() < deadline,
                "serve never served:\n{}",
                serving.log()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Its output so far.
    fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("serve.log")).unwrap_or_default()
    }

    /// `host:port` it serves at.
    fn addr(&self) -> &str {
        self.base.trim_start_matches("http://")
    }
}

/// The base URL `serve`'s banner gives its local sub-ontologies.
fn banner_base(log: &str) -> Option<String> {
    let banner = log.split_once("ontology serve: ")?.1;
    banner
        .lines()
        .filter(|line| !line.contains("(remote, not served here)"))
        .find_map(|line| {
            let url = &line[line.find("http://")?..];
            let host = &url["http://".len()..];
            Some(format!("http://{}", &host[..host.find('/')?]))
        })
}

/// `serve --supergraph` on `repo`, and a gateway on what it published.
async fn published(
    binary: &Path,
    kind: Kind,
    repo: &Path,
    args: &[&str],
    env: &Env,
) -> (Gateway, Serving, tempfile::TempDir) {
    let out = tempfile::tempdir().unwrap();
    let supergraph = out.path().join("supergraph.graphql");
    let path = supergraph.to_str().unwrap();
    let mut all = vec!["--supergraph", path];
    all.extend_from_slice(args);
    let serving = Serving::start(repo, "127.0.0.1:0", &all, env).await;
    let gateway = Gateway::start(binary, kind, &read(&supergraph)).await;
    (gateway, serving, out)
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The answers to [`BATTERY`], in order.
async fn battery(gateway: &Gateway) -> Vec<Value> {
    let mut answers = Vec::new();
    for query in BATTERY {
        answers.push(gateway.data(query).await);
    }
    answers
}

// ---------------------------------------------------------------------------
// The documented path.

/// docs/ONTOLOGY.md, "Running a federated query": init, serve publishing a
/// supergraph, a gateway on it, and the query the docs send, verbatim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_documented_quickstart_answers_as_documented() {
    let Some((binary, kind)) = gateway("the_documented_quickstart") else {
        return;
    };
    let docs = include_str!("../../../docs/ONTOLOGY.md");
    let section = docs
        .split("### Running a federated query")
        .nth(1)
        .expect("the quickstart section");
    let body = section
        .split("-d '")
        .nth(1)
        .and_then(|rest| rest.split('\'').next())
        .expect("the query the quickstart sends");
    let query = serde_json::from_str::<Value>(body).unwrap()["query"]
        .as_str()
        .unwrap()
        .to_string();
    let repo = example().await;
    let (gateway, _serving, _out) = published(&binary, kind, repo.path(), &[], &Env::new()).await;
    assert_eq!(gateway.data(&query).await, bert());
}

// ---------------------------------------------------------------------------
// Git is the backend: a commit is a version.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_commit_is_a_version_that_can_be_served() {
    let Some((binary, kind)) = gateway("every_commit_is_a_version") else {
        return;
    };
    let repo = example().await;
    let first = head(repo.path());
    write(
        repo.path(),
        &[("ontology/core/data/Person/devlin.yaml", "name: J. Devlin\n")],
    );
    git(repo.path(), &["commit", "-qam", "core: shorten a name"]);
    let second = head(repo.path());
    // Edited, not committed: never served.
    write(
        repo.path(),
        &[(
            "ontology/core/data/Person/vaswani.yaml",
            "name: UNCOMMITTED\n",
        )],
    );

    let query =
        r#"{ devlin: person(id: "devlin") { name } vaswani: person(id: "vaswani") { name } }"#;
    let (now, serving_now, out_now) = published(&binary, kind, repo.path(), &[], &Env::new()).await;
    assert_eq!(
        now.data(query).await,
        json!({ "devlin": { "name": "J. Devlin" }, "vaswani": { "name": "Ashish Vaswani" } })
    );
    let (then, serving_then, out_then) =
        published(&binary, kind, repo.path(), &["--rev", &first], &Env::new()).await;
    assert_eq!(
        then.data(query).await,
        json!({ "devlin": { "name": "Jacob Devlin" }, "vaswani": { "name": "Ashish Vaswani" } })
    );

    // Each supergraph names the commit it was composed from, and each
    // subgraph the commit it serves, with every answer.
    let composed = |out: &tempfile::TempDir| read(&out.path().join("supergraph.graphql"));
    assert!(composed(&out_now).contains(&format!("HEAD@{}", &second[..7])));
    assert!(composed(&out_then).contains(&format!("{first}@{}", &first[..7])));
    for (serving, commit) in [(&serving_now, &second), (&serving_then, &first)] {
        let response = client()
            .post(format!("{}/core/graphql", serving.base))
            .json(&json!({ "query": "{ __typename }" }))
            .send()
            .await
            .unwrap();
        let revision = response.headers()["scriptorium-ontology-revision"]
            .to_str()
            .unwrap()
            .to_string();
        assert!(
            revision.contains(commit.as_str()),
            "{revision} is not {commit}"
        );
    }

    // Serving the working tree is refused: a version is a commit.
    let refused = run(
        repo.path(),
        &["serve", "--addr", "127.0.0.1:0", "--worktree"],
        &Env::new(),
    )
    .await;
    assert!(!refused.status.success(), "{}", text(&refused));
    assert!(
        text(&refused).contains("reads a commit"),
        "{}",
        text(&refused)
    );
}

// ---------------------------------------------------------------------------
// Publishing: a gateway only ever gets a supergraph that composes.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_that_does_not_compose_never_reaches_the_gateway() {
    let Some((binary, kind)) = gateway("a_commit_that_does_not_compose") else {
        return;
    };
    let repo = example().await;
    let (gateway, _serving, out) = published(&binary, kind, repo.path(), &[], &Env::new()).await;
    let supergraph = out.path().join("supergraph.graphql");
    let good = read(&supergraph);

    // software now says a paper's year is text; research says a number.
    let software = read(&repo.path().join("ontology/software/schema.graphql")).replace(
        "extend type Paper @key(fields: \"id\") {\n  id: ID!\n",
        "extend type Paper @key(fields: \"id\") {\n  id: ID!\n  year: String\n",
    );
    write(
        repo.path(),
        &[("ontology/software/schema.graphql", &software)],
    );
    git(
        repo.path(),
        &["commit", "-qam", "software: a conflicting year"],
    );

    let checked = run(repo.path(), &["check"], &Env::new()).await;
    assert!(!checked.status.success(), "{}", text(&checked));
    assert!(text(&checked).contains("Paper.year"), "{}", text(&checked));
    let path = supergraph.to_str().unwrap();
    let composed = run(
        repo.path(),
        &["compose", "--out", path, "--offline"],
        &Env::new(),
    )
    .await;
    assert!(!composed.status.success(), "{}", text(&composed));
    assert!(
        text(&composed).contains("composition failed"),
        "{}",
        text(&composed)
    );
    assert_eq!(read(&supergraph), good, "the published supergraph is kept");
    let elsewhere = out.path().join("next.graphql");
    let served = run(
        repo.path(),
        &[
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--supergraph",
            elsewhere.to_str().unwrap(),
        ],
        &Env::new(),
    )
    .await;
    assert!(!served.status.success(), "{}", text(&served));
    assert!(!elsewhere.exists(), "nothing is published");

    // The gateway still answers from the last good version.
    assert_eq!(gateway.data(BATTERY[0]).await, bert());
}

// ---------------------------------------------------------------------------
// Remote sub-ontologies: pinned in git, refreshed only by `pull`.

const VENUES: &str = r#"extend schema
  @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])

"A publication venue."
type Venue @key(fields: "id") {
  id: ID!
  name: String!
}

type Query {
  venue(id: ID!): Venue
  venues(first: Int): [Venue!]!
}
"#;

/// Talks: given at a venue another repository serves, about a paper this
/// one's research holds.
const TALKS: &str = r#"extend schema
  @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])

directive @inverse(of: String!) on FIELD_DEFINITION

"A talk about a paper, given at a venue."
type Talk @key(fields: "id") {
  id: ID!
  title: String!
  venue: Venue!
  paper: Paper!
}

# talks adds the talks given at each venue to the partner's Venue.
extend type Venue @key(fields: "id") {
  id: ID!
  talks: [Talk!]! @inverse(of: "venue")
}

# talks adds the talks about each paper to research's Paper.
extend type Paper @key(fields: "id") {
  id: ID!
  talks: [Talk!]! @inverse(of: "paper")
}

type Query {
  talk(id: ID!): Talk
  talks(first: Int): [Talk!]!
}
"#;

/// The partner's repository: venues, in git.
fn partner() -> tempfile::TempDir {
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    write(
        repo.path(),
        &[
            ("ontology/venues/subgraph.toml", GIT),
            ("ontology/venues/schema.graphql", VENUES),
            (
                "ontology/venues/data/Venue/neurips.yaml",
                "name: Neural Information Processing Systems\n",
            ),
            (
                "ontology/venues/data/Venue/naacl.yaml",
                "name: North American Chapter of the ACL\n",
            ),
        ],
    );
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-qm", "venues"]);
    repo
}

/// The example with talks, committed, and the partner's venues at `url`
/// pinned with `add-remote` (and `extra` arguments).
async fn with_talks_and_venues(repo: &Path, url: &str, extra: &[&str], env: &Env) {
    write(
        repo,
        &[
            ("ontology/talks/subgraph.toml", GIT),
            ("ontology/talks/schema.graphql", TALKS),
            (
                "ontology/talks/data/Talk/attention-in-practice.yaml",
                "title: Attention in practice\nvenue: neurips\npaper: attention-is-all-you-need\n",
            ),
            (
                "ontology/talks/data/Talk/bert-in-practice.yaml",
                "title: BERT in practice\nvenue: naacl\npaper: bert\n",
            ),
        ],
    );
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-qm", "talks"]);
    let mut args = vec!["add-remote", "venues", url];
    args.extend_from_slice(extra);
    ok(repo, &args, env).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_schema_change_reaches_clients_only_through_pull() {
    let Some((binary, kind)) = gateway("a_remote_schema_change") else {
        return;
    };
    let partner = partner();
    let serving_partner = Serving::start(partner.path(), "127.0.0.1:0", &[], &Env::new()).await;
    let url = format!("{}/venues/graphql", serving_partner.base);
    let repo = example().await;
    with_talks_and_venues(repo.path(), &url, &[], &Env::new()).await;

    let (gateway, serving, out) = published(&binary, kind, repo.path(), &[], &Env::new()).await;
    let supergraph = out.path().join("supergraph.graphql");
    let query = r#"{ venue(id: "neurips") { name talks { title paper { title } } } }"#;
    assert_eq!(
        gateway.data(query).await,
        json!({ "venue": { "name": "Neural Information Processing Systems", "talks": [
            { "title": "Attention in practice", "paper": { "title": "Attention Is All You Need" } },
        ]}})
    );

    // The partner adds a field, and serves it where it served before.
    let addr = serving_partner.addr().to_string();
    drop(serving_partner);
    write(
        partner.path(),
        &[
            (
                "ontology/venues/schema.graphql",
                &VENUES.replace("  name: String!\n", "  name: String!\n  city: String\n"),
            ),
            (
                "ontology/venues/data/Venue/neurips.yaml",
                "name: Neural Information Processing Systems\ncity: Vancouver\n",
            ),
        ],
    );
    git(partner.path(), &["commit", "-qam", "venues: city"]);
    let _serving_partner = Serving::start(partner.path(), &addr, &[], &Env::new()).await;

    // The pin is stale: publishing refuses, and the gateway knows no city.
    let pinned = read(&supergraph);
    let refused = run(
        repo.path(),
        &["compose", "--out", supergraph.to_str().unwrap()],
        &Env::new(),
    )
    .await;
    assert!(!refused.status.success(), "{}", text(&refused));
    assert!(text(&refused).contains("venues"), "{}", text(&refused));
    assert_eq!(read(&supergraph), pinned);
    let unknown = gateway
        .post(r#"{ venue(id: "neurips") { city } }"#, json!({}))
        .await;
    assert!(
        unknown["errors"].as_array().is_some_and(|e| !e.is_empty()),
        "{unknown}"
    );

    // `pull --dry-run` shows the change and writes nothing; `pull` commits
    // exactly the new pin.
    let before = head(repo.path());
    let diff = ok(repo.path(), &["pull", "--dry-run"], &Env::new()).await;
    assert!(diff.contains("city"), "{diff}");
    assert_eq!(head(repo.path()), before);
    ok(repo.path(), &["pull"], &Env::new()).await;
    assert_eq!(
        git(repo.path(), &["rev-parse", "HEAD~1"]).trim(),
        before,
        "one commit"
    );
    assert_eq!(
        git(repo.path(), &["diff", "--name-only", "HEAD~1", "HEAD"]).trim(),
        "ontology/venues/schema.graphql"
    );

    // Served again at the new commit, the gateway answers the new field.
    drop((gateway, serving));
    let (gateway, _serving, _out) = published(&binary, kind, repo.path(), &[], &Env::new()).await;
    assert_eq!(
        gateway
            .data(r#"{ venue(id: "neurips") { name city } }"#)
            .await,
        json!({ "venue": { "name": "Neural Information Processing Systems", "city": "Vancouver" } })
    );
}

// ---------------------------------------------------------------------------
// Every store, and another repository, behind one gateway.

#[cfg(all(feature = "postgres", feature = "dynamodb"))]
/// What the system answers across every store and both repositories: the
/// partner's venue (git, served by the partner's `serve`), the talks given
/// there (git), their papers (git), the authors (Postgres), the code
/// (`DynamoDB`), and the topics (markdown).
const ACROSS: &str = r#"{
  venue(id: "neurips") {
    name
    talks {
      title
      paper {
        title
        authors { name }
        implementations { url }
        topics { title backlinks { id } }
      }
    }
  }
  paper(id: "bert") { talks { title venue { name } } }
}"#;

#[cfg(all(feature = "postgres", feature = "dynamodb"))]
fn across() -> Value {
    json!({
        "venue": {
            "name": "Neural Information Processing Systems",
            "talks": [{
                "title": "Attention in practice",
                "paper": {
                    "title": "Attention Is All You Need",
                    "authors": [
                        { "name": "Ashish Vaswani" },
                        { "name": "Noam Shazeer" },
                        { "name": "Niki Parmar" },
                        { "name": "Jakob Uszkoreit" },
                        { "name": "Llion Jones" },
                        { "name": "Aidan N. Gomez" },
                        { "name": "Łukasz Kaiser" },
                        { "name": "Illia Polosukhin" },
                    ],
                    "implementations": [{ "url": "https://github.com/tensorflow/tensor2tensor" }],
                    "topics": [{ "title": "Transformers", "backlinks": [{ "id": "pretraining" }] }],
                },
            }],
        },
        "paper": { "talks": [{
            "title": "BERT in practice",
            "venue": { "name": "North American Chapter of the ACL" },
        }]},
    })
}

#[cfg(all(feature = "postgres", feature = "dynamodb"))]
mod stores {
    //! The system with every store: the example ontology with its people in
    //! Postgres and its code repositories in `DynamoDB`, its papers in git,
    //! its topics in markdown, and the partner's venues pinned.

    use super::{
        example, gateway, git, ok, partner, read, with_talks_and_venues, write, Env, Gateway, Path,
        Serving, Value,
    };
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    };
    use std::collections::HashMap;
    use std::sync::Arc;

    const POSTGRES_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_POSTGRES_URL";
    const DYNAMODB_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_DYNAMODB_ENDPOINT";
    const REGION: &str = "eu-central-1";

    /// A name no other run uses.
    fn unique(base: &str) -> String {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_micros() % 1_000_000_000);
        format!("{base}_{}_{at}_{n}", std::process::id())
    }

    /// The fields of every record of `ty` in the scaffolded `sub`, by key.
    fn records(repo: &Path, sub: &str, ty: &str) -> Vec<(String, Value)> {
        let dir = repo.join(format!("ontology/{sub}/data/{ty}"));
        let mut out: Vec<(String, Value)> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| {
                let path = entry.unwrap().path();
                let key = path.file_stem().unwrap().to_string_lossy().into_owned();
                (key, serde_yml::from_str(&read(&path)).unwrap())
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// The code repositories' `DynamoDB` table.
    pub struct Table {
        client: aws_sdk_dynamodb::Client,
        pub name: String,
        items: Vec<(String, Value)>,
    }

    impl Table {
        /// The table, with `items` (the scaffolded repositories) in it.
        pub async fn create(&self) {
            self.client
                .create_table()
                .table_name(&self.name)
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
            let s = |text: &str| AttributeValue::S(text.to_string());
            for (id, fields) in &self.items {
                let implements = fields["implements"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|key| s(key.as_str().unwrap()))
                    .collect();
                let item = HashMap::from([
                    ("pk".to_string(), s(&format!("Repository#{id}"))),
                    ("type".to_string(), s("Repository")),
                    ("url".to_string(), s(fields["url"].as_str().unwrap())),
                    ("implements".to_string(), AttributeValue::L(implements)),
                ]);
                self.client
                    .put_item()
                    .table_name(&self.name)
                    .set_item(Some(item))
                    .send()
                    .await
                    .unwrap();
            }
        }

        pub async fn delete(&self) {
            self.client
                .delete_table()
                .table_name(&self.name)
                .send()
                .await
                .unwrap();
        }
    }

    /// The example's people (from its scaffolded records), in a Postgres
    /// schema of this run's own; the client that made it, and its name.
    async fn people_in_postgres(url: &str, root: &Path) -> (tokio_postgres::Client, String) {
        let (db, connection) = tokio_postgres::connect(url, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let schema = unique("e2e_core");
        db.batch_execute(&format!(
            "CREATE SCHEMA {schema};
             CREATE TABLE {schema}.person (id text PRIMARY KEY, name text NOT NULL);"
        ))
        .await
        .unwrap();
        for (id, fields) in records(root, "core", "Person") {
            db.execute(
                &format!("INSERT INTO {schema}.person VALUES ($1, $2)"),
                &[&id, &fields["name"].as_str().unwrap()],
            )
            .await
            .unwrap();
        }
        (db, schema)
    }

    /// The example's code repositories, in a `DynamoDB` table of this run's
    /// own.
    async fn code_in_dynamodb(endpoint: &str, root: &Path) -> Table {
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(REGION))
            .endpoint_url(endpoint)
            .credentials_provider(aws_sdk_dynamodb::config::Credentials::new(
                "test", "test", None, None, "e2e",
            ))
            .load()
            .await;
        let table = Table {
            client: aws_sdk_dynamodb::Client::new(&config),
            name: unique("e2e-software").replace('_', "-"),
            items: records(root, "software", "Repository"),
        };
        table.create().await;
        table
    }

    /// `core` read from Postgres `schema` and `software` from `DynamoDB`
    /// `table`, with their records gone from git: committed.
    fn store_settings(root: &Path, schema: &str, table: &str) {
        write(
            root,
            &[
                (
                    "ontology/core/subgraph.toml",
                    &format!(
                        "description = \"People, in Postgres.\"\nowners = []\n\n[storage]\nkind = \"postgres\"\nurl_env = \"CORE_DB_URL\"\nschema = \"{schema}\"\n"
                    ),
                ),
                (
                    "ontology/software/subgraph.toml",
                    &format!(
                        "description = \"Code, in DynamoDB.\"\nowners = []\n\n[storage]\nkind = \"dynamodb\"\ntable = \"{table}\"\nregion = \"{REGION}\"\nendpoint_url_env = \"ONTOLOGY_DYNAMODB_ENDPOINT\"\n"
                    ),
                ),
            ],
        );
        git(
            root,
            &["rm", "-rq", "ontology/core/data", "ontology/software/data"],
        );
        git(
            root,
            &["commit", "-qam", "core in Postgres, software in DynamoDB"],
        );
    }

    /// The whole system, running. Dropped, the gateway and both servers
    /// stop; [`System::remove`] also removes what the run made in the
    /// databases.
    pub struct System {
        pub gateway: Arc<Gateway>,
        pub serving: Serving,
        partner: Serving,
        pub repo: tempfile::TempDir,
        _partner_repo: tempfile::TempDir,
        _out: tempfile::TempDir,
        pub db: tokio_postgres::Client,
        /// The Postgres schema holding the people's table, `person`.
        pub schema: String,
        pub table: Table,
        /// The partner's credential, which `add-remote` binds by name.
        pub token: String,
    }

    impl System {
        /// The system, or `None` (after a note) without a gateway or the
        /// test databases.
        pub async fn start(test: &str) -> Option<Self> {
            let (binary, kind) = gateway(test)?;
            let (Ok(url), Some(endpoint)) = (
                std::env::var(POSTGRES_ENV),
                std::env::var(DYNAMODB_ENV)
                    .ok()
                    .filter(|e| !e.trim().is_empty()),
            ) else {
                eprintln!("skipping {test}: set {POSTGRES_ENV} and {DYNAMODB_ENV}");
                return None;
            };
            let partner_repo = partner();
            let partner =
                Serving::start(partner_repo.path(), "127.0.0.1:0", &[], &Env::new()).await;
            let repo = example().await;
            let root = repo.path();

            let (db, schema) = people_in_postgres(&url, root).await;
            let table = code_in_dynamodb(&endpoint, root).await;
            // The ontology says so, and pins the partner's venues with a
            // credential; `check` agrees.
            store_settings(root, &schema, &table.name);
            let token = unique("e2e-partner-token");
            let env: Env = vec![
                ("CORE_DB_URL".into(), url),
                ("ONTOLOGY_DYNAMODB_ENDPOINT".into(), endpoint),
                ("AWS_ACCESS_KEY_ID".into(), "test".into()),
                ("AWS_SECRET_ACCESS_KEY".into(), "test".into()),
                ("AWS_CONFIG_FILE".into(), "/nonexistent".into()),
                ("AWS_SHARED_CREDENTIALS_FILE".into(), "/nonexistent".into()),
                ("AWS_EC2_METADATA_DISABLED".into(), "true".into()),
                ("PARTNER_TOKEN".into(), token.clone()),
            ];
            let venues = format!("{}/venues/graphql", partner.base);
            with_talks_and_venues(
                root,
                &venues,
                &["--header", "Authorization=PARTNER_TOKEN"],
                &env,
            )
            .await;
            ok(root, &["check"], &env).await;

            let out = tempfile::tempdir().unwrap();
            let supergraph = out.path().join("supergraph.graphql");
            let serving = Serving::start(
                root,
                "127.0.0.1:0",
                &["--supergraph", supergraph.to_str().unwrap()],
                &env,
            )
            .await;
            let gateway = Gateway::start(&binary, kind, &read(&supergraph)).await;
            Some(Self {
                gateway: Arc::new(gateway),
                serving,
                partner,
                repo,
                _partner_repo: partner_repo,
                _out: out,
                db,
                schema,
                table,
                token,
            })
        }

        /// Stops everything and removes what the run made in the databases.
        pub async fn remove(self) {
            let Self {
                gateway,
                serving,
                partner,
                db,
                schema,
                table,
                ..
            } = self;
            drop((gateway, serving, partner));
            db.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                .await
                .unwrap();
            table.delete().await;
        }
    }
}

#[cfg(all(feature = "postgres", feature = "dynamodb"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_ontology_across_every_store_and_another_repository() {
    let Some(system) = stores::System::start("one_ontology_across_every_store").await else {
        return;
    };
    assert_eq!(system.gateway.data(ACROSS).await, across());

    // Moving people to Postgres and code to DynamoDB changes no answer: the
    // example, all in git, answers the battery alike.
    let (binary, kind) = gateway("one_ontology_across_every_store").unwrap();
    let baseline_repo = example().await;
    let (baseline, _serving, _out) =
        published(&binary, kind, baseline_repo.path(), &[], &Env::new()).await;
    let expected = battery(&baseline).await;
    assert_eq!(expected[0], bert());
    assert_eq!(battery(&system.gateway).await, expected);

    // Many clients at once: every plan gets its own answers.
    let tasks: Vec<_> = (0..48)
        .map(|_| {
            let gateway = std::sync::Arc::clone(&system.gateway);
            tokio::spawn(async move { gateway.data(ACROSS).await })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap(), across());
    }

    // The partner's credential is bound in the git directory, by name, and
    // is in no commit.
    let history = git(system.repo.path(), &["log", "--all", "-p"]);
    assert!(!history.contains(&system.token), "a commit holds the token");
    let bindings = read(
        &system
            .repo
            .path()
            .join(".git/scriptorium-ontology/remotes.toml"),
    );
    assert!(bindings.contains("PARTNER_TOKEN"), "{bindings}");
    assert!(!bindings.contains(&system.token), "{bindings}");
    system.remove().await;
}

/// docs/ONTOLOGY.md, "Commands": a store that fails while answering gives
/// the client only a generic error, and `serve` logs the details.
#[cfg(all(feature = "postgres", feature = "dynamodb"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_store_fails_only_its_own_fields_and_recovers() {
    const MIXED: &str = r#"{
      paper(id: "bert") { title }
      person(id: "devlin") { name }
      repository(id: "google-research-bert") { url }
      venue(id: "naacl") { name }
    }"#;
    let Some(system) = stores::System::start("a_failing_store").await else {
        return;
    };
    let healthy = json!({
        "paper": { "title": "BERT: Pre-training of Deep Bidirectional Transformers for Language Understanding" },
        "person": { "name": "Jacob Devlin" },
        "repository": { "url": "https://github.com/google-research/bert" },
        "venue": { "name": "North American Chapter of the ACL" },
    });
    assert_eq!(system.gateway.data(MIXED).await, healthy);

    // Only the field the failing store answers fails; nothing names the
    // store to the client; serve logs it.
    let assert_only_lost = |answer: &Value, lost: &str, secret: &str| {
        assert!(
            answer["errors"].as_array().is_some_and(|e| !e.is_empty()),
            "{answer}"
        );
        for (field, value) in healthy.as_object().unwrap() {
            let expected = if field == lost { &Value::Null } else { value };
            assert_eq!(&answer["data"][field], expected, "{answer}");
        }
        assert!(!answer.to_string().contains(secret), "{answer}");
    };

    // Postgres: the people's table moves away under the running serve.
    let (schema, moved) = (&system.schema, format!("{}.person_moved", system.schema));
    system
        .db
        .batch_execute(&format!(
            "ALTER TABLE {schema}.person RENAME TO person_moved"
        ))
        .await
        .unwrap();
    let answer = system.gateway.post(MIXED, json!({})).await;
    assert_only_lost(&answer, "person", schema);
    assert_logged_privately(
        &system,
        "core",
        r#"{ person(id: "devlin") { name } }"#,
        schema,
    )
    .await;
    system
        .db
        .batch_execute(&format!("ALTER TABLE {moved} RENAME TO person"))
        .await
        .unwrap();
    assert_eq!(
        system.gateway.data(MIXED).await,
        healthy,
        "nothing restarted"
    );

    // DynamoDB: the code's table is deleted, then made again.
    system.table.delete().await;
    let answer = system.gateway.post(MIXED, json!({})).await;
    assert_only_lost(&answer, "repository", &system.table.name);
    assert_logged_privately(
        &system,
        "software",
        r#"{ repository(id: "google-research-bert") { url } }"#,
        &system.table.name,
    )
    .await;
    system.table.create().await;
    assert_eq!(
        system.gateway.data(MIXED).await,
        healthy,
        "nothing restarted"
    );
    system.remove().await;
}

/// Asked directly, subgraph `sub` answers `query` with an error that
/// names a reference but not `secret` (gateways may mask errors, so this
/// is serve's own answer), and serve logs the details under it.
#[cfg(all(feature = "postgres", feature = "dynamodb"))]
async fn assert_logged_privately(system: &stores::System, sub: &str, query: &str, secret: &str) {
    let answer: Value = client()
        .post(format!("{}/{sub}/graphql", system.serving.base))
        .json(&json!({ "query": query }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let message = answer["errors"][0]["message"].as_str().unwrap_or_default();
    assert!(!answer.to_string().contains(secret), "{answer}");
    let reference = message
        .split("reference ")
        .nth(1)
        .and_then(|rest| rest.split(|c: char| !c.is_ascii_hexdigit()).next())
        .filter(|reference| !reference.is_empty())
        .unwrap_or_else(|| panic!("no reference in {answer}"));
    let log = system.serving.log();
    let line = log
        .lines()
        .find(|line| line.contains(reference))
        .unwrap_or_else(|| panic!("reference {reference} is not in the log:\n{log}"));
    assert!(line.contains(secret), "{line}");
}

/// docs/ONTOLOGY.md, "Commands": every store must be reachable before
/// anything is published.
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serve_publishes_nothing_while_a_store_is_unreachable() {
    let repo = example().await;
    write(
        repo.path(),
        &[(
            "ontology/core/subgraph.toml",
            "[storage]\nkind = \"postgres\"\nurl_env = \"CORE_DB_URL\"\n",
        )],
    );
    git(repo.path(), &["rm", "-rq", "ontology/core/data"]);
    git(repo.path(), &["commit", "-qam", "core in Postgres"]);
    // Nothing listens at the port: connections are refused at once.
    let closed = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let env: Env = vec![(
        "CORE_DB_URL".into(),
        format!("postgres://ontology:hunter2@127.0.0.1:{closed}/ontology"),
    )];
    let out = tempfile::tempdir().unwrap();
    let supergraph = out.path().join("supergraph.graphql");
    let refused = run(
        repo.path(),
        &[
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--supergraph",
            supergraph.to_str().unwrap(),
        ],
        &env,
    )
    .await;
    assert!(!refused.status.success(), "{}", text(&refused));
    assert!(!supergraph.exists(), "nothing is published");
    assert!(text(&refused).contains("core"), "{}", text(&refused));
    assert!(!text(&refused).contains("hunter2"), "{}", text(&refused));
}

// ---------------------------------------------------------------------------
// Apollo's composition, as Apollo's tooling runs it.

const ROVER_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_ROVER";
/// The version of Apollo's composition the comparison runs.
const APOLLO_FEDERATION: &str = "=2.15.2";

/// Apollo's composition, run by Rover on what the running subgraphs serve
/// (`_service`), gives a supergraph a gateway answers exactly as it answers
/// with the one scriptorium publishes, through the same client-facing
/// schema.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apollo_composition_agrees_with_this_one() {
    let Some((binary, kind)) = gateway("apollo_composition_agrees") else {
        return;
    };
    let Some(rover) = std::env::var_os(ROVER_ENV).filter(|r| !r.is_empty()) else {
        eprintln!("skipping apollo_composition_agrees: set {ROVER_ENV} to a rover binary");
        return;
    };
    if std::env::var("APOLLO_ELV2_LICENSE").as_deref() != Ok("accept") {
        eprintln!(
            "skipping apollo_composition_agrees: Apollo's composition plugin is under the Elastic License 2.0; set APOLLO_ELV2_LICENSE=accept to accept it"
        );
        return;
    }
    let repo = example().await;
    let (ours, serving, _out) = published(&binary, kind, repo.path(), &[], &Env::new()).await;

    let dir = tempfile::tempdir().unwrap();
    let mut config = format!("federation_version: \"{APOLLO_FEDERATION}\"\nsubgraphs:\n");
    for name in ["core", "notes", "research", "software"] {
        use std::fmt::Write as _;
        let url = format!("{}/{name}/graphql", serving.base);
        let _ = write!(
            config,
            "  {name}:\n    routing_url: {url}\n    schema:\n      subgraph_url: {url}\n"
        );
    }
    let file = dir.path().join("supergraph.yaml");
    std::fs::write(&file, config).unwrap();
    let mut command = Command::new(rover);
    command
        .args(["supergraph", "compose", "--config"])
        .arg(&file)
        .env("APOLLO_TELEMETRY_DISABLED", "true");
    // A first run downloads the plugin.
    let composed = finish(command, Duration::from_mins(5)).await;
    assert!(composed.status.success(), "{}", text(&composed));
    let apollo = Gateway::start(&binary, kind, &String::from_utf8(composed.stdout).unwrap()).await;

    assert_eq!(battery(&ours).await, battery(&apollo).await);
    assert_eq!(api(&ours).await, api(&apollo).await);
}

/// The client-facing schema `gateway` serves, as introspection shows it:
/// each type's fields, arguments, and enum values, all in name order.
async fn api(gateway: &Gateway) -> Value {
    const INTROSPECTION: &str = "{ __schema { queryType { name } types { name kind fields { name args { name type { ...Ref } } type { ...Ref } } enumValues { name } inputFields { name type { ...Ref } } interfaces { name } possibleTypes { name } } } }
fragment Ref on __Type { kind name ofType { kind name ofType { kind name ofType { kind name } } } }";
    let mut schema = gateway.data(INTROSPECTION).await["__schema"].take();
    let by_name = |a: &Value, b: &Value| a["name"].as_str().cmp(&b["name"].as_str());
    let types = schema["types"].as_array_mut().unwrap();
    types.retain(|ty| !ty["name"].as_str().unwrap_or_default().starts_with("__"));
    types.sort_by(by_name);
    for ty in types {
        for list in [
            "fields",
            "enumValues",
            "inputFields",
            "interfaces",
            "possibleTypes",
        ] {
            if let Some(items) = ty[list].as_array_mut() {
                items.sort_by(by_name);
            }
        }
    }
    schema
}
