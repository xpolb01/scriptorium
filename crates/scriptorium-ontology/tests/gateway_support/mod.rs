//! What the gateway test binaries share: scratch repositories, served
//! sub-ontologies, and a gateway process on their supergraph. Each binary
//! uses its own part of it.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use git2::{IndexAddOption, Repository, RepositoryInitOptions, Signature};
use scriptorium_ontology::compose::compose;
use scriptorium_ontology::scaffold::scaffold_example;
use scriptorium_ontology::serve::{build_subgraphs, serve_listener};
use scriptorium_ontology::snapshot::GitSnapshot;
use scriptorium_ontology::{check, Registry, Snapshot};
use serde_json::{json, Value};

const GATEWAY_ENV: &str = "SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY";

/// Which gateway `SCRIPTORIUM_ONTOLOGY_TEST_GATEWAY` names, by its file name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    HiveRouter,
    HiveGateway,
}

/// The gateway binary, or `None` after a note saying why `test` skips.
pub fn gateway(test: &str) -> Option<(PathBuf, Kind)> {
    let Some(path) = std::env::var_os(GATEWAY_ENV).filter(|p| !p.is_empty()) else {
        eprintln!("skipping {test}: set {GATEWAY_ENV} to a Hive Router or Hive Gateway binary");
        return None;
    };
    let path = PathBuf::from(path);
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let kind = if name.contains("router") {
        Kind::HiveRouter
    } else {
        Kind::HiveGateway
    };
    Some((path, kind))
}

pub fn commit_all(root: &Path, message: &str) {
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

/// A scratch repository holding `files`, committed.
pub fn repository(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (path, contents) in files {
        let full = dir.path().join(path);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(full, contents).unwrap();
    }
    commit_all(dir.path(), "fixture");
    dir
}

/// The example ontology `init` scaffolds, committed.
pub fn example() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    scaffold_example(dir.path(), "ontology").unwrap();
    commit_all(dir.path(), "scaffold");
    dir
}

/// A repository's local sub-ontologies, checked and served over HTTP on a
/// port of their own until dropped.
pub struct Served {
    pub base: String,
    pub registry: Registry,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Served {
    pub async fn start(root: &Path) -> Self {
        let snapshot: Arc<dyn Snapshot> = Arc::new(GitSnapshot::open(root, "HEAD").unwrap());
        // `serve` refuses what `check` rejects; so do these tests.
        let report = check::check(snapshot.as_ref(), "ontology").unwrap();
        assert_eq!(report.errors(), 0, "{:#?}", report.issues);
        let (registry, issues) = Registry::load(snapshot.as_ref(), "ontology").unwrap();
        assert!(issues.is_empty(), "{issues:?}");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let subgraphs = build_subgraphs(&registry, &snapshot).unwrap();
        for subgraph in &subgraphs {
            subgraph.store.ready().await.unwrap();
        }
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        tokio::spawn(serve_listener(listener, subgraphs, async {
            let _ = stopped.await;
        }));
        Self {
            base,
            registry,
            stop: Some(stop),
        }
    }

    pub fn supergraph(&self) -> String {
        let composition = compose(&self.registry, &self.base);
        assert!(composition.is_ok(), "{:?}", composition.errors);
        composition.supergraph.unwrap()
    }
}

impl Drop for Served {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// How long a test waits for any one HTTP answer.
const ANSWER_TIMEOUT: Duration = Duration::from_mins(1);
/// How long a gateway may take to get ready.
const READY_TIMEOUT: Duration = Duration::from_mins(1);
/// How long one readiness probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Gateway starts before a test gives up finding a port it can bind.
const STARTS: usize = 5;

/// An HTTP client whose requests give up after [`ANSWER_TIMEOUT`], so a
/// stalled server fails the test instead of hanging it.
pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(ANSWER_TIMEOUT)
        .build()
        .unwrap()
}

/// A gateway process on a supergraph, killed when dropped.
pub struct Gateway {
    // Dropped in this order: the process is gone before its port is let go.
    process: Running,
    pub url: String,
    client: reqwest::Client,
    dir: tempfile::TempDir,
    _port: Port,
}

impl Gateway {
    /// A gateway of `kind` on `supergraph`, once it is ready.
    pub async fn start(binary: &Path, kind: Kind, supergraph: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("supergraph.graphql"), supergraph).unwrap();
        let client = client();
        // What takes no port lock (another program, a subgraph server, an
        // outgoing connection) can take the port between picking it and the
        // gateway binding it: that gateway exits, and another starts on a
        // new port.
        for _ in 0..STARTS {
            let port = free_port();
            let mut process = spawn(binary, kind, dir.path(), port.number);
            let base = format!("http://127.0.0.1:{}", port.number);
            match ready(
                &mut process,
                &client,
                &format!("{base}/readiness"),
                dir.path(),
            )
            .await
            {
                Ok(()) => {
                    return Self {
                        process,
                        url: format!("{base}/graphql"),
                        client,
                        dir,
                        _port: port,
                    }
                }
                Err(exit) if exit.to_lowercase().contains("address already in use") => {}
                Err(exit) => panic!("{exit}"),
            }
        }
        panic!(
            "the gateway found no port to bind in {STARTS} starts:\n{}",
            log(dir.path())
        );
    }

    pub fn log(&self) -> String {
        log(self.dir.path())
    }

    /// The whole answer to `query` with `variables`.
    pub async fn post(&self, query: &str, variables: Value) -> Value {
        let response = self
            .client
            .post(&self.url)
            .json(&json!({ "query": query, "variables": variables }))
            .send()
            .await
            .unwrap();
        let body = response.text().await.unwrap();
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}"))
    }

    /// The data answering `query`, which must answer without errors.
    pub async fn data(&self, query: &str) -> Value {
        let response = self.post(query, json!({})).await;
        let errors = response.get("errors").and_then(Value::as_array);
        assert!(
            errors.is_none_or(Vec::is_empty),
            "{query}\n{response}\n{}",
            self.log()
        );
        response["data"].clone()
    }
}

/// The gateway `binary` on the supergraph in `dir`, at 127.0.0.1:`port`,
/// logging to `dir`.
fn spawn(binary: &Path, kind: Kind, dir: &Path, port: u16) -> Running {
    let file = dir.join("supergraph.graphql");
    let log = std::fs::File::create(dir.join("gateway.log")).unwrap();
    let mut command = Command::new(binary);
    command
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(log.try_clone().unwrap())
        .stderr(log);
    match kind {
        Kind::HiveRouter => {
            command
                .env("SUPERGRAPH_FILE_PATH", &file)
                .env("HOST", "127.0.0.1")
                .env("PORT", port.to_string())
                .env("LOG_LEVEL", "warn");
        }
        Kind::HiveGateway => {
            command.arg("supergraph").arg(&file).args([
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ]);
        }
    }
    Running(
        command
            .spawn()
            .unwrap_or_else(|e| panic!("starting {}: {e}", binary.display())),
    )
}

/// Waits until the gateway answers at `readiness`; if it exits first, says
/// why, with its log.
async fn ready(
    process: &mut Running,
    client: &reqwest::Client,
    readiness: &str,
    dir: &Path,
) -> Result<(), String> {
    let deadline = Instant::now() + READY_TIMEOUT;
    loop {
        let answered = client
            .get(readiness)
            .timeout(PROBE_TIMEOUT)
            .send()
            .await
            .is_ok_and(|r| r.status().is_success());
        // An answer counts only while the gateway still runs.
        if let Ok(Some(status)) = process.0.try_wait() {
            return Err(format!("the gateway exited ({status}):\n{}", log(dir)));
        }
        if answered {
            return Ok(());
        }
        assert!(
            Instant::now() < deadline,
            "the gateway never got ready:\n{}",
            log(dir)
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn log(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("gateway.log")).unwrap_or_default()
}

/// A port that was free a moment ago. While it is held, no other gateway
/// these tests start picks it, in this process or another run from the
/// same target directory: each takes the lock of the port it picks.
struct Port {
    number: u16,
    _lock: std::fs::File,
}

fn free_port() -> Port {
    let locks = Path::new(env!("CARGO_TARGET_TMPDIR")).join("gateway-ports");
    std::fs::create_dir_all(&locks).unwrap();
    loop {
        let number = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(locks.join(number.to_string()))
            .unwrap();
        match lock.try_lock() {
            Ok(()) => {
                return Port {
                    number,
                    _lock: lock,
                }
            }
            Err(std::fs::TryLockError::WouldBlock) => {}
            Err(std::fs::TryLockError::Error(e)) => panic!("locking port {number}: {e}"),
        }
    }
}

/// The repository at `root`, served and behind the gateway; `None` (after
/// a note) when no gateway is configured.
pub async fn behind_gateway(test: &str, root: &Path) -> Option<(Served, Gateway)> {
    let (binary, kind) = gateway(test)?;
    let served = Served::start(root).await;
    let gateway = Gateway::start(&binary, kind, &served.supergraph()).await;
    Some((served, gateway))
}

/// The binary, with the repository's git environment cleared.
pub fn scriptorium_ontology() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_scriptorium-ontology"));
    for var in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_COMMON_DIR",
        "GIT_PREFIX",
    ] {
        command.env_remove(var);
    }
    command
}

/// A child process, killed when dropped.
pub struct Running(pub Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub const LINK: &str =
    r#"extend schema @link(url: "https://specs.apollo.dev/federation/v2.3", import: ["@key"])"#;
pub const GIT: &str = "[storage]\nkind = \"git\"\n";
