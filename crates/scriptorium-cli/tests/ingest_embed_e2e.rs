//! End-to-end tests for the commands that ingest, and the embeddings store.
//!
//! Every command that commits pages must embed them, as `ingest` does.
//! Without that step the new pages stay out of search until a manual
//! `scriptorium reindex`. With `[embeddings] contextual = true`, every
//! command that embeds must embed as `reindex` does: plain rows would pass
//! for cache hits there and never get their situating headers.
//!
//! Each test runs the `scriptorium` binary against a fresh vault. The
//! vault's chat provider is `openai`, pointed at a local stub that answers
//! every chat completion with one fixed ingest plan, or with one situating
//! header per chunk when the request asks for contextual headers.
//! Embeddings use the mock provider. No test reaches the network or the
//! keychain.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::missing_panics_doc)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

use assert_cmd::cargo::CommandCargoExt;
use assert_cmd::Command;
use scriptorium_core::embed::EmbeddingsStore;
use tempfile::TempDir;

/// The page the stubbed LLM tells every ingest to create. Its two
/// sections make two chunks.
const PAGE: &str = "wiki/concepts/kestrel-relay.md";

/// A word in every situating header the stub writes, to find those headers
/// in the store's keyword index.
const CONTEXT_MARKER: &str = "zephyrctx";

/// The source the tests ingest.
const NOTE: &str = "# Kestrel relay\n\nThe Kestrel relay forwards each message to two peers.\n";

/// Answer every HTTP request on a fresh local port with a chat completion.
/// A request for contextual headers, which lists the page's chunks, gets
/// one header per chunk, each carrying [`CONTEXT_MARKER`]. Any other request
/// gets an ingest plan that creates [`PAGE`]. Returns the base URL.
fn spawn_chat_stub() -> String {
    let plan = serde_json::json!({
        "summary": "Kestrel relay test note",
        "pages": [{
            "action": "create",
            "path": PAGE,
            "title": "Kestrel Relay",
            "tags": ["concept", "domain/test"],
            "body": "## Overview\n\nThe Kestrel relay forwards each message to two peers.\n\n\
                     ## Failover\n\nA peer that misses three beacons is dropped.\n",
            "source_quote": null,
            "supersedes": []
        }],
        "log_entry": "ingested the Kestrel relay test note",
        "redundant": false
    });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            // Read the request head and body before answering.
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut len = 0;
            let mut line = String::new();
            while reader.read_line(&mut line).unwrap_or(0) > 0 && line != "\r\n" {
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    len = value.trim().parse().unwrap_or(0);
                }
                line.clear();
            }
            let mut request = vec![0; len];
            let _ = reader.read_exact(&mut request);
            let request = String::from_utf8_lossy(&request);
            let content = if request.contains("situating") {
                let headers: Vec<String> = (0..request.matches("--- chunk ").count())
                    .map(|i| format!("{CONTEXT_MARKER} header for chunk {i}"))
                    .collect();
                serde_json::json!({ "contexts": headers }).to_string()
            } else {
                plan.to_string()
            };
            let body = serde_json::json!({
                "model": "stub",
                "choices": [{"message": {"content": content}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1}
            })
            .to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    url
}

/// A `scriptorium` process that sees none of the caller's environment
/// except `PATH`: `HOME` is `home`, and the chat provider is the stub at
/// `chat_url`.
fn scriptorium(home: &Path, chat_url: &str) -> std::process::Command {
    let mut cmd = std::process::Command::cargo_bin("scriptorium").expect("scriptorium binary");
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("OPENAI_BASE_URL", chat_url)
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_MODEL", "stub");
    cmd
}

/// A child process that is killed and reaped when dropped, so a failing
/// test leaves no process behind.
struct KillOnDrop(Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A directory outside the vault holding one source file with [`NOTE`].
fn source_dir() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("kestrel.md"), NOTE).unwrap();
    dir
}

/// A fresh HOME, a vault initialised in its own tempdir and switched to the
/// stubbed chat provider and the mock embedder, a session note in that
/// vault, and the stub's URL. Contextual retrieval is off unless the
/// fixture comes from [`Fixture::with_contextual`].
struct Fixture {
    home: TempDir,
    vault: TempDir,
    chat_url: String,
}

impl Fixture {
    fn new() -> Self {
        Self::with_contextual(false)
    }

    /// Like [`Fixture::new`], with `[embeddings] contextual` set to
    /// `contextual`.
    fn with_contextual(contextual: bool) -> Self {
        let home = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".scriptorium")).unwrap();
        let vault = tempfile::tempdir().unwrap();
        let chat_url = spawn_chat_stub();
        let fixture = Self {
            home,
            vault,
            chat_url,
        };
        fixture
            .run(&["init"])
            .arg(fixture.vault.path())
            .assert()
            .success();
        std::fs::write(
            fixture.vault.path().join(".scriptorium/config.toml"),
            format!(
                "[llm]\nprovider = \"openai\"\nmodel = \"stub\"\n\n\
                 [embeddings]\nprovider = \"mock\"\nmodel = \"mock-embed\"\n\
                 contextual = {contextual}\n"
            ),
        )
        .unwrap();
        std::fs::create_dir_all(fixture.vault.path().join("sources/sessions")).unwrap();
        std::fs::write(fixture.note(), NOTE).unwrap();
        fixture
    }

    /// The session note in the vault.
    fn note(&self) -> PathBuf {
        self.vault
            .path()
            .join("sources/sessions/2026-10-29-session-kestrel.md")
    }

    /// Queue the session note for a drain.
    fn queue_note(&self) {
        self.run_in_vault(&["ingest-enqueue"])
            .arg(self.note())
            .assert()
            .success();
    }

    fn run(&self, args: &[&str]) -> Command {
        let mut cmd = scriptorium(self.home.path(), &self.chat_url);
        cmd.args(args);
        Command::from_std(cmd)
    }

    fn run_in_vault(&self, args: &[&str]) -> Command {
        let mut cmd = self.run(&["-C"]);
        cmd.arg(self.vault.path()).args(args);
        cmd
    }

    fn store_path(&self) -> PathBuf {
        self.vault.path().join(".scriptorium/embeddings.sqlite")
    }

    /// Pages with rows in the embeddings store.
    fn embedded_pages(&self) -> usize {
        if !self.store_path().exists() {
            return 0;
        }
        EmbeddingsStore::open(&self.store_path())
            .expect("open store")
            .distinct_page_count()
            .expect("count pages")
    }

    /// Chunks in the store whose text carries a situating header from the
    /// stub.
    fn context_hits(&self) -> usize {
        let store = EmbeddingsStore::open(&self.store_path()).expect("open store");
        store
            .keyword_search(CONTEXT_MARKER, "mock", "mock-embed", 10)
            .expect("keyword search")
            .len()
    }

    /// Run `ingest-drain` with no debounce and return its stdout.
    fn drain(&self, extra: &[&str]) -> String {
        let out = self
            .run_in_vault(&["ingest-drain", "--debounce", "0"])
            .args(extra)
            .assert()
            .success();
        String::from_utf8(out.get_output().stdout.clone()).unwrap()
    }

    /// Run `bulk-ingest` on `dir` and return its stdout.
    fn bulk_ingest(&self, dir: &Path, extra: &[&str]) -> String {
        let out = self
            .run_in_vault(&["bulk-ingest"])
            .arg(dir)
            .args(extra)
            .assert()
            .success();
        String::from_utf8(out.get_output().stdout.clone()).unwrap()
    }
}

#[test]
fn drain_embeds_the_pages_it_ingests() {
    let fixture = Fixture::new();
    fixture.queue_note();

    let stdout = fixture.drain(&[]);

    assert!(stdout.contains("ingested=1"), "drain output: {stdout}");
    assert!(
        fixture.vault.path().join(PAGE).is_file(),
        "drain should have written {PAGE}; output: {stdout}"
    );
    assert!(
        stdout.contains("drain: embedded "),
        "drain output should report the embedded chunks: {stdout}"
    );
    assert!(
        fixture.embedded_pages() >= 1,
        "the page the drain wrote should be in the embeddings store"
    );
    assert_eq!(
        fixture.context_hits(),
        0,
        "contextual retrieval is off, so no chunk carries a header"
    );
}

/// With `[embeddings] contextual = true`, the drain embeds as `reindex`
/// does: the chat provider writes a situating header for each chunk.
#[test]
fn drain_embeds_contextually_when_configured() {
    let fixture = Fixture::with_contextual(true);
    fixture.queue_note();

    let stdout = fixture.drain(&[]);

    assert!(
        stdout.contains("drain: embedded "),
        "drain output: {stdout}"
    );
    assert_eq!(
        fixture.context_hits(),
        2,
        "both chunks of {PAGE} should carry a situating header; output: {stdout}"
    );
}

/// The drain's hash-store append runs after the ingest commit. When it
/// fails, the source does not count as ingested, but its pages are
/// committed and must still be embedded. A read-only hash store makes the
/// append fail.
#[cfg(unix)]
#[test]
fn drain_embeds_pages_committed_before_a_hash_store_failure() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    fixture.queue_note();
    let hashes = fixture.vault.path().join(".scriptorium/source-hashes.txt");
    if !hashes.exists() {
        std::fs::write(&hashes, "").unwrap();
    }
    let set_mode = |mode| std::fs::set_permissions(&hashes, std::fs::Permissions::from_mode(mode));
    set_mode(0o444).unwrap();
    if std::fs::OpenOptions::new()
        .append(true)
        .open(&hashes)
        .is_ok()
    {
        // The mode bits do not bind root.
        set_mode(0o644).unwrap();
        return;
    }

    let out = fixture
        .run_in_vault(&["ingest-drain", "--debounce", "0"])
        .assert()
        .success();
    set_mode(0o644).unwrap();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();

    assert!(
        stdout.contains("ingested=0") && stdout.contains("failures=1"),
        "drain output: {stdout}"
    );
    assert!(
        stderr.contains("hash-store append"),
        "drain stderr: {stderr}"
    );
    assert!(
        fixture.vault.path().join(PAGE).is_file(),
        "drain should have written {PAGE}; output: {stdout}"
    );
    assert!(
        stdout.contains("drain: embedded "),
        "drain output should report the embedded chunks: {stdout}"
    );
    assert!(
        fixture.embedded_pages() >= 1,
        "the page the drain committed should be in the embeddings store"
    );
}

#[test]
fn drain_dry_run_embeds_nothing() {
    let fixture = Fixture::new();
    fixture.queue_note();

    let stdout = fixture.drain(&["--dry-run"]);

    assert!(stdout.contains("ingested=0"), "drain output: {stdout}");
    assert!(!stdout.contains("embedded"), "drain output: {stdout}");
    assert_eq!(
        fixture.embedded_pages(),
        0,
        "a dry run must not write embeddings"
    );
}

/// With `[embeddings] contextual = true`, `ingest` embeds as `reindex`
/// does.
#[test]
fn ingest_embeds_contextually_when_configured() {
    let fixture = Fixture::with_contextual(true);

    let out = fixture
        .run_in_vault(&["ingest"])
        .arg(fixture.note())
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();

    assert!(stdout.contains("embedded: "), "ingest output: {stdout}");
    assert_eq!(
        fixture.context_hits(),
        2,
        "both chunks of {PAGE} should carry a situating header; output: {stdout}"
    );
}

#[test]
fn bulk_ingest_embeds_the_pages_it_ingests() {
    let fixture = Fixture::new();
    let sources = source_dir();

    let stdout = fixture.bulk_ingest(sources.path(), &[]);

    assert!(stdout.contains("Ingested: 1"), "bulk output: {stdout}");
    assert!(
        stdout.contains("Embedded: "),
        "bulk output should report the embedded chunks: {stdout}"
    );
    assert!(
        fixture.embedded_pages() >= 1,
        "the page bulk-ingest wrote should be in the embeddings store"
    );
    assert_eq!(
        fixture.context_hits(),
        0,
        "contextual retrieval is off, so no chunk carries a header"
    );
}

/// With `[embeddings] contextual = true`, `bulk-ingest` embeds as
/// `reindex` does.
#[test]
fn bulk_ingest_embeds_contextually_when_configured() {
    let fixture = Fixture::with_contextual(true);
    let sources = source_dir();

    let stdout = fixture.bulk_ingest(sources.path(), &[]);

    assert_eq!(
        fixture.context_hits(),
        2,
        "both chunks of {PAGE} should carry a situating header; output: {stdout}"
    );
}

#[test]
fn bulk_ingest_dry_run_embeds_nothing() {
    let fixture = Fixture::new();
    let sources = source_dir();

    let stdout = fixture.bulk_ingest(sources.path(), &["--dry-run"]);

    assert!(!stdout.contains("Embedded"), "bulk output: {stdout}");
    assert_eq!(
        fixture.embedded_pages(),
        0,
        "a dry run must not write embeddings"
    );
}

/// A file that cannot be read fails on its own. The run goes on and
/// embeds the page committed before it.
#[cfg(unix)]
#[test]
fn bulk_ingest_embeds_pages_committed_before_an_unreadable_file() {
    use std::os::unix::fs::PermissionsExt;
    let fixture = Fixture::new();
    let sources = source_dir();
    let unreadable = sources.path().join("later.md");
    std::fs::write(&unreadable, "# Later\n\nA file nobody can read.\n").unwrap();
    let set_mode =
        |mode| std::fs::set_permissions(&unreadable, std::fs::Permissions::from_mode(mode));
    set_mode(0o000).unwrap();
    if std::fs::File::open(&unreadable).is_ok() {
        // The mode bits do not bind root.
        set_mode(0o644).unwrap();
        return;
    }

    let out = fixture
        .run_in_vault(&["bulk-ingest"])
        .arg(sources.path())
        .assert();
    set_mode(0o644).unwrap();
    let out = out.success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();

    assert!(
        stdout.contains("Ingested: 1") && stdout.contains("Failed: 1"),
        "bulk output: {stdout}"
    );
    assert!(stdout.contains("Embedded: "), "bulk output: {stdout}");
    assert!(
        fixture.embedded_pages() >= 1,
        "the committed page should be in the embeddings store"
    );
}

/// `watch` embeds what each ingest commits, as `reindex` does when
/// `[embeddings] contextual` is on.
#[test]
fn watch_embeds_contextually_when_configured() {
    let fixture = Fixture::with_contextual(true);
    let articles = fixture.vault.path().join("sources/articles");
    std::fs::create_dir_all(&articles).unwrap();
    let mut watch = KillOnDrop(
        scriptorium(fixture.home.path(), &fixture.chat_url)
            .arg("-C")
            .arg(fixture.vault.path())
            .arg("watch")
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start watch"),
    );
    let mut lines = BufReader::new(watch.0.stdout.take().unwrap()).lines();
    // The banner comes just before the watcher arms.
    let banner = lines
        .by_ref()
        .map_while(Result::ok)
        .find(|line| line.contains("watch: watching"));
    assert!(banner.is_some(), "watch exited before its banner");
    // Keep reading, so a full pipe never blocks the watcher.
    std::thread::spawn(move || lines.for_each(drop));
    std::thread::sleep(Duration::from_secs(1));

    std::fs::write(articles.join("kestrel.md"), NOTE).unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut hits = 0;
    while hits < 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        // The watcher may hold the store; retry until it lets go.
        hits = EmbeddingsStore::open(&fixture.store_path())
            .and_then(|store| store.keyword_search(CONTEXT_MARKER, "mock", "mock-embed", 10))
            .map_or(0, |found| found.len());
    }
    drop(watch);

    assert_eq!(
        hits, 2,
        "both chunks of {PAGE} should carry a situating header"
    );
}

/// `maintain --fix` re-embeds a stale page as `reindex` does when
/// `[embeddings] contextual` is on.
#[test]
fn maintain_fix_embeds_contextually_when_configured() {
    let fixture = Fixture::with_contextual(true);
    fixture
        .run_in_vault(&["ingest"])
        .arg(fixture.note())
        .assert()
        .success();
    // Drop every row, so the page is stale.
    EmbeddingsStore::open(&fixture.store_path())
        .expect("open store")
        .retain_page_versions(&[])
        .expect("drop every row");

    let out = fixture
        .run_in_vault(&["maintain", "--fix"])
        .assert()
        .success();
    let stdout = String::from_utf8(out.get_output().stdout.clone()).unwrap();

    assert_eq!(
        fixture.context_hits(),
        2,
        "both chunks of {PAGE} should carry a situating header; output: {stdout}"
    );
}

/// `social facebook` embeds the pages it ingests as `reindex` does when
/// `[embeddings] contextual` is on.
#[test]
fn social_facebook_embeds_contextually_when_configured() {
    let fixture = Fixture::with_contextual(true);
    let export = tempfile::tempdir().unwrap();
    let posts = export.path().join("your_facebook_activity/posts");
    std::fs::create_dir_all(&posts).unwrap();
    let post = serde_json::json!([{
        "timestamp": 1_700_000_000,
        "title": "Kestrel relay",
        "data": [{"post": "The Kestrel relay forwards each message to two peers."}]
    }]);
    std::fs::write(posts.join("your_posts_1.json"), post.to_string()).unwrap();

    let out = fixture
        .run_in_vault(&["social", "facebook"])
        .arg(export.path())
        .args(["--categories", "posts"])
        .assert()
        .success();
    let stderr = String::from_utf8(out.get_output().stderr.clone()).unwrap();

    assert_eq!(
        fixture.context_hits(),
        2,
        "both chunks of {PAGE} should carry a situating header; stderr: {stderr}"
    );
}
