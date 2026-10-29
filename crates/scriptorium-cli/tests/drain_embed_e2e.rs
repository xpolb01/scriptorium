//! End-to-end tests for `scriptorium ingest-drain` and the embeddings store.
//!
//! A drain that ingests a queued source must embed the pages it writes, as
//! `ingest` does. Without that step the new pages stay out of search until a
//! manual `scriptorium reindex`.
//!
//! Each test runs the `scriptorium` binary against a fresh vault. The
//! vault's chat provider is `openai`, pointed at a local stub that answers
//! every chat completion with one fixed ingest plan. Embeddings use the mock
//! provider. No test reaches the network or the keychain.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::missing_panics_doc)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use scriptorium_core::embed::EmbeddingsStore;
use tempfile::TempDir;

/// The page the stubbed LLM tells every ingest to create.
const PAGE: &str = "wiki/concepts/kestrel-relay.md";

/// Answer every HTTP request on a fresh local port with a chat completion
/// whose content is an ingest plan that creates [`PAGE`]. Returns the base
/// URL.
fn spawn_chat_stub() -> String {
    let plan = serde_json::json!({
        "summary": "Kestrel relay test note",
        "pages": [{
            "action": "create",
            "path": PAGE,
            "title": "Kestrel Relay",
            "tags": ["concept", "domain/test"],
            "body": "## Overview\n\nThe Kestrel relay forwards each message to two peers.\n",
            "source_quote": null,
            "supersedes": []
        }],
        "log_entry": "ingested the Kestrel relay test note",
        "redundant": false
    });
    let body = serde_json::json!({
        "model": "stub",
        "choices": [{"message": {"content": plan.to_string()}}],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1}
    })
    .to_string();
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
            let _ = reader.read_exact(&mut vec![0; len]);
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

/// A `scriptorium` command that sees none of the caller's environment
/// except `PATH`: `HOME` is `home`, and the chat provider is the stub at
/// `chat_url`.
fn scriptorium(home: &Path, chat_url: &str) -> Command {
    let mut cmd = Command::cargo_bin("scriptorium").expect("scriptorium binary");
    cmd.env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", home)
        .env("OPENAI_BASE_URL", chat_url)
        .env("OPENAI_API_KEY", "test-key")
        .env("OPENAI_MODEL", "stub");
    cmd
}

/// A fresh HOME, a vault initialised in its own tempdir and switched to the
/// stubbed chat provider and the mock embedder, a session note queued in
/// that vault, and the stub's URL.
struct Fixture {
    home: TempDir,
    vault: TempDir,
    chat_url: String,
}

impl Fixture {
    fn new() -> Self {
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
            "[llm]\nprovider = \"openai\"\nmodel = \"stub\"\n\n\
             [embeddings]\nprovider = \"mock\"\nmodel = \"mock-embed\"\n",
        )
        .unwrap();
        let sessions = fixture.vault.path().join("sources/sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        let note = sessions.join("2026-10-29-session-kestrel.md");
        std::fs::write(
            &note,
            "# Kestrel relay\n\nThe Kestrel relay forwards each message to two peers.\n",
        )
        .unwrap();
        fixture
            .run_in_vault(&["ingest-enqueue"])
            .arg(&note)
            .assert()
            .success();
        fixture
    }

    fn run(&self, args: &[&str]) -> Command {
        let mut cmd = scriptorium(self.home.path(), &self.chat_url);
        cmd.args(args);
        cmd
    }

    fn run_in_vault(&self, args: &[&str]) -> Command {
        let mut cmd = self.run(&["-C"]);
        cmd.arg(self.vault.path()).args(args);
        cmd
    }

    fn store_path(&self) -> PathBuf {
        self.vault.path().join(".scriptorium/embeddings.sqlite")
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
}

#[test]
fn drain_embeds_the_pages_it_ingests() {
    let fixture = Fixture::new();

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
    let store = EmbeddingsStore::open(&fixture.store_path()).expect("open store");
    assert!(
        store.distinct_page_count().expect("count pages") >= 1,
        "the page the drain wrote should be in the embeddings store"
    );
}

#[test]
fn drain_dry_run_embeds_nothing() {
    let fixture = Fixture::new();

    let stdout = fixture.drain(&["--dry-run"]);

    assert!(stdout.contains("ingested=0"), "drain output: {stdout}");
    assert!(!stdout.contains("embedded"), "drain output: {stdout}");
    let rows = if fixture.store_path().exists() {
        EmbeddingsStore::open(&fixture.store_path())
            .expect("open store")
            .len()
            .expect("count rows")
    } else {
        0
    };
    assert_eq!(rows, 0, "a dry run must not write embeddings");
}
