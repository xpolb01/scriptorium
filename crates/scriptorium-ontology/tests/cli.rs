//! `scriptorium-ontology` end to end through the binary: repository
//! selection, commit-only serving, and refusing to publish an ontology
//! that silently drops a broken sub-ontology.

use std::path::Path;
use std::process::Command;

use assert_cmd::prelude::*;

/// Drop the `GIT_*` variables a parent git exports to its children (hooks
/// and `rebase --exec` set `GIT_DIR`). Inherited, they point `git init` at
/// the caller's repository instead of the test's temporary one.
fn hermetic(cmd: &mut Command) -> &mut Command {
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("GIT_") {
            cmd.env_remove(name);
        }
    }
    cmd
}

/// The binary with an isolated `HOME` (telemetry, global config, git config).
fn scriptorium(home: &Path, cwd: &Path) -> Command {
    let mut cmd = Command::cargo_bin("scriptorium-ontology").unwrap();
    hermetic(&mut cmd)
        .current_dir(cwd)
        .env("HOME", home)
        .env("RUST_LOG", "error")
        .env_remove("SCRIPTORIUM_TRACEPARENT");
    cmd
}

fn git(cwd: &Path, args: &[&str]) {
    let status = hermetic(&mut Command::new("git"))
        .args(["-c", "user.name=t", "-c", "user.email=t@example.com"])
        .args(args)
        .current_dir(cwd)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?}");
}

fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// `stderr` as one line: miette wraps reports at the terminal width.
fn stderr_flat(output: &std::process::Output) -> String {
    stderr(output)
        .split_whitespace()
        .filter(|word| !matches!(*word, "│" | "×"))
        .collect::<Vec<_>>()
        .join(" ")
}

#[test]
fn init_targets_the_enclosing_repository_and_refuses_outside_one() {
    let home = tempfile::tempdir().unwrap();
    let not_a_repo = tempfile::tempdir().unwrap();
    let out = scriptorium(home.path(), not_a_repo.path())
        .args(["init"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("not inside a git repository"),
        "{}",
        stderr(&out)
    );
    assert!(!not_a_repo.path().join("ontology").exists());

    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    std::fs::create_dir_all(repo.path().join("sub/dir")).unwrap();
    scriptorium(home.path(), &repo.path().join("sub/dir"))
        .args(["init"])
        .assert()
        .success();
    assert!(
        repo.path().join("ontology/core/schema.graphql").exists(),
        "scaffolded at the repo root"
    );

    let out = scriptorium(home.path(), repo.path())
        .args(["check"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
}

#[test]
fn init_commits_with_an_absolute_repo_and_a_dotted_dir() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    let elsewhere = tempfile::tempdir().unwrap();
    scriptorium(home.path(), elsewhere.path())
        .arg("-C")
        .arg(repo.path())
        .args(["init", "--dir", "./onto/"])
        .assert()
        .success();
    let out = scriptorium(home.path(), elsewhere.path())
        .arg("-C")
        .arg(repo.path())
        .args(["list", "--dir", "./onto"])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("research"),
        "committed and listed: {stdout}"
    );
}

#[test]
fn compose_and_serve_refuse_a_registry_with_broken_sub_ontologies() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    std::fs::create_dir_all(repo.path().join("ontology/broken")).unwrap();
    std::fs::write(
        repo.path().join("ontology/broken/subgraph.toml"),
        "descripton = \"typo\"\n",
    )
    .unwrap();
    std::fs::write(
        repo.path().join("ontology/broken/schema.graphql"),
        "type Query { x: Int }\n",
    )
    .unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-qm", "broken sub-ontology"]);

    let out = scriptorium(home.path(), repo.path())
        .args(["compose"])
        .output()
        .unwrap();
    assert!(!out.status.success(), "compose must fail");
    assert!(
        stderr(&out).contains("ontology/broken/subgraph.toml"),
        "{}",
        stderr(&out)
    );

    let supergraph = repo.path().join("supergraph.graphql");
    let out = scriptorium(home.path(), repo.path())
        .args(["serve", "--addr", "127.0.0.1:0", "--supergraph"])
        .arg(&supergraph)
        .output()
        .unwrap();
    assert!(!out.status.success(), "serve must fail before binding");
    assert!(!supergraph.exists(), "no partial supergraph written");

    // `list` still works and reports the problem.
    let out = scriptorium(home.path(), repo.path())
        .args(["list"])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(
        stderr(&out).contains("warning: ontology/broken/subgraph.toml"),
        "{}",
        stderr(&out)
    );
}

#[test]
fn serve_leaves_the_supergraph_alone_when_the_port_is_taken() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = taken.local_addr().unwrap().to_string();
    let supergraph = repo.path().join("supergraph.graphql");
    std::fs::write(&supergraph, "# the running server's supergraph\n").unwrap();

    let out = scriptorium(home.path(), repo.path())
        .args(["serve", "--addr", &addr, "--supergraph"])
        .arg(&supergraph)
        .output()
        .unwrap();
    assert!(!out.status.success(), "bind must fail");
    assert!(stderr(&out).contains("binding"), "{}", stderr(&out));
    assert_eq!(
        std::fs::read_to_string(&supergraph).unwrap(),
        "# the running server's supergraph\n",
        "supergraph untouched"
    );
    drop(taken);
}

#[test]
fn one_publisher_per_supergraph_path() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let supergraph = repo.path().join("supergraph.graphql");

    // A running server holds the publication lock.
    let mut server = scriptorium(home.path(), repo.path())
        .args(["serve", "--addr", "127.0.0.1:0", "--supergraph"])
        .arg(&supergraph)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    for _ in 0..100 {
        if supergraph.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(supergraph.exists(), "first server published");
    let published = std::fs::read_to_string(&supergraph).unwrap();

    for args in [
        vec!["serve", "--addr", "127.0.0.1:0", "--supergraph"],
        vec!["compose", "--offline", "--out"],
    ] {
        let out = scriptorium(home.path(), repo.path())
            .args(&args)
            .arg(&supergraph)
            .output()
            .unwrap();
        assert!(
            !out.status.success(),
            "{args:?} must not publish over a live server"
        );
        assert!(
            stderr(&out).contains("being published by another"),
            "{}",
            stderr(&out)
        );
    }
    assert_eq!(std::fs::read_to_string(&supergraph).unwrap(), published);

    server.kill().unwrap();
    server.wait().unwrap();
    // With the server gone, compose may publish again.
    scriptorium(home.path(), repo.path())
        .args(["compose", "--offline", "--out"])
        .arg(&supergraph)
        .assert()
        .success();
}

#[cfg(unix)]
#[test]
fn publishing_follows_symlinks_and_keeps_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let real = repo.path().join("real.graphql");
    std::fs::write(&real, "# old\n").unwrap();
    std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o640)).unwrap();
    let link = repo.path().join("link.graphql");
    std::os::unix::fs::symlink(&real, &link).unwrap();

    scriptorium(home.path(), repo.path())
        .args(["compose", "--offline", "--out"])
        .arg(&link)
        .assert()
        .success();

    assert!(
        std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink(),
        "the symlink stays a symlink"
    );
    let published = std::fs::read_to_string(&real).unwrap();
    assert!(
        published.contains("join__Graph"),
        "the target was replaced: {published}"
    );
    assert_eq!(
        std::fs::metadata(&real).unwrap().permissions().mode() & 0o777,
        0o640,
        "permissions kept"
    );
}

#[test]
fn compose_refuses_a_commit_serve_could_not_start() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    // `Person.papers` is `@inverse(of: "authors")`, so serving must index
    // every Paper; a malformed one stops `serve` at startup.
    std::fs::write(
        repo.path().join("ontology/research/data/Paper/bad.yaml"),
        "title: [unclosed\n",
    )
    .unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-qm", "malformed record"]);

    let out_path = repo.path().join("supergraph.graphql");
    std::fs::write(&out_path, "# previous\n").unwrap();
    let out = scriptorium(home.path(), repo.path())
        .args(["compose", "--offline", "--out"])
        .arg(&out_path)
        .output()
        .unwrap();
    assert!(!out.status.success(), "compose must refuse");
    // The check gate reports the malformed record before the serve preflight runs.
    assert!(stderr(&out).contains("check error(s)"), "{}", stderr(&out));
    assert_eq!(std::fs::read_to_string(&out_path).unwrap(), "# previous\n");

    // Publishing from the worktree is refused outright; previewing is not.
    let out = scriptorium(home.path(), repo.path())
        .args(["compose", "--worktree", "--out"])
        .arg(&out_path)
        .output()
        .unwrap();
    assert!(
        !out.status.success(),
        "compose --worktree --out must refuse"
    );
    assert!(
        stderr(&out).contains("publishes a commit"),
        "{}",
        stderr(&out)
    );
    assert_eq!(std::fs::read_to_string(&out_path).unwrap(), "# previous\n");
    scriptorium(home.path(), repo.path())
        .args(["compose", "--worktree"])
        .assert()
        .success();
}

#[test]
fn publishing_requires_a_clean_check() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    // Person has no inverse relation in core, so no index would read this.
    std::fs::write(
        repo.path().join("ontology/core/data/Person/vaswani.yaml"),
        "name: [unclosed\n",
    )
    .unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-qm", "malformed core record"]);

    let out_path = repo.path().join("supergraph.graphql");
    for args in [
        vec!["compose", "--offline", "--out"],
        vec!["serve", "--addr", "127.0.0.1:0", "--supergraph"],
    ] {
        let out = scriptorium(home.path(), repo.path())
            .args(&args)
            .arg(&out_path)
            .output()
            .unwrap();
        assert!(!out.status.success(), "{args:?} must refuse");
        assert!(stderr(&out).contains("check error(s)"), "{}", stderr(&out));
        assert!(!out_path.exists(), "nothing published");
    }
    // Previewing (no --out) is still allowed.
    scriptorium(home.path(), repo.path())
        .args(["compose"])
        .assert()
        .success();
}

#[test]
fn serve_refuses_the_worktree() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let out = scriptorium(home.path(), repo.path())
        .args(["serve", "--worktree", "--addr", "127.0.0.1:0"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(stderr(&out).contains("reads a commit"), "{}", stderr(&out));
}

/// A server on a loopback port that answers every `/<name>/graphql` with
/// `_service { sdl }` = `sdl`: a stand-in for a `serve` pinned to another
/// commit.
fn stale_server(sdl: &'static str) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let app = axum::Router::new().route(
                "/{name}/graphql",
                axum::routing::post(move || async move {
                    axum::Json(serde_json::json!({ "data": { "_service": { "sdl": sdl } } }))
                }),
            );
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });
    base
}

#[test]
fn compose_publishes_only_what_the_running_subgraphs_serve() {
    use std::io::BufRead as _;

    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let out_path = repo.path().join("supergraph.graphql");

    // A server still on another commit: refused, nothing written.
    let stale = stale_server("type Query { old: Int }");
    let out = scriptorium(home.path(), repo.path())
        .args(["compose", "--base-url", &stale, "--out"])
        .arg(&out_path)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("serve another commit or schema"),
        "{}",
        stderr(&out)
    );
    assert!(!out_path.exists());
    // Something that answers but not over working TLS: unknown, so refused.
    let broken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let broken_base = format!("https://{}", broken.local_addr().unwrap());
    std::thread::spawn(move || {
        for connection in broken.incoming() {
            drop(connection);
        }
    });
    let out = scriptorium(home.path(), repo.path())
        .args(["compose", "--base-url", &broken_base, "--out"])
        .arg(&out_path)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("could not ask"),
        "{}",
        stderr(&out)
    );
    assert!(!out_path.exists());
    // `--offline` publishes anyway.
    scriptorium(home.path(), repo.path())
        .args(["compose", "--offline", "--base-url", &stale, "--out"])
        .arg(&out_path)
        .assert()
        .success();
    std::fs::remove_file(&out_path).unwrap();

    // `serve` at this commit: published.
    let mut server = scriptorium(home.path(), repo.path())
        .args(["serve", "--addr", "127.0.0.1:0"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    // Keep draining the server's stdout: a closed pipe would kill it with
    // SIGPIPE on its next line.
    let stdout = std::io::BufReader::new(server.stdout.take().unwrap());
    let (found, base) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in stdout.lines().map_while(Result::ok) {
            let url = line.split_whitespace().nth(1).unwrap_or_default();
            if let Some(base) = url.strip_suffix("/core/graphql") {
                let _ = found.send(base.to_string());
            }
        }
    });
    let base = base
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("serve prints its URLs");
    let out = scriptorium(home.path(), repo.path())
        .args(["compose", "--base-url", &base, "--out"])
        .arg(&out_path)
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", stderr(&out));
    assert!(out_path.exists());

    // A commit that changes only records: same schema, other data.
    std::fs::write(
        repo.path().join("ontology/core/data/Person/vaswani.yaml"),
        "name: A. Vaswani\n",
    )
    .unwrap();
    git(repo.path(), &["commit", "-qam", "rename vaswani"]);
    let out = scriptorium(home.path(), repo.path())
        .args(["compose", "--base-url", &base, "--out"])
        .arg(&out_path)
        .output()
        .unwrap();
    server.kill().unwrap();
    server.wait().unwrap();
    assert!(
        !out.status.success(),
        "the running server is a commit behind"
    );
    assert!(
        stderr_flat(&out).contains("serve another commit"),
        "{}",
        stderr(&out)
    );
}

const PINNED_VENUES: &str = "extend schema @link(url: \"https://specs.apollo.dev/federation/v2.3\", import: [\"@key\"])\ntype Venue @key(fields: \"id\") { id: ID! name: String! }\ntype Query { venue(id: ID!): Venue }\n";

/// Commit a remote `venues` sub-ontology at `url`, pinned to [`PINNED_VENUES`].
fn pin_venues(repo: &Path, url: &str) {
    std::fs::create_dir_all(repo.join("ontology/venues")).unwrap();
    std::fs::write(
        repo.join("ontology/venues/subgraph.toml"),
        format!("[remote]\nurl = \"{url}\"\n"),
    )
    .unwrap();
    std::fs::write(repo.join("ontology/venues/schema.graphql"), PINNED_VENUES).unwrap();
    git(repo, &["add", "-A"]);
    git(repo, &["commit", "-qm", "pin venues"]);
}

#[test]
fn compose_checks_pinned_remote_schemas_against_the_remote() {
    const PINNED: &str = PINNED_VENUES;
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let pin = |url: &str| pin_venues(repo.path(), url);
    let out_path = repo.path().join("supergraph.graphql");
    let publish = || {
        scriptorium(home.path(), repo.path())
            .args(["compose", "--base-url", "http://127.0.0.1:9", "--out"])
            .arg(&out_path)
            .output()
            .unwrap()
    };

    // The remote now serves something else than the pin.
    let drifted = stale_server("type Query { venue(id: ID!): String }");
    pin(&format!("{drifted}/venues/graphql"));
    let out = publish();
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("no longer serve the schema pinned"),
        "{}",
        stderr(&out)
    );
    assert!(!out_path.exists());

    // Unreachable: unknown, so refused as well.
    pin("http://127.0.0.1:9/venues/graphql");
    let out = publish();
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("could not ask remote `venues`"),
        "{}",
        stderr(&out)
    );

    // The pin matches what the remote serves: published.
    let current = stale_server(PINNED);
    pin(&format!("{current}/venues/graphql"));
    let out = publish();
    assert!(out.status.success(), "{}", stderr(&out));
}

/// A subgraph stand-in whose every answer is 9 MiB: declared up front in
/// `Content-Length`, or streamed chunked without one.
fn oversized_server(chunked: bool) -> String {
    use std::io::{BufRead, BufReader, Read, Write};
    const SIZE: usize = 9 * 1024 * 1024;
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            std::thread::spawn(move || {
                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).unwrap_or(0) == 0 {
                        return;
                    }
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let _ = reader.read_exact(&mut vec![0; length]);
                let mut stream = stream;
                let chunk = vec![b' '; 64 * 1024];
                let head = if chunked {
                    "transfer-encoding: chunked".to_string()
                } else {
                    format!("content-length: {SIZE}")
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n{head}\r\n\r\n"
                );
                for _ in 0..SIZE / chunk.len() {
                    let sent = if chunked {
                        write!(stream, "{:x}\r\n", chunk.len())
                            .and_then(|()| stream.write_all(&chunk))
                            .and_then(|()| stream.write_all(b"\r\n"))
                    } else {
                        stream.write_all(&chunk)
                    };
                    if sent.is_err() {
                        return;
                    }
                }
                if chunked {
                    let _ = stream.write_all(b"0\r\n\r\n");
                }
            });
        }
    });
    base
}

#[test]
fn publishing_refuses_oversized_probe_answers() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let out_path = repo.path().join("supergraph.graphql");
    let publish = |base_url: &str| {
        scriptorium(home.path(), repo.path())
            .args(["compose", "--base-url", base_url, "--out"])
            .arg(&out_path)
            .output()
            .unwrap()
    };

    // A local subgraph streaming its answer, with no length to refuse up front.
    let out = publish(&oversized_server(true));
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("larger than 8 MiB"),
        "{}",
        stderr(&out)
    );
    // A remote declaring a length over the cap.
    pin_venues(
        repo.path(),
        &format!("{}/venues/graphql", oversized_server(false)),
    );
    let out = publish("http://127.0.0.1:9");
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("larger than 8 MiB"),
        "{}",
        stderr(&out)
    );
    assert!(!out_path.exists());
}

/// Answers `_service` for each sub-ontology in `sdls` the way `serve` at
/// `revision` would, but with HTTP `status`.
fn answering_server(
    status: u16,
    revision: String,
    sdls: std::collections::HashMap<String, String>,
) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let sdls = std::sync::Arc::new(sdls);
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async move {
            let app = axum::Router::new().route(
                "/{name}/graphql",
                axum::routing::post(
                    move |axum::extract::Path(name): axum::extract::Path<String>| {
                        let (sdls, revision) = (std::sync::Arc::clone(&sdls), revision.clone());
                        async move {
                            (
                                axum::http::StatusCode::from_u16(status).unwrap(),
                                [("scriptorium-ontology-revision", revision)],
                                axum::Json(serde_json::json!({
                                    "data": { "_service": { "sdl": sdls.get(&name) } }
                                })),
                            )
                        }
                    },
                ),
            );
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            axum::serve(listener, app).await.unwrap();
        });
    });
    base
}

#[test]
fn publishing_refuses_failing_local_subgraphs() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let head = git2::Repository::open(repo.path())
        .unwrap()
        .head()
        .unwrap()
        .peel_to_commit()
        .unwrap()
        .id()
        .to_string();
    let mut sdls = std::collections::HashMap::new();
    for entry in std::fs::read_dir(repo.path().join("ontology")).unwrap() {
        let dir = entry.unwrap().path();
        if let Ok(sdl) = std::fs::read_to_string(dir.join("schema.graphql")) {
            let name = dir.file_name().unwrap().to_string_lossy().into_owned();
            sdls.insert(name, sdl);
        }
    }
    let out_path = repo.path().join("supergraph.graphql");
    let publish = |base_url: &str| {
        scriptorium(home.path(), repo.path())
            .args(["compose", "--base-url", base_url, "--out"])
            .arg(&out_path)
            .output()
            .unwrap()
    };

    // The right commit and schema, but failing: refused.
    let out = publish(&answering_server(503, head.clone(), sdls.clone()));
    assert!(!out.status.success());
    assert!(stderr_flat(&out).contains("HTTP 503"), "{}", stderr(&out));
    assert!(!out_path.exists());
    // The same answers, succeeding: published.
    let out = publish(&answering_server(200, head, sdls));
    assert!(out.status.success(), "{}", stderr(&out));
}

/// Run `cmd`, which should exit on its own: a server that starts instead is
/// killed, failing the test rather than hanging it.
fn exits_within(mut cmd: Command, secs: u64) -> std::process::Output {
    let mut child = cmd
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    for _ in 0..secs * 10 {
        if child.try_wait().unwrap().is_some() {
            return child.wait_with_output().unwrap();
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
    panic!("still running after {secs}s: {cmd:?}");
}

#[test]
fn serve_checks_pinned_remote_schemas_before_publishing() {
    const PREVIOUS: &str = "# the gateway's current supergraph\n";
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let supergraph = repo.path().join("supergraph.graphql");
    std::fs::write(&supergraph, PREVIOUS).unwrap();
    let serve = |extra: &[&str]| {
        let mut cmd = scriptorium(home.path(), repo.path());
        cmd.args(["serve", "--addr", "127.0.0.1:0", "--supergraph"])
            .arg(&supergraph)
            .args(extra);
        cmd
    };

    // Drifted and unreachable remotes: refused before binding, the
    // gateway's file untouched.
    let drifted = stale_server("type Query { venue(id: ID!): String }");
    for (url, expected) in [
        (
            format!("{drifted}/venues/graphql"),
            "no longer serve the schema pinned",
        ),
        (
            "http://127.0.0.1:9/venues/graphql".to_string(),
            "could not ask remote `venues`",
        ),
    ] {
        pin_venues(repo.path(), &url);
        let out = exits_within(serve(&[]), 30);
        assert!(!out.status.success(), "serve must refuse");
        assert!(stderr_flat(&out).contains(expected), "{}", stderr(&out));
        assert_eq!(std::fs::read_to_string(&supergraph).unwrap(), PREVIOUS);
    }

    // A matching pin publishes, and so does --offline without asking.
    let current = stale_server(PINNED_VENUES);
    for (url, extra) in [
        (format!("{current}/venues/graphql"), &[][..]),
        (
            "http://127.0.0.1:9/venues/graphql".to_string(),
            &["--offline"][..],
        ),
    ] {
        std::fs::write(&supergraph, PREVIOUS).unwrap();
        pin_venues(repo.path(), &url);
        let mut server = serve(extra)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let mut published = false;
        for _ in 0..100 {
            if std::fs::read_to_string(&supergraph).is_ok_and(|s| s.contains("venue")) {
                published = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let _ = server.kill();
        let _ = server.wait();
        assert!(published, "serve {extra:?} published at {url}");
    }
}

#[test]
fn publishing_needs_a_routable_address() {
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let supergraph = repo.path().join("supergraph.graphql");
    let mut serve = scriptorium(home.path(), repo.path());
    serve
        .args(["serve", "--addr", "0.0.0.0:0", "--supergraph"])
        .arg(&supergraph);
    let out = exits_within(serve, 30);
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("wildcard address"),
        "{}",
        stderr(&out)
    );
    let out = scriptorium(home.path(), repo.path())
        .args([
            "compose",
            "--offline",
            "--base-url",
            "http://0.0.0.0:4200",
            "--out",
        ])
        .arg(&supergraph)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("wildcard address"),
        "{}",
        stderr(&out)
    );
    // Not a URL a gateway can use, even offline: nothing is asked, but
    // nothing unroutable is written either.
    let out = scriptorium(home.path(), repo.path())
        .args([
            "compose",
            "--offline",
            "--base-url",
            "localhost:4200",
            "--out",
        ])
        .arg(&supergraph)
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("is not an http:// or https:// URL"),
        "{}",
        stderr(&out)
    );
    // …nor a configured wildcard, even offline.
    std::fs::write(
        repo.path().join("ontology/ontology.toml"),
        "[serve]\nbase_url = \"http://[::]:4200\"\n",
    )
    .unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-qm", "wildcard base url"]);
    let mut serve = scriptorium(home.path(), repo.path());
    serve
        .args([
            "serve",
            "--addr",
            "127.0.0.1:0",
            "--offline",
            "--supergraph",
        ])
        .arg(&supergraph);
    let out = exits_within(serve, 30);
    assert!(!out.status.success());
    assert!(
        stderr_flat(&out).contains("wildcard address"),
        "{}",
        stderr(&out)
    );
    assert!(!supergraph.exists());
}

#[test]
fn serve_publishes_only_a_base_url_that_reaches_it() {
    const PREVIOUS: &str = "# the gateway's current supergraph\n";
    let home = tempfile::tempdir().unwrap();
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    scriptorium(home.path(), repo.path())
        .args(["init"])
        .assert()
        .success();
    let supergraph = repo.path().join("supergraph.graphql");
    std::fs::write(&supergraph, PREVIOUS).unwrap();
    let set_base_url = |url: &str| {
        std::fs::write(
            repo.path().join("ontology/ontology.toml"),
            format!("[serve]\nbase_url = \"{url}\"\n"),
        )
        .unwrap();
        git(repo.path(), &["add", "-A"]);
        git(repo.path(), &["commit", "-qm", "base url"]);
    };
    let serve = |addr: &str| {
        let mut cmd = scriptorium(home.path(), repo.path());
        cmd.args(["serve", "--addr", addr, "--supergraph"])
            .arg(&supergraph);
        cmd
    };

    // An older server answers at the configured base URL: the gateway
    // would query it, so nothing is published.
    set_base_url(&stale_server("type Query { old: Int }"));
    let out = exits_within(serve("127.0.0.1:0"), 30);
    assert!(!out.status.success(), "serve must refuse");
    assert!(
        stderr_flat(&out).contains("`serve.base_url` leads to another server"),
        "{}",
        stderr(&out)
    );
    assert_eq!(std::fs::read_to_string(&supergraph).unwrap(), PREVIOUS);

    // The base URL reaches this server: published.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let url = format!("http://127.0.0.1:{port}");
    set_base_url(&url);
    let mut server = serve(&format!("127.0.0.1:{port}"))
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut published = false;
    for _ in 0..100 {
        if std::fs::read_to_string(&supergraph).is_ok_and(|s| s.contains(&url)) {
            published = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    let _ = server.kill();
    let _ = server.wait();
    assert!(published, "serve published with base_url {url}");
}
