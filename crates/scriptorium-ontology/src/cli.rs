//! The `scriptorium-ontology` command line.
//!
//! Thin shims over the library: every subcommand reads one snapshot (a git
//! revision, or the working tree with `--worktree`) and reports what it
//! found. See `docs/ONTOLOGY.md`.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use crate::check::{check, Severity};
use crate::compose::compose;
use crate::layout::ONTOLOGY_DIR;
use crate::model::SubOntologyModel;
use crate::scaffold::scaffold_example;
use crate::serve::build_subgraphs;
use crate::snapshot::{
    commit_paths, discover_repo_root, has_staged_changes, has_uncommitted_changes,
};
use crate::{Registry, Snapshot, SnapshotSpec, SubOntologySource};
use clap::{Args, Subcommand};
use miette::{miette, IntoDiagnostic, Result};

#[derive(Debug, Args)]
pub struct OntologyCommand {
    #[command(subcommand)]
    pub action: OntologyAction,
}

#[derive(Debug, Clone, Args)]
pub struct SourceArgs {
    /// Git revision to read: branch, tag, sha, `HEAD~1`, …
    #[arg(long, default_value = "HEAD", conflicts_with = "worktree")]
    pub rev: String,
    /// Read uncommitted files from the working tree instead of a revision.
    #[arg(long)]
    pub worktree: bool,
    /// Ontology directory, relative to the repository root.
    #[arg(long, default_value = ONTOLOGY_DIR)]
    pub dir: String,
}

impl SourceArgs {
    /// `--dir` in canonical form (`./ontology/` → `ontology`).
    fn dir(&self) -> String {
        crate::layout::normalize_dir(&self.dir)
    }

    fn spec(&self) -> SnapshotSpec {
        if self.worktree {
            SnapshotSpec::Worktree
        } else {
            SnapshotSpec::Rev(self.rev.clone())
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum OntologyAction {
    /// Scaffold the example ontology (core, research, software, notes) and commit it.
    Init {
        /// Ontology directory, relative to the repository root.
        #[arg(long, default_value = ONTOLOGY_DIR)]
        dir: String,
        /// Write the files without committing them.
        #[arg(long)]
        no_commit: bool,
    },
    /// List sub-ontologies: storage, owners, entity types.
    List {
        #[command(flatten)]
        source: SourceArgs,
        /// Machine-readable output.
        #[arg(long)]
        json: bool,
    },
    /// Compose every sub-ontology into a federated supergraph.
    Compose {
        #[command(flatten)]
        source: SourceArgs,
        /// Base URL the gateway uses to reach `scriptorium-ontology serve`
        /// (default: from ontology.toml, else `http://127.0.0.1:4200`).
        #[arg(long)]
        base_url: Option<String>,
        /// Write to this file instead of stdout.
        #[arg(long, short)]
        out: Option<PathBuf>,
        /// Emit the client-facing API schema instead of the supergraph.
        #[arg(long)]
        api: bool,
        /// Publish without first asking the local subgraphs at the base URL
        /// what they serve.
        #[arg(long, requires = "out")]
        offline: bool,
    },
    /// Validate registry, composition, and git-stored records.
    Check {
        #[command(flatten)]
        source: SourceArgs,
        /// Machine-readable output.
        #[arg(long)]
        json: bool,
    },
    /// Pin a remote Federation subgraph as a sub-ontology: fetch its schema,
    /// check it composes with the ontology at HEAD, write it, and commit.
    AddRemote {
        /// Sub-ontology name, the directory under the ontology directory.
        name: String,
        /// The subgraph's GraphQL endpoint; it must answer `_service { sdl }`.
        url: String,
        /// Send header HEADER with the value of environment variable ENV,
        /// e.g. `--header Authorization=PEOPLE_TOKEN` (repeatable). The
        /// binding (variable names, and this URL) is kept in the git
        /// directory, never in a commit.
        #[arg(long = "header", value_name = "HEADER=ENV")]
        headers: Vec<String>,
        /// Ontology directory, relative to the repository root.
        #[arg(long, default_value = ONTOLOGY_DIR)]
        dir: String,
        /// Write the files without committing them.
        #[arg(long)]
        no_commit: bool,
    },
    /// Refresh pinned remote schemas from their subgraphs, and commit the
    /// ones that changed if everything still composes.
    Pull {
        /// Remote sub-ontologies to refresh (default: every remote).
        names: Vec<String>,
        /// Bind headers to the one named remote at its current URL, and send
        /// them (`--header Authorization=PEOPLE_TOKEN`). Otherwise a remote
        /// gets the headers bound to it by `add-remote` or an earlier pull,
        /// and only while its URL is the one they were bound to.
        #[arg(long = "header", value_name = "HEADER=ENV")]
        headers: Vec<String>,
        /// Ontology directory, relative to the repository root.
        #[arg(long, default_value = ONTOLOGY_DIR)]
        dir: String,
        /// Show what would change, with a diff, without writing anything.
        #[arg(long)]
        dry_run: bool,
        /// Write the files without committing them.
        #[arg(long)]
        no_commit: bool,
    },
    /// Serve local sub-ontologies as Federation v2 subgraphs over HTTP.
    Serve {
        #[command(flatten)]
        source: SourceArgs,
        /// Address to bind (default: from ontology.toml, else 127.0.0.1:4200).
        #[arg(long)]
        addr: Option<String>,
        /// Also write a supergraph pointing at this server, for a gateway.
        #[arg(long)]
        supergraph: Option<PathBuf>,
        /// Publish the supergraph without first asking remote sub-ontologies,
        /// and this server at its base URL, what they serve.
        #[arg(long, requires = "supergraph")]
        offline: bool,
    },
}

/// Pick the repository root: `-C` if given, else the git repository that
/// contains the current directory.
pub fn repo_root(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    discover_repo_root(Path::new(".")).ok_or_else(|| {
        miette!("not inside a git repository: run `git init` first, or pass -C <repo>")
    })
}

pub async fn run(cmd: OntologyCommand, root: &Path) -> Result<ExitCode> {
    match cmd.action {
        OntologyAction::Init { dir, no_commit } => init(root, &dir, no_commit),
        OntologyAction::List { source, json } => list(root, &source, json),
        OntologyAction::Compose {
            source,
            base_url,
            out,
            api,
            offline,
        } => compose_cmd(root, &source, base_url, out, api, offline).await,
        OntologyAction::Check { source, json } => check_cmd(root, &source, json),
        OntologyAction::AddRemote {
            name,
            url,
            headers,
            dir,
            no_commit,
        } => add_remote(root, &name, &url, &headers, &dir, no_commit).await,
        OntologyAction::Pull {
            names,
            headers,
            dir,
            dry_run,
            no_commit,
        } => pull(root, &names, &headers, &dir, dry_run, no_commit).await,
        OntologyAction::Serve {
            source,
            addr,
            supergraph,
            offline,
        } => serve(root, &source, addr, supergraph, offline).await,
    }
}

fn init(root: &Path, dir: &str, no_commit: bool) -> Result<ExitCode> {
    let written = scaffold_example(root, dir).into_diagnostic()?;
    println!(
        "scaffolded {} files under {}",
        written.len(),
        root.join(dir).display()
    );
    if no_commit {
        println!("not committed: commit {dir}/ before serving from HEAD (or pass --worktree)");
        return Ok(ExitCode::SUCCESS);
    }
    if has_staged_changes(root).into_diagnostic()? {
        println!("other changes are staged, so {dir}/ was not committed: commit it yourself");
        return Ok(ExitCode::SUCCESS);
    }
    let sha =
        commit_paths(root, &written, "ontology: scaffold example ontology").into_diagnostic()?;
    println!("committed {}", &sha[..7.min(sha.len())]);
    Ok(ExitCode::SUCCESS)
}

/// Most bytes publishing reads from a subgraph it asks what it serves; a
/// schema is far smaller.
const MAX_PROBE_BYTES: usize = 8 * 1024 * 1024;

/// A probe's answer as JSON (`None` when it is not JSON), refusing one
/// larger than [`MAX_PROBE_BYTES`] before buffering it.
async fn probe_json(
    mut response: reqwest::Response,
) -> std::result::Result<Option<serde_json::Value>, String> {
    let too_large = || format!("its answer is larger than {} MiB", MAX_PROBE_BYTES >> 20);
    if response
        .content_length()
        .is_some_and(|n| n > MAX_PROBE_BYTES as u64)
    {
        return Err(too_large());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| error_chain(&e.without_url()))?
    {
        if body.len() + chunk.len() > MAX_PROBE_BYTES {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&body).ok())
}

/// Who must answer at the base URL a supergraph routes local subgraphs to.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Answering {
    /// A `serve` run on its own, perhaps started after publishing
    /// (`compose --out`): nothing listening yet is only a note.
    Separately,
    /// This process (`serve --supergraph`), already listening: nothing
    /// answering, or another server answering, means the base URL leads
    /// somewhere else.
    ThisServer,
}

/// Before publishing a supergraph, ask each local subgraph at `base_url`
/// which commit it serves ([`crate::serve::REVISION_HEADER`]) and its
/// schema (`_service { sdl }`, the committed one verbatim): a server still
/// on another commit would get requests for fields it cannot resolve, or
/// answer from other records. Any failure to ask (TLS, a timeout, a
/// non-GraphQL answer) refuses, since then nothing is known about what runs
/// there, except that nothing listening is only a note while
/// [`Answering::Separately`].
async fn require_live_subgraphs_match(
    registry: &Registry,
    base_url: &str,
    answering: Answering,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .into_diagnostic()?;
    let wanted = registry.commit.as_deref().unwrap_or(&registry.source);
    let mut stale = Vec::new();
    for sub in registry.local() {
        let url = sub.subgraph_url(base_url);
        let cannot_ask = |why: String| {
            match answering {
            Answering::Separately => miette!(
                "could not ask {url} what it serves ({why}); pass --offline to publish without asking"
            ),
            Answering::ThisServer => miette!(
                "could not reach this server at {url} ({why}); set `serve.base_url` in ontology.toml to an address that reaches it, or pass --offline"
            ),
        }
        };
        let answer = client
            .post(&url)
            .json(&serde_json::json!({ "query": "{ _service { sdl } }" }))
            .send()
            .await;
        let response = match answer {
            Ok(response) => response,
            Err(e) if answering == Answering::Separately && connection_refused(&e) => {
                eprintln!(
                    "note: nothing answers at {url} yet; start `scriptorium-ontology serve` at {} before a gateway uses this supergraph",
                    registry.source
                );
                continue;
            }
            Err(e) => return Err(cannot_ask(error_chain(&e.without_url()))),
        };
        // A failing answer is no answer, whatever it carries: the gateway
        // would route to an endpoint that fails.
        let status = response.status();
        if !status.is_success() {
            return Err(cannot_ask(format!("HTTP {status}")));
        }
        let revision = response
            .headers()
            .get(crate::serve::REVISION_HEADER)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let body = probe_json(response).await.map_err(cannot_ask)?;
        let served = body.and_then(|body| {
            body.pointer("/data/_service/sdl")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        });
        if revision.as_deref() != Some(wanted) || served.as_deref() != Some(sub.sdl.as_str()) {
            stale.push(url);
        }
    }
    if stale.is_empty() {
        return Ok(());
    }
    let stale = stale.join(", ");
    Err(match answering {
        Answering::Separately => miette!(
            "{stale} serve another commit or schema than {} (an older `serve`?); restart `scriptorium-ontology serve` at this commit first, publish with `serve --supergraph`, or pass --offline",
            registry.source
        ),
        Answering::ThisServer => miette!(
            "{stale} answer with another commit or schema than this server ({}): `serve.base_url` leads to another server; point it at this one, or pass --offline",
            registry.source
        ),
    })
}

/// Before publishing, ask each remote sub-ontology what it serves: a pinned
/// schema the remote no longer matches would route queries to fields it
/// lacks. Any failure refuses, since the remote's schema is then unknown.
/// The request is the one `pull` makes, with the headers bound to the
/// remote in this clone (only while its URL is the bound one).
async fn require_remote_pins_match(root: &Path, registry: &Registry) -> Result<()> {
    let credentials = crate::remote::Credentials::load(root).into_diagnostic()?;
    let mut drifted = Vec::new();
    for sub in &registry.sub_ontologies {
        let SubOntologySource::Remote(remote) = &sub.source else {
            continue;
        };
        let unknown = |why: String| {
            miette!(
                "could not ask remote `{}` what it serves ({why}); pass --offline to publish without asking",
                sub.name
            )
        };
        let headers = credentials
            .headers_for(&sub.dir, &sub.name, &remote.url)
            .map_err(&unknown)?;
        let served = crate::remote::fetch_sdl(&remote.url, &headers)
            .await
            .map_err(|e| unknown(e.to_string()))?;
        if served != sub.sdl {
            drifted.push(format!("`{}`", sub.name));
        }
    }
    if drifted.is_empty() {
        return Ok(());
    }
    Err(miette!(
        "remote {} no longer serve the schema pinned at {}; `scriptorium-ontology pull` them, or pass --offline",
        drifted.join(", "),
        registry.source
    ))
}

/// Every URL a supergraph routes to must be one a gateway can connect to.
/// Checked even `--offline`: nothing is asked then, but nothing unroutable
/// is written either.
fn require_routable(registry: &Registry, base_url: &str) -> Result<()> {
    for sub in &registry.sub_ontologies {
        let url = sub.subgraph_url(base_url);
        let Some(why) = unroutable(&url) else {
            continue;
        };
        let fix = match sub.source {
            SubOntologySource::Local(_) => {
                "pass --base-url, or set `serve.base_url` in ontology.toml, to the address a gateway reaches the server at"
            }
            SubOntologySource::Remote(_) => "fix the `url` in its subgraph.toml",
        };
        return Err(miette!(
            "the supergraph would route `{}` to {url}, which {why}; {fix}",
            sub.name
        ));
    }
    Ok(())
}

/// Why a gateway could not connect to `url`, if it could not: not an
/// absolute http(s) URL, no host, or the wildcard address (`0.0.0.0`,
/// `::`) a server binds to listen everywhere, which names no host.
fn unroutable(url: &str) -> Option<&'static str> {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return Some("is not an absolute URL");
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return Some("is not an http:// or https:// URL");
    }
    let Some(host) = parsed.host_str() else {
        return Some("has no host");
    };
    let wildcard = host
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_unspecified());
    wildcard.then_some("is a wildcard address no gateway can reach")
}

/// Whether nothing listens at the address (as opposed to a TLS failure, a
/// timeout, or anything else that leaves the server's state unknown).
fn connection_refused(e: &reqwest::Error) -> bool {
    let mut source: Option<&dyn std::error::Error> = Some(e);
    while let Some(error) = source {
        if error
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::ConnectionRefused)
        {
            return true;
        }
        source = error.source();
    }
    false
}

/// `e` and its causes, joined.
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut source = e.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

/// Held for the whole of `add-remote` and `pull`: one process at a time
/// changes a clone's pins and bindings. `None` outside a repository, where
/// there is nothing to share.
fn lock_pins(root: &Path) -> Result<Option<std::fs::File>> {
    let repo = match git2::Repository::open(root) {
        Ok(repo) => repo,
        Err(e) if e.code() == git2::ErrorCode::NotFound => return Ok(None),
        Err(e) => return Err(miette!("{e}")),
    };
    let dir = repo.commondir().join("scriptorium-ontology");
    std::fs::create_dir_all(&dir).into_diagnostic()?;
    let path = dir.join("pins.lock");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .into_diagnostic()?;
    match lock.try_lock() {
        Ok(()) => Ok(Some(lock)),
        Err(std::fs::TryLockError::WouldBlock) => Err(miette!(
            "another `add-remote` or `pull` is changing this repository's pins; wait for it to finish"
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(miette!("locking {}: {e}", path.display())),
    }
}

/// The ontology at HEAD, which `add-remote` and `pull` extend: what they
/// commit is HEAD plus the pinned schemas. A repository without commits has
/// an empty ontology.
fn head_registry(root: &Path, dir: &str) -> Result<Registry> {
    let source = SourceArgs {
        rev: "HEAD".into(),
        worktree: false,
        dir: dir.to_string(),
    };
    let has_head = git2::Repository::open(root)
        .ok()
        .is_some_and(|repo| repo.head().is_ok());
    if !has_head {
        return Ok(Registry {
            ontology_dir: source.dir(),
            manifest: crate::manifest::OntologyManifest::default(),
            sub_ontologies: Vec::new(),
            source: "(no commits)".into(),
            commit: None,
        });
    }
    let snapshot = source.spec().open(root).into_diagnostic()?;
    load(snapshot.as_ref(), &source, Issues::Fail)
}

/// `registry` with `pins` added, or replacing the sub-ontologies of the
/// same name.
fn with_pins(registry: &Registry, pins: &[crate::SubOntology]) -> Registry {
    let mut out = registry.clone();
    for pin in pins {
        out.sub_ontologies.retain(|s| s.name != pin.name);
        out.sub_ontologies.push(pin.clone());
    }
    out.sub_ontologies.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Refuse pins that do not compose: printing why, and writing nothing.
fn require_composes(registry: &Registry, pins: &[crate::SubOntology], what: &str) -> Result<()> {
    let composition = compose(
        &with_pins(registry, pins),
        &registry.manifest.serve.base_url(),
    );
    for warning in &composition.warnings {
        eprintln!("warning: {warning}");
    }
    if composition.is_ok() {
        return Ok(());
    }
    for error in &composition.errors {
        eprintln!("error: {error}");
    }
    Err(miette!(
        "{what} does not compose with the ontology at {}; nothing written",
        registry.source
    ))
}

/// Commit `paths` unless told not to, or unless unrelated changes are
/// staged (the commit would include them).
fn commit_pinned(
    root: &Path,
    paths: &[PathBuf],
    message: &str,
    no_commit: bool,
    parent: Option<&str>,
) -> Result<()> {
    if no_commit {
        println!("not committed (--no-commit)");
        return Ok(());
    }
    if has_staged_changes(root).into_diagnostic()? {
        println!("other changes are staged, so nothing was committed: commit it yourself");
        return Ok(());
    }
    // Onto the commit the pins were checked against, or not at all.
    let sha = crate::snapshot::commit_paths_onto(root, paths, message, parent).into_diagnostic()?;
    println!("committed {}", &sha[..7.min(sha.len())]);
    Ok(())
}

/// `--header HEADER=ENV` arguments as header → variable name.
fn parse_headers(args: &[String]) -> Result<std::collections::BTreeMap<String, String>> {
    let mut headers = std::collections::BTreeMap::new();
    let mut seen = std::collections::BTreeSet::new();
    for arg in args {
        let (header, var) = crate::remote::parse_header_arg(arg).map_err(|e| miette!("{e}"))?;
        // Header names ignore case: `Authorization` and `authorization` are
        // one header, and only one of the two values would be sent.
        if !seen.insert(header.to_ascii_lowercase()) {
            return Err(miette!("header `{header}` is given twice"));
        }
        headers.insert(header, var);
    }
    Ok(headers)
}

/// Bind `headers` to remote `name` at `url` in this clone's credentials.
/// [`bind_headers`], returning the bindings as they were, to put back if
/// what the binding is for fails.
fn rebind(
    root: &Path,
    sub: &PinnedRemote<'_>,
    headers: Headers,
) -> Result<crate::remote::Credentials> {
    let previous = crate::remote::Credentials::load(root).into_diagnostic()?;
    bind_headers(root, sub, headers)?;
    Ok(previous)
}

/// The remote sub-ontology a binding is for: its name, directory, and URL.
struct PinnedRemote<'a> {
    name: &'a str,
    dir: &'a str,
    url: &'a str,
}

type Headers = std::collections::BTreeMap<String, String>;

fn bind_headers(root: &Path, sub: &PinnedRemote<'_>, headers: Headers) -> Result<()> {
    let PinnedRemote { name, dir, url } = *sub;
    let mut credentials = crate::remote::Credentials::load(root).into_diagnostic()?;
    credentials.bind(dir, url, headers);
    credentials.save(root).into_diagnostic()?;
    println!(
        "headers for `{name}` bound to {} in {}",
        crate::remote::display_url(url),
        crate::remote::Credentials::path(root)
            .into_diagnostic()?
            .display()
    );
    Ok(())
}

async fn add_remote(
    root: &Path,
    name: &str,
    url: &str,
    headers: &[String],
    dir: &str,
    no_commit: bool,
) -> Result<ExitCode> {
    crate::layout::validate_sub_ontology_name(name).map_err(|e| miette!("{e}"))?;
    crate::remote::validate_url(url).map_err(|e| miette!("{e}"))?;
    let headers = parse_headers(headers)?;
    let dir = crate::layout::normalize_dir(dir);
    let sub_dir = crate::layout::join(&dir, name);
    crate::scaffold::confine(root, &sub_dir).into_diagnostic()?;
    let _pins = lock_pins(root)?;
    let registry = head_registry(root, &dir)?;
    if registry.get(name).is_some() || root.join(&sub_dir).exists() {
        return Err(miette!(
            "{sub_dir}/ already exists; choose another name, or refresh a pinned remote with `scriptorium-ontology pull {name}`"
        ));
    }
    let sdl = crate::remote::fetch_sdl(url, &headers)
        .await
        .into_diagnostic()?;
    let remote = crate::manifest::RemoteConfig {
        url: url.to_string(),
    };
    let pin = crate::SubOntology {
        name: name.to_string(),
        dir: sub_dir.clone(),
        description: None,
        owners: Vec::new(),
        sdl,
        source: SubOntologySource::Remote(remote.clone()),
    };
    require_composes(&registry, std::slice::from_ref(&pin), &format!("`{name}`"))?;

    let manifest = remote_manifest(name, remote)?;
    let manifest_path = PathBuf::from(crate::layout::join(
        &sub_dir,
        crate::layout::SUBGRAPH_MANIFEST,
    ));
    let schema_path = PathBuf::from(pin.schema_path());
    // The remote may have been slow: nothing may have moved HEAD or claimed
    // the directory meanwhile, and nothing already there is written over.
    if head_commit(root) != registry.commit {
        return Err(miette!(
            "HEAD moved while `{name}` was fetched; nothing written, add it again"
        ));
    }
    let created = create_pin(
        root,
        &sub_dir,
        &[
            (manifest_path, manifest.as_bytes()),
            (schema_path, pin.sdl.as_bytes()),
        ],
    )
    .map_err(|e| {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            miette!("{sub_dir}/ appeared while `{name}` was fetched; nothing written")
        } else {
            miette!("writing {sub_dir}/: {e}; nothing written")
        }
    })?;
    let rollback = |created: &[PathBuf]| remove_pin(root, &sub_dir, created);
    println!(
        "pinned `{name}` from {} → {sub_dir}/",
        crate::remote::display_url(url)
    );
    // Bound before the commit, so no pin lands without the credential it
    // needs; the bindings as they were come back if the commit fails.
    let previous = if headers.is_empty() {
        None
    } else {
        let pinned = PinnedRemote {
            name,
            dir: &sub_dir,
            url,
        };
        match rebind(root, &pinned, headers) {
            Ok(previous) => Some(previous),
            Err(e) => {
                rollback(&created);
                return Err(miette!("{e}; {sub_dir}/ is removed again"));
            }
        }
    };
    if let Err(e) = commit_pinned(
        root,
        &created,
        &format!("ontology: pin remote sub-ontology {name}"),
        no_commit,
        registry.commit.as_deref(),
    ) {
        let _ = crate::snapshot::unstage_paths(root, &created);
        rollback(&created);
        if let Some(previous) = previous {
            let _ = previous.save(root);
        }
        return Err(miette!("{e}; {sub_dir}/ is removed again"));
    }
    Ok(ExitCode::SUCCESS)
}

/// The `subgraph.toml` of a remote sub-ontology pinned from `remote`.
fn remote_manifest(name: &str, remote: crate::manifest::RemoteConfig) -> Result<String> {
    let manifest = crate::manifest::SubgraphManifest {
        name: None,
        description: None,
        owners: Vec::new(),
        storage: None,
        remote: Some(remote),
    };
    Ok(format!(
        "# A remote sub-ontology: schema.graphql is a pinned copy of what the subgraph\n# serves. Refresh it with `scriptorium-ontology pull {name}`.\n{}",
        toml::to_string(&manifest).into_diagnostic()?
    ))
}

/// Create the new directory `sub_dir` holding `files`, refusing a directory
/// or file already there; on failure nothing it created is left.
fn create_pin(
    root: &Path,
    sub_dir: &str,
    files: &[(PathBuf, &[u8])],
) -> std::io::Result<Vec<PathBuf>> {
    let dir = root.join(sub_dir);
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::create_dir(&dir)?;
    let mut created = Vec::new();
    for (path, contents) in files {
        if let Err(e) = write_new(&root.join(path), contents) {
            remove_pin(root, sub_dir, &created);
            return Err(e);
        }
        created.push(path.clone());
    }
    Ok(created)
}

/// Remove what [`create_pin`] created: the files, then the directory if it
/// is empty.
fn remove_pin(root: &Path, sub_dir: &str, created: &[PathBuf]) {
    for path in created {
        let _ = std::fs::remove_file(root.join(path));
    }
    let _ = std::fs::remove_dir(root.join(sub_dir));
}

/// Create `path` holding `contents`, refusing whatever is already there: a
/// file, or a symlink, which an exclusive create does not follow.
fn write_new(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    use std::io::Write as _;
    write_new_with(path, |file| file.write_all(contents))
}

/// [`write_new`] with `write` filling the file; a file it cannot finish is
/// removed, so nothing half written stays behind.
fn write_new_with(
    path: &Path,
    write: impl FnOnce(&mut std::fs::File) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    let written = write(&mut file);
    if written.is_err() {
        drop(file);
        let _ = std::fs::remove_file(path);
    }
    written
}

/// The commit HEAD points at, if there is one.
fn head_commit(root: &Path) -> Option<String> {
    let repo = git2::Repository::open(root).ok()?;
    let id = repo.head().ok()?.peel_to_commit().ok()?.id();
    Some(id.to_string())
}

/// The remote sub-ontologies `pull` refreshes: `names`, or every remote.
fn pull_targets<'a>(
    registry: &'a Registry,
    names: &[String],
) -> Result<Vec<(&'a crate::SubOntology, &'a crate::manifest::RemoteConfig)>> {
    let mut targets = Vec::new();
    for sub in &registry.sub_ontologies {
        if let SubOntologySource::Remote(remote) = &sub.source {
            if names.is_empty() || names.contains(&sub.name) {
                targets.push((sub, remote));
            }
        }
    }
    for name in names {
        if !targets.iter().any(|(sub, _)| &sub.name == name) {
            return Err(miette!(
                "`{name}` is not a remote sub-ontology at {}",
                registry.source
            ));
        }
    }
    Ok(targets)
}

async fn pull(
    root: &Path,
    names: &[String],
    headers: &[String],
    dir: &str,
    dry_run: bool,
    no_commit: bool,
) -> Result<ExitCode> {
    let explicit = parse_headers(headers)?;
    if !explicit.is_empty() && names.len() != 1 {
        return Err(miette!(
            "--header binds credentials to one remote: name exactly one (`pull <name> --header …`)"
        ));
    }
    let _pins = if dry_run { None } else { lock_pins(root)? };
    let registry = head_registry(root, dir)?;
    let targets = pull_targets(&registry, names)?;
    if targets.is_empty() {
        println!("no remote sub-ontologies at {}", registry.source);
        return Ok(ExitCode::SUCCESS);
    }
    if !dry_run {
        for (sub, _) in &targets {
            crate::scaffold::confine(root, &sub.dir).into_diagnostic()?;
            if has_uncommitted_changes(root, &sub.dir).into_diagnostic()? {
                return Err(miette!(
                    "{}/ has uncommitted changes; commit or discard them before `pull` replaces its schema",
                    sub.dir
                ));
            }
        }
    }

    let credentials = crate::remote::Credentials::load(root).into_diagnostic()?;
    let mut pins = Vec::new();
    let mut failed = 0;
    // `--header` binds one remote.
    let mut bind_to = None;
    for (sub, remote) in targets {
        let headers = if explicit.is_empty() {
            match credentials.headers_for(&sub.dir, &sub.name, &remote.url) {
                Ok(headers) => headers,
                Err(e) => {
                    eprintln!("error: {}: {e}", sub.name);
                    failed += 1;
                    continue;
                }
            }
        } else {
            explicit.clone()
        };
        match crate::remote::fetch_sdl(&remote.url, &headers).await {
            Ok(sdl) if sdl == sub.sdl => println!("  {}: unchanged", sub.name),
            Ok(sdl) => {
                let (added, removed) = crate::remote::line_changes(&sub.sdl, &sdl);
                println!("  {}: +{added} -{removed} lines", sub.name);
                if dry_run {
                    print!(
                        "{}",
                        crate::remote::unified_diff(&sub.sdl, &sdl, &sub.schema_path())
                    );
                }
                pins.push(crate::SubOntology { sdl, ..sub.clone() });
            }
            Err(e) => {
                eprintln!("error: {}: {e}", sub.name);
                failed += 1;
                continue;
            }
        }
        if !explicit.is_empty() && !dry_run {
            bind_to = Some((sub.name.clone(), sub.dir.clone(), remote.url.clone()));
        }
    }
    if failed > 0 {
        return Err(miette!(
            "{failed} remote schema(s) could not be fetched; nothing written"
        ));
    }
    if !pins.is_empty() {
        require_composes(&registry, &pins, "the refreshed schema set")?;
        if dry_run {
            println!("dry run: nothing written");
            return Ok(ExitCode::SUCCESS);
        }
    }
    // A new binding is saved before the pins it opens are committed, and
    // put back as it was if they are not.
    let previous = match &bind_to {
        Some((name, dir, url)) => Some(rebind(root, &PinnedRemote { name, dir, url }, explicit)?),
        None => None,
    };
    if pins.is_empty() {
        println!("remote schemas are up to date at {}", registry.source);
        return Ok(ExitCode::SUCCESS);
    }
    let names: Vec<&str> = pins.iter().map(|p| p.name.as_str()).collect();
    let installed = install_pins(
        root,
        &registry,
        &pins,
        &format!("ontology: pull remote schemas ({})", names.join(", ")),
        no_commit,
    );
    if let (Err(_), Some(previous)) = (&installed, previous) {
        let _ = previous.save(root);
    }
    installed?;
    Ok(ExitCode::SUCCESS)
}

/// A new schema written beside the pin it replaces: (repository path,
/// staged copy, the pinned schema at HEAD).
type Staged<'a> = (PathBuf, PathBuf, &'a str);

fn discard(staged: &[Staged<'_>]) {
    for (_, temp, _) in staged {
        let _ = std::fs::remove_file(temp);
    }
}

/// Write each pin's new schema beside its target, created exclusively and
/// with the target's mode. Nothing is left behind on failure.
fn stage_pins<'a>(
    root: &Path,
    registry: &'a Registry,
    pins: &[crate::SubOntology],
    suffix: &str,
) -> Result<Vec<Staged<'a>>> {
    let mut staged: Vec<Staged<'a>> = Vec::new();
    for pin in pins {
        let path = PathBuf::from(pin.schema_path());
        let target = root.join(&path);
        let temp = target.with_file_name(format!(".schema.graphql.{suffix}.pull"));
        let pinned = registry.get(&pin.name).map_or("", |sub| sub.sdl.as_str());
        if let Err(e) = write_new(&temp, pin.sdl.as_bytes()) {
            // Only a staged copy this pull created is removed.
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                let _ = std::fs::remove_file(&temp);
            }
            discard(&staged);
            return Err(miette!("writing {}: {e}; nothing written", path.display()));
        }
        let mode = std::fs::metadata(&target)
            .and_then(|meta| std::fs::set_permissions(&temp, meta.permissions()));
        if let Err(e) = mode {
            let _ = std::fs::remove_file(&temp);
            discard(&staged);
            return Err(miette!("writing {}: {e}; nothing written", path.display()));
        }
        staged.push((path, temp, pinned));
    }
    Ok(staged)
}

/// The new pins were composed against HEAD as it was, and replace the
/// schemas as they were: anything else by now is someone else's change.
fn require_unmoved(
    root: &Path,
    registry: &Registry,
    pins: &[crate::SubOntology],
    staged: &[Staged<'_>],
) -> Result<()> {
    if head_commit(root) != registry.commit {
        return Err(miette!(
            "HEAD moved while pulling (the new schemas were checked against {}); nothing written, pull again",
            registry.source
        ));
    }
    let unchanged = |file: &Path, expected: &str| {
        std::fs::read(file).is_ok_and(|now| now == expected.as_bytes())
    };
    let edited: Vec<String> = staged
        .iter()
        .zip(pins)
        .filter(|((path, temp, pinned), pin)| {
            !unchanged(&root.join(path), pinned) || !unchanged(temp, &pin.sdl)
        })
        .map(|((path, _, _), _)| path.display().to_string())
        .collect();
    if edited.is_empty() {
        return Ok(());
    }
    Err(miette!(
        "{} changed while pulling; nothing written",
        edited.join(", ")
    ))
}

/// Replace each pin's `schema.graphql` and commit them, all or none: every
/// new schema is staged beside its target first, and nothing is replaced if
/// HEAD or a pinned schema changed while the remotes answered. Then they
/// are swapped in and committed onto the commit they were checked against;
/// a failed swap or commit puts the previous pins back.
fn install_pins(
    root: &Path,
    registry: &Registry,
    pins: &[crate::SubOntology],
    message: &str,
    no_commit: bool,
) -> Result<()> {
    // Names no one can plant a file or symlink at in advance.
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos())
    );
    let staged = stage_pins(root, registry, pins, &suffix)?;
    if let Err(e) = require_unmoved(root, registry, pins, &staged) {
        discard(&staged);
        return Err(e);
    }
    // Each replaced pin is kept beside it until the commit lands, and put
    // back by renaming it: whatever its mode, no write is needed.
    let backup = |path: &Path| {
        root.join(path)
            .with_file_name(format!(".schema.graphql.{suffix}.orig"))
    };
    let restore = |replaced: &[Staged<'_>]| -> String {
        let stuck: Vec<String> = replaced
            .iter()
            .filter(|(path, _, _)| std::fs::rename(backup(path), root.join(path)).is_err())
            .map(|(path, _, _)| path.display().to_string())
            .collect();
        if stuck.is_empty() {
            "the previous schemas are back".to_string()
        } else {
            format!(
                "could not put back {}; `git checkout -- <path>` restores them",
                stuck.join(", ")
            )
        }
    };
    for (i, (path, temp, _)) in staged.iter().enumerate() {
        let target = root.join(path);
        let saved = backup(path);
        // A rename replaces whatever is there, so nothing may be.
        let moved_aside = if std::fs::symlink_metadata(&saved).is_ok() {
            Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("{} is in the way", saved.display()),
            ))
        } else {
            std::fs::rename(&target, &saved)
        };
        if let Err(e) = moved_aside {
            let restored = restore(&staged[..i]);
            discard(&staged[i..]);
            return Err(miette!("replacing {}: {e}; {restored}", path.display()));
        }
        if let Err(e) = std::fs::rename(temp, &target) {
            let restored = restore(&staged[..=i]);
            discard(&staged[i..]);
            return Err(miette!("replacing {}: {e}; {restored}", path.display()));
        }
    }
    let paths: Vec<PathBuf> = staged.iter().map(|(path, _, _)| path.clone()).collect();
    if let Err(e) = commit_pinned(root, &paths, message, no_commit, registry.commit.as_deref()) {
        let restored = restore(&staged);
        let _ = crate::snapshot::unstage_paths(root, &paths);
        return Err(miette!("{e}; {restored}"));
    }
    for (path, _, _) in &staged {
        let _ = std::fs::remove_file(backup(path));
    }
    Ok(())
}

/// Refuse to publish or serve a revision that `check` rejects: a malformed
/// record anywhere would otherwise surface as a failing query later.
fn require_clean(snapshot: &dyn Snapshot, source: &SourceArgs, action: &str) -> Result<()> {
    let report = check(snapshot, &source.dir()).into_diagnostic()?;
    if report.errors() == 0 {
        return Ok(());
    }
    for issue in report
        .issues
        .iter()
        .filter(|i| i.severity == Severity::Error)
        .take(20)
    {
        let location = issue
            .path
            .as_deref()
            .map(|p| format!("{p}: "))
            .unwrap_or_default();
        eprintln!("error: [{}] {location}{}", issue.rule, issue.message);
    }
    Err(miette!(
        "{} check error(s) at {}; fix them (`scriptorium-ontology check`) before you {action}",
        report.errors(),
        report.source
    ))
}

fn open(root: &Path, source: &SourceArgs) -> Result<Box<dyn Snapshot>> {
    let snapshot = source.spec().open(root).into_diagnostic()?;
    if !source.worktree
        && source.rev == "HEAD"
        && has_uncommitted_changes(root, &source.dir()).unwrap_or(false)
    {
        eprintln!(
            "note: {}/ has uncommitted changes; reading {} (pass --worktree to include them)",
            source.dir(),
            snapshot.label()
        );
    }
    Ok(snapshot)
}

/// How registry problems (a malformed `subgraph.toml`, a missing schema, …)
/// are treated. A problem sub-ontology is left out of the registry, so
/// commands that publish the composed ontology must not proceed without it.
#[derive(Clone, Copy)]
enum Issues {
    /// Report and continue (`list`).
    Warn,
    /// Report and fail (`compose`, `serve`): a supergraph that silently
    /// drops a sub-ontology would make its fields vanish from the gateway.
    Fail,
}

fn load(snapshot: &dyn Snapshot, source: &SourceArgs, policy: Issues) -> Result<Registry> {
    let (registry, issues) = Registry::load(snapshot, &source.dir()).into_diagnostic()?;
    let label = match policy {
        Issues::Warn => "warning",
        Issues::Fail => "error",
    };
    for issue in &issues {
        eprintln!("{label}: {}: {}", issue.path, issue.message);
    }
    if matches!(policy, Issues::Fail) && !issues.is_empty() {
        return Err(miette!(
            "{} sub-ontology problem(s) at {}; fix them (see `scriptorium-ontology check`) before composing or serving",
            issues.len(),
            registry.source
        ));
    }
    Ok(registry)
}

fn list(root: &Path, source: &SourceArgs, json: bool) -> Result<ExitCode> {
    let snapshot = open(root, source)?;
    let registry = load(snapshot.as_ref(), source, Issues::Warn)?;
    let base_url = registry.manifest.serve.base_url();
    let rows: Vec<serde_json::Value> = registry
        .sub_ontologies
        .iter()
        .map(|sub| {
            // Owned entities by name; extensions (fields added to an entity
            // another sub-ontology owns) marked with a leading `+`.
            let entities: Vec<String> = SubOntologyModel::parse(&sub.sdl)
                .map(|m| {
                    m.entities()
                        .map(|o| {
                            if o.extension {
                                format!("+{}", o.name)
                            } else {
                                o.name.clone()
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            serde_json::json!({
                "name": sub.name,
                "dir": sub.dir,
                "storage": sub.storage_kind(),
                "url": sub.subgraph_url(&base_url),
                "owners": sub.owners,
                "description": sub.description,
                "entities": entities,
            })
        })
        .collect();
    if json {
        let out = serde_json::json!({
            "source": registry.source,
            "commit": registry.commit,
            "sub_ontologies": rows,
        });
        println!("{}", serde_json::to_string_pretty(&out).into_diagnostic()?);
        return Ok(ExitCode::SUCCESS);
    }
    println!(
        "ontology {}/ at {}  (+Type = extends an entity owned elsewhere)",
        registry.ontology_dir, registry.source
    );
    if rows.is_empty() {
        println!("  (no sub-ontologies — a sub-ontology is a directory with subgraph.toml)");
    }
    let width = registry
        .sub_ontologies
        .iter()
        .map(|s| s.name.len())
        .max()
        .unwrap_or(0);
    for (sub, row) in registry.sub_ontologies.iter().zip(&rows) {
        let entities = row["entities"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let mut line = format!(
            "  {:width$}  {:6}  entities: {}",
            sub.name,
            sub.storage_kind(),
            if entities.is_empty() {
                "—"
            } else {
                &entities
            }
        );
        if let SubOntologySource::Remote(remote) = &sub.source {
            let _ = write!(line, "  → {}", remote.url);
        }
        if !sub.owners.is_empty() {
            let _ = write!(line, "  owners: {}", sub.owners.join(", "));
        }
        println!("{line}");
    }
    Ok(ExitCode::SUCCESS)
}

async fn compose_cmd(
    root: &Path,
    source: &SourceArgs,
    base_url: Option<String>,
    out: Option<PathBuf>,
    api: bool,
    offline: bool,
) -> Result<ExitCode> {
    if source.worktree && out.is_some() {
        // Publishing reads a commit, like `serve`: what a gateway loads must
        // be what `serve` can start from. `--worktree` without `--out` still
        // previews on stdout.
        return Err(miette!(
            "`compose --out` publishes a commit; commit your changes (or pass --rev) instead of --worktree"
        ));
    }
    let snapshot = open(root, source)?;
    let registry = load(snapshot.as_ref(), source, Issues::Fail)?;
    let base_url = base_url.unwrap_or_else(|| registry.manifest.serve.base_url());
    let composition = compose(&registry, &base_url);
    for warning in &composition.warnings {
        eprintln!("warning: {warning}");
    }
    if !composition.is_ok() {
        for error in &composition.errors {
            eprintln!("error: {error}");
        }
        eprintln!("composition failed at {}", registry.source);
        return Ok(ExitCode::from(1));
    }
    // A supergraph is only useful if `serve` can run every local subgraph it
    // routes to: preflight exactly what `serve` does at startup (real
    // stores, inverse indexes), and refuse to publish if it would fail.
    let snapshot: Arc<dyn Snapshot> = Arc::from(snapshot);
    if out.is_some() {
        require_clean(snapshot.as_ref(), source, "publish a supergraph")?;
    }
    if let Err(e) = build_subgraphs(&registry, &snapshot) {
        eprintln!("error: {e}");
        eprintln!(
            "composition succeeded, but `scriptorium-ontology serve` could not start at {}",
            registry.source
        );
        return Ok(ExitCode::from(1));
    }
    let sdl = if api {
        composition.api_schema.unwrap_or_default()
    } else {
        composition.supergraph.unwrap_or_default()
    };
    match out {
        Some(path) => {
            require_routable(&registry, &base_url)?;
            if !offline {
                require_live_subgraphs_match(&registry, &base_url, Answering::Separately).await?;
                require_remote_pins_match(root, &registry).await?;
            }
            let target = publication_target(&path)?;
            let _publisher = lock_publication(&target)?;
            write_atomically(&target, &sdl)?;
            eprintln!(
                "composed {} sub-ontologies at {} → {}",
                registry.sub_ontologies.len(),
                registry.source,
                path.display()
            );
        }
        None => print!("{sdl}"),
    }
    Ok(ExitCode::SUCCESS)
}

fn check_cmd(root: &Path, source: &SourceArgs, json: bool) -> Result<ExitCode> {
    let snapshot = open(root, source)?;
    let report = check(snapshot.as_ref(), &source.dir()).into_diagnostic()?;
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).into_diagnostic()?
        );
    } else {
        println!(
            "ontology check: {} — {} sub-ontologies, {} records",
            report.source, report.sub_ontologies, report.records
        );
        for issue in &report.issues {
            let severity = match issue.severity {
                Severity::Error => "error  ",
                Severity::Warning => "warning",
            };
            let location = issue
                .path
                .as_deref()
                .map(|p| format!("{p}: "))
                .unwrap_or_default();
            println!("{severity} [{}] {location}{}", issue.rule, issue.message);
        }
        if report.issues.is_empty() {
            println!("ok: no issues");
        } else {
            println!(
                "{} error(s), {} warning(s)",
                report.errors(),
                report.warnings()
            );
        }
    }
    Ok(if report.errors() > 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}

async fn serve(
    root: &Path,
    source: &SourceArgs,
    addr: Option<String>,
    supergraph: Option<PathBuf>,
    offline: bool,
) -> Result<ExitCode> {
    if source.worktree {
        // Serving reads a commit: its records are cached and indexed, and
        // what the gateway sees is exactly what is committed.
        return Err(miette!(
            "`scriptorium-ontology serve` reads a commit; commit your changes (or pass --rev) instead of --worktree"
        ));
    }
    let snapshot: Arc<dyn Snapshot> = Arc::from(open(root, source)?);
    let registry = load(snapshot.as_ref(), source, Issues::Fail)?;
    require_clean(snapshot.as_ref(), source, "serve it")?;
    let settings = &registry.manifest.serve;
    let addr = addr.unwrap_or_else(|| settings.addr().to_string());

    let subgraphs = build_subgraphs(&registry, &snapshot).into_diagnostic()?;
    if subgraphs.is_empty() {
        return Err(miette!(
            "no local sub-ontologies to serve under {}/ at {}",
            registry.ontology_dir,
            registry.source
        ));
    }
    let composition = compose(&registry, "http://placeholder.invalid");
    if supergraph.is_some() && !composition.is_ok() {
        for error in &composition.errors {
            eprintln!("error: {error}");
        }
        return Err(miette!("composition failed; supergraph not written"));
    }
    // A remote's pin can be stale (this server's own subgraphs are asked
    // once it listens).
    if supergraph.is_some() && !offline {
        require_remote_pins_match(root, &registry).await?;
    }
    // One publisher per supergraph path, held for the server's lifetime, so
    // a second (possibly older) serve cannot replace what this one published.
    let supergraph = supergraph.as_deref().map(publication_target).transpose()?;
    let _publisher = supergraph.as_deref().map(lock_publication).transpose()?;

    // Every store must be reachable before anything is published: a
    // database outage fails startup instead of serving a broken subgraph.
    for subgraph in &subgraphs {
        subgraph
            .store
            .ready()
            .await
            .map_err(|e| miette!("[{}] {}: {e}", subgraph.name, subgraph.store.describe()))?;
    }

    // Bind before publishing the supergraph: if the port is taken (say an
    // older server still runs), fail without touching the gateway's file.
    let listener = crate::serve::bind(&addr)
        .await
        .map_err(|e| miette!("binding {addr}: {e}"))?;
    let bound = listener.local_addr().into_diagnostic()?;
    let base_url = settings
        .base_url
        .clone()
        .unwrap_or_else(|| format!("http://{bound}"))
        .trim_end_matches('/')
        .to_string();
    if supergraph.is_some() {
        require_routable(&registry, &base_url)?;
    }
    let composition = compose(&registry, &base_url);
    if supergraph.is_none() && !composition.is_ok() {
        eprintln!(
            "warning: sub-ontologies do not compose ({} error(s)); subgraphs are served anyway — run `scriptorium-ontology check`",
            composition.errors.len()
        );
    }

    let banner = serve_banner(&registry, &subgraphs, &base_url, supergraph.as_deref());
    let server = tokio::spawn(crate::serve::serve_listener(listener, subgraphs, async {
        let _ = tokio::signal::ctrl_c().await;
    }));
    // Serving already: the supergraph routes to the base URL, so what
    // answers there must be this server (a configured `base_url` may lead
    // to an older one).
    if let Some(path) = &supergraph {
        let published = async {
            if !offline {
                require_live_subgraphs_match(&registry, &base_url, Answering::ThisServer).await?;
            }
            write_atomically(path, composition.supergraph.as_deref().unwrap_or_default())
        }
        .await;
        if let Err(e) = published {
            server.abort();
            return Err(e);
        }
    }
    {
        use std::io::Write as _;
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(banner.as_bytes());
        let _ = out.flush();
    }

    server
        .await
        .into_diagnostic()?
        .map_err(|e| miette!("serving on {bound}: {e}"))?;
    Ok(ExitCode::SUCCESS)
}

/// What `serve` prints once it serves: each sub-ontology's URL and store,
/// and the supergraph it published. Printed with write errors ignored: the
/// server outlives whoever reads it (`| head`).
fn serve_banner(
    registry: &Registry,
    subgraphs: &[crate::serve::Subgraph],
    base_url: &str,
    supergraph: Option<&Path>,
) -> String {
    let mut banner = String::new();
    let _ = writeln!(banner, "ontology serve: {}", registry.source);
    for sub in &registry.sub_ontologies {
        let _ = match subgraphs.iter().find(|s| s.name == sub.name) {
            Some(served) => writeln!(
                banner,
                "  {:12} {}  ({})",
                sub.name,
                sub.subgraph_url(base_url),
                served.store.describe()
            ),
            None => writeln!(
                banner,
                "  {:12} {}  (remote, not served here)",
                sub.name,
                sub.subgraph_url(base_url)
            ),
        };
    }
    if let Some(path) = supergraph {
        let _ = writeln!(banner, "supergraph: {}", path.display());
        let _ = writeln!(
            banner,
            "gateway:    npx @graphql-hive/gateway supergraph {}",
            path.display()
        );
    }
    banner
}

/// Where publishing to `path` must write: through an existing symlink to its
/// target, so the file a gateway reads is the one replaced (and so every
/// publisher of that file contends for the same lock).
fn publication_target(path: &Path) -> Result<PathBuf> {
    let is_symlink = std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink());
    if !is_symlink {
        return Ok(path.to_path_buf());
    }
    std::fs::canonicalize(path).map_err(|e| {
        miette!(
            "{} is a symlink that cannot be resolved: {e}",
            path.display()
        )
    })
}

/// Give `tmp` the owner and group of the file it will replace, or explain
/// why not. Changing the owner needs privileges and the group needs
/// membership; when either is refused, publishing would change who can read
/// the file (a gateway running as its owner could lose access), so the
/// caller keeps the old file instead.
#[cfg(unix)]
fn keep_ownership(tmp: &Path, uid: u32, gid: u32) -> std::result::Result<(), String> {
    use std::os::unix::fs::MetadataExt;

    let current = std::fs::metadata(tmp).map_err(|e| e.to_string())?;
    if current.uid() == uid && current.gid() == gid {
        return Ok(());
    }
    std::os::unix::fs::chown(tmp, Some(uid), Some(gid)).map_err(|e| {
        format!(
            "cannot keep its owner/group (uid {uid}, gid {gid}): {e}; publish as that user, or remove the file first"
        )
    })
}

/// Take the exclusive publication lock for `path` (a sibling `.lock` file).
/// The lock lasts as long as the returned file is open.
fn lock_publication(path: &Path) -> Result<std::fs::File> {
    let file_name = path
        .file_name()
        .ok_or_else(|| miette!("{} is not a file path", path.display()))?;
    let lock_path = path.with_file_name(format!(".{}.lock", file_name.to_string_lossy()));
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)
        .into_diagnostic()?;
    match lock.try_lock() {
        Ok(()) => Ok(lock),
        Err(std::fs::TryLockError::WouldBlock) => Err(miette!(
            "{} is being published by another `scriptorium-ontology` process; stop it or choose another path",
            path.display()
        )),
        Err(std::fs::TryLockError::Error(e)) => Err(miette!("locking {}: {e}", lock_path.display())),
    }
}

/// Replace `path` in one step (write a sibling, then rename), so a gateway
/// watching it never reads a half-written supergraph. The sibling's name is
/// unique and created exclusively, so concurrent publishers never share it.
fn write_atomically(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write as _;

    let file_name = path
        .file_name()
        .ok_or_else(|| miette!("{} is not a file path", path.display()))?;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let tmp = path.with_file_name(format!(
        ".{}.{}.{nanos}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    // Keep an existing file's permissions, owner, and group (refusing to
    // publish when the owner or group cannot be kept), since a gateway may
    // read it as another user; a new file gets the same umask-derived mode
    // `fs::write` would. ACLs are not carried over.
    let existing = std::fs::metadata(path).ok();
    let existing_permissions = existing.as_ref().map(std::fs::Metadata::permissions);
    let written = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .and_then(|mut file| {
            if let Some(permissions) = existing_permissions {
                file.set_permissions(permissions)?;
            }
            #[cfg(unix)]
            if let Some(existing) = &existing {
                use std::os::unix::fs::MetadataExt;
                keep_ownership(&tmp, existing.uid(), existing.gid())
                    .map_err(std::io::Error::other)?;
            }
            file.write_all(contents.as_bytes())?;
            file.sync_all()
        })
        .and_then(|()| std::fs::rename(&tmp, path));
    if written.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    written.map_err(|e| {
        miette!(
            "publishing {}: {e}; the existing file was left in place",
            path.display()
        )
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_header_given_twice_in_any_case_is_refused() {
        let args = |list: &[&str]| list.iter().map(ToString::to_string).collect::<Vec<_>>();
        for twice in [
            ["Authorization=A", "Authorization=B"],
            ["Authorization=A", "authorization=B"],
            ["X-Key=A", "x-KEY=B"],
        ] {
            let err = parse_headers(&args(&twice)).unwrap_err().to_string();
            assert!(err.contains("given twice"), "{twice:?}: {err}");
        }
        let headers = parse_headers(&args(&["Authorization=A", "X-Key=B"])).unwrap();
        assert_eq!(
            headers.into_iter().collect::<Vec<_>>(),
            [
                ("Authorization".to_string(), "A".to_string()),
                ("X-Key".to_string(), "B".to_string())
            ]
        );
    }

    #[test]
    fn exclusive_writes_refuse_what_is_there() {
        let dir = tempfile::tempdir().unwrap();
        let outside = dir.path().join("outside.txt");
        std::fs::write(&outside, "keep me").unwrap();
        let link = dir.path().join(".schema.graphql.1.pull");
        std::os::unix::fs::symlink(&outside, &link).unwrap();
        let err = write_new(&link, b"new").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "keep me");
        let fresh = dir.path().join("fresh");
        write_new(&fresh, b"new").unwrap();
        assert_eq!(std::fs::read_to_string(&fresh).unwrap(), "new");
        assert!(write_new(&fresh, b"again").is_err());
        // A write that fails partway leaves nothing behind.
        let partial = dir.path().join("partial");
        let err = write_new_with(&partial, |file| {
            use std::io::Write as _;
            file.write_all(b"half")?;
            Err(std::io::Error::other("disk full"))
        })
        .unwrap_err();
        assert_eq!(err.to_string(), "disk full");
        assert!(!partial.exists());
    }

    #[test]
    fn ownership_that_cannot_be_kept_is_an_error() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp");
        std::fs::write(&tmp, "x").unwrap();
        let mine = std::fs::metadata(&tmp).unwrap();
        assert!(
            keep_ownership(&tmp, mine.uid(), mine.gid()).is_ok(),
            "same owner is a no-op"
        );
        if mine.uid() == 0 {
            return; // root may chown to anyone
        }
        let err = keep_ownership(&tmp, 0, 0).unwrap_err();
        assert!(err.contains("cannot keep its owner/group"), "{err}");
    }
}
