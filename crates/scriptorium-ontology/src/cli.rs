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
    /// Scaffold the example ontology (core, research, software) and commit it.
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
/// No credentials are sent.
async fn require_remote_pins_match(registry: &Registry) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .into_diagnostic()?;
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
        let response = client
            .post(&remote.url)
            .json(&serde_json::json!({ "query": "{ _service { sdl } }" }))
            .send()
            .await
            .map_err(|e| unknown(error_chain(&e.without_url())))?;
        let status = response.status();
        if !status.is_success() {
            return Err(unknown(format!("HTTP {status}")));
        }
        let served = probe_json(response)
            .await
            .map_err(unknown)?
            .and_then(|body| {
                body.pointer("/data/_service/sdl")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            });
        if served.as_deref() != Some(sub.sdl.as_str()) {
            drifted.push(format!("`{}`", sub.name));
        }
    }
    if drifted.is_empty() {
        return Ok(());
    }
    Err(miette!(
        "remote {} no longer serve the schema pinned at {}; commit their current schema.graphql, or pass --offline",
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
                require_remote_pins_match(&registry).await?;
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
        require_remote_pins_match(&registry).await?;
    }
    // One publisher per supergraph path, held for the server's lifetime, so
    // a second (possibly older) serve cannot replace what this one published.
    let supergraph = supergraph.as_deref().map(publication_target).transpose()?;
    let _publisher = supergraph.as_deref().map(lock_publication).transpose()?;

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
                served.store
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
