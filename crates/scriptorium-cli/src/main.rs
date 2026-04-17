//! Scriptorium CLI entrypoint.
//!
//! Subcommands are thin shims over [`scriptorium_core`] operations. Each one
//! parses args, builds the requested core call, and renders results. Error
//! rendering uses `miette` so library `thiserror` errors pretty-print with
//! source context.

#[cfg(feature = "dashboard")]
mod dashboard;
mod log_cmd;
mod trace_cmd;

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use miette::{miette, IntoDiagnostic, Result};
use scriptorium_core::config::Config;
use scriptorium_core::embed::EmbeddingsStore;
use scriptorium_core::lint::Severity;
use scriptorium_core::llm::{
    ClaudeConfig, ClaudeProvider, GeminiConfig, GeminiProvider, LlmProvider, MockProvider,
    OllamaConfig, OllamaProvider, OpenAiConfig, OpenAiProvider,
};
use scriptorium_core::telemetry::store::GLOBAL_STATS;
use scriptorium_core::telemetry::{
    cap_body, install_global, Source, TelemetryStore, DEFAULT_PAYLOAD_CAP,
};
use scriptorium_core::{self as core, ingest, query, Vault};
use tracing::Instrument;

#[derive(Debug, Parser)]
#[command(
    name = "scriptorium",
    version,
    about = "LLM-maintained Obsidian vault — ingest, query, lint",
    propagate_version = true
)]
struct Cli {
    /// Vault root (defaults to current directory).
    #[arg(short = 'C', long, global = true, value_name = "PATH")]
    vault: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Interactive setup wizard: configure providers, store API keys in
    /// the macOS keychain, and write config.toml.
    Setup,

    /// Scaffold a new vault from the bundled templates.
    Init {
        /// Target directory (defaults to the `-C` vault or current dir).
        path: Option<PathBuf>,
    },

    /// Ingest a source: read it (or fetch a URL and run Readability),
    /// prompt the LLM, and commit the resulting wiki pages.
    Ingest {
        /// Path to a local source file. Mutually exclusive with --url.
        source: Option<PathBuf>,
        /// URL to fetch, extract main content from via Mozilla Readability,
        /// convert to markdown, and ingest as if it were a local file.
        /// Mutually exclusive with the positional source.
        #[arg(long, conflicts_with = "source")]
        url: Option<String>,
        /// Override the provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
        /// Override the model declared in config (e.g. "claude-sonnet-4-6").
        #[arg(long)]
        model: Option<String>,
        /// Stage all writes, call the LLM, then print a preview of the
        /// change set and abort instead of committing. The source file is
        /// still interned into `sources/` but no wiki pages are written
        /// and no git commit is made. Use this to preview what an ingest
        /// would do before you trust it.
        #[arg(long)]
        dry_run: bool,
    },

    /// Ask a question against the vault.
    Query {
        /// The question to ask (in quotes if it has spaces).
        question: String,
        /// Number of chunks to retrieve from the embeddings store.
        #[arg(long, default_value_t = 5)]
        top_k: usize,
        /// Override the provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
    },

    /// Rebuild the embeddings store from the current vault contents.
    Reindex {
        /// Override the provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
    },

    /// Run mechanical lint rules and print the report.
    Lint {
        /// Exit with a non-zero status if any issues are found (not just errors).
        #[arg(long)]
        strict: bool,
        /// Auto-fix the safe rules before reporting. Currently only
        /// `frontmatter.bad_timestamps` is auto-fixable; everything else is
        /// reported as "skipped" with a reason.
        #[arg(long)]
        fix: bool,
    },

    /// Undo the most recent scriptorium commit via `git revert HEAD`.
    Undo,

    /// Run health checks on the vault: git repo, schema, embeddings,
    /// links, git status. No LLM required.
    Doctor {
        /// Output as JSON instead of a human-readable table.
        #[arg(long)]
        json: bool,
    },

    /// Run maintenance tasks: lint, stale detection, embedding coverage.
    /// Optionally auto-fix safe issues.
    Maintain {
        /// Auto-fix safe issues (re-embed stale pages, fix bad timestamps).
        #[arg(long)]
        fix: bool,
        /// Override the embeddings provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
    },

    /// Bulk-ingest all eligible files from a directory. Supports checkpoint
    /// resume — interrupted imports pick up where they left off.
    BulkIngest {
        /// Directory containing source files to ingest.
        dir: PathBuf,
        /// Override the provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
        /// Override the model declared in config (e.g. "claude-sonnet-4-6").
        #[arg(long)]
        model: Option<String>,
        /// Start fresh, ignoring any existing checkpoint.
        #[arg(long)]
        fresh: bool,
        /// Dry run: report what would be ingested without committing.
        #[arg(long)]
        dry_run: bool,
    },

    /// Show the resolved config for this vault.
    Config,

    /// Manage skills: list, show, or scaffold defaults.
    #[command(subcommand)]
    Skill(SkillCommand),

    /// Manage the self-learning journal.
    #[command(subcommand)]
    Learn(LearnCommand),

    /// Run retrieval quality benchmarks (precision@k, recall, MRR).
    Bench {
        /// Output as JSON instead of a human-readable table.
        #[arg(long)]
        json: bool,
        /// Scaffold an empty benchmarks.json if none exists.
        #[arg(long)]
        init: bool,
        /// Override the provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
    },

    /// Run the MCP server on stdio so Claude Code (and other MCP clients)
    /// can drive this vault as native tools.
    Serve {
        /// Override the provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
    },

    /// Watch `sources/` and auto-ingest new files.
    Watch {
        /// Override the provider declared in config.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
    },

    /// Import data from social media platforms.
    #[command(subcommand)]
    Social(SocialCommand),

    /// Manage registered vaults: list, add, remove, set default.
    #[command(name = "vault", subcommand)]
    VaultMgmt(VaultCommand),

    /// Claude Code hook event ingestion into `SQLite`.
    #[command(subcommand)]
    Hooks(HooksCommand),

    /// OTel-shaped log emit / tail subcommands.
    Log(log_cmd::LogCommand),

    /// Trace tree inspect / new-root + span start / span end.
    Trace(trace_cmd::TraceCommand),
}

#[derive(Debug, Subcommand)]
enum HooksCommand {
    /// Read a JSON hook event from stdin and write it to the hooks `SQLite` database.
    ///
    /// Designed for Claude Code hooks to pipe events directly into `SQLite`,
    /// bypassing the JSONL intermediate format. Always exits 0 — hook
    /// reliability invariant means we never break the caller.
    Log {
        /// Path to the hooks `SQLite` database.
        /// Defaults to `~/.scriptorium/hooks.sqlite`.
        #[arg(long)]
        db: Option<PathBuf>,
        /// Validate the event but don't write to the database.
        #[arg(long)]
        dry_run: bool,
    },

    /// One-time full backfill: import all events from a JSONL file into
    /// the hooks `SQLite` database. Idempotent — safe to re-run; duplicates
    /// are silently skipped via content hash.
    Migrate {
        /// Path to the JSONL file.
        /// Defaults to `~/.claude/artifacts/scriptorium-classifier.jsonl`.
        #[arg(long)]
        jsonl: Option<PathBuf>,
        /// Path to the hooks `SQLite` database.
        /// Defaults to `~/.scriptorium/hooks.sqlite`.
        #[arg(long)]
        db: Option<PathBuf>,
        /// Parse and validate only — don't write to the database.
        #[arg(long)]
        dry_run: bool,
    },

    /// Incremental import: import new events from a JSONL file into the
    /// hooks `SQLite` database. Semantically identical to `migrate` (both
    /// are idempotent), but signals intent for ongoing use.
    Import {
        /// Path to the JSONL file.
        /// Defaults to `~/.claude/artifacts/scriptorium-classifier.jsonl`.
        #[arg(long)]
        jsonl: Option<PathBuf>,
        /// Path to the hooks `SQLite` database.
        /// Defaults to `~/.scriptorium/hooks.sqlite`.
        #[arg(long)]
        db: Option<PathBuf>,
        /// Parse and validate only — don't write to the database.
        #[arg(long)]
        dry_run: bool,
    },

    /// Backfill existing `hook_events` rows into the new OTel-shaped
    /// `logs`/`spans` tables. Idempotent and safe to re-run.
    MigrateBackfill {
        /// Path to the hooks `SQLite` database.
        /// Defaults to `~/.scriptorium/hooks.sqlite`.
        #[arg(long)]
        db: Option<PathBuf>,
        /// Walk the data and report what would be inserted, but do not
        /// write.
        #[arg(long)]
        dry_run: bool,
    },

    /// Run health checks on Claude Code hook registrations.
    ///
    /// By default runs static checks only (registration, permissions,
    /// timeouts, guard patterns, freshness). Use `--test` to also
    /// execute hooks with synthetic payloads and `SCRIPTORIUM_DRY_RUN=1`.
    Check {
        /// Also run dynamic validation: execute each hook with a
        /// synthetic payload and `SCRIPTORIUM_DRY_RUN=1`.
        #[arg(long)]
        test: bool,
        /// Output as JSON instead of a human-readable table.
        #[arg(long)]
        json: bool,
        /// Lightweight subset: static session-hook checks only
        /// (skip vault checks, dynamic checks, even if `--test`).
        #[arg(long)]
        quick: bool,
        /// Path to Claude Code `settings.json`.
        /// Defaults to `~/.claude/settings.json`.
        #[arg(long)]
        settings: Option<PathBuf>,
        /// Path to the hooks directory containing `scriptorium-*.sh` scripts.
        /// Defaults to `~/dotfiles/claude/hooks/`.
        #[arg(long)]
        hooks_dir: Option<PathBuf>,
        /// Path to the scriptorium vault root (for vault-level checks).
        #[arg(long)]
        vault: Option<PathBuf>,
    },

    /// List all registered and on-disk hooks in a table.
    List {
        /// Path to Claude Code `settings.json`.
        /// Defaults to `~/.claude/settings.json`.
        #[arg(long)]
        settings: Option<PathBuf>,
        /// Path to the hooks directory containing `scriptorium-*.sh` scripts.
        /// Defaults to `~/dotfiles/claude/hooks/`.
        #[arg(long)]
        hooks_dir: Option<PathBuf>,
    },

    /// Launch the hooks health dashboard web UI.
    ///
    /// Starts a localhost-only axum server serving the telemetry dashboard.
    /// Optionally imports events from a JSONL file before starting.
    /// Runs in the foreground until Ctrl-C.
    #[cfg(feature = "dashboard")]
    Dashboard {
        /// Port to listen on.
        #[arg(long, default_value = "3457")]
        port: u16,
        /// Don't auto-open the browser.
        #[arg(long)]
        no_browser: bool,
        /// Path to the hooks `SQLite` database.
        /// Defaults to `~/.scriptorium/hooks.sqlite`.
        #[arg(long)]
        db: Option<PathBuf>,
        /// Path to JSONL file to import before starting.
        #[arg(long)]
        jsonl: Option<PathBuf>,
        /// Path to Claude Code `settings.json`.
        /// Defaults to `~/.claude/settings.json`.
        #[arg(long)]
        settings: Option<PathBuf>,
        /// Path to the hooks directory containing `scriptorium-*.sh` scripts.
        /// Defaults to `~/dotfiles/claude/hooks/`.
        #[arg(long)]
        hooks_dir: Option<PathBuf>,
        /// Path to the scriptorium vault root (for vault-level info).
        /// Defaults to the default vault from global config, if set.
        #[arg(long)]
        vault: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum SocialCommand {
    /// Import a Facebook data export: parse JSON, generate markdown sources,
    /// and ingest them into the vault — all in one step.
    ///
    /// Accepts one or more export directories (Facebook splits large exports
    /// across multiple ZIPs). The command auto-detects which directory
    /// contains JSON data.
    Facebook {
        /// One or more Facebook export directories.
        #[arg(required = true)]
        export_dirs: Vec<PathBuf>,
        /// Only process specific categories (comma-separated).
        /// Options: messages, posts, comments, friends, search, events, groups
        #[arg(long, value_delimiter = ',')]
        categories: Option<Vec<String>>,
        /// Override the LLM provider for ingestion.
        #[arg(long, value_enum)]
        provider: Option<ProviderKind>,
        /// Override the LLM model for ingestion (e.g. "claude-haiku-4-5-20251001").
        #[arg(long)]
        model: Option<String>,
        /// Preview what would be generated without writing or ingesting.
        #[arg(long)]
        dry_run: bool,
    },
}

#[derive(Debug, Subcommand)]
enum SkillCommand {
    /// List all registered skills.
    List,
    /// Show the full content of a skill.
    Show {
        /// Skill name (e.g. "ingest", "query", "maintain").
        name: String,
    },
    /// Scaffold the default skills into <vault>/skills/.
    Init,
}

#[derive(Debug, Subcommand)]
enum LearnCommand {
    /// List the N most recent learnings.
    List {
        /// How many entries to show.
        #[arg(short, long, default_value_t = 20)]
        n: usize,
    },
    /// Search learnings by keyword.
    Search {
        /// Search query.
        query: String,
    },
    /// Add a learning from a JSON string.
    Add {
        /// JSON learning entry.
        json: String,
    },
    /// Remove learnings whose confidence has decayed to 0.
    Prune,
}

#[derive(Debug, Subcommand)]
enum VaultCommand {
    /// List all registered vaults; the default is marked with *.
    List,
    /// Register an existing vault directory under a name.
    Add {
        /// Short name for this vault (e.g. "main", "work").
        name: String,
        /// Path to the vault root directory.
        path: PathBuf,
    },
    /// Unregister a vault (does NOT delete files on disk).
    Remove {
        /// Name of the vault to unregister.
        name: String,
    },
    /// Set a vault as the default (used when -C is not provided).
    Default {
        /// Name of the vault to make default.
        name: String,
    },
    /// Show details about a vault: path, page count, config.
    Show {
        /// Vault name (uses the default vault if omitted).
        name: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ProviderKind {
    Claude,
    Openai,
    Gemini,
    Ollama,
    Mock,
}

#[tokio::main]
async fn main() -> ExitCode {
    // Global fmt subscriber as a fallback. `install_global` below uses a
    // thread-local scoped default which supersedes this for the duration
    // of `main`; the global remains as a safety net if telemetry init
    // fails.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "scriptorium=info".into()),
        )
        .try_init();

    let cli = Cli::parse();

    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "scriptorium=info");
    }

    // ── Install the telemetry tracing::Layer once per CLI invocation.
    // Best-effort: on failure we stderr + bump GLOBAL_STATS.dropped_count
    // and continue without telemetry. CLI never fails due to telemetry. ──
    let telemetry_source = select_source(&cli.command);
    let _telemetry_guard = install_telemetry_layer(telemetry_source);

    // ── Wrap every subcommand dispatch in a `cli.command` span. ──
    let cmd_name = command_name_for_span(&cli.command);
    let is_setup = matches!(cli.command, Command::Setup);
    let span = tracing::info_span!("cli.command", command = cmd_name, otel.kind = "server");

    let result = async move {
        // Setup subcommand omits args structurally (plan T12 guardrail
        // carve-out): the wizard takes API keys on stdin, so the span
        // records only command + duration + exit_code. This is NOT
        // content-based redaction.
        if is_setup {
            tracing::info!("cli.command.start");
        } else {
            let args = format_args_for_telemetry(&cli.command);
            tracing::info!(command_args = %args, "cli.command.start");
        }
        let res = run(cli).await;
        match &res {
            Ok(_) => tracing::info!(otel.status = "ok", "cli.command.end"),
            Err(e) => {
                tracing::error!(otel.status = "error", error = %e, "cli.command.end");
            }
        }
        res
    }
    .instrument(span)
    .await;

    match result {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err:?}");
            ExitCode::from(2)
        }
    }
}

/// Subcommand-aware source selection (plan T12).
///
/// - `Serve`          → [`Source::Mcp`] (this process is the MCP stdio server)
/// - `Hooks::Log`     → [`Source::Hook`]
/// - everything else  → [`Source::Cli`]
///
/// The `Log(emit)` subcommand reads its logical source from the stdin
/// JSON payload (T17), so the layer default stays `Cli` for that case.
fn select_source(cmd: &Command) -> Source {
    match cmd {
        Command::Serve { .. } => Source::Mcp,
        Command::Hooks(HooksCommand::Log { .. }) => Source::Hook,
        _ => Source::Cli,
    }
}

/// Install the SQLite-backed tracing layer. Returns `None` on failure
/// (store open error); in that case the CLI continues without
/// telemetry and `GLOBAL_STATS.dropped_count` is incremented. This is
/// the best-effort contract: telemetry must never fail the CLI.
fn install_telemetry_layer(source: Source) -> Option<tracing::dispatcher::DefaultGuard> {
    let db_path = default_hooks_db_path();
    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    match TelemetryStore::open(&db_path) {
        Ok(store) => Some(install_global(Arc::new(store), source, None)),
        Err(e) => {
            eprintln!("telemetry: store open failed, running without telemetry: {e}");
            GLOBAL_STATS
                .dropped_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            None
        }
    }
}

/// Map every `Command` variant to a short static subcommand name used
/// as the `command` attribute on the `cli.command` span.
fn command_name_for_span(cmd: &Command) -> &'static str {
    match cmd {
        Command::Setup => "setup",
        Command::Init { .. } => "init",
        Command::Ingest { .. } => "ingest",
        Command::Query { .. } => "query",
        Command::Reindex { .. } => "reindex",
        Command::Lint { .. } => "lint",
        Command::Undo => "undo",
        Command::Doctor { .. } => "doctor",
        Command::Maintain { .. } => "maintain",
        Command::BulkIngest { .. } => "bulk-ingest",
        Command::Config => "config",
        Command::Skill(_) => "skill",
        Command::Learn(_) => "learn",
        Command::Bench { .. } => "bench",
        Command::Serve { .. } => "serve",
        Command::Watch { .. } => "watch",
        Command::Social(_) => "social",
        Command::VaultMgmt(_) => "vault",
        Command::Hooks(_) => "hooks",
        Command::Log(_) => "log",
        Command::Trace(_) => "trace",
    }
}

/// Render the parsed command as the `command_args` attribute on the
/// `cli.command.start` event. Size-capped via T8's `cap_body`; no
/// content-based redaction. The Setup variant is excluded by the
/// caller (structural carve-out).
fn format_args_for_telemetry(cmd: &Command) -> String {
    let debug = format!("{cmd:?}");
    let (capped, _meta) = cap_body(&debug, &DEFAULT_PAYLOAD_CAP);
    capped
}

#[allow(clippy::too_many_lines)] // one arm per subcommand; flat is clearer
async fn run(cli: Cli) -> Result<ExitCode> {
    let explicit_vault = cli.vault.clone();
    match cli.command {
        // --- commands that don't need vault resolution ---
        Command::VaultMgmt(sub) => handle_vault_command(sub),
        Command::Log(cmd) => {
            log_cmd::run(cmd, default_hooks_db_path())?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Trace(cmd) => {
            trace_cmd::run(cmd, default_hooks_db_path())?;
            Ok(ExitCode::SUCCESS)
        }
        Command::Hooks(sub) => {
            #[cfg(feature = "dashboard")]
            if let HooksCommand::Dashboard {
                port,
                no_browser,
                db,
                jsonl,
                settings,
                hooks_dir,
                vault,
            } = sub
            {
                let db_path = db.unwrap_or_else(default_hooks_db_path);
                let settings_path = settings.unwrap_or_else(default_settings_path);
                let hooks_dir_path = hooks_dir.unwrap_or_else(default_hooks_dir);
                let vault_path = vault.or_else(|| {
                    scriptorium_core::global_config::GlobalConfig::load()
                        .ok()
                        .and_then(|cfg| cfg.default_vault().map(|v| v.path.clone()))
                });
                if !no_browser {
                    let _ = std::process::Command::new("open")
                        .arg(format!("http://127.0.0.1:{port}"))
                        .spawn();
                }
                dashboard::start_dashboard(
                    port,
                    db_path,
                    jsonl,
                    settings_path,
                    hooks_dir_path,
                    vault_path,
                )
                .await?;
                return Ok(ExitCode::SUCCESS);
            }
            handle_hooks_command(sub)
        }
        Command::Init { path } => {
            let target = path
                .or(explicit_vault)
                .unwrap_or_else(|| PathBuf::from("."));
            init_vault(&target)?;
            // Also scaffold skills and benchmarks.
            if let Ok(vault) = Vault::open(&target) {
                let _ = scriptorium_core::skills::init_skills(&vault);
                let _ = scriptorium_core::bench::save_suite(
                    &vault,
                    &scriptorium_core::bench::BenchmarkSuite {
                        benchmarks: Vec::new(),
                    },
                );
            }
            auto_register_vault(&target);
            println!("scriptorium init: scaffolded vault at {}", target.display());
            Ok(ExitCode::SUCCESS)
        }
        // --- all remaining commands need a resolved vault path ---
        command => {
            let vault_path = resolve_vault_path(explicit_vault)?;
            match command {
                Command::Setup => {
                    let vault = open_vault(&vault_path)?;
                    let stdin = std::io::stdin();
                    let mut input = stdin.lock();
                    let mut output = std::io::stderr();
                    scriptorium_core::setup::run_setup(&vault, &mut input, &mut output)
                        .into_diagnostic()?;
                    auto_register_vault(vault.root().as_std_path());
                    Ok(ExitCode::SUCCESS)
                }
                Command::Ingest {
                    source,
                    url,
                    provider,
                    model,
                    dry_run,
                } => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    let resolved_provider = provider.unwrap_or_else(|| provider_from(&cfg));
                    if let Some(ref m) = model {
                        set_model_env(provider_kind_name(resolved_provider), m);
                    }
                    let provider = build_provider(resolved_provider)?;
                    // Resolve to a concrete file path. URL ingest fetches, runs
                    // Readability, converts to markdown, writes a tempfile, and the
                    // regular file-ingest path takes over. The returned struct holds
                    // the tempdir alive until end of scope.
                    let resolved = resolve_ingest_source(source, url).await?;
                    let report = ingest::ingest_with_options(
                        &vault,
                        provider.as_ref(),
                        &resolved.path,
                        ingest::IngestOptions {
                            dry_run,
                            hooks: Some(cfg.hooks.clone()),
                        },
                    )
                    .await
                    .into_diagnostic()?;
                    if dry_run {
                        println!(
                            "ingest DRY RUN: would create {} page(s), update {} — nothing written.",
                            report.created, report.updated
                        );
                        println!("         summary: {}", report.summary);
                        println!("         diff:");
                        for change in &report.dry_run_diff {
                            println!(
                                "           {:?} {} ({} bytes)",
                                change.action, change.path, change.bytes
                            );
                        }
                    } else {
                        println!(
                            "ingest: {} created, {} updated, commit {}",
                            report.created, report.updated, report.commit_id
                        );
                        println!("        summary: {}", report.summary);

                        // Refresh the embeddings store so freshly-ingested pages
                        // are immediately searchable via `query` and the MCP
                        // `scriptorium_search` / `scriptorium_query` tools.
                        // `embed::reindex` is cache-aware (keyed by content_hash)
                        // so existing chunks are skipped — only new/changed
                        // content pays the embedding cost. Without this step the
                        // store stays stale until a manual `scriptorium reindex`,
                        // which makes new pages invisible to retrieval.
                        let store = open_store(&vault)?;
                        let embed_provider = build_provider(embed_provider_from(&cfg))?;
                        let embedded = scriptorium_core::embed::reindex(
                            &vault,
                            &store,
                            embed_provider.as_ref(),
                            &cfg.embeddings.model,
                        )
                        .await
                        .into_diagnostic()?;
                        if embedded > 0 {
                            println!("        embedded: {embedded} new chunk(s)");
                        }
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Command::Query {
                    question,
                    top_k,
                    provider,
                } => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    // Query needs TWO providers: one for the chat (answer generation)
                    // and one for embedding the question (retrieval). If --provider
                    // overrides, use it for chat only; embeddings always come from
                    // cfg.embeddings.provider.
                    let llm_provider =
                        build_provider(provider.unwrap_or_else(|| provider_from(&cfg)))?;
                    let embed_provider = build_provider(embed_provider_from(&cfg))?;
                    let store = open_store(&vault)?;
                    let report = query::query(
                        &vault,
                        &store,
                        llm_provider.as_ref(),
                        embed_provider.as_ref(),
                        &cfg.embeddings.model,
                        &question,
                        top_k,
                    )
                    .await
                    .into_diagnostic()?;
                    println!("{}\n", report.answer.answer);
                    if !report.cited_stems.is_empty() {
                        println!("cited: {}", report.cited_stems.join(", "));
                    }
                    if let Some(conf) = report.answer.confidence {
                        println!("confidence: {conf:.2}");
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Command::Reindex { provider } => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    // reindex uses the embeddings provider, not the chat provider.
                    // --provider here overrides the embeddings choice, not the chat.
                    let embed_provider =
                        build_provider(provider.unwrap_or_else(|| embed_provider_from(&cfg)))?;
                    let store = open_store(&vault)?;
                    let report = core::reindex::reindex_all(
                        &vault,
                        &store,
                        embed_provider.as_ref(),
                        &cfg.embeddings.model,
                    )
                    .await
                    .into_diagnostic()?;
                    println!("reindex: embedded {} chunk(s)", report.embeddings_written);
                    if report.index_updated {
                        println!("reindex: regenerated index.md");
                    } else {
                        println!("reindex: index.md already up to date");
                    }
                    print_lint_report(&report.lint);
                    Ok(ExitCode::SUCCESS)
                }
                Command::Lint { strict, fix } => {
                    let vault = open_vault(&vault_path)?;
                    if fix {
                        let fix_report = core::lint::fix::run(&vault).into_diagnostic()?;
                        if fix_report.is_noop() {
                            println!("lint --fix: nothing to fix");
                        } else {
                            println!(
                                "lint --fix: fixed {} issue(s) in commit {}",
                                fix_report.fixed.len(),
                                fix_report.commit_id.as_deref().unwrap_or("?")
                            );
                            for issue in &fix_report.fixed {
                                let path =
                                    issue.path.as_deref().map_or("?", camino::Utf8Path::as_str);
                                println!("  fixed: {path} [{}] {}", issue.rule, issue.message);
                            }
                        }
                        if !fix_report.skipped.is_empty() {
                            println!(
                                "lint --fix: {} issue(s) skipped (require manual review):",
                                fix_report.skipped.len()
                            );
                            for (issue, reason) in &fix_report.skipped {
                                let path =
                                    issue.path.as_deref().map_or("?", camino::Utf8Path::as_str);
                                println!(
                                    "  skipped: {path} [{}] {} — {reason}",
                                    issue.rule, issue.message
                                );
                            }
                        }
                        println!();
                    }
                    let report = core::lint::run(&vault).into_diagnostic()?;
                    print_lint_report(&report);
                    let exit = if report.has_errors() || (strict && !report.is_clean()) {
                        ExitCode::from(1)
                    } else {
                        ExitCode::SUCCESS
                    };
                    Ok(exit)
                }
                Command::Undo => {
                    let vault = open_vault(&vault_path)?;
                    undo(&vault)?;
                    println!("undo: reverted the most recent scriptorium commit");
                    Ok(ExitCode::SUCCESS)
                }
                Command::Doctor { json } => {
                    let vault = open_vault(&vault_path)?;
                    let store = open_store(&vault).ok();
                    let report = scriptorium_core::doctor::run_doctor(&vault, store.as_ref());
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&report)
                                .map_err(|e| miette!("json: {e}"))?
                        );
                    } else {
                        print_doctor_report(&report);
                    }
                    let exit = if report.has_failures() {
                        ExitCode::from(1)
                    } else {
                        ExitCode::SUCCESS
                    };
                    Ok(exit)
                }
                Command::Maintain { fix, provider } => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    let store = open_store(&vault)?;
                    let embed_provider =
                        build_provider(provider.unwrap_or_else(|| embed_provider_from(&cfg)))?;
                    let options = scriptorium_core::maintain::MaintainOptions { fix };
                    let report = scriptorium_core::maintain::maintain(
                        &vault,
                        &store,
                        Some(embed_provider.as_ref()),
                        &cfg.embeddings.model,
                        &options,
                    )
                    .await
                    .into_diagnostic()?;
                    let s = report.summary();
                    println!("Maintenance Report");
                    println!("==================");
                    println!("  Lint: {} error(s), {} warning(s)", s.errors, s.warnings);
                    println!("  Stale pages: {}", s.stale_pages);
                    println!("  Stale embeddings: {}", s.stale_embeddings);
                    println!("  Embedding coverage: {}/{}", s.embedded, s.total_pages);
                    if fix {
                        println!("  Auto-fixed: {}", s.auto_fixed);
                        println!("  Chunks re-embedded: {}", s.chunks_reembedded);
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Command::BulkIngest {
                    dir,
                    provider,
                    model,
                    fresh,
                    dry_run,
                } => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    let resolved_provider = provider.unwrap_or_else(|| provider_from(&cfg));
                    if let Some(ref m) = model {
                        // Use the resolved provider name (--provider flag or config
                        // default) so the right env var is set when --provider
                        // overrides the config.
                        set_model_env(provider_kind_name(resolved_provider), m);
                    }
                    let llm = build_provider(resolved_provider)?;
                    if fresh {
                        let cp = vault.meta_dir().join("bulk-ingest-checkpoint.json");
                        let _ = std::fs::remove_file(cp.as_std_path());
                    }
                    let options = scriptorium_core::bulk_ingest::BulkIngestOptions {
                        dry_run,
                        ..Default::default()
                    };
                    let report = scriptorium_core::bulk_ingest::bulk_ingest(
                        &vault,
                        llm.as_ref(),
                        &dir,
                        &options,
                        |cur, total, path| {
                            eprintln!("[{cur}/{total}] {}", path.display());
                        },
                    )
                    .await
                    .into_diagnostic()?;
                    println!("Bulk Ingest Report");
                    println!("==================");
                    println!("  Discovered: {}", report.total_discovered);
                    println!("  Skipped (checkpoint): {}", report.skipped_checkpoint);
                    println!(
                        "  Skipped (already interned): {}",
                        report.skipped_already_interned
                    );
                    println!("  Ingested: {}", report.ingested);
                    println!("  Failed: {}", report.failed.len());
                    for err in &report.failed {
                        eprintln!("    {} — {}", err.path.display(), err.error);
                    }
                    println!("  Elapsed: {:.1}s", report.elapsed.as_secs_f64());
                    Ok(ExitCode::SUCCESS)
                }
                Command::Bench {
                    json,
                    init,
                    provider,
                } => {
                    let vault = open_vault(&vault_path)?;
                    if init {
                        let suite =
                            scriptorium_core::bench::load_suite(&vault).into_diagnostic()?;
                        if suite.benchmarks.is_empty() {
                            scriptorium_core::bench::save_suite(&vault, &suite)
                                .into_diagnostic()?;
                            println!(
                                "Created empty benchmarks.json at {}",
                                vault.meta_dir().join("benchmarks.json")
                            );
                        } else {
                            println!(
                                "benchmarks.json already exists with {} cases.",
                                suite.benchmarks.len()
                            );
                        }
                        return Ok(ExitCode::SUCCESS);
                    }
                    let cfg = load_config(&vault);
                    let store = open_store(&vault)?;
                    let llm = build_provider(provider.unwrap_or_else(|| provider_from(&cfg)))?;
                    let embed = build_provider(embed_provider_from(&cfg))?;
                    let report = scriptorium_core::bench::run_benchmarks(
                        &vault,
                        &store,
                        embed.as_ref(),
                        llm.as_ref(),
                        &cfg.embeddings.model,
                    )
                    .await
                    .into_diagnostic()?;
                    if json {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&report)
                                .map_err(|e| miette!("json: {e}"))?
                        );
                    } else {
                        println!("Benchmark Report");
                        println!("================");
                        if report.results.is_empty() {
                            println!("  No benchmark cases defined. Run `scriptorium bench --init` first.");
                        } else {
                            for r in &report.results {
                                println!(
                            "  {}: P@{k}={p:.2} R={r:.2} F1={f:.2} MRR={m:.2} NDCG@{k}={n:.2}",
                            r.query,
                            k = r.k,
                            p = r.precision,
                            r = r.recall,
                            f = r.f1,
                            m = r.mrr,
                            n = r.ndcg,
                        );
                            }
                        }
                        println!();
                        println!("  Mean precision: {:.2}", report.mean_precision);
                        println!("  Mean recall:    {:.2}", report.mean_recall);
                        println!("  Mean F1:        {:.2}", report.mean_f1);
                        println!("  Mean MRR:       {:.2}", report.mean_mrr);
                        println!("  Mean NDCG:      {:.2}", report.mean_ndcg);
                        println!("  Coverage:       {:.0}%", report.coverage * 100.0);
                        println!("  Stale ratio:    {:.0}%", report.stale_ratio * 100.0);
                        println!("  Health score:   {:.1}/10", report.health_score);
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Command::Learn(sub) => {
                    let vault = open_vault(&vault_path)?;
                    match sub {
                        LearnCommand::List { n } => {
                            let entries = scriptorium_core::learnings::list_recent(&vault, n)
                                .into_diagnostic()?;
                            if entries.is_empty() {
                                println!("No learnings yet.");
                            } else {
                                for l in &entries {
                                    println!(
                                        "  [{:?}] {} — {} (conf: {}, {})",
                                        l.learning_type,
                                        l.key,
                                        l.insight,
                                        l.confidence,
                                        l.ts.format("%Y-%m-%d")
                                    );
                                }
                            }
                        }
                        LearnCommand::Search { query } => {
                            let entries = scriptorium_core::learnings::search(&vault, &query)
                                .into_diagnostic()?;
                            if entries.is_empty() {
                                println!("No matches for '{query}'.");
                            } else {
                                for l in &entries {
                                    println!("  [{:?}] {} — {}", l.learning_type, l.key, l.insight);
                                }
                            }
                        }
                        LearnCommand::Add { json } => {
                            let learning: scriptorium_core::learnings::Learning =
                                serde_json::from_str(&json)
                                    .map_err(|e| miette!("invalid JSON: {e}"))?;
                            scriptorium_core::learnings::capture(&vault, &learning)
                                .into_diagnostic()?;
                            println!("Captured: [{:?}] {}", learning.learning_type, learning.key);
                        }
                        LearnCommand::Prune => {
                            let pruned = scriptorium_core::learnings::prune_stale(&vault)
                                .into_diagnostic()?;
                            println!("Pruned {pruned} stale learning(s).");
                        }
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Command::Skill(sub) => {
                    let vault = open_vault(&vault_path)?;
                    match sub {
                        SkillCommand::List => {
                            let skills =
                                scriptorium_core::skills::list_skills(&vault).into_diagnostic()?;
                            if skills.is_empty() {
                                println!("No skills found. Run `scriptorium skill init` to scaffold defaults.");
                            } else {
                                for s in &skills {
                                    println!("  {} — {}", s.name, s.description);
                                }
                            }
                        }
                        SkillCommand::Show { name } => {
                            let skill = scriptorium_core::skills::load_skill(&vault, &name)
                                .into_diagnostic()?;
                            println!("{}", skill.content);
                        }
                        SkillCommand::Init => {
                            let written =
                                scriptorium_core::skills::init_skills(&vault).into_diagnostic()?;
                            if written == 0 {
                                println!("Skills already exist, nothing to do.");
                            } else {
                                println!(
                                    "Scaffolded {written} skill files in {}/",
                                    scriptorium_core::skills::skills_dir(&vault)
                                );
                            }
                        }
                    }
                    Ok(ExitCode::SUCCESS)
                }
                Command::Config => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    print_config(&cfg);
                    Ok(ExitCode::SUCCESS)
                }
                Command::Serve { provider } => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    // The MCP server needs both a chat provider (for ingest + query)
                    // and an embeddings provider (for search + query retrieval).
                    // --provider overrides the chat slot only; embeddings always
                    // come from cfg.embeddings.provider.
                    let llm_provider =
                        build_provider(provider.unwrap_or_else(|| provider_from(&cfg)))?;
                    let embed_provider = build_provider(embed_provider_from(&cfg))?;
                    let context = scriptorium_mcp::ServerContext {
                        vault,
                        llm_provider,
                        embed_provider,
                        embeddings_model: cfg.embeddings.model.clone(),
                    };
                    scriptorium_mcp::serve_stdio(context)
                        .await
                        .map_err(|e| miette!("mcp server: {e}"))?;
                    Ok(ExitCode::SUCCESS)
                }
                Command::Watch { provider } => {
                    let vault = open_vault(&vault_path)?;
                    let cfg = load_config(&vault);
                    let provider = build_provider(provider.unwrap_or_else(|| provider_from(&cfg)))?;
                    println!(
                        "watch: watching {}/sources/ — drop new files there to auto-ingest",
                        vault.root()
                    );
                    let hooks = if cfg.hooks == scriptorium_core::hooks::HooksConfig::default() {
                        None
                    } else {
                        Some(cfg.hooks.clone())
                    };
                    core::watch::watch(vault, provider, hooks)
                        .await
                        .into_diagnostic()?;
                    Ok(ExitCode::SUCCESS)
                }
                Command::Social(sub) => {
                    let vault = open_vault(&vault_path)?;
                    match sub {
                        SocialCommand::Facebook {
                            export_dirs,
                            categories,
                            provider,
                            model,
                            dry_run,
                        } => {
                            handle_social_facebook(
                                &vault,
                                &vault_path,
                                export_dirs,
                                categories,
                                provider,
                                model,
                                dry_run,
                            )
                            .await?;
                            Ok(ExitCode::SUCCESS)
                        }
                    }
                }
                // Already handled before vault resolution.
                Command::VaultMgmt(_)
                | Command::Init { .. }
                | Command::Hooks(_)
                | Command::Log(_)
                | Command::Trace(_) => unreachable!(),
            } // inner match
        } // outer `command =>` arm
    }
}

// ---------- social facebook ----------

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn handle_social_facebook(
    vault: &Vault,
    _vault_path: &Path,
    export_dirs: Vec<PathBuf>,
    categories: Option<Vec<String>>,
    provider: Option<ProviderKind>,
    model: Option<String>,
    dry_run: bool,
) -> Result<()> {
    use scriptorium_core::social::facebook::{self, Category};

    let cfg = load_config(vault);

    // Parse categories
    let cats = if let Some(cat_strs) = categories {
        let mut cats = Vec::new();
        for s in &cat_strs {
            match Category::from_str(s) {
                Some(c) => cats.push(c),
                None => {
                    return Err(miette!(
                        "unknown category: '{s}'. Options: messages, posts, comments, friends, search, events, groups"
                    ));
                }
            }
        }
        cats
    } else {
        vec![]
    };

    // ── Phase 1: Parse Facebook export ──────────────────────────────
    let phase1 = ProgressBar::new_spinner();
    phase1.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {prefix:.bold} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    phase1.set_prefix("[1/3]");
    phase1.set_message("Parsing Facebook export...");
    phase1.enable_steady_tick(std::time::Duration::from_millis(80));

    // Write to a persistent staging dir (inside vault's .scriptorium/)
    // so bulk-ingest can intern them. Using tempdir would drop files
    // before ingest finishes.
    let staging_dir = vault.meta_dir().as_std_path().join("facebook-staging");
    if staging_dir.exists() {
        let _ = std::fs::remove_dir_all(&staging_dir);
    }
    std::fs::create_dir_all(&staging_dir).map_err(|e| miette!("create staging dir: {e}"))?;

    let export_options = facebook::FacebookImportOptions {
        export_dirs,
        output_dir: staging_dir.clone(),
        categories: cats,
        dry_run,
    };

    let export_report = facebook::import(&export_options, |cur, total, name| {
        phase1.set_message(format!("[{cur}/{total}] {name}"));
    })
    .into_diagnostic()?;

    phase1.finish_with_message(format!(
        "Parsed {} categories, {} source files in {:.1}s",
        export_report.categories.len(),
        export_report.total_files_written,
        export_report.elapsed.as_secs_f64(),
    ));

    // Print category breakdown
    for cat in &export_report.categories {
        eprintln!(
            "       {} — {} file(s), {} item(s)",
            cat.category, cat.files_written, cat.items_processed
        );
    }

    if dry_run {
        eprintln!("\n  (dry run — nothing ingested)");
        return Ok(());
    }

    if export_report.total_files_written == 0 {
        eprintln!("  Nothing to ingest.");
        return Ok(());
    }

    // ── Phase 2: Ingest into vault ──────────────────────────────────
    let resolved_provider = provider.unwrap_or_else(|| provider_from(&cfg));
    if let Some(ref m) = model {
        set_model_env(provider_kind_name(resolved_provider), m);
    }
    let llm = build_provider(resolved_provider)?;

    let total_files = export_report.total_files_written as u64;
    let phase2 = ProgressBar::new(total_files);
    phase2.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} {prefix:.bold} [{bar:40.green/dim}] {pos}/{len} {msg} ({eta} remaining)"
        )
        .unwrap()
        .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
        .progress_chars("█▓▒░  "),
    );
    phase2.set_prefix("[2/3]");
    phase2.set_message("Ingesting...");
    phase2.enable_steady_tick(std::time::Duration::from_millis(80));

    let ingest_options = scriptorium_core::bulk_ingest::BulkIngestOptions {
        dry_run: false,
        ..Default::default()
    };

    // Open embeddings store + provider for retrieval-augmented ingestion.
    // Falls back gracefully if not available.
    let store = open_store(vault).ok();
    let embed_provider = build_provider(embed_provider_from(&cfg)).ok();

    let ingest_report = scriptorium_core::bulk_ingest::bulk_ingest_with_retrieval(
        vault,
        llm.as_ref(),
        &staging_dir,
        &ingest_options,
        |cur, _total, path| {
            phase2.set_position(cur as u64);
            let fname = path.file_name().unwrap_or_default().to_string_lossy();
            phase2.set_message(fname.to_string());
        },
        store.as_ref(),
        embed_provider
            .as_ref()
            .map(|p| p.as_ref() as &dyn LlmProvider),
        Some(&cfg.embeddings.model),
    )
    .await
    .into_diagnostic()?;

    phase2.finish_with_message(format!(
        "Ingested {} files in {:.1}s",
        ingest_report.ingested,
        ingest_report.elapsed.as_secs_f64(),
    ));

    // ── Phase 3: Reindex embeddings ─────────────────────────────────
    let phase3 = ProgressBar::new_spinner();
    phase3.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {prefix:.bold} {msg}")
            .unwrap()
            .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
    );
    phase3.set_prefix("[3/3]");
    phase3.set_message("Reindexing embeddings...");
    phase3.enable_steady_tick(std::time::Duration::from_millis(80));

    let store = open_store(vault)?;
    let embed_provider = build_provider(embed_provider_from(&cfg))?;
    let embedded = scriptorium_core::embed::reindex(
        vault,
        &store,
        embed_provider.as_ref(),
        &cfg.embeddings.model,
    )
    .await
    .into_diagnostic()?;

    phase3.finish_with_message(format!("Embedded {embedded} new chunk(s)"));

    // ── Summary ─────────────────────────────────────────────────────
    eprintln!();
    eprintln!("Facebook Import Complete");
    eprintln!("========================");
    eprintln!("  Sources generated: {}", export_report.total_files_written);
    eprintln!("  Pages ingested:    {}", ingest_report.ingested);
    eprintln!(
        "  Pages skipped:     {}",
        ingest_report.skipped_already_interned
    );
    eprintln!("  Failures:          {}", ingest_report.failed.len());
    eprintln!("  Chunks embedded:   {embedded}");
    if !ingest_report.failed.is_empty() {
        eprintln!("  Failed files:");
        for err in &ingest_report.failed {
            eprintln!("    {} — {}", err.path.display(), err.error);
        }
    }

    // Clean up staging dir
    let _ = std::fs::remove_dir_all(&staging_dir);

    Ok(())
}

// ---------- helpers ----------

/// Result of resolving the `Ingest { source, url }` arg pair into one
/// concrete file path. The optional `_resolved` field keeps the
/// `core::url_fetch::ResolvedSource` (and its tempdir) alive until this
/// struct is dropped, so the path remains valid for the duration of the
/// ingest call.
struct ResolvedIngestSource {
    path: PathBuf,
    /// Held to keep the URL-ingest tempdir alive; never read directly.
    _resolved: Option<core::url_fetch::ResolvedSource>,
}

/// Resolve the Ingest CLI args into a concrete source path. For local files,
/// returns the path directly with no tempdir. For URLs, delegates to
/// `core::url_fetch::fetch_to_tempfile` which handles fetch + Readability +
/// HTML→md + tempfile write in one call.
async fn resolve_ingest_source(
    source: Option<PathBuf>,
    url: Option<String>,
) -> Result<ResolvedIngestSource> {
    match (source, url) {
        (Some(path), None) => Ok(ResolvedIngestSource {
            path,
            _resolved: None,
        }),
        (None, Some(url)) => {
            println!("ingest: fetching {url}");
            let resolved = core::url_fetch::fetch_to_tempfile(&url)
                .await
                .map_err(|e| miette!("URL fetch failed: {e}"))?;
            println!(
                "ingest: extracted \"{}\" from {} ({} bytes, fetched {})",
                resolved.doc.title,
                resolved.doc.url,
                resolved.doc.markdown.len(),
                resolved.doc.fetched_at.format("%Y-%m-%dT%H:%M:%SZ")
            );
            let path = resolved.path().to_path_buf();
            Ok(ResolvedIngestSource {
                path,
                _resolved: Some(resolved),
            })
        }
        (None, None) => Err(miette!("must provide either <source> path or --url <url>")),
        // clap's `conflicts_with = "source"` should make this case unreachable
        // at parse time, but we still return a clean error rather than panic.
        (Some(_), Some(_)) => Err(miette!("cannot provide both <source> path and --url")),
    }
}

/// Resolve which vault to use:
/// 1. Explicit `-C <path>` flag
/// 2. Current directory, if it contains `.scriptorium/`
/// 3. Default vault from global config
/// 4. Error with guidance
fn resolve_vault_path(explicit: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(path);
    }
    let cwd = PathBuf::from(".");
    if scriptorium_core::vault::is_vault(&cwd) {
        return Ok(cwd);
    }
    let global = scriptorium_core::global_config::GlobalConfig::load().unwrap_or_default();
    if let Some(entry) = global.default_vault() {
        return Ok(entry.path.clone());
    }
    Err(miette!(
        "No vault found. Either:\n\
         - Run from inside a vault directory (has .scriptorium/)\n\
         - Pass -C <path>\n\
         - Register a default: scriptorium vault add <name> <path> && scriptorium vault default <name>"
    ))
}

/// Auto-register a vault in the global config. Best-effort — never fails
/// the calling command.
fn auto_register_vault(path: &std::path::Path) {
    let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let name = canonical
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unnamed")
        .to_string();
    if let Ok(mut global) = scriptorium_core::global_config::GlobalConfig::load() {
        global.register(name.clone(), canonical);
        if global.default.is_none() {
            global.default = Some(name);
        }
        let _ = global.save();
    }
}

fn handle_vault_command(sub: VaultCommand) -> Result<ExitCode> {
    let mut global = scriptorium_core::global_config::GlobalConfig::load().into_diagnostic()?;

    match sub {
        VaultCommand::List => {
            if global.vaults.is_empty() {
                println!("No vaults registered. Use `scriptorium vault add <name> <path>` to register one.");
            } else {
                for entry in &global.vaults {
                    let marker = if global.default.as_deref() == Some(&entry.name) {
                        "*"
                    } else {
                        " "
                    };
                    println!("{marker} {:<16} {}", entry.name, entry.path.display());
                }
            }
        }
        VaultCommand::Add { name, path } => {
            let canonical = std::fs::canonicalize(&path)
                .map_err(|e| miette!("cannot resolve path {}: {e}", path.display()))?;
            if !scriptorium_core::vault::is_vault(&canonical) {
                eprintln!(
                    "warning: {} does not look like a scriptorium vault (no .scriptorium/)",
                    canonical.display()
                );
            }
            let updated = global.register(name.clone(), canonical.clone());
            if global.default.is_none() {
                global.default = Some(name.clone());
            }
            global.save().into_diagnostic()?;
            if updated {
                println!("Updated vault '{name}' → {}", canonical.display());
            } else {
                println!("Registered vault '{name}' → {}", canonical.display());
            }
        }
        VaultCommand::Remove { name } => {
            if global.unregister(&name).is_some() {
                global.save().into_diagnostic()?;
                println!("Unregistered vault '{name}' (files on disk are untouched).");
            } else {
                return Err(miette!("no vault named '{name}' is registered"));
            }
        }
        VaultCommand::Default { name } => {
            if global.find(&name).is_none() {
                return Err(miette!(
                    "no vault named '{name}' is registered. Use `scriptorium vault add` first."
                ));
            }
            global.default = Some(name.clone());
            global.save().into_diagnostic()?;
            println!("Default vault set to '{name}'.");
        }
        VaultCommand::Show { name } => {
            let entry = if let Some(n) = &name {
                global
                    .find(n)
                    .ok_or_else(|| miette!("no vault named '{n}'"))?
            } else {
                global
                    .default_vault()
                    .ok_or_else(|| miette!("no default vault set"))?
            };
            println!("Name:    {}", entry.name);
            println!("Path:    {}", entry.path.display());
            let is_default = global.default.as_deref() == Some(&entry.name);
            println!("Default: {}", if is_default { "yes" } else { "no" });
            if let Ok(vault) = Vault::open(&entry.path) {
                let scan = vault.scan();
                let page_count = scan.as_ref().map_or(0, |r| r.pages.len());
                println!("Pages:   {page_count}");
                let cfg_path = vault.meta_dir().join("config.toml");
                if cfg_path.as_std_path().exists() {
                    let cfg = load_config(&vault);
                    println!("LLM:     {} / {}", cfg.llm.provider, cfg.llm.model);
                    println!(
                        "Embed:   {} / {}",
                        cfg.embeddings.provider, cfg.embeddings.model
                    );
                }
            } else {
                println!("Status:  NOT FOUND (directory missing or inaccessible)");
            }
        }
    }
    Ok(ExitCode::SUCCESS)
}

// ---------- hooks ----------

#[allow(clippy::unnecessary_wraps)]
fn handle_hooks_command(sub: HooksCommand) -> Result<ExitCode> {
    match sub {
        HooksCommand::Log { db, dry_run } => {
            if let Err(e) = hooks_log_inner(db, dry_run) {
                eprintln!("scriptorium hooks log: {e}");
            }
            Ok(ExitCode::SUCCESS)
        }
        HooksCommand::Migrate { jsonl, db, dry_run }
        | HooksCommand::Import { jsonl, db, dry_run } => hooks_import_inner(jsonl, db, dry_run),
        HooksCommand::MigrateBackfill { db, dry_run } => hooks_migrate_backfill_inner(db, dry_run),
        HooksCommand::Check {
            test,
            json,
            quick,
            settings,
            hooks_dir,
            vault,
        } => hooks_check_inner(test, json, quick, settings, hooks_dir, vault),
        HooksCommand::List {
            settings,
            hooks_dir,
        } => hooks_list_inner(settings, hooks_dir),
        #[cfg(feature = "dashboard")]
        HooksCommand::Dashboard { .. } => unreachable!("handled in run()"),
    }
}

fn hooks_check_inner(
    test: bool,
    json: bool,
    quick: bool,
    settings: Option<PathBuf>,
    hooks_dir: Option<PathBuf>,
    vault: Option<PathBuf>,
) -> Result<ExitCode> {
    let settings_path = settings.unwrap_or_else(default_settings_path);
    let hooks_path = hooks_dir.unwrap_or_else(default_hooks_dir);

    let mut report =
        scriptorium_core::hooks_check::check_session_hooks(&settings_path, &hooks_path);

    if !quick {
        if let Some(vault_path) = vault {
            let vault_report = scriptorium_core::hooks_check::check_vault_hooks(&vault_path);
            report.items.extend(vault_report.items);
        }

        if test {
            let dynamic_report =
                scriptorium_core::hooks_check::check_dynamic(&settings_path, &hooks_path);
            report.items.extend(dynamic_report.items);
        }
    }

    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&report).map_err(|e| miette!("json: {e}"))?
        );
    } else {
        print_hooks_check_report(&report);
    }

    #[allow(clippy::cast_sign_loss, clippy::cast_possible_truncation)]
    Ok(ExitCode::from(report.exit_code() as u8))
}

#[allow(clippy::unnecessary_wraps)]
fn hooks_list_inner(settings: Option<PathBuf>, hooks_dir: Option<PathBuf>) -> Result<ExitCode> {
    let settings_path = settings.unwrap_or_else(default_settings_path);
    let hooks_path = hooks_dir.unwrap_or_else(default_hooks_dir);

    let entries = scriptorium_core::hooks_check::list_hooks(&settings_path, &hooks_path);

    if entries.is_empty() {
        println!("No hooks found.");
    } else {
        println!(
            "{:<35} {:<18} {:>4} {:>7} {:>5} {:>7}",
            "NAME", "EVENT TYPE", "REG", "ON-DISK", "EXEC", "TIMEOUT"
        );
        println!("{}", "\u{2500}".repeat(82));
        for e in &entries {
            println!(
                "{:<35} {:<18} {:>4} {:>7} {:>5} {:>7}",
                e.name,
                e.event_type,
                if e.registered { "\u{2713}" } else { "\u{2717}" },
                if e.on_disk { "\u{2713}" } else { "\u{2717}" },
                if e.executable { "\u{2713}" } else { "\u{2717}" },
                e.timeout
                    .map_or_else(|| "\u{2014}".to_string(), |t| format!("{t}s")),
            );
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn hooks_import_inner(
    jsonl_path: Option<PathBuf>,
    db_path: Option<PathBuf>,
    dry_run: bool,
) -> Result<ExitCode> {
    let jsonl = jsonl_path.unwrap_or_else(default_hooks_jsonl_path);
    let db = db_path.unwrap_or_else(default_hooks_db_path);

    if !jsonl.exists() {
        return Err(miette!(
            "JSONL file not found: {}\n\
             Use --jsonl <path> to specify a different file.",
            jsonl.display()
        ));
    }

    if let Some(parent) = db.parent() {
        std::fs::create_dir_all(parent).into_diagnostic()?;
    }

    eprintln!("source: {}", jsonl.display());
    eprintln!("target: {}", db.display());
    if dry_run {
        eprintln!("mode:   dry-run (validate only)");
    }

    let store = scriptorium_core::hooks_store::HooksStore::open(&db)
        .map_err(|e| miette!("open hooks db: {e}"))?;

    let report = store
        .import_jsonl(&jsonl, dry_run)
        .map_err(|e| miette!("import: {e}"))?;

    eprintln!();
    println!(
        "Total: {} | Imported: {} | Duplicates: {} | Malformed: {} | Privacy: {}",
        report.total_lines,
        report.imported,
        report.skipped_duplicate,
        report.skipped_malformed,
        report.privacy_flagged,
    );

    if !report.errors.is_empty() {
        eprintln!();
        for err in &report.errors {
            eprintln!("  error: {err}");
        }
    }

    Ok(ExitCode::SUCCESS)
}

fn hooks_migrate_backfill_inner(db_path: Option<PathBuf>, dry_run: bool) -> Result<ExitCode> {
    let db = db_path.unwrap_or_else(default_hooks_db_path);
    if let Some(parent) = db.parent() {
        std::fs::create_dir_all(parent).into_diagnostic()?;
    }
    eprintln!("target: {}", db.display());
    if dry_run {
        eprintln!("mode:   dry-run (no writes)");
    }

    let store = scriptorium_core::telemetry::TelemetryStore::open(&db)
        .map_err(|e| miette!("open telemetry store: {e}"))?;

    let report = scriptorium_core::telemetry::backfill_hook_events(&store, dry_run)
        .map_err(|e| miette!("backfill: {e}"))?;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|e| miette!("json: {e}"))?
    );
    Ok(ExitCode::SUCCESS)
}

fn hooks_log_inner(
    db_path: Option<PathBuf>,
    dry_run_flag: bool,
) -> std::result::Result<(), String> {
    let raw = std::io::read_to_string(std::io::stdin()).map_err(|e| format!("read stdin: {e}"))?;

    if raw.trim().is_empty() {
        return Ok(());
    }

    let parsed: serde_json::Value =
        serde_json::from_str(&raw).map_err(|e| format!("parse JSON: {e}"))?;

    let event = map_raw_to_hook_event(&parsed, &raw)?;

    let is_dry_run = dry_run_flag || std::env::var("SCRIPTORIUM_DRY_RUN").is_ok_and(|v| v == "1");
    if is_dry_run {
        return Ok(());
    }

    let path = db_path.unwrap_or_else(default_hooks_db_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("create dir {}: {e}", parent.display()))?;
    }

    let store = scriptorium_core::hooks_store::HooksStore::open(&path)
        .map_err(|e| format!("open hooks db: {e}"))?;

    store
        .insert_event_idempotent(&event)
        .map_err(|e| format!("insert event: {e}"))?;

    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
fn map_raw_to_hook_event(
    v: &serde_json::Value,
    raw: &str,
) -> std::result::Result<scriptorium_core::hooks_store::HookEvent, String> {
    use scriptorium_core::hooks_store::{HookEvent, HooksStore};

    let str_field =
        |key: &str| -> Option<String> { v.get(key).and_then(|val| val.as_str()).map(String::from) };
    let int_field = |key: &str| -> Option<i32> {
        v.get(key)
            .and_then(serde_json::Value::as_i64)
            .map(|n| n as i32)
    };

    let scored = v.get("scored");

    let score = scored
        .and_then(|s| s.get("score"))
        .and_then(serde_json::Value::as_i64)
        .map(|n| n as i32)
        .or_else(|| int_field("score"));

    let signals = scored
        .and_then(|s| s.get("signals"))
        .or_else(|| v.get("signals"))
        .map(|val| {
            if val.is_string() {
                val.as_str().unwrap_or_default().to_string()
            } else {
                serde_json::to_string(val).unwrap_or_default()
            }
        });

    let metrics = scored
        .and_then(|s| s.get("metrics"))
        .or_else(|| v.get("metrics"))
        .map(|val| {
            if val.is_string() {
                val.as_str().unwrap_or_default().to_string()
            } else {
                serde_json::to_string(val).unwrap_or_default()
            }
        });

    let hook_type = str_field("hook_type").unwrap_or_else(|| {
        infer_hook_type(
            &str_field("source").unwrap_or_default(),
            &str_field("action").unwrap_or_default(),
        )
    });

    let session_id =
        str_field("session_id").ok_or_else(|| "missing required field: session_id".to_string())?;

    let ts = str_field("ts").unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

    Ok(HookEvent {
        id: None,
        ts,
        session_id,
        parent_session_id: str_field("parent_session_id"),
        hook_type,
        source: str_field("source"),
        cwd: str_field("cwd"),
        score,
        threshold: int_field("threshold"),
        signals,
        metrics,
        agent_type: str_field("agent_type"),
        privacy_veto: str_field("privacy_veto"),
        decision: str_field("decision"),
        peak_turn_score: int_field("peak_turn_score"),
        session_aggregate_score: int_field("session_aggregate_score"),
        final_score: int_field("final_score"),
        turn_count: int_field("turn_count"),
        subagent_count: int_field("subagent_count"),
        raw_json: Some(raw.to_string()),
        raw_json_hash: HooksStore::hash_raw(raw),
    })
}

fn infer_hook_type(source: &str, action: &str) -> String {
    let s = source.to_lowercase();
    let a = action.to_lowercase();
    if s.contains("subagent") || a.contains("subagent") {
        "subagent_stop".to_string()
    } else if s.contains("session") && (s.contains("end") || s.contains("stop")) {
        "session_end".to_string()
    } else {
        "stop".to_string()
    }
}

fn default_hooks_db_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".scriptorium")
        .join("hooks.sqlite")
}

fn default_hooks_jsonl_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".claude")
        .join("artifacts")
        .join("scriptorium-classifier.jsonl")
}

fn default_settings_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".claude").join("settings.json")
}

fn default_hooks_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join("dotfiles")
        .join("claude")
        .join("hooks")
}

fn print_hooks_check_report(report: &scriptorium_core::hooks_check::HooksCheckReport) {
    use scriptorium_core::hooks_check::CheckStatus;
    println!("Hooks Health Check");
    println!("==================");
    for item in &report.items {
        let tag = match item.status {
            CheckStatus::Pass => "  [PASS]",
            CheckStatus::Warn => "  [WARN]",
            CheckStatus::Fail => "  [FAIL]",
            CheckStatus::Info => "  [INFO]",
        };
        println!("{tag} [{}] {}", item.name, item.message);
    }
    if report.has_failures() {
        println!("\nResult: FAILURES detected (exit 2)");
    } else if report.has_warnings() {
        println!("\nResult: Warnings found (exit 1)");
    } else {
        println!("\nResult: All checks passed (exit 0)");
    }
}

/// Map a `ProviderKind` to the string name expected by `set_model_env`.
fn provider_kind_name(kind: ProviderKind) -> &'static str {
    match kind {
        ProviderKind::Claude => "claude",
        ProviderKind::Openai => "openai",
        ProviderKind::Gemini => "gemini",
        ProviderKind::Ollama => "ollama",
        ProviderKind::Mock => "mock",
    }
}

/// Set the provider-specific model env var so `from_env()` picks it up.
fn set_model_env(provider_name: &str, model: &str) {
    let var = match provider_name {
        "claude" => "ANTHROPIC_MODEL",
        "openai" => "OPENAI_MODEL",
        "gemini" | "google" => "GEMINI_MODEL",
        "ollama" => "OLLAMA_MODEL",
        _ => return,
    };
    std::env::set_var(var, model);
}

fn open_vault(root: &std::path::Path) -> Result<Vault> {
    Vault::open(root).into_diagnostic()
}

fn load_config(vault: &Vault) -> Config {
    let path = vault.meta_dir().join("config.toml");
    let Ok(text) = std::fs::read_to_string(path.as_std_path()) else {
        return Config::default();
    };
    toml::from_str(&text).unwrap_or_default()
}

fn provider_from(cfg: &Config) -> ProviderKind {
    provider_kind_from_string(&cfg.llm.provider)
}

fn embed_provider_from(cfg: &Config) -> ProviderKind {
    provider_kind_from_string(&cfg.embeddings.provider)
}

fn provider_kind_from_string(s: &str) -> ProviderKind {
    match s {
        "claude" => ProviderKind::Claude,
        "openai" => ProviderKind::Openai,
        "gemini" | "google" => ProviderKind::Gemini,
        "ollama" => ProviderKind::Ollama,
        _ => ProviderKind::Mock,
    }
}

fn build_provider(kind: ProviderKind) -> Result<Arc<dyn LlmProvider>> {
    let provider: Arc<dyn LlmProvider> = match kind {
        ProviderKind::Claude => {
            let cfg = ClaudeConfig::from_env().map_err(|e| miette!("claude config: {e}"))?;
            Arc::new(ClaudeProvider::new(cfg).map_err(|e| miette!("claude init: {e}"))?)
        }
        ProviderKind::Openai => {
            let cfg = OpenAiConfig::from_env().map_err(|e| miette!("openai config: {e}"))?;
            Arc::new(OpenAiProvider::new(cfg).map_err(|e| miette!("openai init: {e}"))?)
        }
        ProviderKind::Gemini => {
            let cfg = GeminiConfig::from_env().map_err(|e| miette!("gemini config: {e}"))?;
            Arc::new(GeminiProvider::new(cfg).map_err(|e| miette!("gemini init: {e}"))?)
        }
        ProviderKind::Ollama => Arc::new(
            OllamaProvider::new(OllamaConfig::default())
                .map_err(|e| miette!("ollama init: {e}"))?,
        ),
        ProviderKind::Mock => Arc::new(MockProvider::constant("{}")),
    };
    Ok(provider)
}

fn open_store(vault: &Vault) -> Result<EmbeddingsStore> {
    let meta = vault.meta_dir();
    std::fs::create_dir_all(meta.as_std_path()).into_diagnostic()?;
    let path = meta.join("embeddings.sqlite");
    EmbeddingsStore::open(path.as_std_path()).into_diagnostic()
}

const TPL_CLAUDE_MD: &str = include_str!("../../../templates/CLAUDE.md");
const TPL_INDEX_MD: &str = include_str!("../../../templates/index.md");
const TPL_LOG_MD: &str = include_str!("../../../templates/log.md");
const TPL_GITIGNORE: &str = include_str!("../../../templates/gitignore");
const TPL_CONFIG: &str = include_str!("../../../templates/config.toml");

fn init_vault(target: &std::path::Path) -> Result<()> {
    // Create directory structure.
    for sub in [
        "",
        "wiki/concepts",
        "wiki/entities",
        "wiki/topics",
        "sources/articles",
        "sources/data",
        ".scriptorium",
    ] {
        std::fs::create_dir_all(target.join(sub)).into_diagnostic()?;
    }
    // Write starter files, but do not clobber existing ones.
    let writes = [
        ("CLAUDE.md", TPL_CLAUDE_MD),
        ("index.md", TPL_INDEX_MD),
        ("log.md", TPL_LOG_MD),
        (".gitignore", TPL_GITIGNORE),
        (".scriptorium/config.toml", TPL_CONFIG),
    ];
    for (rel, content) in writes {
        let path = target.join(rel);
        if !path.exists() {
            std::fs::write(&path, content).into_diagnostic()?;
        }
    }
    // Initialize a git repo and commit the starter files.
    core::git::open_or_init(target).into_diagnostic()?;
    let paths: Vec<PathBuf> = writes.iter().map(|(r, _)| target.join(r)).collect();
    core::git::commit_paths(target, &paths, "scriptorium: init vault").into_diagnostic()?;
    Ok(())
}

fn undo(vault: &Vault) -> Result<()> {
    // Shell out to `git revert --no-edit HEAD`. Implementing revert manually
    // with git2 is surprisingly tricky (merge-base resolution + tree diffing);
    // delegating to the git CLI is shorter and matches what the user would
    // type by hand.
    let status = std::process::Command::new("git")
        .args(["revert", "--no-edit", "HEAD"])
        .current_dir(vault.root().as_std_path())
        .status()
        .into_diagnostic()?;
    if !status.success() {
        return Err(miette!("git revert failed with status {:?}", status.code()));
    }
    Ok(())
}

fn print_config(cfg: &Config) {
    println!("llm:");
    println!("  provider = {}", cfg.llm.provider);
    println!("  model    = {}", cfg.llm.model);
    println!("  timeout  = {}s", cfg.llm.timeout_secs);
    println!("embeddings:");
    println!("  provider = {}", cfg.embeddings.provider);
    println!("  model    = {}", cfg.embeddings.model);
    println!("git:");
    println!("  auto_commit = {}", cfg.git.auto_commit);
    println!("  auto_init   = {}", cfg.git.auto_init);
}

fn print_lint_report(report: &core::LintReport) {
    if report.is_clean() {
        println!("lint: clean ✓");
        return;
    }
    let errors = report.count_by_severity(Severity::Error);
    let warnings = report.count_by_severity(Severity::Warning);
    let infos = report.count_by_severity(Severity::Info);
    for issue in &report.issues {
        let sev = match issue.severity {
            Severity::Error => "error",
            Severity::Warning => "warn",
            Severity::Info => "info",
        };
        match (&issue.path, &issue.page) {
            (Some(path), _) => println!("{sev}: {} [{}] {}", path, issue.rule, issue.message),
            _ => println!("{sev}: [{}] {}", issue.rule, issue.message),
        }
    }
    println!("lint: {errors} error(s), {warnings} warning(s), {infos} info");
}

fn print_doctor_report(report: &scriptorium_core::doctor::DoctorReport) {
    use scriptorium_core::doctor::CheckStatus;
    println!("Scriptorium Health Check");
    println!("========================");
    for check in &report.checks {
        let tag = match check.status {
            CheckStatus::Ok => "  [OK]  ",
            CheckStatus::Warn => "  [WARN]",
            CheckStatus::Fail => "  [FAIL]",
        };
        println!("{tag} {}: {}", check.name, check.message);
    }
    let summary = match report.status {
        scriptorium_core::doctor::OverallStatus::Healthy => "All checks passed.",
        scriptorium_core::doctor::OverallStatus::Degraded => "Some warnings found.",
        scriptorium_core::doctor::OverallStatus::Unhealthy => "Failures detected!",
    };
    println!("\n{summary}");
}
