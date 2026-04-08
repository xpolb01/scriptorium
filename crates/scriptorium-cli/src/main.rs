//! Scriptorium CLI entrypoint.
//!
//! Subcommands are thin shims over [`scriptorium_core`] operations. Each one
//! parses args, builds the requested core call, and renders results. Error
//! rendering uses `miette` so library `thiserror` errors pretty-print with
//! source context.

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};
use miette::{miette, IntoDiagnostic, Result};
use scriptorium_core::config::Config;
use scriptorium_core::embed::EmbeddingsStore;
use scriptorium_core::lint::Severity;
use scriptorium_core::llm::{
    ClaudeConfig, ClaudeProvider, GeminiConfig, GeminiProvider, LlmProvider, MockProvider,
    OllamaConfig, OllamaProvider, OpenAiConfig, OpenAiProvider,
};
use scriptorium_core::{self as core, ingest, query, Vault};

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

    /// Show the resolved config for this vault.
    Config,

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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "scriptorium=info".into()),
        )
        .init();

    let cli = Cli::parse();
    match run(cli).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err:?}");
            ExitCode::from(2)
        }
    }
}

#[allow(clippy::too_many_lines)] // one arm per subcommand; flat is clearer
async fn run(cli: Cli) -> Result<ExitCode> {
    // Resolve the vault path once up front so match arms can destructure
    // `cli.command` without borrow-checker conflicts.
    let vault_path = cli.vault.clone().unwrap_or_else(|| PathBuf::from("."));
    match cli.command {
        Command::Init { path } => {
            let target = path.unwrap_or(vault_path);
            init_vault(&target)?;
            println!("scriptorium init: scaffolded vault at {}", target.display());
            Ok(ExitCode::SUCCESS)
        }
        Command::Ingest {
            source,
            url,
            provider,
            dry_run,
        } => {
            let vault = open_vault(&vault_path)?;
            let cfg = load_config(&vault);
            let provider = build_provider(provider.unwrap_or_else(|| provider_from(&cfg)))?;
            // Resolve to a concrete file path. URL ingest fetches, runs
            // Readability, converts to markdown, writes a tempfile, and the
            // regular file-ingest path takes over. The returned struct holds
            // the tempdir alive until end of scope.
            let resolved = resolve_ingest_source(source, url).await?;
            let report = ingest::ingest_with_options(
                &vault,
                provider.as_ref(),
                &resolved.path,
                ingest::IngestOptions { dry_run },
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
            let llm_provider = build_provider(provider.unwrap_or_else(|| provider_from(&cfg)))?;
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
                        let path = issue.path.as_deref().map_or("?", camino::Utf8Path::as_str);
                        println!("  fixed: {path} [{}] {}", issue.rule, issue.message);
                    }
                }
                if !fix_report.skipped.is_empty() {
                    println!(
                        "lint --fix: {} issue(s) skipped (require manual review):",
                        fix_report.skipped.len()
                    );
                    for (issue, reason) in &fix_report.skipped {
                        let path = issue.path.as_deref().map_or("?", camino::Utf8Path::as_str);
                        println!("  skipped: {path} [{}] {} — {reason}", issue.rule, issue.message);
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
            let llm_provider = build_provider(provider.unwrap_or_else(|| provider_from(&cfg)))?;
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
            core::watch::watch(vault, provider)
                .await
                .into_diagnostic()?;
            Ok(ExitCode::SUCCESS)
        }
    }
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
        (None, None) => Err(miette!(
            "must provide either <source> path or --url <url>"
        )),
        // clap's `conflicts_with = "source"` should make this case unreachable
        // at parse time, but we still return a clean error rather than panic.
        (Some(_), Some(_)) => Err(miette!(
            "cannot provide both <source> path and --url"
        )),
    }
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
