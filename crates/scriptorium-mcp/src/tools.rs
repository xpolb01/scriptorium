//! Tool definitions exposed via the MCP `tools/list` and `tools/call`
//! methods.
//!
//! Each tool is a thin wrapper over an operation in [`scriptorium_core`]:
//!
//! | Tool                       | Operation                    |
//! |----------------------------|------------------------------|
//! | `scriptorium_ingest`       | `ingest::ingest`             |
//! | `scriptorium_query`        | `query::query`               |
//! | `scriptorium_lint`         | `lint::run`                  |
//! | `scriptorium_list_pages`   | `Vault::scan`                |
//! | `scriptorium_read_page`    | `Page::read` (via Vault)     |
//! | `scriptorium_write_page`   | `VaultTx::write_page + commit` |
//! | `scriptorium_search`       | `EmbeddingsStore::search`    |
//! | `scriptorium_log_tail`     | tail of `log.md`             |
//!
//! Every tool has a JSON Schema for its input so the MCP client (Claude
//! Code) can validate arguments before invoking. Path-taking tools reject
//! absolute paths and `..` components — writing outside the vault root is
//! always a `ToolError::InvalidArgs`.

use std::fmt::Write as _;
use std::path::PathBuf;

use camino::{Utf8Path, Utf8PathBuf};
use scriptorium_core::embed::EmbeddingsStore;
use scriptorium_core::{self as core, ingest, query, Page};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::server::ServerContext;

#[derive(Debug)]
pub enum ToolError {
    NotFound(String),
    InvalidArgs(String),
    Failed(String),
}

impl From<serde_json::Error> for ToolError {
    fn from(err: serde_json::Error) -> Self {
        Self::InvalidArgs(format!("argument parse: {err}"))
    }
}

pub struct ToolRegistry {
    tools: Vec<ToolSpec>,
}

struct ToolSpec {
    name: &'static str,
    description: &'static str,
    input_schema: Value,
}

impl ToolRegistry {
    pub fn new() -> Self {
        Self {
            tools: all_tool_specs(),
        }
    }

    pub fn describe_all(&self) -> Vec<Value> {
        self.tools
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": t.input_schema,
                })
            })
            .collect()
    }

    pub async fn invoke(
        &self,
        name: &str,
        args: Value,
        ctx: &ServerContext,
    ) -> Result<String, ToolError> {
        match name {
            "scriptorium_ingest" => ingest_tool(args, ctx).await,
            "scriptorium_query" => query_tool(args, ctx).await,
            "scriptorium_lint" => lint_tool(ctx),
            "scriptorium_list_pages" => list_pages_tool(ctx),
            "scriptorium_read_page" => read_page_tool(args, ctx),
            "scriptorium_write_page" => write_page_tool(args, ctx),
            "scriptorium_search" => search_tool(args, ctx).await,
            "scriptorium_log_tail" => log_tail_tool(args, ctx),
            "scriptorium_doctor" => doctor_tool(ctx),
            "scriptorium_maintain" => maintain_tool(args, ctx).await,
            "scriptorium_skill_list" => skill_list_tool(ctx),
            "scriptorium_skill_read" => skill_read_tool(args, ctx),
            "scriptorium_learn_capture" => learn_capture_tool(args, ctx),
            "scriptorium_learn_search" => learn_search_tool(args, ctx),
            "scriptorium_learn_retrieve" => learn_retrieve_tool(args, ctx),
            "scriptorium_bench" => bench_tool(ctx).await,
            other => Err(ToolError::NotFound(other.to_string())),
        }
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------- tool specs ----------

#[allow(clippy::too_many_lines)]
fn all_tool_specs() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "scriptorium_ingest",
            description: "Ingest a source into the vault. Provide either `source` (absolute path to a local file) or `url` (HTTP URL to fetch, run through Readability, and convert to markdown) — exactly one is required. Returns the commit id, or a dry-run diff if `dry_run` is true.",
            // IMPORTANT: do NOT express the "source XOR url" constraint using
            // `oneOf` / `anyOf` / `allOf` at the top level of this schema.
            // The Anthropic API's `tools.*.custom.input_schema` rejects those
            // combinators at the top level with a 400 that POISONS the tool
            // registry — every subsequent API call in the session 400s until
            // the tool is removed. The XOR is already enforced at runtime by
            // `ingest_tool` which returns a clear InvalidArgs error on
            // (None, None) and (Some, Some). Keep the schema flat.
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source":  {"type": "string", "description": "Absolute path to a local source file. Mutually exclusive with `url`; exactly one of `source` or `url` is required."},
                    "url":     {"type": "string", "description": "URL to fetch and ingest as if it were a local file. Mutually exclusive with `source`; exactly one of `source` or `url` is required."},
                    "dry_run": {"type": "boolean", "default": false, "description": "If true, stage the ingest, return the diff, and do not commit."}
                }
            }),
        },
        ToolSpec {
            name: "scriptorium_query",
            description: "Answer a question against the vault with citations.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "top_k":    {"type": "integer", "default": 5, "minimum": 1, "maximum": 50}
                },
                "required": ["question"]
            }),
        },
        ToolSpec {
            name: "scriptorium_lint",
            description: "Run mechanical lint rules and return the report.",
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "scriptorium_list_pages",
            description: "List all wiki pages with their titles, paths, and tags.",
            input_schema: json!({"type": "object", "properties": {}}),
        },
        ToolSpec {
            name: "scriptorium_read_page",
            description: "Read a wiki page as markdown. Path must be vault-relative.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Vault-relative path, e.g. wiki/concepts/foo.md"}
                },
                "required": ["path"]
            }),
        },
        ToolSpec {
            name: "scriptorium_write_page",
            description: "Create or update a wiki page. Path is vault-relative; absolute or `..` paths are rejected.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "path":    {"type": "string"},
                    "content": {"type": "string", "description": "Full markdown including frontmatter"},
                    "message": {"type": "string", "description": "Git commit message"}
                },
                "required": ["path", "content"]
            }),
        },
        ToolSpec {
            name: "scriptorium_search",
            description: "Semantic top-k search over the embeddings store.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "top_k": {"type": "integer", "default": 5, "minimum": 1, "maximum": 50}
                },
                "required": ["query"]
            }),
        },
        ToolSpec {
            name: "scriptorium_log_tail",
            description: "Return the last N lines of log.md.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "n": {"type": "integer", "default": 20, "minimum": 1, "maximum": 200}
                }
            }),
        },
        ToolSpec {
            name: "scriptorium_maintain",
            description: "Run maintenance tasks: lint, stale page detection, \
                          embedding coverage, and optionally auto-fix safe \
                          issues (re-embed stale pages, fix bad timestamps).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "fix": {
                        "type": "boolean",
                        "default": false,
                        "description": "Auto-fix safe issues (re-embed stale, fix timestamps)"
                    }
                }
            }),
        },
        ToolSpec {
            name: "scriptorium_doctor",
            description: "Run health checks on the vault: git repo, schema, \
                          embeddings coverage, broken links, git status. \
                          No LLM required. Returns a structured report.",
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolSpec {
            name: "scriptorium_skill_list",
            description: "List all available skills with their names and descriptions. \
                          Skills are markdown instruction sets that teach agents how \
                          to use scriptorium workflows.",
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolSpec {
            name: "scriptorium_skill_read",
            description: "Read the full content of a named skill (e.g. 'ingest', \
                          'query', 'maintain'). Returns the SKILL.md markdown.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Skill name (e.g. 'ingest', 'query', 'maintain')"
                    }
                },
                "required": ["name"]
            }),
        },
        ToolSpec {
            name: "scriptorium_bench",
            description: "Run retrieval quality benchmarks: precision@k, recall, MRR, \
                          and a composite health score. Requires benchmark cases in \
                          .scriptorium/benchmarks.json.",
            input_schema: json!({
                "type": "object",
                "properties": {}
            }),
        },
        ToolSpec {
            name: "scriptorium_learn_capture",
            description: "Capture a learning (pattern, pitfall, correction, etc.) \
                          into the self-learning journal. Use after observing \
                          something that would save 5+ minutes in a future session.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "skill": { "type": "string", "description": "Which skill (ingest, query, maintain, etc.)" },
                    "type": { "type": "string", "enum": ["pattern", "pitfall", "preference", "correction", "domain_knowledge"] },
                    "key": { "type": "string", "description": "Short kebab-case key for dedup" },
                    "insight": { "type": "string", "description": "What was learned" },
                    "confidence": { "type": "integer", "minimum": 1, "maximum": 10, "default": 7 },
                    "source": { "type": "string", "enum": ["observed", "user_stated", "inferred"], "default": "observed" },
                    "tags": { "type": "array", "items": { "type": "string" } },
                    "files": { "type": "array", "items": { "type": "string" } }
                },
                "required": ["skill", "type", "key", "insight"]
            }),
        },
        ToolSpec {
            name: "scriptorium_learn_search",
            description: "Search the learning journal by keyword. Returns matching \
                          entries with their confidence scores.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Search keyword (empty = list recent)" }
                }
            }),
        },
        ToolSpec {
            name: "scriptorium_learn_retrieve",
            description: "Retrieve learnings relevant to specific tags, sorted by \
                          effective confidence. Used to inject context into prompts.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "tags": { "type": "array", "items": { "type": "string" }, "description": "Tags to match against" },
                    "limit": { "type": "integer", "default": 5, "minimum": 1, "maximum": 20 }
                }
            }),
        },
    ]
}

// ---------- tool handlers ----------

#[derive(Debug, Deserialize)]
struct IngestArgs {
    /// Absolute path to a local source file. Mutually exclusive with `url`.
    #[serde(default)]
    source: Option<String>,
    /// URL to fetch, extract main content from via Readability, and ingest.
    /// Mutually exclusive with `source`.
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    dry_run: bool,
}

async fn ingest_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let args: IngestArgs = serde_json::from_value(args)?;

    // Resolve `source` xor `url` to a concrete file path. URL ingest fetches,
    // runs Readability + HTML→md, writes to a tempfile inside `_resolved`'s
    // tempdir; the tempdir lives until the end of this function so the path
    // stays valid through the ingest call.
    let (source_path, _resolved): (PathBuf, Option<core::url_fetch::ResolvedSource>) =
        match (args.source, args.url) {
            (Some(path), None) => (PathBuf::from(path), None),
            (None, Some(url)) => {
                let resolved = core::url_fetch::fetch_to_tempfile(&url)
                    .await
                    .map_err(|e| ToolError::Failed(format!("URL fetch failed: {e}")))?;
                (resolved.path().to_path_buf(), Some(resolved))
            }
            (None, None) => {
                return Err(ToolError::InvalidArgs(
                    "must provide either `source` path or `url`".into(),
                ))
            }
            (Some(_), Some(_)) => {
                return Err(ToolError::InvalidArgs(
                    "cannot provide both `source` and `url` — they are mutually exclusive".into(),
                ))
            }
        };

    let report = ingest::ingest_with_options(
        &ctx.vault,
        ctx.llm_provider.as_ref(),
        &source_path,
        ingest::IngestOptions {
            dry_run: args.dry_run,
            hooks: None,
        },
    )
    .await
    .map_err(|e| ToolError::Failed(format!("ingest: {e}")))?;
    if args.dry_run {
        let mut out = format!(
            "ingest DRY RUN: would create {} page(s), update {} — nothing written.\nsummary: {}\nchanges:\n",
            report.created, report.updated, report.summary
        );
        for change in &report.dry_run_diff {
            // write! against a String is infallible.
            let _ = writeln!(
                out,
                "  {:?} {} ({} bytes)",
                change.action, change.path, change.bytes
            );
        }
        Ok(out)
    } else {
        // Refresh the embeddings store so the freshly-ingested pages are
        // immediately searchable via `scriptorium_search` and queryable
        // via `scriptorium_query`. `embed::reindex` is cache-aware
        // (content-hash keyed) so only new/changed chunks are re-embedded
        // — milliseconds when unchanged. Without this step, ingested
        // pages stay invisible to retrieval until a manual `reindex`.
        let store = open_store(ctx)?;
        let embedded = scriptorium_core::embed::reindex(
            &ctx.vault,
            &store,
            ctx.embed_provider.as_ref(),
            &ctx.embeddings_model,
        )
        .await
        .map_err(|e| ToolError::Failed(format!("post-ingest reindex: {e}")))?;
        let embed_note = if embedded > 0 {
            format!(" (embedded {embedded} new chunk(s))")
        } else {
            String::new()
        };
        Ok(format!(
            "ingested {}: {} created, {} updated, commit {}{}\nsummary: {}",
            report.source,
            report.created,
            report.updated,
            report.commit_id,
            embed_note,
            report.summary
        ))
    }
}

#[derive(Debug, Deserialize)]
struct QueryArgs {
    question: String,
    #[serde(default = "default_top_k")]
    top_k: usize,
}

fn default_top_k() -> usize {
    5
}

async fn query_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let args: QueryArgs = serde_json::from_value(args)?;
    let store = open_store(ctx)?;
    let report = query::query(
        &ctx.vault,
        &store,
        ctx.llm_provider.as_ref(),
        ctx.embed_provider.as_ref(),
        &ctx.embeddings_model,
        &args.question,
        args.top_k,
    )
    .await
    .map_err(|e| ToolError::Failed(format!("query: {e}")))?;
    let mut out = report.answer.answer;
    if !report.cited_stems.is_empty() {
        out.push_str("\n\ncited: ");
        out.push_str(&report.cited_stems.join(", "));
    }
    Ok(out)
}

fn lint_tool(ctx: &ServerContext) -> Result<String, ToolError> {
    let report =
        core::lint::run(&ctx.vault).map_err(|e| ToolError::Failed(format!("lint: {e}")))?;
    Ok(serde_json::to_string_pretty(&report).unwrap_or_default())
}

fn list_pages_tool(ctx: &ServerContext) -> Result<String, ToolError> {
    let scan = ctx
        .vault
        .scan()
        .map_err(|e| ToolError::Failed(format!("scan: {e}")))?;
    let rows: Vec<Value> = scan
        .pages
        .iter()
        .map(|p| {
            json!({
                "id":    p.frontmatter.id.to_string(),
                "title": p.frontmatter.title,
                "path":  p.path.as_str(),
                "tags":  p.frontmatter.tags,
            })
        })
        .collect();
    Ok(serde_json::to_string_pretty(&rows).unwrap_or_default())
}

#[derive(Debug, Deserialize)]
struct ReadPageArgs {
    path: String,
}

fn read_page_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let args: ReadPageArgs = serde_json::from_value(args)?;
    let rel = parse_vault_path(&args.path)?;
    let abs = ctx
        .vault
        .resolve(&rel)
        .map_err(|e| ToolError::InvalidArgs(format!("resolve: {e}")))?;
    std::fs::read_to_string(abs.as_std_path())
        .map_err(|e| ToolError::Failed(format!("read {abs}: {e}")))
}

#[derive(Debug, Deserialize)]
struct WritePageArgs {
    path: String,
    content: String,
    #[serde(default)]
    message: Option<String>,
}

fn write_page_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let args: WritePageArgs = serde_json::from_value(args)?;
    let rel = parse_vault_path(&args.path)?;
    // Only allow writes under `wiki/` for this tool — the `log.md` append
    // and source interning have their own paths. This blocks the obvious
    // footgun of an MCP client rewriting CLAUDE.md or config.toml.
    if !rel.as_str().starts_with("wiki/") {
        return Err(ToolError::InvalidArgs(
            "scriptorium_write_page is restricted to paths under wiki/".into(),
        ));
    }
    // Validate the content parses as a page.
    let _page = Page::parse(rel.clone(), &args.content)
        .map_err(|e| ToolError::InvalidArgs(format!("content is not a valid page: {e}")))?;
    let mut tx = ctx.vault.begin();
    tx.put_file(&rel, args.content)
        .map_err(|e| ToolError::Failed(format!("stage write: {e}")))?;
    let msg = args
        .message
        .unwrap_or_else(|| format!("scriptorium_write_page: {rel}"));
    let commit_id = tx
        .commit(&msg)
        .map_err(|e| ToolError::Failed(format!("commit: {e}")))?;
    Ok(format!("wrote {rel}, commit {commit_id}"))
}

#[derive(Debug, Deserialize)]
struct SearchArgs {
    query: String,
    #[serde(default = "default_top_k")]
    top_k: usize,
}

async fn search_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let args: SearchArgs = serde_json::from_value(args)?;
    let store = open_store(ctx)?;
    let query_vec = ctx
        .embed_provider
        .embed(std::slice::from_ref(&args.query))
        .await
        .map_err(|e| ToolError::Failed(format!("embed query: {e}")))?
        .pop()
        .ok_or_else(|| ToolError::Failed("embed provider returned no embedding".into()))?;
    let hits = store
        .search(
            &query_vec,
            ctx.embed_provider.name(),
            &ctx.embeddings_model,
            args.top_k,
        )
        .map_err(|e| ToolError::Failed(format!("search: {e}")))?;
    let rows: Vec<Value> = hits
        .into_iter()
        .map(|h| {
            json!({
                "page_id":    h.page_id.to_string(),
                "chunk_idx":  h.chunk_idx,
                "heading":    h.heading,
                "chunk_text": h.chunk_text,
                "score":      h.score,
            })
        })
        .collect();
    Ok(serde_json::to_string_pretty(&rows).unwrap_or_default())
}

#[derive(Debug, Deserialize)]
struct LogTailArgs {
    #[serde(default = "default_log_n")]
    n: usize,
}

fn default_log_n() -> usize {
    20
}

fn log_tail_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let args: LogTailArgs = serde_json::from_value(args)?;
    let path = ctx.vault.log_path();
    let text = std::fs::read_to_string(path.as_std_path()).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(args.n);
    Ok(lines[start..].join("\n"))
}

async fn maintain_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let fix = args
        .get("fix")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let store = open_store(ctx)?;
    let options = scriptorium_core::maintain::MaintainOptions { fix };
    let report = scriptorium_core::maintain::maintain(
        &ctx.vault,
        &store,
        Some(ctx.embed_provider.as_ref()),
        &ctx.embeddings_model,
        &options,
    )
    .await
    .map_err(|e| ToolError::Failed(format!("maintain: {e}")))?;
    serde_json::to_string_pretty(&report)
        .map_err(|e| ToolError::Failed(format!("json: {e}")))
}

fn doctor_tool(ctx: &ServerContext) -> Result<String, ToolError> {
    let store = open_store(ctx).ok();
    let report =
        scriptorium_core::doctor::run_doctor(&ctx.vault, store.as_ref());
    serde_json::to_string_pretty(&report).map_err(|e| ToolError::Failed(format!("json: {e}")))
}

fn skill_list_tool(ctx: &ServerContext) -> Result<String, ToolError> {
    let skills = scriptorium_core::skills::list_skills(&ctx.vault)
        .map_err(|e| ToolError::Failed(format!("skills: {e}")))?;
    serde_json::to_string_pretty(&skills).map_err(|e| ToolError::Failed(format!("json: {e}")))
}

async fn bench_tool(ctx: &ServerContext) -> Result<String, ToolError> {
    let store = open_store(ctx)?;
    let report = scriptorium_core::bench::run_benchmarks(
        &ctx.vault,
        &store,
        ctx.embed_provider.as_ref(),
        ctx.llm_provider.as_ref(),
        &ctx.embeddings_model,
    )
    .await
    .map_err(|e| ToolError::Failed(format!("bench: {e}")))?;
    serde_json::to_string_pretty(&report)
        .map_err(|e| ToolError::Failed(format!("json: {e}")))
}

#[allow(clippy::needless_pass_by_value)]
fn learn_capture_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    use scriptorium_core::learnings::{Learning, LearningSource, LearningType};

    let skill = args.get("skill").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidArgs("missing 'skill'".into()))?
        .to_string();
    let learning_type: LearningType = args.get("type").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidArgs("missing 'type'".into()))
        .and_then(|s| serde_json::from_value(serde_json::Value::String(s.into()))
            .map_err(|e| ToolError::InvalidArgs(format!("invalid type: {e}"))))?;
    let key = args.get("key").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidArgs("missing 'key'".into()))?
        .to_string();
    let insight = args.get("insight").and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidArgs("missing 'insight'".into()))?
        .to_string();
    #[allow(clippy::cast_possible_truncation)]
    let confidence = args.get("confidence").and_then(Value::as_u64).unwrap_or(7) as u8;
    let source: LearningSource = args.get("source").and_then(Value::as_str)
        .map_or(LearningSource::Observed, |s| {
            serde_json::from_value(Value::String(s.into())).unwrap_or(LearningSource::Observed)
        });
    let tags: Vec<String> = args.get("tags")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    let files: Vec<String> = args.get("files")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    let learning = Learning {
        ts: chrono::Utc::now(),
        skill,
        learning_type,
        key,
        insight,
        confidence,
        source,
        tags,
        files,
    };
    scriptorium_core::learnings::capture(&ctx.vault, &learning)
        .map_err(|e| ToolError::Failed(format!("capture: {e}")))?;
    Ok(format!("Captured: [{:?}] {}", learning.learning_type, learning.key))
}

#[allow(clippy::needless_pass_by_value)]
fn learn_search_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let query = args
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let results = if query.is_empty() {
        scriptorium_core::learnings::list_recent(&ctx.vault, 20)
    } else {
        scriptorium_core::learnings::search(&ctx.vault, query)
    }
    .map_err(|e| ToolError::Failed(format!("search: {e}")))?;
    serde_json::to_string_pretty(&results).map_err(|e| ToolError::Failed(format!("json: {e}")))
}

#[allow(clippy::needless_pass_by_value)]
fn learn_retrieve_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let tags: Vec<String> = args
        .get("tags")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();
    #[allow(clippy::cast_possible_truncation)]
    let limit = args
        .get("limit")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(5) as usize;
    let tag_refs: Vec<&str> = tags.iter().map(String::as_str).collect();
    let results = scriptorium_core::learnings::retrieve(&ctx.vault, &tag_refs, limit)
        .map_err(|e| ToolError::Failed(format!("retrieve: {e}")))?;
    serde_json::to_string_pretty(&results).map_err(|e| ToolError::Failed(format!("json: {e}")))
}

#[allow(clippy::needless_pass_by_value)] // matches the convention of all other tool handlers
fn skill_read_tool(args: Value, ctx: &ServerContext) -> Result<String, ToolError> {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ToolError::InvalidArgs("missing 'name' field".into()))?;
    let skill = scriptorium_core::skills::load_skill(&ctx.vault, name)
        .map_err(|e| ToolError::Failed(format!("skill '{name}': {e}")))?;
    Ok(skill.content)
}

// ---------- helpers ----------

fn parse_vault_path(raw: &str) -> Result<Utf8PathBuf, ToolError> {
    let rel = Utf8Path::new(raw);
    if rel.is_absolute() {
        return Err(ToolError::InvalidArgs(format!(
            "absolute paths are rejected: {raw}"
        )));
    }
    for component in rel.components() {
        if matches!(component, camino::Utf8Component::ParentDir) {
            return Err(ToolError::InvalidArgs(format!(
                "`..` in path is rejected: {raw}"
            )));
        }
    }
    Ok(rel.to_path_buf())
}

fn open_store(ctx: &ServerContext) -> Result<EmbeddingsStore, ToolError> {
    let meta = ctx.vault.meta_dir();
    std::fs::create_dir_all(meta.as_std_path())
        .map_err(|e| ToolError::Failed(format!("mkdir: {e}")))?;
    let path = meta.join("embeddings.sqlite");
    EmbeddingsStore::open(path.as_std_path())
        .map_err(|e| ToolError::Failed(format!("open embeddings store: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use scriptorium_core::llm::{IngestAction, IngestPageAction, IngestPlan, MockProvider};
    use scriptorium_core::Vault;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn parse_vault_path_rejects_absolute() {
        assert!(parse_vault_path("/etc/passwd").is_err());
    }

    #[test]
    fn parse_vault_path_rejects_parent_dir() {
        assert!(parse_vault_path("../../etc/passwd").is_err());
        assert!(parse_vault_path("wiki/../../etc").is_err());
    }

    #[test]
    fn parse_vault_path_accepts_normal_relative_path() {
        let p = parse_vault_path("wiki/concepts/foo.md").unwrap();
        assert_eq!(p.as_str(), "wiki/concepts/foo.md");
    }

    #[test]
    fn registry_describes_all_tools() {
        let reg = ToolRegistry::new();
        let specs = reg.describe_all();
        let names: Vec<&str> = specs.iter().filter_map(|s| s["name"].as_str()).collect();
        assert!(names.contains(&"scriptorium_ingest"));
        assert!(names.contains(&"scriptorium_query"));
        assert!(names.contains(&"scriptorium_write_page"));
        assert!(names.contains(&"scriptorium_read_page"));
        assert!(names.contains(&"scriptorium_search"));
        assert!(names.contains(&"scriptorium_log_tail"));
        assert!(names.contains(&"scriptorium_list_pages"));
        assert!(names.contains(&"scriptorium_lint"));
    }

    /// Regression guard: the Anthropic API's
    /// `tools.*.custom.input_schema` rejects `oneOf` / `anyOf` / `allOf`
    /// at the top level of a tool's input schema with a 400 that
    /// POISONS the tool registry for the rest of the session — every
    /// subsequent API call also fails until the tool is removed.
    ///
    /// scriptorium_ingest previously used `oneOf` at the top level to
    /// express "source XOR url", which broke Claude Code sessions that
    /// tried to load it. Runtime handlers do the XOR check instead. No
    /// tool spec should ever re-introduce a top-level combinator.
    #[test]
    fn tool_specs_have_no_top_level_json_schema_combinators() {
        let reg = ToolRegistry::new();
        let specs = reg.describe_all();
        let forbidden = ["oneOf", "anyOf", "allOf", "not"];
        for spec in &specs {
            let name = spec["name"].as_str().unwrap_or("?");
            let input_schema = &spec["inputSchema"];
            assert!(
                input_schema.is_object(),
                "tool `{name}` has a non-object input schema"
            );
            for key in &forbidden {
                assert!(
                    input_schema.get(key).is_none(),
                    "tool `{name}` has top-level `{key}` in its input schema. \
                     The Anthropic API rejects this with a 400 that poisons \
                     the whole tool registry. Express constraints at runtime \
                     in the tool handler instead."
                );
            }
        }
    }

    // ---------- ingest_tool: post-ingest auto-reindex coverage ----------
    //
    // These tests pin the contract added when scriptorium_ingest learned to
    // refresh the embeddings store after a successful commit. Without that
    // step, freshly ingested pages stayed invisible to scriptorium_search /
    // scriptorium_query until a manual `scriptorium reindex` was run.
    //
    // The tests use MockProvider for both chat (returns a fixed IngestPlan
    // JSON) and embeddings (returns deterministic SHA-derived vectors).

    /// Build a temp vault, a ServerContext, and a source file inside the
    /// vault wired up so a single ingest_tool call exercises the full
    /// commit + auto-reindex path. Returns:
    ///   - the TempDir guard (kept alive by caller)
    ///   - the ServerContext (already wired with mock providers)
    ///   - the absolute path of the source file the test should pass to
    ///     ingest_tool
    fn make_test_context(
        plan: &IngestPlan,
        source_name: &str,
        source_body: &str,
    ) -> (TempDir, ServerContext, PathBuf) {
        let dir = TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("wiki/concepts")).unwrap();
        std::fs::create_dir_all(dir.path().join("sources/articles")).unwrap();
        std::fs::write(
            dir.path().join("CLAUDE.md"),
            "# Test Vault\n\nMinimal schema for tests.\n",
        )
        .unwrap();
        let vault = Vault::open(dir.path()).unwrap();

        let llm: Arc<dyn scriptorium_core::llm::LlmProvider> =
            Arc::new(MockProvider::constant(serde_json::to_string(plan).unwrap()));
        let embed: Arc<dyn scriptorium_core::llm::LlmProvider> =
            Arc::new(MockProvider::constant(""));

        let ctx = ServerContext {
            vault,
            llm_provider: llm,
            embed_provider: embed,
            embeddings_model: "mock-embed-test".into(),
        };

        let source = dir.path().join("sources/articles").join(source_name);
        std::fs::write(&source, source_body).unwrap();

        (dir, ctx, source)
    }

    fn make_simple_plan(path: &str, title: &str, body: &str) -> IngestPlan {
        IngestPlan {
            summary: format!("ingest {title}"),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: path.into(),
                title: title.into(),
                tags: vec!["test".into()],
                body: body.into(),
            }],
            log_entry: format!("ingested {title}"),
        }
    }

    /// Happy path: a successful ingest_tool call leaves the embeddings
    /// store populated, so the freshly created page is immediately
    /// retrievable. This is the contract the auto-reindex behavior
    /// guarantees — without it, store.len() would be 0 after the call.
    #[tokio::test]
    async fn ingest_tool_embeds_pages_into_store_after_commit() {
        let plan = make_simple_plan(
            "wiki/concepts/alpha.md",
            "Alpha",
            "## Overview\n\nFirst section body for alpha.\n\n## Details\n\nMore content.\n",
        );
        let (_dir, ctx, source) =
            make_test_context(&plan, "alpha-source.md", "alpha source contents");

        let args = json!({"source": source.to_string_lossy(), "dry_run": false});
        let out = ingest_tool(args, &ctx).await.expect("ingest_tool succeeds");

        assert!(out.contains("1 created"), "ingest output: {out}");
        assert!(out.contains("commit "), "ingest output: {out}");

        // The store the production code wrote to is the vault's
        // .scriptorium/embeddings.sqlite — not in-memory. Open the same
        // file the dispatch site opened and assert it has rows.
        let store = open_store(&ctx).expect("open store");
        let total = store.len().expect("count rows");
        assert!(
            total > 0,
            "expected embeddings store to have at least one chunk after ingest_tool, found {total}"
        );

        // The output should mention the embedded count when chunks were
        // newly written.
        assert!(
            out.contains("embedded ") && out.contains("new chunk"),
            "expected ingest output to mention embedded chunk count, got: {out}"
        );
    }

    /// Dry-run path: ingest_tool with dry_run=true should NOT touch the
    /// embeddings store at all. Even though the source is interned and the
    /// LLM is consulted, no commit happens, so no reindex should fire and
    /// the store should remain empty.
    #[tokio::test]
    async fn ingest_tool_dry_run_does_not_touch_embeddings_store() {
        let plan = make_simple_plan(
            "wiki/concepts/preview.md",
            "Preview",
            "## Section\n\nDry-run preview body.\n",
        );
        let (_dir, ctx, source) =
            make_test_context(&plan, "preview-source.md", "preview contents");

        let args = json!({"source": source.to_string_lossy(), "dry_run": true});
        let out = ingest_tool(args, &ctx)
            .await
            .expect("dry-run ingest_tool succeeds");

        assert!(
            out.contains("DRY RUN"),
            "dry-run output should announce itself: {out}"
        );

        let store = open_store(&ctx).expect("open store");
        assert_eq!(
            store.len().expect("count"),
            0,
            "dry-run must NOT write embeddings"
        );
        assert!(
            !out.contains("embedded "),
            "dry-run should not report embedded chunks: {out}"
        );
    }

    /// A second ingest that updates an existing page produces new
    /// embedding rows even when the body is unchanged. This is correct:
    /// `Page::content_hash` hashes the rendered markdown including
    /// frontmatter, and an Update bumps `frontmatter.updated` and
    /// appends to `frontmatter.sources`, which changes the hash. The
    /// pure cache-hit behavior of `embed::reindex` is covered by the
    /// `embed_reindex_after_ingest_is_cache_hit_on_repeat` test in
    /// `crates/scriptorium-core/tests/e2e.rs`. This test pins the MCP
    /// dispatch contract: a successful repeat ingest still re-embeds
    /// (so search stays consistent with the latest page state) and the
    /// store grows monotonically.
    #[tokio::test]
    async fn ingest_tool_repeat_update_re_embeds_on_frontmatter_change() {
        let mut plan = make_simple_plan(
            "wiki/concepts/repeat.md",
            "Repeat",
            "## Body\n\nIdentical body across both ingests.\n",
        );
        let (_dir, ctx, source) =
            make_test_context(&plan, "repeat-source-1.md", "first-source");

        // First ingest: create + embed.
        let args1 = json!({"source": source.to_string_lossy(), "dry_run": false});
        let out1 = ingest_tool(args1, &ctx).await.expect("first ingest");
        assert!(out1.contains("1 created"), "first ingest: {out1}");

        let store = open_store(&ctx).expect("open store");
        let chunks_after_first = store.len().expect("count");
        assert!(
            chunks_after_first > 0,
            "first ingest should write at least one embedding chunk"
        );

        // Second ingest, same body, action=Update. The Update path mutates
        // frontmatter (bumps `updated`, appends to `sources`), which
        // changes `Page::content_hash`, which is part of the embeddings
        // cache key — so reindex must re-embed.
        plan.pages[0].action = IngestAction::Update;
        let llm2: Arc<dyn scriptorium_core::llm::LlmProvider> =
            Arc::new(MockProvider::constant(serde_json::to_string(&plan).unwrap()));
        let ctx2 = ServerContext {
            vault: ctx.vault.clone(),
            llm_provider: llm2,
            embed_provider: ctx.embed_provider.clone(),
            embeddings_model: ctx.embeddings_model.clone(),
        };
        let source2 = ctx
            .vault
            .root()
            .as_std_path()
            .join("sources/articles/repeat-source-2.md");
        std::fs::write(&source2, "second-source-different-bytes").unwrap();
        let args2 = json!({"source": source2.to_string_lossy(), "dry_run": false});
        let out2 = ingest_tool(args2, &ctx2).await.expect("second ingest");

        assert!(out2.contains("1 updated"), "second ingest: {out2}");
        assert!(
            out2.contains("embedded ") && out2.contains("new chunk"),
            "frontmatter change must trigger re-embed: {out2}"
        );

        // Store size grows monotonically (the old version's rows remain
        // in the store; the new version's rows are appended).
        let store2 = open_store(&ctx2).expect("open store again");
        let chunks_after_second = store2.len().expect("count");
        assert!(
            chunks_after_second >= chunks_after_first,
            "store must grow monotonically: {chunks_after_first} -> {chunks_after_second}"
        );
    }
}
