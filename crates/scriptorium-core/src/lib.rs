//! Scriptorium core engine.
//!
//! This crate provides the building blocks of an LLM-maintained Obsidian vault:
//!
//! - [`vault`] — vault scan, page model, wikilinks, link graph, transactional writes
//! - [`schema`] — `CLAUDE.md` schema loader with token budgeting
//! - [`lint`] — mechanical lint rules (broken links, orphans, frontmatter)
//! - [`llm`] — pluggable LLM provider trait + Mock/Claude/OpenAI/Ollama implementations
//! - [`embed`] — sqlite-backed embeddings store with hash-keyed chunks
//! - [`ingest`] — source-to-wiki ingest pipeline
//! - [`query`] — retrieval-augmented question answering
//! - [`config`] — `.scriptorium/config.toml` loading
//!
//! All mutating operations go through [`vault::tx::VaultTx`], which stages writes,
//! validates them with mechanical lint, takes a vault lock, fsyncs files, and
//! creates a single git commit. Git is the durable undo log.

pub mod bulk_ingest;
pub mod config;
pub mod doctor;
pub mod embed;
pub mod error;
pub mod git;
pub mod index;
pub mod ingest;
pub mod lint;
pub mod llm;
pub mod maintain;
pub mod query;
pub mod reindex;
pub mod schema;
pub mod search;
pub mod url_fetch;
pub mod vault;
pub mod watch;

pub use config::Config;
pub use error::{Error, Result};
pub use lint::{LintIssue, LintReport, Severity};
pub use llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, Message, MockProvider, Role,
    Usage,
};
pub use vault::{
    Frontmatter, LinkGraph, LinkRef, Page, PageId, PageInfo, Resolution, ScanReport, Vault,
    Wikilink,
};
