//! Versioned prompt templates and structured-output types.
//!
//! Every LLM call scriptorium makes goes through one of the builders in this
//! module (`ingest_prompt`, `query_prompt`, `lint_llm_prompt`). Each builder
//! takes typed context — the rendered schema, the source text, the retrieved
//! neighbours — and returns a [`CompletionRequest`] with both the prompt and
//! the JSON Schema for the expected response.
//!
//! The response types — [`IngestPlan`], [`QueryAnswer`] — derive both
//! [`serde::Serialize`] / [`serde::Deserialize`] (for parsing) and
//! [`schemars::JsonSchema`] (for forcing structured output). Real providers
//! will translate the schema into their native strict-mode format (Claude
//! `tools[].input_schema`, `OpenAI` `response_format: json_schema`); the mock
//! provider simply returns whatever fixture text the test supplied.
//!
//! Prompts are intentionally short. The heavy lifting happens in `CLAUDE.md`
//! (rendered into the system string), not in these templates. That keeps
//! prompt changes reviewable and schemas easy to lock down.

use std::fmt::Write;

use schemars::{schema_for, JsonSchema};
use serde::{Deserialize, Serialize};

use crate::vault::Page;

use super::{CompletionRequest, Message, Role};

/// The structured response the ingest prompt demands from the LLM.
///
/// One plan may create zero or more new pages and update zero or more
/// existing ones. The engine applies each action deterministically after
/// re-validating the plan against [`IngestPlan::schema`].
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IngestPlan {
    /// One-line human summary of what the source contained.
    pub summary: String,
    /// Pages to create or update. Order matters: later entries may link to
    /// earlier ones.
    pub pages: Vec<IngestPageAction>,
    /// The entry to append to `log.md`. Should be a single line.
    pub log_entry: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IngestPageAction {
    pub action: IngestAction,
    /// Vault-relative path, e.g. `wiki/concepts/attention.md`. Must end in
    /// `.md` and live under `wiki/`.
    pub path: String,
    /// Human-readable title for the page frontmatter.
    pub title: String,
    /// Tags to attach in the frontmatter.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Markdown body of the page (no frontmatter — the engine writes that).
    pub body: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum IngestAction {
    Create,
    Update,
}

impl IngestPlan {
    /// JSON Schema for the ingest response, used to force structured output
    /// at providers that support it. Computed lazily via `schemars`.
    pub fn schema() -> serde_json::Value {
        serde_json::to_value(schema_for!(IngestPlan)).expect("schemars produces valid JSON")
    }
}

/// The structured response the query prompt demands from the LLM.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct QueryAnswer {
    /// The answer in prose, with `[[Page]]` citations inline.
    pub answer: String,
    /// The set of page stems the answer cites. Must be a subset of the
    /// retrieved pages; the engine rejects answers that cite pages it never
    /// supplied.
    #[serde(default)]
    pub citations: Vec<String>,
    /// Confidence 0.0–1.0. `None` means "the model did not express one".
    #[serde(default)]
    pub confidence: Option<f32>,
}

impl QueryAnswer {
    pub fn schema() -> serde_json::Value {
        serde_json::to_value(schema_for!(QueryAnswer)).expect("schemars produces valid JSON")
    }
}

/// Context shared by every prompt: the rendered schema and the set of
/// existing pages that are relevant to this operation.
pub struct PromptContext<'a> {
    pub rendered_schema: &'a str,
    pub relevant_pages: &'a [&'a Page],
}

impl<'a> PromptContext<'a> {
    pub fn new(rendered_schema: &'a str, relevant_pages: &'a [&'a Page]) -> Self {
        Self {
            rendered_schema,
            relevant_pages,
        }
    }

    fn render_pages(&self) -> String {
        if self.relevant_pages.is_empty() {
            return "(no relevant existing pages)\n".into();
        }
        let mut out = String::new();
        for page in self.relevant_pages {
            let _ = writeln!(out, "### `{}` — {}", page.path, page.frontmatter.title);
            out.push_str(&page.body);
            out.push_str("\n\n");
        }
        out
    }
}

/// Build an ingest prompt for a single source.
///
/// `source_label` is a short human identifier shown to the model
/// (e.g. the filename). `source_text` is the extracted plain text of the
/// source document.
pub fn ingest_prompt(
    ctx: &PromptContext<'_>,
    source_label: &str,
    source_text: &str,
) -> CompletionRequest {
    let system = format!(
        "You are Scriptorium's ingest operator. Given a raw source document, \
         you produce an `IngestPlan`: a list of wiki pages to create or update \
         so the vault captures the source's key claims.\n\n\
         Follow the vault schema below exactly. Never fabricate facts — cite \
         only what is in the source. Prefer updating an existing page over \
         creating a duplicate.\n\n\
         === vault schema ===\n{schema}\n=== end schema ===",
        schema = ctx.rendered_schema
    );
    let user = format!(
        "## Source: `{source_label}`\n\n\
         ```\n{source_text}\n```\n\n\
         ## Relevant existing pages\n\n{pages}\n\
         ## Task\n\n\
         Return an `IngestPlan` with (1) a one-line `summary`, (2) `pages` \
         describing create/update actions, (3) a one-line `log_entry` for \
         `log.md`. Respond with JSON only.",
        pages = ctx.render_pages()
    );
    CompletionRequest {
        system,
        messages: vec![Message {
            role: Role::User,
            content: user,
        }],
        max_tokens: 4096,
        temperature: Some(0.2),
        response_schema: Some(IngestPlan::schema()),
    }
}

/// Build a query prompt.
pub fn query_prompt(ctx: &PromptContext<'_>, question: &str) -> CompletionRequest {
    let system = format!(
        "You are Scriptorium's query operator. Given a question and a set of \
         retrieved wiki pages, you return a cited answer in the structured \
         `QueryAnswer` format. You may only cite pages that were supplied — \
         never invent targets. If the retrieved context is insufficient, say \
         so plainly.\n\n\
         === vault schema ===\n{schema}\n=== end schema ===",
        schema = ctx.rendered_schema
    );
    let user = format!(
        "## Question\n\n{question}\n\n\
         ## Retrieved pages\n\n{pages}\n\
         ## Task\n\n\
         Answer the question. Use `[[stem]]` wikilink format for citations. \
         Only cite pages that appear above. Respond with JSON only.",
        pages = ctx.render_pages()
    );
    CompletionRequest {
        system,
        messages: vec![Message {
            role: Role::User,
            content: user,
        }],
        max_tokens: 2048,
        temperature: Some(0.1),
        response_schema: Some(QueryAnswer::schema()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vault::page::{Frontmatter, PageId};
    use camino::Utf8PathBuf;
    use chrono::{TimeZone, Utc};
    use std::collections::BTreeMap;

    fn sample_page() -> Page {
        let now = Utc.with_ymd_and_hms(2026, 4, 6, 12, 0, 0).unwrap();
        Page {
            path: Utf8PathBuf::from("wiki/concepts/attention.md"),
            frontmatter: Frontmatter {
                id: PageId::new(),
                title: "Attention".into(),
                created: now,
                updated: now,
                sources: vec![],
                tags: vec!["concept".into()],
                aliases: vec![],
                schema_version: 1,
                extra: BTreeMap::new(),
            },
            body: "Scaled dot-product attention.\n".into(),
        }
    }

    #[test]
    fn ingest_plan_schema_is_well_formed_json() {
        let schema = IngestPlan::schema();
        // Top-level is an object schema.
        assert_eq!(schema["type"], "object");
        // Must have the three fields we rely on.
        let props = schema["properties"]
            .as_object()
            .expect("schema has properties");
        assert!(props.contains_key("summary"));
        assert!(props.contains_key("pages"));
        assert!(props.contains_key("log_entry"));
    }

    #[test]
    fn query_answer_schema_has_citations_field() {
        let schema = QueryAnswer::schema();
        let props = schema["properties"]
            .as_object()
            .expect("schema has properties");
        assert!(props.contains_key("answer"));
        assert!(props.contains_key("citations"));
    }

    #[test]
    fn ingest_plan_round_trips_through_json() {
        let plan = IngestPlan {
            summary: "attention is parallel".into(),
            pages: vec![IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/concepts/attention.md".into(),
                title: "Attention".into(),
                tags: vec!["concept".into()],
                body: "body\n".into(),
            }],
            log_entry: "[2026-04-06] ingest | attention source".into(),
        };
        let json = serde_json::to_string(&plan).unwrap();
        let parsed: IngestPlan = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.summary, plan.summary);
        assert_eq!(parsed.pages[0].action, IngestAction::Create);
    }

    #[test]
    fn ingest_prompt_embeds_schema_and_source() {
        let page = sample_page();
        let pages = [&page];
        let ctx = PromptContext::new("# Rules\n\nBe nice.\n", &pages);
        let req = ingest_prompt(&ctx, "foo.md", "source body");
        assert!(req.system.contains("vault schema"));
        assert!(req.system.contains("Be nice"));
        assert!(req.messages[0].content.contains("foo.md"));
        assert!(req.messages[0].content.contains("source body"));
        assert!(req.messages[0]
            .content
            .contains("wiki/concepts/attention.md"));
        assert!(req.response_schema.is_some());
    }

    #[test]
    fn query_prompt_embeds_question_and_pages() {
        let page = sample_page();
        let pages = [&page];
        let ctx = PromptContext::new("", &pages);
        let req = query_prompt(&ctx, "how does attention work?");
        assert!(req.messages[0].content.contains("how does attention work"));
        assert!(req.messages[0].content.contains("Attention"));
        assert!(req.response_schema.is_some());
    }
}
