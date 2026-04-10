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
///
/// ## Wire-compatibility salvage
///
/// Observed in production: the model occasionally omits the `log_entry`
/// field on large ingests (likely when it runs out of output budget before
/// finishing the object). To keep ingest moving instead of aborting the
/// whole commit, `IngestPlan` is deserialized via an intermediate
/// [`IngestPlanRaw`] that treats `log_entry` as optional and synthesizes a
/// fallback from `summary` and `pages` when the model doesn't supply one.
/// `summary` and `pages` remain hard requirements.
///
/// **Important**: `JsonSchema` still derives from *this* struct (the
/// target, not the raw wrapper), so the schema reported to Anthropic's
/// strict mode keeps `log_entry` in `required`. Strict mode will still
/// prevent the bug at the API boundary; the `try_from` layer only catches
/// the fallback providers (mock, Ollama) and any future regression.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[serde(try_from = "IngestPlanRaw")]
pub struct IngestPlan {
    /// One-line human summary of what the source contained.
    pub summary: String,
    /// Pages to create or update. Order matters: later entries may link to
    /// earlier ones.
    pub pages: Vec<IngestPageAction>,
    /// The entry to append to `log.md`. Should be a single line.
    pub log_entry: String,
}

/// Lenient wire shape used only as a deserialization waypoint. Accepts
/// responses where `log_entry` is missing or empty and synthesizes it from
/// the summary + page count. `summary` and `pages` remain hard requirements
/// — if the model omits either of those there is nothing sensible to
/// synthesize from, and the ingest should fail loudly.
///
/// This struct is crate-private and never appears in the public API; it
/// exists purely as the target of `#[serde(try_from)]` on [`IngestPlan`].
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct IngestPlanRaw {
    summary: String,
    pages: Vec<IngestPageAction>,
    #[serde(default)]
    log_entry: Option<String>,
}

impl TryFrom<IngestPlanRaw> for IngestPlan {
    type Error = String;
    fn try_from(r: IngestPlanRaw) -> Result<Self, Self::Error> {
        let log_entry = r
            .log_entry
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| derive_log_entry(&r.summary, &r.pages));
        Ok(Self {
            summary: r.summary,
            pages: r.pages,
            log_entry,
        })
    }
}

/// Synthesize a one-line `log_entry` from the rest of an [`IngestPlan`] when
/// the model didn't emit one. The output shape mirrors the normal log
/// entries so the resulting `log.md` stays uniform.
fn derive_log_entry(summary: &str, pages: &[IngestPageAction]) -> String {
    let first_line = summary.lines().next().unwrap_or("ingest").trim();
    let page_count = pages.len();
    let suffix = if page_count == 1 { "page" } else { "pages" };
    format!("{first_line} ({page_count} {suffix}, log_entry auto-synthesized)")
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
         `log.md`. All three fields are REQUIRED — omitting any one will \
         cause the ingest to be rejected. Respond with JSON only: no prose, \
         no apology, no markdown fence.",
        pages = ctx.render_pages()
    );
    CompletionRequest {
        system,
        messages: vec![Message {
            role: Role::User,
            content: user,
        }],
        // 16K output budget. A single ingest over a mid-size source routinely
        // produces 4–6 wiki pages (titles + bodies) plus the summary and
        // log_entry — ~8K tokens of actual content is common, and the old
        // 4K ceiling truncated responses mid-tool-call, producing partial
        // JSON missing required fields. 16K comfortably fits the long tail
        // without wasting context on the short tail.
        max_tokens: 16_384,
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
    fn ingest_plan_salvage_synthesizes_missing_log_entry() {
        // This is the exact shape that caused today's production failure:
        // summary and pages are present, log_entry is missing entirely.
        let input = r#"{
            "summary": "Cross-system survey of LLM structured output",
            "pages": [{
                "action": "create",
                "path": "wiki/patterns/test.md",
                "title": "Test",
                "body": "body\n"
            }]
        }"#;
        let plan: IngestPlan =
            serde_json::from_str(input).expect("salvage must accept missing log_entry");
        assert_eq!(plan.summary, "Cross-system survey of LLM structured output");
        assert_eq!(plan.pages.len(), 1);
        assert!(
            !plan.log_entry.trim().is_empty(),
            "synthesized log_entry must be non-empty"
        );
        assert!(
            plan.log_entry.contains("auto-synthesized"),
            "synthesized entry should be marked as such: {}",
            plan.log_entry
        );
    }

    #[test]
    fn ingest_plan_salvage_synthesizes_empty_log_entry() {
        // Whitespace-only log_entry counts as missing.
        let input = r#"{
            "summary": "whitespace test",
            "pages": [],
            "log_entry": "   "
        }"#;
        let plan: IngestPlan = serde_json::from_str(input).unwrap();
        assert!(!plan.log_entry.trim().is_empty());
        assert!(plan.log_entry.contains("auto-synthesized"));
    }

    #[test]
    fn ingest_plan_salvage_rejects_missing_summary() {
        // Salvage must be targeted: if summary is missing, there's nothing
        // sensible to synthesize from, so the ingest should fail loudly.
        let input = r#"{ "pages": [], "log_entry": "x" }"#;
        let result: Result<IngestPlan, _> = serde_json::from_str(input);
        assert!(result.is_err(), "missing summary must still fail");
    }

    #[test]
    fn ingest_plan_salvage_rejects_missing_pages() {
        // Same as above for `pages`: without knowing which pages the model
        // wanted to write, we cannot invent them.
        let input = r#"{ "summary": "x", "log_entry": "x" }"#;
        let result: Result<IngestPlan, _> = serde_json::from_str(input);
        assert!(result.is_err(), "missing pages must still fail");
    }

    #[test]
    fn ingest_plan_salvage_rejects_unknown_fields() {
        // deny_unknown_fields is required for Anthropic strict mode
        // (additionalProperties: false) and also catches LLM drift early.
        let input = r#"{
            "summary": "x",
            "pages": [],
            "log_entry": "x",
            "bogus": "field"
        }"#;
        let result: Result<IngestPlan, _> = serde_json::from_str(input);
        assert!(result.is_err(), "unknown fields must be rejected");
    }

    #[test]
    fn ingest_plan_schema_still_requires_log_entry() {
        // This is the critical compatibility test between `try_from` and
        // `JsonSchema`. schemars derives the schema from the target type
        // (IngestPlan), not the raw wrapper (IngestPlanRaw), so `log_entry`
        // must still appear in `required`. If this assertion ever fails,
        // Anthropic strict mode will stop enforcing the field and we'll
        // silently regress to the old bug.
        let schema = IngestPlan::schema();
        let required = schema["required"]
            .as_array()
            .expect("schema has required array");
        let names: Vec<&str> = required.iter().filter_map(|v| v.as_str()).collect();
        assert!(
            names.contains(&"log_entry"),
            "log_entry must remain required in schema, got: {names:?}"
        );
        assert!(names.contains(&"summary"));
        assert!(names.contains(&"pages"));
    }

    #[test]
    fn derive_log_entry_is_stable_and_descriptive() {
        let entry = derive_log_entry(
            "Cross-system survey of LLM structured output",
            &[IngestPageAction {
                action: IngestAction::Create,
                path: "wiki/a.md".into(),
                title: "A".into(),
                tags: vec![],
                body: "x".into(),
            }],
        );
        assert!(entry.contains("Cross-system survey"));
        assert!(entry.contains("1 page"));
        assert!(!entry.contains("1 pages"));

        let multi = derive_log_entry("topic", &vec![]);
        assert!(multi.contains("0 pages"));
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
