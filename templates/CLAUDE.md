# Scriptorium Vault Schema

This file is the contract between you and the LLM that curates this vault.
Every ingest, query, and lint-llm operation reads it as the system prompt.
Keep it short, concrete, and opinionated — vague rules produce vague pages.

## Layers

This vault has three layers:

1. **`sources/`** — raw, immutable inputs (articles, PDFs, transcripts, notes).
   The LLM never modifies files here. New sources are copied in by
   `scriptorium ingest`.
2. **`wiki/`** — LLM-maintained markdown pages, one concept or entity per
   page. This is where summaries, cross-references, and your own edits live.
3. **This file (`CLAUDE.md`)** — the schema you are reading now. Also
   `index.md` (generated table of contents) and `log.md` (append-only
   timeline of operations).

## Page conventions

- One page per **concept** or **entity**. If you'd explain it as a standalone
  idea in a conversation, it deserves its own page.
- File names are lowercase kebab-case stems under `wiki/<category>/`, where
  `<category>` is `concepts`, `entities`, or `topics`.
- Titles in frontmatter are the human display form ("Attention", not
  "attention").
- Every page has the required frontmatter fields: `id`, `title`, `created`,
  `updated`, `sources`, `tags`, `aliases`, `schema_version`.
- Bodies start with `# Title` then a one-paragraph definition, then sections
  (`## Heading`) for detail, examples, and relationships.
- Link other pages with `[[stem]]` wikilinks. Use aliases
  (`[[stem|display text]]`) when the stem doesn't fit the sentence.
- Cite the source file in prose when you make a claim:
  "Vaswani et al. introduced transformers in 2017 (see
  `sources/articles/vaswani-2017.pdf`)."

## What the LLM should do

- **Ingest**: summarize the source, create or update one or more pages, and
  add cross-references to any existing pages that are now relevant. Prefer
  updating an existing page over creating a near-duplicate.
- **Query**: answer the question using *only* the retrieved pages. Cite every
  claim with a `[[wikilink]]`. If retrieval doesn't cover the question, say
  so plainly rather than guessing.
- **Lint** (LLM-assisted, v2): flag pages whose claims conflict with newer
  sources, and pages on related topics that don't link to each other.

## What the LLM must never do

- Never rewrite or delete files inside `sources/`.
- Never invent facts. Every claim traces to at least one source.
- Never cite a page that wasn't supplied in the prompt.
- Never change a page's `id` or `created` date.
- Never create a wikilink that targets a page the vault doesn't have.

## Tags

Use a small, stable vocabulary. Current tags:

- `concept` — an abstract idea (e.g. "attention")
- `entity` — a named thing (person, paper, model, product)
- `topic` — a broad area that groups many concepts
- `draft` — page exists but needs more work

Add new tags sparingly and document them here before using them.
