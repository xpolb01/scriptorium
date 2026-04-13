# Skill: Ingest

## When to use

When the user provides a source file (markdown, text, PDF, URL) to add to
the vault. Also when you observe knowledge worth persisting from a
conversation.

## Workflow

1. **Check for duplicates** — call `scriptorium_search` with key terms from
   the source to see if the vault already covers this topic.
2. **Dry run first** — call `scriptorium_ingest` with `dry_run: true` to
   preview what pages would be created/updated without committing.
3. **Review the plan** — check that page titles are descriptive, tags are
   appropriate, and no near-duplicate pages would be created.
4. **Commit** — call `scriptorium_ingest` with `dry_run: false` to execute.
5. **Verify** — call `scriptorium_read_page` on the created pages to
   confirm content quality.

## Quality rules

- Never create a page that duplicates an existing one. Prefer `update` over
  `create` when a related page already exists.
- Every page must have at least one type tag (`concept`, `entity`, `topic`,
  `pattern`, `playbook`, `reference`, `paper`, `talk`, `note`).
- Every page must have at least one domain tag (`domain/...`).
- Every wikilink must point to an existing page.
- Source files are immutable — never modify `sources/`.
- The `log_entry` must be a single descriptive line.

## Tools used

- `scriptorium_search` — find existing pages to avoid duplicates
- `scriptorium_ingest` — run the ingest pipeline (with `dry_run` option)
- `scriptorium_read_page` — verify created/updated pages
- `scriptorium_list_pages` — check vault inventory
- `scriptorium_lint` — validate the vault after ingest

## Operational self-improvement

After completing this skill, consider:
- Did the LLM produce a valid IngestPlan on the first try?
- Were there any missing fields or truncation errors?
- Did the dry run reveal any issues that needed correction?

If you learned something non-obvious, capture it:
```
scriptorium_learn_capture {
  "skill": "ingest",
  "type": "pitfall" or "pattern",
  "key": "short-kebab-key",
  "insight": "What you learned",
  "confidence": 7,
  "source": "observed"
}
```
