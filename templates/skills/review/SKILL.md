# Skill: Review

## When to use

After ingesting new content, or periodically to audit vault quality. Use
this to catch issues the mechanical lint cannot: factual accuracy, missing
context, poor organization, redundant pages.

## Workflow

1. **Check recent activity** — call `scriptorium_log_tail` to see the
   latest ingest operations.
2. **Read recent pages** — for each recently created/updated page, call
   `scriptorium_read_page` and evaluate:
   - Is the summary accurate and complete?
   - Are wikilinks pointing to the right pages?
   - Are tags appropriate (type tag + domain tags)?
   - Is the content well-organized with clear headings?
   - Does the compiled truth section (above `---`) accurately synthesize
     the evidence in the timeline (below `---`)?
3. **Cross-reference** — call `scriptorium_search` to find related pages
   and check for contradictions or redundancy.
4. **Fix issues** — use `scriptorium_write_page` to correct problems.
5. **Run lint** — call `scriptorium_lint` to verify mechanical rules pass.

## Quality rules

- Review is about judgment, not mechanics — lint handles syntax.
- Focus on: accuracy, completeness, organization, redundancy.
- Never rewrite a page wholesale — make targeted corrections.
- If a page is fundamentally wrong, re-ingest from the source rather
  than manually rewriting.
- Check that every claim in compiled truth has supporting evidence in
  the timeline section.

## Tools used

- `scriptorium_log_tail` — recent activity
- `scriptorium_read_page` — read page content
- `scriptorium_search` — find related pages
- `scriptorium_write_page` — make corrections
- `scriptorium_lint` — verify after changes
- `scriptorium_list_pages` — browse inventory

## Operational self-improvement

After reviewing, consider:
- What quality issues appeared most often?
- Are there patterns in how the LLM structures pages that could be improved?
- Did the ingest prompt produce good page titles and tags?

Log quality patterns:
```
scriptorium_learn_capture {
  "skill": "review",
  "type": "pattern" or "pitfall",
  "key": "quality-pattern-key",
  "insight": "What you observed about content quality",
  "confidence": 7,
  "source": "observed"
}
```
