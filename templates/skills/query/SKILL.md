# Skill: Query

## When to use

When the user asks a question that the vault might be able to answer, or
when you need to look up information before responding.

## Workflow

1. **Keyword search first** — call `scriptorium_search` with specific terms
   (names, dates, technical terms) to find exact matches.
2. **Hybrid query** — call `scriptorium_query` with the full question for
   semantic + keyword retrieval with RRF fusion.
3. **Read top hits** — for the most relevant results, call
   `scriptorium_read_page` to get full page content (search returns chunks,
   not full pages).
4. **Synthesize** — answer the question using the retrieved context. Cite
   every page you reference by its filename stem.
5. **Flag stale pages** — if the query report includes stale pages, mention
   them to the user so they can re-ingest updated sources.

## Quality rules

- Only cite pages that were in the retrieval set — never hallucinate citations.
- If the vault does not contain enough information, say so explicitly.
- Prefer compiled truth (above `---`) over raw timeline entries for answers.
- When citing, use the page's filename stem (e.g. `hybrid-search-rrf`).
- Short queries (< 3 words) skip multi-query expansion — add context.

## Tools used

- `scriptorium_search` — keyword/vector search for specific terms
- `scriptorium_query` — full hybrid search + LLM synthesis
- `scriptorium_read_page` — read full page content for deep context
- `scriptorium_list_pages` — browse the vault inventory

## Operational self-improvement

After completing this skill, consider:
- Did the search return the expected pages?
- Were there relevant pages that the search missed?
- Was the answer well-supported by citations?

If a search consistently misses relevant content, capture it:
```
scriptorium_learn_capture {
  "skill": "query",
  "type": "pitfall",
  "key": "search-miss-description",
  "insight": "Query X misses page Y because ...",
  "confidence": 6,
  "source": "observed"
}
```
