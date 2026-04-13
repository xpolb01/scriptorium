# Skill: Learn

## When to use

When you want to view, search, or manage the self-learning journal. Also
when the user asks "what have you learned?" or "what mistakes have you made?"

## Workflow

### View recent learnings
1. Call `scriptorium_learn_search` with an empty query to list recent entries.
2. Present them grouped by type (pitfall, pattern, preference, etc.).

### Search learnings
1. Call `scriptorium_learn_search` with keywords.
2. Present matching entries with their confidence scores.

### Capture a new learning
1. Identify the insight — what was learned?
2. Classify: `pattern` (what works), `pitfall` (what to avoid),
   `preference` (user preference), `correction` (user corrected you),
   `domain_knowledge` (codebase-specific fact).
3. Assign confidence 1-10 (8+ for user-stated, 5-7 for observed).
4. Call `scriptorium_learn_capture` with the structured entry.

### Prune stale learnings
1. Call `scriptorium_learn_search` to review all entries.
2. Identify entries that are outdated (files deleted, patterns changed).
3. Note: confidence decays 1 point per 30 days for observed/inferred
   learnings. Entries at confidence 0 are effectively expired.

## Quality rules

- Only log learnings that would save 5+ minutes in a future session.
- Be specific: "Claude omits log_entry on 12KB+ ingests" is better than
  "ingest sometimes fails."
- Use the `key` field for dedup — same key+type = latest entry wins.
- User-stated learnings (`source: "user-stated"`) do not decay.
- Never log sensitive information (API keys, credentials, PII).

## Tools used

- `scriptorium_learn_capture` — add a new learning
- `scriptorium_learn_search` — search/list learnings
- `scriptorium_learn_retrieve` — get relevant learnings for a context

## The 5-minute test

Before capturing a learning, ask: "Would knowing this save 5+ minutes in a
future session?" If yes, log it. If no, it's ephemeral context, not a
durable learning.
