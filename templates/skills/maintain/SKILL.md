# Skill: Maintain (Dream Cycle)

## When to use

Run periodically (daily or weekly) to keep the vault healthy. Can also be
triggered after large batch operations or when the doctor reports issues.

## Workflow

1. **Health check** — call `scriptorium_doctor` to get the current vault
   health status.
2. **Run maintenance** — call `scriptorium_maintain` with `fix: false` to
   get a report of all issues without changing anything.
3. **Review the report**:
   - **Stale pages**: pages whose source material is newer than the page.
     These need re-ingestion to incorporate new information.
   - **Stale embeddings**: pages whose content changed since last embed.
     Fix with `scriptorium_maintain fix: true`.
   - **Broken links**: wikilinks pointing to non-existent pages.
     Either create the missing page or fix the link.
   - **Orphan pages**: pages with no inbound links. Consider adding
     wikilinks from related pages.
   - **Missing tags**: pages without a type tag.
4. **Auto-fix** — call `scriptorium_maintain` with `fix: true` to
   re-embed stale pages and fix bad timestamps.
5. **Manual fixes** — for issues that require judgment (broken links,
   orphans, stale pages), use `scriptorium_write_page` to update content.

## Quality rules

- Never auto-fix issues that require judgment (broken links, orphans).
- Always run `fix: false` first to preview before `fix: true`.
- After manual fixes, run `scriptorium_lint` to verify.
- Stale pages should be re-ingested, not manually edited (re-run
  `scriptorium_ingest` on the updated source).

## Tools used

- `scriptorium_doctor` — vault health check
- `scriptorium_maintain` — run all maintenance tasks
- `scriptorium_lint` — mechanical lint rules
- `scriptorium_search` — find related pages for orphan linking
- `scriptorium_write_page` — manual page fixes
- `scriptorium_ingest` — re-ingest stale sources

## Operational self-improvement

After completing maintenance, consider:
- What was the most common issue type?
- Are there recurring patterns (same pages always stale, same links breaking)?
- Did the auto-fix resolve everything it should have?

Log recurring patterns:
```
scriptorium_learn_capture {
  "skill": "maintain",
  "type": "pattern",
  "key": "recurring-issue-type",
  "insight": "Description of the recurring pattern",
  "confidence": 7,
  "source": "observed"
}
```
