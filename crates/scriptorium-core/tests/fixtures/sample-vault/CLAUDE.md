# Scriptorium Vault Schema (test fixture)

This fixture drives the end-to-end integration tests for `scriptorium-core`.
It intentionally exercises the edge cases the engine needs to handle:

- pages with normal links, broken links, orphans
- both `wiki/concepts/` and `wiki/topics/` subdirectories
- YAML frontmatter with all required fields plus a custom `extra` key

## Conventions

- Every wiki page has frontmatter with `id`, `title`, `created`, `updated`.
- Link other pages via `[[stem]]` where `stem` is the filename without `.md`.
- Sources live under `sources/`; the LLM never modifies them.
