# `hook_events` Consumer & Writer Inventory

**Task**: T1 of `scriptorium-logging-hardening` plan.
**Purpose**: Enumerate every reader, writer, and schema reference of the
`hook_events` SQLite table so T11 (compatibility shim) can pick a
migration strategy with full knowledge of blast radius.
**Method**: `grep -rn "hook_events"` across the scriptorium repo,
`~/dotfiles/claude/hooks/`, and `~/scriptorium-vault/`, plus targeted
inspection of the `HooksStore` API surface and its callers.

---

## Scope summary

Raw verification grep:

```bash
grep -rn "hook_events" \
  /Users/bogdan/Work/PersonalProjects/scriptorium/crates \
  ~/dotfiles/claude/hooks \
  ~/scriptorium-vault 2>/dev/null | wc -l
# → 35
```

Match distribution:

| Location | Files | Matches | Kind |
|---|---|---|---|
| `scriptorium/crates/scriptorium-core/src/hooks_store.rs` | 1 | 29 | DDL + SELECT/INSERT SQL + doc comment |
| `scriptorium/crates/scriptorium-core/src/hooks_check.rs` | 1 | 3 | 1 raw `SELECT MAX(ts)` + 2 test-only DDL/INSERT |
| `scriptorium/tests/hooks_e2e.sh` | 1 | 1 | Integration test `SELECT COUNT(*)` (outside the `crates/` scope but cited here) |
| `~/dotfiles/claude/hooks/*.sh` | 0 | 0 | **No direct consumers.** Hooks emit via JSONL or `scriptorium hooks log` stdin. |
| `~/scriptorium-vault/**/*.md` | 3 | 3 | Design-doc prose only (no executable reads/writes). |

External writers (anything outside the scriptorium repo writing to the
table) count: **0**. Every row entering `hook_events` does so via the
`HooksStore` Rust API.

---

## Readers

| # | File | Line(s) | Operation | Risk if schema changes | Migration strategy |
|---|---|---|---|---|---|
| R1 | `crates/scriptorium-core/src/hooks_store.rs` | 536, 760, 771, 802 | `SELECT … FROM hook_events` (all 21 columns) in `query_session`, `query_events`, `query_errors` | **High** — column names are hard-coded in SQL strings. | view-compatible (VIEW must project all 21 columns with same names) |
| R2 | `crates/scriptorium-core/src/hooks_store.rs` | 563, 572, 581, 590, 599, 609, 618, 639, 658, 679, 697, 707, 717 | Aggregate SELECTs (`COUNT`, `AVG`, `MAX`, `GROUP BY score`, `GROUP BY hook_type`, signals extraction) in `query_summary` | **High** — depends on `decision`, `privacy_veto`, `score`, `signals`, `hook_type`, `ts` columns. | view-compatible (VIEW must preserve these columns; indexes need to exist on the underlying table for perf) |
| R3 | `crates/scriptorium-core/src/hooks_check.rs` | 703 | `SELECT MAX(ts) FROM hook_events` (read-only connection, **bypasses `HooksStore`**) | **Medium** — only needs `ts`. | view-compatible; flag for T11: this is the only raw-SQL reader outside `hooks_store.rs`. |
| R4 | `crates/scriptorium-cli/src/dashboard.rs` | 78, 113, 162 | Indirect: each HTTP handler (`summary_handler`, `events_handler`, `errors_handler`) re-opens `HooksStore` and calls R1/R2 methods. | Low — insulated by the `HooksStore` facade. | none (follows R1/R2 mitigation) |
| R5 | `tests/hooks_e2e.sh` | 95 | `sqlite3 "$DB" 'SELECT COUNT(*) FROM hook_events'` | Low — integration test only; easy to update. | view-compatible (VIEW makes no changes needed) |
| R6 | `scripts/sqlite3-smoke-test.sh` | 17 | `sqlite3 "$DB" ".schema hook_events"` (pre-commit smoke) | Low — introspection only, expects *any* schema to load. | view-compatible (VIEW has a `.schema` output) or must-update |

**Observation**: every reader outside `hooks_store.rs` (R3, R5, R6) treats
`hook_events` as an opaque table name. None of them care whether it is a
physical table or a view.

---

## Writers

| # | File | Line(s) | Operation | Invoked by | Risk | Migration |
|---|---|---|---|---|---|---|
| W1 | `crates/scriptorium-core/src/hooks_store.rs` | 446 (`INSERT INTO`) | `insert_event` — 20-column raw `INSERT` with busy-retry | Tests only. | Low | centralized; can be redirected by T11. |
| W2 | `crates/scriptorium-core/src/hooks_store.rs` | 488 (`INSERT OR IGNORE`) | `insert_event_idempotent` — dedup on `raw_json_hash` | `scriptorium hooks log` (stdin → `hooks_log_inner` in `main.rs:1584`), JSONL importer internally (`do_import_lines`, `main.rs:962`) | **Primary writer**; all hooks route through here. | centralized; T11 redirects to `logs` via direct call OR via INSTEAD OF INSERT trigger. |
| W3 | `crates/scriptorium-core/src/hooks_store.rs` | 838 (`import_jsonl`) | Wraps W2 inside a transaction. | `scriptorium hooks migrate`/`import` (`main.rs:1529`), dashboard startup when `--jsonl` is passed (`dashboard.rs:251`) | Batch writer. | inherits W2 strategy. |
| W4 | `crates/scriptorium-core/src/hooks_check.rs` | 1493 | `INSERT INTO hook_events (ts, session_id, raw_json_hash)` | Test-only (`sqlite_freshness_recent_passes` and similar). | Low | must-update if the physical table is renamed — or keep working if we keep a physical `hook_events`/VIEW-with-triggers. |
| W5 | `crates/scriptorium-cli/src/main.rs` | 1584 | Indirect: `hooks_log_inner` → `insert_event_idempotent` (W2). | Shell hooks piping JSON to `scriptorium hooks log`. | Low (facade). | none (follows W2). |

**External (non-Rust) writers**: **none**. Shell hooks never write to
`hook_events` directly; they either append to
`~/.claude/artifacts/scriptorium-classifier.jsonl` (later imported via
W3) or stream JSON to `scriptorium hooks log` (W5 → W2).

---

## Schema References

| # | File | Line(s) | Kind | Notes |
|---|---|---|---|---|
| S1 | `crates/scriptorium-core/src/hooks_store.rs` | 360–382 | Canonical `CREATE TABLE IF NOT EXISTS hook_events` (21 columns) | Runs on every `HooksStore::open`. |
| S2 | `crates/scriptorium-core/src/hooks_store.rs` | 384–391 | 4 indexes: `idx_hook_events_session`, `idx_hook_events_ts`, `idx_hook_events_parent` (partial), `idx_hook_events_dedup` (UNIQUE on `raw_json_hash`). | Dedup index is the integrity contract. |
| S3 | `crates/scriptorium-core/src/hooks_store.rs` | 10–33 | Doc comment mirroring S1. | Keep in sync with S1. |
| S4 | `crates/scriptorium-core/src/hooks_check.rs` | 1483–1488 | Test-only minimal `CREATE TABLE hook_events (id, ts, session_id, raw_json_hash)` in a `#[cfg(test)]` block. | Used to test the freshness-check helper without depending on `HooksStore`. |
| S5 | `scripts/sqlite3-smoke-test.sh` | 17 | `.schema hook_events` smoke assertion. | Verifies *some* schema exists; tolerates changes. |
| S6 | `~/scriptorium-vault/sources/articles/42f5f73a3222-2026-04-15-notepad-scriptorium-hooks-health.md` | 65 | Prose mention of `idx_hook_events_dedup`. | Documentation only; no runtime impact. |
| S7 | `~/scriptorium-vault/sources/articles/636228985629-2026-04-17-notepad-scriptorium-logging-hardening.md` | 76 | Prose reference to `hook_events tables`. | Plan mirror; no runtime impact. |
| S8 | `~/scriptorium-vault/sources/sessions/2026-04-17-notepad-scriptorium-logging-hardening.md` | 76 | Same prose reference as S7. | Session notepad mirror. |
| S9 | `.sisyphus/plans/scriptorium-hooks-health.md` | 412–438 | Original design doc; declares the schema inline. | Historical. |

---

## `HooksStore` public API surface

From `crates/scriptorium-core/src/hooks_store.rs`:

| Method | Kind | Callers |
|---|---|---|
| `HooksStore::open(path)` | constructor | `dashboard.rs` (4 sites), `main.rs` (`hooks_log_inner`, `hooks_import_inner`, tests) |
| `HooksStore::in_memory()` | `cfg(test)` only | tests in `hooks_store.rs` |
| `HooksStore::hash_raw(raw)` | static helper | `main.rs:map_raw_to_hook_event` (for `hooks log`), tests |
| `insert_event` | writer | tests only |
| `insert_event_idempotent` | writer | `hooks_log_inner` (`main.rs:1584`), internal `do_import_lines` |
| `import_jsonl` | writer (batch) | `hooks_import_inner` (`main.rs:1529`), `start_dashboard` (`dashboard.rs:251`) |
| `query_session` | reader | tests; no production caller yet (reserved for drill-down UI) |
| `query_summary` | reader | `summary_handler` (`dashboard.rs:79`) |
| `query_events` | reader | `events_handler` (`dashboard.rs:114`) |
| `query_errors` | reader | `errors_handler` (`dashboard.rs:163`) |
| `is_wal_mode` | reader | tests |
| (static) `wal_age_seconds` | private | `query_summary` |

All production readers go through `query_summary`/`query_events`/`query_errors`,
i.e. they are insulated from physical-table details by the facade. The
one exception is `check_sqlite_freshness_at` in `hooks_check.rs`, which
opens its own read-only connection and runs raw SQL.

---

## Shell-hook landscape (`~/dotfiles/claude/hooks/`)

9 scripts (verified by `ls`):

```
auto-stash-before-destructive.sh
lib/ (helpers)
scriptorium-classifier.sh
scriptorium-health-check.sh
scriptorium-notepad-ingest.sh
scriptorium-nudge.sh
scriptorium-recall-nudge.sh
scriptorium-session-end.sh
scriptorium-subagent-start.sh
scriptorium-subagent-stop.sh
```

`grep -rn "hook_events"` returns **zero matches**. None of them open the
SQLite file or issue SQL. The only DB-adjacent touchpoints are:

- `scriptorium-health-check.sh:22` — shells out to `scriptorium hooks check --quick --json` (read-only CLI).
- `scriptorium-session-end.sh:287` — runs `scriptorium ingest …` (unrelated pipeline; vault, not `hooks.sqlite`).

So the only path shell hooks take into the table is via the
`scriptorium hooks log` or `scriptorium hooks migrate` CLI entry points,
both of which bottom out in W2/W3. No shell-side breakage risk from
schema changes.

---

## Migration Strategy Per Consumer

| Consumer | Strategy |
|---|---|
| R1, R2 (`hooks_store.rs` SELECTs) | **view-compatible** — VIEW must export all 21 columns by name. |
| R3 (`hooks_check.rs` raw `SELECT MAX(ts)`) | **view-compatible**; re-evaluate in T11 whether to route through `HooksStore`. |
| R4 (dashboard handlers) | **automatic** — inherits R1/R2 strategy. |
| R5 (`tests/hooks_e2e.sh`) | **view-compatible**; optionally update to read from the new `logs` table in the same test. |
| R6 (`sqlite3-smoke-test.sh`) | **view-compatible** — `.schema` accepts views. |
| W1, W4 (test-only INSERTs) | **triggers-needed** or **must-migrate**: if `hook_events` becomes a VIEW, tests must either (a) write to the new `logs` table directly, or (b) rely on an INSTEAD OF INSERT trigger. |
| W2, W3, W5 (production writers via `HooksStore`) | **must-migrate** when T11 lands: redirect to `logs` in-code. Optionally add an INSTEAD OF INSERT trigger on the view to keep the old API working during transition. |
| S1, S2 (runtime DDL in `HooksStore::init`) | **must-migrate** — replace with VIEW definition or drop, per strategy chosen. |
| S4 (test DDL clone) | **must-migrate** — keep in lock-step with S1/S2. |
| S5 (`.schema` smoke) | no change if VIEW; drop assertion if strategy C. |
| S6–S9 (docs) | **informational update** after T11. |

---

## Recommendation: **Strategy B — `hook_events` becomes a VIEW over `logs` with INSTEAD OF INSERT triggers**

### One-paragraph justification

The grep evidence (35 matches across 5 files) shows the table has
exactly **zero external writers** and exactly **three non-`HooksStore`
readers** (`hooks_check.rs:703`, `tests/hooks_e2e.sh:95`,
`scripts/sqlite3-smoke-test.sh:17`) — all inside the scriptorium repo.
Every production insert is funneled through
`HooksStore::insert_event_idempotent` (W2), itself reached only via the
`scriptorium hooks log` / `scriptorium hooks migrate` CLI, which means
T11 can redirect writes at a single call site. On the read side, the 13
aggregate SELECTs and 3 row-projecting SELECTs inside `query_summary`,
`query_events`, `query_errors`, and `query_session` all use stable
column names — a VIEW that projects those 21 columns from the new
`logs` table (via `json_extract` on the attributes blob or via
materialized columns) preserves the contract with zero code churn in
`hooks_store.rs` SQL strings. Strategy A (dual-write) doubles insert IO
and forces us to keep two schemas in sync for a table that has no
third-party writers needing a grace period — pure cost, no benefit.
Strategy C (drop) is viable only if we rewrite `hooks_check.rs:703`,
`tests/hooks_e2e.sh`, the smoke script, and every `sqlite3` ad-hoc
inspection reflex in documentation; it also forfeits the ability to
`sqlite3 hooks.sqlite "SELECT * FROM hook_events LIMIT 10"` as a fast
debugging idiom. Strategy B gives a clean cut-over: drop the physical
table DDL in `init`, replace it with `CREATE VIEW hook_events AS SELECT
…FROM logs…`, add an `INSTEAD OF INSERT ON hook_events` trigger that
rewrites into `logs`, and leave every existing reader and ad-hoc
tooling working unchanged.

### Concrete T11 action list (derived from this inventory)

1. In `HooksStore::init` (hooks_store.rs:354), replace the `CREATE TABLE
   hook_events` DDL with `CREATE VIEW hook_events` over `logs`; keep
   the 4 index-equivalents on the underlying `logs` table.
2. Add an `INSTEAD OF INSERT ON hook_events` trigger mapping the 20
   inserted columns into `logs`; dedup stays enforced via a `UNIQUE`
   index on `logs(raw_json_hash)` (or its equivalent attribute).
3. Leave R1/R2 SQL strings in `hooks_store.rs` untouched.
4. Update the test-only DDL in `hooks_check.rs:1483` to either mirror
   the new logs/view pair, or rewrite the test to exercise
   `HooksStore::open` instead of hand-rolled `CREATE TABLE`.
5. Leave `tests/hooks_e2e.sh:95` and `scripts/sqlite3-smoke-test.sh:17`
   unchanged — they run against the VIEW.
6. Update the 3 vault/plan prose references after the shim lands.

---

## Evidence artifacts

- `/Users/bogdan/Work/.sisyphus/evidence/task-1-inventory-coverage.txt`
  — grep count vs inventory coverage.
- `/Users/bogdan/Work/.sisyphus/evidence/task-1-recommendation.md`
  — the Recommendation section in isolation, for T11 selection.
