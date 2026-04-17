-- Migration 002: hook_events compatibility shim (Strategy B).
--
-- Per T1 inventory (scripts/inventory-hook-events-consumers.md):
--   * 0 external writers — every production insert funnels through
--     HooksStore::insert_event_idempotent (hooks_store.rs:488).
--   * 3 raw-SQL readers outside HooksStore (hooks_check.rs:703,
--     tests/hooks_e2e.sh:95, scripts/sqlite3-smoke-test.sh:17) — all
--     SELECT-only and view-compatible.
--   * 4 dashboard handlers insulated by the HooksStore facade.
--
-- Strategy B replaces the physical hook_events table with a VIEW over
-- `logs WHERE source='hook'` plus an INSTEAD OF INSERT trigger that
-- redirects legacy writes. This keeps every existing SELECT and every
-- `.schema` introspection call working unchanged.
--
-- Precondition: run `scriptorium hooks migrate-backfill` (T10) first if
-- the legacy hook_events table holds rows you want to preserve. This
-- migration unconditionally drops the physical table.
--
-- Idempotency: DROP TABLE IF EXISTS + CREATE VIEW IF NOT EXISTS +
-- CREATE TRIGGER IF NOT EXISTS + INSERT OR IGNORE into schema_version
-- all tolerate re-application.

-- ── Step 1: Seed a compat resource for INSTEAD OF INSERT trigger ──────
-- The trigger needs a valid resource_id FK. We pre-create a dedicated
-- resource here so the trigger never has to allocate one on the hot
-- path. `INSERT OR IGNORE` makes re-apply a no-op.
INSERT OR IGNORE INTO resources (attributes, attributes_hash)
VALUES (
    '{"service.name":"hook-events-compat-shim"}',
    'hook-events-compat-shim-v1'
);

-- ── Step 2: Drop legacy physical table (+ cascades its indexes) ───────
-- SQLite drops dependent indexes (idx_hook_events_*) automatically.
DROP TABLE IF EXISTS hook_events;

-- ── Step 3: CREATE VIEW — project the 21 legacy columns from `logs` ──
-- Column mapping mirrors hooks_store.rs:982-1005 (row_to_event).
-- Strings stored in attributes are JSON-serialized; json_extract on a
-- missing key returns NULL, which matches the legacy nullable columns.
CREATE VIEW IF NOT EXISTS hook_events AS
SELECT
    logs.id                                                                 AS id,
    -- ts: convert INTEGER nanos back to ISO-8601 UTC string to match the
    -- legacy RFC3339 format readers expect (hooks_store.rs stores
    -- Utc::now().to_rfc3339()). Millisecond precision is sufficient
    -- for dashboard ordering and string-range comparisons.
    strftime('%Y-%m-%dT%H:%M:%fZ',
             CAST(logs.time_unix_nano AS REAL) / 1000000000.0,
             'unixepoch')                                                   AS ts,
    json_extract(logs.attributes, '$.session_id')                           AS session_id,
    json_extract(logs.attributes, '$.parent_session_id')                    AS parent_session_id,
    -- hook_type: prefer explicit attribute, fall back to body classification.
    COALESCE(
        json_extract(logs.attributes, '$.hook_type'),
        CASE
            WHEN logs.body = 'hook.turn_scored'   THEN 'stop'
            WHEN logs.body = 'hook.subagent_stop' THEN 'subagent-stop'
            WHEN logs.body = 'hook.session_end'   THEN 'session-end'
            ELSE 'stop'
        END
    )                                                                       AS hook_type,
    -- source: legacy column (unrelated to logs.source='hook'). Stored
    -- under attributes.hook_source to avoid the name collision.
    json_extract(logs.attributes, '$.hook_source')                          AS source,
    json_extract(logs.attributes, '$.cwd')                                  AS cwd,
    json_extract(logs.attributes, '$.score')                                AS score,
    json_extract(logs.attributes, '$.threshold')                            AS threshold,
    json_extract(logs.attributes, '$.signals')                              AS signals,
    json_extract(logs.attributes, '$.metrics')                              AS metrics,
    json_extract(logs.attributes, '$.agent_type')                           AS agent_type,
    json_extract(logs.attributes, '$.privacy_veto')                         AS privacy_veto,
    json_extract(logs.attributes, '$.decision')                             AS decision,
    json_extract(logs.attributes, '$.peak_turn_score')                      AS peak_turn_score,
    json_extract(logs.attributes, '$.session_aggregate_score')              AS session_aggregate_score,
    json_extract(logs.attributes, '$.final_score')                          AS final_score,
    json_extract(logs.attributes, '$.turn_count')                           AS turn_count,
    json_extract(logs.attributes, '$.subagent_count')                       AS subagent_count,
    COALESCE(json_extract(logs.attributes, '$.raw_json'), logs.body)        AS raw_json,
    json_extract(logs.attributes, '$.raw_json_hash')                        AS raw_json_hash
FROM logs
WHERE logs.source = 'hook';

-- ── Step 4: INSTEAD OF INSERT trigger ─────────────────────────────────
-- Redirects any legacy INSERT INTO hook_events to the logs table.
-- This path is EXERCISED by HooksStore::insert_event_idempotent and
-- HooksStore::insert_event (both still run their raw INSERT SQL, which
-- now hits the view and fires this trigger).
--
-- dedup_hash is prefixed with 'legacy-shim:' so collisions between
-- shim-routed writes and direct TelemetryStore::insert_log writes are
-- impossible.
CREATE TRIGGER IF NOT EXISTS hook_events_insert
INSTEAD OF INSERT ON hook_events
BEGIN
    INSERT OR IGNORE INTO logs (
        time_unix_nano,
        observed_time_unix_nano,
        severity_number,
        severity_text,
        body,
        trace_id,
        span_id,
        resource_id,
        attributes,
        source,
        dedup_hash
    ) VALUES (
        -- Parse NEW.ts (ISO-8601 string) back to nanos. unixepoch('subsec')
        -- accepts RFC3339; multiply to nanoseconds. Fall back to 'now' if
        -- NEW.ts is NULL (shouldn't happen — hook_events.ts is NOT NULL).
        CAST(unixepoch(COALESCE(NEW.ts, 'now'), 'subsec') * 1000000000 AS INTEGER),
        CAST(unixepoch('subsec') * 1000000000 AS INTEGER),
        9,        -- INFO (OTel severity_number for 'INFO')
        'INFO',
        -- body: canonical event name derived from hook_type.
        CASE NEW.hook_type
            WHEN 'stop'           THEN 'hook.turn_scored'
            WHEN 'subagent-stop'  THEN 'hook.subagent_stop'
            WHEN 'subagent_stop'  THEN 'hook.subagent_stop'
            WHEN 'session-end'    THEN 'hook.session_end'
            WHEN 'session_end'    THEN 'hook.session_end'
            ELSE 'hook.legacy_shim'
        END,
        NULL,     -- trace_id: legacy path has none
        NULL,     -- span_id: legacy path has none
        (SELECT id FROM resources WHERE attributes_hash = 'hook-events-compat-shim-v1'),
        json_object(
            'hook_type',                NEW.hook_type,
            'session_id',               NEW.session_id,
            'parent_session_id',        NEW.parent_session_id,
            'hook_source',              NEW.source,
            'cwd',                      NEW.cwd,
            'score',                    NEW.score,
            'threshold',                NEW.threshold,
            'signals',                  NEW.signals,
            'metrics',                  NEW.metrics,
            'agent_type',               NEW.agent_type,
            'privacy_veto',             NEW.privacy_veto,
            'decision',                 NEW.decision,
            'peak_turn_score',          NEW.peak_turn_score,
            'session_aggregate_score',  NEW.session_aggregate_score,
            'final_score',              NEW.final_score,
            'turn_count',               NEW.turn_count,
            'subagent_count',           NEW.subagent_count,
            'raw_json',                 NEW.raw_json,
            'raw_json_hash',            NEW.raw_json_hash
        ),
        'hook',
        'legacy-shim:' || COALESCE(
            NEW.raw_json_hash,
            NEW.ts || '|' || COALESCE(NEW.hook_type, '') || '|' || COALESCE(NEW.session_id, '')
        )
    );
END;

-- ── Step 5: Record applied version ────────────────────────────────────
INSERT OR IGNORE INTO schema_version (version, applied_at_unix_nano, note)
VALUES (
    2,
    CAST((unixepoch('subsec') * 1000000000) AS INTEGER),
    'hook_events compat shim (Strategy B: view + INSTEAD OF INSERT trigger)'
);
