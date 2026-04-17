-- Migration 001: OTel-shaped telemetry schema (initial)
--
-- Creates the four telemetry tables plus dashboard-critical indexes.
-- Idempotent: every CREATE uses IF NOT EXISTS, and the schema_version
-- INSERT uses OR IGNORE so re-applying is a no-op.
--
-- SQLite conventions followed (see plan lines 318-320):
--   * JSON columns are TEXT with CHECK(json_valid(...)), never a "JSON" type.
--   * Timestamps are INTEGER nanos-since-epoch (i64). Never floating-point.
--   * Field access elsewhere uses json_extract(col, '$.path') for portability.
--
-- This migration MUST NOT drop or alter `hook_events`. That compatibility
-- work lives in migration 002 (task T11).

-- ── schema_version ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS schema_version (
    version              INTEGER PRIMARY KEY,
    applied_at_unix_nano INTEGER NOT NULL,
    note                 TEXT
);

-- ── resources ──────────────────────────────────────────────────────────
-- One row per unique (service.name, service.version, host.name, pid, vault)
-- Resource attribute combo. Dedup via `attributes_hash` (sha256 of canonical
-- JSON). The `attributes` column holds the full JSON object for debugging
-- and for OTLP export fidelity.
CREATE TABLE IF NOT EXISTS resources (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    attributes       TEXT NOT NULL CHECK(json_valid(attributes)),
    attributes_hash  TEXT NOT NULL UNIQUE
);

-- ── spans ──────────────────────────────────────────────────────────────
-- OpenTelemetry Span Data Model. `end_time_unix_nano` is nullable so a
-- process can record a "started" span and close it later (or never — a
-- dangling span surfaces in the dashboard as an unfinished operation).
-- `status_code` values: UNSET | OK | ERROR (matches Status::code()).
-- `kind` values: INTERNAL | SERVER | CLIENT | PRODUCER | CONSUMER.
-- `source` values: hook | cli | mcp | core.
CREATE TABLE IF NOT EXISTS spans (
    span_id              TEXT PRIMARY KEY,
    trace_id             TEXT NOT NULL,
    parent_span_id       TEXT,
    name                 TEXT NOT NULL,
    kind                 TEXT NOT NULL,
    start_time_unix_nano INTEGER NOT NULL,
    end_time_unix_nano   INTEGER,
    status_code          TEXT NOT NULL,
    status_message       TEXT,
    resource_id          INTEGER NOT NULL REFERENCES resources(id),
    attributes           TEXT NOT NULL CHECK(json_valid(attributes)),
    events               TEXT          CHECK(events IS NULL OR json_valid(events)),
    source               TEXT NOT NULL,
    dedup_hash           TEXT UNIQUE
);

-- ── logs ───────────────────────────────────────────────────────────────
-- OpenTelemetry Logs Data Model. Two timestamps: `time_unix_nano` is the
-- event's own timestamp (may be reconstructed during backfill);
-- `observed_time_unix_nano` is when the writer actually saw the record.
-- `severity_number` follows the OTel severity mapping (1..24).
CREATE TABLE IF NOT EXISTS logs (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    time_unix_nano           INTEGER NOT NULL,
    observed_time_unix_nano  INTEGER NOT NULL,
    severity_number          INTEGER NOT NULL,
    severity_text            TEXT,
    body                     TEXT NOT NULL,
    trace_id                 TEXT,
    span_id                  TEXT,
    resource_id              INTEGER NOT NULL REFERENCES resources(id),
    attributes               TEXT NOT NULL CHECK(json_valid(attributes)),
    source                   TEXT NOT NULL,
    dedup_hash               TEXT NOT NULL UNIQUE
);

-- ── Indexes (dashboard-critical per Oracle guidance) ───────────────────
CREATE INDEX IF NOT EXISTS idx_spans_trace    ON spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_start    ON spans(start_time_unix_nano DESC);
CREATE INDEX IF NOT EXISTS idx_spans_source   ON spans(source);
CREATE INDEX IF NOT EXISTS idx_logs_time      ON logs(time_unix_nano DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace     ON logs(trace_id);
CREATE INDEX IF NOT EXISTS idx_logs_source    ON logs(source);
CREATE INDEX IF NOT EXISTS idx_logs_severity  ON logs(severity_number);

-- ── Record applied version (idempotent re-apply sentinel) ──────────────
-- INSERT OR IGNORE: second run is a no-op; COUNT stays at exactly 1.
INSERT OR IGNORE INTO schema_version (version, applied_at_unix_nano, note)
VALUES (
    1,
    CAST((unixepoch('subsec') * 1000000000) AS INTEGER),
    'initial telemetry schema'
);
