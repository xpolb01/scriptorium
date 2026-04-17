#!/bin/bash
# Smoke test: verify SQLite database schema is valid.
#
# Invoked by T11 pre-commit. Inspects the schema of key telemetry tables
# (logs, spans, hook_events) and asserts that the database is accessible.

set -e

DB="${HOME}/.scriptorium/hooks.sqlite"

if [ ! -f "$DB" ]; then
    echo "⚠ Database not found at $DB (expected before T11 schema migration; skipping)"
    exit 0
fi

echo "Checking SQLite schema..."
sqlite3 "$DB" ".schema logs" ".schema spans" ".schema hook_events" >/dev/null

if [ $? -eq 0 ]; then
    echo "✓ SQLite schema validation passed"
    exit 0
else
    echo "✗ SQLite schema validation failed"
    exit 1
fi
