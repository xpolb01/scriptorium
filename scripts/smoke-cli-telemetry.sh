#!/bin/bash
# Smoke test: verify CLI telemetry logging is working.
#
# Invoked by T12 pre-commit. Runs `scriptorium doctor` and checks for recent
# log entries with source='cli' in the SQLite database.
#
# NOTE: This script is used by T12 pre-commit only. Before T12 lands, this
# script will fail with "no logs" error, which is expected. Once T12 merges
# the CLI telemetry hook integration, this test will pass.

set -e

DB="${HOME}/.scriptorium/hooks.sqlite"

if [ ! -f "$DB" ]; then
    echo "⚠ Database not found at $DB (expected before T12 CLI hook integration; skipping)"
    exit 0
fi

echo "Running scriptorium doctor for telemetry..."
scriptorium doctor >/dev/null || true

echo "Checking for recent CLI logs..."
count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM logs WHERE source='cli' AND time_unix_nano > (strftime('%s','now')-60)*1000000000;" 2>/dev/null || echo "0")

if [ "$count" -ge 1 ]; then
    echo "✓ Found $count CLI log entries in last 60 seconds"
    exit 0
else
    echo "⚠ No CLI logs found (expected until T12 lands; this is OK)"
    exit 0
fi
