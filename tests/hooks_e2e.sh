#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TMPDIR_E2E="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_E2E"' EXIT

PASS=0
FAIL=0

pass() { PASS=$((PASS + 1)); printf "  ✓ %s\n" "$1"; }
fail() { FAIL=$((FAIL + 1)); printf "  ✗ %s\n" "$1"; }

echo "=== Hooks E2E Integration Test ==="
echo "Working dir: $TMPDIR_E2E"
echo ""

# ── Step 1: Generate synthetic JSONL (50 valid + 3 malformed) ─────────
JSONL="$TMPDIR_E2E/synthetic.jsonl"
DB="$TMPDIR_E2E/e2e-test.sqlite"

for i in $(seq 1 20); do
  printf '{"ts":"2026-04-15T10:%02d:00Z","session_id":"e2e-sess-1","cwd":"/tmp","scored":{"score":%d,"signals":["sig-%d"],"metrics":{}},"threshold":6,"decision":"ingest"}\n' \
    "$i" "$((i % 10))" "$i" >> "$JSONL"
done

for i in $(seq 1 15); do
  printf '{"ts":"2026-04-15T11:%02d:00Z","session_id":"e2e-sess-2","parent_session_id":"e2e-sess-1","source":"subagent-stop","agent_type":"explore","cwd":"/tmp","scored":{"score":%d,"signals":["nav"],"metrics":{}},"threshold":6,"decision":"skip"}\n' \
    "$i" "$((i % 5))" >> "$JSONL"
done

for i in $(seq 1 15); do
  printf '{"ts":"2026-04-15T12:%02d:00Z","session_id":"e2e-agg-%d","action":"session-aggregate","peak_turn":%d,"session_aggregate":%d,"final_score":%d,"turn_count":%d,"subagent_count":%d,"decision":"ingest"}\n' \
    "$i" "$i" "$((i * 2))" "$((i * 3))" "$((i + 1))" "$i" "$((i % 3))" >> "$JSONL"
done

echo "NOT JSON AT ALL" >> "$JSONL"
echo "{incomplete" >> "$JSONL"
echo "" >> "$JSONL"

TOTAL_LINES=$(wc -l < "$JSONL" | tr -d ' ')
echo "Generated $TOTAL_LINES lines ($JSONL)"
echo ""

# ── Step 2: Dry-run — verify no DB created ────────────────────────────
echo "--- Step 2: Dry-run migrate ---"
cd "$SCRIPT_DIR"
OUTPUT=$(cargo run --quiet -- hooks migrate --jsonl "$JSONL" --db "$DB" --dry-run 2>&1)

if [ ! -f "$DB" ]; then
  pass "Dry-run did not create DB file"
else
  SIZE=$(stat -f%z "$DB" 2>/dev/null || stat -c%s "$DB" 2>/dev/null || echo "0")
  if [ "$SIZE" -le 0 ] 2>/dev/null; then
    pass "Dry-run DB is empty"
  else
    fail "Dry-run created a non-empty DB ($SIZE bytes)"
  fi
fi

if echo "$OUTPUT" | grep -q "Imported: 50"; then
  pass "Dry-run reports 50 imported"
else
  fail "Dry-run import count mismatch: $OUTPUT"
fi
echo ""

# ── Step 3: Actual migrate ────────────────────────────────────────────
echo "--- Step 3: Actual migrate ---"
rm -f "$DB"
OUTPUT=$(cargo run --quiet -- hooks migrate --jsonl "$JSONL" --db "$DB" 2>&1)

if [ -f "$DB" ]; then
  pass "DB file created"
else
  fail "DB file not created"
fi

if echo "$OUTPUT" | grep -q "Imported: 50"; then
  pass "Actual migrate reports 50 imported"
else
  fail "Actual migrate count mismatch: $OUTPUT"
fi

if echo "$OUTPUT" | grep -q "Malformed: 3"; then
  pass "3 malformed lines detected"
else
  fail "Malformed count mismatch: $OUTPUT"
fi
echo ""

# ── Step 4: Verify row count via sqlite3 ──────────────────────────────
echo "--- Step 4: Verify DB row count ---"
if command -v sqlite3 &>/dev/null; then
  COUNT=$(sqlite3 "$DB" 'SELECT COUNT(*) FROM hook_events')
  if [ "$COUNT" -eq 50 ]; then
    pass "DB has exactly 50 rows"
  else
    fail "DB has $COUNT rows (expected 50)"
  fi
else
  echo "  ⚠ sqlite3 not in PATH, skipping row count check"
fi
echo ""

# ── Step 5: Idempotent re-import ──────────────────────────────────────
echo "--- Step 5: Idempotent re-import ---"
OUTPUT=$(cargo run --quiet -- hooks migrate --jsonl "$JSONL" --db "$DB" 2>&1)

if echo "$OUTPUT" | grep -q "Imported: 0"; then
  pass "Re-import: 0 new rows (idempotent)"
else
  fail "Re-import should have 0 imported: $OUTPUT"
fi

if echo "$OUTPUT" | grep -q "Duplicates: 50"; then
  pass "Re-import: 50 duplicates detected"
else
  fail "Re-import duplicate count mismatch: $OUTPUT"
fi
echo ""

# ── Step 6: hooks check --quick ───────────────────────────────────────
echo "--- Step 6: hooks check --quick ---"
cargo run --quiet -- hooks check --quick 2>&1 && CHECK_EXIT=$? || CHECK_EXIT=$?

if [ "$CHECK_EXIT" -eq 0 ] || [ "$CHECK_EXIT" -eq 1 ] || [ "$CHECK_EXIT" -eq 2 ]; then
  pass "hooks check --quick exited $CHECK_EXIT (no crash)"
else
  fail "hooks check --quick crashed with exit $CHECK_EXIT"
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────
echo "=== Results: $PASS passed, $FAIL failed ==="
if [ "$FAIL" -gt 0 ]; then
  exit 1
fi
exit 0
