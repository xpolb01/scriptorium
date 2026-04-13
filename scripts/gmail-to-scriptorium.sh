#!/bin/bash
# Gmail → Scriptorium pipeline orchestrator
#
# This script processes a batch of email IDs that were already fetched
# by the Claude Code gmail-search step. It expects email JSON files
# in the staging directory, one per email.
#
# Usage:
#   # Step 1: Claude Code searches Gmail, writes IDs to batch file
#   # Step 2: Claude Code reads each email, writes JSON to staging
#   # Step 3: Run this script to convert staged JSON → markdown sources
#   bash scripts/gmail-to-scriptorium.sh
#
# Directories:
#   ~/.scriptorium-gmail-export/staging/   - raw email JSON (from Claude)
#   ~/.scriptorium-gmail-export/sources/   - markdown source files (output)
#   ~/.scriptorium-gmail-export/attachments/ - downloaded attachments
#   ~/.scriptorium-gmail-export/processed/ - IDs already processed

set -euo pipefail

BASE_DIR="$HOME/.scriptorium-gmail-export"
STAGING="$BASE_DIR/staging"
SOURCES="$BASE_DIR/sources"
ATTACHMENTS="$BASE_DIR/attachments"
PROCESSED="$BASE_DIR/processed"
LOG="$BASE_DIR/export.log"

mkdir -p "$STAGING" "$SOURCES" "$ATTACHMENTS" "$PROCESSED"

log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }

TOTAL=0
WRITTEN=0
SKIPPED=0
ERRORS=0

for json_file in "$STAGING"/*.json; do
    [ -f "$json_file" ] || continue
    TOTAL=$((TOTAL + 1))

    ID=$(jq -r '.id' "$json_file" 2>/dev/null)
    if [ -z "$ID" ] || [ "$ID" = "null" ]; then
        log "ERROR: no id in $json_file"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # Skip if already processed
    if [ -f "$PROCESSED/$ID" ]; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    SUBJECT=$(jq -r '.subject // "No Subject"' "$json_file")
    FROM=$(jq -r '.from // "unknown"' "$json_file")
    TO=$(jq -r '.to // ""' "$json_file")
    DATE_RAW=$(jq -r '.date // ""' "$json_file")
    BODY=$(jq -r '.body // ""' "$json_file")
    THREAD_ID=$(jq -r '.threadId // ""' "$json_file")
    ATTACH_COUNT=$(jq -r '.attachments // [] | length' "$json_file")
    ATTACH_LIST=$(jq -r '.attachments // [] | .[] | "- \(.name) (\(.size))"' "$json_file" 2>/dev/null || echo "")

    # Parse date for filename prefix
    DATE_PREFIX=$(echo "$DATE_RAW" | grep -oE '[0-9]{1,2} [A-Z][a-z]{2} [0-9]{4}' | head -1 | \
        xargs -I{} date -j -f "%d %b %Y" {} "+%Y-%m-%d" 2>/dev/null || \
        echo "$DATE_RAW" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -1 || \
        echo "unknown-date")

    # Create safe slug from subject
    SLUG=$(echo "$SUBJECT" | \
        tr '[:upper:]' '[:lower:]' | \
        sed 's/^re: *//;s/^fwd: *//' | \
        sed 's/[^a-z0-9]/-/g' | \
        sed 's/--*/-/g;s/^-//;s/-$//' | \
        head -c 80)

    FILENAME="${DATE_PREFIX}-${SLUG}.md"

    # Deduplicate: if same thread already has a file, append to it
    # For now, just use unique filenames per message
    if [ -f "$SOURCES/$FILENAME" ]; then
        FILENAME="${DATE_PREFIX}-${SLUG}-${ID:0:8}.md"
    fi

    # Write markdown source
    {
        echo "---"
        echo "source: gmail"
        echo "message_id: $ID"
        echo "thread_id: $THREAD_ID"
        # Escape quotes in subject for YAML
        echo "subject: \"$(echo "$SUBJECT" | sed 's/"/\\"/g')\""
        echo "from: \"$(echo "$FROM" | sed 's/"/\\"/g')\""
        echo "to: \"$(echo "$TO" | sed 's/"/\\"/g')\""
        echo "date: \"$DATE_RAW\""
        echo "attachment_count: $ATTACH_COUNT"
        echo "---"
        echo ""
        echo "# $SUBJECT"
        echo ""
        echo "**From:** $FROM"
        echo "**To:** $TO"
        echo "**Date:** $DATE_RAW"
        if [ -n "$ATTACH_LIST" ]; then
            echo ""
            echo "**Attachments:**"
            echo "$ATTACH_LIST"
        fi
        echo ""
        echo "---"
        echo ""
        echo "$BODY"
    } > "$SOURCES/$FILENAME"

    touch "$PROCESSED/$ID"
    WRITTEN=$((WRITTEN + 1))
    log "OK: $FILENAME ($FROM)"
done

log "Done: $TOTAL staged, $WRITTEN written, $SKIPPED skipped, $ERRORS errors"
echo ""
echo "Sources ready at: $SOURCES"
echo "Total source files: $(ls "$SOURCES"/*.md 2>/dev/null | wc -l | tr -d ' ')"
echo ""
echo "Next step: scriptorium bulk-ingest $SOURCES"
