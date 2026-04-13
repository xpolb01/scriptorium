#!/bin/bash
# Gmail → Scriptorium export script
# Fetches emails via Gmail MCP (through Claude Code), saves as markdown sources.
#
# Usage (run from Claude Code via Bash tool):
#   bash scripts/gmail-export.sh <batch_file>
#
# The batch_file is a newline-delimited list of Gmail message IDs.
# This script is designed to be called by Claude Code which handles
# the MCP interaction. The actual workflow is:
#
# 1. Claude searches Gmail via MCP, saves IDs to a batch file
# 2. Claude reads each email via MCP, calls this script to write it
# 3. This script formats and saves the markdown source file
#
# But since MCP tools can only be called from Claude (not from bash),
# this script handles the LOCAL side: writing markdown files from
# pre-fetched email data passed via stdin.

set -euo pipefail

OUTPUT_DIR="${1:?Usage: gmail-export.sh <output_dir>}"
mkdir -p "$OUTPUT_DIR" "$OUTPUT_DIR/attachments"

# Read JSON email data from stdin (one email object per call)
# Expected format: { "id": "...", "threadId": "...", "subject": "...",
#   "from": "...", "to": "...", "date": "...", "body": "...",
#   "attachments": [{"name": "...", "id": "..."}] }

EMAIL_JSON=$(cat)

ID=$(echo "$EMAIL_JSON" | jq -r '.id')
THREAD_ID=$(echo "$EMAIL_JSON" | jq -r '.threadId // empty')
SUBJECT=$(echo "$EMAIL_JSON" | jq -r '.subject // "No Subject"')
FROM=$(echo "$EMAIL_JSON" | jq -r '.from // "unknown"')
TO=$(echo "$EMAIL_JSON" | jq -r '.to // ""')
DATE=$(echo "$EMAIL_JSON" | jq -r '.date // ""')
BODY=$(echo "$EMAIL_JSON" | jq -r '.body // ""')
ATTACHMENTS=$(echo "$EMAIL_JSON" | jq -r '.attachments // [] | length')

# Create a safe filename from subject
SLUG=$(echo "$SUBJECT" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/-/g' | sed 's/--*/-/g' | sed 's/^-//' | sed 's/-$//' | head -c 80)
DATE_PREFIX=$(echo "$DATE" | grep -oE '^[0-9]{4}-[0-9]{2}-[0-9]{2}' 2>/dev/null || date -j -f "%a, %d %b %Y" "$(echo "$DATE" | sed 's/ [0-9:+-].*$//')" "+%Y-%m-%d" 2>/dev/null || echo "unknown-date")
FILENAME="${DATE_PREFIX}-${SLUG}.md"

# Skip if already exported
if [ -f "$OUTPUT_DIR/$FILENAME" ]; then
    echo "SKIP: $FILENAME (already exists)"
    exit 0
fi

# Write markdown source file
cat > "$OUTPUT_DIR/$FILENAME" << HEREDOC
---
source: gmail
message_id: ${ID}
thread_id: ${THREAD_ID}
subject: "${SUBJECT}"
from: "${FROM}"
to: "${TO}"
date: "${DATE}"
attachments: ${ATTACHMENTS}
---

# ${SUBJECT}

**From:** ${FROM}
**To:** ${TO}
**Date:** ${DATE}

---

${BODY}
HEREDOC

echo "OK: $FILENAME"
