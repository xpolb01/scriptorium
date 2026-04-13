#!/bin/bash
# Bulk download Gmail emails using the Gmail MCP's OAuth token.
# Refreshes the token automatically before each run.
#
# Usage:
#   bash scripts/gmail-bulk-download.sh [--limit N]
#
# Reads IDs from ~/.scriptorium-gmail-export/all-ids.txt
# Writes JSON to ~/.scriptorium-gmail-export/staging/<id>.json

set -euo pipefail

BASE_DIR="$HOME/.scriptorium-gmail-export"
IDS_FILE="$BASE_DIR/all-ids.txt"
STAGING="$BASE_DIR/staging"
LOG="$BASE_DIR/download.log"

mkdir -p "$STAGING"

LIMIT=999999
if [ "${1:-}" = "--limit" ]; then
    LIMIT=${2:-50}
fi

# --- Refresh OAuth token from Gmail MCP credentials ---
CREDS="$HOME/.gmail-mcp/gcp-oauth.keys.json"
TOKENS="$HOME/.gmail-mcp/credentials.json"

CLIENT_ID=$(node -e "const d=JSON.parse(require('fs').readFileSync('$CREDS','utf8')); console.log(d.installed.client_id)")
CLIENT_SECRET=$(node -e "const d=JSON.parse(require('fs').readFileSync('$CREDS','utf8')); console.log(d.installed.client_secret)")
REFRESH_TOKEN=$(node -e "const d=JSON.parse(require('fs').readFileSync('$TOKENS','utf8')); console.log(d.refresh_token)")

TOKEN=$(curl -sS -X POST https://oauth2.googleapis.com/token \
    -d "client_id=$CLIENT_ID" \
    -d "client_secret=$CLIENT_SECRET" \
    -d "refresh_token=$REFRESH_TOKEN" \
    -d "grant_type=refresh_token" | \
    node -e "const d=JSON.parse(require('fs').readFileSync(0,'utf8')); if(d.access_token) console.log(d.access_token); else { console.error('Token refresh failed:', d.error); process.exit(1); }")

API="https://gmail.googleapis.com/gmail/v1/users/me/messages"

log() { echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }

TOTAL=$(wc -l < "$IDS_FILE" | tr -d ' ')
DOWNLOADED=0
SKIPPED=0
ERRORS=0
COUNT=0

log "Starting bulk download: $TOTAL IDs, limit=$LIMIT"

while IFS= read -r ID; do
    [ -z "$ID" ] && continue
    COUNT=$((COUNT + 1))

    if [ "$COUNT" -gt "$LIMIT" ]; then
        break
    fi

    # Skip if already staged
    if [ -f "$STAGING/$ID.json" ]; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    # Fetch full message
    HTTP_CODE=$(curl -sS -o "/tmp/gmail-msg-$ID.json" -w "%{http_code}" \
        -H "Authorization: Bearer $TOKEN" \
        "$API/$ID?format=full" 2>/dev/null)

    if [ "$HTTP_CODE" != "200" ]; then
        log "ERROR [$HTTP_CODE]: $ID"
        ERRORS=$((ERRORS + 1))
        rm -f "/tmp/gmail-msg-$ID.json"
        if [ "$HTTP_CODE" = "429" ]; then
            log "Rate limited, sleeping 5s..."
            sleep 5
        fi
        if [ "$HTTP_CODE" = "401" ]; then
            log "Token expired, refreshing..."
            TOKEN=$(curl -sS -X POST https://oauth2.googleapis.com/token \
                -d "client_id=$CLIENT_ID" \
                -d "client_secret=$CLIENT_SECRET" \
                -d "refresh_token=$REFRESH_TOKEN" \
                -d "grant_type=refresh_token" | \
                node -e "const d=JSON.parse(require('fs').readFileSync(0,'utf8')); console.log(d.access_token || '')")
            if [ -z "$TOKEN" ]; then
                log "FATAL: token refresh failed"
                break
            fi
        fi
        continue
    fi

    # Parse with node
    node -e "
const fs = require('fs');
const msg = JSON.parse(fs.readFileSync('/tmp/gmail-msg-$ID.json', 'utf8'));

function getHeader(headers, name) {
    const h = (headers || []).find(h => h.name.toLowerCase() === name.toLowerCase());
    return h ? h.value : '';
}

function getBody(payload) {
    if (!payload) return '';
    if (payload.body && payload.body.data) {
        return Buffer.from(payload.body.data, 'base64url').toString('utf8');
    }
    if (payload.parts) {
        for (const part of payload.parts) {
            if (part.mimeType === 'text/plain' && part.body && part.body.data) {
                return Buffer.from(part.body.data, 'base64url').toString('utf8');
            }
        }
        for (const part of payload.parts) {
            if (part.mimeType === 'text/html' && part.body && part.body.data) {
                return Buffer.from(part.body.data, 'base64url').toString('utf8')
                    .replace(/<style[^>]*>[\s\S]*?<\/style>/gi, '')
                    .replace(/<[^>]+>/g, ' ')
                    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&')
                    .replace(/&lt;/g, '<').replace(/&gt;/g, '>')
                    .replace(/&quot;/g, '\"').replace(/&#39;/g, \"'\")
                    .replace(/[ \t]+/g, ' ').replace(/\n\s*\n\s*\n/g, '\n\n')
                    .trim();
            }
        }
        for (const part of payload.parts) {
            if (part.mimeType && part.mimeType.startsWith('multipart/')) {
                const nested = getBody(part);
                if (nested) return nested;
            }
        }
    }
    return '';
}

function getAttachments(payload) {
    const attachments = [];
    function walk(parts) {
        if (!parts) return;
        for (const part of parts) {
            if (part.filename && part.body && part.body.attachmentId) {
                attachments.push({
                    name: part.filename,
                    type: part.mimeType || 'application/octet-stream',
                    size: part.body.size || 0,
                    id: part.body.attachmentId
                });
            }
            if (part.parts) walk(part.parts);
        }
    }
    if (payload) walk(payload.parts);
    return attachments;
}

const headers = msg.payload ? msg.payload.headers : [];
const email = {
    id: msg.id,
    threadId: msg.threadId,
    subject: getHeader(headers, 'Subject'),
    from: getHeader(headers, 'From'),
    to: getHeader(headers, 'To'),
    cc: getHeader(headers, 'Cc'),
    date: getHeader(headers, 'Date'),
    body: getBody(msg.payload),
    attachments: getAttachments(msg.payload),
    labels: msg.labelIds || [],
    snippet: msg.snippet || ''
};

fs.writeFileSync('$STAGING/' + msg.id + '.json', JSON.stringify(email, null, 2));
    " 2>/dev/null && {
        DOWNLOADED=$((DOWNLOADED + 1))
        rm -f "/tmp/gmail-msg-$ID.json"
        if [ $((DOWNLOADED % 100)) -eq 0 ]; then
            log "Progress: $DOWNLOADED/$TOTAL downloaded ($SKIPPED skipped, $ERRORS errors)"
        fi
    } || {
        log "PARSE ERROR: $ID"
        ERRORS=$((ERRORS + 1))
        rm -f "/tmp/gmail-msg-$ID.json"
    }

done < "$IDS_FILE"

log "Done: $DOWNLOADED downloaded, $SKIPPED skipped, $ERRORS errors"
echo ""
echo "Staged: $(ls "$STAGING"/*.json 2>/dev/null | wc -l | tr -d ' ') files at $STAGING"
