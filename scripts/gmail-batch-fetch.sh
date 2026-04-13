#!/bin/bash
# Gmail batch fetch: reads message IDs from a file, fetches each email
# via Claude Code's Gmail MCP, and writes JSON to staging.
#
# This script is meant to be SOURCE'd by Claude Code, not run standalone,
# because it needs MCP access. Claude Code drives the loop:
#
#   1. Search Gmail → save IDs to file
#   2. For each ID: read_email via MCP → parse → write staging JSON
#   3. Run gmail-to-scriptorium.sh to convert staging → markdown sources
#   4. Run scriptorium bulk-ingest on the sources
#
# This file documents the ID file format and staging JSON schema.

# ID file: one Gmail message ID per line
# Example:
#   19d591f3d1f03426
#   19d58fbf6e64bbe9

# Staging JSON schema (one file per email at ~/.scriptorium-gmail-export/staging/<id>.json):
# {
#   "id": "19d591f3d1f03426",
#   "threadId": "19d58f015c2e7700",
#   "subject": "Re: Atlassian",
#   "from": "Alice Smith <alice@example.com>",
#   "to": "Bob Jones <bob@example.com>",
#   "date": "Sat, 4 Apr 2026 11:31:39 -0400",
#   "body": "full email body text...",
#   "attachments": [
#     {"name": "file.pdf", "type": "application/pdf", "size": "123 KB", "id": "ANGjdJ..."}
#   ]
# }

echo "This script is documentation only. Claude Code drives the fetch loop via MCP."
echo "See the comments above for the pipeline."
