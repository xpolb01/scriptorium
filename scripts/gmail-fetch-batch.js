#!/usr/bin/env node
// Gmail batch processor: reads email IDs from stdin (one per line),
// reads email JSON from pre-fetched files, and writes staging JSON.
//
// This script is called AFTER Claude Code has fetched emails via MCP
// and written the raw results. It parses the MCP output format into
// clean JSON for the pipeline.
//
// Usage:
//   # Claude fetches batch, saves to file, then:
//   node scripts/gmail-fetch-batch.js < batch-ids.txt
//
// But in practice, Claude drives the whole flow directly.

const fs = require('fs');
const path = require('path');

const STAGING = path.join(process.env.HOME, '.scriptorium-gmail-export', 'staging');
fs.mkdirSync(STAGING, { recursive: true });

// Parse email text output from Gmail MCP read_email into structured JSON
function parseEmailOutput(text, messageId) {
    const lines = text.split('\n');
    const email = { id: messageId, attachments: [] };

    for (let i = 0; i < lines.length; i++) {
        const line = lines[i];
        if (line.startsWith('Thread ID: ')) email.threadId = line.slice(11).trim();
        else if (line.startsWith('Subject: ')) email.subject = line.slice(9).trim();
        else if (line.startsWith('From: ')) email.from = line.slice(6).trim();
        else if (line.startsWith('To: ')) email.to = line.slice(4).trim();
        else if (line.startsWith('Date: ')) email.date = line.slice(6).trim();
        else if (line.startsWith('Attachments (')) {
            // Parse attachment lines: "- filename (type, size, ID: xxx)"
            for (let j = i + 1; j < lines.length; j++) {
                const m = lines[j].match(/^- (.+?) \((.+?), (.+?), ID: (.+?)\)$/);
                if (m) {
                    email.attachments.push({
                        name: m[1], type: m[2], size: m[3], id: m[4]
                    });
                } else break;
            }
        }
    }

    // Body is everything between the Date line and Attachments (or end)
    const dateIdx = lines.findIndex(l => l.startsWith('Date: '));
    const attachIdx = lines.findIndex(l => l.startsWith('Attachments ('));
    const bodyStart = dateIdx >= 0 ? dateIdx + 1 : 0;
    const bodyEnd = attachIdx >= 0 ? attachIdx : lines.length;

    // Skip the first blank line after Date
    let start = bodyStart;
    while (start < bodyEnd && lines[start].trim() === '') start++;

    email.body = lines.slice(start, bodyEnd).join('\n').trim();

    return email;
}

// Write a single email to staging
function stageEmail(email) {
    const outPath = path.join(STAGING, `${email.id}.json`);
    if (fs.existsSync(outPath)) {
        return 'skip';
    }
    fs.writeFileSync(outPath, JSON.stringify(email, null, 2));
    return 'ok';
}

module.exports = { parseEmailOutput, stageEmail, STAGING };

// If run directly, read email JSON objects from stdin
if (require.main === module) {
    const input = fs.readFileSync(0, 'utf8').trim();
    if (!input) {
        console.log('No input. Pipe email JSON or use as module.');
        process.exit(0);
    }
    try {
        const email = JSON.parse(input);
        const result = stageEmail(email);
        console.log(`${result}: ${email.id} — ${email.subject}`);
    } catch (e) {
        console.error('Parse error:', e.message);
        process.exit(1);
    }
}
