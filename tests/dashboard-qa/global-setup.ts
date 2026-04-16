import { mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const FIXTURES_DIR = resolve(__dirname, '.fixtures');
const SEED_JSONL = resolve(FIXTURES_DIR, 'seed.jsonl');
const SETTINGS_JSON = resolve(FIXTURES_DIR, 'settings.json');
const NONEXISTENT_HOOKS = resolve(FIXTURES_DIR, 'nonexistent-hooks', 'scriptorium-classify.sh');

const SESSION_PRIMARY = '11111111-1111-4111-8111-111111111111';
const SESSION_SUBAGENT = '22222222-2222-4222-8222-222222222222';
const SESSION_AGG_A = '33333333-3333-4333-8333-333333333333';
const SESSION_AGG_B = '44444444-4444-4444-8444-444444444444';

function todayIsoMinute(hour: number, minute: number): string {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(now.getUTCDate()).padStart(2, '0');
  const hh = String(hour).padStart(2, '0');
  const mi = String(minute).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}T${hh}:${mi}:00Z`;
}

function buildSeedJsonl(): string {
  const lines: string[] = [];

  const stopEvents = [
    { ts: todayIsoMinute(9, 5), score: 8, signals: ['nav', 'edit'], decision: 'ingest' },
    { ts: todayIsoMinute(9, 12), score: 3, signals: ['nav'], decision: 'skip' },
    { ts: todayIsoMinute(9, 25), score: 11, signals: ['nav', 'edit', 'test'], decision: 'ingest' },
    { ts: todayIsoMinute(9, 38), score: 2, signals: ['nav'], decision: 'veto' },
    { ts: todayIsoMinute(9, 51), score: 7, signals: ['edit'], decision: 'ingest' },
  ];
  for (const e of stopEvents) {
    lines.push(
      JSON.stringify({
        ts: e.ts,
        session_id: SESSION_PRIMARY,
        cwd: '/tmp/qa',
        scored: { score: e.score, signals: e.signals, metrics: {} },
        threshold: 6,
        decision: e.decision,
      }),
    );
  }

  const subagentEvents = [
    { ts: todayIsoMinute(10, 3), agent_type: 'explore', score: 5, decision: 'skip' },
    { ts: todayIsoMinute(10, 17), agent_type: 'plan', score: 9, decision: 'ingest' },
    { ts: todayIsoMinute(10, 33), agent_type: 'librarian', score: 4, decision: 'skip' },
  ];
  for (const e of subagentEvents) {
    lines.push(
      JSON.stringify({
        ts: e.ts,
        session_id: SESSION_SUBAGENT,
        parent_session_id: SESSION_PRIMARY,
        source: 'subagent-stop',
        agent_type: e.agent_type,
        cwd: '/tmp/qa',
        scored: { score: e.score, signals: ['nav'], metrics: {} },
        threshold: 6,
        decision: e.decision,
      }),
    );
  }

  const aggregates = [
    {
      ts: todayIsoMinute(11, 0),
      session_id: SESSION_AGG_A,
      peak_turn: 14,
      session_aggregate: 28,
      final_score: 12,
      turn_count: 7,
      subagent_count: 2,
      decision: 'ingest',
    },
    {
      ts: todayIsoMinute(11, 30),
      session_id: SESSION_AGG_B,
      peak_turn: 6,
      session_aggregate: 9,
      final_score: 4,
      turn_count: 3,
      subagent_count: 0,
      decision: 'skip',
    },
  ];
  for (const e of aggregates) {
    lines.push(
      JSON.stringify({
        ts: e.ts,
        session_id: e.session_id,
        action: 'session-aggregate',
        peak_turn: e.peak_turn,
        session_aggregate: e.session_aggregate,
        final_score: e.final_score,
        turn_count: e.turn_count,
        subagent_count: e.subagent_count,
        decision: e.decision,
      }),
    );
  }

  return `${lines.join('\n')}\n`;
}

function buildSettings(): string {
  return `${JSON.stringify(
    {
      hooks: {
        SessionEnd: [
          {
            matcher: '*',
            hooks: [
              {
                type: 'command',
                command: NONEXISTENT_HOOKS,
              },
            ],
          },
        ],
      },
    },
    null,
    2,
  )}\n`;
}

export default async function globalSetup(): Promise<void> {
  rmSync(FIXTURES_DIR, { recursive: true, force: true });
  mkdirSync(FIXTURES_DIR, { recursive: true });

  writeFileSync(SEED_JSONL, buildSeedJsonl(), 'utf8');
  writeFileSync(SETTINGS_JSON, buildSettings(), 'utf8');
}
