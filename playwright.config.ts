import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright config for the Scriptorium telemetry dashboard QA harness.
 *
 * The webServer command intentionally points at fixture paths under
 * `tests/dashboard-qa/.fixtures/`. The hooks-dir is intentionally absent
 * (`nonexistent-hooks/`) so that `check_session_hooks` produces a
 * deterministic Fail item for downstream specs (T14+).
 */
export default defineConfig({
  testDir: 'tests/dashboard-qa',
  timeout: 30_000,
  expect: { timeout: 5_000 },
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: process.env.CI ? 'line' : 'list',
  globalSetup: './tests/dashboard-qa/global-setup.ts',
  globalTeardown: './tests/dashboard-qa/global-teardown.ts',
  use: {
    baseURL: 'http://127.0.0.1:38271',
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command:
      'cargo run --features dashboard -p scriptorium-cli -- hooks dashboard --port 38271 --no-browser --db ./tests/dashboard-qa/.fixtures/test.sqlite --jsonl ./tests/dashboard-qa/.fixtures/seed.jsonl --settings ./tests/dashboard-qa/.fixtures/settings.json --hooks-dir ./tests/dashboard-qa/.fixtures/nonexistent-hooks/',
    url: 'http://127.0.0.1:38271/',
    timeout: 120_000,
    reuseExistingServer: !process.env.CI,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
