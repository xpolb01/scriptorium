import { expect, test } from '@playwright/test';

test('navigates all 3 tabs with content visible', async ({ page }) => {
  await page.goto('/');
  await expect(page).toHaveTitle(/Scriptorium Telemetry Dashboard/);

  // Overview
  await expect(page.locator('#overview')).toBeVisible();
  await expect(page.locator('#stat-ingests')).toBeVisible();
  await expect(page.locator('#walCheckpointText')).toBeVisible();

  // Events tab
  await page.click('[data-target="events"]');
  await expect(page.locator('#events')).toBeVisible();

  // Health tab
  await page.click('[data-target="health"]');
  await expect(page.locator('#health')).toBeVisible();
});

test('window switch preserves Ingests Today', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('#stat-ingests');
  const before = (await page.locator('#stat-ingests').textContent())?.trim();

  await page.selectOption('#timeWindow', '3600000');
  await page.waitForTimeout(500);
  const after = (await page.locator('#stat-ingests').textContent())?.trim();

  expect(after).toBe(before);
});

test('WAL checkpoint label matches expected format', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('#walCheckpointText');
  const txt = await page.locator('#walCheckpointText').textContent() ?? '';
  expect(txt).not.toContain('Syncing');
  expect(txt).toMatch(/(WAL checkpoint: unknown)|(WAL last checkpoint: \d+[smhd] ago)/);
});
