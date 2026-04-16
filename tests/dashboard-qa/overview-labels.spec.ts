import { expect, test } from '@playwright/test';

test('WAL checkpoint age renders with concrete number', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('#lagText');
  
  const walText = await page.locator('#lagText').textContent();
  expect(walText).toMatch(/WAL last checkpoint: \d+[smhd] ago/);
  expect(walText).not.toContain('Syncing');
});

test('Ingests Today value stable across window switch', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('#stat-ingests');
  
  const initialValue = await page.locator('#stat-ingests').textContent();
  
  await page.selectOption('#timeWindow', '3600000');
  await page.waitForTimeout(500);
  
  const valueAfterSwitch = await page.locator('#stat-ingests').textContent();
  expect(valueAfterSwitch).toBe(initialValue);
});

test('No Syncing placeholder anywhere in Overview', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('#overview');
  
  await expect(page.locator('#overview')).not.toContainText('Syncing');
});
