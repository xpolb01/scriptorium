import { test, expect } from '@playwright/test';

test('Overlay visible during slow fetch, hidden after', async ({ page }) => {
  await page.goto('/');
  await page.route('**/api/events*', async route => {
    await new Promise(r => setTimeout(r, 1500));
    await route.continue();
  });
  // click events tab
  await page.click('[data-target="events"]');
  // overlay must appear briefly
  await expect(page.locator('#loadingOverlay')).toBeVisible({ timeout: 2000 });
  // and hide afterwards
  await expect(page.locator('#loadingOverlay')).toBeHidden({ timeout: 5000 });
});

test('Overlay NOT visible on auto-refresh', async ({ page }) => {
  await page.goto('/');
  await page.waitForSelector('#stat-total-events', { timeout: 5000 });
  // wait until the dashboard's own auto-refresh tick would fire (default 10s)
  await page.waitForTimeout(11_000);
  // at this stable moment, overlay should not be visible
  await expect(page.locator('#loadingOverlay')).toBeHidden();
});
