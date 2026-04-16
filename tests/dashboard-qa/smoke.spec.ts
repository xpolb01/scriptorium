import { expect, test } from '@playwright/test';

test('dashboard root returns 200 and renders title', async ({ page, request }) => {
  const response = await request.get('/');
  expect(response.status()).toBe(200);

  await page.goto('/');
  await expect(page).toHaveTitle(/Scriptorium Telemetry Dashboard/);
});
