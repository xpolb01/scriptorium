import { test, expect } from '@playwright/test';

test.describe('Error Banner Display', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/');
  });

  test('Overview tab shows error banner on /api/summary 500', async ({ page }) => {
    await page.route('**/api/summary*', route =>
      route.fulfill({ status: 500, body: '{}' })
    );

    await page.reload();

    const errorBanner = page.locator('#error-overview');
    await expect(errorBanner).toHaveClass(/visible/);
    await expect(errorBanner).not.toHaveText(/^\s*$/);
  });

  test('Events tab banner on /api/events 500', async ({ page }) => {
    await page.route('**/api/events*', route =>
      route.fulfill({ status: 500, body: '{}' })
    );

    await page.click('[data-target="events"]');

    const errorBanner = page.locator('#error-events');
    await expect(errorBanner).toHaveClass(/visible/);
    await expect(errorBanner).not.toHaveText(/^\s*$/);
  });

  test('Health tab banner on /api/health 500', async ({ page }) => {
    await page.route('**/api/health*', route =>
      route.fulfill({ status: 500, body: '{}' })
    );

    await page.click('[data-target="health"]');

    const errorBanner = page.locator('#error-health');
    await expect(errorBanner).toHaveClass(/visible/);
    await expect(errorBanner).not.toHaveText(/^\s*$/);
  });

  test('Banner clears on subsequent success', async ({ page }) => {
    await page.route('**/api/summary*', route =>
      route.fulfill({ status: 500, body: '{}' })
    );

    await page.reload();

    const errorBanner = page.locator('#error-overview');
    await expect(errorBanner).toHaveClass(/visible/);

    await page.unroute('**/api/summary*');

    await page.reload();

    await expect(errorBanner).not.toHaveClass(/visible/);
  });
});
