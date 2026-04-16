import { test, expect } from '@playwright/test';

test.describe('XSS Safety', () => {
  test('Script in session_id does not execute', async ({ page }) => {
    await page.route('**/api/events*', route =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify([{
          ts: new Date().toISOString(),
          session_id: '<script>window.xssMarker1=1</script>',
          hook_type: 'stop',
          score: 50,
          signals: [],
          decision: 'ingest'
        }])
      })
    );

    await page.goto('/');
    await page.click('[data-target="events"]');
    await page.waitForSelector('#eventsTableBody tr');

    const marker = await page.evaluate(() => (window as any).xssMarker1);
    expect(marker).toBeUndefined();
  });

  test('Script in signals does not execute', async ({ page }) => {
    await page.route('**/api/events*', route =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify([{
          ts: new Date().toISOString(),
          session_id: '12345678-1234-4567-8901-234567890123',
          hook_type: 'stop',
          score: 50,
          signals: ['<img src=x onerror="window.xssMarker2=1">'],
          decision: 'ingest'
        }])
      })
    );

    await page.goto('/');
    await page.click('[data-target="events"]');
    await page.waitForSelector('#eventsTableBody tr');

    const marker = await page.evaluate(() => (window as any).xssMarker2);
    expect(marker).toBeUndefined();
  });

  test('HTML rendered as literal text in cells', async ({ page }) => {
    await page.route('**/api/events*', route =>
      route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify([{
          ts: new Date().toISOString(),
          session_id: '<script>alert("xss")</script>',
          hook_type: 'stop',
          score: 50,
          signals: [],
          decision: 'ingest'
        }])
      })
    );

    await page.goto('/');
    await page.click('[data-target="events"]');
    await page.waitForSelector('#eventsTableBody tr');

    const tbody = page.locator('#eventsTableBody');
    const cellText = await tbody.textContent();
    expect(cellText).toContain('<script>');
  });
});
