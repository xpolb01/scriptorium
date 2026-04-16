import { expect, test } from '@playwright/test';

test('Health tab renders CheckItems from /api/health', async ({ page }) => {
  await page.goto('/');
  await page.click('[data-target="health"]');
  
  await page.waitForSelector('#healthGrid .health-item');
  
  const items = page.locator('#healthGrid .health-item');
  const count = await items.count();
  expect(count).toBeGreaterThan(0);
  
  const textContent = await page.locator('#healthGrid').textContent();
  expect(textContent).not.toContain('No health checks reported');

  const statuses = page.locator('.health-status');
  const statusCount = await statuses.count();
  
  for (let i = 0; i < statusCount; i++) {
    const classList = await statuses.nth(i).getAttribute('class') || '';
    const hasValidStatusClass = classList.includes('status-pass') || 
                                classList.includes('status-warn') || 
                                classList.includes('status-fail') || 
                                classList.includes('status-info');
    expect(hasValidStatusClass).toBe(true);
  }
});

test('Missing hooks_dir surfaces a fail item', async ({ request }) => {
  const response = await request.get('/api/health');
  expect(response.status()).toBe(200);
  const body = await response.json();
  const hasFail = body.some((item: { status: string }) => item.status === 'fail');
  expect(hasFail).toBe(true);
});

test('Status classes are lowercase', async ({ page }) => {
  await page.goto('/');
  await page.click('[data-target="health"]');
  
  await page.waitForSelector('#healthGrid .health-item');
  
  const statuses = page.locator('.health-status');
  const count = await statuses.count();
  
  for (let i = 0; i < count; i++) {
    const className = await statuses.nth(i).getAttribute('class') || '';
    expect(className).not.toContain('status-Pass');
    expect(className).not.toContain('status-Warn');
    expect(className).not.toContain('status-Fail');
    expect(className).not.toContain('status-Info');
  }
});
