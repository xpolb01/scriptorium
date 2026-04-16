import { expect, test } from '@playwright/test';

test('invalid since returns 400', async ({ request }) => {
  const r = await request.get('/api/events?since=not-a-date');
  expect(r.status()).toBe(400);
  const body = await r.json();
  expect(body.error).toBe('invalid_since');
});

test('valid RFC3339 since returns 200', async ({ request }) => {
  const r = await request.get('/api/events?since=2026-04-16T08:00:00Z');
  expect(r.status()).toBe(200);
  const body = await r.json();
  expect(Array.isArray(body)).toBe(true);
});

test('missing since returns 200', async ({ request }) => {
  const r = await request.get('/api/events');
  expect(r.status()).toBe(200);
  const body = await r.json();
  expect(Array.isArray(body)).toBe(true);
});

test('since in far future returns 200 with empty array', async ({ request }) => {
  const r = await request.get('/api/events?since=2099-01-01T00:00:00Z');
  expect(r.status()).toBe(200);
  const body = await r.json();
  expect(Array.isArray(body)).toBe(true);
  expect(body).toEqual([]);
});
