import { rmSync } from 'node:fs';
import { resolve } from 'node:path';

const FIXTURES_DIR = resolve(__dirname, '.fixtures');

export default async function globalTeardown(): Promise<void> {
  rmSync(FIXTURES_DIR, { recursive: true, force: true });
}
