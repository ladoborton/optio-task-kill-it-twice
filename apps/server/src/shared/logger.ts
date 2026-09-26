import { config } from './config';

type Level = 'info' | 'warn' | 'error';

// Structured JSON lines (SPEC §8.4): one object per line, greppable with `docker compose logs`.
export function log(event: string, fields: Record<string, unknown> = {}, level: Level = 'info'): void {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, role: config.role, event, ...fields });
  (level === 'error' ? process.stderr : process.stdout).write(line + '\n');
}
