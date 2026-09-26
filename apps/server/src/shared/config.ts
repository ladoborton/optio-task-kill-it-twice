// All runtime configuration comes from env vars, with defaults documented here (AGENTS.md §7).

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env var ${name}`);
  return value;
}

function int(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`Env var ${name} must be a positive integer, got "${raw}"`);
  return value;
}

export const config = {
  role: process.env.ROLE ?? 'unknown',
  databaseUrl: required('DATABASE_URL'),
  esUrl: process.env.ES_URL ?? 'http://elasticsearch:9200',
  esIndex: process.env.ES_INDEX ?? 'customers',
  // SPEC §4.8: 1M rows by default.
  seedCount: int('SEED_COUNT', 1_000_000),
  // Rows per INSERT statement during seed; keeps each statement (and its WAL) bounded.
  seedChunk: int('SEED_CHUNK', 100_000),
};
