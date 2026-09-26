import { Pool } from 'pg';
import { env } from './env';

export const db = new Pool({ connectionString: env.databaseUrl, max: 4 });

export async function scalar<T>(sql: string, params: unknown[] = []): Promise<T> {
  const { rows } = await db.query(sql, params);
  return Object.values(rows[0] ?? {})[0] as T;
}
