import { db, scalar } from './db';

export const MIN = 60_000;
export const fmt = (n: number) => n.toLocaleString('en-US');

export const backfillPosition = async () =>
  Number(await scalar<string>(`SELECT position FROM pipeline_checkpoints WHERE stream = 'backfill'`));

export const backfillCompleted = async () =>
  (await scalar<string | null>(`SELECT completed_at FROM pipeline_checkpoints WHERE stream = 'backfill'`)) !== null;

/** Backfill from zero on the next pipeline start (the pipeline must be stopped). */
export async function resetBackfill(): Promise<void> {
  await db.query(`UPDATE pipeline_checkpoints SET position = 0, completed_at = NULL, updated_at = now() WHERE stream = 'backfill'`);
  await db.query(`UPDATE pipeline_control SET backfill_state = 'running', updated_at = now()`);
}
