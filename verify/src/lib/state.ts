import { db, scalar } from './db';

export const MIN = 60_000;
export const fmt = (n: number) => n.toLocaleString('en-US');

export const backfillPosition = async () =>
  Number(await scalar<string>(`SELECT position FROM pipeline_checkpoints WHERE stream = 'backfill'`));

export const backfillCompleted = async () =>
  (await scalar<string | null>(`SELECT completed_at FROM pipeline_checkpoints WHERE stream = 'backfill'`)) !== null;

/**
 * Outbox rows not yet covered by the incremental checkpoint. The checkpoint is a (txid, seq)
 * pair and rows are consumed in that order (SPEC §5.4), so "behind" means (txid, seq) > checkpoint.
 */
export const incrementalLagEvents = async () =>
  Number(await scalar<string>(`
    SELECT count(*) FROM customer_changes o, pipeline_checkpoints c
     WHERE c.stream = 'incremental' AND (o.txid, o.seq) > (c.position_txid, c.position)`));

/**
 * Incremental starts at the current end of the outbox: everything committed before this point is
 * in the source rows, which the (reset) backfill copies anyway. The pipeline must be stopped.
 */
export async function resetIncrementalToOutboxEnd(): Promise<void> {
  await db.query(`
    UPDATE pipeline_checkpoints c
       SET position = coalesce(last.seq, 0), position_txid = coalesce(last.txid, 0), completed_at = NULL, updated_at = now()
      FROM (SELECT max(txid) AS txid, max(seq) AS seq FROM customer_changes
             WHERE txid = (SELECT max(txid) FROM customer_changes)) last
     WHERE c.stream = 'incremental'`);
}

/** Backfill from zero on the next pipeline start (the pipeline must be stopped). */
export async function resetBackfill(): Promise<void> {
  await db.query(`UPDATE pipeline_checkpoints SET position = 0, completed_at = NULL, updated_at = now() WHERE stream = 'backfill'`);
  await db.query(`UPDATE pipeline_control SET backfill_state = 'running', incremental_state = 'running', updated_at = now()`);
}
