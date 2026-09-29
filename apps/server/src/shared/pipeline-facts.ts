import { DataSource } from 'typeorm';

/** Durable facts about replication progress, read from Postgres alone (SPEC §8.2). */
export interface PipelineFacts {
  backfillPosition: number;
  backfillCompletedAt: Date | null;
  /** max(customers.id): where the backfill ends. */
  backfillTotal: number;
  incrementalTxid: number;
  incrementalSeq: number;
  /** Outbox rows beyond the incremental checkpoint, and the age of the oldest of them. */
  lagEvents: number;
  lagSeconds: number;
  dlqPending: number;
  dlqReplayed: number;
  dlqTotal: number;
}

/**
 * One definition of "where are we", shared by the pipeline heartbeat (→ /metrics gauges) and the
 * api (→ /api/status), so the two can never disagree. Everything here is durable state, which is
 * why the api can still answer while the pipeline is dead.
 */
export async function readPipelineFacts(db: DataSource): Promise<PipelineFacts> {
  const [r] = await db.query(`
    SELECT b.position                                    AS backfill_position,
           b.completed_at                                AS backfill_completed_at,
           (SELECT coalesce(max(id), 0) FROM customers)  AS backfill_total,
           i.position_txid                               AS incremental_txid,
           i.position                                    AS incremental_seq,
           lag.events                                    AS lag_events,
           lag.seconds                                   AS lag_seconds,
           dlq.pending, dlq.replayed, dlq.total
      FROM pipeline_checkpoints b
      JOIN pipeline_checkpoints i ON i.stream = 'incremental'
      CROSS JOIN LATERAL (
        SELECT count(*) AS events,
               coalesce(extract(epoch FROM now() - min(o.created_at)), 0) AS seconds
          FROM customer_changes o
         WHERE (o.txid, o.seq) > (i.position_txid, i.position)) lag
      CROSS JOIN LATERAL (
        SELECT count(*) FILTER (WHERE status = 'pending')  AS pending,
               count(*) FILTER (WHERE status = 'replayed') AS replayed,
               count(*)                                    AS total
          FROM dlq_records) dlq
     WHERE b.stream = 'backfill'`);
  return {
    backfillPosition: Number(r.backfill_position),
    backfillCompletedAt: r.backfill_completed_at,
    backfillTotal: Number(r.backfill_total),
    incrementalTxid: Number(r.incremental_txid),
    incrementalSeq: Number(r.incremental_seq),
    lagEvents: Number(r.lag_events),
    lagSeconds: Math.round(Number(r.lag_seconds)),
    dlqPending: Number(r.pending),
    dlqReplayed: Number(r.replayed),
    dlqTotal: Number(r.total),
  };
}
