import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { config } from '../../../shared/config';
import { readPipelineFacts } from '../../../shared/pipeline-facts';
import { queueStats } from '../../../shared/rabbitmq/management';
import { DEAD_LETTER_QUEUE, QUEUE } from '../../../shared/rabbitmq/topology';

type Circuit = 'closed' | 'open' | 'half_open' | 'unknown';

export interface Status {
  generated_at: string;
  health: { status: 'ok' | 'degraded' | 'down'; reasons: string[] };
  pipeline: { alive: boolean; instance_id: string | null; started_at: string | null; last_beat_at: string | null; last_beat_age_seconds: number | null };
  backfill: { state: 'running' | 'paused' | 'completed'; position: number; total: number; percent: number; rate_per_sec: number };
  incremental: { state: 'running' | 'paused'; position: { txid: number; seq: number }; lag_events: number; lag_seconds: number; rate_per_sec: number };
  sinks: { elasticsearch: { circuit: Circuit }; rabbitmq: { circuit: Circuit } };
  dlq: { pending: number; replayed: number; total: number };
  consumer: { applied: number; duplicates: number; queue_ready: number; queue_unacked: number; consumers: number; dead_lettered: number };
  settings: { batch_size: number; poll_interval_ms: number };
}

/**
 * SPEC §8.2 / §8.3: the answer to "where are we and are we healthy", built only from durable
 * sources — Postgres (checkpoints, heartbeat, DLQ, consumer counters) and the RabbitMQ management
 * API. Nothing here asks the pipeline process, so the answer stays correct while it is dead.
 */
@Injectable()
export class StatusService {
  constructor(private readonly db: DataSource) {}

  async status(): Promise<Status> {
    const [facts, [control], [beat], [consumer]] = await Promise.all([
      readPipelineFacts(this.db),
      this.db.query(`SELECT backfill_state, incremental_state, batch_size, poll_interval_ms FROM pipeline_control WHERE id = 1`),
      this.db.query(`
        SELECT instance_id, started_at, last_beat_at, backfill_rate, incremental_rate, details,
               extract(epoch FROM now() - last_beat_at) AS age
          FROM pipeline_heartbeat ORDER BY last_beat_at DESC LIMIT 1`),
      this.db.query(`SELECT applied, duplicates FROM consumer.stats`),
    ]);
    const reasons: string[] = [];

    // Liveness: the pipeline writes a heartbeat every HEARTBEAT_MS; a stale one means dead or hung.
    const age = beat ? Number(beat.age) : null;
    const alive = age !== null && age * 1000 <= config.healthDownAfterMs;
    if (!alive) reasons.push(beat ? `pipeline heartbeat ${Math.round(age!)}s old` : 'no pipeline heartbeat yet');

    // Breaker states come from the last heartbeat; unknown while the pipeline is not alive.
    const circuits = (beat?.details?.circuits ?? {}) as Record<string, Circuit>;
    const circuit = (sink: string): Circuit => (alive ? circuits[sink] ?? 'unknown' : 'unknown');
    for (const sink of ['elasticsearch', 'rabbitmq']) {
      if (circuit(sink) === 'open' || circuit(sink) === 'half_open') reasons.push(`${sink} circuit ${circuit(sink).replace('_', '-')}`);
    }

    if (facts.lagSeconds > config.healthMaxLagSeconds) reasons.push(`incremental lag ${facts.lagSeconds}s (${facts.lagEvents} events)`);
    if (facts.dlqPending > 0) reasons.push(`${facts.dlqPending} DLQ record(s) pending replay`);

    let queue = { ready: 0, unacked: 0, consumers: 0 };
    let deadLettered = 0;
    try {
      queue = (await queueStats(QUEUE)) ?? queue;
      deadLettered = (await queueStats(DEAD_LETTER_QUEUE))?.ready ?? 0;
    } catch (e) {
      reasons.push(`rabbitmq management unreachable: ${(e as Error).message}`);
    }

    const health: Status['health']['status'] = !alive ? 'down' : reasons.length ? 'degraded' : 'ok';
    const round = (n: number) => Math.round(n * 10) / 10;
    return {
      generated_at: new Date().toISOString(),
      health: { status: health, reasons },
      pipeline: {
        alive,
        instance_id: beat?.instance_id ?? null,
        started_at: beat?.started_at ?? null,
        last_beat_at: beat?.last_beat_at ?? null,
        last_beat_age_seconds: age === null ? null : round(age),
      },
      backfill: {
        state: facts.backfillCompletedAt ? 'completed' : control.backfill_state,
        position: facts.backfillPosition,
        total: facts.backfillTotal,
        // Once completed it stays 100 %: rows inserted afterwards raise max(id) but are the
        // incremental loop's job, not the backfill's.
        percent: facts.backfillCompletedAt || !facts.backfillTotal ? 100 : round((facts.backfillPosition / facts.backfillTotal) * 100),
        rate_per_sec: alive ? round(beat.backfill_rate) : 0,
      },
      incremental: {
        state: control.incremental_state,
        position: { txid: facts.incrementalTxid, seq: facts.incrementalSeq },
        lag_events: facts.lagEvents,
        lag_seconds: facts.lagSeconds,
        rate_per_sec: alive ? round(beat.incremental_rate) : 0,
      },
      sinks: { elasticsearch: { circuit: circuit('elasticsearch') }, rabbitmq: { circuit: circuit('rabbitmq') } },
      dlq: { pending: facts.dlqPending, replayed: facts.dlqReplayed, total: facts.dlqTotal },
      consumer: {
        applied: Number(consumer.applied),
        duplicates: Number(consumer.duplicates),
        queue_ready: queue.ready,
        queue_unacked: queue.unacked,
        consumers: queue.consumers,
        dead_lettered: deadLettered,
      },
      settings: { batch_size: control.batch_size, poll_interval_ms: control.poll_interval_ms },
    };
  }
}
