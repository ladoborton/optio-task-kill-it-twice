import { Check, fail, pass } from '../types';
import { waitConverged } from '../lib/compare';
import { db, scalar } from '../lib/db';
import { jsonLogsSince, start, stop } from '../lib/docker';
import { esHealthy } from '../lib/es';
import { env } from '../lib/env';
import { MIN } from '../lib/state';
import { sleep, waitFor } from '../lib/wait';

/** The /api/status contract the UI and operators rely on (SPEC §8.2). */
interface Status {
  health: { status: 'ok' | 'degraded' | 'down'; reasons: string[] };
  pipeline: { alive: boolean; last_beat_at: string | null };
  backfill: { state: string; position: number; total: number; percent: number; rate_per_sec: number };
  incremental: { state: string; lag_events: number; lag_seconds: number; rate_per_sec: number };
  sinks: Record<'elasticsearch' | 'rabbitmq', { circuit: string }>;
  dlq: { pending: number; replayed: number; total: number };
  consumer: { applied: number; duplicates: number; queue_ready: number; queue_unacked: number; dead_lettered: number };
}

const status = async (): Promise<Status> => {
  const res = await fetch(`${env.apiUrl}/api/status`, { signal: AbortSignal.timeout(10_000) });
  if (!res.ok) throw new Error(`GET /api/status -> HTTP ${res.status}`);
  return (await res.json()) as Status;
};

const REQUIRED_METRICS = [
  'pipeline_records_total',
  'pipeline_batch_duration_seconds',
  'pipeline_checkpoint_position',
  'pipeline_throughput_records_per_second',
  'pipeline_incremental_lag_events',
  'pipeline_incremental_lag_seconds',
  'pipeline_circuit_state',
  'pipeline_dlq_pending',
];

/** Waits until /api/status health matches, returns the status and how long it took. */
async function healthBecomes(want: Status['health']['status'], reason: RegExp | null, timeoutMs: number): Promise<[Status, number]> {
  const t0 = Date.now();
  const s = await waitFor(`health "${want}"${reason ? ` with reason ${reason}` : ''}`, async () => {
    const s = await status().catch(() => undefined);
    if (!s || s.health.status !== want) return undefined;
    if (reason && !s.health.reasons.some((r) => reason.test(r))) return undefined;
    return s;
  }, timeoutMs, 1_000);
  return [s, Date.now() - t0];
}

const secs = (ms: number) => `${(ms / 1000).toFixed(0)}s`;

// SPEC §11 G5: from metrics, logs and the api alone — without reading code — an operator can tell
// where the backfill is, the throughput, the incremental lag, the DLQ size and whether the system
// is healthy; and the health verdict reacts to real failures.
export const g5: Check = {
  id: 'G5',
  title: 'observability',
  async run() {
    const steps: string[] = [];
    await waitConverged();

    // 1. Metrics endpoint (Prometheus text format).
    const metricsRes = await fetch(env.pipelineMetricsUrl, { signal: AbortSignal.timeout(10_000) });
    const metrics = metricsRes.ok ? await metricsRes.text() : '';
    const missing = REQUIRED_METRICS.filter((m) => !new RegExp(`^${m}(\\{|\\s|_bucket|_sum|_count)`, 'm').test(metrics));
    steps.push(`/metrics: ${REQUIRED_METRICS.length - missing.length}/${REQUIRED_METRICS.length} required metrics${missing.length ? `, missing ${missing.join(', ')}` : ''}`);
    if (missing.length) return fail(`metrics missing: ${missing.join(', ')}`, steps);

    // 2. /api/status answers the five questions, consistent with the database.
    const s = await status();
    const dbPosition = Number(await scalar<string>(`SELECT position FROM pipeline_checkpoints WHERE stream = 'backfill'`));
    const dbPending = Number(await scalar<string>(`SELECT count(*) FROM dlq_records WHERE status = 'pending'`));
    const dbDlqTotal = Number(await scalar<string>(`SELECT count(*) FROM dlq_records`));
    const dbApplied = Number(await scalar<string>(`SELECT applied FROM consumer.stats`));
    steps.push(`where is the backfill: ${s.backfill.state}, ${s.backfill.position.toLocaleString('en-US')} / ${s.backfill.total.toLocaleString('en-US')} (${s.backfill.percent}%)`);
    steps.push(`throughput: backfill ${s.backfill.rate_per_sec}/s, incremental ${s.incremental.rate_per_sec}/s`);
    steps.push(`incremental lag: ${s.incremental.lag_events} events, ${s.incremental.lag_seconds}s`);
    steps.push(`dlq: ${s.dlq.pending} pending, ${s.dlq.replayed} replayed, ${s.dlq.total} total; consumer queue ${s.consumer.queue_ready} ready, ${s.consumer.dead_lettered} dead-lettered`);
    steps.push(`health: ${s.health.status}${s.health.reasons.length ? ` (${s.health.reasons.join('; ')})` : ''}`);
    const mismatches = [
      s.backfill.position !== dbPosition && `backfill position ${s.backfill.position} ≠ db ${dbPosition}`,
      s.dlq.pending !== dbPending && `dlq pending ${s.dlq.pending} ≠ db ${dbPending}`,
      s.dlq.total !== dbDlqTotal && `dlq total ${s.dlq.total} ≠ db ${dbDlqTotal}`,
      s.consumer.applied !== dbApplied && `consumer applied ${s.consumer.applied} ≠ db ${dbApplied}`,
      s.incremental.lag_events !== 0 && `lag ${s.incremental.lag_events} after convergence`,
      s.health.status !== 'ok' && `health ${s.health.status} on a converged system`,
    ].filter(Boolean);
    if (mismatches.length) return fail(mismatches.join('; '), steps);

    // 3. Health reacts to a dead pipeline — and the api still answers while it is dead.
    await stop('pipeline');
    const [dead, downIn] = await healthBecomes('down', /heartbeat/, 30_000);
    await start('pipeline');
    const [, backIn] = await healthBecomes('ok', null, MIN);
    steps.push(`pipeline stopped → "down" in ${secs(downIn)} (${dead.health.reasons[0]}); restarted → "ok" in ${secs(backIn)}`);

    // 4. Health reacts to a sink outage, as soon as the pipeline trips over it.
    let degradedEs: [Status, number] | undefined;
    await stop('elasticsearch');
    try {
      const writer = (async () => {
        for (let i = 0; i < 30 && !degradedEs; i++) {
          await db.query(`UPDATE customers SET balance = balance + 1 WHERE id = (SELECT min(id) FROM customers)`);
          await sleep(1_000);
        }
      })();
      degradedEs = await healthBecomes('degraded', /elasticsearch/i, MIN);
      await writer;
    } finally {
      await start('elasticsearch');
    }
    await waitFor('elasticsearch to be healthy', async () => ((await esHealthy()) ? true : undefined), 3 * MIN, 1_000);
    const [, esBackIn] = await healthBecomes('ok', null, 2 * MIN);
    steps.push(`index stopped → "degraded" in ${secs(degradedEs[1])} (${degradedEs[0].health.reasons.join('; ')}); back → "ok" in ${secs(esBackIn)}`);

    // 5. Health reacts to a record parked in the DLQ.
    const { rows: [bad] } = await db.query<{ id: string }>(
      `INSERT INTO customers (email, name, city, segment, attributes) VALUES ('g5@example.com', 'G5', 'Poti', 'new', '{"age": "n/a"}') RETURNING id`);
    try {
      const [dlqState, dlqIn] = await healthBecomes('degraded', /dlq/i, MIN);
      steps.push(`bad record → "degraded" in ${secs(dlqIn)} (${dlqState.health.reasons.join('; ')})`);
    } finally {
      await db.query(`UPDATE customers SET attributes = '{"age": 1}' WHERE id = $1`, [bad.id]);
      await db.query(`UPDATE dlq_records SET replay_requested_at = now() WHERE customer_id = $1 AND status = 'pending'`, [bad.id]);
    }
    const [, dlqBackIn] = await healthBecomes('ok', null, MIN);
    steps.push(`fixed + replayed → "ok" in ${secs(dlqBackIn)}`);

    // 6. Logs: structured JSON, one line per batch with stream and batch id.
    const logs = await jsonLogsSince('pipeline', new Date(Date.now() - 5 * MIN).toISOString());
    const batchLines = logs.filter((l) => String(l.event).endsWith('.batch'));
    const wellFormed = batchLines.filter((l) => l.ts && l.role === 'pipeline' && l.stream && (l.batch_id || l.stream === 'dlq_replay'));
    steps.push(`logs: ${batchLines.length} batch lines in the last 5 min, ${wellFormed.length} with ts/role/stream/batch_id`);
    if (batchLines.length === 0 || wellFormed.length !== batchLines.length) return fail('batch log lines missing or malformed', steps);

    return pass('status, metrics and logs answer all five questions; health went down/degraded/ok as the system did', steps);
  },
};
