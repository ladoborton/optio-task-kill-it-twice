import { Check, fail, pass } from '../types';
import { compareAll, describe, QUEUE, waitConverged } from '../lib/compare';
import { db, scalar } from '../lib/db';
import { cpuPercent, jsonLogsSince, start, stop } from '../lib/docker';
import { deleteIndex, esHealthy } from '../lib/es';
import { purgeQueue } from '../lib/rabbit';
import { backfillCompleted, backfillPosition, fmt, MIN, resetBackfill } from '../lib/state';
import { startTraffic } from '../lib/traffic';
import { sleep, waitFor } from '../lib/wait';

const OUTAGE_MS = 60_000;
const CPU_SAMPLE_MS = 5_000;
// The outage starts in the middle of the work: at this share of the backfill, with change traffic
// flowing, so both the backfill and the incremental loop hit the dead index.
const OUTAGE_AT = 0.3;
// A busy loop pins a core (~100 %). A process that waits between retries is near idle.
const MAX_AVG_CPU = 10;
// Requests to a down sink are bounded (SPEC §7.2): a few backoff-spaced failures open the breaker,
// then one probe per open interval (5 s) — ~15 per minute. A busy loop would log thousands.
const MAX_RETRIES = 40;
// "Recovers on its own" needs a bound to be testable. Writing must resume within about one breaker
// probe interval (5 s) plus a slow first request after the restart. With per-loop backoff alone
// (30 s cap) the worst case is ~30 s; two measured runs happened to resume in 3.8 s and 8.5 s.
const MAX_RESUME_MS = 15_000;

const errorEvents = new Set(['backfill.error', 'incremental.error', 'dlq_replay.error']);
const writeEvents = new Set(['backfill.batch', 'incremental.batch']);
const secs = (ms: number) => `${(ms / 1000).toFixed(1)}s`;

// SPEC §11 G3: the index goes away for 60 s in the middle of a backfill while changes keep coming.
// Nothing may be lost, the checkpoint must not move while the sink is down, the pipeline must wait
// rather than spin, and it must recover on its own once the index is back.
export const g3: Check = {
  id: 'G3',
  title: 'sink outage',
  async run() {
    const steps: string[] = [];

    // 1. Known state: empty index, empty projection and queue, backfill from zero. Without an empty
    //    index, "nothing lost" would hold even if the backfill skipped the outage window.
    await stop('pipeline');
    await stop('consumer');
    await resetBackfill();
    await deleteIndex();
    await db.query('TRUNCATE consumer.customers, consumer.applied_events');
    await db.query('UPDATE consumer.stats SET applied = 0, duplicates = 0');
    await purgeQueue(QUEUE);
    await start('consumer');
    await start('pipeline');
    const maxId = Number(await scalar<string>('SELECT max(id) FROM customers'));
    const traffic = startTraffic(maxId);
    steps.push('reset: empty index, empty projection, purged queue, backfill from 0, change traffic on');

    let downAt = '', upAt = '';
    const cpu: number[] = [];
    let healthyAt = 0, convergedAt = 0;
    let frozenAt = 0, afterOutage = 0;
    try {
      // 2. Outage in the middle of the backfill.
      const target = Math.floor(maxId * OUTAGE_AT);
      await waitFor(`backfill to reach ${OUTAGE_AT * 100}%`, async () => ((await backfillPosition()) >= target ? true : undefined), 10 * MIN);
      downAt = new Date().toISOString();
      await stop('elasticsearch');
      // A batch that was already past the index when it stopped may still checkpoint; after that the
      // position must not move until the index is back.
      await sleep(CPU_SAMPLE_MS);
      frozenAt = await backfillPosition();
      const until = Date.now() + OUTAGE_MS - CPU_SAMPLE_MS;
      while (Date.now() < until) {
        await sleep(CPU_SAMPLE_MS);
        cpu.push(await cpuPercent('pipeline'));
      }
      afterOutage = await backfillPosition();
      steps.push(`index stopped at backfill id ${fmt(frozenAt)} (${Math.round((frozenAt / maxId) * 100)}%); checkpoint after ${OUTAGE_MS / 1000}s down: ${fmt(afterOutage)}`);

      // 3. Back up. Recovery is measured from the moment the index is healthy again.
      await start('elasticsearch');
      await waitFor('elasticsearch to be healthy', async () => ((await esHealthy()) ? true : undefined), 3 * MIN, 1_000);
      upAt = new Date().toISOString();
      healthyAt = Date.now();
      await waitFor('backfill to complete', async () => ((await backfillCompleted()) ? true : undefined), 15 * MIN, 1_000);
      await sleep(3_000); // a little incremental-only traffic after the backfill, too
      const t = await traffic.stop();
      steps.push(`traffic: ${fmt(t.transactions)} transactions — ${fmt(t.updates)} row updates, ${fmt(t.inserts)} inserts, ${fmt(t.deletes)} deletes`);
      convergedAt = await waitConverged(10 * MIN);
    } finally {
      await traffic.stop();
      if (!(await esHealthy())) await start('elasticsearch').catch(() => undefined);
    }

    // 4. Behaviour during the outage: waiting, not spinning, not moving the checkpoint.
    const avgCpu = cpu.reduce((a, b) => a + b, 0) / Math.max(cpu.length, 1);
    const during = await jsonLogsSince('pipeline', downAt, upAt);
    const retries = during.filter((l) => errorEvents.has(String(l.event))).length;
    const opened = during.filter((l) => l.event === 'circuit.opened').length;
    steps.push(`pipeline CPU during the outage: avg ${avgCpu.toFixed(1)}% (samples ${cpu.map((c) => c.toFixed(1)).join(', ')})`);
    steps.push(`requests that failed during the outage: ${retries} (bound ${MAX_RETRIES}); breaker opened ${opened}×`);

    // 5. Recovery: first successful write of either loop after the index came back, then full catch-up.
    const after = await jsonLogsSince('pipeline', upAt);
    const firstWrite = after.find((l) => writeEvents.has(String(l.event)));
    const resumedMs = firstWrite ? Date.parse(String(firstWrite.ts)) - healthyAt : Number.POSITIVE_INFINITY;
    const caughtUpMs = convergedAt - healthyAt;
    steps.push(`index healthy → writing resumed in ${secs(resumedMs)} → backfill finished and both sinks caught up in ${secs(caughtUpMs)}`);

    // 6. Nothing lost: every (id, version) in both sinks.
    const c = await compareAll();
    steps.push(...describe(c, fmt));

    if (afterOutage !== frozenAt) return fail(`backfill checkpoint moved while the index was down (${fmt(frozenAt)} → ${fmt(afterOutage)})`, steps);
    if (avgCpu > MAX_AVG_CPU) return fail(`busy loop: pipeline CPU ${avgCpu.toFixed(1)}% during outage`, steps);
    if (retries > MAX_RETRIES) return fail(`${retries} failed requests in ${OUTAGE_MS / 1000}s — not backing off`, steps);
    if (c.problems > 0) return fail(`${c.problems} mismatches after recovery`, steps);
    if (resumedMs > MAX_RESUME_MS) return fail(`writing resumed only ${secs(resumedMs)} after the index was back (bound ${MAX_RESUME_MS / 1000}s)`, steps);
    return pass(`${OUTAGE_MS / 1000}s down at ${Math.round((frozenAt / maxId) * 100)}% of the backfill, 0 lost, resumed in ${secs(resumedMs)}, caught up in ${secs(caughtUpMs)}`, steps);
  },
};
