import { Check, fail, pass } from '../types';
import { compareAll, describe, waitConverged } from '../lib/compare';
import { scalar } from '../lib/db';
import { cpuPercent, jsonLogsSince, start, stop } from '../lib/docker';
import { esHealthy } from '../lib/es';
import { backfillCompleted, fmt, MIN } from '../lib/state';
import { startTraffic } from '../lib/traffic';
import { sleep, waitFor } from '../lib/wait';

const OUTAGE_MS = 60_000;
const CPU_SAMPLE_MS = 5_000;
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
const secs = (ms: number) => `${(ms / 1000).toFixed(1)}s`;

// SPEC §11 G3: the index goes away for 60 s while changes keep coming. Nothing may be lost, the
// pipeline must wait rather than spin, and it must recover on its own once the index is back.
export const g3: Check = {
  id: 'G3',
  title: 'sink outage',
  async run() {
    const steps: string[] = [];

    // Start from a finished backfill, so the outage hits the incremental path under live traffic.
    await waitFor('backfill to be completed', async () => ((await backfillCompleted()) ? true : undefined), 15 * MIN, 1_000);
    await waitConverged();
    const maxId = Number(await scalar<string>('SELECT max(id) FROM customers'));
    const traffic = startTraffic(maxId);

    let downAt = '', upAt = '';
    const cpu: number[] = [];
    let healthyAt = 0, convergedAt = 0;
    try {
      await sleep(5_000);

      // 1. Outage.
      downAt = new Date().toISOString();
      await stop('elasticsearch');
      const until = Date.now() + OUTAGE_MS;
      while (Date.now() < until) {
        await sleep(CPU_SAMPLE_MS);
        cpu.push(await cpuPercent('pipeline'));
      }

      // 2. Back up. Recovery is measured from the moment the index is healthy again.
      await start('elasticsearch');
      await waitFor('elasticsearch to be healthy', async () => ((await esHealthy()) ? true : undefined), 3 * MIN, 1_000);
      upAt = new Date().toISOString();
      healthyAt = Date.now();
      await sleep(3_000); // a little traffic after the outage, too
      const t = await traffic.stop();
      steps.push(`traffic: ${fmt(t.transactions)} transactions — ${fmt(t.updates)} row updates, ${fmt(t.inserts)} inserts, ${fmt(t.deletes)} deletes`);
      convergedAt = await waitConverged(5 * MIN);
    } finally {
      await traffic.stop();
      if (!(await esHealthy())) await start('elasticsearch').catch(() => undefined);
    }

    // 3. Behaviour during the outage: waiting, not spinning.
    const avgCpu = cpu.reduce((a, b) => a + b, 0) / Math.max(cpu.length, 1);
    const during = await jsonLogsSince('pipeline', downAt, upAt);
    const retries = during.filter((l) => errorEvents.has(String(l.event))).length;
    const opened = during.filter((l) => l.event === 'circuit.opened').length;
    steps.push(`outage: ${OUTAGE_MS / 1000}s, pipeline CPU avg ${avgCpu.toFixed(1)}% (samples ${cpu.map((c) => c.toFixed(1)).join(', ')})`);
    steps.push(`requests that failed during the outage: ${retries} (bound ${MAX_RETRIES}); breaker opened ${opened}×`);

    // 4. Recovery: first successful incremental write after the index came back, then full catch-up.
    const after = await jsonLogsSince('pipeline', upAt);
    const firstWrite = after.find((l) => l.event === 'incremental.batch');
    const resumedMs = firstWrite ? Date.parse(String(firstWrite.ts)) - healthyAt : Number.POSITIVE_INFINITY;
    const caughtUpMs = convergedAt - healthyAt;
    steps.push(`index healthy → writing resumed in ${secs(resumedMs)} → backlog drained, both sinks caught up in ${secs(caughtUpMs)}`);

    // 5. Nothing lost: every (id, version) in both sinks.
    const c = await compareAll();
    steps.push(...describe(c, fmt));

    if (avgCpu > MAX_AVG_CPU) return fail(`busy loop: pipeline CPU ${avgCpu.toFixed(1)}% during outage`, steps);
    if (retries > MAX_RETRIES) return fail(`${retries} failed requests in ${OUTAGE_MS / 1000}s — not backing off`, steps);
    if (c.problems > 0) return fail(`${c.problems} mismatches after recovery`, steps);
    if (resumedMs > MAX_RESUME_MS) return fail(`writing resumed only ${secs(resumedMs)} after the index was back (bound ${MAX_RESUME_MS / 1000}s)`, steps);
    return pass(`${OUTAGE_MS / 1000}s down, 0 lost, resumed in ${secs(resumedMs)}, caught up in ${secs(caughtUpMs)}`, steps);
  },
};
