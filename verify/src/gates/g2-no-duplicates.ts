import { Check, fail, pass } from '../types';
import { compareAll, describe, QUEUE, waitConverged } from '../lib/compare';
import { db, scalar } from '../lib/db';
import { kill, start, stop } from '../lib/docker';
import { deleteIndex, versionsInRange } from '../lib/es';
import { purgeQueue } from '../lib/rabbit';
import { backfillCompleted, backfillPosition, fmt, MIN, resetBackfill, resetIncrementalToOutboxEnd } from '../lib/state';
import { slowTransaction, startTraffic } from '../lib/traffic';
import { sleep, waitFor } from '../lib/wait';

const SLOW_TX_MS = 8_000; // longer than the 5 s "skip the gap" timeout SPEC v1 proposed

// SPEC §11 G2: backfill and incremental sync run together under change traffic (updates, inserts,
// deletes, one slow transaction) while the pipeline and the consumer are killed repeatedly.
// Afterwards every customer is present exactly once, at its current version, in the index and in
// the consumer's projection — and every deleted customer is gone from both. The stream itself may
// carry duplicate messages (at-least-once); the consumer must absorb them.
export const g2: Check = {
  id: 'G2',
  title: 'no duplicates',
  async run() {
    const steps: string[] = [];

    // 1. Everything downstream empty; backfill from zero, incremental from the end of the outbox.
    await stop('pipeline');
    await stop('consumer');
    await resetBackfill();
    await resetIncrementalToOutboxEnd();
    await deleteIndex();
    await db.query('TRUNCATE consumer.customers, consumer.applied_events');
    await db.query('UPDATE consumer.stats SET applied = 0, duplicates = 0');
    await purgeQueue(QUEUE);
    await start('consumer');
    await start('pipeline');
    const startMaxId = Number(await scalar<string>('SELECT max(id) FROM customers'));
    steps.push('reset: empty index, empty projection, purged queue, backfill from 0');

    // 2. Change traffic for the whole run, plus one slow transaction at ~40%.
    const traffic = startTraffic(startMaxId);
    let slowTx: Promise<void> | undefined;
    let slowId = 0;

    // 3. Kill both sides mid-run. Every kill of the pipeline replays its in-flight batch into both
    //    sinks; every kill of the consumer redelivers its unacked messages.
    const kills: [number, 'pipeline' | 'consumer'][] = [[0.25, 'pipeline'], [0.5, 'consumer'], [0.75, 'pipeline']];
    try {
      for (const [share, service] of kills) {
        const target = Math.floor(startMaxId * share);
        await waitFor(`backfill to reach ${share * 100}%`, async () => ((await backfillPosition()) >= target ? true : undefined), 10 * MIN);
        if (!slowTx) {
          // A customer the backfill has ALREADY copied: only the incremental loop can deliver this
          // change. (A row ahead of the backfill would be copied by it and hide a lost change.)
          const candidate = 1 + Math.floor(Math.random() * ((await backfillPosition()) - 1_000));
          slowId = Number(await scalar<string>('SELECT id FROM customers WHERE id >= $1 ORDER BY id LIMIT 1', [candidate]));
          slowTx = slowTransaction(slowId, SLOW_TX_MS);
        }
        await kill(service);
        await sleep(1_000);
        await start(service);
        steps.push(`killed + restarted ${service} at ~${share * 100}%`);
      }
      await waitFor('backfill to complete', async () => ((await backfillCompleted()) ? true : undefined), 15 * MIN, 1_000);
      await sleep(3_000); // a little incremental-only traffic after the backfill is done
    } finally {
      const t = await traffic.stop();
      await slowTx;
      steps.push(`traffic: ${fmt(t.transactions)} transactions — ${fmt(t.updates)} row updates, ${fmt(t.inserts)} inserts, ${fmt(t.deletes)} deletes`);
      steps.push(`slow transaction: customer #${fmt(slowId)} (already backfilled) held open ${SLOW_TX_MS / 1000}s while later ones committed`);
    }

    // 4. Wait until nothing is in flight anywhere: outbox consumed, queue drained.
    await waitConverged();

    // 5. The slow transaction's change specifically (also covered by the full comparison below).
    const slowSource = await scalar<string | null>('SELECT version FROM customers WHERE id = $1', [slowId]);
    const slowIndex = (await versionsInRange(slowId, slowId)).get(slowId);
    steps.push(`slow transaction: source v${slowSource ?? '-'} / index v${slowIndex ?? '-'}`);

    // 6. Every (id, version): source ↔ index and source ↔ consumer projection.
    const c = await compareAll();
    steps.push(...describe(c, fmt));
    const summary = `${fmt(c.sourceRows)} source / ${fmt(c.indexDocs)} index / ${fmt(c.consumer.projected)} consumer`;
    if (c.problems > 0) return fail(`${summary}, ${c.problems} mismatches`, steps);
    return pass(`${summary} / 0 dupes, ${fmt(c.stream.duplicates)} replays absorbed`, steps);
  },
};
