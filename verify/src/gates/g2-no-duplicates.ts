import { Check, fail, pass } from '../types';
import { db, scalar } from '../lib/db';
import { kill, start, stop } from '../lib/docker';
import { countDocs, deleteIndex, versionsInRange } from '../lib/es';
import { purgeQueue, queueStats } from '../lib/rabbit';
import {
  backfillCompleted,
  backfillPosition,
  fmt,
  incrementalLagEvents,
  MIN,
  resetBackfill,
  resetIncrementalToOutboxEnd,
} from '../lib/state';
import { slowTransaction, startTraffic } from '../lib/traffic';
import { sleep, waitFor } from '../lib/wait';

const QUEUE = 'consumer.customers';
const PAGE = 10_000; // ids per comparison page (ES max_result_window)
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
    let lagQuiet = 0;
    await waitFor('incremental lag to reach 0', async () => {
      lagQuiet = (await incrementalLagEvents()) === 0 ? lagQuiet + 1 : 0;
      return lagQuiet >= 3 ? true : undefined;
    }, 10 * MIN, 1_000);
    let quietPolls = 0;
    await waitFor(
      `queue ${QUEUE} to drain`,
      async () => {
        const q = await queueStats(QUEUE);
        quietPolls = q && q.messages === 0 && q.unacked === 0 ? quietPolls + 1 : 0;
        return quietPolls >= 3 ? true : undefined;
      },
      10 * MIN,
      1_000,
    );

    // Highest id ever handed out, so rows inserted and then deleted are checked for leftovers too.
    const maxId = Number(await scalar<string>('SELECT greatest((SELECT max(id) FROM customers), (SELECT last_value FROM customers_id_seq))'));

    // 5. Compare (id, version) source ↔ index, page by page (never the whole table in memory).
    const sourceRows = Number(await scalar<string>('SELECT count(*) FROM customers'));
    const indexDocs = await countDocs();
    let esMissing = 0, esStale = 0, esExtra = 0;
    for (let from = 1; from <= maxId; from += PAGE) {
      const to = from + PAGE - 1;
      const { rows } = await db.query<{ id: string; version: string }>(
        'SELECT id, version FROM customers WHERE id BETWEEN $1 AND $2', [from, to]);
      const index = await versionsInRange(from, to);
      for (const r of rows) {
        const v = index.get(Number(r.id));
        if (v === undefined) esMissing++;
        else if (v !== Number(r.version)) esStale++;
        index.delete(Number(r.id));
      }
      esExtra += index.size;
    }

    // The slow transaction's change specifically (also covered by the full comparison above).
    const slowSource = await scalar<string | null>('SELECT version FROM customers WHERE id = $1', [slowId]);
    const slowIndex = (await versionsInRange(slowId, slowId)).get(slowId);
    steps.push(`slow transaction: source v${slowSource ?? '-'} / index v${slowIndex ?? '-'}`);

    // 6. Compare source ↔ consumer projection in one SQL join (same Postgres, separate schema).
    const { rows: [c] } = await db.query<{ missing: string; stale: string; extra: string; projected: string }>(`
      SELECT count(*) FILTER (WHERE p.id IS NULL)                                          AS missing,
             count(*) FILTER (WHERE s.id IS NOT NULL AND p.id IS NOT NULL
                                AND (p.version <> s.version OR p.deleted))                 AS stale,
             count(*) FILTER (WHERE s.id IS NULL AND NOT p.deleted)                        AS extra,
             count(*) FILTER (WHERE p.id IS NOT NULL AND NOT p.deleted)                    AS projected
        FROM public.customers s
        FULL JOIN consumer.customers p ON p.id = s.id`);
    const stats = (await db.query<{ applied: string; duplicates: string }>('SELECT applied, duplicates FROM consumer.stats')).rows[0];

    steps.push(`index: ${fmt(indexDocs)} docs, ${esMissing} missing, ${esStale} stale, ${esExtra} extra`);
    steps.push(`consumer: ${fmt(Number(c.projected))} rows, ${c.missing} missing, ${c.stale} stale, ${c.extra} extra`);
    steps.push(`stream: ${fmt(Number(stats.applied) + Number(stats.duplicates))} delivered, ${fmt(Number(stats.duplicates))} duplicates ignored`);

    const problems = esMissing + esStale + esExtra + Number(c.missing) + Number(c.stale) + Number(c.extra);
    const summary = `${fmt(sourceRows)} source / ${fmt(indexDocs)} index / ${fmt(Number(c.projected))} consumer`;
    if (problems > 0 || indexDocs !== sourceRows) return fail(`${summary}, ${problems} mismatches`, steps);
    return pass(`${summary} / 0 dupes, ${fmt(Number(stats.duplicates))} replays absorbed`, steps);
  },
};
