import { Check, fail, pass } from '../types';
import { db, scalar } from '../lib/db';
import { kill, start, stop } from '../lib/docker';
import { countDocs, deleteIndex, versionsInRange } from '../lib/es';
import { purgeQueue, queueStats } from '../lib/rabbit';
import { backfillCompleted, backfillPosition, fmt, MIN, resetBackfill } from '../lib/state';
import { sleep, waitFor } from '../lib/wait';

const QUEUE = 'consumer.customers';
const PAGE = 10_000; // ids per comparison page (ES max_result_window)

// SPEC §11 G2: after repeated kills of the pipeline and the consumer, every customer is present
// exactly once, at its current version, in the index and in the consumer's projection.
// The stream itself may carry duplicate messages (at-least-once); the consumer must absorb them.
export const g2: Check = {
  id: 'G2',
  title: 'no duplicates',
  async run() {
    const steps: string[] = [];

    // 1. Everything downstream empty, backfill from zero.
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
    steps.push('reset: empty index, empty projection, purged queue, backfill from 0');

    // 2. Kill both sides mid-run. Every kill of the pipeline replays its in-flight batch into both
    //    sinks; every kill of the consumer redelivers its unacked messages.
    const kills: [number, 'pipeline' | 'consumer'][] = [[0.25, 'pipeline'], [0.5, 'consumer'], [0.75, 'pipeline']];
    for (const [share, service] of kills) {
      const target = Math.floor(maxId * share);
      await waitFor(`backfill to reach ${share * 100}%`, async () => ((await backfillPosition()) >= target ? true : undefined), 10 * MIN);
      await kill(service);
      await sleep(1_000);
      await start(service);
      steps.push(`killed + restarted ${service} at ~${share * 100}%`);
    }

    // 3. Wait until nothing is in flight anywhere.
    await waitFor('backfill to complete', async () => ((await backfillCompleted()) ? true : undefined), 15 * MIN, 1_000);
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

    // 4. Compare (id, version) source ↔ index, page by page (never the whole table in memory).
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

    // 5. Compare source ↔ consumer projection in one SQL join (same Postgres, separate schema).
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
