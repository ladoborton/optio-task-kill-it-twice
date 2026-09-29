import { db, scalar } from './db';
import { countDocs, versionsInRange } from './es';
import { queueStats } from './rabbit';
import { incrementalLagEvents, MIN } from './state';
import { waitFor } from './wait';

export const QUEUE = 'consumer.customers';
const PAGE = 10_000; // ids per comparison page (ES max_result_window)

/**
 * Waits until nothing is in flight: outbox consumed by the incremental loop, queue drained.
 * Each condition must hold for 3 consecutive polls; the returned time is when the final quiet
 * streak STARTED — the moment the system actually converged, not when verify became sure of it.
 */
export async function waitConverged(timeoutMs = 10 * MIN): Promise<number> {
  const quiet = async (what: string, isQuiet: () => Promise<boolean>): Promise<number> => {
    let streak = 0;
    let since = 0;
    return waitFor(what, async () => {
      if (await isQuiet()) {
        if (streak++ === 0) since = Date.now();
      } else streak = 0;
      return streak >= 3 ? since : undefined;
    }, timeoutMs, 1_000);
  };
  const lagZeroAt = await quiet('incremental lag to reach 0', async () => (await incrementalLagEvents()) === 0);
  const drainedAt = await quiet(`queue ${QUEUE} to drain`, async () => {
    const q = await queueStats(QUEUE);
    return !!q && q.messages === 0 && q.unacked === 0;
  });
  return Math.max(lagZeroAt, drainedAt);
}

export interface Comparison {
  sourceRows: number;
  indexDocs: number;
  index: { missing: number; stale: number; extra: number };
  consumer: { projected: number; missing: number; stale: number; extra: number };
  stream: { applied: number; duplicates: number };
  /** Total mismatches in index + consumer; 0 means both match the source exactly. */
  problems: number;
}

/**
 * Every (id, version) of the source against the index (page by page — never the whole table in
 * memory) and against the consumer projection (one SQL join). Deleted customers must be absent.
 */
export async function compareAll(): Promise<Comparison> {
  // Highest id ever handed out, so rows inserted and then deleted are checked for leftovers too.
  const maxId = Number(await scalar<string>('SELECT greatest((SELECT max(id) FROM customers), (SELECT last_value FROM customers_id_seq))'));
  const sourceRows = Number(await scalar<string>('SELECT count(*) FROM customers'));
  const indexDocs = await countDocs();

  const index = { missing: 0, stale: 0, extra: 0 };
  for (let from = 1; from <= maxId; from += PAGE) {
    const to = from + PAGE - 1;
    const { rows } = await db.query<{ id: string; version: string }>('SELECT id, version FROM customers WHERE id BETWEEN $1 AND $2', [from, to]);
    const versions = await versionsInRange(from, to);
    for (const r of rows) {
      const v = versions.get(Number(r.id));
      if (v === undefined) index.missing++;
      else if (v !== Number(r.version)) index.stale++;
      versions.delete(Number(r.id));
    }
    index.extra += versions.size;
  }

  const { rows: [c] } = await db.query<{ missing: string; stale: string; extra: string; projected: string }>(`
    SELECT count(*) FILTER (WHERE p.id IS NULL)                                          AS missing,
           count(*) FILTER (WHERE s.id IS NOT NULL AND p.id IS NOT NULL
                              AND (p.version <> s.version OR p.deleted))                 AS stale,
           count(*) FILTER (WHERE s.id IS NULL AND NOT p.deleted)                        AS extra,
           count(*) FILTER (WHERE p.id IS NOT NULL AND NOT p.deleted)                    AS projected
      FROM public.customers s
      FULL JOIN consumer.customers p ON p.id = s.id`);
  const consumer = { projected: Number(c.projected), missing: Number(c.missing), stale: Number(c.stale), extra: Number(c.extra) };
  const s = (await db.query<{ applied: string; duplicates: string }>('SELECT applied, duplicates FROM consumer.stats')).rows[0];

  const problems = index.missing + index.stale + index.extra + consumer.missing + consumer.stale + consumer.extra
    + (indexDocs !== sourceRows ? 1 : 0);
  return { sourceRows, indexDocs, index, consumer, stream: { applied: Number(s.applied), duplicates: Number(s.duplicates) }, problems };
}

export function describe(c: Comparison, fmt: (n: number) => string): string[] {
  return [
    `index: ${fmt(c.indexDocs)} docs, ${c.index.missing} missing, ${c.index.stale} stale, ${c.index.extra} extra`,
    `consumer: ${fmt(c.consumer.projected)} rows, ${c.consumer.missing} missing, ${c.consumer.stale} stale, ${c.consumer.extra} extra`,
    `stream: ${fmt(c.stream.applied + c.stream.duplicates)} delivered, ${fmt(c.stream.duplicates)} duplicates ignored`,
  ];
}
