import { Check, fail, pass } from '../types';
import { db } from '../lib/db';
import { versionsOf } from '../lib/es';
import { incrementalLagEvents, MIN } from '../lib/state';
import { waitFor } from '../lib/wait';

const BATCH = 500;
const BAD_POSITIONS = [17, 250, 433]; // positions inside the batch that the index will reject
// Valid in Postgres (it's just JSONB), rejected by the index mapping: attributes.age is an integer
// field (SPEC §4.5).
const BAD_ATTRIBUTES = { age: 'not-a-number' };
const FIXED_ATTRIBUTES = { age: 30 };

type DlqRow = { customer_id: string; status: string; attempts: number; batch_id: string; batch_position: number; error_type: string; sink: string };

const lagZero = async () => {
  let quiet = 0;
  await waitFor('incremental lag to reach 0', async () => {
    quiet = (await incrementalLagEvents()) === 0 ? quiet + 1 : 0;
    return quiet >= 2 ? true : undefined;
  }, 2 * MIN, 500);
};

async function requestReplay(ids: string[]): Promise<void> {
  await db.query(`UPDATE dlq_records SET replay_requested_at = now() WHERE sink = 'es' AND customer_id = ANY($1::bigint[])`, [ids]);
  await waitFor('replay requests to be handled', async () => {
    const open = Number((await db.query(`SELECT count(*) AS n FROM dlq_records WHERE customer_id = ANY($1::bigint[]) AND replay_requested_at IS NOT NULL`, [ids])).rows[0].n);
    return open === 0 ? true : undefined;
  }, MIN, 500);
}

const dlqRows = async (ids: string[]) =>
  (await db.query<DlqRow>(
    `SELECT customer_id, status, attempts, batch_id, batch_position, error_type, sink
       FROM dlq_records WHERE customer_id = ANY($1::bigint[]) ORDER BY customer_id`, [ids])).rows;

// SPEC §11 G4: 3 of 500 records in one batch are rejected by the index. The other 497 are written,
// the 3 land in the DLQ with enough context to replay, the batch is not rolled back or blocked,
// and a replay after fixing the source delivers them.
export const g4: Check = {
  id: 'G4',
  title: 'partial batch failure',
  async run() {
    const steps: string[] = [];
    let badIds: string[] = [];
    try {
      // 1. Idle pipeline, so the 500 inserts below form exactly one incremental page.
      await db.query(`UPDATE pipeline_control SET incremental_state = 'running', batch_size = $1, updated_at = now()`, [BATCH]);
      await lagZero();

      // 2. 500 new customers in one transaction (one txid ⇒ one page), 3 of them malformed.
      const run = Date.now();
      const { rows } = await db.query<{ id: string }>(
        `INSERT INTO customers (email, name, city, segment, attributes)
         SELECT 'g4-' || $1 || '-' || n || '@example.com', 'G4 ' || n, 'Kutaisi', 'new',
                CASE WHEN n = ANY($2::int[]) THEN $3::jsonb ELSE '{"age": 40}'::jsonb END
           FROM generate_series(0, $4 - 1) AS n
         RETURNING id`,
        [run, BAD_POSITIONS, JSON.stringify(BAD_ATTRIBUTES), BATCH]);
      const ids = rows.map((r) => r.id);
      badIds = BAD_POSITIONS.map((p) => ids[p]);
      const goodIds = ids.filter((id) => !badIds.includes(id));
      steps.push(`inserted ${BATCH} customers in one transaction, bad at positions ${BAD_POSITIONS.join(', ')}`);

      // 3. The page must be fully handled — not stuck retrying because of 3 bad records.
      await lagZero();
      const indexed = await versionsOf(ids.map(Number));
      const goodIndexed = goodIds.filter((id) => indexed.has(Number(id))).length;
      const badIndexed = badIds.filter((id) => indexed.has(Number(id))).length;
      const dlq = await dlqRows(ids);
      steps.push(`index: ${goodIndexed} of ${goodIds.length} good written, ${badIndexed} bad written`);
      steps.push(`dlq: ${dlq.length} rows — ${dlq.map((d) => `#${d.customer_id} pos ${d.batch_position} ${d.error_type}`).join('; ')}`);

      if (goodIndexed !== goodIds.length) return fail(`${goodIds.length - goodIndexed} good records not written`, steps);
      if (badIndexed !== 0) return fail(`${badIndexed} bad records ended up in the index`, steps);
      const sameBatch = new Set(dlq.map((d) => d.batch_id)).size === 1;
      const exact = dlq.length === 3 && dlq.every((d) => badIds.includes(d.customer_id) && d.status === 'pending' && d.sink === 'es');
      if (!exact || !sameBatch) return fail(`expected exactly the 3 bad records pending in the DLQ from one batch`, steps);

      // 4. Replay without a fix: still rejected, stays pending, attempt counted.
      await requestReplay(badIds);
      const unfixed = await dlqRows(badIds);
      steps.push(`replay before fix: ${unfixed.map((d) => `${d.status}/attempts ${d.attempts}`).join(', ')}`);
      if (!unfixed.every((d) => d.status === 'pending' && d.attempts === 2)) return fail('replay of an unfixed record did not stay pending with attempts = 2', steps);

      // 5. Fix the source while incremental is paused, so only the replay can deliver the fix.
      await db.query(`UPDATE pipeline_control SET incremental_state = 'paused', updated_at = now()`);
      await db.query(`UPDATE customers SET attributes = $2::jsonb WHERE id = ANY($1::bigint[])`, [badIds, JSON.stringify(FIXED_ATTRIBUTES)]);
      await requestReplay(badIds);
      const fixed = await dlqRows(badIds);
      const afterReplay = await versionsOf(badIds.map(Number));
      steps.push(`replay after fix (incremental paused): ${fixed.map((d) => d.status).join(', ')}, ${afterReplay.size} of 3 in index`);
      if (!fixed.every((d) => d.status === 'replayed') || afterReplay.size !== 3) return fail('replay after fixing the source did not deliver the records', steps);

      return pass(`${goodIndexed} written, 3 in DLQ, 3 replayed after fix`, steps);
    } finally {
      // Leave a converged system behind whatever happened: no malformed rows, incremental running.
      if (badIds.length) await db.query(`UPDATE customers SET attributes = $2::jsonb WHERE id = ANY($1::bigint[]) AND attributes->>'age' = $3`, [badIds, JSON.stringify(FIXED_ATTRIBUTES), BAD_ATTRIBUTES.age]);
      await db.query(`UPDATE pipeline_control SET incremental_state = 'running', updated_at = now()`);
    }
  },
};
