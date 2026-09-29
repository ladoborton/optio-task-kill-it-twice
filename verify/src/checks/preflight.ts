import { Check, fail, pass } from '../types';
import { db, scalar } from '../lib/db';
import { basicAuth, getJson } from '../lib/http';
import { docker } from '../lib/docker';
import { env } from '../lib/env';

const REQUIRED_TABLES = [
  'customers',
  'customer_changes',
  'pipeline_checkpoints',
  'pipeline_control',
  'pipeline_heartbeat',
  'dlq_records',
];

type Step = { name: string; run: () => Promise<string> };

const steps: Step[] = [
  {
    name: 'docker control (socket mounted)',
    run: async () => `daemon ${await docker('version', '--format', '{{.Server.Version}}')}`,
  },
  {
    name: 'postgres reachable',
    run: async () => `${await scalar<string>('SHOW server_version')}`,
  },
  {
    name: 'schema migrated',
    run: async () => {
      const missing: string[] = [];
      for (const t of REQUIRED_TABLES) {
        if (!(await scalar<string | null>('SELECT to_regclass($1)::text', [t]))) missing.push(t);
      }
      if (missing.length) throw new Error(`missing tables: ${missing.join(', ')}`);
      return `${REQUIRED_TABLES.length} tables`;
    },
  },
  {
    name: 'elasticsearch healthy',
    run: async () => {
      const h = await getJson<{ status: string }>(`${env.esUrl}/_cluster/health`);
      if (h.status === 'red') throw new Error('cluster status red');
      return `status ${h.status}`;
    },
  },
  {
    name: 'rabbitmq reachable',
    run: async () => {
      const o = await getJson<{ rabbitmq_version: string }>(
        `${env.rabbitMgmtUrl}/api/overview`,
        { headers: basicAuth(env.rabbitUser, env.rabbitPassword) },
      );
      return `v${o.rabbitmq_version}`;
    },
  },
  {
    name: `seeded (ids 1..${env.seedCount.toLocaleString('en-US')})`,
    run: async () => {
      // Every seeded id is either still present or was deleted through the outbox since the seed
      // (seed truncates the outbox, so every later delete is recorded there). Gates and the
      // simulation delete rows legitimately; a row that vanished without a trace is still caught.
      const { rows: [r] } = await db.query<{ present: string; deleted: string; unaccounted: string }>(`
        SELECT count(*) FILTER (WHERE c.id IS NOT NULL)                AS present,
               count(*) FILTER (WHERE c.id IS NULL AND d.id IS NOT NULL) AS deleted,
               count(*) FILTER (WHERE c.id IS NULL AND d.id IS NULL)     AS unaccounted
          FROM generate_series(1, $1::bigint) AS g(id)
          LEFT JOIN customers c ON c.id = g.id
          LEFT JOIN (SELECT DISTINCT customer_id AS id FROM customer_changes WHERE op = 'delete') d ON d.id = g.id`,
        [env.seedCount]);
      const fmt = (s: string) => Number(s).toLocaleString('en-US');
      if (Number(r.unaccounted) > 0) throw new Error(`${fmt(r.unaccounted)} seeded ids missing without a recorded delete`);
      return `${fmt(r.present)} present, ${fmt(r.deleted)} deleted since seed`;
    },
  },
  { name: 'triggers: version bump + outbox', run: checkTriggers },
];

// Exercises the triggers inside a transaction that is always rolled back, so verify leaves
// no data behind. Note: sequences are not transactional — the rollback still burns seq
// values, i.e. this check itself creates the outbox gaps described in SPEC §5.4.
async function checkTriggers(): Promise<string> {
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    const since = Number((await client.query('SELECT coalesce(max(seq), 0) AS s FROM customer_changes')).rows[0].s);
    const outbox = async () =>
      (await client.query(
        'SELECT customer_id::int AS id, op, version::int AS version FROM customer_changes WHERE seq > $1 ORDER BY seq',
        [since],
      )).rows as { id: number; op: string; version: number }[];

    const { id, version } = (await client.query(
      'SELECT id::int AS id, version::int AS version FROM customers ORDER BY id LIMIT 1',
    )).rows[0];

    await client.query(`UPDATE customers SET name = name || ' (verify)' WHERE id = $1`, [id]);
    const bumped = Number((await client.query('SELECT version FROM customers WHERE id = $1', [id])).rows[0].version);
    if (bumped !== version + 1) throw new Error(`UPDATE: version ${version} -> ${bumped}, expected ${version + 1}`);

    await client.query('DELETE FROM customers WHERE id = $1', [id]);

    const inserted = (await client.query(
      `INSERT INTO customers (email, name, city, segment) VALUES ('verify@example.com', 'Verify', 'Tbilisi', 'new')
       RETURNING id::int AS id, version::int AS version`,
    )).rows[0];

    const expected = [
      { id, op: 'upsert', version: version + 1 },
      { id, op: 'delete', version: version + 2 },
      { id: inserted.id, op: 'upsert', version: 1 },
    ];
    const actual = await outbox();
    if (JSON.stringify(actual) !== JSON.stringify(expected)) {
      throw new Error(`outbox ${JSON.stringify(actual)} != expected ${JSON.stringify(expected)}`);
    }
    return 'update/delete/insert produce expected outbox rows';
  } finally {
    await client.query('ROLLBACK').catch(() => undefined);
    client.release();
  }
}

export const preflight: Check = {
  id: 'preflight',
  title: 'preflight',
  async run() {
    const lines: string[] = [];
    let failed = 0;
    for (const s of steps) {
      try {
        lines.push(`[ok]   ${s.name}: ${await s.run()}`);
      } catch (e) {
        failed++;
        lines.push(`[FAIL] ${s.name}: ${(e as Error).message}`);
      }
    }
    return failed
      ? fail(`${failed} of ${steps.length} steps failed`, lines)
      : pass(`${steps.length} steps`, lines);
  },
};
