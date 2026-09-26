import { dataSource } from '../database/data-source';
import { config } from '../shared/config';
import { log } from '../shared/logger';

const INSERT_CHUNK = `
  INSERT INTO customers (id, email, name, city, segment, balance, attributes, version, updated_at)
  SELECT i,
         'customer' || i || '@example.com',
         'Customer ' || i,
         (ARRAY['Tbilisi','Batumi','Kutaisi','Rustavi','Zugdidi','Gori','Telavi','Poti'])[1 + (i % 8)::int],
         (ARRAY['retail','vip','churn-risk','new'])[1 + (i % 4)::int],
         ((i * 7919) % 1000000) / 100.0,
         jsonb_build_object(
           'age', 18 + i % 60,
           'signup_channel', (ARRAY['web','mobile','branch','partner'])[1 + ((i / 7) % 4)::int]
         ),
         1,
         now()
  FROM generate_series($1::bigint, $2::bigint) AS i`;

// ROLE=seed: deterministic SEED_COUNT customers, generated inside Postgres (no rows pass through
// Node). Resets pipeline state so the next backfill starts from zero.
export async function seed(): Promise<void> {
  const started = Date.now();
  await dataSource.initialize();
  // One dedicated connection: session_replication_role is per session, and a pooled query
  // could land on a different connection where the triggers are still active.
  const qr = dataSource.createQueryRunner();
  await qr.connect();
  try {
    // SPEC §4.8: initial data is the backfill's job, not the incremental's. `replica` disables
    // ordinary triggers for this session only, so the 1M inserts produce no outbox rows.
    await qr.query(`SET session_replication_role = replica`);
    await qr.query(`TRUNCATE customers, customer_changes, dlq_records RESTART IDENTITY`);

    for (let from = 1; from <= config.seedCount; from += config.seedChunk) {
      const to = Math.min(from + config.seedChunk - 1, config.seedCount);
      await qr.query(INSERT_CHUNK, [from, to]);
      log('seed.progress', { inserted: to, total: config.seedCount });
    }

    await qr.query(`SET session_replication_role = origin`);
    // Explicit ids were inserted, so move the identity sequence past them for later inserts.
    await qr.query(`SELECT setval(pg_get_serial_sequence('customers', 'id'), $1)`, [config.seedCount]);
    await qr.query(`UPDATE pipeline_checkpoints SET position = 0, completed_at = NULL, updated_at = now()`);
    await qr.query(`ANALYZE customers`);
  } finally {
    await qr.release();
    await dataSource.destroy();
  }

  await deleteIndex();
  log('seed.done', { rows: config.seedCount, seconds: Math.round((Date.now() - started) / 1000) });
}

// Re-seeding restarts versions at 1; an old index would hold higher versions and silently reject
// every new write (SPEC §5.5), so the index goes with the data.
async function deleteIndex(): Promise<void> {
  const res = await fetch(`${config.esUrl}/${config.esIndex}`, { method: 'DELETE' });
  if (!res.ok && res.status !== 404) throw new Error(`DELETE index -> HTTP ${res.status}`);
  log('seed.index_deleted', { index: config.esIndex, existed: res.ok });
}
