import { db } from './db';
import { sleep } from './wait';

export interface TrafficStats {
  updates: number;
  inserts: number;
  deletes: number;
  transactions: number;
}

/**
 * Source change traffic while a gate runs: single-row updates, multi-row updates in one
 * transaction, inserts and deletes, spread over the whole id range — so changes hit rows the
 * backfill has already copied as well as rows it has not reached yet.
 */
export function startTraffic(maxId: number): { stop: () => Promise<TrafficStats> } {
  const stats: TrafficStats = { updates: 0, inserts: 0, deletes: 0, transactions: 0 };
  let running = true;
  const randomId = () => 1 + Math.floor(Math.random() * maxId);

  const loop = (async () => {
    while (running) {
      const roll = Math.random();
      if (roll < 0.55) {
        const r = await db.query('UPDATE customers SET balance = balance + 1 WHERE id = $1', [randomId()]);
        stats.updates += r.rowCount ?? 0;
      } else if (roll < 0.7) {
        // Many rows, one transaction: several outbox rows share one txid.
        const ids = Array.from({ length: 25 }, randomId);
        const r = await db.query(`UPDATE customers SET segment = 'vip' WHERE id = ANY($1::bigint[])`, [ids]);
        stats.updates += r.rowCount ?? 0;
      } else if (roll < 0.85) {
        await db.query(`INSERT INTO customers (email, name, city, segment) VALUES ('traffic@example.com', 'Traffic', 'Batumi', 'new')`);
        stats.inserts++;
      } else {
        const r = await db.query('DELETE FROM customers WHERE id = $1', [randomId()]);
        stats.deletes += r.rowCount ?? 0;
      }
      stats.transactions++;
      await sleep(5);
    }
  })();

  return {
    stop: async () => {
      running = false;
      await loop;
      return stats;
    },
  };
}

/**
 * SPEC §5.4 / §14.1: a transaction that takes an outbox seq early but commits late, while other
 * transactions with higher seqs commit in the meantime. A reader that trusts seq order alone
 * (or skips "gaps" after a timeout shorter than holdMs) loses this change.
 */
export async function slowTransaction(customerId: number, holdMs: number): Promise<void> {
  const client = await db.connect();
  try {
    await client.query('BEGIN');
    await client.query(`UPDATE customers SET city = 'SlowTx' WHERE id = $1`, [customerId]);
    await sleep(holdMs);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK').catch(() => undefined);
    throw e;
  } finally {
    client.release();
  }
}
