import { CustomerRow } from '../source/customer-source';

/** What the sinks are asked to do for one customer. */
export type Change =
  | { op: 'upsert'; id: string; version: string; row: CustomerRow }
  | { op: 'delete'; id: string; version: string };

export const upsert = (row: CustomerRow): Change => ({ op: 'upsert', id: row.id, version: row.version, row });

/** An outbox row as read by the incremental loop (BIGINTs as strings). */
export interface OutboxRow {
  seq: string;
  txid: string;
  customer_id: string;
  op: 'upsert' | 'delete';
  version: string;
}

/**
 * SPEC §5.4: turn a page of outbox rows into one change per customer (pure, unit-tested).
 *
 * The outbox holds keys, not payloads: for every customer touched we ship its CURRENT row, so ten
 * updates to one customer in the page become a single upsert of the latest state. A customer
 * whose row no longer exists is a delete — but only if this page contains its delete entry,
 * because that entry carries the delete's version. If the row is gone and the delete entry is in a
 * later page, we skip it here; that later page ships the delete.
 */
export function planChanges(outbox: OutboxRow[], currentRows: CustomerRow[]): Change[] {
  const current = new Map(currentRows.map((r) => [r.id, r]));
  const deleteVersion = new Map<string, bigint>();
  const touched = new Set<string>();

  for (const o of outbox) {
    touched.add(o.customer_id);
    if (o.op === 'delete') {
      const v = BigInt(o.version);
      const seen = deleteVersion.get(o.customer_id);
      if (seen === undefined || v > seen) deleteVersion.set(o.customer_id, v);
    }
  }

  const changes: Change[] = [];
  for (const id of touched) {
    const row = current.get(id);
    if (row) changes.push(upsert(row));
    else {
      const v = deleteVersion.get(id);
      if (v !== undefined) changes.push({ op: 'delete', id, version: v.toString() });
    }
  }
  return changes;
}
