// The message contract on the `customers` exchange — the only thing publisher and consumer share.

export interface CustomerEvent {
  customer_id: number;
  /** customers.version this event describes; (customer_id, version) identifies the event. */
  version: number;
  op: 'upsert' | 'delete';
  source: 'backfill' | 'incremental';
  /** Current row for upserts, null for deletes. */
  data: { email: string; city: string; segment: string; [field: string]: unknown } | null;
  emitted_at: string;
}

/** Throws on anything that is not a well-formed CustomerEvent (→ dead-lettered, never retried). */
export function parseCustomerEvent(body: Buffer): CustomerEvent {
  const e = JSON.parse(body.toString('utf8')) as Partial<CustomerEvent>;
  const ok =
    Number.isSafeInteger(e.customer_id) &&
    Number.isSafeInteger(e.version) &&
    (e.op === 'upsert' || e.op === 'delete') &&
    (e.source === 'backfill' || e.source === 'incremental') &&
    (e.op === 'delete' ? e.data === null : typeof e.data === 'object' && e.data !== null);
  if (!ok) throw new Error('not a valid CustomerEvent');
  return e as CustomerEvent;
}

/** Within one batch keep only the newest event per customer (unit-tested). */
export function latestPerCustomer(events: CustomerEvent[]): CustomerEvent[] {
  const latest = new Map<number, CustomerEvent>();
  for (const e of events) {
    const seen = latest.get(e.customer_id);
    if (!seen || e.version > seen.version) latest.set(e.customer_id, e);
  }
  return [...latest.values()];
}
