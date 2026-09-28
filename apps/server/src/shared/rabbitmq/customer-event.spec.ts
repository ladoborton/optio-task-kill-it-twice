import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CustomerEvent, latestPerCustomer, parseCustomerEvent } from './customer-event';

const event = (customer_id: number, version: number, op: CustomerEvent['op'] = 'upsert'): CustomerEvent => ({
  customer_id,
  version,
  op,
  source: 'incremental',
  data: op === 'delete' ? null : { email: 'a@b.c', city: 'Tbilisi', segment: 'vip' },
  emitted_at: '2026-09-28T00:00:00Z',
});

test('latestPerCustomer keeps the highest version per customer, order-independent', () => {
  const out = latestPerCustomer([event(1, 2), event(2, 1), event(1, 5), event(1, 3), event(2, 1)]);
  assert.deepEqual(out.map((e) => [e.customer_id, e.version]).sort(), [[1, 5], [2, 1]]);
});

test('a delete with a higher version wins over an older upsert', () => {
  const [only] = latestPerCustomer([event(7, 3), event(7, 4, 'delete')]);
  assert.equal(only.op, 'delete');
});

test('parse accepts a valid event and rejects malformed ones', () => {
  assert.equal(parseCustomerEvent(Buffer.from(JSON.stringify(event(1, 1)))).customer_id, 1);
  assert.throws(() => parseCustomerEvent(Buffer.from('not json')));
  assert.throws(() => parseCustomerEvent(Buffer.from(JSON.stringify({ ...event(1, 1), version: '1' }))));
  assert.throws(() => parseCustomerEvent(Buffer.from(JSON.stringify({ ...event(1, 1), op: 'upsert', data: null }))));
});
