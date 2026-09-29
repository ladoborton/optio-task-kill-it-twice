import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CustomerRow } from '../source/customer-source';
import { OutboxRow, planChanges } from './change';

const row = (id: string, version: string): CustomerRow => ({
  id, version, email: 'x@y.z', name: 'X', city: 'Tbilisi', segment: 'vip', balance: '1.00', attributes: {}, updated_at: new Date(0),
});
let seq = 0;
const o = (customer_id: string, op: OutboxRow['op'], version: string): OutboxRow => ({
  seq: String(++seq), txid: '1', customer_id, op, version,
});

test('many updates to one customer become one upsert of the current row', () => {
  const changes = planChanges([o('1', 'upsert', '2'), o('1', 'upsert', '3'), o('1', 'upsert', '4')], [row('1', '5')]);
  assert.deepEqual(changes.map((c) => [c.op, c.id, c.version]), [['upsert', '1', '5']]);
});

test('a missing row with a delete entry becomes a delete carrying the delete version', () => {
  const changes = planChanges([o('2', 'upsert', '2'), o('2', 'delete', '3')], []);
  assert.deepEqual(changes.map((c) => [c.op, c.id, c.version]), [['delete', '2', '3']]);
});

test('a missing row without its delete entry in this page is skipped (a later page deletes it)', () => {
  assert.deepEqual(planChanges([o('3', 'upsert', '7')], []), []);
});

test('an existing row wins over an older delete entry (row re-created after the delete)', () => {
  const changes = planChanges([o('4', 'delete', '2'), o('4', 'upsert', '1')], [row('4', '1')]);
  assert.deepEqual(changes.map((c) => c.op), ['upsert']);
});

test('versions compare numerically, not as strings', () => {
  const changes = planChanges([o('5', 'delete', '9'), o('5', 'delete', '10')], []);
  assert.equal(changes[0].version, '10');
});
