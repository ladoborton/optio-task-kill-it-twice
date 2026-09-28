import { test } from 'node:test';
import assert from 'node:assert/strict';
import { backoffDelay } from './backoff';

const max = () => 1;
const min = () => 0;

test('ceiling doubles per attempt', () => {
  assert.deepEqual([1, 2, 3, 4].map((a) => backoffDelay(a, 500, 30_000, max)), [500, 1_000, 2_000, 4_000]);
});

test('ceiling is capped', () => {
  assert.equal(backoffDelay(20, 500, 30_000, max), 30_000);
  assert.equal(backoffDelay(1_000, 500, 30_000, max), 30_000);
});

test('never below half the ceiling — no zero-delay retries', () => {
  assert.equal(backoffDelay(1, 500, 30_000, min), 250);
  assert.equal(backoffDelay(20, 500, 30_000, min), 15_000);
});
