import { test } from 'node:test';
import assert from 'node:assert/strict';
import { CircuitBreaker, CircuitOpenError } from './circuit-breaker';

const ok = async () => 'ok';
const boom = async () => {
  throw new Error('sink down');
};

function setup() {
  let t = 0;
  const transitions: string[] = [];
  const breaker = new CircuitBreaker('es', 3, 5_000, (from, to) => transitions.push(`${from}->${to}`), () => t);
  return { breaker, transitions, advance: (ms: number) => (t += ms) };
}

test('opens after `threshold` consecutive failures and then fails fast without calling', async () => {
  const { breaker, transitions } = setup();
  for (let i = 0; i < 3; i++) await assert.rejects(breaker.run(boom), /sink down/);
  assert.equal(breaker.current, 'open');
  let called = false;
  await assert.rejects(breaker.run(async () => { called = true; }), CircuitOpenError);
  assert.equal(called, false);
  assert.deepEqual(transitions, ['closed->open']);
});

test('a success in between resets the failure count', async () => {
  const { breaker } = setup();
  await assert.rejects(breaker.run(boom));
  await assert.rejects(breaker.run(boom));
  await breaker.run(ok);
  await assert.rejects(breaker.run(boom));
  assert.equal(breaker.current, 'closed');
});

test('after openMs one probe goes through; success closes the circuit', async () => {
  const { breaker, transitions, advance } = setup();
  for (let i = 0; i < 3; i++) await assert.rejects(breaker.run(boom));
  advance(4_999);
  await assert.rejects(breaker.run(ok), CircuitOpenError);
  advance(1);
  assert.equal(await breaker.run(ok), 'ok');
  assert.equal(breaker.current, 'closed');
  assert.deepEqual(transitions, ['closed->open', 'open->half_open', 'half_open->closed']);
});

test('a failed probe re-opens for another full interval', async () => {
  const { breaker, advance } = setup();
  for (let i = 0; i < 3; i++) await assert.rejects(breaker.run(boom));
  advance(5_000);
  await assert.rejects(breaker.run(boom), /sink down/);
  assert.equal(breaker.current, 'open');
  advance(4_000);
  await assert.rejects(breaker.run(ok), (e: CircuitOpenError) => e.retryAfterMs === 1_000);
});

test('only one probe at a time', async () => {
  const { breaker, advance } = setup();
  for (let i = 0; i < 3; i++) await assert.rejects(breaker.run(boom));
  advance(5_000);
  let release!: () => void;
  const probe = breaker.run(() => new Promise<void>((r) => (release = r)));
  await assert.rejects(breaker.run(ok), CircuitOpenError);
  release();
  await probe;
  assert.equal(breaker.current, 'closed');
});
