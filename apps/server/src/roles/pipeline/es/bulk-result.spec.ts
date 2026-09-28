import { test } from 'node:test';
import assert from 'node:assert/strict';
import { classifyItem, summarize } from './bulk-result';

test('2xx is written, 409 is a conflict (idempotent replay), not a failure', () => {
  assert.equal(classifyItem({ status: 201 }), 'written');
  assert.equal(classifyItem({ status: 200 }), 'written');
  assert.equal(classifyItem({ status: 409, error: { type: 'version_conflict_engine_exception' } }), 'conflict');
});

test('overload and server errors are retryable', () => {
  assert.equal(classifyItem({ status: 429, error: { type: 'es_rejected_execution_exception' } }), 'retryable');
  assert.equal(classifyItem({ status: 503, error: { type: 'unavailable_shards_exception' } }), 'retryable');
});

test('a document the mapping rejects is permanent', () => {
  assert.equal(classifyItem({ status: 400, error: { type: 'mapper_parsing_exception' } }), 'permanent');
  assert.equal(classifyItem({ status: 400, error: { type: 'strict_dynamic_mapping_exception' } }), 'permanent');
});

test('missing index is its own outcome', () => {
  assert.equal(classifyItem({ status: 404, error: { type: 'index_not_found_exception' } }), 'index_missing');
});

test('summarize keeps batch positions of failed items', () => {
  const s = summarize([
    { status: 201 },
    { status: 400, error: { type: 'mapper_parsing_exception', reason: 'bad age' } },
    { status: 409, error: { type: 'version_conflict_engine_exception' } },
    { status: 201 },
  ]);
  assert.equal(s.written, 2);
  assert.equal(s.conflicts, 1);
  assert.deepEqual(s.failed, [
    { position: 1, outcome: 'permanent', status: 400, type: 'mapper_parsing_exception', reason: 'bad age' },
  ]);
});
