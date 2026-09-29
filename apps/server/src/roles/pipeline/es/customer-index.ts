import { Client, errors } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import { config } from '../../../shared/config';
import { ES_CLIENT } from '../../../shared/elasticsearch';
import { log } from '../../../shared/logger';
import { CustomerRow } from '../source/customer-source';

// SPEC §4.5: `dynamic: strict` everywhere. An unknown field or a wrongly typed value is
// rejected per document — that is how a bad client record becomes visible (G4) instead of
// silently polluting the index.
const MAPPINGS = {
  dynamic: 'strict',
  properties: {
    id: { type: 'long' },
    email: { type: 'keyword' },
    name: { type: 'text', fields: { raw: { type: 'keyword' } } },
    city: { type: 'keyword' },
    segment: { type: 'keyword' },
    balance: { type: 'scaled_float', scaling_factor: 100 },
    attributes: {
      type: 'object',
      dynamic: 'strict',
      properties: {
        age: { type: 'integer' },
        signup_channel: { type: 'keyword' },
      },
    },
    version: { type: 'long' },
    updated_at: { type: 'date' },
  },
} as const;

export function toDocument(row: CustomerRow) {
  return {
    id: Number(row.id),
    email: row.email,
    name: row.name,
    city: row.city,
    segment: row.segment,
    balance: Number(row.balance),
    attributes: row.attributes,
    version: Number(row.version),
    updated_at: row.updated_at,
  };
}

@Injectable()
export class CustomerIndex {
  readonly name = config.esIndex;
  private ready = false;

  constructor(@Inject(ES_CLIENT) private readonly es: Client) {}

  /** ensure() once per process, again after invalidate(). */
  async ensureReady(): Promise<void> {
    if (this.ready) return;
    await this.ensure();
    this.ready = true;
  }

  /** The index turned out to be missing (deleted by seed/verify): recreate it on next use. */
  invalidate(): void {
    this.ready = false;
  }

  /** Creates the index with its strict mapping if it doesn't exist. Safe to call concurrently. */
  async ensure(): Promise<void> {
    if (await this.es.indices.exists({ index: this.name })) return;
    try {
      await this.es.indices.create({
        index: this.name,
        settings: {
          // Single-node dev cluster: replicas could never be assigned and would keep health yellow.
          number_of_shards: 1,
          number_of_replicas: 0,
          // A delete leaves a versioned tombstone; only while it exists is a late, older upsert
          // (a backfill page read before the delete, a retried batch) rejected. The default 60 s
          // is shorter than a long backoff; keep tombstones for an hour.
          'index.gc_deletes': '1h',
        },
        mappings: MAPPINGS,
      });
      log('es.index_created', { index: this.name });
    } catch (e) {
      const alreadyExists =
        e instanceof errors.ResponseError && e.body?.error?.type === 'resource_already_exists_exception';
      if (!alreadyExists) throw e;
    }
  }
}
