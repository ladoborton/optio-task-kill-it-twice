import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { toDocument } from '../es/customer-index';
import { Rejection } from '../es/es.sink';

export interface ReplayRequest {
  id: string;
  customer_id: string;
}

@Injectable()
export class DlqRepository {
  constructor(private readonly db: DataSource) {}

  /**
   * SPEC §4.4 / §7.3: park permanently rejected records with enough context to understand and
   * replay them without logs. ON CONFLICT DO NOTHING on (sink, customer_id, version): a batch
   * replayed after a crash hits the same rejections and must not record them twice.
   */
  async record(rejections: Rejection[], stream: 'backfill' | 'incremental', batchId: string): Promise<void> {
    if (rejections.length === 0) return;
    await this.db.query(
      `INSERT INTO dlq_records (sink, customer_id, version, payload, error_type, error_reason, stream, batch_id, batch_position)
       SELECT 'es', r.customer_id, r.version, r.payload::jsonb, r.error_type, r.error_reason, $7, $8, r.batch_position
         FROM unnest($1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::int[])
              AS r(customer_id, version, payload, error_type, error_reason, batch_position)
       ON CONFLICT (sink, customer_id, version) DO NOTHING`,
      [
        rejections.map((r) => r.change.id),
        rejections.map((r) => r.change.version),
        rejections.map((r) => JSON.stringify(r.change.op === 'upsert' ? toDocument(r.change.row) : { id: Number(r.change.id), op: 'delete' })),
        rejections.map((r) => r.failure.type),
        rejections.map((r) => r.failure.reason),
        rejections.map((r) => r.failure.position),
        stream,
        batchId,
      ],
    );
  }

  /** Oldest open replay requests (SPEC §7.4). */
  replayRequests(limit: number): Promise<ReplayRequest[]> {
    return this.db.query(
      `SELECT id, customer_id FROM dlq_records
        WHERE replay_requested_at IS NOT NULL AND status = 'pending'
        ORDER BY id LIMIT $1`,
      [limit],
    );
  }

  async markReplayed(ids: string[]): Promise<void> {
    if (ids.length === 0) return;
    await this.db.query(
      `UPDATE dlq_records
          SET status = 'replayed', attempts = attempts + 1, replay_requested_at = NULL, last_attempt_at = now()
        WHERE id = ANY($1::bigint[])`,
      [ids],
    );
  }

  /** The replay was attempted and rejected again: stays pending, attempt counted, latest reason kept. */
  async markAttemptFailed(id: string, errorType: string, errorReason: string): Promise<void> {
    await this.db.query(
      `UPDATE dlq_records
          SET attempts = attempts + 1, replay_requested_at = NULL, last_attempt_at = now(),
              error_type = $2, error_reason = $3
        WHERE id = $1`,
      [id, errorType, errorReason],
    );
  }
}
