import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { CustomerEvent, latestPerCustomer } from '../../shared/rabbitmq/customer-event';

export interface ApplyResult {
  applied: number;
  duplicates: number;
}

@Injectable()
export class ProjectionRepository {
  constructor(private readonly db: DataSource) {}

  /**
   * SPEC §6.3 / AGENTS invariant 3: dedup, projection update and counters commit in ONE
   * transaction. Either the batch is fully recorded as applied, or not at all — so the ack that
   * follows can never acknowledge something that was not stored, and a redelivery after a crash
   * finds every event already in applied_events.
   */
  apply(events: CustomerEvent[]): Promise<ApplyResult> {
    return this.db.transaction(async (tx) => {
      // The (customer_id, version) primary key is the dedup record. DO NOTHING + RETURNING yields
      // exactly the events that are new; everything else is a redelivery or a replayed batch.
      const fresh: { customer_id: string; version: string }[] = await tx.query(
        `INSERT INTO consumer.applied_events (customer_id, version)
         SELECT * FROM unnest($1::bigint[], $2::bigint[])
         ON CONFLICT DO NOTHING
         RETURNING customer_id, version`,
        [events.map((e) => e.customer_id), events.map((e) => e.version)],
      );
      const freshKeys = new Set(fresh.map((r) => `${r.customer_id}:${r.version}`));
      // One row per customer: ON CONFLICT DO UPDATE may not touch the same row twice in a statement.
      const toApply = latestPerCustomer(events.filter((e) => freshKeys.has(`${e.customer_id}:${e.version}`)));

      if (toApply.length > 0) {
        // Compare-and-write in one statement (SPEC §5.6 rule 1): an older version never
        // overwrites a newer one, whatever order the events arrive in.
        await tx.query(
          `INSERT INTO consumer.customers (id, version, deleted, email, city, segment, source)
           SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::boolean[], $4::text[], $5::text[], $6::text[], $7::text[])
           ON CONFLICT (id) DO UPDATE
              SET version = EXCLUDED.version, deleted = EXCLUDED.deleted, email = EXCLUDED.email,
                  city = EXCLUDED.city, segment = EXCLUDED.segment, source = EXCLUDED.source, applied_at = now()
            WHERE consumer.customers.version < EXCLUDED.version`,
          [
            toApply.map((e) => e.customer_id),
            toApply.map((e) => e.version),
            toApply.map((e) => e.op === 'delete'),
            toApply.map((e) => e.data?.email ?? null),
            toApply.map((e) => e.data?.city ?? null),
            toApply.map((e) => e.data?.segment ?? null),
            toApply.map((e) => e.source),
          ],
        );
      }

      const duplicates = events.length - fresh.length;
      await tx.query(`UPDATE consumer.stats SET applied = applied + $1, duplicates = duplicates + $2`, [fresh.length, duplicates]);
      return { applied: fresh.length, duplicates };
    });
  }
}
