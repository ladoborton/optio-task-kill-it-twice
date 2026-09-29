import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { OutboxRow } from '../changes/change';

/** Incremental position: outbox rows are consumed in (txid, seq) order. */
export interface OutboxPosition {
  txid: string;
  seq: string;
}

@Injectable()
export class OutboxReader {
  constructor(private readonly db: DataSource) {}

  /**
   * SPEC v1.3 §5.4 (D-003): the next outbox rows after `after`, from FINISHED transactions only.
   *
   * pg_snapshot_xmin(pg_current_snapshot()) is the oldest transaction still running; every txid
   * below it has committed or rolled back, so the set of rows with txid < xmin is final — nothing
   * can later appear before our position. A long-running transaction holds xmin back: the loop
   * then waits (lag grows) instead of skipping past it and losing its changes.
   */
  readCommitted(after: OutboxPosition, limit: number): Promise<OutboxRow[]> {
    return this.db.query(
      `SELECT seq, txid, customer_id, op, version
         FROM customer_changes
        WHERE (txid, seq) > ($1::bigint, $2::bigint)
          AND txid < pg_snapshot_xmin(pg_current_snapshot())::text::bigint
        ORDER BY txid, seq
        LIMIT $3`,
      [after.txid, after.seq, limit],
    );
  }
}
