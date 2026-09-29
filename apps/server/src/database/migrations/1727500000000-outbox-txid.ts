import { MigrationInterface, QueryRunner } from 'typeorm';

// SPEC v1.3 §5.4 (D-003): read the outbox in commit-safe order.
//
// `seq` is taken when a row is inserted, but transactions commit in a different order, so a
// reader can see seq 101 while seq 100 is still invisible — and a checkpoint past 101 would lose
// 100 for good. Recording the writing transaction's id lets the reader consume only rows of
// transactions that have finished (txid < xmin of the current snapshot), ordered by (txid, seq).
// No row can ever appear behind that position later.
export class OutboxTxid1727500000000 implements MigrationInterface {
  name = 'OutboxTxid1727500000000';

  async up(q: QueryRunner): Promise<void> {
    // pg_current_xact_id() is the 64-bit, wraparound-free transaction id (PG 13+) of the
    // transaction inserting the row — the same value for every row that transaction writes.
    await q.query(`
      ALTER TABLE customer_changes
        ADD COLUMN txid BIGINT NOT NULL DEFAULT (pg_current_xact_id()::text::bigint)`);
    await q.query(`CREATE INDEX customer_changes_txid_seq ON customer_changes (txid, seq)`);

    // The incremental checkpoint becomes the pair (position_txid, position = seq).
    await q.query(`ALTER TABLE pipeline_checkpoints ADD COLUMN position_txid BIGINT NOT NULL DEFAULT 0`);
  }

  async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE pipeline_checkpoints DROP COLUMN position_txid`);
    await q.query(`DROP INDEX customer_changes_txid_seq`);
    await q.query(`ALTER TABLE customer_changes DROP COLUMN txid`);
  }
}
