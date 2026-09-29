import { MigrationInterface, QueryRunner } from 'typeorm';

// SPEC §7.4: a replay is requested by setting replay_requested_at (api, UI or verify) and carried
// out by the pipeline's DLQ replay loop — the same "desired state in Postgres" pattern as
// pipeline_control, so a request survives a pipeline crash.
export class DlqReplayRequest1727600000000 implements MigrationInterface {
  name = 'DlqReplayRequest1727600000000';

  async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE dlq_records ADD COLUMN replay_requested_at TIMESTAMPTZ`);
    await q.query(`CREATE INDEX dlq_records_replay_requested ON dlq_records (id) WHERE replay_requested_at IS NOT NULL`);
  }

  async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP INDEX dlq_records_replay_requested`);
    await q.query(`ALTER TABLE dlq_records DROP COLUMN replay_requested_at`);
  }
}
