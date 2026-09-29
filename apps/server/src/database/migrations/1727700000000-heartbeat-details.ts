import { MigrationInterface, QueryRunner } from 'typeorm';

// SPEC §4.7 / §8.2: the heartbeat also carries the breaker state of each sink, so the api can
// report "elasticsearch circuit open" from Postgres alone — even while the pipeline is unreachable.
export class HeartbeatDetails1727700000000 implements MigrationInterface {
  name = 'HeartbeatDetails1727700000000';

  async up(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE pipeline_heartbeat ADD COLUMN details JSONB NOT NULL DEFAULT '{}'::jsonb`);
  }

  async down(q: QueryRunner): Promise<void> {
    await q.query(`ALTER TABLE pipeline_heartbeat DROP COLUMN details`);
  }
}
