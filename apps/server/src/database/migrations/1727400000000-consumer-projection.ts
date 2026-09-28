import { MigrationInterface, QueryRunner } from 'typeorm';

// SPEC §6.3: the independent consumer's own storage, in a separate schema so it never touches the
// source tables. Accessed only through raw SQL by the consumer role, hence no TypeORM entities.
export class ConsumerProjection1727400000000 implements MigrationInterface {
  name = 'ConsumerProjection1727400000000';

  async up(q: QueryRunner): Promise<void> {
    await q.query(`CREATE SCHEMA consumer`);

    // The consumer's view of every customer: latest version it has applied.
    await q.query(`
      CREATE TABLE consumer.customers (
        id          BIGINT      PRIMARY KEY,
        version     BIGINT      NOT NULL,
        deleted     BOOLEAN     NOT NULL DEFAULT false,
        email       TEXT,
        city        TEXT,
        segment     TEXT,
        source      TEXT        NOT NULL,
        applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
      )`);

    // Dedup record: one row per (customer, version) ever applied. The primary key is what turns
    // a redelivered message into a no-op.
    await q.query(`
      CREATE TABLE consumer.applied_events (
        customer_id  BIGINT      NOT NULL,
        version      BIGINT      NOT NULL,
        applied_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (customer_id, version)
      )`);

    // Durable counters (in-memory ones would reset on every kill): delivered = applied + duplicates.
    await q.query(`
      CREATE TABLE consumer.stats (
        id          SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        applied     BIGINT   NOT NULL DEFAULT 0,
        duplicates  BIGINT   NOT NULL DEFAULT 0
      )`);
    await q.query(`INSERT INTO consumer.stats DEFAULT VALUES`);
  }

  async down(q: QueryRunner): Promise<void> {
    await q.query(`DROP SCHEMA consumer CASCADE`);
  }
}
