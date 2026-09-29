import { BadRequestException, Body, Controller, Get, HttpCode, Param, Post } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { DockerClient } from './docker.client';

const bounded = (value: unknown, max: number, fallback: number): number => {
  const n = value === undefined ? fallback : Number(value);
  if (!Number.isInteger(n) || n < 0 || n > max) throw new BadRequestException(`expected an integer in [0, ${max}]`);
  return n;
};

/** SPEC §9 simulation: cause the failures the gates test, by hand, from the UI. */
@Controller('api/sim')
export class SimController {
  constructor(
    private readonly db: DataSource,
    private readonly docker: DockerClient,
  ) {}

  @Get('services')
  services(): Promise<Record<string, string>> {
    return this.docker.states();
  }

  /** stop = graceful (SIGTERM), kill = crash (SIGKILL, the G1 scenario), start. */
  @Post('services/:service/:action')
  @HttpCode(204)
  async service(@Param('service') service: string, @Param('action') action: string): Promise<void> {
    await this.docker.act(service, action);
  }

  /**
   * Customers the index mapping will reject (SPEC §4.5): valid JSONB for Postgres, but
   * attributes.age is not an integer — or an unknown attribute under dynamic: strict.
   */
  @Post('bad-records')
  async badRecords(@Body() body: { count?: unknown }): Promise<{ ids: string[] }> {
    const count = bounded(body.count, 100, 3);
    const rows: { id: string }[] = await this.db.query(
      `INSERT INTO customers (email, name, city, segment, attributes)
       SELECT 'bad-' || n || '-' || floor(extract(epoch FROM now())) || '@example.com', 'Bad record ' || n, 'Gori', 'new',
              CASE WHEN n % 2 = 0 THEN '{"age": "not-a-number"}'::jsonb ELSE '{"favourite_colour": "green"}'::jsonb END
         FROM generate_series(1, $1) AS n
       RETURNING id`,
      [count],
    );
    return { ids: rows.map((r) => r.id) };
  }

  /** A burst of ordinary source changes: updates spread over the table, inserts and deletes. */
  @Post('changes')
  async changes(@Body() body: { updates?: unknown; inserts?: unknown; deletes?: unknown }) {
    const updates = bounded(body.updates, 100_000, 1_000);
    const inserts = bounded(body.inserts, 10_000, 50);
    const deletes = bounded(body.deletes, 10_000, 20);
    // Random existing ids without scanning the table: sample the id range, keep the ones that exist.
    // A constant SQL fragment (no values in it); the count is still a bound parameter.
    const randomIds = `SELECT DISTINCT (1 + floor(random() * (SELECT max(id) FROM customers)))::bigint AS id FROM generate_series(1, $1)`;
    const [, updated] = await this.db.query(`UPDATE customers SET balance = balance + 1 WHERE id IN (${randomIds})`, [updates]);
    await this.db.query(
      `INSERT INTO customers (email, name, city, segment)
       SELECT 'sim-' || n || '-' || floor(extract(epoch FROM now())) || '@example.com', 'Simulated ' || n, 'Rustavi', 'new'
         FROM generate_series(1, $1) AS n`,
      [inserts],
    );
    const [, deleted] = await this.db.query(`DELETE FROM customers WHERE id IN (${randomIds})`, [deletes]);
    return { updated, inserted: inserts, deleted };
  }
}
