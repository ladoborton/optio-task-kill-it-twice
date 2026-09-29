import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';

/** A customers row exactly as node-postgres returns it (BIGINT and NUMERIC arrive as strings). */
export interface CustomerRow {
  id: string;
  email: string;
  name: string;
  city: string;
  segment: string;
  balance: string;
  attributes: Record<string, unknown>;
  version: string;
  updated_at: Date;
}

@Injectable()
export class CustomerSource {
  constructor(private readonly db: DataSource) {}

  // SPEC §5.3: keyset pagination. `WHERE id > $1` walks the primary-key index, so page 2,000
  // costs the same as page 1 (OFFSET would re-scan every skipped row) and only one page is
  // ever in memory. Raw rows, no entity hydration (SPEC §5.6 rule 2).
  readPage(afterId: string, limit: number): Promise<CustomerRow[]> {
    return this.db.query(
      `SELECT id, email, name, city, segment, balance, attributes, version, updated_at
         FROM customers
        WHERE id > $1
        ORDER BY id
        LIMIT $2`,
      [afterId, limit],
    );
  }

  /** Current rows for the given ids; ids of deleted customers are simply absent from the result. */
  readByIds(ids: string[]): Promise<CustomerRow[]> {
    return this.db.query(
      `SELECT id, email, name, city, segment, balance, attributes, version, updated_at
         FROM customers
        WHERE id = ANY($1::bigint[])`,
      [ids],
    );
  }
}
