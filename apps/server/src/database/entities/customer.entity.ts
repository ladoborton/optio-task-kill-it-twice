import { Column, Entity, PrimaryGeneratedColumn } from 'typeorm';

// Source table (SPEC §4.1). The schema itself is owned by migrations; this class only maps it.
// BIGINT columns are strings in node-postgres (JS numbers lose precision above 2^53).
@Entity('customers')
export class Customer {
  @PrimaryGeneratedColumn('identity', { type: 'bigint', generatedIdentity: 'BY DEFAULT' })
  id: string;

  @Column('text')
  email: string;

  @Column('text')
  name: string;

  @Column('text')
  city: string;

  @Column('text')
  segment: string;

  @Column('numeric', { precision: 12, scale: 2 })
  balance: string;

  @Column('jsonb')
  attributes: Record<string, unknown>;

  // Maintained by the customers_bump_version trigger; never set it from application code.
  @Column('bigint')
  version: string;

  @Column('timestamptz', { name: 'updated_at' })
  updatedAt: Date;
}
