import { Column, Entity, PrimaryGeneratedColumn } from 'typeorm';

export type ChangeOp = 'upsert' | 'delete';

// Outbox row written by the customers_to_outbox trigger (SPEC §4.2). Keys only, no payload.
@Entity('customer_changes')
export class CustomerChange {
  @PrimaryGeneratedColumn({ type: 'bigint' })
  seq: string;

  @Column('bigint', { name: 'customer_id' })
  customerId: string;

  @Column('text')
  op: ChangeOp;

  @Column('bigint')
  version: string;

  @Column('timestamptz', { name: 'created_at' })
  createdAt: Date;
}
