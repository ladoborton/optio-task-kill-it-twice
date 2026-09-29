import { Column, Entity, PrimaryGeneratedColumn } from 'typeorm';
import { StreamName } from './pipeline-checkpoint.entity';

export type SinkName = 'es' | 'stream';
export type DlqStatus = 'pending' | 'replayed' | 'failed';

// SPEC §4.4. Unique on (sink, customer_id, version): a batch replayed after a crash
// must not insert the same failure twice.
@Entity('dlq_records')
export class DlqRecord {
  @PrimaryGeneratedColumn({ type: 'bigint' })
  id: string;

  @Column('text')
  sink: SinkName;

  @Column('bigint', { name: 'customer_id' })
  customerId: string;

  @Column('bigint')
  version: string;

  @Column('jsonb')
  payload: Record<string, unknown>;

  @Column('text', { name: 'error_type' })
  errorType: string;

  @Column('text', { name: 'error_reason' })
  errorReason: string;

  @Column('text')
  stream: StreamName;

  @Column('text', { name: 'batch_id' })
  batchId: string;

  @Column('int', { name: 'batch_position' })
  batchPosition: number;

  @Column('int')
  attempts: number;

  @Column('text')
  status: DlqStatus;

  @Column('timestamptz', { name: 'created_at' })
  createdAt: Date;

  @Column('timestamptz', { name: 'last_attempt_at' })
  lastAttemptAt: Date;

  /** Set to request a replay; cleared by the pipeline once the attempt is made (SPEC §7.4). */
  @Column('timestamptz', { name: 'replay_requested_at', nullable: true })
  replayRequestedAt: Date | null;
}
