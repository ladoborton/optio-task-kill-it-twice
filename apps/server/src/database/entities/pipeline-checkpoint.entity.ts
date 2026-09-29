import { Column, Entity, PrimaryColumn } from 'typeorm';

export type StreamName = 'backfill' | 'incremental';

// SPEC §4.3. `position` is the last fully-handled customers.id (backfill) or
// customer_changes.seq (incremental).
@Entity('pipeline_checkpoints')
export class PipelineCheckpoint {
  @PrimaryColumn('text')
  stream: StreamName;

  @Column('bigint')
  position: string;

  /** Incremental only: txid half of the (txid, seq) position; 0 for backfill. */
  @Column('bigint', { name: 'position_txid' })
  positionTxid: string;

  @Column('timestamptz', { name: 'completed_at', nullable: true })
  completedAt: Date | null;

  @Column('timestamptz', { name: 'updated_at' })
  updatedAt: Date;
}
