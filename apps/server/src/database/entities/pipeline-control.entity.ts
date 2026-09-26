import { Column, Entity, PrimaryColumn } from 'typeorm';

export type LoopState = 'running' | 'paused';

// SPEC §4.6. Single row (id = 1): the desired state, written by the api, read by the pipeline.
@Entity('pipeline_control')
export class PipelineControl {
  @PrimaryColumn('smallint')
  id: number;

  @Column('text', { name: 'backfill_state' })
  backfillState: LoopState;

  @Column('text', { name: 'incremental_state' })
  incrementalState: LoopState;

  @Column('int', { name: 'batch_size' })
  batchSize: number;

  @Column('int', { name: 'poll_interval_ms' })
  pollIntervalMs: number;

  @Column('timestamptz', { name: 'updated_at' })
  updatedAt: Date;
}
