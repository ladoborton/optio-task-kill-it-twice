import { Column, Entity, PrimaryColumn } from 'typeorm';

// SPEC §4.7. Lets the api report "pipeline is dead" and last known throughput while it is down.
@Entity('pipeline_heartbeat')
export class PipelineHeartbeat {
  @PrimaryColumn('text', { name: 'instance_id' })
  instanceId: string;

  @Column('timestamptz', { name: 'started_at' })
  startedAt: Date;

  @Column('timestamptz', { name: 'last_beat_at' })
  lastBeatAt: Date;

  @Column('double precision', { name: 'backfill_rate' })
  backfillRate: number;

  @Column('double precision', { name: 'incremental_rate' })
  incrementalRate: number;

  /** { circuits: { elasticsearch: 'closed' | 'open' | 'half_open', rabbitmq: … } } */
  @Column('jsonb')
  details: Record<string, unknown>;
}
