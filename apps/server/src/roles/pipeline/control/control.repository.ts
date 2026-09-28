import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { LoopState } from '../../../database/entities/pipeline-control.entity';

export interface Control {
  backfillState: LoopState;
  incrementalState: LoopState;
  batchSize: number;
  pollIntervalMs: number;
}

// SPEC §4.6: desired state lives in Postgres and is re-read every step, so a pause set through
// the api survives a pipeline crash and takes effect within one batch.
@Injectable()
export class ControlRepository {
  constructor(private readonly db: DataSource) {}

  async get(): Promise<Control> {
    const [row] = await this.db.query(
      `SELECT backfill_state, incremental_state, batch_size, poll_interval_ms FROM pipeline_control WHERE id = 1`,
    );
    return {
      backfillState: row.backfill_state,
      incrementalState: row.incremental_state,
      batchSize: row.batch_size,
      pollIntervalMs: row.poll_interval_ms,
    };
  }
}
