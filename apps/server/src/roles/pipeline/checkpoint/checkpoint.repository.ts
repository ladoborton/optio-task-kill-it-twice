import { Injectable } from '@nestjs/common';
import { DataSource } from 'typeorm';
import { PipelineCheckpoint, StreamName } from '../../../database/entities/pipeline-checkpoint.entity';
import { OutboxPosition } from '../source/outbox-reader';

export interface Checkpoint {
  position: string;
  /** Incremental only (see OutboxPosition); '0' for backfill. */
  positionTxid: string;
  completedAt: Date | null;
}

/** The checkpoint changed under us (seed/reset, or a second pipeline instance). */
export class CheckpointMovedError extends Error {
  constructor(readonly stream: StreamName, readonly expected: string) {
    super(`${stream} checkpoint is no longer at ${expected}`);
  }
}

@Injectable()
export class CheckpointRepository {
  constructor(private readonly db: DataSource) {}

  async get(stream: StreamName): Promise<Checkpoint> {
    const [row] = await this.db.query(
      `SELECT position, position_txid, completed_at FROM pipeline_checkpoints WHERE stream = $1`,
      [stream],
    );
    return { position: row.position, positionTxid: row.position_txid, completedAt: row.completed_at };
  }

  /**
   * SPEC §6.2: called only after the sinks acknowledged the batch.
   * Compare-and-set (D-001): the update applies only if the checkpoint is still where this batch
   * started. If someone reset it meanwhile, a blind write would put the old position back and
   * silently skip everything before it.
   */
  async advance(stream: StreamName, from: string, to: string): Promise<void> {
    const res = await this.db
      .createQueryBuilder()
      .update(PipelineCheckpoint)
      .set({ position: to, updatedAt: () => 'now()' })
      .where('stream = :stream AND position = :from AND completed_at IS NULL', { stream, from })
      .execute();
    if (res.affected !== 1) throw new CheckpointMovedError(stream, from);
  }

  /** Same contract as advance(), for the (txid, seq) position of the incremental stream. */
  async advanceIncremental(from: OutboxPosition, to: OutboxPosition): Promise<void> {
    const res = await this.db
      .createQueryBuilder()
      .update(PipelineCheckpoint)
      .set({ positionTxid: to.txid, position: to.seq, updatedAt: () => 'now()' })
      .where(`stream = 'incremental' AND position_txid = :txid AND position = :seq`, { txid: from.txid, seq: from.seq })
      .execute();
    if (res.affected !== 1) throw new CheckpointMovedError('incremental', `${from.txid}/${from.seq}`);
  }

  async complete(stream: StreamName, at: string): Promise<void> {
    const res = await this.db
      .createQueryBuilder()
      .update(PipelineCheckpoint)
      .set({ completedAt: () => 'now()', updatedAt: () => 'now()' })
      .where('stream = :stream AND position = :at AND completed_at IS NULL', { stream, at })
      .execute();
    if (res.affected !== 1) throw new CheckpointMovedError(stream, at);
  }
}
