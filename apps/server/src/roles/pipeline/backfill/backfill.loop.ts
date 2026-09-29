import { Injectable } from '@nestjs/common';
import { log } from '../../../shared/logger';
import { upsert } from '../changes/change';
import { CheckpointRepository } from '../checkpoint/checkpoint.repository';
import { ControlRepository } from '../control/control.repository';
import { BatchDelivery } from '../delivery/batch-delivery';
import { PipelineMetrics } from '../metrics/pipeline-metrics';
import { CustomerSource } from '../source/customer-source';
import { StepLoop } from '../step-loop';

/** SPEC §5.3: copies every existing customer into both sinks, one keyset page at a time. */
@Injectable()
export class BackfillLoop extends StepLoop {
  protected readonly stream = 'backfill' as const;
  private paused = false;
  private announced = false;

  constructor(
    private readonly control: ControlRepository,
    private readonly checkpoints: CheckpointRepository,
    private readonly source: CustomerSource,
    private readonly delivery: BatchDelivery,
    private readonly metrics: PipelineMetrics,
  ) {
    super();
  }

  protected async step(): Promise<number> {
    const control = await this.control.get();
    const checkpoint = await this.checkpoints.get(this.stream);

    if (!this.announced) {
      // G1 evidence: where this process resumes, read straight from the durable checkpoint.
      log('backfill.started', { stream: this.stream, from: Number(checkpoint.position), completed: checkpoint.completedAt !== null });
      this.announced = true;
    }

    if (control.backfillState === 'paused') {
      if (!this.paused) log('backfill.paused', { stream: this.stream, at: Number(checkpoint.position) });
      this.paused = true;
      return control.pollIntervalMs;
    }
    if (this.paused) log('backfill.resumed', { stream: this.stream, from: Number(checkpoint.position) });
    this.paused = false;

    // Done: idle, but keep watching — a reset (completed_at = NULL) starts a new backfill.
    if (checkpoint.completedAt) return control.pollIntervalMs;

    const rows = await this.source.readPage(checkpoint.position, control.batchSize);
    if (rows.length === 0) {
      await this.checkpoints.complete(this.stream, checkpoint.position);
      log('backfill.completed', { stream: this.stream, position: Number(checkpoint.position) });
      return 0;
    }

    const from = checkpoint.position;
    const to = rows[rows.length - 1].id;
    // Deterministic: a batch replayed after a crash gets the same id, so logs/DLQ entries line up.
    const batchId = `${this.stream}:${from}-${to}`;
    const startedAt = Date.now();
    const result = await this.delivery.deliver(rows.map(upsert), this.stream, batchId);

    // SPEC §6.2: the checkpoint moves only after every record of the batch is acknowledged by both
    // sinks or parked in the DLQ. A crash before this line means the batch is written again after
    // restart — never skipped.
    await this.checkpoints.advance(this.stream, from, to);

    const ms = Date.now() - startedAt;
    this.metrics.recordBatch(this.stream, rows.length, ms, result);
    log('backfill.batch', { stream: this.stream, batch_id: batchId, from: Number(from), to: Number(to), count: rows.length, ...result, ms });
    return 0;
  }
}
