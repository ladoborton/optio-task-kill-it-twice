import { Injectable } from '@nestjs/common';
import { log } from '../../../shared/logger';
import { upsert } from '../changes/change';
import { CheckpointRepository } from '../checkpoint/checkpoint.repository';
import { ControlRepository } from '../control/control.repository';
import { EsSink } from '../es/es.sink';
import { CustomerSource } from '../source/customer-source';
import { StepLoop } from '../step-loop';
import { StreamSink } from '../stream/stream.sink';

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
    private readonly es: EsSink,
    private readonly streamSink: StreamSink,
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
    const changes = rows.map(upsert);

    const result = await this.es.write(changes);
    const published = await this.streamSink.publish(changes, this.stream);

    // SPEC §6.2: the checkpoint moves only after BOTH sinks acknowledged every record of the batch.
    // A crash before this line means the batch is written again after restart — never skipped.
    await this.checkpoints.advance(this.stream, from, to);

    log('backfill.batch', {
      stream: this.stream,
      batch_id: batchId,
      from: Number(from),
      to: Number(to),
      count: rows.length,
      written: result.written,
      conflicts: result.conflicts,
      published,
      ms: Date.now() - startedAt,
    });
    return 0;
  }
}
