import { Injectable } from '@nestjs/common';
import { log } from '../../../shared/logger';
import { planChanges } from '../changes/change';
import { CheckpointRepository } from '../checkpoint/checkpoint.repository';
import { ControlRepository } from '../control/control.repository';
import { EsSink } from '../es/es.sink';
import { CustomerSource } from '../source/customer-source';
import { OutboxReader } from '../source/outbox-reader';
import { StepLoop } from '../step-loop';
import { StreamSink } from '../stream/stream.sink';

/**
 * SPEC §5.4: follows the outbox and ships every committed change (upserts and deletes) to both
 * sinks. Runs alongside the backfill; the two never coordinate — the sinks' version checks decide
 * which write wins (SPEC §5.5).
 */
@Injectable()
export class IncrementalLoop extends StepLoop {
  protected readonly stream = 'incremental' as const;
  private paused = false;
  private announced = false;

  constructor(
    private readonly control: ControlRepository,
    private readonly checkpoints: CheckpointRepository,
    private readonly outbox: OutboxReader,
    private readonly source: CustomerSource,
    private readonly es: EsSink,
    private readonly streamSink: StreamSink,
  ) {
    super();
  }

  protected async step(): Promise<number> {
    const control = await this.control.get();
    const checkpoint = await this.checkpoints.get(this.stream);
    const from = { txid: checkpoint.positionTxid, seq: checkpoint.position };

    if (!this.announced) {
      log('incremental.started', { stream: this.stream, from_txid: Number(from.txid), from_seq: Number(from.seq) });
      this.announced = true;
    }

    if (control.incrementalState === 'paused') {
      if (!this.paused) log('incremental.paused', { stream: this.stream });
      this.paused = true;
      return control.pollIntervalMs;
    }
    if (this.paused) log('incremental.resumed', { stream: this.stream });
    this.paused = false;

    const entries = await this.outbox.readCommitted(from, control.batchSize);
    if (entries.length === 0) return control.pollIntervalMs;

    const startedAt = Date.now();
    const ids = [...new Set(entries.map((e) => e.customer_id))];
    const changes = planChanges(entries, await this.source.readByIds(ids));
    const last = entries[entries.length - 1];
    const to = { txid: last.txid, seq: last.seq };

    let written = 0, absent = 0, conflicts = 0, published = 0;
    if (changes.length > 0) {
      const result = await this.es.write(changes);
      ({ written, absent, conflicts } = result);
      published = await this.streamSink.publish(changes, this.stream);
    }

    // SPEC §6.2: only after both sinks acknowledged. The position covers every entry read, including
    // ones that collapsed into another entry's change or were skipped (delete shipped later).
    await this.checkpoints.advanceIncremental(from, to);

    log('incremental.batch', {
      stream: this.stream,
      batch_id: `${this.stream}:${from.txid}.${from.seq}-${to.txid}.${to.seq}`,
      entries: entries.length,
      upserts: changes.filter((c) => c.op === 'upsert').length,
      deletes: changes.filter((c) => c.op === 'delete').length,
      written,
      absent,
      conflicts,
      published,
      ms: Date.now() - startedAt,
    });
    // A full page means more is waiting; a partial page means we have caught up.
    return entries.length < control.batchSize ? control.pollIntervalMs : 0;
  }
}
