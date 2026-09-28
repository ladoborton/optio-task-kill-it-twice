import { BeforeApplicationShutdown, Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { backoffDelay } from '../../../shared/backoff';
import { config } from '../../../shared/config';
import { log } from '../../../shared/logger';
import { sleep } from '../../../shared/sleep';
import { CheckpointMovedError, CheckpointRepository } from '../checkpoint/checkpoint.repository';
import { ControlRepository } from '../control/control.repository';
import { CustomerIndex } from '../es/customer-index';
import { EsSink, IndexMissingError } from '../es/es.sink';
import { CustomerSource } from '../source/customer-source';
import { StreamSink } from '../stream/stream.sink';

const STREAM = 'backfill';

/**
 * SPEC §5.3: copies every existing customer into the sinks, one keyset page at a time.
 *
 * All progress lives in pipeline_checkpoints. Nothing about "where we are" is kept in memory
 * between steps, so a SIGKILL at any line loses at most the batch in flight — and that batch
 * is simply read and written again on restart (SPEC §6.1: at-least-once, idempotent sinks).
 */
@Injectable()
export class BackfillLoop implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly stopping = new AbortController();
  private loop?: Promise<void>;
  private indexReady = false;
  private paused = false;
  private announced = false;

  constructor(
    private readonly control: ControlRepository,
    private readonly checkpoints: CheckpointRepository,
    private readonly source: CustomerSource,
    private readonly index: CustomerIndex,
    private readonly es: EsSink,
    private readonly stream: StreamSink,
  ) {}

  onApplicationBootstrap(): void {
    this.loop = this.run();
  }

  // Graceful stop (SIGTERM): finish the current step, then exit. docker kill skips this entirely.
  async beforeApplicationShutdown(): Promise<void> {
    this.stopping.abort();
    await this.loop;
  }

  private async run(): Promise<void> {
    const signal = this.stopping.signal;
    let failures = 0;

    while (!signal.aborted) {
      try {
        const idleMs = await this.step();
        failures = 0;
        if (idleMs > 0) await sleep(idleMs, signal);
      } catch (e) {
        if (e instanceof CheckpointMovedError) {
          // Someone reset the checkpoint (seed, api, verify). Drop the batch; the next step
          // re-reads the checkpoint and starts from the new position.
          log('backfill.checkpoint_moved', { stream: STREAM, expected: e.expected }, 'warn');
          continue;
        }
        if (e instanceof IndexMissingError) this.indexReady = false;

        // SPEC §7.2: no busy loop — every failure waits, and the wait grows while failures continue.
        failures++;
        const retryInMs = backoffDelay(failures, config.retryBaseMs, config.retryMaxMs);
        log('backfill.error', { stream: STREAM, attempt: failures, retry_in_ms: retryInMs, error: (e as Error).message }, 'warn');
        await sleep(retryInMs, signal);
      }
    }
    log('backfill.stopped', { stream: STREAM });
  }

  /** One batch. Returns how long to idle before the next step (0 = continue immediately). */
  private async step(): Promise<number> {
    const control = await this.control.get();
    const checkpoint = await this.checkpoints.get(STREAM);

    if (!this.announced) {
      // G1 evidence: where this process resumes, read straight from the durable checkpoint.
      log('backfill.started', { stream: STREAM, from: Number(checkpoint.position), completed: checkpoint.completedAt !== null });
      this.announced = true;
    }

    if (!this.indexReady) {
      await this.index.ensure();
      this.indexReady = true;
    }

    if (control.backfillState === 'paused') {
      if (!this.paused) log('backfill.paused', { stream: STREAM, at: Number(checkpoint.position) });
      this.paused = true;
      return control.pollIntervalMs;
    }
    if (this.paused) log('backfill.resumed', { stream: STREAM, from: Number(checkpoint.position) });
    this.paused = false;

    // Done: idle, but keep watching — a reset (completed_at = NULL) starts a new backfill.
    if (checkpoint.completedAt) return control.pollIntervalMs;

    const rows = await this.source.readPage(checkpoint.position, control.batchSize);
    if (rows.length === 0) {
      await this.checkpoints.complete(STREAM, checkpoint.position);
      log('backfill.completed', { stream: STREAM, position: Number(checkpoint.position) });
      return 0;
    }

    const from = checkpoint.position;
    const to = rows[rows.length - 1].id;
    // Deterministic: a batch replayed after a crash gets the same id, so logs/DLQ entries line up.
    const batchId = `${STREAM}:${from}-${to}`;
    const startedAt = Date.now();

    const result = await this.es.write(rows);
    const published = await this.stream.publish(rows, STREAM);

    // SPEC §6.2: the checkpoint moves only after BOTH sinks acknowledged every record of the batch.
    // A crash before this line means the batch is written again after restart — never skipped.
    await this.checkpoints.advance(STREAM, from, to);

    log('backfill.batch', {
      stream: STREAM,
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
