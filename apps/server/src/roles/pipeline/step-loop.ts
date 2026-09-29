import { BeforeApplicationShutdown, OnApplicationBootstrap } from '@nestjs/common';
import { StreamName } from '../../database/entities/pipeline-checkpoint.entity';
import { backoffDelay } from '../../shared/backoff';
import { config } from '../../shared/config';
import { log } from '../../shared/logger';
import { sleep } from '../../shared/sleep';
import { CheckpointMovedError } from './checkpoint/checkpoint.repository';

/**
 * Shared skeleton of the backfill and incremental loops: run step() forever, idle when told to,
 * retry failures with backoff, stop cleanly on SIGTERM.
 *
 * Subclasses keep NO progress in memory between steps — every step starts by reading its
 * checkpoint — so a SIGKILL at any line loses at most the batch in flight, which is then read and
 * written again on restart (SPEC §6.1: at-least-once, idempotent sinks).
 */
export abstract class StepLoop implements OnApplicationBootstrap, BeforeApplicationShutdown {
  /** Log prefix and `stream` field: backfill | incremental | dlq_replay. */
  protected abstract readonly stream: StreamName | 'dlq_replay';
  private readonly stopping = new AbortController();
  private loop?: Promise<void>;

  /** One batch. Returns how long to idle before the next step (0 = continue immediately). */
  protected abstract step(): Promise<number>;

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
          log(`${this.stream}.checkpoint_moved`, { stream: this.stream, expected: e.expected }, 'warn');
          continue;
        }
        // SPEC §7.2: no busy loop — every failure waits, and the wait grows while failures continue.
        failures++;
        const retryInMs = backoffDelay(failures, config.retryBaseMs, config.retryMaxMs);
        log(`${this.stream}.error`, { stream: this.stream, attempt: failures, retry_in_ms: retryInMs, error: (e as Error).message }, 'warn');
        await sleep(retryInMs, signal);
      }
    }
    log(`${this.stream}.stopped`, { stream: this.stream });
  }
}
