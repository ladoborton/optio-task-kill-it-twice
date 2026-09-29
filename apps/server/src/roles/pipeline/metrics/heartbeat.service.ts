import { BeforeApplicationShutdown, Inject, Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { hostname } from 'node:os';
import { DataSource } from 'typeorm';
import { CircuitBreaker, CircuitState } from '../../../shared/circuit-breaker';
import { config } from '../../../shared/config';
import { log } from '../../../shared/logger';
import { readPipelineFacts } from '../../../shared/pipeline-facts';
import { ES_BREAKER, STREAM_BREAKER } from '../breakers';
import { PipelineMetrics } from './pipeline-metrics';

const CIRCUIT_VALUE: Record<CircuitState, number> = { closed: 0, half_open: 1, open: 2 };

/**
 * SPEC §4.7: every HEARTBEAT_MS the pipeline refreshes its gauges from Postgres and writes
 * "I am alive" + throughput + breaker states to pipeline_heartbeat. The api judges liveness by the
 * age of that row, so a dead (or hung) pipeline shows up as "down" without anyone reaching it.
 * Runs on its own timer, independent of the loops: a loop stuck on a sink does not stop the beat.
 */
@Injectable()
export class HeartbeatService implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly instanceId = `${hostname()}-${process.pid}`;
  private readonly startedAt = new Date();
  private timer?: NodeJS.Timeout;
  private beating = false;

  constructor(
    private readonly db: DataSource,
    private readonly metrics: PipelineMetrics,
    @Inject(ES_BREAKER) private readonly esBreaker: CircuitBreaker,
    @Inject(STREAM_BREAKER) private readonly streamBreaker: CircuitBreaker,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    // Rows of long-gone instances (every restart is a new container/pid) are only noise.
    await this.db.query(`DELETE FROM pipeline_heartbeat WHERE last_beat_at < now() - interval '1 hour'`).catch(() => undefined);
    this.timer = setInterval(() => void this.beat(), config.heartbeatMs);
    void this.beat();
  }

  beforeApplicationShutdown(): void {
    clearInterval(this.timer);
  }

  private async beat(): Promise<void> {
    if (this.beating) return; // never overlap when Postgres is slow
    this.beating = true;
    try {
      const facts = await readPipelineFacts(this.db);
      this.metrics.checkpoint.labels('backfill').set(facts.backfillPosition);
      this.metrics.checkpoint.labels('incremental').set(facts.incrementalSeq);
      this.metrics.backfillTotal.set(facts.backfillTotal);
      this.metrics.lagEvents.set(facts.lagEvents);
      this.metrics.lagSeconds.set(facts.lagSeconds);
      this.metrics.dlqPending.set(facts.dlqPending);

      const circuits = { elasticsearch: this.esBreaker.current, rabbitmq: this.streamBreaker.current };
      for (const [sink, state] of Object.entries(circuits)) this.metrics.circuit.labels(sink).set(CIRCUIT_VALUE[state]);

      await this.db.query(
        `INSERT INTO pipeline_heartbeat (instance_id, started_at, last_beat_at, backfill_rate, incremental_rate, details)
         VALUES ($1, $2, now(), $3, $4, $5::jsonb)
         ON CONFLICT (instance_id) DO UPDATE
            SET last_beat_at = now(), backfill_rate = EXCLUDED.backfill_rate,
                incremental_rate = EXCLUDED.incremental_rate, details = EXCLUDED.details`,
        [
          this.instanceId,
          this.startedAt,
          this.metrics.rate('backfill'),
          this.metrics.rate('incremental'),
          JSON.stringify({ circuits }),
        ],
      );
    } catch (e) {
      log('heartbeat.failed', { error: (e as Error).message }, 'warn');
    } finally {
      this.beating = false;
    }
  }
}
