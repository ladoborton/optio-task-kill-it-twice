import { Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { createServer, Server } from 'node:http';
import { collectDefaultMetrics, Counter, Gauge, Histogram, Registry } from 'prom-client';
import { config } from '../../../shared/config';
import { log } from '../../../shared/logger';
import { DeliveryResult } from '../delivery/batch-delivery';

type LoopName = 'backfill' | 'incremental' | 'dlq_replay';

const THROUGHPUT_WINDOW_MS = 10_000;

/**
 * SPEC §8.1: Prometheus metrics of the pipeline, served on :METRICS_PORT/metrics.
 * Counters are per process (they restart at 0 after a crash, as Prometheus expects); the durable
 * answers — positions, lag, DLQ — are gauges refreshed by the heartbeat from Postgres.
 */
@Injectable()
export class PipelineMetrics implements OnModuleInit, OnApplicationShutdown {
  readonly registry = new Registry();
  private server?: Server;
  private readonly recent: Record<LoopName, { at: number; count: number }[]> = { backfill: [], incremental: [], dlq_replay: [] };

  readonly records = new Counter({
    name: 'pipeline_records_total',
    help: 'Records handled, by loop, sink and result (written|absent|conflict|dlq|published)',
    labelNames: ['stream', 'sink', 'result'],
    registers: [this.registry],
  });
  readonly batchDuration = new Histogram({
    name: 'pipeline_batch_duration_seconds',
    help: 'Wall time of one batch: read, both sinks, checkpoint',
    labelNames: ['stream'],
    buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10],
    registers: [this.registry],
  });
  readonly checkpoint = new Gauge({
    name: 'pipeline_checkpoint_position',
    help: 'Durable checkpoint: last customer id (backfill) / last outbox seq (incremental)',
    labelNames: ['stream'],
    registers: [this.registry],
  });
  readonly backfillTotal = new Gauge({ name: 'pipeline_backfill_total_rows', help: 'max(customers.id): where the backfill ends', registers: [this.registry] });
  readonly throughput = new Gauge({
    name: 'pipeline_throughput_records_per_second',
    help: `Records per second over the last ${THROUGHPUT_WINDOW_MS / 1000} s`,
    labelNames: ['stream'],
    registers: [this.registry],
  });
  readonly lagEvents = new Gauge({ name: 'pipeline_incremental_lag_events', help: 'Outbox rows not yet shipped', registers: [this.registry] });
  readonly lagSeconds = new Gauge({ name: 'pipeline_incremental_lag_seconds', help: 'Age of the oldest outbox row not yet shipped', registers: [this.registry] });
  readonly circuit = new Gauge({
    name: 'pipeline_circuit_state',
    help: 'Per-sink circuit breaker: 0 closed, 1 half_open, 2 open',
    labelNames: ['sink'],
    registers: [this.registry],
  });
  readonly dlqPending = new Gauge({ name: 'pipeline_dlq_pending', help: 'DLQ records waiting for a replay', registers: [this.registry] });

  onModuleInit(): void {
    collectDefaultMetrics({ register: this.registry }); // process CPU, memory, event-loop lag
    // Export every known series from the start: a freshly restarted, idle pipeline must show
    // "0 records", not no metric at all (which dashboards and alerts read as "no data").
    for (const stream of ['backfill', 'incremental'] as const) {
      for (const result of ['written', 'absent', 'conflict', 'dlq']) this.records.labels(stream, 'es', result).inc(0);
      this.records.labels(stream, 'stream', 'published').inc(0);
    }
    for (const stream of ['backfill', 'incremental', 'dlq_replay'] as const) {
      this.batchDuration.zero({ stream });
      this.throughput.labels(stream).set(0);
    }
    this.server = createServer(async (req, res) => {
      if (req.url !== '/metrics') {
        res.writeHead(404).end();
        return;
      }
      res.writeHead(200, { 'content-type': this.registry.contentType }).end(await this.registry.metrics());
    }).listen(config.metricsPort);
    log('metrics.listening', { port: config.metricsPort });
  }

  onApplicationShutdown(): void {
    this.server?.close();
  }

  /** Called by a loop after each batch it checkpointed. */
  recordBatch(stream: LoopName, count: number, ms: number, result?: DeliveryResult): void {
    this.batchDuration.labels(stream).observe(ms / 1000);
    if (result) {
      this.records.labels(stream, 'es', 'written').inc(result.written);
      this.records.labels(stream, 'es', 'absent').inc(result.absent);
      this.records.labels(stream, 'es', 'conflict').inc(result.conflicts);
      this.records.labels(stream, 'es', 'dlq').inc(result.dlq);
      this.records.labels(stream, 'stream', 'published').inc(result.published);
    }
    this.recent[stream].push({ at: Date.now(), count });
  }

  /** Records per second over the sliding window (also written to the heartbeat for the api). */
  rate(stream: LoopName): number {
    const since = Date.now() - THROUGHPUT_WINDOW_MS;
    const window = (this.recent[stream] = this.recent[stream].filter((s) => s.at >= since));
    const perSec = window.reduce((sum, s) => sum + s.count, 0) / (THROUGHPUT_WINDOW_MS / 1000);
    this.throughput.labels(stream).set(perSec);
    return perSec;
  }
}
