import { BeforeApplicationShutdown, Injectable, OnApplicationBootstrap } from '@nestjs/common';
import { Channel, connect, ConsumeMessage } from 'amqplib';
import { backoffDelay } from '../../shared/backoff';
import { config } from '../../shared/config';
import { log } from '../../shared/logger';
import { parseCustomerEvent } from '../../shared/rabbitmq/customer-event';
import { assertTopology, QUEUE } from '../../shared/rabbitmq/topology';
import { sleep } from '../../shared/sleep';
import { ProjectionRepository } from './projection.repository';

/**
 * The independent consumer (SPEC §5.1, §6.3): reads the `consumer.customers` queue and keeps its
 * own projection of every customer. Manual acks: a message is acknowledged only after the
 * transaction that applied it has committed, so a kill at any point means redelivery, not loss.
 */
@Injectable()
export class CustomerEventsConsumer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly stopping = new AbortController();
  private loop?: Promise<void>;

  constructor(private readonly projection: ProjectionRepository) {}

  onApplicationBootstrap(): void {
    this.loop = this.run();
  }

  async beforeApplicationShutdown(): Promise<void> {
    this.stopping.abort();
    await this.loop;
  }

  private async run(): Promise<void> {
    let failures = 0;
    while (!this.stopping.signal.aborted) {
      try {
        await this.session();
        failures = 0;
      } catch (e) {
        // Broker down or restarted: reconnect with growing delays (SPEC §7.2).
        failures++;
        const retryInMs = backoffDelay(failures, config.retryBaseMs, config.retryMaxMs);
        log('consumer.error', { attempt: failures, retry_in_ms: retryInMs, error: (e as Error).message }, 'warn');
        await sleep(retryInMs, this.stopping.signal);
      }
    }
    log('consumer.stopped', {});
  }

  /** One connection's lifetime: consume until the connection is lost (throws) or we are stopping (returns). */
  private async session(): Promise<void> {
    const connection = await connect(config.rabbitmqUrl);
    const lost = new Promise<Error>((resolve) => {
      connection.on('error', (e: Error) => resolve(e));
      connection.on('close', () => resolve(new Error('connection closed')));
    });
    try {
      const channel = await connection.createChannel();
      channel.on('error', (e: Error) => log('consumer.channel_error', { error: e.message }, 'warn'));
      await assertTopology(channel);
      // At most this many unacked messages are in flight: bounds memory and redelivery after a kill.
      await channel.prefetch(config.consumerPrefetch);

      const batcher = new Batcher(channel, this.projection, this.stopping.signal);
      const { consumerTag } = await channel.consume(QUEUE, (msg) => msg && batcher.add(msg), { noAck: false });
      log('consumer.started', { queue: QUEUE, prefetch: config.consumerPrefetch, batch_size: config.consumerBatchSize });

      const stopped = new Promise<void>((resolve) => {
        if (this.stopping.signal.aborted) resolve();
        this.stopping.signal.addEventListener('abort', () => resolve(), { once: true });
      });
      const lostWith = await Promise.race([lost, stopped.then(() => undefined)]);
      if (lostWith) throw lostWith;

      // Graceful stop: take no new deliveries, finish and ack what we hold.
      await channel.cancel(consumerTag).catch(() => undefined);
      await batcher.drain();
    } finally {
      await connection.close().catch(() => undefined);
    }
  }
}

/**
 * Groups deliveries into batches (size or time bound) and applies them one after another.
 * Batches are formed and processed strictly in delivery order — that is what makes the
 * `ack(last, multiple=true)` below acknowledge exactly one batch.
 */
class Batcher {
  private buffer: ConsumeMessage[] = [];
  private timer?: NodeJS.Timeout;
  private chain: Promise<void> = Promise.resolve();

  constructor(
    private readonly channel: Channel,
    private readonly projection: ProjectionRepository,
    private readonly stopping: AbortSignal,
  ) {}

  add(msg: ConsumeMessage): void {
    this.buffer.push(msg);
    if (this.buffer.length >= config.consumerBatchSize) this.flush();
    else this.timer ??= setTimeout(() => this.flush(), config.consumerFlushMs);
  }

  async drain(): Promise<void> {
    this.flush();
    await this.chain;
  }

  private flush(): void {
    clearTimeout(this.timer);
    this.timer = undefined;
    const batch = this.buffer;
    this.buffer = [];
    if (batch.length > 0) this.chain = this.chain.then(() => this.process(batch));
  }

  private async process(batch: ConsumeMessage[]): Promise<void> {
    const started = Date.now();
    const valid: ConsumeMessage[] = [];
    const events = [];
    for (const msg of batch) {
      try {
        events.push(parseCustomerEvent(msg.content));
        valid.push(msg);
      } catch (e) {
        // SPEC §7.3: a malformed message can never succeed — dead-letter it now (requeue = false)
        // instead of letting it bounce until the delivery limit.
        this.safely(() => this.channel.nack(msg, false, false));
        log('consumer.dead_lettered', { message_id: msg.properties.messageId, error: (e as Error).message }, 'warn');
      }
    }
    if (events.length === 0) return;

    // Postgres trouble is transient: hold the messages (unacked, so no redelivery count is burned)
    // and retry with backoff. On shutdown give up without acking — they are redelivered later.
    for (let failures = 0; ; ) {
      try {
        const r = await this.projection.apply(events);
        log('consumer.batch', { count: events.length, applied: r.applied, duplicates: r.duplicates, ms: Date.now() - started });
        break;
      } catch (e) {
        if (this.stopping.aborted) return;
        failures++;
        const retryInMs = backoffDelay(failures, config.retryBaseMs, config.retryMaxMs);
        log('consumer.apply_failed', { attempt: failures, retry_in_ms: retryInMs, error: (e as Error).message }, 'warn');
        await sleep(retryInMs, this.stopping);
      }
    }

    // Only now, after COMMIT. A kill between COMMIT and this ack redelivers the batch, and the
    // applied_events primary key turns every redelivered event into a counted duplicate.
    this.safely(() => this.channel.ack(valid[valid.length - 1], true));
  }

  // The channel may already be gone (broker restart); the messages will then be redelivered.
  private safely(fn: () => void): void {
    try {
      fn();
    } catch (e) {
      log('consumer.ack_failed', { error: (e as Error).message }, 'warn');
    }
  }
}
