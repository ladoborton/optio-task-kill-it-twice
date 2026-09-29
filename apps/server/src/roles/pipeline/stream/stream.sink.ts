import { Inject, Injectable, OnApplicationShutdown } from '@nestjs/common';
import { connect, ConfirmChannel } from 'amqplib';
import { CircuitBreaker } from '../../../shared/circuit-breaker';
import { config } from '../../../shared/config';
import { log } from '../../../shared/logger';
import { CustomerEvent } from '../../../shared/rabbitmq/customer-event';
import { assertTopology, EXCHANGE, routingKey } from '../../../shared/rabbitmq/topology';
import { STREAM_BREAKER } from '../breakers';
import { Change } from '../changes/change';
import { toDocument } from '../es/customer-index';

type Connection = Awaited<ReturnType<typeof connect>>;

/**
 * Publishes a batch to the `customers` exchange and waits for the broker's publisher confirms.
 * Resolves only when RabbitMQ has taken responsibility for every message (persisted to the
 * quorum queue); otherwise throws, so the caller does not checkpoint (SPEC §6.2).
 */
@Injectable()
export class StreamSink implements OnApplicationShutdown {
  private connection?: Connection;
  private channel?: ConfirmChannel;

  constructor(@Inject(STREAM_BREAKER) private readonly breaker: CircuitBreaker) {}

  /** publish() behind the broker's circuit breaker: while it is open, fails fast without connecting. */
  publish(changes: Change[], source: CustomerEvent['source']): Promise<number> {
    return this.breaker.run(() => this.publishConfirmed(changes, source));
  }

  private async publishConfirmed(changes: Change[], source: CustomerEvent['source']): Promise<number> {
    const ch = await this.confirmChannel();
    const emittedAt = new Date().toISOString();
    for (const change of changes) {
      const event: CustomerEvent = {
        customer_id: Number(change.id),
        version: Number(change.version),
        op: change.op,
        source,
        data: change.op === 'upsert' ? toDocument(change.row) : null,
        emitted_at: emittedAt,
      };
      ch.publish(EXCHANGE, routingKey(change.op), Buffer.from(JSON.stringify(event)), {
        persistent: true, // written to disk, survives a broker restart
        contentType: 'application/json',
        // Same id on every replay of this (customer, version): consumers can dedup on it.
        messageId: `${event.customer_id}:${event.version}`,
      });
    }
    try {
      await withTimeout(ch.waitForConfirms(), config.streamConfirmTimeoutMs, 'publisher confirms');
    } catch (e) {
      // Unknown state: some messages may be stored, some not. Drop the channel; the whole batch is
      // published again on retry and the consumer absorbs the duplicates.
      this.reset();
      throw e;
    }
    return changes.length;
  }

  async onApplicationShutdown(): Promise<void> {
    await this.connection?.close().catch(() => undefined);
  }

  // Lazily (re)connects: after a broker restart the next publish simply opens a new connection.
  private async confirmChannel(): Promise<ConfirmChannel> {
    if (this.channel) return this.channel;
    const connection = await connect(config.rabbitmqUrl);
    // Without an 'error' listener amqplib's EventEmitter would crash the process on a broker restart.
    connection.on('error', (e: Error) => log('stream.connection_error', { error: e.message }, 'warn'));
    // Only react if this is still the current connection: a late 'close' from an old, already
    // replaced connection must not tear down the new one.
    connection.on('close', () => this.connection === connection && this.reset());
    const channel = await connection.createConfirmChannel();
    channel.on('error', (e: Error) => log('stream.channel_error', { error: e.message }, 'warn'));
    channel.on('close', () => this.channel === channel && this.reset());
    await assertTopology(channel);
    this.connection = connection;
    this.channel = channel;
    log('stream.connected', {});
    return channel;
  }

  private reset(): void {
    const connection = this.connection;
    this.connection = undefined;
    this.channel = undefined;
    connection?.close().catch(() => undefined);
  }
}

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}
