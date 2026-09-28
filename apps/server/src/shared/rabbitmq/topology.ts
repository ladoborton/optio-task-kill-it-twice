import type { Channel } from 'amqplib';
import { config } from '../config';

export const EXCHANGE = 'customers';
export const QUEUE = 'consumer.customers';
export const DEAD_LETTER_EXCHANGE = 'customers.dlx';
export const DEAD_LETTER_QUEUE = 'consumer.customers.dlq';

/** Routing keys: consumers can bind to `customer.#` or to a single kind of change. */
export const routingKey = (op: 'upsert' | 'delete') => (op === 'upsert' ? 'customer.upserted' : 'customer.deleted');

/**
 * Idempotent declaration of the whole topology (SPEC §5.2, §7.3). Both the publisher and the
 * consumer call it: if only the consumer declared its queue, everything the pipeline published
 * before the consumer's first start would be unroutable and silently dropped. (In production the
 * topology would be provisioned up front; here both sides assert the same arguments.)
 */
export async function assertTopology(ch: Channel): Promise<void> {
  await ch.assertExchange(EXCHANGE, 'topic', { durable: true });
  await ch.assertExchange(DEAD_LETTER_EXCHANGE, 'fanout', { durable: true });

  await ch.assertQueue(DEAD_LETTER_QUEUE, { durable: true, arguments: { 'x-queue-type': 'quorum' } });
  await ch.bindQueue(DEAD_LETTER_QUEUE, DEAD_LETTER_EXCHANGE, '');

  // Quorum queue: replicated, fsync'd log — messages survive a broker restart; delivery-limit
  // dead-letters poison messages instead of redelivering them forever.
  await ch.assertQueue(QUEUE, {
    durable: true,
    arguments: {
      'x-queue-type': 'quorum',
      'x-delivery-limit': config.deliveryLimit,
      'x-dead-letter-exchange': DEAD_LETTER_EXCHANGE,
    },
  });
  await ch.bindQueue(QUEUE, EXCHANGE, 'customer.#');
}
