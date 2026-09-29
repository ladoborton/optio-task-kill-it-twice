import { BadRequestException, Body, Controller, Get, Post, Query } from '@nestjs/common';
import { connect } from 'amqplib';
import { DataSource } from 'typeorm';
import { config } from '../../../shared/config';
import { assertTopology, DEAD_LETTER_QUEUE, EXCHANGE } from '../../../shared/rabbitmq/topology';

@Controller('api')
export class DlqController {
  constructor(private readonly db: DataSource) {}

  /** SPEC §4.4: DLQ records with their full context, newest first. */
  @Get('dlq')
  list(@Query('status') status = 'all', @Query('limit') limit = '100') {
    const statuses = status === 'all' ? ['pending', 'replayed', 'failed'] : [status];
    return this.db.query(
      `SELECT id, sink, customer_id, version, status, attempts, error_type, error_reason, stream, batch_id,
              batch_position, payload, created_at, last_attempt_at, replay_requested_at
         FROM dlq_records WHERE status = ANY($1::text[]) ORDER BY id DESC LIMIT $2`,
      [statuses, Math.min(Number(limit) || 100, 1_000)],
    );
  }

  /**
   * SPEC §7.4 / D-004: request a replay of the given pending records (or all of them). The pipeline's
   * DLQ replay loop carries it out; the request itself is durable.
   */
  @Post('dlq/replay')
  async replay(@Body() body: { ids?: unknown }): Promise<{ requested: number }> {
    const ids = body.ids === undefined ? null : body.ids;
    if (ids !== null && (!Array.isArray(ids) || !ids.every((i) => Number.isInteger(Number(i))))) {
      throw new BadRequestException('ids must be an array of DLQ record ids, or omitted for all pending');
    }
    const r = await this.db.query(
      `UPDATE dlq_records SET replay_requested_at = now()
        WHERE status = 'pending' AND ($1::bigint[] IS NULL OR id = ANY($1::bigint[]))`,
      [ids],
    );
    return { requested: r[1] ?? 0 };
  }

  /**
   * SPEC §7.4: move the consumer's dead-lettered messages back onto the main exchange (e.g. after
   * the consumer was fixed). Each message is re-published with confirms before it is acked here,
   * so a crash in between duplicates at worst — which the consumer's dedup absorbs.
   */
  @Post('consumer-dlq/requeue')
  async requeue(@Body() body: { limit?: unknown }): Promise<{ requeued: number }> {
    const limit = Math.min(Number(body.limit) || 1_000, 10_000);
    const connection = await connect(config.rabbitmqUrl);
    let requeued = 0;
    try {
      const ch = await connection.createConfirmChannel();
      await assertTopology(ch);
      for (; requeued < limit; requeued++) {
        const msg = await ch.get(DEAD_LETTER_QUEUE, { noAck: false });
        if (!msg) break;
        ch.publish(EXCHANGE, msg.fields.routingKey, msg.content, { ...msg.properties, headers: {} });
        await ch.waitForConfirms();
        ch.ack(msg);
      }
    } finally {
      await connection.close();
    }
    return { requeued };
  }
}
