import { Injectable } from '@nestjs/common';
import { log } from '../../../shared/logger';
import { Change } from '../changes/change';
import { DlqRepository } from '../dlq/dlq.repository';
import { EsSink } from '../es/es.sink';
import { StreamSink } from '../stream/stream.sink';

export interface DeliveryResult {
  written: number;
  absent: number;
  conflicts: number;
  dlq: number;
  published: number;
}

/**
 * Delivers one batch to both sinks — the part of SPEC §6.2 that backfill and incremental share:
 *
 *   ES _bulk → permanent rejections into the DLQ → RabbitMQ publish + confirms
 *
 * When this resolves, every change is acknowledged by each sink or durably parked in dlq_records,
 * so the caller may advance its checkpoint. When it throws, nothing may be checkpointed; the
 * caller retries the whole batch and the sinks absorb the repeats.
 */
@Injectable()
export class BatchDelivery {
  constructor(
    private readonly es: EsSink,
    private readonly dlq: DlqRepository,
    private readonly stream: StreamSink,
  ) {}

  async deliver(changes: Change[], source: 'backfill' | 'incremental', batchId: string): Promise<DeliveryResult> {
    const es = await this.es.write(changes);

    // SPEC §7.3: a bad record never blocks the batch — it is parked, the rest go on.
    if (es.rejected.length > 0) {
      await this.dlq.record(es.rejected, source, batchId);
      log('dlq.recorded', {
        stream: source,
        batch_id: batchId,
        count: es.rejected.length,
        records: es.rejected.map((r) => ({ customer_id: Number(r.change.id), position: r.failure.position, type: r.failure.type })),
      }, 'warn');
    }

    // The stream gets every change, including ones the index refused: the sinks are independent
    // (SPEC §14.6) and a downstream consumer may well accept what the index mapping does not.
    const published = await this.stream.publish(changes, source);
    return { written: es.written, absent: es.absent, conflicts: es.conflicts, dlq: es.rejected.length, published };
  }
}
