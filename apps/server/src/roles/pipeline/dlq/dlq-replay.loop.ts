import { Injectable } from '@nestjs/common';
import { config } from '../../../shared/config';
import { log } from '../../../shared/logger';
import { upsert } from '../changes/change';
import { EsSink } from '../es/es.sink';
import { CustomerSource } from '../source/customer-source';
import { StepLoop } from '../step-loop';
import { DlqRepository } from './dlq.repository';

/**
 * SPEC §7.4: carries out DLQ replay requests (dlq_records.replay_requested_at, set by api/UI/verify).
 *
 * A replay re-reads the CURRENT source row — the usual fix for a rejected record is in the source
 * data — and sends it to the sink that rejected it. Accepted (or already there with that version)
 * ⇒ `replayed`; rejected again ⇒ stays `pending`, attempt counted, latest reason kept.
 * The stream is not re-sent: it never rejected the record, and a fix made in the source reaches the
 * stream through the incremental loop like any other change.
 */
@Injectable()
export class DlqReplayLoop extends StepLoop {
  protected readonly stream = 'dlq_replay' as const;

  constructor(
    private readonly dlq: DlqRepository,
    private readonly source: CustomerSource,
    private readonly es: EsSink,
  ) {
    super();
  }

  protected async step(): Promise<number> {
    const requests = await this.dlq.replayRequests(config.dlqReplayBatch);
    if (requests.length === 0) return config.dlqReplayPollMs;

    const ids = [...new Set(requests.map((r) => r.customer_id))];
    const rows = await this.source.readByIds(ids);
    const present = new Set(rows.map((r) => r.id));

    // Throws on transient ES trouble: the requests stay open and StepLoop retries with backoff.
    const result = rows.length > 0 ? await this.es.write(rows.map(upsert)) : { rejected: [] };
    const rejectedBy = new Map(result.rejected.map((r) => [r.change.id, r.failure]));

    const replayed: string[] = [];
    for (const request of requests) {
      const failure = rejectedBy.get(request.customer_id);
      if (failure) await this.dlq.markAttemptFailed(request.id, failure.type, failure.reason);
      // Delivered — or the customer has been deleted since, which the incremental loop propagates.
      else replayed.push(request.id);
    }
    await this.dlq.markReplayed(replayed);

    log('dlq_replay.batch', {
      stream: this.stream,
      requests: requests.length,
      replayed: replayed.length,
      still_failing: requests.length - replayed.length,
      customers_deleted: ids.filter((id) => !present.has(id)).length,
    });
    return 0;
  }
}
