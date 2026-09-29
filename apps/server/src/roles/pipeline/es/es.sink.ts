import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import { CircuitBreaker } from '../../../shared/circuit-breaker';
import { ES_CLIENT } from '../../../shared/elasticsearch';
import { ES_BREAKER } from '../breakers';
import { Change } from '../changes/change';
import { BulkItem, BulkSummary, FailedItem, summarize } from './bulk-result';
import { CustomerIndex, toDocument } from './customer-index';

export class IndexMissingError extends Error {
  constructor(index: string) {
    super(`index "${index}" does not exist`);
  }
}

/** Some items hit a transient error (429/5xx); the batch must be retried, not checkpointed. */
export class EsItemsRetryableError extends Error {
  constructor(readonly failed: FailedItem[]) {
    const first = failed[0];
    super(`${failed.length} item(s) retryable, first: ${first.status} ${first.type}: ${first.reason}`);
  }
}

/** A change the index refused for good, and why. */
export interface Rejection {
  change: Change;
  failure: FailedItem;
}

export interface EsWriteResult extends BulkSummary {
  /** Permanently rejected changes (SPEC §7.3): the caller parks them in the DLQ. */
  rejected: Rejection[];
}

@Injectable()
export class EsSink {
  constructor(
    @Inject(ES_CLIENT) private readonly es: Client,
    @Inject(ES_BREAKER) private readonly breaker: CircuitBreaker,
    private readonly index: CustomerIndex,
  ) {}

  /** write() behind the index's circuit breaker: while it is open, fails fast without a request. */
  write(changes: Change[]): Promise<EsWriteResult> {
    return this.breaker.run(() => this.bulk(changes));
  }

  /**
   * Writes a batch with one _bulk request and classifies every item (SPEC §7.3):
   * - acknowledged (written / 409 already there / delete of an absent doc) → counted;
   * - permanently rejected (the document itself is bad) → returned in `rejected`, the batch goes on;
   * - transient (429/5xx) or missing index → throws, the whole batch is retried with backoff.
   * Retrying the whole batch is safe: the items that did succeed come back as 409.
   * Transport errors (ES down, timeout) propagate as thrown client errors.
   */
  private async bulk(changes: Change[]): Promise<EsWriteResult> {
    await this.index.ensureReady();

    // SPEC §5.5 / §6.3: _id = customer id and external versioning make every write idempotent and
    // order-proof — replaying a batch rewrites the same documents, and an older version (upsert
    // or delete) can never overwrite a newer one, whichever loop sends it and in whatever order.
    const target = (c: Change) => ({ _index: this.index.name, _id: c.id, version: Number(c.version), version_type: 'external' as const });
    // _bulk body: an action line, followed by the document for index actions only.
    const operations = changes.flatMap((c): object[] =>
      c.op === 'upsert' ? [{ index: target(c) }, toDocument(c.row)] : [{ delete: target(c) }],
    );

    const res = await this.es.bulk({ operations });
    const items: BulkItem[] = res.items.map((item) => {
      const r = Object.values(item)[0]!;
      return { status: r.status, result: r.result, error: r.error };
    });
    const summary = summarize(items);

    if (summary.failed.some((f) => f.outcome === 'index_missing')) {
      this.index.invalidate();
      throw new IndexMissingError(this.index.name);
    }
    const retryable = summary.failed.filter((f) => f.outcome === 'retryable');
    if (retryable.length) throw new EsItemsRetryableError(retryable);

    const rejected = summary.failed.map((failure) => ({ change: changes[failure.position], failure }));
    return { ...summary, rejected };
  }
}
