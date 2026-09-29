import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import { ES_CLIENT } from '../../../shared/elasticsearch';
import { Change } from '../changes/change';
import { BulkItem, BulkSummary, FailedItem, summarize } from './bulk-result';
import { CustomerIndex, toDocument } from './customer-index';

export class IndexMissingError extends Error {
  constructor(index: string) {
    super(`index "${index}" does not exist`);
  }
}

/** Some items were not accepted; the batch must not be checkpointed yet. */
export class EsItemsFailedError extends Error {
  constructor(readonly failed: FailedItem[]) {
    const first = failed[0];
    super(`${failed.length} item(s) failed, first: ${first.status} ${first.type}: ${first.reason}`);
  }
}

@Injectable()
export class EsSink {
  constructor(
    @Inject(ES_CLIENT) private readonly es: Client,
    private readonly index: CustomerIndex,
  ) {}

  /**
   * Writes a batch with one _bulk request. Resolves only if every change is acknowledged
   * (written, 409 = already there, or delete of an absent doc); otherwise throws, so the caller
   * does not checkpoint. Transport errors (ES down, timeout) propagate as thrown client errors.
   */
  async write(changes: Change[]): Promise<BulkSummary> {
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
    // S2 scope: any rejected item fails the whole batch, which is then retried with backoff.
    // SPEC §7.3 (permanent failures → DLQ, the rest proceed) arrives with G4 in S5.
    if (summary.failed.length) throw new EsItemsFailedError(summary.failed);
    return summary;
  }
}
