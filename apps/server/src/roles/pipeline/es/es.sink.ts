import { Client } from '@elastic/elasticsearch';
import { Inject, Injectable } from '@nestjs/common';
import { ES_CLIENT } from '../../../shared/elasticsearch';
import { CustomerRow } from '../source/customer-source';
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
   * Writes a batch with one _bulk request. Resolves only if every record is acknowledged
   * (written, or 409 = already there); otherwise throws, so the caller does not checkpoint.
   * Transport errors (ES down, timeout) propagate as thrown client errors.
   */
  async write(rows: CustomerRow[]): Promise<BulkSummary> {
    const operations = rows.flatMap((row) => [
      // SPEC §5.5 / §6.3: _id = customer id and external versioning make the write idempotent —
      // replaying a batch after a crash rewrites the same documents, and an older version can
      // never overwrite a newer one.
      { index: { _index: this.index.name, _id: row.id, version: Number(row.version), version_type: 'external' as const } },
      toDocument(row),
    ]);

    const res = await this.es.bulk({ operations });
    const items: BulkItem[] = res.items.map((item) => {
      const result = Object.values(item)[0]!;
      return { status: result.status, error: result.error };
    });
    const summary = summarize(items);

    if (summary.failed.some((f) => f.outcome === 'index_missing')) throw new IndexMissingError(this.index.name);
    // S2 scope: any rejected item fails the whole batch, which is then retried with backoff.
    // SPEC §7.3 (permanent failures → DLQ, the rest proceed) arrives with G4 in S5.
    if (summary.failed.length) throw new EsItemsFailedError(summary.failed);
    return summary;
  }
}
