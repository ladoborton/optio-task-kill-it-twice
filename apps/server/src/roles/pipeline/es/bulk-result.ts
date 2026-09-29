// Pure classification of Elasticsearch _bulk item results (unit-tested, no I/O).

/** The part of one _bulk response item we act on. */
export interface BulkItem {
  status: number;
  /** ES result string: created | updated | deleted | not_found | noop. */
  result?: string;
  error?: { type: string; reason?: string | null };
}

export type ItemOutcome = 'written' | 'absent' | 'conflict' | 'retryable' | 'permanent' | 'index_missing';

export function classifyItem(item: BulkItem): ItemOutcome {
  if (item.status >= 200 && item.status < 300) return 'written';
  // A delete of a document the index never had (e.g. created and deleted before the backfill
  // reached it). The goal state — "not in the index" — holds; ES still records the delete's
  // version as a tombstone, so a late, older upsert is rejected.
  if (item.status === 404 && item.result === 'not_found' && !item.error) return 'absent';
  // SPEC §5.5: external versioning refused the write because the index already holds this
  // version or a newer one. That is the idempotency working, not a failure.
  if (item.status === 409) return 'conflict';
  if (item.error?.type === 'index_not_found_exception') return 'index_missing';
  // Overloaded or unavailable shard: the same request can succeed later.
  if (item.status === 429 || item.status >= 500) return 'retryable';
  // SPEC §7.3: the document itself is unacceptable (e.g. mapper_parsing_exception);
  // retrying the same bytes cannot help.
  return 'permanent';
}

export interface FailedItem {
  /** Index of the record inside the batch. */
  position: number;
  outcome: Exclude<ItemOutcome, 'written' | 'absent' | 'conflict'>;
  status: number;
  type: string;
  reason: string;
}

export interface BulkSummary {
  written: number;
  absent: number;
  conflicts: number;
  failed: FailedItem[];
}

export function summarize(items: BulkItem[]): BulkSummary {
  const summary: BulkSummary = { written: 0, absent: 0, conflicts: 0, failed: [] };
  items.forEach((item, position) => {
    const outcome = classifyItem(item);
    if (outcome === 'written') summary.written++;
    else if (outcome === 'absent') summary.absent++;
    else if (outcome === 'conflict') summary.conflicts++;
    else
      summary.failed.push({
        position,
        outcome,
        status: item.status,
        type: item.error?.type ?? 'unknown',
        reason: item.error?.reason ?? '',
      });
  });
  return summary;
}
