import { env } from './env';

const INDEX = 'customers';

async function call(method: string, path: string): Promise<Response> {
  return fetch(`${env.esUrl}${path}`, { method, signal: AbortSignal.timeout(30_000) });
}

export async function deleteIndex(): Promise<void> {
  const res = await call('DELETE', `/${INDEX}`);
  if (!res.ok && res.status !== 404) throw new Error(`DELETE /${INDEX} -> HTTP ${res.status}`);
}

/**
 * `_id -> _version` for docs with id in [from, to]. With external versioning `_version` is the
 * customers.version that was written, so this is exactly what the index believes is current.
 * The caller keeps ranges ≤ 10,000 ids (ES max_result_window).
 */
export async function versionsInRange(from: number, to: number): Promise<Map<number, number>> {
  const res = await fetch(`${env.esUrl}/${INDEX}/_search`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    signal: AbortSignal.timeout(30_000),
    body: JSON.stringify({
      size: 10_000,
      _source: false,
      version: true,
      query: { range: { id: { gte: from, lte: to } } },
    }),
  });
  if (!res.ok) throw new Error(`search ${from}-${to} -> HTTP ${res.status}`);
  const body = (await res.json()) as { hits: { hits: { _id: string; _version: number }[] } };
  return new Map(body.hits.hits.map((h) => [Number(h._id), h._version]));
}

/** Document count after a refresh, so recently indexed docs are visible. 0 if the index is missing. */
export async function countDocs(): Promise<number> {
  const refresh = await call('POST', `/${INDEX}/_refresh`);
  if (refresh.status === 404) return 0;
  const res = await call('GET', `/${INDEX}/_count`);
  if (!res.ok) throw new Error(`GET /${INDEX}/_count -> HTTP ${res.status}`);
  return ((await res.json()) as { count: number }).count;
}
