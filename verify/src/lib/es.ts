import { env } from './env';

const INDEX = 'customers';

async function call(method: string, path: string): Promise<Response> {
  return fetch(`${env.esUrl}${path}`, { method, signal: AbortSignal.timeout(30_000) });
}

export async function deleteIndex(): Promise<void> {
  const res = await call('DELETE', `/${INDEX}`);
  if (!res.ok && res.status !== 404) throw new Error(`DELETE /${INDEX} -> HTTP ${res.status}`);
}

/** Document count after a refresh, so recently indexed docs are visible. 0 if the index is missing. */
export async function countDocs(): Promise<number> {
  const refresh = await call('POST', `/${INDEX}/_refresh`);
  if (refresh.status === 404) return 0;
  const res = await call('GET', `/${INDEX}/_count`);
  if (!res.ok) throw new Error(`GET /${INDEX}/_count -> HTTP ${res.status}`);
  return ((await res.json()) as { count: number }).count;
}
