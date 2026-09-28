import { env } from './env';
import { basicAuth } from './http';

const VHOST = '%2F';
const auth = () => basicAuth(env.rabbitUser, env.rabbitPassword);

export interface QueueStats {
  messages: number;
  unacked: number;
}

/** Ready + unacknowledged message counts from the management API; undefined if the queue doesn't exist. */
export async function queueStats(queue: string): Promise<QueueStats | undefined> {
  const res = await fetch(`${env.rabbitMgmtUrl}/api/queues/${VHOST}/${encodeURIComponent(queue)}`, {
    headers: auth(),
    signal: AbortSignal.timeout(10_000),
  });
  if (res.status === 404) return undefined;
  if (!res.ok) throw new Error(`GET queue ${queue} -> HTTP ${res.status}`);
  const q = (await res.json()) as { messages?: number; messages_unacknowledged?: number };
  return { messages: q.messages ?? 0, unacked: q.messages_unacknowledged ?? 0 };
}

export async function purgeQueue(queue: string): Promise<void> {
  const res = await fetch(`${env.rabbitMgmtUrl}/api/queues/${VHOST}/${encodeURIComponent(queue)}/contents`, {
    method: 'DELETE',
    headers: auth(),
    signal: AbortSignal.timeout(30_000),
  });
  if (!res.ok && res.status !== 404) throw new Error(`purge ${queue} -> HTTP ${res.status}`);
}
