import { config } from '../config';

export interface QueueStats {
  ready: number;
  unacked: number;
  consumers: number;
}

/** Queue depth from the RabbitMQ management API; undefined when the queue does not exist yet. */
export async function queueStats(queue: string): Promise<QueueStats | undefined> {
  const auth = Buffer.from(`${config.rabbitUser}:${config.rabbitPassword}`).toString('base64');
  const res = await fetch(`${config.rabbitMgmtUrl}/api/queues/%2F/${encodeURIComponent(queue)}`, {
    headers: { authorization: `Basic ${auth}` },
    signal: AbortSignal.timeout(3_000),
  });
  if (res.status === 404) return undefined;
  if (!res.ok) throw new Error(`rabbitmq management: HTTP ${res.status}`);
  const q = (await res.json()) as { messages_ready?: number; messages_unacknowledged?: number; consumers?: number };
  return { ready: q.messages_ready ?? 0, unacked: q.messages_unacknowledged ?? 0, consumers: q.consumers ?? 0 };
}
