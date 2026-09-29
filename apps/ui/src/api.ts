import { onBeforeUnmount, onMounted, ref, type Ref } from 'vue';

// Thin client for the api (SPEC §9). Same origin: nginx (or the Vite dev server) proxies /api.

export async function api<T = unknown>(path: string, init: { method?: string; body?: unknown } = {}): Promise<T> {
  const res = await fetch(`/api${path}`, {
    method: init.method ?? 'GET',
    headers: init.body === undefined ? undefined : { 'content-type': 'application/json' },
    body: init.body === undefined ? undefined : JSON.stringify(init.body),
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    throw new Error((detail as { message?: string }).message ?? `HTTP ${res.status}`);
  }
  return (res.status === 204 ? undefined : await res.json()) as T;
}

/** SPEC §10: the UI polls every 2 s. Returns reactive data, the last error and a manual refresh. */
export function usePoll<T>(load: () => Promise<T>, intervalMs = 2_000): { data: Ref<T | undefined>; error: Ref<string>; refresh: () => Promise<void> } {
  const data = ref<T>();
  const error = ref('');
  let timer: number | undefined;
  const refresh = async () => {
    try {
      data.value = await load();
      error.value = '';
    } catch (e) {
      error.value = (e as Error).message;
    }
  };
  onMounted(() => {
    void refresh();
    timer = window.setInterval(refresh, intervalMs);
  });
  onBeforeUnmount(() => window.clearInterval(timer));
  return { data: data as Ref<T | undefined>, error, refresh };
}

export interface Status {
  generated_at: string;
  health: { status: 'ok' | 'degraded' | 'down'; reasons: string[] };
  pipeline: { alive: boolean; instance_id: string | null; started_at: string | null; last_beat_at: string | null; last_beat_age_seconds: number | null };
  backfill: { state: string; position: number; total: number; percent: number; rate_per_sec: number };
  incremental: { state: string; position: { txid: number; seq: number }; lag_events: number; lag_seconds: number; rate_per_sec: number };
  sinks: Record<'elasticsearch' | 'rabbitmq', { circuit: string }>;
  dlq: { pending: number; replayed: number; total: number };
  consumer: { applied: number; duplicates: number; queue_ready: number; queue_unacked: number; consumers: number; dead_lettered: number };
  settings: { batch_size: number; poll_interval_ms: number };
}

export const n = (v: number | string | null | undefined) => (v === null || v === undefined ? '—' : Number(v).toLocaleString('en-US'));
