<script setup lang="ts">
import { ref, watch } from 'vue';
import { api, n, usePoll, type Status } from '../api';

// SPEC §10 screen 3: start/pause the loops, settings, DLQ replay. Every action only records the
// desired state in Postgres; the pipeline applies it on its next step (the status line shows when).

const props = defineProps<{ status?: Status }>();

interface DlqRow {
  id: string; customer_id: string; version: string; status: string; attempts: number; error_type: string;
  error_reason: string; stream: string; batch_id: string; batch_position: number; created_at: string; replay_requested_at: string | null;
}

const message = ref('');
const failure = ref('');
const busy = ref('');
let clearTimer: number | undefined;
async function run(label: string, action: () => Promise<unknown>) {
  busy.value = label;
  failure.value = '';
  window.clearTimeout(clearTimer);
  try {
    const r = await action();
    message.value = `${label}${r && typeof r === 'object' ? ` — ${JSON.stringify(r)}` : ''}. The pipeline applies it on its next step.`;
    clearTimer = window.setTimeout(() => (message.value = ''), 5_000);
    await refreshDlq();
  } catch (e) {
    failure.value = `${label} failed: ${(e as Error).message}`;
  } finally {
    busy.value = '';
  }
}

const batchSize = ref(500);
const pollMs = ref(1000);
let settingsLoaded = false;
watch(() => props.status, (s) => {
  if (s && !settingsLoaded) {
    batchSize.value = s.settings.batch_size;
    pollMs.value = s.settings.poll_interval_ms;
    settingsLoaded = true;
  }
}, { immediate: true });

const dlqFilter = ref<'pending' | 'all'>('pending');
const { data: dlq, refresh: refreshDlq } = usePoll(() => api<DlqRow[]>(`/dlq?status=${dlqFilter.value}&limit=200`));
watch(dlqFilter, () => void refreshDlq());
const picked = ref<string[]>([]);

const resetBackfill = () => {
  if (confirm('Start the backfill over from id 0? Documents already in the sinks are rewritten (idempotent).')) {
    void run('Backfill reset', () => api('/backfill/reset', { method: 'POST' }));
  }
};
const replaySelected = () => {
  const ids = picked.value;
  picked.value = [];
  void run(`Replay requested for ${ids.length} record(s)`, () => api('/dlq/replay', { method: 'POST', body: { ids } }));
};
</script>

<template>
  <div aria-live="polite">
    <div v-if="failure" class="notice error" role="alert">{{ failure }}</div>
    <div v-else-if="message" class="notice info">{{ message }}</div>
  </div>

  <div class="stack">
    <div class="grid cols-3">
      <section class="card">
        <div class="card-head">
          <h3>Backfill</h3>
          <span :class="['badge', status?.backfill.state ?? 'neutral']">{{ status?.backfill.state ?? '—' }}</span>
        </div>
        <p class="small muted" style="margin: 0 0 12px">{{ status?.backfill.percent ?? '—' }}% · checkpoint id {{ n(status?.backfill.position) }}</p>
        <div class="btn-group">
          <button :disabled="!!busy" @click="run('Backfill paused', () => api('/backfill/pause', { method: 'POST' }))">Pause</button>
          <button :disabled="!!busy" @click="run('Backfill resumed', () => api('/backfill/resume', { method: 'POST' }))">Resume</button>
          <button class="danger" :disabled="!!busy" @click="resetBackfill">Reset to 0…</button>
        </div>
      </section>

      <section class="card">
        <div class="card-head">
          <h3>Incremental sync</h3>
          <span :class="['badge', status?.incremental.state ?? 'neutral']">{{ status?.incremental.state ?? '—' }}</span>
        </div>
        <p class="small muted" style="margin: 0 0 12px">lag {{ n(status?.incremental.lag_events) }} events · {{ status?.incremental.lag_seconds ?? '—' }} s</p>
        <div class="btn-group">
          <button :disabled="!!busy" @click="run('Incremental paused', () => api('/incremental/pause', { method: 'POST' }))">Pause</button>
          <button :disabled="!!busy" @click="run('Incremental resumed', () => api('/incremental/resume', { method: 'POST' }))">Resume</button>
        </div>
      </section>

      <section class="card">
        <div class="card-head"><h3>Settings</h3></div>
        <form class="stack" @submit.prevent="run('Settings saved', () => api('/settings', { method: 'PUT', body: { batch_size: batchSize, poll_interval_ms: pollMs } }))">
          <div class="row">
            <label>batch size <input v-model.number="batchSize" type="number" min="1" max="10000" /></label>
            <label>poll interval, ms <input v-model.number="pollMs" type="number" min="100" max="60000" /></label>
          </div>
          <div class="row between">
            <span class="small muted">Both loops pick it up on their next step.</span>
            <button class="primary" :disabled="!!busy">Save</button>
          </div>
        </form>
      </section>
    </div>

    <section class="card">
      <div class="card-head">
        <div>
          <h2 style="margin-bottom: 2px">Index DLQ</h2>
          <span class="small muted">Records Elasticsearch rejected, with the context to replay them.</span>
        </div>
        <div class="row">
          <select v-model="dlqFilter" aria-label="Filter DLQ records"><option value="pending">pending</option><option value="all">all</option></select>
          <button :disabled="!picked.length || !!busy" @click="replaySelected">Replay selected ({{ picked.length }})</button>
          <button class="primary" :disabled="!status?.dlq.pending || !!busy" @click="run('Replay requested for all pending', () => api('/dlq/replay', { method: 'POST', body: {} }))">Replay all pending</button>
        </div>
      </div>
      <p class="small muted" style="margin: 0 0 10px">A replay re-reads the <em>current</em> source row — fix the data first, then replay. Still bad ⇒ it stays pending with attempts + 1.</p>
      <div class="table-wrap scroll">
        <table>
          <thead><tr><th><span class="sr-only">select</span></th><th class="num">id</th><th class="num">customer</th><th class="num">v</th><th>status</th><th class="num">attempts</th><th>error</th><th>batch · position</th></tr></thead>
          <tbody>
            <tr v-for="d in dlq ?? []" :key="d.id">
              <td><input v-if="d.status === 'pending'" v-model="picked" type="checkbox" :value="d.id" :aria-label="`Select DLQ record ${d.id}`" /></td>
              <td class="num mono">{{ d.id }}</td>
              <td class="num mono">#{{ d.customer_id }}</td>
              <td class="num">{{ d.version }}</td>
              <td>
                <span :class="['badge', d.status]">{{ d.status }}</span>
                <div v-if="d.replay_requested_at" class="small muted">replay requested</div>
              </td>
              <td class="num">{{ d.attempts }}</td>
              <td class="small"><strong>{{ d.error_type }}</strong><br /><span class="muted">{{ d.error_reason }}</span></td>
              <td class="small mono">{{ d.batch_id }} · {{ d.batch_position }}</td>
            </tr>
          </tbody>
        </table>
        <div v-if="dlq && !dlq.length" class="empty">Nothing here — every record the index was given has been accepted.</div>
      </div>
    </section>

    <section class="card">
      <div class="card-head">
        <div>
          <h2 style="margin-bottom: 2px">Consumer dead-letter queue</h2>
          <span class="small muted">RabbitMQ messages the consumer could not parse.</span>
        </div>
        <div class="row">
          <span :class="['badge', status?.consumer.dead_lettered ? 'pending' : 'ok']">{{ n(status?.consumer.dead_lettered) }} message(s)</span>
          <button :disabled="!status?.consumer.dead_lettered || !!busy" @click="run('Consumer dead letters requeued', () => api('/consumer-dlq/requeue', { method: 'POST', body: {} }))">Requeue to the main queue</button>
        </div>
      </div>
    </section>
  </div>
</template>

<style scoped>
.sr-only { position: absolute; width: 1px; height: 1px; overflow: hidden; clip: rect(0 0 0 0); white-space: nowrap; }
</style>
