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
async function run(label: string, action: () => Promise<unknown>) {
  failure.value = '';
  try {
    const r = await action();
    message.value = `${label}${r && typeof r === 'object' ? ` — ${JSON.stringify(r)}` : ''}`;
    await refreshDlq();
  } catch (e) {
    failure.value = `${label}: ${(e as Error).message}`;
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
    void run('backfill reset', () => api('/backfill/reset', { method: 'POST' }));
  }
};
</script>

<template>
  <div v-if="failure" class="notice error">{{ failure }}</div>
  <div v-else-if="message" class="notice info">{{ message }}</div>

  <div class="grid">
    <section class="card">
      <h3>Backfill</h3>
      <p class="small">State: <span :class="['badge', status?.backfill.state ?? 'unknown']">{{ status?.backfill.state ?? '—' }}</span>
        · {{ status?.backfill.percent ?? '—' }}%</p>
      <div class="row">
        <button @click="run('backfill paused', () => api('/backfill/pause', { method: 'POST' }))">Pause</button>
        <button @click="run('backfill resumed', () => api('/backfill/resume', { method: 'POST' }))">Resume</button>
        <button class="danger" @click="resetBackfill">Reset to 0</button>
      </div>
    </section>

    <section class="card">
      <h3>Incremental sync</h3>
      <p class="small">State: <span :class="['badge', status?.incremental.state ?? 'unknown']">{{ status?.incremental.state ?? '—' }}</span>
        · lag {{ n(status?.incremental.lag_events) }} events</p>
      <div class="row">
        <button @click="run('incremental paused', () => api('/incremental/pause', { method: 'POST' }))">Pause</button>
        <button @click="run('incremental resumed', () => api('/incremental/resume', { method: 'POST' }))">Resume</button>
      </div>
    </section>

    <section class="card">
      <h3>Settings</h3>
      <form class="row" @submit.prevent="run('settings saved', () => api('/settings', { method: 'PUT', body: { batch_size: batchSize, poll_interval_ms: pollMs } }))">
        <label>batch size <input v-model.number="batchSize" type="number" min="1" max="10000" /></label>
        <label>poll interval ms <input v-model.number="pollMs" type="number" min="100" max="60000" /></label>
        <button class="primary">Save</button>
      </form>
      <p class="small muted">Applied by both loops from their next step.</p>
    </section>
  </div>

  <section class="card" style="margin-top: 12px">
    <div class="row" style="justify-content: space-between">
      <h2>Index DLQ <span class="small muted">— records Elasticsearch rejected, with the context to replay them</span></h2>
      <div class="row">
        <select v-model="dlqFilter"><option value="pending">pending</option><option value="all">all</option></select>
        <button :disabled="!picked.length" @click="run(`replay requested for ${picked.length}`, () => api('/dlq/replay', { method: 'POST', body: { ids: picked } })); picked = []">Replay selected</button>
        <button class="primary" :disabled="!status?.dlq.pending" @click="run('replay requested for all pending', () => api('/dlq/replay', { method: 'POST', body: {} }))">Replay all pending</button>
      </div>
    </div>
    <p class="small muted">A replay re-reads the <em>current</em> source row — fix the data first (or the mapping), then replay. Still bad ⇒ stays pending, attempts +1.</p>
    <div class="scroll">
      <table>
        <thead><tr><th></th><th>id</th><th>customer</th><th>v</th><th>status</th><th>attempts</th><th>error</th><th>batch</th></tr></thead>
        <tbody>
          <tr v-for="d in dlq ?? []" :key="d.id">
            <td><input v-if="d.status === 'pending'" v-model="picked" type="checkbox" :value="d.id" /></td>
            <td class="mono">{{ d.id }}</td>
            <td class="mono">#{{ d.customer_id }}</td>
            <td>{{ d.version }}</td>
            <td><span :class="['badge', d.status]">{{ d.status }}</span><span v-if="d.replay_requested_at" class="small muted"> replay requested</span></td>
            <td>{{ d.attempts }}</td>
            <td class="small"><strong>{{ d.error_type }}</strong><br /><span class="muted">{{ d.error_reason }}</span></td>
            <td class="small mono">{{ d.batch_id }} @{{ d.batch_position }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="dlq && !dlq.length" class="muted small">Nothing here.</p>
    </div>
  </section>

  <section class="card" style="margin-top: 12px">
    <h2>Consumer dead-letter queue <span class="small muted">— RabbitMQ messages the consumer could not parse</span></h2>
    <div class="row">
      <span>{{ n(status?.consumer.dead_lettered) }} message(s)</span>
      <button :disabled="!status?.consumer.dead_lettered" @click="run('consumer DLQ requeued', () => api('/consumer-dlq/requeue', { method: 'POST', body: {} }))">Requeue to the main queue</button>
    </div>
  </section>
</template>
