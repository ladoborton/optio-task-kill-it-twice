<script setup lang="ts">
import { ref } from 'vue';
import { api, usePoll } from '../api';

// SPEC §10 screen 4: cause the failures the gates test, by hand. Watch the header and the Status tab.

const services = [
  { id: 'elasticsearch', what: 'Search index sink', watch: 'breaker opens, health "degraded"; the checkpoint stops moving (G3)', kill: false },
  { id: 'rabbitmq', what: 'Event stream sink / broker', watch: 'rabbitmq breaker opens; both pipeline and consumer reconnect on their own', kill: false },
  { id: 'pipeline', what: 'Backfill + incremental + DLQ replay', watch: 'health "down" within ~10 s; after Start it resumes from its checkpoint (G1)', kill: true },
  { id: 'consumer', what: 'Independent stream consumer', watch: 'queue grows while it is gone; after Start it drains, duplicates are ignored (G2)', kill: true },
];

const { data: states, refresh } = usePoll(() => api<Record<string, string>>('/sim/services'));
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
    message.value = `${label}${r && typeof r === 'object' ? ` — ${JSON.stringify(r)}` : ''}`;
    clearTimer = window.setTimeout(() => (message.value = ''), 6_000);
    await refresh();
  } catch (e) {
    failure.value = `${label} failed: ${(e as Error).message}`;
  } finally {
    busy.value = '';
  }
}

const badCount = ref(3);
const updates = ref(1000);
const inserts = ref(50);
const deletes = ref(20);
</script>

<template>
  <div aria-live="polite">
    <div v-if="failure" class="notice error" role="alert">{{ failure }}</div>
    <div v-else-if="busy" class="notice info">{{ busy }}…</div>
    <div v-else-if="message" class="notice info">{{ message }}</div>
  </div>

  <div class="stack">
    <section class="card">
      <div class="card-head">
        <div>
          <h2 style="margin-bottom: 2px">Containers</h2>
          <span class="small muted">Stop = graceful (SIGTERM). Kill = crash (SIGKILL) — no chance to flush or checkpoint. Talks to Docker through the mounted socket: dev only.</span>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>service</th><th>state</th><th>what to watch</th><th></th></tr></thead>
          <tbody>
            <tr v-for="s in services" :key="s.id">
              <td><strong>{{ s.id }}</strong><div class="small muted">{{ s.what }}</div></td>
              <td><span :class="['badge', states?.[s.id] ?? 'neutral']">{{ states?.[s.id] ?? '—' }}</span></td>
              <td class="small muted">{{ s.watch }}</td>
              <td>
                <div class="btn-group" style="justify-content: flex-end; display: flex">
                  <button :disabled="!!busy" @click="run(`Stopping ${s.id}`, () => api(`/sim/services/${s.id}/stop`, { method: 'POST' }))">Stop</button>
                  <button v-if="s.kill" class="danger" :disabled="!!busy" @click="run(`Killing ${s.id}`, () => api(`/sim/services/${s.id}/kill`, { method: 'POST' }))">Kill</button>
                  <button class="primary" :disabled="!!busy" @click="run(`Starting ${s.id}`, () => api(`/sim/services/${s.id}/start`, { method: 'POST' }))">Start</button>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <div class="grid cols-2">
      <section class="card">
        <div class="card-head"><h2>Inject bad records</h2></div>
        <p class="small muted" style="margin: 0 0 12px">New customers Postgres accepts but the index mapping rejects (<code>age</code> is not a number, or an unknown attribute). Expect them in the DLQ on the Control tab while everything else keeps flowing (G4).</p>
        <form class="row between" @submit.prevent="run('Bad records inserted', () => api('/sim/bad-records', { method: 'POST', body: { count: badCount } }))">
          <label>count <input v-model.number="badCount" type="number" min="1" max="100" /></label>
          <button class="primary" :disabled="!!busy">Insert</button>
        </form>
      </section>

      <section class="card">
        <div class="card-head"><h2>Generate source changes</h2></div>
        <p class="small muted" style="margin: 0 0 12px">A burst of changes in Postgres. Watch the lag rise and fall on the Status tab and the rows arrive on the Records tab.</p>
        <form class="stack" @submit.prevent="run('Changes generated', () => api('/sim/changes', { method: 'POST', body: { updates, inserts, deletes } }))">
          <div class="row">
            <label>updates <input v-model.number="updates" type="number" min="0" max="100000" /></label>
            <label>inserts <input v-model.number="inserts" type="number" min="0" max="10000" /></label>
            <label>deletes <input v-model.number="deletes" type="number" min="0" max="10000" /></label>
          </div>
          <div class="row" style="justify-content: flex-end"><button class="primary" :disabled="!!busy">Generate</button></div>
        </form>
      </section>
    </div>
  </div>
</template>
