<script setup lang="ts">
import { ref } from 'vue';
import { api, usePoll } from '../api';

// SPEC §10 screen 4: cause the failures the gates test, by hand. Watch the header and the Status tab.

const services = [
  { id: 'elasticsearch', what: 'search index sink (G3)', kill: false },
  { id: 'rabbitmq', what: 'event stream sink / broker', kill: false },
  { id: 'pipeline', what: 'backfill + incremental + DLQ replay (G1)', kill: true },
  { id: 'consumer', what: 'independent stream consumer (G2)', kill: true },
];

const { data: states, refresh } = usePoll(() => api<Record<string, string>>('/sim/services'));
const message = ref('');
const failure = ref('');
const busy = ref(false);

async function run(label: string, action: () => Promise<unknown>) {
  busy.value = true;
  failure.value = '';
  try {
    const r = await action();
    message.value = `${label}${r && typeof r === 'object' ? ` — ${JSON.stringify(r)}` : ''}`;
    await refresh();
  } catch (e) {
    failure.value = `${label}: ${(e as Error).message}`;
  } finally {
    busy.value = false;
  }
}

const badCount = ref(3);
const updates = ref(1000);
const inserts = ref(50);
const deletes = ref(20);
</script>

<template>
  <div v-if="failure" class="notice error">{{ failure }}</div>
  <div v-else-if="message" class="notice info">{{ message }}</div>

  <section class="card">
    <h2>Containers</h2>
    <p class="small muted">Stop = graceful (SIGTERM). Kill = crash (SIGKILL) — no chance to flush or checkpoint. This control talks to Docker through the mounted socket: dev only.</p>
    <table>
      <thead><tr><th>service</th><th>role</th><th>state</th><th></th></tr></thead>
      <tbody>
        <tr v-for="s in services" :key="s.id">
          <td><strong>{{ s.id }}</strong></td>
          <td class="small muted">{{ s.what }}</td>
          <td><span :class="['badge', states?.[s.id] ?? 'unknown']">{{ states?.[s.id] ?? '—' }}</span></td>
          <td class="row">
            <button :disabled="busy" @click="run(`${s.id} stopped`, () => api(`/sim/services/${s.id}/stop`, { method: 'POST' }))">Stop</button>
            <button v-if="s.kill" class="danger" :disabled="busy" @click="run(`${s.id} killed`, () => api(`/sim/services/${s.id}/kill`, { method: 'POST' }))">Kill</button>
            <button :disabled="busy" @click="run(`${s.id} started`, () => api(`/sim/services/${s.id}/start`, { method: 'POST' }))">Start</button>
          </td>
        </tr>
      </tbody>
    </table>
  </section>

  <div class="grid" style="margin-top: 12px">
    <section class="card">
      <h2>Inject bad records</h2>
      <p class="small muted">New customers whose attributes Postgres accepts but the index mapping rejects (<code>age</code> not a number, or an unknown attribute). Expect them in the DLQ (Control tab) while everything else keeps flowing (G4).</p>
      <form class="row" @submit.prevent="run('bad records inserted', () => api('/sim/bad-records', { method: 'POST', body: { count: badCount } }))">
        <label>count <input v-model.number="badCount" type="number" min="1" max="100" /></label>
        <button class="primary" :disabled="busy">Insert</button>
      </form>
    </section>

    <section class="card">
      <h2>Generate source changes</h2>
      <p class="small muted">A burst of changes in Postgres. Watch the lag rise and fall on the Status tab and the rows arrive on the Records tab.</p>
      <form class="row" @submit.prevent="run('changes generated', () => api('/sim/changes', { method: 'POST', body: { updates, inserts, deletes } }))">
        <label>updates <input v-model.number="updates" type="number" min="0" max="100000" /></label>
        <label>inserts <input v-model.number="inserts" type="number" min="0" max="10000" /></label>
        <label>deletes <input v-model.number="deletes" type="number" min="0" max="10000" /></label>
        <button class="primary" :disabled="busy">Generate</button>
      </form>
    </section>
  </div>
</template>
