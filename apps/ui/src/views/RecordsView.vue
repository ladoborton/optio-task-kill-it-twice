<script setup lang="ts">
import { ref, watch } from 'vue';
import { api, n, usePoll } from '../api';

// SPEC §10 screen 2: the replicated records (searched in the index), one record in all three places,
// and the live stream of source changes.

interface Customer { id: number; name: string; email: string; city: string; segment: string; balance: number; version: number }
interface Page { total: number; page: number; page_size: number; items: Customer[] }
interface Detail {
  source: Record<string, unknown> | null;
  index: Record<string, unknown> | null;
  consumer: Record<string, unknown> | null;
  dlq: { id: string; version: string; status: string; attempts: number; error_type: string; error_reason: string }[];
}
interface Change { seq: string; customer_id: string; op: string; version: string; created_at: string; shipped: boolean }

const CITIES = ['Tbilisi', 'Batumi', 'Kutaisi', 'Rustavi', 'Zugdidi', 'Gori', 'Telavi', 'Poti'];
const SEGMENTS = ['retail', 'vip', 'churn-risk', 'new'];

const q = ref('');
const city = ref('');
const segment = ref('');
const page = ref(1);
const result = ref<Page>();
const searchError = ref('');
const selected = ref<Detail>();
const selectedId = ref<number>();

async function search() {
  try {
    const params = new URLSearchParams({ q: q.value, city: city.value, segment: segment.value, page: String(page.value) });
    result.value = await api<Page>(`/customers?${params}`);
    searchError.value = '';
  } catch (e) {
    searchError.value = (e as Error).message;
  }
}
watch([city, segment], () => { page.value = 1; void search(); });
watch(page, () => void search());
void search();

async function open(id: number | string) {
  selectedId.value = Number(id);
  selected.value = await api<Detail>(`/customers/${id}`).catch(() => undefined);
}
// Re-read the open record while it is shown, so an arriving change is visible.
usePoll(async () => (selectedId.value ? open(selectedId.value) : undefined));

const { data: changes } = usePoll(() => api<Change[]>('/changes?limit=40'));

const version = (row: Record<string, unknown> | null, key = 'version') => (row ? Number(row[key]) : null);
const fields = ['version', 'email', 'city', 'segment', 'balance'];
const show = (row: Record<string, unknown> | null, f: string) =>
  row ? (f === 'version' && row._version !== undefined ? `${row._version}` : String(row[f] ?? '—')) : '—';
const inSync = (d: Detail) => {
  const v = version(d.source);
  const iv = d.index ? Number(d.index._version) : null;
  const cv = d.consumer && !d.consumer.deleted ? version(d.consumer) : null;
  return v === iv && v === cv;
};
</script>

<template>
  <div class="split">
    <div>
      <section class="card">
        <h2>Replicated customers <span class="small muted">(search runs against Elasticsearch)</span></h2>
        <form class="row" style="margin-bottom: 10px" @submit.prevent="page = 1; search()">
          <input v-model="q" placeholder="name or email, e.g. Customer 42" style="flex: 1; min-width: 200px" />
          <select v-model="city"><option value="">any city</option><option v-for="c in CITIES" :key="c">{{ c }}</option></select>
          <select v-model="segment"><option value="">any segment</option><option v-for="s in SEGMENTS" :key="s">{{ s }}</option></select>
          <button class="primary">Search</button>
        </form>
        <div v-if="searchError" class="notice error">{{ searchError }}</div>
        <template v-if="result">
          <table>
            <thead><tr><th>id</th><th>name</th><th>email</th><th>city</th><th>segment</th><th>balance</th><th>v</th></tr></thead>
            <tbody>
              <tr v-for="c in result.items" :key="c.id" class="clickable" @click="open(c.id)">
                <td class="mono">{{ c.id }}</td><td>{{ c.name }}</td><td>{{ c.email }}</td><td>{{ c.city }}</td>
                <td>{{ c.segment }}</td><td>{{ c.balance }}</td><td>{{ c.version }}</td>
              </tr>
            </tbody>
          </table>
          <div class="row" style="margin-top: 8px; justify-content: space-between">
            <span class="small muted">{{ n(result.total) }} matches</span>
            <div class="row">
              <button :disabled="page <= 1" @click="page--">‹ prev</button>
              <span class="small">page {{ page }}</span>
              <button :disabled="page * result.page_size >= result.total" @click="page++">next ›</button>
            </div>
          </div>
        </template>
      </section>

      <section v-if="selected" class="card" style="margin-top: 12px">
        <div class="row" style="justify-content: space-between">
          <h2>Customer #{{ selectedId }}</h2>
          <span :class="['badge', inSync(selected) ? 'ok' : 'pending']">{{ inSync(selected) ? 'in sync everywhere' : 'not (yet) in sync' }}</span>
        </div>
        <table>
          <thead><tr><th></th><th>Postgres (source)</th><th>Elasticsearch (index)</th><th>Consumer projection</th></tr></thead>
          <tbody>
            <tr v-for="f in fields" :key="f">
              <th>{{ f }}</th>
              <td>{{ show(selected.source, f) }}</td>
              <td :class="{ mismatch: f === 'version' && show(selected.index, f) !== show(selected.source, f) }">{{ show(selected.index, f) }}</td>
              <td :class="{ mismatch: f === 'version' && show(selected.consumer, f) !== show(selected.source, f) }">{{ show(selected.consumer, f) }}</td>
            </tr>
            <tr><th>present</th><td>{{ selected.source ? 'yes' : 'deleted' }}</td><td>{{ selected.index ? 'yes' : 'no' }}</td><td>{{ selected.consumer ? (selected.consumer.deleted ? 'deleted' : 'yes') : 'no' }}</td></tr>
          </tbody>
        </table>
        <div v-if="selected.dlq.length" style="margin-top: 10px">
          <h3>DLQ entries for this customer</h3>
          <div v-for="d in selected.dlq" :key="d.id" class="small">
            <span :class="['badge', d.status]">{{ d.status }}</span> v{{ d.version }} · {{ d.error_type }} · attempts {{ d.attempts }}
          </div>
        </div>
      </section>
    </div>

    <section class="card">
      <h2>Live source changes <span class="small muted">(outbox, newest first)</span></h2>
      <div class="scroll">
        <table>
          <thead><tr><th>seq</th><th>op</th><th>customer</th><th>v</th><th>shipped</th></tr></thead>
          <tbody>
            <tr v-for="c in changes ?? []" :key="c.seq" class="clickable" @click="open(c.customer_id)">
              <td class="mono">{{ c.seq }}</td>
              <td>{{ c.op }}</td>
              <td class="mono">#{{ c.customer_id }}</td>
              <td>{{ c.version }}</td>
              <td><span :class="['badge', c.shipped ? 'ok' : 'pending']">{{ c.shipped ? 'shipped' : 'waiting' }}</span></td>
            </tr>
          </tbody>
        </table>
        <p v-if="changes && !changes.length" class="muted small">No changes yet — generate some on the Simulation tab.</p>
      </div>
    </section>
  </div>
</template>
