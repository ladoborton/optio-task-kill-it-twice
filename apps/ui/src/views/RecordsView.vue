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
    <div class="stack">
      <section class="card">
        <div class="card-head">
          <h2>Replicated customers</h2>
          <span class="badge neutral">searched in Elasticsearch</span>
        </div>
        <form class="row" style="margin-bottom: 12px" role="search" @submit.prevent="page = 1; search()">
          <input v-model="q" aria-label="Search by name or email" placeholder="name or email, e.g. Customer 42" style="flex: 1; min-width: 200px" />
          <select v-model="city" aria-label="City"><option value="">any city</option><option v-for="c in CITIES" :key="c">{{ c }}</option></select>
          <select v-model="segment" aria-label="Segment"><option value="">any segment</option><option v-for="s in SEGMENTS" :key="s">{{ s }}</option></select>
          <button class="primary">Search</button>
        </form>
        <div v-if="searchError" class="notice error" role="alert">{{ searchError }}</div>
        <div v-if="!result" class="skeleton" style="height: 220px" aria-busy="true" />
        <template v-else>
          <div class="table-wrap">
            <table>
              <thead><tr><th class="num">id</th><th>name</th><th>email</th><th>city</th><th>segment</th><th class="num">balance</th><th class="num">v</th></tr></thead>
              <tbody>
                <tr v-for="c in result.items" :key="c.id" class="clickable" tabindex="0"
                    :aria-label="`Open customer ${c.id}`" @click="open(c.id)" @keydown.enter="open(c.id)">
                  <td class="num mono">{{ c.id }}</td><td>{{ c.name }}</td><td>{{ c.email }}</td><td>{{ c.city }}</td>
                  <td>{{ c.segment }}</td><td class="num">{{ c.balance }}</td><td class="num">{{ c.version }}</td>
                </tr>
              </tbody>
            </table>
            <div v-if="!result.items.length" class="empty">No customers match. Clear the filters, or wait for the backfill to index more.</div>
          </div>
          <div class="row between" style="margin-top: 10px">
            <span class="small muted">{{ n(result.total) }} matches · click a row to compare it across all three stores</span>
            <div class="row">
              <button :disabled="page <= 1" @click="page--">‹ Prev</button>
              <span class="small">page {{ page }}</span>
              <button :disabled="page * result.page_size >= result.total" @click="page++">Next ›</button>
            </div>
          </div>
        </template>
      </section>

      <section v-if="selected" class="card" aria-live="polite">
        <div class="card-head">
          <h2>Customer #{{ selectedId }}</h2>
          <span :class="['badge', inSync(selected) ? 'ok' : 'pending']">{{ inSync(selected) ? 'in sync everywhere' : 'not (yet) in sync' }}</span>
        </div>
        <div class="table-wrap">
          <table>
            <thead><tr><th></th><th>Postgres · source</th><th>Elasticsearch · index</th><th>Consumer · projection</th></tr></thead>
            <tbody>
              <tr v-for="f in fields" :key="f">
                <th scope="row">{{ f }}</th>
                <td>{{ show(selected.source, f) }}</td>
                <td :class="{ mismatch: f === 'version' && show(selected.index, f) !== show(selected.source, f) }">{{ show(selected.index, f) }}</td>
                <td :class="{ mismatch: f === 'version' && show(selected.consumer, f) !== show(selected.source, f) }">{{ show(selected.consumer, f) }}</td>
              </tr>
              <tr><th scope="row">present</th><td>{{ selected.source ? 'yes' : 'deleted' }}</td><td>{{ selected.index ? 'yes' : 'no' }}</td><td>{{ selected.consumer ? (selected.consumer.deleted ? 'deleted' : 'yes') : 'no' }}</td></tr>
            </tbody>
          </table>
        </div>
        <p class="small muted" style="margin: 8px 0 0">The consumer's projection stores only id, version, email, city and segment — "—" elsewhere is expected.</p>
        <div v-if="selected.dlq.length" style="margin-top: 12px">
          <h3 style="margin-bottom: 6px">DLQ entries for this customer</h3>
          <div v-for="d in selected.dlq" :key="d.id" class="row small" style="margin-top: 4px">
            <span :class="['badge', d.status]">{{ d.status }}</span> v{{ d.version }} · {{ d.error_type }} · attempts {{ d.attempts }}
          </div>
        </div>
      </section>
    </div>

    <section class="card">
      <div class="card-head">
        <h2>Live source changes</h2>
        <span class="badge neutral">outbox · newest first</span>
      </div>
      <div class="table-wrap scroll">
        <table>
          <thead><tr><th class="num">seq</th><th>op</th><th class="num">customer</th><th class="num">v</th><th>state</th></tr></thead>
          <tbody>
            <tr v-for="c in changes ?? []" :key="c.seq" class="clickable" tabindex="0"
                :aria-label="`Open customer ${c.customer_id}`" @click="open(c.customer_id)" @keydown.enter="open(c.customer_id)">
              <td class="num mono">{{ c.seq }}</td>
              <td>{{ c.op }}</td>
              <td class="num mono">#{{ c.customer_id }}</td>
              <td class="num">{{ c.version }}</td>
              <td><span :class="['badge', c.shipped ? 'ok' : 'waiting']">{{ c.shipped ? 'shipped' : 'waiting' }}</span></td>
            </tr>
          </tbody>
        </table>
        <div v-if="changes && !changes.length" class="empty">No changes yet — generate some on the Simulation tab.</div>
        <div v-if="!changes" class="skeleton" style="height: 200px; margin: 8px" aria-busy="true" />
      </div>
    </section>
  </div>
</template>
