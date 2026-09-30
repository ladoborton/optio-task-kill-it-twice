<script setup lang="ts">
import { ref } from 'vue';
import { api, usePoll, type Status } from './api';
import ControlView from './views/ControlView.vue';
import RecordsView from './views/RecordsView.vue';
import SimulationView from './views/SimulationView.vue';
import StatusView from './views/StatusView.vue';

// SPEC §10: four screens. The health line stays visible on all of them.
const tabs = [
  { id: 'status', label: 'Status' },
  { id: 'records', label: 'Records' },
  { id: 'control', label: 'Control' },
  { id: 'simulation', label: 'Simulation' },
] as const;
const tab = ref<(typeof tabs)[number]['id']>('status');

const { data: status, error } = usePoll(() => api<Status>('/status'));
</script>

<template>
  <header class="header">
    <div class="page top">
      <div class="brand">
        <span class="mark" aria-hidden="true" />
        <div>
          <strong>Kill It Twice</strong>
          <div class="sub">Postgres → Elasticsearch + RabbitMQ · replication console</div>
        </div>
      </div>
      <div class="health" aria-live="polite">
        <template v-if="status">
          <span :class="['badge', 'lg', status.health.status]">{{ status.health.status }}</span>
          <span class="reasons">{{ status.health.reasons.join(' · ') || 'all systems nominal' }}</span>
        </template>
        <span v-else-if="error" class="badge lg down">api unreachable</span>
        <span v-else class="badge lg neutral">connecting…</span>
      </div>
    </div>
    <nav class="page tabs" role="tablist" aria-label="Console sections">
      <button
        v-for="t in tabs"
        :key="t.id"
        role="tab"
        :aria-selected="tab === t.id"
        :class="{ active: tab === t.id }"
        @click="tab = t.id"
      >{{ t.label }}</button>
    </nav>
  </header>

  <main class="page" role="tabpanel">
    <div v-if="error" class="notice error" role="alert">Cannot reach the api: {{ error }}</div>
    <StatusView v-if="tab === 'status'" :status="status" />
    <RecordsView v-else-if="tab === 'records'" />
    <ControlView v-else-if="tab === 'control'" :status="status" />
    <SimulationView v-else />
  </main>
</template>

<style scoped>
.header { background: var(--header); border-bottom: 1px solid var(--line); position: sticky; top: 0; z-index: 10; }
.top { display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; padding-bottom: 12px; }
.brand { display: flex; align-items: center; gap: 12px; }
.brand strong { font-size: 16px; letter-spacing: -0.01em; }
.sub { color: var(--muted); font-size: 12px; }
.mark { width: 28px; height: 28px; border-radius: 8px; background: linear-gradient(135deg, #2563eb, #22c55e); box-shadow: 0 0 0 1px rgba(255, 255, 255, 0.08) inset; }
.health { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
.reasons { color: var(--muted); font-size: 12.5px; max-width: 560px; }
.tabs { display: flex; gap: 4px; padding-top: 0; padding-bottom: 0; }
.tabs button {
  background: transparent; border: none; border-radius: 0; color: var(--muted);
  padding: 10px 14px; min-height: 40px; border-bottom: 2px solid transparent;
}
.tabs button:hover:not(.active) { color: var(--text); border-bottom-color: var(--line-strong); }
.tabs button.active { color: #fff; border-bottom-color: var(--focus); }
</style>
