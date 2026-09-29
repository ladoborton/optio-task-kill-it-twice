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
    <div class="page row" style="justify-content: space-between">
      <div class="row">
        <strong>Kill It Twice</strong>
        <span class="muted small">Postgres → Elasticsearch + RabbitMQ</span>
      </div>
      <div class="row">
        <template v-if="status">
          <span :class="['badge', status.health.status]">{{ status.health.status }}</span>
          <span class="small muted">{{ status.health.reasons.join(' · ') || 'all good' }}</span>
        </template>
        <span v-else-if="error" class="badge down">api unreachable</span>
      </div>
    </div>
    <nav class="page row tabs">
      <button v-for="t in tabs" :key="t.id" :class="{ active: tab === t.id }" @click="tab = t.id">{{ t.label }}</button>
    </nav>
  </header>

  <main class="page">
    <div v-if="error" class="notice error">api: {{ error }}</div>
    <StatusView v-if="tab === 'status'" :status="status" />
    <RecordsView v-else-if="tab === 'records'" />
    <ControlView v-else-if="tab === 'control'" :status="status" />
    <SimulationView v-else />
  </main>
</template>

<style scoped>
.header { background: #1c2330; color: #fff; }
.header .page { padding-top: 10px; padding-bottom: 10px; }
.header .muted { color: #aab2c3; }
.tabs { padding-top: 0 !important; gap: 4px; }
.tabs button { background: transparent; border: none; color: #aab2c3; border-radius: 6px 6px 0 0; }
.tabs button.active { background: var(--bg); color: var(--text); }
.tabs button:hover:not(.active) { color: #fff; }
</style>
