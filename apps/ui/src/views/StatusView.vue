<script setup lang="ts">
import { n, type Status } from '../api';

// SPEC §10 screen 1 — the visual side of G5: where is the backfill, throughput, lag, DLQ, health.
defineProps<{ status?: Status }>();
const since = (iso: string | null) => (iso ? new Date(iso).toLocaleTimeString() : '—');
</script>

<template>
  <div v-if="!status" class="muted">Loading…</div>
  <template v-else>
    <div class="grid">
      <section class="card">
        <h3>Health</h3>
        <div class="row">
          <span :class="['badge', status.health.status]" style="font-size: 16px">{{ status.health.status }}</span>
        </div>
        <ul v-if="status.health.reasons.length" class="small">
          <li v-for="r in status.health.reasons" :key="r">{{ r }}</li>
        </ul>
        <p v-else class="small muted">Pipeline alive, circuits closed, lag within bounds, no pending DLQ records.</p>
      </section>

      <section class="card">
        <h3>Pipeline process</h3>
        <div class="row">
          <span :class="['badge', status.pipeline.alive ? 'ok' : 'down']">{{ status.pipeline.alive ? 'alive' : 'not responding' }}</span>
          <span class="small muted">last heartbeat {{ status.pipeline.last_beat_age_seconds ?? '—' }}s ago</span>
        </div>
        <p class="small muted">
          instance <code>{{ status.pipeline.instance_id ?? '—' }}</code><br />
          started {{ since(status.pipeline.started_at) }}
        </p>
      </section>

      <section class="card">
        <h3>Backfill</h3>
        <div class="row" style="justify-content: space-between">
          <span :class="['badge', status.backfill.state]">{{ status.backfill.state }}</span>
          <span class="big">{{ status.backfill.percent }}%</span>
        </div>
        <div class="bar"><div :style="{ width: status.backfill.percent + '%' }" /></div>
        <div class="small muted">
          checkpoint id {{ n(status.backfill.position) }} of {{ n(status.backfill.total) }} ·
          {{ n(status.backfill.rate_per_sec) }} rec/s
        </div>
      </section>

      <section class="card">
        <h3>Incremental sync</h3>
        <div class="row" style="justify-content: space-between">
          <span :class="['badge', status.incremental.state]">{{ status.incremental.state }}</span>
          <span class="big">{{ n(status.incremental.lag_events) }} <span class="small muted">events behind</span></span>
        </div>
        <div class="small muted">
          lag {{ status.incremental.lag_seconds }}s · {{ n(status.incremental.rate_per_sec) }} changes/s<br />
          position txid {{ n(status.incremental.position.txid) }} / seq {{ n(status.incremental.position.seq) }}
        </div>
      </section>

      <section class="card">
        <h3>Sinks (circuit breakers)</h3>
        <div v-for="(sink, name) in status.sinks" :key="name" class="row" style="justify-content: space-between; margin-bottom: 6px">
          <span>{{ name }}</span>
          <span :class="['badge', sink.circuit]">{{ sink.circuit.replace('_', '-') }}</span>
        </div>
      </section>

      <section class="card">
        <h3>Dead letters</h3>
        <div class="row" style="justify-content: space-between">
          <span>index DLQ pending</span><span :class="['badge', status.dlq.pending ? 'pending' : 'ok']">{{ n(status.dlq.pending) }}</span>
        </div>
        <div class="small muted">{{ n(status.dlq.replayed) }} replayed · {{ n(status.dlq.total) }} total</div>
        <div class="row" style="justify-content: space-between; margin-top: 8px">
          <span>consumer dead-letter queue</span><span class="badge unknown" v-if="status.consumer.dead_lettered">{{ n(status.consumer.dead_lettered) }}</span><span v-else class="badge ok">0</span>
        </div>
      </section>

      <section class="card">
        <h3>Stream consumer</h3>
        <div class="big">{{ n(status.consumer.applied) }} <span class="small muted">events applied</span></div>
        <div class="small muted">
          {{ n(status.consumer.duplicates) }} duplicates ignored (at-least-once replays)<br />
          queue: {{ n(status.consumer.queue_ready) }} ready · {{ n(status.consumer.queue_unacked) }} in flight ·
          {{ status.consumer.consumers }} consumer(s)
        </div>
      </section>
    </div>
    <p class="small muted">Updated {{ since(status.generated_at) }} · refreshes every 2 s · raw: <a href="/api/status" target="_blank">/api/status</a></p>
  </template>
</template>
