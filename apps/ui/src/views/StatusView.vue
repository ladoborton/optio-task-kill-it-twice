<script setup lang="ts">
import { computed } from 'vue';
import { n, type Status } from '../api';

// SPEC §10 screen 1 — the visual side of G5: where is the backfill, throughput, lag, DLQ, health.
const props = defineProps<{ status?: Status }>();
const time = (iso: string | null) => (iso ? new Date(iso).toLocaleTimeString() : '—');
const s = computed(() => props.status);
const headline = computed(() => {
  switch (s.value?.health.status) {
    case 'ok': return 'Healthy — both sinks are in sync and nothing is waiting';
    case 'degraded': return 'Degraded — running, but something needs attention';
    case 'down': return 'Down — the pipeline is not reporting';
    default: return '';
  }
});
</script>

<template>
  <!-- Loading: reserve the layout so nothing jumps when data arrives. -->
  <div v-if="!s" class="stack" aria-busy="true">
    <div class="skeleton" style="height: 74px" />
    <div class="grid cols-3">
      <div v-for="i in 6" :key="i" class="skeleton" style="height: 150px" />
    </div>
  </div>

  <div v-else class="stack">
    <section :class="['card', 'banner', s.health.status]" aria-live="polite">
      <div class="row between">
        <div class="row">
          <span :class="['badge', 'lg', s.health.status]">{{ s.health.status }}</span>
          <strong>{{ headline }}</strong>
        </div>
        <span class="small muted">updated {{ time(s.generated_at) }} · every 2 s · <a href="/api/status" target="_blank" rel="noopener">raw JSON</a></span>
      </div>
      <ul v-if="s.health.reasons.length" class="reasons">
        <li v-for="r in s.health.reasons" :key="r">{{ r }}</li>
      </ul>
    </section>

    <div class="grid cols-3">
      <section class="card">
        <div class="card-head">
          <h3>Backfill</h3>
          <span :class="['badge', s.backfill.state]">{{ s.backfill.state }}</span>
        </div>
        <div class="big">{{ s.backfill.percent }}<span class="unit"> %</span></div>
        <div :class="['bar', { done: s.backfill.state === 'completed' }]" role="progressbar"
             :aria-valuenow="s.backfill.percent" aria-valuemin="0" aria-valuemax="100" aria-label="Backfill progress">
          <div :style="{ width: s.backfill.percent + '%' }" />
        </div>
        <dl class="kv">
          <dt>checkpoint id</dt><dd>{{ n(s.backfill.position) }}</dd>
          <dt>ends at id</dt><dd>{{ n(s.backfill.total) }}</dd>
          <dt>throughput</dt><dd>{{ n(s.backfill.rate_per_sec) }} rec/s</dd>
        </dl>
      </section>

      <section class="card">
        <div class="card-head">
          <h3>Incremental sync</h3>
          <span :class="['badge', s.incremental.state]">{{ s.incremental.state }}</span>
        </div>
        <div class="big">{{ n(s.incremental.lag_events) }}<span class="unit"> events behind</span></div>
        <dl class="kv" style="margin-top: 12px">
          <dt>lag</dt><dd>{{ s.incremental.lag_seconds }} s</dd>
          <dt>throughput</dt><dd>{{ n(s.incremental.rate_per_sec) }} changes/s</dd>
          <dt>position</dt><dd class="mono">txid {{ n(s.incremental.position.txid) }} · seq {{ n(s.incremental.position.seq) }}</dd>
        </dl>
      </section>

      <section class="card">
        <div class="card-head">
          <h3>Pipeline process</h3>
          <span :class="['badge', s.pipeline.alive ? 'ok' : 'down']">{{ s.pipeline.alive ? 'alive' : 'not responding' }}</span>
        </div>
        <div class="big">{{ s.pipeline.last_beat_age_seconds ?? '—' }}<span class="unit"> s since heartbeat</span></div>
        <dl class="kv" style="margin-top: 12px">
          <dt>instance</dt><dd class="mono">{{ s.pipeline.instance_id ?? '—' }}</dd>
          <dt>started</dt><dd>{{ time(s.pipeline.started_at) }}</dd>
          <dt>declared dead after</dt><dd>10 s without a heartbeat</dd>
        </dl>
      </section>

      <section class="card">
        <div class="card-head"><h3>Sinks — circuit breakers</h3></div>
        <div v-for="(sink, name) in s.sinks" :key="name" class="row between sink">
          <span>{{ name }}</span>
          <span :class="['badge', sink.circuit]">{{ sink.circuit.replace('_', '-') }}</span>
        </div>
        <p class="small muted" style="margin: 10px 0 0">Open = the sink failed 5 times in a row; one probe request every 5 s until it answers.</p>
      </section>

      <section class="card">
        <div class="card-head">
          <h3>Dead letters</h3>
          <span :class="['badge', s.dlq.pending ? 'pending' : 'ok']">{{ s.dlq.pending ? `${n(s.dlq.pending)} pending` : 'none pending' }}</span>
        </div>
        <div class="big">{{ n(s.dlq.pending) }}<span class="unit"> waiting for replay</span></div>
        <dl class="kv" style="margin-top: 12px">
          <dt>index DLQ replayed</dt><dd>{{ n(s.dlq.replayed) }}</dd>
          <dt>index DLQ total</dt><dd>{{ n(s.dlq.total) }}</dd>
          <dt>consumer dead-letter queue</dt><dd>{{ n(s.consumer.dead_lettered) }}</dd>
        </dl>
      </section>

      <section class="card">
        <div class="card-head">
          <h3>Stream consumer</h3>
          <span :class="['badge', s.consumer.consumers ? 'ok' : 'unknown']">{{ s.consumer.consumers ? `${s.consumer.consumers} connected` : 'no consumer' }}</span>
        </div>
        <div class="big">{{ n(s.consumer.applied) }}<span class="unit"> events applied</span></div>
        <dl class="kv" style="margin-top: 12px">
          <dt>duplicates ignored</dt><dd>{{ n(s.consumer.duplicates) }}</dd>
          <dt>queue ready</dt><dd>{{ n(s.consumer.queue_ready) }}</dd>
          <dt>in flight (unacked)</dt><dd>{{ n(s.consumer.queue_unacked) }}</dd>
        </dl>
      </section>
    </div>
  </div>
</template>

<style scoped>
.banner { border-left: 4px solid var(--line-strong); }
.banner.ok { border-left-color: var(--ok); }
.banner.degraded { border-left-color: var(--warn); }
.banner.down { border-left-color: var(--bad); }
.reasons { margin: 10px 0 0; padding-left: 20px; color: var(--text); }
.reasons li + li { margin-top: 2px; }
.sink { padding: 6px 0; border-bottom: 1px solid var(--line); }
.sink:last-of-type { border-bottom: none; }
</style>
