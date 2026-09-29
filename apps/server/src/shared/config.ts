// All runtime configuration comes from env vars, with defaults documented here (AGENTS.md §7).

function required(name: string): string {
  const value = process.env[name];
  if (!value) throw new Error(`Missing env var ${name}`);
  return value;
}

function int(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) throw new Error(`Env var ${name} must be a positive integer, got "${raw}"`);
  return value;
}

export const config = {
  role: process.env.ROLE ?? 'unknown',
  databaseUrl: required('DATABASE_URL'),
  esUrl: process.env.ES_URL ?? 'http://elasticsearch:9200',
  esIndex: process.env.ES_INDEX ?? 'customers',
  // A bulk of 500 docs normally takes well under a second; a hung sink must not hang the loop.
  esRequestTimeoutMs: int('ES_REQUEST_TIMEOUT_MS', 30_000),
  rabbitmqUrl: process.env.RABBITMQ_URL ?? 'amqp://optio:optio@rabbitmq:5672',
  // Publisher confirms for one batch; a broker that never answers must not hang the loop.
  streamConfirmTimeoutMs: int('STREAM_CONFIRM_TIMEOUT_MS', 30_000),
  // SPEC §7.3: after this many redeliveries (e.g. a message that crashes the consumer every time)
  // the queue dead-letters the message instead of looping forever.
  deliveryLimit: int('DELIVERY_LIMIT', 5),
  // Consumer: unacked messages in flight, and how many are applied per Postgres transaction.
  consumerPrefetch: int('CONSUMER_PREFETCH', 1_000),
  consumerBatchSize: int('CONSUMER_BATCH_SIZE', 500),
  // Flush a partial batch after this long, so a quiet stream is not held back waiting for 500.
  consumerFlushMs: int('CONSUMER_FLUSH_MS', 50),
  // SPEC §7.4: DLQ replay requests handled per step, and how often to look for new ones.
  dlqReplayBatch: int('DLQ_REPLAY_BATCH', 100),
  dlqReplayPollMs: int('DLQ_REPLAY_POLL_MS', 1_000),
  // SPEC §7.2: retry delays grow 0.5 s → 1 → 2 → 4 → 5 s. The cap is short on purpose (D-005): a
  // down sink is protected by its circuit breaker, not by loops sleeping for long.
  retryBaseMs: int('RETRY_BASE_MS', 500),
  retryMaxMs: int('RETRY_MAX_MS', 5_000),
  // SPEC §7.2: consecutive failures that open a sink's circuit, and how long it stays open before
  // one probe request is let through.
  breakerThreshold: int('BREAKER_THRESHOLD', 5),
  breakerOpenMs: int('BREAKER_OPEN_MS', 5_000),
  // SPEC §4.8: 1M rows by default.
  seedCount: int('SEED_COUNT', 1_000_000),
  // Rows per INSERT statement during seed; keeps each statement (and its WAL) bounded.
  seedChunk: int('SEED_CHUNK', 100_000),
};
