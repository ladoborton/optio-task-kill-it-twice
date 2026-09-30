# Deviations log

Every place where the implementation differs from `SPEC.md`, recorded when it happens — not afterwards.
Entries feed the README section "Where the AI deviated from the spec".

Kinds:
- **agent** — the coding agent did something other than what the SPEC/task said.
- **spec** — the SPEC turned out to be wrong or incomplete once built or measured.
- **scope** — deliberately not built / built differently because of time.

## Entry format

```
### D-NNN — short title
- Date / slice:
- Kind: agent | spec | scope
- SPEC said:
- What happened:
- Why it was wrong / insufficient:
- Resolution: (code change, SPEC change vX.Y, or accepted as is)
- Decision or accident?:
```

## Entries

### D-001 — A reset can race a running pipeline
- Date / slice: 2026-09-26 / S2 (spotted by the agent while handing over S1)
- Kind: spec
- SPEC said: §4.8 seed "resets pipeline state"; §6.2 "advance checkpoint" after the sinks ack. Both
  silently assume the pipeline is the only writer of `pipeline_checkpoints` and nobody touches the index.
- What happened: nothing yet — found by reading, before it could bite. A seed (or any reset) while a batch
  is in flight would (a) be overwritten by that batch's checkpoint write, so the new backfill would skip
  everything below the old position, and (b) the batch's bulk write would auto-create the deleted index
  with dynamic mapping, losing `dynamic: strict` (SPEC §4.5).
- Why it was wrong / insufficient: "the checkpoint only moves after the sinks ack" protects against
  crashes, not against a second writer.
- Resolution:
  1. `CheckpointRepository.advance/complete` are compare-and-set: `WHERE position = :from`. If the row moved,
     the batch is dropped and the loop re-reads the checkpoint (`backfill.checkpoint_moved` in the logs).
     Verified by resetting the checkpoint by hand mid-batch.
  2. `seed.sh` stops the pipeline before seeding and starts it afterwards.
  3. Elasticsearch runs with `action.auto_create_index=false`; only the pipeline creates the index.
  Recorded in SPEC v1.2 (§4.8, §6.2).
  Extended in S3: the consumer's projection and dedup record hold versions just like the index, so
  seed also truncates `consumer.*`, purges the queue, and `seed.sh` stops the consumer too.
- Decision or accident?: decision.

### D-002 — The consumer applies events in batches, not one by one
- Date / slice: 2026-09-28 / S3
- Kind: spec
- SPEC said: §6.3 "in one Postgres transaction it inserts into `consumer_applied` … updates
  `consumer_customers` … then acks" — written as if per message.
- What happened: the agent noticed before implementing that one commit per message means ~1M commits for a
  backfill (≈ 15–30 min at ~1 ms per commit on Docker Desktop), which would make G2 impractically slow.
- Why it was wrong / insufficient: the SPEC described the atomicity rule but not its granularity.
- Resolution: messages are grouped (up to 500, or whatever arrived within 50 ms) and each group is applied in
  one transaction — `INSERT … ON CONFLICT DO NOTHING RETURNING` for dedup, one version-guarded upsert, one
  counter update — then acknowledged with a single `ack(last, multiple=true)`. The guarantee is unchanged:
  nothing is acked before its transaction commits. Measured: the consumer keeps pace with the pipeline
  (~7k events/s). Table names also differ from the SPEC sketch: `consumer.applied_events`,
  `consumer.customers`, `consumer.stats` (schema `consumer`).
- Decision or accident?: decision.

### D-003 — The SPEC's outbox gap rule would lose changes
- Date / slice: 2026-09-28 / S4
- Kind: spec
- SPEC said: §5.4 "only consume up to the first gap; if a gap persists longer than 5 s, treat it as a
  rolled-back transaction and skip it" (flagged as open question §14.1).
- What happened: before implementing, the agent showed that a gap is not only a rollback — it is also a
  transaction that took its `seq` early and has not committed yet. A transaction open longer than 5 s
  would be skipped and its change lost for good. Reproduced by hand: with transaction A (seq 180723)
  still open and B (seq 180724) committed, a `seq > N` reader returns only 180724.
- Why it was wrong / insufficient: `seq` order is allocation order, not commit order; no timeout can
  tell "rolled back" from "slow".
- Resolution (chosen by the human over keeping v1): the outbox records the writing transaction's id
  (`txid`, migration `OutboxTxid1727500000000`); the incremental loop reads only rows with
  `txid < pg_snapshot_xmin(pg_current_snapshot())` — i.e. of finished transactions — in `(txid, seq)`
  order, and the checkpoint is the pair `(position_txid, position)`. A long transaction now delays the
  loop (lag grows) instead of losing data. G2 was extended with an 8-second transaction on an
  already-backfilled customer, so only the incremental loop can deliver it. SPEC v1.3.
- Decision or accident?: decision.

### D-004 — How a DLQ replay is triggered and where it goes
- Date / slice: 2026-09-29 / S5
- Kind: spec
- SPEC said: §7.4 "Replay … re-reads the current source row … and sends it through the same sink path";
  §9 `POST /api/dlq/replay`; §6.2 order "ES → RabbitMQ → DLQ rows → checkpoint". It did not say who
  performs a replay or how a request survives a crash, and "the same sink path" is ambiguous.
- What happened: there is no api yet (S7+), and G4 needs a replay.
- Resolution:
  1. A replay is **requested in Postgres** (`dlq_records.replay_requested_at`, new migration) and carried
     out by a third pipeline loop (`DlqReplayLoop`) — the same desired-state pattern as `pipeline_control`.
     The api will only set that column.
  2. A replay goes **only to the sink that rejected the record** (ES). The stream never rejected it, and a
     fix made in the source reaches the stream through the incremental loop anyway.
  3. Order inside a batch is ES → DLQ rows → RabbitMQ → checkpoint. Equivalent for the guarantee (both
     before the checkpoint), and a rejection is persisted as early as possible.
  4. `sink = 'stream'` DLQ rows are never produced: RabbitMQ does not reject individual messages that the
     broker accepted; the consumer's poison messages go to the RabbitMQ dead-letter queue instead.
- Decision or accident?: decision.

### D-005 — Breaker and backoff timings; G3 did not fail first
- Date / slice: 2026-09-29 / S6
- Kind: spec (timings) and process (gate-first)
- SPEC said: §7.2 backoff "0.5 s → 1 → 2 → 4 … capped at 30 s"; breaker "5 consecutive failures ⇒ open
  for 15 s, then one probe". AGENTS §2: a slice's gate must FAIL before the feature exists.
- What happened: G3 passed on the S5 code already — per-loop backoff (built in S2) was enough for 0 lost
  and no busy loop. The agent first reported a 89 s recovery as the problem the breaker would fix; that
  number was not reproducible (later backoff-only runs: first write 3.8 s and 8.5 s after recovery), and
  part of it was verify's own measurement (it counted its "3 quiet polls" as recovery time). The claim was
  withdrawn and the measurement fixed (convergence time = start of the quiet streak; "resumed" = first
  successful write) before anything was committed.
- Why it matters: the real effect of the breaker is a bounded worst case, not a better average. With a
  30 s backoff cap, a loop can sleep up to 30 s after the sink is back; with a breaker every waiting loop
  wakes at the next probe (one request per interval for all loops together).
- Resolution: breaker per sink (ES, RabbitMQ) shared by all loops; open interval **5 s** (not 15 s) and
  backoff cap **5 s** (not 30 s) — a down sink is protected by the breaker's single probe, so long per-loop
  sleeps only delay recovery. G3 bound: writing resumes ≤ 15 s after the index is healthy. Measured with
  the breaker: 0.6–3.7 s to resume, 19–21 s to drain a one-minute backlog. SPEC v1.5.
- Decision or accident?: timings — decision; the 89 s claim — an agent mistake, caught by measuring
  again before committing.

### D-006 — /api/status reads only durable state
- Date / slice: 2026-09-30 / S7
- Kind: spec
- SPEC said: §8.2 status is "built from durable facts in Postgres … plus live pipeline metrics when
  reachable"; §4.7 heartbeat columns without breaker state.
- What happened: scraping the pipeline from the api would make the status depend on the process whose
  death it must report, and duplicate the "where are we" logic in two places.
- Resolution: the heartbeat also stores each sink's breaker state (`pipeline_heartbeat.details`, new
  migration); throughput comes from the heartbeat's rates. `/api/status` reads Postgres + the RabbitMQ
  management API only. One SQL (`readPipelineFacts`) defines position/lag/DLQ for both the heartbeat's
  gauges and the api. While the pipeline is dead, breaker states are reported as `unknown` and health is
  `down`. The consumer's RabbitMQ dead-letter queue is shown but does not degrade health (the SPEC's
  health rules name only `dlq_records`).
  Also found by G5: labelled Prometheus series did not exist until the first batch after a restart; they
  are now initialised to 0.
- Decision or accident?: decision.

### D-007 — Simulation and UI details
- Date / slice: 2026-09-30 / S8
- Kind: spec
- SPEC said: §9 `POST /api/sim/sinks/:name/{stop|start}`; §10 UI talks to the api.
- What happened / resolution:
  - Simulation is `POST /api/sim/services/:service/{stop|start|kill}` for elasticsearch, rabbitmq, **pipeline
    and consumer** — the UI can reproduce G1 (kill the pipeline) and G2 (kill the consumer), not only sink
    outages. `kill` is SIGKILL; `stop` is graceful. Allowed services are whitelisted; the api never stops
    itself or the databases.
  - The UI container is nginx serving the built Vue app and proxying `/api` to the api service: one origin,
    no CORS, no api host baked into the bundle.
  - Records detail shows the source row, the index document and the consumer projection side by side; the
    projection intentionally stores only id/version/deleted/email/city/segment, so e.g. balance shows "—".
  - The UI has no way to edit source data: the source is the client's system. Fixing a bad record before a
    replay is done in Postgres (as a client would), then "Replay" in the UI.
- Decision or accident?: decision.

### D-008 — Two loops could each open a RabbitMQ connection
- Date / slice: 2026-09-30 / final review
- Kind: agent
- SPEC said: nothing about connection handling; the agent's S3 `StreamSink` connected lazily with
  `if (!channel) connect()` — an `await` between the check and the assignment.
- What happened: found in the pre-submission review, not by a gate. When backfill and incremental both had
  work at startup, both saw "no channel" and each opened a connection; the second assignment orphaned the
  first, which was never used or closed. Reproduced by counting the pipeline's connections in the RabbitMQ
  management API after restarts under load: 2 in 2 of 3 runs.
- Why it was wrong: a check-then-act across an `await` is a race even in single-threaded Node.
- Resolution: the connection attempt is a shared promise — every caller waits for the same attempt; a
  connection whose channel/topology setup fails is closed. After the fix: 1 connection in 5 of 5 restarts
  under load, and 1 after a broker restart mid-backfill. No data was at risk (publisher confirms), only a
  leaked connection per occurrence.
- Decision or accident?: accident — an agent bug, fixed.

### D-009 — A cold `docker compose build` failed
- Date / slice: 2026-09-30 / clean-clone test
- Kind: agent
- SPEC said: §5.1 "one Docker image, started in roles".
- What happened: the agent gave the five server services (migrate, seed, pipeline, consumer, api) the same
  `build` and the same `image: kill-it-twice/server` tag. With a warm build cache this never showed. In the
  clean-clone test (fresh clone from GitHub, all containers, volumes and images removed,
  `docker compose build --no-cache`) Compose built the five services in parallel and they raced to export the
  one tag: `image "kill-it-twice/server:latest": already exists`, exit 1. A reviewer's first
  `docker compose up -d --build` has no cache either.
- Why it was wrong: one tag written by several concurrent builds is a race; "it works on my machine" was the
  cache.
- Resolution: no shared `image:` name — each service builds the same Dockerfile, BuildKit builds the layers
  once and the images share them. Verified in the clone: two cold builds in a row, exit 0, ~40 s each; then
  `up --build`, `seed.sh` and the full `verify.sh` from the clone. SPEC v1.7 §5.1 wording adjusted.
- Decision or accident?: accident — found by the clean-clone test, which is why it was run.
