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
