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
- Decision or accident?: decision.
