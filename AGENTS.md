# AGENTS.md — instructions for coding agents in this repo

You are helping build a crash-tolerant replication pipeline (Postgres → Elasticsearch + RabbitMQ).
The human owner reviews every step and must be able to explain every line in a technical interview.
Optimise for **correctness and explainability**, not for volume of code.

## 1. Read first

- `SPEC.md` is the source of truth. Before touching code, read the SPEC sections the task refers to.
- `docs/DEVIATIONS.md` lists every known place where the code differs from the SPEC, and why.
- If the task, the SPEC and the code disagree: **stop and ask**. Do not pick one silently.

## 2. How work is done: slices

Work happens in vertical slices (S1, S2, …), each introducing one concept. For every slice:

1. **Brief** — state the goal, the SPEC sections involved, the files you expect to touch.
2. **Gate first** — add or extend the relevant check in `verify/` and show it **FAIL** on the current code.
3. **Implement** — the smallest change that makes the gate pass without weakening it.
4. **Walk through** — point at the lines where a crash, outage or bad record matters and say what happens there.
5. **Hand over** — give the human the exact commands to break it by hand (`docker kill …`, SQL, etc.).
6. **Commit only after the human says ok.** Never push without explicit approval.

## 3. Repo map

```
SPEC.md                 what to build and why (changes need human approval)
AGENTS.md               this file
README.md               user-facing docs, ADRs, capacity notes, gate table
docs/DEVIATIONS.md      spec ↔ code deviations log
apps/server/            NestJS, one codebase started in roles via ROLE=pipeline|consumer|api
  src/roles/pipeline/   backfill loop, incremental loop, sinks, checkpoints, /metrics
  src/roles/consumer/   RabbitMQ consumer with its own projection + dedup
  src/roles/api/        REST for UI and verify: status, control, data, simulation
  src/shared/           config, logging, ES/RabbitMQ clients, backoff, circuit breaker
  src/database/         TypeORM data source, entities/, migrations/
  src/tools/            one-shot roles: migrate, seed
apps/ui/                Vue 3 + Vite, polling every 2 s
verify/                 gate runner, runs in a container with the Docker socket mounted
docker-compose.yml      whole system; `migrate` is a one-shot service the roles depend on
Makefile, seed.sh, verify.sh   thin wrappers around `docker compose run`
```

Parts of this tree do not exist yet; create them in the slice that needs them, following this layout.

## 4. Commands

| Purpose | Command |
|---------|---------|
| start everything | `docker compose up -d --build` |
| seed 1M customers | `./seed.sh` (or `make seed`) |
| all gates | `./verify.sh` (or `make verify`) |
| one gate | `./verify.sh G1` |
| logs of a role | `docker compose logs -f pipeline` |
| typecheck (all workspaces) / server unit tests | `npm run typecheck`, `npm test -w apps/server` |

The host has no `make` and no local Postgres/Node requirement is assumed for reviewers — everything
that matters must work through Docker. `.sh` files must stay LF (enforced by `.gitattributes`).

## 5. Invariants — never break these

These are the guarantees the gates prove. A change that violates one is wrong even if a gate still passes.

1. **Checkpoint after sinks.** A checkpoint advances only when every record of the batch is acknowledged by
   each sink or durably stored in `dlq_records` for that sink (SPEC §6.2).
2. **ES writes are idempotent.** `_id = customer id`, `version_type=external`, `version = customers.version`;
   `409 version_conflict` counts as success (SPEC §5.5).
3. **Consumer applies atomically.** Dedup insert + projection update + ack decision happen in one
   transaction; projection updates are `WHERE version < :v` (SPEC §6.3).
4. **Never load the table.** Keyset pagination, bounded batches. Do not raise the pipeline `mem_limit`
   (256 MB) to make something pass.
5. **No busy loops.** Every retry goes through the shared backoff / circuit breaker (SPEC §7.2).
6. **A bad record never blocks the batch.** Per-item classification; permanent failures go to DLQ (SPEC §7.3).
7. **Control state lives in Postgres** (`pipeline_control`), not in process memory.

## 6. Data access rules (TypeORM) — SPEC §5.6

- In `pipeline` and `consumer`: **no `repository.save()`, no `find*()` followed by a write.**
  Compare-and-write is one SQL statement (QueryBuilder or `query()`).
- In hot loops: `getRawMany()` / `query()`, never `getMany()`.
- `synchronize: false`. Schema, triggers and functions change only through **new** migrations; never edit
  an applied migration.
- Always bound parameters. Never build SQL by string concatenation or template literals with values.
- In `api`, ordinary repository usage is fine.

## 7. Code conventions

- TypeScript `strict`. No `any` without a comment explaining why.
- **English** everywhere in the repo: code, comments, logs, docs, commit messages.
- Comments explain **why**, not what. Where code implements a SPEC decision, reference it:
  `// SPEC §6.2: checkpoint moves only after both sinks ack`.
- One responsibility per file, named by role: `backfill.loop.ts`, `es.sink.ts`, `checkpoint.repository.ts`,
  `bulk-result.classifier.ts`.
- Configuration only through `src/shared/config` (env vars with documented defaults). No magic numbers
  for batch sizes, timeouts, backoff limits.
- Logs: structured JSON via the shared logger, with `role`, `stream`, `batch_id`, `event`. One line per batch,
  one per state change. No `console.log`.
- Unit tests only for pure logic (bulk result classification, backoff schedule, outbox collapsing).
  Behaviour under failure is proven by `verify/`, not by mocks.
- New dependency ⇒ say in the brief what it is for and why an existing one is not enough.

## 8. Do not touch without explicit approval

- `SPEC.md` — propose the change in chat; after approval it goes in its own `docs(spec): …` commit with a
  changelog row.
- **Gate assertions in `verify/`.** Never weaken, skip or loosen an assertion to make a gate pass. If a gate
  fails, report it — a FAIL with an honest explanation is an acceptable outcome.
- `docs/ASSIGNMENT.md`, `docs/notes-ka.md` — private, git-ignored. Never commit, quote or rewrite them.
- `.env` files, credentials, git config, anything outside this repository.
- `git push`, force-push, history rewrites.

## 9. Deviations

When the implementation has to differ from the SPEC (the SPEC is wrong, incomplete, or reality disagrees):

1. Stop and describe it in chat: what the SPEC says, what you want to do, why.
2. After the human decides, add an entry to `docs/DEVIATIONS.md` (format is in that file).
3. If the SPEC should change, that is a separate `docs(spec)` commit.

Silently "improving" on the SPEC is itself a deviation and must be reported.

## 10. Before handing a slice over

- [ ] `npm run typecheck` and unit tests pass (no linter is configured — strict TypeScript is the check)
- [ ] `docker compose up -d --build` from a clean state works
- [ ] `./verify.sh <gate>` for this slice shows PASS, and previously passing gates still pass
- [ ] Every new decision point has a `SPEC §…` comment or a DEVIATIONS entry
- [ ] Briefing, walkthrough and "break it yourself" commands are written in the chat

## 11. Commits

Conventional commits, English, one slice (or one spec change) per commit:
`feat(pipeline): …`, `test(verify): add G1 gate (failing)`, `docs(spec): v1.2 …`, `fix(consumer): …`.
Gate-first work normally produces two commits per slice: the failing gate, then the feature.
