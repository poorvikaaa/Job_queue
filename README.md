# Distributed Job Queue with Retries & Dead-Letter Queue

A job queue built from scratch on **Node.js, Redis and PostgreSQL**: REST API to enqueue jobs, multiple worker processes, at-least-once delivery via visibility timeouts, exponential-backoff retries, a dead-letter queue, idempotency keys, priority and delayed jobs, and a live dashboard.

```
            POST /jobs                 reserve / ack / fail
 client ───────────────▶  API  ─────────────────────────────▶ Workers (N processes x M concurrent jobs)
                           │  ▲                                   │
              insert row   │  │ stats                             │ state + history
                           ▼  │                                   ▼
                      PostgreSQL  ◀───────────────────────────────┘
                   (jobs, job_events)
                           │ enqueue
                           ▼
                 Redis (Lua scripts, atomic)
        ready ZSET · delayed ZSET · processing ZSET · dead ZSET
```

## Features

| Feature | How it works |
|---|---|
| **At-least-once delivery** | `reserve` moves a job into a `processing` ZSET scored by its visibility deadline. If the worker never acks (crash, `kill -9`), the sweeper puts the job back on `ready`. Running workers heartbeat to extend the deadline. |
| **Exponential-backoff retries** | A failed job goes to the `delayed` ZSET with `base * 2^(attempt-1)` delay (capped, with jitter). |
| **Dead-letter queue** | After `max_attempts` (failures *or* expired visibility timeouts) the job moves to the `dead` ZSET and `status = dead`. Re-drive from the dashboard or `POST /jobs/:id/retry`. |
| **Idempotency keys** | `Idempotency-Key` header (or `idempotency_key` field) is a `UNIQUE` column: the same key returns the original job instead of creating a second one. Workers also skip a redelivered job that is already `completed`, so a crash between "DB commit" and "Redis ack" does not run the job twice. |
| **Priority + delayed jobs** | Priority 0-9 is encoded in the ready-queue score; `delay_ms` / `run_at` park the job in the delayed ZSET until due. |
| **State + history** | Every job row and every transition (`started`, `failed`, `redelivered`, `dead`, ...) is stored in PostgreSQL. |
| **Dashboard** | `http://localhost:3000/`: queue depth, in-flight, failures, throughput sparkline, DLQ with retry button, recent jobs. |
| **Chaos test** | `npm run chaos` SIGKILLs workers mid-job while enqueuing, then asserts nothing was lost or completed twice. |

All queue state transitions are Redis **Lua scripts**, so they are atomic across any number of workers.

## Run it

```bash
docker compose up --build        # redis + postgres + api + 3 workers
open http://localhost:3000       # dashboard
```

Without Docker (needs local Redis and Postgres; see `.env.example`):

```bash
npm install
npm run start:api      # terminal 1
npm run start:worker   # terminal 2 (start as many as you like)
```

## API

```bash
# enqueue (priority 0-9, higher first; delay_ms or run_at to schedule; max_attempts 1-50)
curl -X POST localhost:3000/jobs -H 'content-type: application/json' -H 'Idempotency-Key: order-42' \
  -d '{"type":"sum","payload":{"numbers":[1,2,3]},"priority":8,"max_attempts":5}'
# -> 201 {"id":"...","status":"queued","duplicate":false}   (same key again -> 200, "duplicate":true)

curl localhost:3000/jobs/<id>                 # job + full event history
curl 'localhost:3000/jobs?status=dead'        # list by status (scheduled|queued|running|retrying|completed|dead)
curl -X POST localhost:3000/jobs/<id>/retry   # re-drive from the DLQ
curl localhost:3000/stats                     # queue depths, counts, throughput
curl localhost:3000/health
```

Built-in job types (`src/handlers.js`): `echo`, `sum`, `sleep`, `flaky`, `fail`. Add your own by adding a function to that object.

## Tests

```bash
npm test          # needs Redis + Postgres; uses a separate key prefix and DB (TEST_DATABASE_URL, default .../jobs_test)
```

Covers priority order, delayed jobs, visibility-timeout redelivery, retry then DLQ, idempotency, validation, and end-to-end worker runs.

## Chaos test (reliability numbers)

```bash
npm run start:api &        # API + Redis + Postgres running
CHAOS_JOBS=5000 CHAOS_WORKERS=4 CHAOS_KILL_MS=1500 npm run chaos
```

It spawns worker processes, enqueues jobs through the REST API (a mix of short, long, flaky and always-failing jobs, plus deliberate duplicate enqueues), and `SIGKILL`s a random worker every `CHAOS_KILL_MS`. When every job is terminal it verifies:

- **lost** = jobs that never reached `completed`/`dead` (must be 0)
- **completedMoreThanOnce** (must be 0) and unexpected dead-letters (must be 0)
- redeliveries, enqueue throughput, end-to-end throughput, p50/p95/p99 enqueue latency

Results are printed and written to `chaos-results.json`; the exit code is non-zero if any invariant fails (CI runs it).

### Sample result

3,000 jobs, 4 workers, 22 `SIGKILL`s, on a **single-vCPU sandbox** where Redis, Postgres, the API, the workers and the load generator all shared one core (so absolute numbers are a floor; run it on your machine for your own):

| lost | completed | dead-lettered (expected) | redelivered after kill | enqueue jobs/s | p95 enqueue latency |
|---|---|---|---|---|---|
| 0 | 2,910 | 90 (90) | 139 | 174.9 | 183.6 ms |

## Configuration

| Env var | Default | Meaning |
|---|---|---|
| `VISIBILITY_TIMEOUT_MS` | 30000 | Redelivery delay for a job whose worker died |
| `WORKER_CONCURRENCY` | 5 | Jobs in flight per worker process |
| `BACKOFF_BASE_MS` / `BACKOFF_MAX_MS` | 1000 / 60000 | Retry backoff |
| `MAX_ATTEMPTS` | 5 | Default attempts before the DLQ |
| `REDIS_URL`, `DATABASE_URL`, `PORT`, `QUEUE_PREFIX` | see `.env.example` | Connections |

## Design notes and known limitations

- **Delivery is at-least-once, not exactly-once.** A handler can run twice if a worker dies mid-job, so handlers should be idempotent. The DB guard prevents re-running a job that already *finished*.
- **Enqueue is two steps** (Postgres insert, then Redis enqueue). If the API dies in between, the periodic `reconcile` in the workers re-enqueues jobs that exist in Postgres but are missing from Redis.
- **Late ack after reclaim:** if a worker stalls longer than the visibility timeout (without heartbeating) the job may be handed to a second worker. The completed-check keeps the result consistent, but the work may run twice.
- **Redis durability:** compose enables AOF; a Redis wipe is healed by `reconcile` for queued/scheduled jobs.
- Lua scripts build the per-job hash key inside the script, which is fine for single-node Redis but would need hash tags for Redis Cluster.

## Layout

```
src/queue.js     Redis queue: Lua scripts, reserve/ack/fail/sweep, backoff
src/jobs.js      Create/get/list/retry jobs, idempotency, stats (Postgres + enqueue)
src/api.js       Express REST API + dashboard
src/worker.js    Worker loop, heartbeats, sweeper, reconciler, graceful shutdown
src/handlers.js  Job handlers
scripts/chaos.js Chaos / reliability test
schema.sql       Postgres schema
test/            node:test integration tests
```

## License

MIT
