const n = (v, d) => (v === undefined || v === '' ? d : Number(v));

export const config = {
  port: n(process.env.PORT, 3000),
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  databaseUrl: process.env.DATABASE_URL || 'postgres://jobs:jobs@localhost:5432/jobs',
  prefix: process.env.QUEUE_PREFIX || 'jq',
  visibilityMs: n(process.env.VISIBILITY_TIMEOUT_MS, 30000), // job is re-delivered if not acked in this time
  concurrency: n(process.env.WORKER_CONCURRENCY, 5),         // jobs in flight per worker process
  pollMs: n(process.env.POLL_MS, 100),
  sweepMs: n(process.env.SWEEP_MS, 1000),
  backoffBaseMs: n(process.env.BACKOFF_BASE_MS, 1000),
  backoffMaxMs: n(process.env.BACKOFF_MAX_MS, 60000),
  defaultMaxAttempts: n(process.env.MAX_ATTEMPTS, 5),
};
