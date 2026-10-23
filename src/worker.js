import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { migrate, pool } from './db.js';
import * as queue from './queue.js';
import { handlers } from './handlers.js';
import { event } from './jobs.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const workerId = `${os.hostname()}:${process.pid}`;
const log = (msg, extra = {}) => console.log(JSON.stringify({ t: new Date().toISOString(), worker: workerId, msg, ...extra }));

async function runJob(id, attempt) {
  const { rows: [job] } = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
  if (!job) return queue.ack(id);

  // Execution guard: a redelivered job that already finished (worker died between DB commit and ack) is not run again.
  if (job.status === 'completed' || job.status === 'dead') {
    await queue.ack(id);
    return event(id, 'duplicate_skipped', workerId);
  }

  await pool.query(`UPDATE jobs SET status = 'running', attempts = $2, started_at = now() WHERE id = $1`, [id, attempt]);
  await event(id, 'started', `${workerId} attempt ${attempt}`);

  // Heartbeat keeps the visibility deadline ahead of a long-running job; a killed worker stops beating.
  const beat = setInterval(() => queue.extend(id).catch(() => {}), config.visibilityMs / 3);
  let result, error;
  try {
    if (!handlers[job.type]) throw new Error(`unknown job type: ${job.type}`);
    result = await handlers[job.type](job.payload);
  } catch (err) {
    error = err;
  } finally {
    clearInterval(beat);
  }

  if (!error) {
    await pool.query(`UPDATE jobs SET status = 'completed', completed_at = now(), result = $2, last_error = NULL WHERE id = $1`,
      [id, JSON.stringify(result ?? null)]);
    await event(id, 'completed', workerId);
    return queue.ack(id);
  }

  const retryAt = Date.now() + queue.backoffMs(attempt);
  const outcome = await queue.fail(id, retryAt);
  if (outcome === 'dead') {
    await pool.query(`UPDATE jobs SET status = 'dead', last_error = $2, completed_at = now() WHERE id = $1`, [id, error.message]);
    await event(id, 'dead', error.message);
  } else {
    await pool.query(`UPDATE jobs SET status = 'retrying', last_error = $2, run_at = to_timestamp($3 / 1000.0) WHERE id = $1`,
      [id, error.message, retryAt]);
    await event(id, 'failed', error.message);
  }
}

// Housekeeping: promote due jobs, reclaim expired ones, record it in Postgres.
async function sweepOnce() {
  const { reclaimed, dead } = await queue.sweep();
  for (const id of reclaimed) {
    await pool.query(`UPDATE jobs SET status = 'queued' WHERE id = $1 AND status = 'running'`, [id]);
    await event(id, 'redelivered', 'visibility timeout expired');
  }
  for (const id of dead) {
    await pool.query(`UPDATE jobs SET status = 'dead', last_error = 'visibility timeout expired on final attempt', completed_at = now() WHERE id = $1`, [id]);
    await event(id, 'dead', 'visibility timeout expired on final attempt');
  }
}

// Safety net for the one non-atomic step (Postgres insert -> Redis enqueue): re-enqueue orphans.
async function reconcile() {
  const { rows } = await pool.query(
    `SELECT id, priority, max_attempts, run_at FROM jobs
      WHERE status IN ('queued', 'scheduled') AND created_at < now() - interval '60 seconds' LIMIT 200`);
  for (const j of rows) {
    if (await queue.isTracked(j.id)) continue;
    await queue.enqueue(j.id, { priority: j.priority, maxAttempts: j.max_attempts, runAtMs: new Date(j.run_at).getTime() });
    await event(j.id, 'reconciled', 'missing from Redis, re-enqueued');
  }
}

export async function startWorker() {
  await migrate();
  let running = 0;
  let stopping = false;
  const timers = [
    setInterval(() => sweepOnce().catch((e) => log('sweep error', { err: e.message })), config.sweepMs),
    setInterval(() => reconcile().catch((e) => log('reconcile error', { err: e.message })), 30000),
  ];

  const loop = (async () => {
    while (!stopping) {
      if (running >= config.concurrency) { await sleep(10); continue; }
      let job;
      try { job = await queue.reserve(); } catch (e) { log('reserve error', { err: e.message }); await sleep(1000); continue; }
      if (!job) { await sleep(config.pollMs); continue; }
      running++;
      runJob(job.id, job.attempts)
        .catch((e) => log('job error (will be redelivered)', { id: job.id, err: e.message }))
        .finally(() => running--);
    }
  })();

  log('worker started', { concurrency: config.concurrency, visibilityMs: config.visibilityMs });

  return {
    async stop() {
      stopping = true;
      await loop;
      timers.forEach(clearInterval);
      for (let i = 0; i < 300 && running > 0; i++) await sleep(100); // drain in-flight jobs (max 30s)
    },
  };
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const w = await startWorker();
  const shutdown = async () => { log('shutting down'); await w.stop(); process.exit(0); };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);
}
