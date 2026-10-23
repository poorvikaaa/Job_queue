// Job lifecycle on the Postgres side (source of truth for state + history) + enqueue into Redis.
import { randomUUID } from 'node:crypto';
import { pool } from './db.js';
import * as queue from './queue.js';
import { config } from './config.js';
import { handlers } from './handlers.js';

export async function event(jobId, name, detail = null) {
  await pool.query('INSERT INTO job_events (job_id, event, detail) VALUES ($1, $2, $3)', [jobId, name, detail]);
}

export class ValidationError extends Error {}

function validate(b) {
  if (!b || typeof b.type !== 'string' || !handlers[b.type]) {
    throw new ValidationError(`type must be one of: ${Object.keys(handlers).join(', ')}`);
  }
  const priority = b.priority ?? 5;
  if (!Number.isInteger(priority) || priority < 0 || priority > 9) throw new ValidationError('priority must be an integer 0-9');
  const maxAttempts = b.max_attempts ?? config.defaultMaxAttempts;
  if (!Number.isInteger(maxAttempts) || maxAttempts < 1 || maxAttempts > 50) throw new ValidationError('max_attempts must be 1-50');
  let runAtMs = Date.now();
  if (b.run_at !== undefined) {
    runAtMs = Date.parse(b.run_at);
    if (Number.isNaN(runAtMs)) throw new ValidationError('run_at must be an ISO timestamp');
  } else if (b.delay_ms !== undefined) {
    if (!Number.isFinite(b.delay_ms) || b.delay_ms < 0) throw new ValidationError('delay_ms must be >= 0');
    runAtMs += b.delay_ms;
  }
  return { priority, maxAttempts, runAtMs, payload: b.payload ?? {} };
}

// Returns { job, duplicate }. Same idempotency_key => same job, never enqueued twice.
export async function createJob(body, idempotencyKey = body?.idempotency_key ?? null) {
  const v = validate(body);
  const status = v.runAtMs > Date.now() ? 'scheduled' : 'queued';
  const id = randomUUID();
  const { rows } = await pool.query(
    `INSERT INTO jobs (id, type, payload, priority, status, max_attempts, idempotency_key, run_at)
     VALUES ($1, $2, $3, $4, $5, $6, $7, to_timestamp($8 / 1000.0))
     ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`,
    [id, body.type, JSON.stringify(v.payload), v.priority, status, v.maxAttempts, idempotencyKey, v.runAtMs],
  );
  if (!rows.length) {
    const existing = await pool.query('SELECT * FROM jobs WHERE idempotency_key = $1', [idempotencyKey]);
    return { job: existing.rows[0], duplicate: true };
  }
  await queue.enqueue(id, v);
  return { job: rows[0], duplicate: false };
}

export async function getJob(id) {
  const { rows } = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
  if (!rows.length) return null;
  const events = await pool.query('SELECT event, detail, at FROM job_events WHERE job_id = $1 ORDER BY id', [id]);
  return { ...rows[0], events: events.rows };
}

export async function listJobs({ status, limit = 50 } = {}) {
  const { rows } = await pool.query(
    `SELECT id, type, status, priority, attempts, max_attempts, last_error, created_at, completed_at
       FROM jobs WHERE ($1::text IS NULL OR status = $1) ORDER BY created_at DESC LIMIT $2`,
    [status ?? null, Math.min(Number(limit) || 50, 500)],
  );
  return rows;
}

// Manual re-drive of a dead-lettered job: attempts reset, goes back to the queue.
export async function retryDead(id) {
  const { rows } = await pool.query(
    `UPDATE jobs SET status = 'queued', attempts = 0, last_error = NULL, completed_at = NULL
      WHERE id = $1 AND status = 'dead' RETURNING *`,
    [id],
  );
  if (!rows.length) return null;
  await queue.removeFromDead(id);
  await queue.enqueue(id, { priority: rows[0].priority, maxAttempts: rows[0].max_attempts, runAtMs: Date.now() });
  await event(id, 'manual_retry');
  return rows[0];
}

export async function stats() {
  const [depths, byStatus, perSec, fails] = await Promise.all([
    queue.depths(),
    pool.query('SELECT status, count(*)::int AS n FROM jobs GROUP BY status'),
    pool.query(`SELECT extract(epoch FROM date_trunc('second', completed_at))::bigint AS s, count(*)::int AS n
                  FROM jobs WHERE completed_at > now() - interval '60 seconds' AND status = 'completed' GROUP BY 1`),
    pool.query(`SELECT count(*)::int AS n FROM job_events WHERE event IN ('failed','dead') AND at > now() - interval '60 seconds'`),
  ]);
  const nowS = Math.floor(Date.now() / 1000);
  const bucket = new Map(perSec.rows.map((r) => [Number(r.s), r.n]));
  const throughput = Array.from({ length: 60 }, (_, i) => bucket.get(nowS - 59 + i) ?? 0);
  return {
    queue: depths,
    byStatus: Object.fromEntries(byStatus.rows.map((r) => [r.status, r.n])),
    throughput,                                  // completed jobs per second, last 60s
    throughputAvg: throughput.reduce((a, b) => a + b, 0) / 60,
    failures60s: fails.rows[0].n,
  };
}
