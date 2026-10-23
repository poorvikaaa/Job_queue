// Chaos test: enqueue N jobs through the REST API while SIGKILLing random worker processes,
// then verify that no job was lost and none was completed twice.
//   API must be running (npm run start:api). Workers are spawned and killed by this script.
import { fork } from 'node:child_process';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { pool, migrate } from '../src/db.js';
import { redis } from '../src/queue.js';

const API = process.env.API_URL || 'http://localhost:3000';
const TOTAL = Number(process.env.CHAOS_JOBS || 2000);
const WORKERS = Number(process.env.CHAOS_WORKERS || 4);
const KILL_EVERY_MS = Number(process.env.CHAOS_KILL_MS || 1500);
const ENQUEUE_CONCURRENCY = Number(process.env.CHAOS_ENQUEUE_CONCURRENCY || 20);
const RUN = `chaos-${Date.now()}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const workerFile = fileURLToPath(new URL('../src/worker.js', import.meta.url));
const workerEnv = { ...process.env, VISIBILITY_TIMEOUT_MS: '3000', WORKER_CONCURRENCY: '10', BACKOFF_BASE_MS: '200', SWEEP_MS: '500' };
const spawnWorker = () => fork(workerFile, { env: workerEnv, stdio: 'ignore' });

// Job mix: mostly short sleeps, some long ones (so kills land mid-job), flaky jobs, and jobs that must reach the DLQ.
function makeJob(i) {
  const r = i % 100;
  if (r < 3) return { type: 'fail', max_attempts: 3, expectDead: true };
  if (r < 10) return { type: 'flaky', payload: { failRate: 0.3, ms: 50 }, max_attempts: 10 };
  if (r < 15) return { type: 'sleep', payload: { ms: 1500 } };
  return { type: 'sleep', payload: { ms: 20 + (i % 180) } };
}

await migrate();
const workers = Array.from({ length: WORKERS }, spawnWorker);
let kills = 0;
const killer = setInterval(() => {
  const i = Math.floor(Math.random() * workers.length);
  workers[i].kill('SIGKILL');
  workers[i] = spawnWorker();
  kills++;
}, KILL_EVERY_MS);

// ---- enqueue phase ----
const latencies = [];
const ids = new Set();
let duplicatesRejected = 0, next = 0;
const t0 = Date.now();

async function post(i, key) {
  const { expectDead, ...body } = makeJob(i);
  const s = performance.now();
  const res = await fetch(`${API}/jobs`, { method: 'POST', headers: { 'content-type': 'application/json', 'idempotency-key': key }, body: JSON.stringify(body) });
  latencies.push(performance.now() - s);
  const json = await res.json();
  if (!res.ok) throw new Error(`enqueue failed: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

await Promise.all(Array.from({ length: ENQUEUE_CONCURRENCY }, async () => {
  while (next < TOTAL) {
    const i = next++;
    const key = `${RUN}-${i}`;
    ids.add((await post(i, key)).id);
    if (i % 50 === 0 && (await post(i, key)).duplicate) duplicatesRejected++; // client retry with the same key
  }
}));
const enqueueSecs = (Date.now() - t0) / 1000;
console.log(`enqueued ${ids.size} jobs in ${enqueueSecs.toFixed(1)}s while killing workers...`);

// ---- wait for every job to reach a terminal state ----
const like = `${RUN}-%`;
const counts = async () => Object.fromEntries((await pool.query(
  'SELECT status, count(*)::int n FROM jobs WHERE idempotency_key LIKE $1 GROUP BY status', [like])).rows.map((r) => [r.status, r.n]));
let c = await counts();
const deadline = Date.now() + 180000;
while ((c.completed ?? 0) + (c.dead ?? 0) < ids.size && Date.now() < deadline) { await sleep(500); c = await counts(); }
const totalSecs = (Date.now() - t0) / 1000;
clearInterval(killer);

// ---- verify ----
const one = async (sql) => (await pool.query(sql, [like])).rows[0].n;
const completed = c.completed ?? 0, dead = c.dead ?? 0;
const expectedDead = [...Array(TOTAL).keys()].filter((i) => i % 100 < 3).length;
const doubleCompleted = await one(`SELECT count(*)::int n FROM (SELECT e.job_id FROM job_events e JOIN jobs j ON j.id = e.job_id
  WHERE j.idempotency_key LIKE $1 AND e.event = 'completed' GROUP BY e.job_id HAVING count(*) > 1) t`);
const redelivered = await one(`SELECT count(*)::int n FROM job_events e JOIN jobs j ON j.id = e.job_id WHERE j.idempotency_key LIKE $1 AND e.event = 'redelivered'`);
const lost = ids.size - completed - dead;
const unexpectedDead = dead - expectedDead;
latencies.sort((a, b) => a - b);
const p = (q) => latencies[Math.min(latencies.length - 1, Math.floor(latencies.length * q))];

const report = {
  jobs: ids.size, workers: WORKERS, workerKills: kills, completed, deadLettered: dead, expectedDeadLettered: expectedDead,
  lost, unexpectedDead, completedMoreThanOnce: doubleCompleted, redeliveredAfterKill: redelivered, duplicateEnqueuesRejected: duplicatesRejected,
  enqueueThroughputPerSec: +(ids.size / enqueueSecs).toFixed(1), endToEndThroughputPerSec: +(ids.size / totalSecs).toFixed(1),
  enqueueLatencyMs: { p50: +p(0.5).toFixed(1), p95: +p(0.95).toFixed(1), p99: +p(0.99).toFixed(1) },
  totalSeconds: +totalSecs.toFixed(1),
};
console.log(JSON.stringify(report, null, 2));
fs.writeFileSync('chaos-results.json', JSON.stringify(report, null, 2) + '\n');
console.log(`\nResume line: ${lost} jobs lost across ${ids.size} jobs with ${kills} worker kills (SIGKILL), ` +
  `${report.enqueueThroughputPerSec} jobs/s enqueue, p95 enqueue latency ${report.enqueueLatencyMs.p95} ms`);

workers.forEach((w) => w.kill('SIGKILL'));
await pool.end();
redis.disconnect();
process.exit(lost === 0 && doubleCompleted === 0 && unexpectedDead <= 0 ? 0 : 1);
