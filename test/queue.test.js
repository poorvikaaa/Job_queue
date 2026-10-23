import { test, before, after, beforeEach } from 'node:test';
import assert from 'node:assert/strict';

// Test config must be set before the modules load (they read env at import time).
process.env.QUEUE_PREFIX = 'jqtest';
process.env.DATABASE_URL = process.env.TEST_DATABASE_URL || 'postgres://jobs:jobs@localhost:5432/jobs_test';
process.env.BACKOFF_BASE_MS = '20';
process.env.POLL_MS = '10';
process.env.SWEEP_MS = '50';
process.env.VISIBILITY_TIMEOUT_MS = '300';

const queue = await import('../src/queue.js');
const { pool, migrate } = await import('../src/db.js');
const jobs = await import('../src/jobs.js');
const { startWorker } = await import('../src/worker.js');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const q = (id, o = {}) => queue.enqueue(id, { priority: 5, maxAttempts: 3, runAtMs: Date.now(), ...o });

async function waitFor(fn, ms = 5000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { const v = await fn(); if (v) return v; await sleep(25); }
  throw new Error('timed out');
}

before(migrate);
beforeEach(async () => { await queue.clearAll(); await pool.query('TRUNCATE jobs CASCADE'); });
after(async () => { await pool.end(); queue.redis.disconnect(); });

test('higher priority jobs are reserved first', async () => {
  await q('low', { priority: 1 });
  await q('high', { priority: 9 });
  await q('mid', { priority: 5 });
  assert.deepEqual([(await queue.reserve()).id, (await queue.reserve()).id, (await queue.reserve()).id], ['high', 'mid', 'low']);
  assert.equal(await queue.reserve(), null);
});

test('delayed jobs are invisible until due', async () => {
  await q('later', { runAtMs: Date.now() + 200 });
  await queue.sweep();
  assert.equal(await queue.reserve(), null);
  await sleep(250);
  await queue.sweep();
  assert.equal((await queue.reserve()).id, 'later');
});

test('visibility timeout re-delivers a job whose worker died', async () => {
  await q('j1');
  const first = await queue.reserve(100);
  assert.equal(first.attempts, 1);
  assert.equal(await queue.reserve(100), null);         // hidden while "being processed"
  await sleep(150);
  const { reclaimed } = await queue.sweep();
  assert.deepEqual(reclaimed, ['j1']);
  assert.equal((await queue.reserve()).attempts, 2);    // at-least-once: delivered again
});

test('failed jobs retry, then land in the dead-letter queue', async () => {
  await q('j2', { maxAttempts: 2 });
  await queue.reserve();
  assert.equal(await queue.fail('j2', Date.now()), 'retry');
  await queue.sweep();
  assert.equal((await queue.reserve()).attempts, 2);
  assert.equal(await queue.fail('j2', Date.now()), 'dead');
  assert.equal((await queue.depths()).dead, 1);
});

test('expired final attempt goes to the DLQ instead of looping forever', async () => {
  await q('j3', { maxAttempts: 1 });
  await queue.reserve(50);
  await sleep(80);
  assert.deepEqual((await queue.sweep()).dead, ['j3']);
});

test('backoff grows exponentially and is capped', () => {
  const base = 20; // BACKOFF_BASE_MS in this test
  for (const [attempt, max] of [[1, base], [2, base * 2], [3, base * 4]]) {
    const v = queue.backoffMs(attempt);
    assert.ok(v >= max / 2 && v <= max, `attempt ${attempt}: ${v}`);
  }
});

test('idempotency key: same key => same job, enqueued once', async () => {
  const a = await jobs.createJob({ type: 'echo', payload: { x: 1 } }, 'order-42');
  const b = await jobs.createJob({ type: 'echo', payload: { x: 1 } }, 'order-42');
  assert.equal(a.duplicate, false);
  assert.equal(b.duplicate, true);
  assert.equal(a.job.id, b.job.id);
  assert.equal((await queue.depths()).ready, 1);
});

test('rejects invalid input', async () => {
  await assert.rejects(jobs.createJob({ type: 'nope' }), jobs.ValidationError);
  await assert.rejects(jobs.createJob({ type: 'echo', priority: 99 }), jobs.ValidationError);
});

test('worker end to end: success, retries -> DLQ, manual re-drive', async () => {
  const w = await startWorker();
  try {
    const ok = (await jobs.createJob({ type: 'sum', payload: { numbers: [1, 2, 3] } })).job;
    const bad = (await jobs.createJob({ type: 'fail', max_attempts: 3 })).job;

    await waitFor(async () => (await jobs.getJob(ok.id)).status === 'completed');
    assert.deepEqual((await jobs.getJob(ok.id)).result, { sum: 6 });

    await waitFor(async () => (await jobs.getJob(bad.id)).status === 'dead');
    const dead = await jobs.getJob(bad.id);
    assert.equal(dead.attempts, 3);
    assert.equal(dead.events.filter((e) => e.event === 'started').length, 3);
    assert.equal((await queue.depths()).dead, 1);

    await jobs.retryDead(bad.id);                         // goes back through the queue, dies again
    await waitFor(async () => (await jobs.getJob(bad.id)).events.filter((e) => e.event === 'dead').length === 2);
  } finally {
    await w.stop();
  }
});

test('a job already completed is not executed again on redelivery', async () => {
  const w = await startWorker();
  try {
    const { job } = await jobs.createJob({ type: 'echo', payload: {} });
    await waitFor(async () => (await jobs.getJob(job.id)).status === 'completed');
    await q(job.id);                                       // simulate a stale redelivery
    await waitFor(async () => (await jobs.getJob(job.id)).events.some((e) => e.event === 'duplicate_skipped'));
    assert.equal((await jobs.getJob(job.id)).events.filter((e) => e.event === 'started').length, 1);
  } finally {
    await w.stop();
  }
});
