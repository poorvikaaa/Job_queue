import express from 'express';
import { fileURLToPath } from 'node:url';
import { config } from './config.js';
import { migrate, pool } from './db.js';
import { redis } from './queue.js';
import { createJob, getJob, listJobs, retryDead, stats, ValidationError } from './jobs.js';

export const app = express();
app.use(express.json({ limit: '256kb' }));
app.use(express.static(fileURLToPath(new URL('../public', import.meta.url))));

const wrap = (fn) => (req, res, next) => fn(req, res).catch(next);

app.post('/jobs', wrap(async (req, res) => {
  const key = req.get('Idempotency-Key') ?? req.body?.idempotency_key ?? null;
  const { job, duplicate } = await createJob(req.body, key);
  res.status(duplicate ? 200 : 201).json({ id: job.id, status: job.status, duplicate });
}));

app.get('/jobs', wrap(async (req, res) => res.json(await listJobs(req.query))));

app.get('/jobs/:id', wrap(async (req, res) => {
  const job = await getJob(req.params.id);
  job ? res.json(job) : res.status(404).json({ error: 'not found' });
}));

app.post('/jobs/:id/retry', wrap(async (req, res) => {
  const job = await retryDead(req.params.id);
  job ? res.json({ id: job.id, status: job.status }) : res.status(409).json({ error: 'job is not in the dead-letter queue' });
}));

app.get('/stats', wrap(async (req, res) => res.json(await stats())));

app.get('/health', wrap(async (req, res) => {
  await Promise.all([pool.query('SELECT 1'), redis.ping()]);
  res.json({ ok: true });
}));

app.use((err, req, res, next) => {
  if (err instanceof ValidationError || err.type === 'entity.parse.failed') return res.status(400).json({ error: err.message });
  // invalid uuid in :id
  if (err.code === '22P02') return res.status(400).json({ error: 'invalid id' });
  console.error(err);
  res.status(500).json({ error: 'internal error' });
});

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await migrate();
  app.listen(config.port, () => console.log(`api listening on :${config.port}`));
}
