import fs from 'node:fs';
import pg from 'pg';
import { config } from './config.js';

export const pool = new pg.Pool({ connectionString: config.databaseUrl, max: 20 });

// Idempotent; advisory lock so API + workers can all call it at startup.
export async function migrate() {
  const sql = fs.readFileSync(new URL('../schema.sql', import.meta.url), 'utf8');
  const c = await pool.connect();
  try {
    await c.query('SELECT pg_advisory_lock(727)');
    await c.query(sql);
  } finally {
    await c.query('SELECT pg_advisory_unlock(727)').catch(() => {});
    c.release();
  }
}
