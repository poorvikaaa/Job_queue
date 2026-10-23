// Redis-backed queue. All state transitions are Lua scripts => atomic.
//
//   ready      ZSET  id -> (9 - priority) * 1e13 + enqueue_ms   (lowest score = next)
//   delayed    ZSET  id -> run_at_ms                            (scheduled jobs + retry backoff)
//   processing ZSET  id -> visibility deadline (ms)             (reserved, not yet acked)
//   dead       ZSET  id -> time moved to DLQ
//   job:<id>   HASH  priority, max, attempts
import Redis from 'ioredis';
import { config } from './config.js';

const p = config.prefix;
const K = {
  ready: `${p}:ready`,
  delayed: `${p}:delayed`,
  processing: `${p}:processing`,
  dead: `${p}:dead`,
  job: `${p}:job:`,
};

export const redis = new Redis(config.redisUrl, { maxRetriesPerRequest: null });

const SCORE = `local function score(prio, now) return string.format('%.0f', (9 - prio) * 1e13 + now) end`;

redis.defineCommand('qEnqueue', {
  numberOfKeys: 3, // ready, delayed, jobhash
  lua: `${SCORE}
    local prio, max, runAt, now = tonumber(ARGV[2]), ARGV[3], tonumber(ARGV[4]), tonumber(ARGV[5])
    redis.call('HSET', KEYS[3], 'priority', prio, 'max', max, 'attempts', 0)
    if runAt > now then
      redis.call('ZADD', KEYS[2], string.format('%.0f', runAt), ARGV[1]); return 'delayed'
    end
    redis.call('ZADD', KEYS[1], score(prio, now), ARGV[1]); return 'ready'`,
});

// Moves due delayed jobs to ready; reclaims jobs whose visibility timeout expired
// (back to ready, or to the DLQ if attempts are exhausted). Returns {reclaimed, dead}.
redis.defineCommand('qSweep', {
  numberOfKeys: 4, // ready, delayed, processing, dead
  lua: `${SCORE}
    local now, jp = tonumber(ARGV[1]), ARGV[2]
    local function prio(id) return tonumber(redis.call('HGET', jp .. id, 'priority')) or 5 end
    for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, 500)) do
      redis.call('ZREM', KEYS[2], id)
      redis.call('ZADD', KEYS[1], score(prio(id), now), id)
    end
    local reclaimed, dead = {}, {}
    for _, id in ipairs(redis.call('ZRANGEBYSCORE', KEYS[3], '-inf', now, 'LIMIT', 0, 500)) do
      redis.call('ZREM', KEYS[3], id)
      local attempts = tonumber(redis.call('HGET', jp .. id, 'attempts')) or 0
      local max = tonumber(redis.call('HGET', jp .. id, 'max')) or 5
      if attempts >= max then
        redis.call('ZADD', KEYS[4], string.format('%.0f', now), id); dead[#dead + 1] = id
      else
        redis.call('ZADD', KEYS[1], score(prio(id), now), id); reclaimed[#reclaimed + 1] = id
      end
    end
    return {reclaimed, dead}`,
});

// Pops the next job, hides it until the visibility deadline, counts the attempt.
redis.defineCommand('qReserve', {
  numberOfKeys: 2, // ready, processing
  lua: `local r = redis.call('ZPOPMIN', KEYS[1])
    if #r == 0 then return false end
    redis.call('ZADD', KEYS[2], string.format('%.0f', tonumber(ARGV[1]) + tonumber(ARGV[2])), r[1])
    return {r[1], redis.call('HINCRBY', ARGV[3] .. r[1], 'attempts', 1)}`,
});

redis.defineCommand('qAck', {
  numberOfKeys: 1,
  lua: `redis.call('ZREM', KEYS[1], ARGV[1]); redis.call('DEL', ARGV[2] .. ARGV[1]); return 1`,
});

// Failure: retry later (delayed ZSET) or DLQ when attempts are exhausted.
redis.defineCommand('qFail', {
  numberOfKeys: 3, // processing, delayed, dead
  lua: `redis.call('ZREM', KEYS[1], ARGV[1])
    local attempts = tonumber(redis.call('HGET', ARGV[4] .. ARGV[1], 'attempts')) or 0
    local max = tonumber(redis.call('HGET', ARGV[4] .. ARGV[1], 'max')) or 5
    if attempts >= max then redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1]); return 'dead' end
    redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1]); return 'retry'`,
});

export const jobKey = (id) => K.job + id;

export const enqueue = (id, { priority, maxAttempts, runAtMs }) =>
  redis.qEnqueue(K.ready, K.delayed, jobKey(id), id, priority, maxAttempts, Math.round(runAtMs), Date.now());

export async function reserve(visibilityMs = config.visibilityMs) {
  const r = await redis.qReserve(K.ready, K.processing, Date.now(), visibilityMs, K.job);
  return r ? { id: r[0], attempts: Number(r[1]) } : null;
}

export const ack = (id) => redis.qAck(K.processing, id, K.job);

// Heartbeat: push the visibility deadline out while a job is still running.
export const extend = (id, visibilityMs = config.visibilityMs) =>
  redis.zadd(K.processing, 'XX', 'CH', Date.now() + visibilityMs, id);

export const fail = (id, retryAtMs) =>
  redis.qFail(K.processing, K.delayed, K.dead, id, Math.round(retryAtMs), Date.now(), K.job);

export async function sweep() {
  const [reclaimed, dead] = await redis.qSweep(K.ready, K.delayed, K.processing, K.dead, Date.now(), K.job);
  return { reclaimed, dead };
}

export const removeFromDead = (id) => redis.zrem(K.dead, id);

export async function isTracked(id) {
  const scores = await Promise.all([K.ready, K.delayed, K.processing, K.dead].map((k) => redis.zscore(k, id)));
  return scores.some((s) => s !== null);
}

export async function depths() {
  const [ready, delayed, processing, dead] = await Promise.all(
    [K.ready, K.delayed, K.processing, K.dead].map((k) => redis.zcard(k)),
  );
  return { ready, delayed, processing, dead };
}

// Exponential backoff with jitter: base * 2^(attempt-1), capped.
export function backoffMs(attempt) {
  const exp = Math.min(config.backoffMaxMs, config.backoffBaseMs * 2 ** (attempt - 1));
  return Math.round(exp * (0.5 + Math.random() / 2));
}

export async function clearAll() {
  const keys = await redis.keys(`${p}:*`);
  if (keys.length) await redis.del(keys);
}
