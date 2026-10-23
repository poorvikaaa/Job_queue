// Job handlers. Add your own: a handler gets the job payload, returns a JSON result or throws to fail.
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export const handlers = {
  echo: async (payload) => payload,
  sum: async ({ numbers = [] }) => ({ sum: numbers.reduce((a, b) => a + b, 0) }),
  sleep: async ({ ms = 100 }) => { await sleep(ms); return { slept: ms }; },
  fail: async () => { throw new Error('this job always fails'); },
  flaky: async ({ failRate = 0.5, ms = 50 }) => {
    await sleep(ms);
    if (Math.random() < failRate) throw new Error('flaky failure');
    return { ok: true };
  },
};
