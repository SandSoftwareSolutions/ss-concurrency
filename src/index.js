/**
 * Runs async work with at most `limit` things at the same time.
 * `const run = limit(3); await Promise.all(items.map((i) => run(() => fetchIt(i))));`
 * The returned function has `activeCount` and `pendingCount`, and `clear()` to drop work that has not started.
 */
export function limit(concurrency) {
  if (!Number.isInteger(concurrency) || concurrency < 1) throw new RangeError("limit: concurrency must be a whole number of at least 1");
  let active = 0;
  const queue = [];
  const next = () => {
    if (active >= concurrency || queue.length === 0) return;
    active++;
    const job = queue.shift();
    Promise.resolve()
      .then(job.fn)
      .then(job.resolve, job.reject)
      .finally(() => {
        active--;
        next();
      });
  };
  const run = (fn) => {
    if (typeof fn !== "function") throw new TypeError("limit: pass a function that returns a promise");
    return new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      next();
    });
  };
  Object.defineProperties(run, {
    activeCount: { get: () => active },
    pendingCount: { get: () => queue.length }
  });
  run.clear = () => {
    const dropped = queue.splice(0);
    for (const job of dropped) job.reject(new Error("limit: cleared before it started"));
  };
  return run;
}

/**
 * Like `Promise.all(items.map(fn))` but with at most `concurrency` at a time, results in the order of the items.
 * On the first failure no new work is started and the error is thrown (work already running finishes on its own).
 * With `{ settle: true }` nothing is thrown: you get `{ status, value | reason }` per item, like `Promise.allSettled`.
 * `signal` stops starting new work and rejects with the signal's reason.
 */
export async function mapLimit(items, concurrency, fn, { settle = false, signal } = {}) {
  if (!Number.isInteger(concurrency) || concurrency < 1) throw new RangeError("mapLimit: concurrency must be a whole number of at least 1");
  const list = [...items];
  const results = new Array(list.length);
  let index = 0;
  let failure;
  let failed = false;
  async function worker() {
    while (!failed && index < list.length) {
      if (signal?.aborted) {
        failed = true;
        failure = signal.reason;
        return;
      }
      const i = index++;
      try {
        const value = await fn(list[i], i);
        results[i] = settle ? { status: "fulfilled", value } : value;
      } catch (reason) {
        if (settle) results[i] = { status: "rejected", reason };
        else if (!failed) {
          failed = true;
          failure = reason;
        }
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(concurrency, list.length) }, worker));
  if (failed) throw failure;
  return results;
}

/** Splits an array into chunks of `size` (the last may be smaller): handy for batch inserts and API calls with a maximum. */
export function chunk(items, size) {
  if (!Number.isInteger(size) || size < 1) throw new RangeError("chunk: size must be a whole number of at least 1");
  const list = [...items];
  const out = [];
  for (let i = 0; i < list.length; i += size) out.push(list.slice(i, i + size));
  return out;
}

/** A lock for one thing at a time, in order: `await mutex.run(async () => { ... })`. */
export function createMutex() {
  const run = limit(1);
  return { run, get locked() { return run.activeCount > 0; } };
}
