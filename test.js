import assert from "node:assert/strict";
import { test } from "node:test";

import { chunk, createMutex, limit, mapLimit } from "./src/index.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test("limit never runs more than the limit at once", async () => {
  const run = limit(3);
  let now = 0, peak = 0;
  const job = async () => { now++; peak = Math.max(peak, now); await sleep(10); now--; };
  await Promise.all(Array.from({ length: 12 }, () => run(job)));
  assert.equal(peak, 3);
  assert.equal(run.activeCount, 0);
  assert.equal(run.pendingCount, 0);
});

test("limit passes results and errors on, and keeps going after an error", async () => {
  const run = limit(1);
  const results = await Promise.allSettled([run(() => 1), run(() => { throw new Error("x"); }), run(async () => 3)]);
  assert.deepEqual(results.map((r) => r.status), ["fulfilled", "rejected", "fulfilled"]);
  assert.equal(results[2].value, 3);
  assert.throws(() => run("x"), TypeError);
  assert.throws(() => limit(0), RangeError);
});

test("limit starts in order and clear drops the waiting work", async () => {
  const run = limit(1);
  const order = [];
  const first = run(async () => { await sleep(20); order.push(1); });
  const second = run(() => order.push(2));
  const third = run(() => order.push(3));
  assert.equal(run.pendingCount, 2);
  const dropped = Promise.all([assert.rejects(second, /cleared/), assert.rejects(third, /cleared/)]);
  run.clear();
  await first;
  await dropped;
  assert.deepEqual(order, [1]);
});

test("mapLimit keeps the order of the results and the limit", async () => {
  let now = 0, peak = 0;
  const out = await mapLimit([30, 5, 20, 1, 10], 2, async (ms, i) => { now++; peak = Math.max(peak, now); await sleep(ms); now--; return `${i}:${ms}`; });
  assert.deepEqual(out, ["0:30", "1:5", "2:20", "3:1", "4:10"]);
  assert.equal(peak, 2);
  assert.deepEqual(await mapLimit([], 3, () => 1), []);
  assert.deepEqual(await mapLimit(new Set([1, 2]), 5, (x) => x * 2), [2, 4]);
});

test("mapLimit stops starting work after the first error", async () => {
  const started = [];
  await assert.rejects(mapLimit([1, 2, 3, 4, 5, 6], 1, async (x) => { started.push(x); if (x === 2) throw new Error("two"); }), /two/);
  assert.deepEqual(started, [1, 2]);
});

test("mapLimit with settle reports every result", async () => {
  const out = await mapLimit([1, 2, 3], 2, async (x) => { if (x === 2) throw new Error("two"); return x; }, { settle: true });
  assert.deepEqual(out.map((r) => r.status), ["fulfilled", "rejected", "fulfilled"]);
  assert.equal(out[1].reason.message, "two");
});

test("mapLimit can be aborted", async () => {
  const controller = new AbortController();
  const reason = new Error("stop");
  const started = [];
  const promise = mapLimit([1, 2, 3, 4], 1, async (x) => { started.push(x); await sleep(20); }, { signal: controller.signal });
  setTimeout(() => controller.abort(reason), 30);
  await assert.rejects(promise, (e) => e === reason);
  assert.ok(started.length < 4);
});

test("chunk", () => {
  assert.deepEqual(chunk([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
  assert.deepEqual(chunk([], 3), []);
  assert.throws(() => chunk([1], 0), RangeError);
});

test("mutex runs one at a time in order", async () => {
  const m = createMutex();
  const log = [];
  await Promise.all([1, 2, 3].map((n) => m.run(async () => { log.push(`start${n}`); await sleep(5); log.push(`end${n}`); })));
  assert.deepEqual(log, ["start1", "end1", "start2", "end2", "start3", "end3"]);
  assert.equal(m.locked, false);
});
