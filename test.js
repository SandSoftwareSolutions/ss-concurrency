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

// ---- review additions ----

test("a long queue does not get slower with every job", async () => {
  const run = limit(2);
  const started = Date.now();
  const all = [];
  for (let i = 0; i < 200_000; i++) all.push(run(() => i));
  assert.equal(run.pendingCount, 200_000 - 2);
  const results = await Promise.all(all);
  assert.equal(results[199_999], 199_999);
  assert.equal(run.pendingCount, 0);
  assert.ok(Date.now() - started < 4000, `took ${Date.now() - started} ms`);
});

test("pendingCount and clear stay right after jobs have started", async () => {
  const run = limit(1);
  const gates = [];
  const jobs = Array.from({ length: 5 }, (_, i) => run(() => new Promise((r) => gates.push(() => r(i)))));
  assert.deepEqual([run.activeCount, run.pendingCount], [1, 4]);
  await sleep(5);
  gates[0]();
  await jobs[0];
  await new Promise((r) => setTimeout(r, 5));
  assert.deepEqual([run.activeCount, run.pendingCount], [1, 3]);
  const outcomes = jobs.slice(2).map((j) => j.then(() => "ran", (e) => e.message));
  run.clear();
  assert.equal(run.pendingCount, 0);
  assert.deepEqual(await Promise.all(outcomes), ["limit: cleared before it started", "limit: cleared before it started", "limit: cleared before it started"]);
  await sleep(5);
  gates[1]();
  assert.equal(await jobs[1], 1);
});

test("mapLimit rejects at once on the first failure, even if other work hangs", async () => {
  const started = Date.now();
  const boom = new Error("boom");
  await assert.rejects(
    mapLimit([1, 2, 3], 3, (n) => (n === 1 ? Promise.reject(boom) : new Promise(() => {}))),
    (e) => e === boom
  );
  assert.ok(Date.now() - started < 1000);
});

test("mapLimit rejects at once when the signal aborts, even if work hangs", async () => {
  const controller = new AbortController();
  const reason = new Error("stop");
  const started = Date.now();
  const promise = mapLimit([1, 2, 3, 4], 2, () => new Promise(() => {}), { signal: controller.signal });
  setTimeout(() => controller.abort(reason), 30);
  await assert.rejects(promise, (e) => e === reason);
  assert.ok(Date.now() - started < 1000);
  await assert.rejects(mapLimit([1], 1, () => 1, { signal: AbortSignal.abort(reason) }), (e) => e === reason);
});

test("mapLimit does not start new work after a failure or an abort", async () => {
  const seen = [];
  await assert.rejects(mapLimit([1, 2, 3, 4, 5, 6], 2, async (n) => { seen.push(n); await new Promise((r) => setTimeout(r, 5)); if (n === 2) throw new Error("x"); }));
  await new Promise((r) => setTimeout(r, 40));
  assert.ok(!seen.includes(6), `started ${seen}`);
});

test("a failure after the answer was given is not an unhandled rejection", async () => {
  const late = [];
  await assert.rejects(mapLimit([1, 2], 2, async (n) => { if (n === 1) throw new Error("first"); await new Promise((r) => setTimeout(r, 10)); late.push(n); throw new Error("second"); }), /first/);
  await new Promise((r) => setTimeout(r, 40)); // the test runner fails the file on an unhandled rejection
  assert.deepEqual(late, [2]);
});
