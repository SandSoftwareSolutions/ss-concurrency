# @sandsoftwaresolutions/concurrency

[![CI](https://github.com/SandSoftwareSolutions/ss-concurrency/actions/workflows/ci.yml/badge.svg)](https://github.com/SandSoftwareSolutions/ss-concurrency/actions/workflows/ci.yml)

Do many things at once, but not too many. Run async work with a maximum, in order, with results in the right place, and nicely stoppable. No dependencies.

## Install

```bash
npm install @sandsoftwaresolutions/concurrency
```

## Usage

```js
import { limit, mapLimit, chunk, createMutex } from "@sandsoftwaresolutions/concurrency";

// At most 5 downloads at the same time
const run = limit(5);
const pages = await Promise.all(urls.map((url) => run(() => fetch(url).then((r) => r.text()))));

// The same, in one go: results come back in the order of the items
const sizes = await mapLimit(files, 8, async (file) => (await stat(file)).size);

// Batch inserts of at most 500 rows
for (const rows of chunk(allRows, 500)) insertMany(rows);

// One at a time, in order (for example writes to one file)
const lock = createMutex();
await lock.run(async () => { /* ... */ });
```

## Details

- `limit(n)` returns a function. Work starts in the order you submit it. `run.activeCount` and `run.pendingCount` tell you what is going on; `run.clear()` rejects the work that has not started yet.
- A failing job rejects its own promise and does not stop the others.
- `mapLimit(items, n, fn)` stops starting new work after the first error and throws that error **at once**, like `Promise.all` (work that is already running finishes by itself, and its result is dropped). With `{ settle: true }` you get `{ status, value | reason }` for every item and nothing is thrown, like `Promise.allSettled`.
- `{ signal }` stops starting work and rejects at once with the signal's reason.
- The queue behind `limit` is cheap: 200 000 queued jobs are fine.
- `chunk(items, size)` splits into groups; the last one may be smaller.
- `createMutex()` is `limit(1)` with a `locked` flag. It is not re-entrant: calling `lock.run` from inside a `lock.run` callback and awaiting it waits for itself for ever.
