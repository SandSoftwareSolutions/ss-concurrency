export interface Limit {
  <T>(fn: () => T | Promise<T>): Promise<T>;
  readonly activeCount: number;
  readonly pendingCount: number;
  /** Rejects the work that has not started yet. */
  clear(): void;
}
export function limit(concurrency: number): Limit;

export type Settled<T> = { status: "fulfilled"; value: T } | { status: "rejected"; reason: unknown };

export function mapLimit<T, R>(items: Iterable<T>, concurrency: number, fn: (item: T, index: number) => R | Promise<R>, options?: { settle?: false; signal?: AbortSignal }): Promise<R[]>;
export function mapLimit<T, R>(items: Iterable<T>, concurrency: number, fn: (item: T, index: number) => R | Promise<R>, options: { settle: true; signal?: AbortSignal }): Promise<Settled<R>[]>;

export function chunk<T>(items: Iterable<T>, size: number): T[][];
export function createMutex(): { run: Limit; readonly locked: boolean };
