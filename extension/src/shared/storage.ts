import type { FailedItem, QueueItem, State } from './types.js';

export interface Store<T> {
    get(): Promise<T>;
    set(value: T): Promise<void>;
    update(fn: (cur: T) => T | Promise<T>): Promise<T>;
}

export function makeStore<T>(key: string, defaults: T): Store<T> {
    const isPlainObject =
        typeof defaults === 'object' &&
        defaults !== null &&
        !Array.isArray(defaults);

    async function get(): Promise<T> {
        const result = await browser.storage.local.get(key);
        const stored = result[key] as T | undefined;
        if (stored === undefined) return structuredClone(defaults);
        if (isPlainObject) {
            return { ...(defaults as object), ...(stored as object) } as T;
        }
        return stored;
    }

    async function set(value: T): Promise<void> {
        await browser.storage.local.set({ [key]: value });
    }

    // Read-modify-write across `await` is racy even in single-threaded JS:
    // two concurrent updates could both read the old value before either writes.
    // chain serializes them so each update sees the previous one's result.
    let chain: Promise<unknown> = Promise.resolve();
    async function update(fn: (cur: T) => T | Promise<T>): Promise<T> {
        const queued = chain.then(async () => {
            const cur = await get();
            const next = await fn(cur);
            await set(next);
            return next;
        });
        chain = queued.catch(() => undefined);
        return queued;
    }

    return { get, set, update };
}

export const DEFAULT_STATE: State = {
    inFlight: null,
    lastRunAt: null,
    todayDownloaded: 0,
    todayResetAt: new Date().toISOString().slice(0, 10),
    consecutiveFailures: 0,
    pausedUntil: null,
};

export const stateStore = makeStore<State>('state', DEFAULT_STATE);
export const queueStore = makeStore<QueueItem[]>('queue', []);
export const completedStore = makeStore<number[]>('completed', []);
export const failedStore = makeStore<FailedItem[]>('failed', []);
