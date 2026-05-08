import { configStore } from '../shared/config.js';
import { log } from '../shared/log.js';
import {
    completedStore,
    failedStore,
    queueStore,
    stateStore,
} from '../shared/storage.js';
import type { QueueItem } from '../shared/types.js';
import { applyCircuitBreaker, dailyResetIfNeeded, nextRunAtIso } from './pacing.js';

export async function popNextItem(): Promise<QueueItem | null> {
    const popped: { item: QueueItem | null } = { item: null };
    await queueStore.update((queue) => {
        if (queue.length === 0) return queue;
        popped.item = queue[0] ?? null;
        return queue.slice(1);
    });
    const head = popped.item;
    if (head === null) return null;
    await stateStore.update((s) => ({ ...s, inFlight: head.id }));
    return head;
}

export async function markCompleted(itemId: number): Promise<void> {
    const config = await configStore.get();
    await completedStore.update((c) => (c.includes(itemId) ? c : [...c, itemId]));
    await failedStore.update((f) => f.filter((x) => x.id !== itemId));
    await stateStore.update((raw) => {
        const s = dailyResetIfNeeded(raw);
        if (s.inFlight !== itemId) {
            void log.warn(
                `markCompleted(${itemId}) but inFlight=${s.inFlight}; clearing anyway`,
            );
        }
        const now = new Date();
        return {
            ...s,
            inFlight: null,
            lastRunAt: now.toISOString(),
            nextRunAt: nextRunAtIso(config, now),
            todayDownloaded: s.todayDownloaded + 1,
            consecutiveFailures: 0,
            pausedUntil: null,
        };
    });
}

export async function markFailed(itemId: number, error: string): Promise<void> {
    const config = await configStore.get();
    let attempts = 1;
    await failedStore.update((f) => {
        const existing = f.find((x) => x.id === itemId);
        attempts = (existing?.attempts ?? 0) + 1;
        const filtered = f.filter((x) => x.id !== itemId);
        return [
            ...filtered,
            { id: itemId, error, lastTryAt: new Date().toISOString(), attempts },
        ];
    });
    await stateStore.update((raw) => {
        const s = dailyResetIfNeeded(raw);
        const consecutive = s.consecutiveFailures + 1;
        const breaker = applyCircuitBreaker(consecutive, config, s.pausedUntil);
        if (breaker.tripped) {
            void log.warn(
                `circuit breaker tripped after ${consecutive} consecutive failures, paused until ${breaker.pausedUntil}`,
            );
        }
        return {
            ...s,
            inFlight: null,
            lastRunAt: new Date().toISOString(),
            nextRunAt: nextRunAtIso(config),
            consecutiveFailures: consecutive,
            pausedUntil: breaker.pausedUntil,
        };
    });
    void log.error(`item ${itemId} failed (attempt ${attempts}): ${error}`);
}

export async function recoverOrphanedInFlight(): Promise<void> {
    const s = await stateStore.get();
    if (s.inFlight === null) return;
    void log.warn(
        `found orphaned inFlight=${s.inFlight} on startup, clearing (item will reappear on next refresh-queue)`,
    );
    await stateStore.update((cur) => ({ ...cur, inFlight: null }));
}

export async function resetRunState(): Promise<void> {
    await stateStore.update((s) => ({
        ...s,
        enabled: false,
        inFlight: null,
        lastRunAt: null,
        nextRunAt: null,
        todayDownloaded: 0,
        consecutiveFailures: 0,
        pausedUntil: null,
    }));
    await completedStore.set([]);
    await failedStore.set([]);
    void log.info('run state reset (queue and config preserved)');
}
