import { configStore, DEFAULT_CONFIG } from '../shared/config.js';
import { log } from '../shared/log.js';
import type {
    FetchFanIdResult,
    Message,
    MessageResponse,
    ProcessTickResult,
    RefreshQueueResult,
    ResolveFirstUrlResult,
    SetEnabledResult,
} from '../shared/messages.js';
import { completedStore, queueStore, stateStore } from '../shared/storage.js';
import {
    fetchHomepageContext,
    paginateCollection,
    resolveSignedDownloadUrl,
} from './api.js';
import { downloadItem } from './downloader.js';
import { dailyResetIfNeeded, shouldRunNow } from './pacing.js';
import {
    markCompleted,
    markFailed,
    popNextItem,
    recoverOrphanedInFlight,
    resetRunState,
} from './queue.js';

const VERSION = browser.runtime.getManifest().version;
const ALARM_NAME = 'warden-tick';

void browser.browserAction.setBadgeBackgroundColor({ color: '#629aa9' });

browser.runtime.onInstalled.addListener(async (details) => {
    await log.info(`installed/updated: ${details.reason}, version ${VERSION}`);
});

browser.runtime.onStartup.addListener(async () => {
    await log.info(`browser startup, version ${VERSION}`);
});

browser.browserAction.onClicked.addListener(() => {
    void browser.runtime.openOptionsPage();
});

function describeError(err: unknown): string {
    if (err instanceof Error) return `${err.name}: ${err.message}`;
    if (typeof err === 'string') return err;
    try {
        return JSON.stringify(err);
    } catch {
        return String(err);
    }
}

interface TickResult {
    run: boolean;
    reason?: string;
    itemId?: number;
}

async function processOneTick(force = false): Promise<TickResult> {
    await stateStore.update((s) => dailyResetIfNeeded(s));
    const [state, config] = await Promise.all([stateStore.get(), configStore.get()]);
    const decision = shouldRunNow(state, config, new Date(), {
        ignoreCooldown: force,
        ignoreDisabled: force,
    });
    if (!decision.run) {
        return { run: false, reason: decision.reason };
    }
    const item = await popNextItem();
    if (!item) {
        await log.info('tick: queue is empty, nothing to do');
        return { run: false, reason: 'queue-empty' };
    }
    void log.info(`tick: processing ${item.bandName} — ${item.itemTitle} (id=${item.id})`);
    try {
        await downloadItem(item, config.format, config.inboxSubfolder);
        await markCompleted(item.id);
        return { run: true, itemId: item.id };
    } catch (err) {
        await markFailed(item.id, describeError(err));
        return { run: false, reason: `failed: ${describeError(err)}` };
    }
}

browser.alarms.onAlarm.addListener((alarm) => {
    if (alarm.name !== ALARM_NAME) return;
    void processOneTick().catch((err) => log.error(`alarm tick threw: ${describeError(err)}`));
});

async function ensureAlarm(): Promise<void> {
    const existing = await browser.alarms.get(ALARM_NAME);
    if (!existing) {
        await browser.alarms.create(ALARM_NAME, { periodInMinutes: 1 });
        await log.info(`alarm '${ALARM_NAME}' created (periodInMinutes=1)`);
    }
}

async function handleMessage(message: Message): Promise<MessageResponse> {
    try {
        switch (message.type) {
            case 'fetch-fan-id': {
                await log.info('fetch-fan-id requested');
                const ctx = await fetchHomepageContext();
                await log.info(`fan_id=${ctx.fanId}, verified=${ctx.isFanVerified}`);
                const data: FetchFanIdResult = ctx;
                return { ok: true, data };
            }
            case 'refresh-queue': {
                await log.info('refresh-queue requested');
                const ctx = await fetchHomepageContext();
                let pages = 0;
                const items = await paginateCollection(ctx.fanId, {
                    onProgress: (p) => {
                        pages = p.page;
                        void log.info(
                            `refresh page ${p.page}: +${p.pageItemCount} (total ${p.fetched})`,
                        );
                    },
                });
                const completed = new Set(await completedStore.get());
                const filtered = items.filter((i) => !completed.has(i.id));
                const skipped = items.length - filtered.length;
                await queueStore.set(filtered);
                await log.info(
                    `refresh-queue done: ${items.length} fetched, ${skipped} skipped (already completed), ${filtered.length} queued, ${pages} pages`,
                );
                const data: RefreshQueueResult = {
                    fetched: items.length,
                    pages,
                    queueSize: filtered.length,
                };
                return { ok: true, data };
            }
            case 'resolve-first-url': {
                const queue = await queueStore.get();
                if (queue.length === 0) {
                    throw new Error('queue is empty — run refresh-queue first');
                }
                const first = queue[0];
                if (!first) throw new Error('queue head is undefined');
                const config = await configStore.get();
                await log.info(
                    `resolve-first-url: ${first.bandName} — ${first.itemTitle} (${config.format})`,
                );
                const signed = await resolveSignedDownloadUrl(
                    first.downloadPageUrl,
                    config.format,
                );
                const data: ResolveFirstUrlResult = {
                    itemId: first.id,
                    bandName: first.bandName,
                    itemTitle: first.itemTitle,
                    signedUrlPreview: signed.slice(0, 160),
                };
                await log.info(`resolved: ${signed.slice(0, 80)}…`);
                return { ok: true, data };
            }
            case 'process-tick': {
                const result = await processOneTick(message.force === true);
                const decision: ProcessTickResult['decision'] = result.run
                    ? { run: true, itemId: result.itemId! }
                    : { run: false, reason: result.reason ?? 'unknown' };
                const data: ProcessTickResult = { decision };
                return { ok: true, data };
            }
            case 'set-enabled': {
                await stateStore.update((s) => ({ ...s, enabled: message.value }));
                await log.info(`enabled = ${message.value}`);
                const data: SetEnabledResult = { enabled: message.value };
                return { ok: true, data };
            }
            case 'reset-run-state': {
                await resetRunState();
                return { ok: true, data: { reset: true } };
            }
            case 'reset-config': {
                await configStore.set({ ...DEFAULT_CONFIG });
                await log.info('config reset to defaults');
                return { ok: true, data: { reset: true } };
            }
        }
    } catch (err) {
        const error = describeError(err);
        await log.error(`message ${message.type} failed: ${error}`);
        return { ok: false, error };
    }
}

browser.runtime.onMessage.addListener((message: Message) => handleMessage(message));

void Promise.all([
    configStore.update((c) => c),
    stateStore.update((s) => s),
])
    .then(() => recoverOrphanedInFlight())
    .then(() => ensureAlarm())
    .then(() => log.info(`background script loaded, version ${VERSION}`));
