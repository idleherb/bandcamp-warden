import { configStore, DEFAULT_CONFIG } from '../shared/config.js';
import { log } from '../shared/log.js';
import type {
    FetchFanIdResult,
    Message,
    MessageResponse,
    ProcessTickResult,
    RefreshQueueResult,
    ResolveFirstUrlResult,
    SaveConfigResult,
    SetEnabledResult,
    SyncCompletedResult,
    TestSidecarResult,
} from '../shared/messages.js';
import { completedStore, queueStore, stateStore } from '../shared/storage.js';
import type { Config } from '../shared/types.js';
import {
    fetchHomepageContext,
    paginateCollection,
    resolveSignedDownloadUrl,
} from './api.js';
import { downloadItem, TransientDownloadError } from './downloader.js';
import { dailyResetIfNeeded, shouldRunNow } from './pacing.js';
import {
    markCompleted,
    markFailed,
    popNextItem,
    recoverOrphanedInFlight,
    requeueItemHead,
    resetRunState,
} from './queue.js';

const TRANSIENT_PAUSE_SEC = 120;

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

function sidecarUrlOrThrow(cfg: Config, path: string): string {
    if (!cfg.sidecarBaseUrl) throw new Error('sidecarBaseUrl is empty (set it in Options)');
    const trimmed = cfg.sidecarBaseUrl.replace(/\/+$/, '');
    return `${trimmed}${path.startsWith('/') ? path : `/${path}`}`;
}

interface SyncCompletedOutcome {
    addedToCompleted: number;
    completedSetSize: number;
    sidecarReportedCount: number;
    scannedFolder: string;
}

/**
 * Pull the completed-ids list from the sidecar and union it into the
 * extension's local completedStore. Returns the outcome stats. Throws on
 * failure (network, auth, missing config) so callers can decide whether
 * to surface or swallow the error.
 */
async function syncCompletedFromSidecar(): Promise<SyncCompletedOutcome> {
    const cfg = await configStore.get();
    const url = sidecarUrlOrThrow(cfg, '/list-completed-ids');
    const res = await fetch(url, { method: 'GET' });
    if (!res.ok) {
        throw new Error(`${url} returned HTTP ${res.status} ${res.statusText}`);
    }
    const json = (await res.json()) as {
        completed_ids?: number[];
        count?: number;
        scanned_folder?: string;
    };
    const incoming = Array.isArray(json.completed_ids) ? json.completed_ids : [];
    let addedToCompleted = 0;
    const next = await completedStore.update((cur) => {
        const have = new Set(cur);
        const merged = [...cur];
        for (const id of incoming) {
            if (!have.has(id)) {
                have.add(id);
                merged.push(id);
                addedToCompleted++;
            }
        }
        return merged;
    });
    return {
        addedToCompleted,
        completedSetSize: next.length,
        sidecarReportedCount:
            typeof json.count === 'number' ? json.count : incoming.length,
        scannedFolder: typeof json.scanned_folder === 'string' ? json.scanned_folder : '',
    };
}

/**
 * Best-effort variant for code paths that should still proceed even if
 * the sync fails (sidecar momentarily unreachable, no config yet, etc.).
 * Logs a warning instead of throwing.
 */
async function syncCompletedFromSidecarBestEffort(label: string): Promise<void> {
    try {
        const cfg = await configStore.get();
        if (!cfg.sidecarBaseUrl || !cfg.sidecarAuthToken) {
            // Sidecar isn't configured yet; nothing to sync.
            return;
        }
        const out = await syncCompletedFromSidecar();
        if (out.addedToCompleted > 0) {
            await log.info(
                `${label}: auto-synced from sidecar (+${out.addedToCompleted} ids, ` +
                `set size ${out.completedSetSize}/${out.sidecarReportedCount})`,
            );
        }
    } catch (err) {
        await log.warn(
            `${label}: auto-sync from sidecar failed (continuing with local cache): ${describeError(err)}`,
        );
    }
}

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
        await downloadItem(item, config);
        await markCompleted(item.id);
        return { run: true, itemId: item.id };
    } catch (err) {
        if (err instanceof TransientDownloadError) {
            await requeueItemHead(item, TRANSIENT_PAUSE_SEC);
            void log.warn(
                `transient ${err.code} on item ${item.id}; requeued, pausing ${TRANSIENT_PAUSE_SEC}s (no failure counter increment)`,
            );
            return { run: false, reason: `transient (${err.code}), requeued` };
        }
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
                // Auto-sync completed IDs from sidecar before dedupe so we
                // see the disk truth (filesystem markers) and don't re-queue
                // items that are already on the NAS. Best-effort — if the
                // sidecar is unreachable we still proceed with local cache.
                await syncCompletedFromSidecarBestEffort('refresh-queue');
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
                // After wiping local state, immediately re-seed completed
                // from the sidecar's disk markers — otherwise the next
                // refresh-queue would treat 297 already-finalized items as
                // pending. Best-effort.
                await syncCompletedFromSidecarBestEffort('reset-run-state');
                return { ok: true, data: { reset: true } };
            }
            case 'reset-config': {
                await configStore.set({ ...DEFAULT_CONFIG });
                await log.info('config reset to defaults');
                return { ok: true, data: { reset: true } };
            }
            case 'save-config': {
                const next = await configStore.update((cur) => ({ ...cur, ...message.config }));
                await log.info(
                    `config saved (transport=${next.transport}, sidecar=${next.sidecarBaseUrl})`,
                );
                const data: SaveConfigResult = { config: next };
                return { ok: true, data };
            }
            case 'sync-completed-from-sidecar': {
                await log.info('sync-completed-from-sidecar requested (manual)');
                const out = await syncCompletedFromSidecar();
                await log.info(
                    `sync-completed: sidecar reported ${out.sidecarReportedCount}, ` +
                    `added ${out.addedToCompleted} new, completed-set is now ${out.completedSetSize}`,
                );
                const data: SyncCompletedResult = out;
                return { ok: true, data };
            }
            case 'test-sidecar': {
                const cfg = await configStore.get();
                if (!cfg.sidecarBaseUrl) throw new Error('sidecarBaseUrl is empty');
                const baseTrimmed = cfg.sidecarBaseUrl.replace(/\/+$/, '');
                const url = `${baseTrimmed}/inbox-status`;
                let originPattern = '';
                try {
                    const u = new URL(baseTrimmed);
                    originPattern = `${u.protocol}//${u.host}/*`;
                } catch {
                    throw new Error(`sidecarBaseUrl is not a valid URL: ${baseTrimmed}`);
                }
                const start = performance.now();
                let res: Response;
                try {
                    res = await fetch(url, { method: 'GET' });
                } catch (err) {
                    const durationMs = performance.now() - start;
                    // TypeError: NetworkError almost always means the host
                    // permission isn't granted. Check explicitly so the user
                    // sees a useful message instead of Firefox's stub.
                    let permissionGranted = false;
                    try {
                        permissionGranted = await browser.permissions.contains({
                            origins: [originPattern],
                        });
                    } catch {
                        // ignore
                    }
                    if (!permissionGranted) {
                        throw new Error(
                            `fetch ${url} blocked: host permission for ${originPattern} not granted. ` +
                            `Click Save (with the URL filled in) and accept the Firefox permission prompt.`,
                        );
                    }
                    throw new Error(
                        `fetch ${url} threw after ${durationMs.toFixed(0)}ms: ${describeError(err)} ` +
                        `(permission IS granted; check sidecar reachability)`,
                    );
                }
                const durationMs = performance.now() - start;
                const text = await res.text();
                let body: unknown = null;
                try {
                    body = JSON.parse(text);
                } catch {
                    body = text.slice(0, 200);
                }
                const data: TestSidecarResult = {
                    ok: res.ok,
                    statusCode: res.status,
                    statusText: res.statusText,
                    durationMs,
                    inboxStatus: body,
                };
                return { ok: true, data };
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
    .then(() => syncCompletedFromSidecarBestEffort('startup'))
    .then(() => log.info(`background script loaded, version ${VERSION}`));
