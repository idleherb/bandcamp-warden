import { configStore } from '../shared/config.js';
import { log } from '../shared/log.js';
import type {
    FetchFanIdResult,
    Message,
    MessageResponse,
    RefreshQueueResult,
    ResolveFirstUrlResult,
} from '../shared/messages.js';
import { queueStore, stateStore } from '../shared/storage.js';
import {
    fetchHomepageContext,
    paginateCollection,
    resolveSignedDownloadUrl,
} from './api.js';

const VERSION = browser.runtime.getManifest().version;

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

async function handleMessage(message: Message): Promise<MessageResponse> {
    try {
        switch (message.type) {
            case 'fetch-fan-id': {
                await log.info('fetch-fan-id requested');
                const ctx = await fetchHomepageContext();
                await log.info(
                    `fan_id=${ctx.fanId}, verified=${ctx.isFanVerified}`,
                );
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
                await queueStore.set(items);
                await log.info(`refresh-queue done: ${items.length} items, ${pages} pages`);
                const data: RefreshQueueResult = {
                    fetched: items.length,
                    pages,
                    queueSize: items.length,
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
]).then(() => log.info(`background script loaded, version ${VERSION}`));
