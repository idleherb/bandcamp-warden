import { configStore } from '../shared/config.js';
import { log } from '../shared/log.js';
import { stateStore } from '../shared/storage.js';

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

// Materialize defaults on every BG-script load. update(x => x) is idempotent:
// it reads (returning defaults if absent or merged-stored if present) and
// writes the same value back, so user-edited config survives reloads.
void Promise.all([
    configStore.update((c) => c),
    stateStore.update((s) => s),
]).then(() => log.info(`background script loaded, version ${VERSION}`));
