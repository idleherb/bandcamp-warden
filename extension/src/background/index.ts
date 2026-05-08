import { configStore } from '../shared/config.js';
import { log } from '../shared/log.js';
import { stateStore } from '../shared/storage.js';

const VERSION = browser.runtime.getManifest().version;

void browser.browserAction.setBadgeBackgroundColor({ color: '#629aa9' });

browser.runtime.onInstalled.addListener(async (details) => {
    await log.info(`installed/updated: ${details.reason}, version ${VERSION}`);
    // Touch stores so defaults materialize and become inspectable in DevTools.
    await configStore.get();
    await stateStore.get();
});

browser.runtime.onStartup.addListener(async () => {
    await log.info(`browser startup, version ${VERSION}`);
});

browser.browserAction.onClicked.addListener(() => {
    void browser.runtime.openOptionsPage();
});

void log.info(`background script loaded, version ${VERSION}`);
