const VERSION = browser.runtime.getManifest().version;

void browser.browserAction.setBadgeBackgroundColor({ color: '#629aa9' });

browser.runtime.onInstalled.addListener((details) => {
    console.log(`[warden] installed/updated: ${details.reason}, version ${VERSION}`);
});

browser.runtime.onStartup.addListener(() => {
    console.log(`[warden] browser startup, version ${VERSION}`);
});

browser.browserAction.onClicked.addListener(() => {
    void browser.runtime.openOptionsPage();
});

console.log(`[warden] background script loaded, version ${VERSION}`);
