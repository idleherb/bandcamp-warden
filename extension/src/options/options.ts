const manifest = browser.runtime.getManifest();
const statusEl = document.getElementById('status');
if (statusEl) {
    statusEl.textContent = `${manifest.name} v${manifest.version} — Phase 1 skeleton.`;
}
