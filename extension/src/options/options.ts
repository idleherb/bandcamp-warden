import { configStore } from '../shared/config.js';
import { log, logStore } from '../shared/log.js';
import { stateStore } from '../shared/storage.js';

const manifest = browser.runtime.getManifest();

function setVersion(): void {
    const el = document.getElementById('version');
    if (el) el.textContent = `${manifest.name} v${manifest.version}`;
}

function renderTable(tableId: string, rows: Record<string, unknown>): void {
    const table = document.getElementById(tableId);
    if (!table) return;
    table.replaceChildren(
        ...Object.entries(rows).map(([key, value]) => {
            const tr = document.createElement('tr');
            const tdKey = document.createElement('td');
            tdKey.className = 'k';
            tdKey.textContent = key;
            const tdVal = document.createElement('td');
            tdVal.className = 'v';
            tdVal.textContent = value === null ? '∅' : String(value);
            tr.append(tdKey, tdVal);
            return tr;
        }),
    );
}

async function renderConfig(): Promise<void> {
    renderTable('config', await configStore.get() as unknown as Record<string, unknown>);
}

async function renderState(): Promise<void> {
    renderTable('state', await stateStore.get() as unknown as Record<string, unknown>);
}

async function renderLog(): Promise<void> {
    const list = document.getElementById('log');
    if (!list) return;
    const entries = await logStore.get();
    const recent = entries.slice(-50).reverse();
    list.replaceChildren(
        ...recent.map((e) => {
            const li = document.createElement('li');
            li.className = `lvl-${e.level}`;
            li.textContent = `${e.ts}  [${e.level}]  ${e.msg}`;
            return li;
        }),
    );
}

async function refreshAll(): Promise<void> {
    await Promise.all([renderConfig(), renderState(), renderLog()]);
}

setVersion();
void refreshAll();

document.getElementById('refresh-log')?.addEventListener('click', () => void refreshAll());
document.getElementById('emit-test-log')?.addEventListener('click', async () => {
    await log.info(`test entry from options page at ${new Date().toLocaleTimeString()}`);
    await refreshAll();
});
