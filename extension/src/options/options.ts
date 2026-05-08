import { configStore } from '../shared/config.js';
import { log, logStore } from '../shared/log.js';
import type {
    FetchFanIdResult,
    Message,
    ProcessTickResult,
    RefreshQueueResult,
    ResolveFirstUrlResult,
    SetEnabledResult,
} from '../shared/messages.js';
import { send } from '../shared/messages.js';
import {
    completedStore,
    failedStore,
    queueStore,
    stateStore,
} from '../shared/storage.js';

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
    renderTable('config', (await configStore.get()) as unknown as Record<string, unknown>);
}

async function renderState(): Promise<void> {
    renderTable('state', (await stateStore.get()) as unknown as Record<string, unknown>);
}

async function renderQueue(): Promise<void> {
    const [queue, completed, failed] = await Promise.all([
        queueStore.get(),
        completedStore.get(),
        failedStore.get(),
    ]);
    const count = document.getElementById('queue-count');
    const preview = document.getElementById('queue-preview');
    if (count) {
        count.textContent = `(${queue.length} queued · ${completed.length} completed · ${failed.length} failed)`;
    }
    if (preview) {
        if (queue.length === 0) {
            preview.textContent = 'empty — click "Refresh queue" to populate.';
        } else {
            const head = queue.slice(0, 3).map((q) => `${q.bandName} — ${q.itemTitle}`);
            preview.textContent = `head: ${head.join(' / ')}${queue.length > 3 ? ' …' : ''}`;
        }
    }
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
    await Promise.all([renderConfig(), renderState(), renderQueue(), renderLog()]);
}

function setResult(elId: string, text: string, isError = false): void {
    const el = document.getElementById(elId);
    if (!el) return;
    el.textContent = text;
    el.classList.toggle('error', isError);
}

function setButtonsDisabled(disabled: boolean): void {
    document
        .querySelectorAll<HTMLButtonElement>('section .buttons button')
        .forEach((b) => {
            b.disabled = disabled;
        });
}

async function runMessage<T>(message: Message, label: string, resultElId: string): Promise<void> {
    setResult(resultElId, `${label}…`);
    setButtonsDisabled(true);
    const start = performance.now();
    try {
        const res = await send<T>(message);
        const elapsed = `${((performance.now() - start) / 1000).toFixed(1)}s`;
        if (res.ok) {
            setResult(resultElId, `${label} OK in ${elapsed}\n\n${JSON.stringify(res.data, null, 2)}`);
        } else {
            setResult(resultElId, `${label} FAILED in ${elapsed}\n\n${res.error}`, true);
        }
    } catch (err) {
        setResult(resultElId, `${label} threw: ${String(err)}`, true);
    } finally {
        setButtonsDisabled(false);
        await refreshAll();
    }
}

setVersion();
void refreshAll();

document.getElementById('refresh-log')?.addEventListener('click', () => void refreshAll());
document.getElementById('emit-test-log')?.addEventListener('click', async () => {
    await log.info(`test entry from options page at ${new Date().toLocaleTimeString()}`);
    await refreshAll();
});

document
    .getElementById('btn-fan-id')
    ?.addEventListener('click', () =>
        void runMessage<FetchFanIdResult>({ type: 'fetch-fan-id' }, 'fetch-fan-id', 'api-result'),
    );

document
    .getElementById('btn-refresh-queue')
    ?.addEventListener('click', () =>
        void runMessage<RefreshQueueResult>({ type: 'refresh-queue' }, 'refresh-queue', 'api-result'),
    );

document
    .getElementById('btn-resolve-first')
    ?.addEventListener('click', () =>
        void runMessage<ResolveFirstUrlResult>(
            { type: 'resolve-first-url' },
            'resolve-first-url',
            'api-result',
        ),
    );

document
    .getElementById('btn-tick')
    ?.addEventListener('click', () =>
        void runMessage<ProcessTickResult>(
            { type: 'process-tick' },
            'process-tick',
            'scheduler-result',
        ),
    );

document
    .getElementById('btn-tick-force')
    ?.addEventListener('click', () =>
        void runMessage<ProcessTickResult>(
            { type: 'process-tick', force: true },
            'process-tick (forced)',
            'scheduler-result',
        ),
    );

document.getElementById('btn-toggle-enabled')?.addEventListener('click', async () => {
    const cur = await stateStore.get();
    await runMessage<SetEnabledResult>(
        { type: 'set-enabled', value: !cur.enabled },
        `set-enabled=${!cur.enabled}`,
        'scheduler-result',
    );
});

document.getElementById('btn-reset-state')?.addEventListener('click', () =>
    void runMessage<{ reset: boolean }>(
        { type: 'reset-run-state' },
        'reset-run-state',
        'scheduler-result',
    ),
);
