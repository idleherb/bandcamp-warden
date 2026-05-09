import { configStore } from '../shared/config.js';
import { log, logStore } from '../shared/log.js';
import type {
    FetchFanIdResult,
    Message,
    ProcessTickResult,
    RefreshQueueResult,
    ResolveFirstUrlResult,
    SaveConfigResult,
    SetEnabledResult,
    SyncCompletedResult,
    TestSidecarResult,
} from '../shared/messages.js';
import { send } from '../shared/messages.js';
import {
    completedStore,
    failedStore,
    queueStore,
    stateStore,
} from '../shared/storage.js';
import type { Config } from '../shared/types.js';

const manifest = browser.runtime.getManifest();

const CONFIG_FIELDS = [
    'transport',
    'sidecarBaseUrl',
    'sidecarAuthToken',
    'dailyQuota',
    'format',
    'minDelaySec',
    'maxDelaySec',
    'circuitBreakerThreshold',
    'circuitBreakerPauseSec',
    'inboxSubfolder',
    'maxUploadBytes',
] as const;

const NUMBER_FIELDS = new Set<(typeof CONFIG_FIELDS)[number]>([
    'dailyQuota',
    'minDelaySec',
    'maxDelaySec',
    'circuitBreakerThreshold',
    'circuitBreakerPauseSec',
    'maxUploadBytes',
]);

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

async function renderConfigForm(): Promise<void> {
    const cfg = await configStore.get();
    const form = document.getElementById('config-form') as HTMLFormElement | null;
    if (!form) return;
    for (const field of CONFIG_FIELDS) {
        const input = form.elements.namedItem(field) as
            | HTMLInputElement
            | HTMLSelectElement
            | null;
        if (!input) continue;
        const v = (cfg as unknown as Record<string, unknown>)[field];
        input.value = v === undefined || v === null ? '' : String(v);
    }
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
    await Promise.all([renderConfigForm(), renderState(), renderQueue(), renderLog()]);
}

function setResult(elId: string, text: string, isError = false): void {
    const el = document.getElementById(elId);
    if (!el) return;
    el.textContent = text;
    el.classList.toggle('error', isError);
}

function setButtonsDisabled(disabled: boolean): void {
    // Reset-run-state is the emergency-stop. It must stay clickable even
    // when another operation is in flight, otherwise a runaway download
    // (e.g. a 25 GB compilation) can't be aborted from the UI.
    document
        .querySelectorAll<HTMLButtonElement>(
            'section .buttons button:not(#btn-reset-state), ' +
            'button#btn-save-config, button#btn-test-sidecar, button#btn-reset-config',
        )
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

function readFormConfig(): Partial<Config> {
    const form = document.getElementById('config-form') as HTMLFormElement | null;
    if (!form) return {};
    const out: Partial<Config> = {};
    for (const field of CONFIG_FIELDS) {
        const input = form.elements.namedItem(field) as
            | HTMLInputElement
            | HTMLSelectElement
            | null;
        if (!input) continue;
        const raw = input.value;
        if (NUMBER_FIELDS.has(field)) {
            const n = Number(raw);
            if (Number.isFinite(n)) {
                (out as Record<string, unknown>)[field] = n;
            }
        } else {
            (out as Record<string, unknown>)[field] = raw;
        }
    }
    return out;
}

async function ensureSidecarPermission(baseUrl: string): Promise<boolean> {
    if (!baseUrl) return true;
    let origin: string;
    try {
        const u = new URL(baseUrl);
        origin = `${u.protocol}//${u.host}/*`;
    } catch {
        return true;
    }
    // CRITICAL: permissions.request must be the FIRST await after the
    // user gesture. A pre-check via permissions.contains breaks the
    // gesture chain and the request silently fails. request() is
    // idempotent — returns true without prompting if already granted.
    return browser.permissions.request({ origins: [origin] });
}

async function handleSaveConfig(event: Event): Promise<void> {
    event.preventDefault();
    const partial = readFormConfig();
    if (typeof partial.sidecarBaseUrl === 'string' && partial.sidecarBaseUrl) {
        const granted = await ensureSidecarPermission(partial.sidecarBaseUrl);
        if (!granted) {
            setResult(
                'config-result',
                `Permission denied for ${partial.sidecarBaseUrl}. Cannot save until granted.`,
                true,
            );
            return;
        }
    }
    await runMessage<SaveConfigResult>(
        { type: 'save-config', config: partial },
        'save-config',
        'config-result',
    );
}

setVersion();
void refreshAll();

document.getElementById('config-form')?.addEventListener('submit', (e) => void handleSaveConfig(e));
document.getElementById('btn-test-sidecar')?.addEventListener('click', () =>
    void runMessage<TestSidecarResult>(
        { type: 'test-sidecar' },
        'test-sidecar',
        'config-result',
    ),
);
document.getElementById('btn-sync-completed')?.addEventListener('click', () =>
    void runMessage<SyncCompletedResult>(
        { type: 'sync-completed-from-sidecar' },
        'sync-completed-from-sidecar',
        'config-result',
    ),
);
document.getElementById('btn-reset-config')?.addEventListener('click', () =>
    void runMessage<{ reset: boolean }>(
        { type: 'reset-config' },
        'reset-config',
        'config-result',
    ),
);

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
