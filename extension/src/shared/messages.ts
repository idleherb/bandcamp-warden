export type Message =
    | { type: 'fetch-fan-id' }
    | { type: 'refresh-queue' }
    | { type: 'resolve-first-url' }
    | { type: 'process-tick'; force?: boolean }
    | { type: 'set-enabled'; value: boolean }
    | { type: 'reset-run-state' }
    | { type: 'reset-config' }
    | { type: 'save-config'; config: Partial<import('./types.js').Config> }
    | { type: 'test-sidecar' };

export interface FetchFanIdResult {
    fanId: number;
    isFanVerified: boolean;
}

export interface RefreshQueueResult {
    fetched: number;
    pages: number;
    queueSize: number;
}

export interface ResolveFirstUrlResult {
    itemId: number;
    bandName: string;
    itemTitle: string;
    signedUrlPreview: string;
}

export interface ProcessTickResult {
    decision: { run: true; itemId: number } | { run: false; reason: string };
}

export interface SetEnabledResult {
    enabled: boolean;
}

export interface SaveConfigResult {
    config: import('./types.js').Config;
}

export interface TestSidecarResult {
    ok: boolean;
    statusCode: number | null;
    statusText: string;
    durationMs: number;
    sidecarVersion?: string;
    inboxStatus?: unknown;
}

export type MessageResponse<T = unknown> =
    | { ok: true; data: T }
    | { ok: false; error: string };

export async function send<T>(message: Message): Promise<MessageResponse<T>> {
    return (await browser.runtime.sendMessage(message)) as MessageResponse<T>;
}
