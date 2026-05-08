export type Message =
    | { type: 'fetch-fan-id' }
    | { type: 'refresh-queue' }
    | { type: 'resolve-first-url' };

export interface FetchFanIdResult {
    fanId: number;
    initialToken: string;
    itemCount: number | null;
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

export type MessageResponse<T = unknown> =
    | { ok: true; data: T }
    | { ok: false; error: string };

export async function send<T>(message: Message): Promise<MessageResponse<T>> {
    return (await browser.runtime.sendMessage(message)) as MessageResponse<T>;
}
