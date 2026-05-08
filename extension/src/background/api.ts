import type { DownloadFormat, QueueItem } from '../shared/types.js';

const HOMEPAGE_URL = 'https://bandcamp.com/';
const COLLECTION_API = 'https://bandcamp.com/api/fancollection/1/collection_items';
const PAGE_SIZE = 100;
const INTER_PAGE_DELAY_MS = 500;

export class BandcampParseError extends Error {
    constructor(msg: string) {
        super(`Bandcamp HTML/API may have changed: ${msg}`);
        this.name = 'BandcampParseError';
    }
}

interface HomepageBlob {
    identities?: { fan?: { id?: number | string } };
    fan_data?: { fan_id?: number | string };
    pageContext?: { identity?: { fanId?: number | string } };
    collection_data?: { last_token?: string; item_count?: number };
}

interface CollectionItemRaw {
    item_id?: number;
    sale_item_id?: number;
    band_name?: string;
    item_title?: string;
    download_url?: string;
    item_url?: string;
}

interface CollectionResponse {
    items?: CollectionItemRaw[];
    more_available?: boolean;
    last_token?: string;
}

interface DownloadPageBlob {
    digital_items?: Array<{
        downloads?: Record<string, { url?: string }>;
    }>;
}

function parseBlob<T>(html: string, divId: string): T {
    const doc = new DOMParser().parseFromString(html, 'text/html');
    const div = doc.getElementById(divId);
    if (!div) throw new BandcampParseError(`<div id="${divId}"> not found`);
    const blob = div.getAttribute('data-blob');
    if (!blob) throw new BandcampParseError(`<div id="${divId}"> has no data-blob`);
    try {
        return JSON.parse(blob) as T;
    } catch {
        throw new BandcampParseError(`data-blob in <div id="${divId}"> is not valid JSON`);
    }
}

async function fetchHomepageBlob(): Promise<HomepageBlob> {
    const res = await fetch(HOMEPAGE_URL, { credentials: 'include' });
    if (!res.ok) throw new Error(`homepage fetch failed: HTTP ${res.status}`);
    const html = await res.text();
    return parseBlob<HomepageBlob>(html, 'pagedata');
}

function asPositiveNumber(v: unknown): number | null {
    const n = typeof v === 'string' ? Number(v) : v;
    return typeof n === 'number' && Number.isFinite(n) && n > 0 ? n : null;
}

interface HomepageContext {
    fanId: number;
    initialToken: string;
    itemCount: number | null;
}

export async function fetchHomepageContext(): Promise<HomepageContext> {
    const blob = await fetchHomepageBlob();
    const fanIdCandidates = [
        blob.identities?.fan?.id,
        blob.fan_data?.fan_id,
        blob.pageContext?.identity?.fanId,
    ];
    let fanId: number | null = null;
    for (const c of fanIdCandidates) {
        const n = asPositiveNumber(c);
        if (n !== null) {
            fanId = n;
            break;
        }
    }
    if (fanId === null) {
        throw new BandcampParseError('fan_id not found in homepage data-blob (are you logged in?)');
    }
    const last = blob.collection_data?.last_token;
    const initialToken =
        typeof last === 'string' && last.length > 0
            ? last
            : `${Math.floor(Date.now() / 1000)}::a::`;
    const itemCount = asPositiveNumber(blob.collection_data?.item_count);
    return { fanId, initialToken, itemCount };
}

export interface PaginateProgress {
    page: number;
    fetched: number;
    moreAvailable: boolean;
}

export interface PaginateOptions {
    onProgress?: (p: PaginateProgress) => void;
    maxPages?: number;
}

export async function paginateCollection(
    fanId: number,
    initialToken: string,
    options: PaginateOptions = {},
): Promise<QueueItem[]> {
    const { onProgress, maxPages = 100 } = options;
    const items: QueueItem[] = [];
    const seen = new Set<number>();
    let token = initialToken;
    for (let page = 1; page <= maxPages; page++) {
        const res = await fetch(COLLECTION_API, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                fan_id: fanId,
                older_than_token: token,
                count: PAGE_SIZE,
            }),
        });
        if (!res.ok) throw new Error(`collection_items POST failed: HTTP ${res.status}`);
        const json = (await res.json()) as CollectionResponse;
        const raw = json.items ?? [];
        for (const r of raw) {
            const id = asPositiveNumber(r.item_id);
            if (id === null || seen.has(id)) continue;
            const url = r.download_url;
            if (typeof url !== 'string' || url.length === 0) continue;
            seen.add(id);
            items.push({
                id,
                bandName: r.band_name ?? '',
                itemTitle: r.item_title ?? '',
                downloadPageUrl: url,
            });
        }
        const more = json.more_available === true;
        onProgress?.({ page, fetched: items.length, moreAvailable: more });
        const next = json.last_token;
        if (!more || typeof next !== 'string' || next.length === 0 || next === token) {
            break;
        }
        token = next;
        if (page < maxPages) {
            await new Promise((resolve) => setTimeout(resolve, INTER_PAGE_DELAY_MS));
        }
    }
    return items;
}

export async function resolveSignedDownloadUrl(
    downloadPageUrl: string,
    format: DownloadFormat,
): Promise<string> {
    const res = await fetch(downloadPageUrl, { credentials: 'include' });
    if (!res.ok) throw new Error(`download page fetch failed: HTTP ${res.status}`);
    const html = await res.text();
    const blob = parseBlob<DownloadPageBlob>(html, 'pagedata');
    const item = blob.digital_items?.[0];
    if (!item) throw new BandcampParseError('digital_items[0] missing on download page');
    const dl = item.downloads?.[format];
    if (!dl?.url) {
        const available = Object.keys(item.downloads ?? {}).join(', ') || '(none)';
        throw new BandcampParseError(
            `format "${format}" not available; got [${available}]`,
        );
    }
    return dl.url;
}
