import type { DownloadFormat, QueueItem } from '../shared/types.js';

const HOMEPAGE_URL = 'https://bandcamp.com/';
const COLLECTION_API = 'https://bandcamp.com/api/fancollection/1/collection_items';
const PAGE_SIZE = 100;
const INTER_PAGE_DELAY_MS = 500;
const MAX_PAGES = 200;

export class BandcampParseError extends Error {
    constructor(msg: string) {
        super(`Bandcamp HTML/API may have changed: ${msg}`);
        this.name = 'BandcampParseError';
    }
}

interface HomepageBlob {
    pageContext?: {
        identity?: {
            fanId?: number | string;
            isFanVerified?: boolean;
        };
    };
    [k: string]: unknown;
}

interface CollectionItemRaw {
    item_id?: number;
    sale_item_id?: number;
    sale_item_type?: string;
    band_name?: string;
    item_title?: string;
    token?: string;
    item_type?: string;
    item_url?: string;
    tralbum_type?: string;
    featured_track_title?: string;
    purchased?: string | number;
    added?: string | number;
    art_id?: number;
}

interface CollectionResponse {
    items?: CollectionItemRaw[];
    redownload_urls?: Record<string, string>;
    more_available?: boolean;
}

interface DownloadPageBlob {
    digital_items?: Array<{
        item_id?: number;
        downloads?: Record<string, { url?: string }>;
    }>;
}

function parseBlob<T>(html: string, divId: string): T {
    const doc = new DOMParser().parseFromString(html, 'text/html');
    const div = doc.getElementById(divId);
    if (!div) {
        const allDivs = Array.from(doc.querySelectorAll('div[id][data-blob]'))
            .map((d) => d.id)
            .join(', ');
        throw new BandcampParseError(
            `<div id="${divId}"> not found (divs with data-blob: ${allDivs || 'none'})`,
        );
    }
    const blob = div.getAttribute('data-blob');
    if (!blob) throw new BandcampParseError(`<div id="${divId}"> has no data-blob`);
    try {
        return JSON.parse(blob) as T;
    } catch {
        throw new BandcampParseError(`data-blob in <div id="${divId}"> is not valid JSON`);
    }
}

function asPositiveNumber(v: unknown): number | null {
    const n = typeof v === 'string' ? Number(v) : v;
    return typeof n === 'number' && Number.isFinite(n) && n > 0 ? n : null;
}

function formatBandcampDate(v: unknown): string | undefined {
    // Bandcamp's collection_items returns purchased/added as either a Unix
    // epoch (seconds) or a pre-formatted "DD MMM YYYY HH:MM:SS GMT" string.
    // Existing bandcamp_<id>.json entries use the GMT string form, so we
    // normalize both to that.
    if (typeof v === 'string' && v.length > 0) return v;
    if (typeof v === 'number' && Number.isFinite(v)) {
        return new Date(v * 1000).toUTCString();
    }
    return undefined;
}

export interface HomepageContext {
    fanId: number;
    isFanVerified: boolean;
}

export async function fetchHomepageContext(): Promise<HomepageContext> {
    const res = await fetch(HOMEPAGE_URL, { credentials: 'include' });
    if (!res.ok) throw new Error(`homepage fetch failed: HTTP ${res.status}`);
    const html = await res.text();
    const blob = parseBlob<HomepageBlob>(html, 'HomepageApp');
    const identity = blob.pageContext?.identity;
    if (!identity || typeof identity !== 'object') {
        throw new BandcampParseError(
            `pageContext.identity missing — are you logged in? top-level keys: ${Object.keys(blob).join(', ')}`,
        );
    }
    const fanId = asPositiveNumber(identity.fanId);
    if (fanId === null) {
        throw new BandcampParseError(
            `fanId missing in pageContext.identity. Identity keys: ${Object.keys(identity).join(', ')}`,
        );
    }
    return { fanId, isFanVerified: identity.isFanVerified === true };
}

function initialPaginationToken(): string {
    return `${Math.floor(Date.now() / 1000)}:0:a::`;
}

export interface PaginateProgress {
    page: number;
    fetched: number;
    pageItemCount: number;
}

export interface PaginateOptions {
    onProgress?: (p: PaginateProgress) => void;
    maxPages?: number;
}

export async function paginateCollection(
    fanId: number,
    options: PaginateOptions = {},
): Promise<QueueItem[]> {
    const { onProgress, maxPages = MAX_PAGES } = options;
    const items: QueueItem[] = [];
    const seen = new Set<number>();
    let token = initialPaginationToken();

    for (let page = 1; page <= maxPages; page++) {
        const res = await fetch(COLLECTION_API, {
            method: 'POST',
            credentials: 'include',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                fan_id: fanId,
                count: PAGE_SIZE,
                older_than_token: token,
            }),
        });
        if (!res.ok) throw new Error(`collection_items POST failed: HTTP ${res.status}`);
        const json = (await res.json()) as CollectionResponse;
        const raw = json.items ?? [];
        if (raw.length === 0) {
            onProgress?.({ page, fetched: items.length, pageItemCount: 0 });
            break;
        }
        const redownloadUrls = json.redownload_urls;
        if (!redownloadUrls || typeof redownloadUrls !== 'object') {
            throw new BandcampParseError('collection_items response missing redownload_urls');
        }

        let pageAdded = 0;
        let lastToken: string | null = null;
        for (const r of raw) {
            if (typeof r.token === 'string' && r.token.length > 0) lastToken = r.token;
            const id = asPositiveNumber(r.item_id);
            if (id === null || seen.has(id)) continue;
            const saleType = r.sale_item_type;
            const saleId = r.sale_item_id;
            if (!saleType || saleId == null) continue;
            const key = `${saleType}${saleId}`;
            const url = redownloadUrls[key];
            if (typeof url !== 'string' || url.length === 0) continue;
            seen.add(id);
            items.push({
                id,
                bandName: r.band_name ?? '',
                itemTitle: r.item_title ?? '',
                downloadPageUrl: url,
                itemUrl: typeof r.item_url === 'string' && r.item_url.length > 0 ? r.item_url : undefined,
                tralbumType: r.tralbum_type ?? r.sale_item_type,
                featuredTrack: r.featured_track_title,
                purchasedAt: formatBandcampDate(r.purchased),
                addedAt: formatBandcampDate(r.added),
                artId: typeof r.art_id === 'number' && Number.isFinite(r.art_id) && r.art_id > 0 ? r.art_id : undefined,
            });
            pageAdded++;
        }
        onProgress?.({ page, fetched: items.length, pageItemCount: pageAdded });

        if (lastToken === null || lastToken === token) break;
        token = lastToken;
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
