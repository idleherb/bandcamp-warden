import { log } from '../shared/log.js';
import type { Config, DownloadFormat, QueueItem } from '../shared/types.js';
import { resolveSignedDownloadUrl } from './api.js';

const DOWNLOAD_TIMEOUT_MS = 60 * 60 * 1000;
const UPLOAD_TIMEOUT_MS = 60 * 60 * 1000;

export class TransientDownloadError extends Error {
    constructor(public code: string) {
        super(`transient download failure: ${code}`);
        this.name = 'TransientDownloadError';
    }
}

// Errors that point to local filesystem / mount instability rather than the
// remote (Bandcamp) side. We retry these without burning the consecutive-
// failure budget that the real circuit breaker watches. FILE_NO_SPACE is
// intentionally NOT in here — it's a hard stop, not a transient blip.
const TRANSIENT_ERROR_CODES = new Set(['FILE_FAILED', 'FILE_TRANSIENT_ERROR']);

const FORMAT_EXT: Record<DownloadFormat, string> = {
    flac: 'flac',
    'mp3-v0': 'mp3',
    'mp3-320': 'mp3',
    'aac-hi': 'm4a',
    vorbis: 'ogg',
    alac: 'm4a',
    wav: 'wav',
    'aiff-lossless': 'aiff',
};

export interface DownloadOutcome {
    transport: 'sidecar-upload' | 'browser-download';
    bytes: number;
    targetHint: string;
    elapsedMs: number;
}

export async function downloadItem(
    item: QueueItem,
    config: Config,
): Promise<DownloadOutcome> {
    const start = performance.now();
    const signedUrl = await resolveSignedDownloadUrl(item.downloadPageUrl, config.format);
    if (config.transport === 'sidecar-upload') {
        const out = await uploadViaSidecar(item, signedUrl, config);
        return { ...out, elapsedMs: performance.now() - start };
    }
    const out = await downloadViaBrowser(item, signedUrl, config);
    return { ...out, elapsedMs: performance.now() - start };
}

// ---------- Path A: sidecar HTTP upload (Plan-E production path) ----------

async function uploadViaSidecar(
    item: QueueItem,
    signedUrl: string,
    config: Config,
): Promise<Omit<DownloadOutcome, 'elapsedMs'>> {
    if (!config.sidecarBaseUrl) {
        throw new Error('sidecarBaseUrl is empty (set it in Options)');
    }
    if (!config.sidecarAuthToken) {
        throw new Error('sidecarAuthToken is empty (set it in Options)');
    }
    const uploadExt = uploadExtensionFor(signedUrl, config.format);
    const uploadUrl = buildUploadUrl(config.sidecarBaseUrl, item.id, uploadExt);

    void log.info(
        `download starting (sidecar-upload): item ${item.id} (${item.bandName} — ${item.itemTitle})`,
    );

    // We tried streaming via TransformStream + body:ReadableStream + duplex:
    // 'half'. Result on a 362 MB album: only 23 bytes reached the sidecar.
    // Firefox MV2 extension-context fetch doesn't honor streaming request
    // bodies the way the spec implies — sympathetic upstream bug or just
    // not implemented for our context. So back to buffer-then-upload, with
    // an explicit Content-Length cap that protects RAM.
    const fetchAbort = new AbortController();
    const fetchTimer = setTimeout(() => fetchAbort.abort(), DOWNLOAD_TIMEOUT_MS);
    const uploadAbort = new AbortController();
    const uploadTimer = setTimeout(() => uploadAbort.abort(), UPLOAD_TIMEOUT_MS);

    let blob: Blob | null = null;
    try {
        // cache: 'no-store' tells Firefox not to copy the response into its
        // HTTP cache. Without it, every 200-500 MB ZIP gets retained in the
        // browser's memory cache even after we consume the blob, which is
        // what produced the 49 GB extension memory leak observed overnight.
        // The signed CDN URL is one-shot anyway — caching it is pure waste.
        const bandcampResp = await fetch(signedUrl, {
            credentials: 'include',
            signal: fetchAbort.signal,
            cache: 'no-store',
        });
        if (!bandcampResp.ok) {
            throw new Error(
                `bandcamp fetch ${bandcampResp.status} ${bandcampResp.statusText}`,
            );
        }
        // Pre-read Content-Length — abort BEFORE pulling the body if the
        // item exceeds the configured cap. Cheap and avoids RAM spikes.
        const claimedLength = parseInt(
            bandcampResp.headers.get('content-length') ?? '0', 10,
        );
        if (claimedLength > 0 && claimedLength > config.maxUploadBytes) {
            await bandcampResp.body?.cancel().catch(() => {});
            throw new Error(
                `item too large: content-length=${claimedLength} ` +
                `(${(claimedLength / (1024 * 1024 * 1024)).toFixed(2)} GB) ` +
                `exceeds maxUploadBytes=${config.maxUploadBytes} ` +
                `(${(config.maxUploadBytes / (1024 * 1024 * 1024)).toFixed(2)} GB). ` +
                `Raise the limit in Options if you trust this item.`,
            );
        }
        // Buffered read with progress logging + a streaming hard-cap so a
        // missing/lying Content-Length can't blow past the configured limit.
        blob = await readWithProgress(bandcampResp, item.id, config.maxUploadBytes);
    } finally {
        clearTimeout(fetchTimer);
    }

    const bytes = blob.size;
    if (bytes === 0) {
        blob = null;
        throw new Error('bandcamp fetch returned 0 bytes');
    }

    try {
        const upResp = await fetch(uploadUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/zip',
                'X-Warden-Auth': config.sidecarAuthToken,
            },
            body: blob,
            signal: uploadAbort.signal,
            cache: 'no-store',
        });
        // Drain response body — keeps Firefox from holding the connection.
        const respText = await upResp.text();
        if (!upResp.ok) {
            throw new Error(
                `sidecar upload ${upResp.status}: ${respText.slice(0, 200)}`,
            );
        }
        void log.info(
            `download done (sidecar-upload): item ${item.id}, ${bytes} bytes, ext=${uploadExt}`,
        );
        return {
            transport: 'sidecar-upload',
            bytes,
            targetHint: `${config.sidecarBaseUrl}/inbox/${item.id}.${uploadExt}`,
        };
    } finally {
        clearTimeout(uploadTimer);
        // Drop the only strong reference so GC can reclaim the ZIP bytes
        // immediately, regardless of whether the upload threw.
        blob = null;
    }
}

async function readWithProgress(
    response: Response,
    itemId: number,
    maxBytes: number,
): Promise<Blob> {
    if (!response.body) {
        return response.blob();
    }
    const totalRaw = response.headers.get('content-length');
    const total = totalRaw ? parseInt(totalRaw, 10) : 0;
    const contentType = response.headers.get('content-type') ?? 'application/zip';
    const reader = response.body.getReader();
    const chunks: BlobPart[] = [];
    let received = 0;
    let lastLogAt = performance.now();
    void log.info(
        `fetching item ${itemId}: total=${total > 0 ? `${total} bytes` : 'unknown'}`,
    );
    try {
        while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            if (!value) continue;
            received += value.byteLength;
            if (received > maxBytes) {
                await reader.cancel().catch(() => {});
                throw new Error(
                    `item ${itemId}: stream exceeded maxUploadBytes=${maxBytes} ` +
                    `mid-download (received ${received}); aborted to protect RAM`,
                );
            }
            chunks.push(value as BlobPart);
            const now = performance.now();
            if (now - lastLogAt >= PROGRESS_LOG_INTERVAL_MS) {
                const pct = total > 0 ? ` (${((received / total) * 100).toFixed(1)}%)` : '';
                const mb = (received / (1024 * 1024)).toFixed(1);
                void log.info(`fetching item ${itemId}: ${mb} MB received${pct}`);
                lastLogAt = now;
            }
        }
    } finally {
        try {
            reader.releaseLock();
        } catch {
            // ignore — reader may already be released after cancel
        }
    }
    return new Blob(chunks, { type: contentType });
}

function buildUploadUrl(baseUrl: string, itemId: number, ext: string): string {
    const trimmed = baseUrl.replace(/\/+$/, '');
    return `${trimmed}/inbox/upload?item_id=${itemId}&ext=${encodeURIComponent(ext)}`;
}

function uploadExtensionFor(signedUrl: string, format: DownloadFormat): string {
    // Bandcamp's signed URL path tells us album vs single track. Albums
    // come as a ZIP archive; single-track purchases come as a raw audio
    // file in the chosen format. If we save tracks as .zip, the watcher
    // sees BadZipFile and quarantines — that's what cost us ~13% of
    // overnight items before this fix.
    try {
        const path = new URL(signedUrl).pathname;
        if (path.includes('/download/track')) {
            return FORMAT_EXT[format];
        }
    } catch {
        // Fall through to album default.
    }
    return 'zip';
}

const PROGRESS_LOG_INTERVAL_MS = 5000;

// ---------- Path B: browser.downloads.download (SMB / local) ----------

async function downloadViaBrowser(
    item: QueueItem,
    signedUrl: string,
    config: Config,
): Promise<Omit<DownloadOutcome, 'elapsedMs'>> {
    const filename = payloadFilenameFor(item.id, signedUrl, config.format, config.inboxSubfolder);
    void log.info(
        `download starting (browser-download): ${filename} (item ${item.id}: ${item.bandName} — ${item.itemTitle})`,
    );
    const downloadId = await browser.downloads.download({
        url: signedUrl,
        filename,
        conflictAction: 'uniquify',
        saveAs: false,
    });
    await waitForDownload(downloadId, DOWNLOAD_TIMEOUT_MS);
    void log.info(`download done (browser-download): ${filename} (downloadId=${downloadId})`);
    return { transport: 'browser-download', bytes: 0, targetHint: filename };
}

function sanitizeSubfolder(raw: string): string {
    const trimmed = raw.replace(/^\/+|\/+$/g, '');
    const parts = trimmed.split('/').filter((p) => p.length > 0 && p !== '..' && p !== '.');
    return parts.join('/');
}

function payloadFilenameFor(
    itemId: number,
    signedUrl: string,
    format: DownloadFormat,
    subfolder: string,
): string {
    let kind: 'album' | 'track' | 'unknown' = 'unknown';
    try {
        const path = new URL(signedUrl).pathname;
        if (path.includes('/download/album')) kind = 'album';
        else if (path.includes('/download/track')) kind = 'track';
    } catch {
        // Malformed URL — fall through to unknown; .zip is a safe-ish default.
    }
    const ext = kind === 'track' ? FORMAT_EXT[format] : 'zip';
    const base = `bandcamp_${itemId}.${ext}`;
    const cleanSub = sanitizeSubfolder(subfolder);
    return cleanSub ? `${cleanSub}/${base}` : base;
}

function waitForDownload(downloadId: number, timeoutMs: number): Promise<void> {
    return new Promise((resolve, reject) => {
        let settled = false;
        const settle = (fn: () => void) => {
            if (settled) return;
            settled = true;
            browser.downloads.onChanged.removeListener(onChanged);
            clearTimeout(timeout);
            fn();
        };
        const timeout = setTimeout(() => {
            settle(() => reject(new Error(`download timed out after ${timeoutMs / 1000}s`)));
        }, timeoutMs);
        const onChanged = (delta: browser.downloads._OnChangedDownloadDelta) => {
            if (delta.id !== downloadId) return;
            const state = delta.state?.current;
            if (state === 'complete') {
                settle(() => resolve());
            } else if (state === 'interrupted') {
                const code = delta.error?.current ?? 'unknown';
                if (TRANSIENT_ERROR_CODES.has(code)) {
                    settle(() => reject(new TransientDownloadError(code)));
                } else {
                    settle(() => reject(new Error(`download interrupted: ${code}`)));
                }
            }
        };
        browser.downloads.onChanged.addListener(onChanged);
    });
}
