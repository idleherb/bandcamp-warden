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
    const uploadUrl = buildUploadUrl(config.sidecarBaseUrl, item.id);

    void log.info(
        `download starting (sidecar-upload): item ${item.id} (${item.bandName} — ${item.itemTitle})`,
    );

    // Memory discipline:
    //   - blob materializes the full ZIP in extension memory exactly once.
    //   - try/finally nulls the reference so GC can reclaim even on error.
    //   - upload Response body is drained explicitly (await arrayBuffer)
    //     so Firefox releases its internal connection buffers.
    //   - One item at a time is enforced upstream by the inFlight gate, so
    //     we never have two of these blobs in RAM concurrently.
    let blob: Blob | null = null;
    const fetchAbort = new AbortController();
    const fetchTimer = setTimeout(() => fetchAbort.abort(), DOWNLOAD_TIMEOUT_MS);
    try {
        const bandcampResp = await fetch(signedUrl, {
            credentials: 'include',
            signal: fetchAbort.signal,
        });
        if (!bandcampResp.ok) {
            throw new Error(`bandcamp fetch ${bandcampResp.status} ${bandcampResp.statusText}`);
        }
        // Read body via ReadableStream so we can log progress every few
        // seconds — without this, a stalled CDN connection looks identical
        // to a working slow download for many minutes. Memory cost is the
        // same as response.blob() since chunks are held until Blob is built.
        blob = await readWithProgress(bandcampResp, item.id);
    } finally {
        clearTimeout(fetchTimer);
    }

    const bytes = blob.size;
    if (bytes === 0) {
        blob = null;
        throw new Error('bandcamp fetch returned 0 bytes');
    }
    if (bytes > config.maxUploadBytes) {
        blob = null;
        throw new Error(
            `bandcamp ZIP is ${bytes} bytes, exceeds maxUploadBytes=${config.maxUploadBytes}`,
        );
    }

    const uploadAbort = new AbortController();
    const uploadTimer = setTimeout(() => uploadAbort.abort(), UPLOAD_TIMEOUT_MS);
    try {
        const upResp = await fetch(uploadUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/zip',
                'X-Warden-Auth': config.sidecarAuthToken,
            },
            body: blob,
            signal: uploadAbort.signal,
        });
        // Drain the response body even if we don't use it — leaving an
        // unclosed body keeps Firefox holding onto the connection.
        const respText = await upResp.text();
        if (!upResp.ok) {
            throw new Error(`sidecar upload ${upResp.status}: ${respText.slice(0, 200)}`);
        }
        void log.info(
            `download done (sidecar-upload): item ${item.id}, ${bytes} bytes`,
        );
        return {
            transport: 'sidecar-upload',
            bytes,
            targetHint: `${config.sidecarBaseUrl}/inbox/${item.id}`,
        };
    } finally {
        clearTimeout(uploadTimer);
        // Drop the only strong reference we hold so GC can reclaim the
        // ZIP bytes immediately, regardless of whether the upload threw.
        blob = null;
    }
}

function buildUploadUrl(baseUrl: string, itemId: number): string {
    const trimmed = baseUrl.replace(/\/+$/, '');
    return `${trimmed}/inbox/upload?item_id=${itemId}`;
}

const PROGRESS_LOG_INTERVAL_MS = 5000;

async function readWithProgress(response: Response, itemId: number): Promise<Blob> {
    if (!response.body) {
        // Some Firefox builds don't expose body on a Response; fall back to
        // .blob() and lose progress visibility for that one download.
        return response.blob();
    }
    const totalRaw = response.headers.get('content-length');
    const total = totalRaw ? parseInt(totalRaw, 10) : 0;
    const contentType = response.headers.get('content-type') ?? 'application/zip';
    const reader = response.body.getReader();
    // BlobPart[] rather than Uint8Array[] — TS narrows Uint8Array's
    // backing buffer to ArrayBuffer | SharedArrayBuffer, and Blob's
    // constructor signature only accepts ArrayBuffer-backed parts.
    // BlobPart is the declared type for the constructor's array.
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
            chunks.push(value as BlobPart);
            received += value.byteLength;
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
            // ignore — reader may already be released on cancel
        }
    }
    return new Blob(chunks, { type: contentType });
}

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
