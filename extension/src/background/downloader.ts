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

    // Streaming pipeline:
    //   bandcamp fetch (response.body) → byte-counter TransformStream → upload fetch (body)
    // Neither end materializes the ZIP in RAM — chunks of ~64KB flow
    // through, and the Sidecar's POST handler streams them straight to
    // the .partial file. Memory stays roughly constant regardless of item
    // size, so a 25 GB compilation works the same as a 200 MB album.
    const fetchAbort = new AbortController();
    const fetchTimer = setTimeout(() => fetchAbort.abort(), DOWNLOAD_TIMEOUT_MS);
    const uploadAbort = new AbortController();
    const uploadTimer = setTimeout(() => uploadAbort.abort(), UPLOAD_TIMEOUT_MS);

    let bytesStreamed = 0;
    let progressTimer: ReturnType<typeof setInterval> | undefined;

    try {
        const bandcampResp = await fetch(signedUrl, {
            credentials: 'include',
            signal: fetchAbort.signal,
        });
        if (!bandcampResp.ok) {
            throw new Error(
                `bandcamp fetch ${bandcampResp.status} ${bandcampResp.statusText}`,
            );
        }
        if (!bandcampResp.body) {
            throw new Error('bandcamp response has no body — Firefox build too old?');
        }

        // Pre-read content-length — abort BEFORE we start streaming if the
        // item exceeds the configured cap. Avoids opening the upload
        // connection just to tear it down mid-flight.
        const claimedLength = parseInt(
            bandcampResp.headers.get('content-length') ?? '0', 10,
        );
        if (claimedLength > 0 && claimedLength > config.maxUploadBytes) {
            await bandcampResp.body.cancel().catch(() => {});
            throw new Error(
                `item too large: content-length=${claimedLength} ` +
                `(${(claimedLength / (1024 * 1024 * 1024)).toFixed(2)} GB) ` +
                `exceeds maxUploadBytes=${config.maxUploadBytes} ` +
                `(${(config.maxUploadBytes / (1024 * 1024 * 1024)).toFixed(2)} GB). ` +
                `Raise the limit in Options if you trust this item.`,
            );
        }
        const totalForLog = claimedLength;
        void log.info(
            `streaming item ${item.id}: total=${totalForLog > 0 ? `${totalForLog} bytes` : 'unknown'}`,
        );

        // Byte-counting passthrough. Also enforces the cap mid-stream
        // for cases where Content-Length lied or was missing.
        const limit = config.maxUploadBytes;
        const counter = new TransformStream<Uint8Array, Uint8Array>({
            transform(chunk, controller) {
                bytesStreamed += chunk.byteLength;
                if (bytesStreamed > limit) {
                    controller.error(
                        new Error(
                            `stream exceeded maxUploadBytes=${limit} mid-download ` +
                            `(received ${bytesStreamed}); aborted`,
                        ),
                    );
                    return;
                }
                controller.enqueue(chunk);
            },
        });
        const piped = bandcampResp.body.pipeThrough(counter);

        progressTimer = setInterval(() => {
            const mb = (bytesStreamed / (1024 * 1024)).toFixed(1);
            const pct = totalForLog > 0
                ? ` (${((bytesStreamed / totalForLog) * 100).toFixed(1)}%)`
                : '';
            void log.info(`streaming item ${item.id}: ${mb} MB${pct}`);
        }, PROGRESS_LOG_INTERVAL_MS);

        // duplex: 'half' is required for streaming request bodies in
        // fetch. Standard TS DOM types haven't caught up to the spec
        // yet, so we cast.
        const upResp = await fetch(uploadUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/zip',
                'X-Warden-Auth': config.sidecarAuthToken,
            },
            body: piped,
            signal: uploadAbort.signal,
            duplex: 'half',
        } as RequestInit & { duplex: 'half' });

        // Drain response body — keeps Firefox from holding the connection.
        const respText = await upResp.text();
        if (!upResp.ok) {
            throw new Error(
                `sidecar upload ${upResp.status}: ${respText.slice(0, 200)}`,
            );
        }
        void log.info(
            `download done (sidecar-upload): item ${item.id}, ${bytesStreamed} bytes`,
        );
        return {
            transport: 'sidecar-upload',
            bytes: bytesStreamed,
            targetHint: `${config.sidecarBaseUrl}/inbox/${item.id}`,
        };
    } finally {
        clearTimeout(fetchTimer);
        clearTimeout(uploadTimer);
        if (progressTimer !== undefined) clearInterval(progressTimer);
    }
}

function buildUploadUrl(baseUrl: string, itemId: number): string {
    const trimmed = baseUrl.replace(/\/+$/, '');
    return `${trimmed}/inbox/upload?item_id=${itemId}`;
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
