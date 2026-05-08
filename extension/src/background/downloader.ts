import { log } from '../shared/log.js';
import type { DownloadFormat, QueueItem } from '../shared/types.js';
import { resolveSignedDownloadUrl } from './api.js';

const DOWNLOAD_TIMEOUT_MS = 60 * 60 * 1000;

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

function sanitizeSubfolder(raw: string): string {
    // Strip leading/trailing slashes and any "../" segments — Firefox would
    // reject the download anyway, but we want a clean log message.
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

export interface DownloadOutcome {
    filename: string;
    signedUrl: string;
    downloadId: number;
}

export async function downloadItem(
    item: QueueItem,
    format: DownloadFormat,
    subfolder: string,
): Promise<DownloadOutcome> {
    const signedUrl = await resolveSignedDownloadUrl(item.downloadPageUrl, format);
    const filename = payloadFilenameFor(item.id, signedUrl, format, subfolder);
    void log.info(
        `download starting: ${filename} (item ${item.id}: ${item.bandName} — ${item.itemTitle})`,
    );
    const downloadId = await browser.downloads.download({
        url: signedUrl,
        filename,
        conflictAction: 'uniquify',
        saveAs: false,
    });
    await waitForDownload(downloadId, DOWNLOAD_TIMEOUT_MS);
    void log.info(`download done: ${filename} (downloadId=${downloadId})`);
    return { filename, signedUrl, downloadId };
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
