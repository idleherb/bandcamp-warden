import { log } from '../shared/log.js';
import type { DownloadFormat, QueueItem } from '../shared/types.js';
import { resolveSignedDownloadUrl } from './api.js';

const DOWNLOAD_TIMEOUT_MS = 60 * 60 * 1000;

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

function filenameFor(itemId: number, signedUrl: string, format: DownloadFormat): string {
    let kind: 'album' | 'track' | 'unknown' = 'unknown';
    try {
        const path = new URL(signedUrl).pathname;
        if (path.includes('/download/album')) kind = 'album';
        else if (path.includes('/download/track')) kind = 'track';
    } catch {
        // Malformed URL — fall through to unknown; .zip is a safe-ish default.
    }
    if (kind === 'track') {
        return `bandcamp_${itemId}.${FORMAT_EXT[format]}`;
    }
    return `bandcamp_${itemId}.zip`;
}

export interface DownloadOutcome {
    filename: string;
    signedUrl: string;
    downloadId: number;
}

export async function downloadItem(
    item: QueueItem,
    format: DownloadFormat,
): Promise<DownloadOutcome> {
    const signedUrl = await resolveSignedDownloadUrl(item.downloadPageUrl, format);
    const filename = filenameFor(item.id, signedUrl, format);
    void log.info(
        `download starting: ${filename} (item ${item.id}: ${item.bandName} — ${item.itemTitle})`,
    );
    const downloadId = await browser.downloads.download({
        url: signedUrl,
        filename,
        conflictAction: 'uniquify',
        saveAs: false,
    });
    await waitForDownload(downloadId);
    void log.info(`download done: ${filename} (downloadId=${downloadId})`);
    return { filename, signedUrl, downloadId };
}

function waitForDownload(downloadId: number): Promise<void> {
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
            settle(() => reject(new Error(`download timed out after ${DOWNLOAD_TIMEOUT_MS / 1000}s`)));
        }, DOWNLOAD_TIMEOUT_MS);
        const onChanged = (delta: browser.downloads._OnChangedDownloadDelta) => {
            if (delta.id !== downloadId) return;
            const state = delta.state?.current;
            if (state === 'complete') {
                settle(() => resolve());
            } else if (state === 'interrupted') {
                const reason = delta.error?.current ?? 'unknown';
                settle(() => reject(new Error(`download interrupted: ${reason}`)));
            }
        };
        browser.downloads.onChanged.addListener(onChanged);
    });
}
