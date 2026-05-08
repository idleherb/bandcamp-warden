import { log } from '../shared/log.js';
import type { DownloadFormat, QueueItem } from '../shared/types.js';
import { resolveSignedDownloadUrl } from './api.js';

const DOWNLOAD_TIMEOUT_MS = 60 * 60 * 1000;
const META_DOWNLOAD_TIMEOUT_MS = 60 * 1000;

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
    return joinSub(subfolder, base);
}

function metaFilenameFor(itemId: number, subfolder: string): string {
    return joinSub(subfolder, `bandcamp_${itemId}.meta.json`);
}

function joinSub(subfolder: string, base: string): string {
    const cleanSub = sanitizeSubfolder(subfolder);
    return cleanSub ? `${cleanSub}/${base}` : base;
}

interface MetaJson {
    item_id: number;
    band_name: string;
    item_title: string;
    item_url: string | null;
    tralbum_type: string | null;
    art_id: number | null;
    featured_track: string | null;
    purchased_at: string | null;
    added_at: string | null;
    downloaded_format: DownloadFormat;
    downloaded_at: string;
    extension_version: string;
}

function buildMetaJson(item: QueueItem, format: DownloadFormat): MetaJson {
    return {
        item_id: item.id,
        band_name: item.bandName,
        item_title: item.itemTitle,
        item_url: item.itemUrl ?? null,
        tralbum_type: item.tralbumType ?? null,
        art_id: typeof item.artId === 'number' ? item.artId : null,
        featured_track: item.featuredTrack ?? null,
        purchased_at: item.purchasedAt ?? null,
        added_at: item.addedAt ?? null,
        downloaded_format: format,
        downloaded_at: new Date().toISOString(),
        extension_version: browser.runtime.getManifest().version,
    };
}

async function writeMetaJson(
    item: QueueItem,
    format: DownloadFormat,
    subfolder: string,
): Promise<{ filename: string; downloadId: number }> {
    const meta = buildMetaJson(item, format);
    const blob = new Blob([JSON.stringify(meta, null, 2)], {
        type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const filename = metaFilenameFor(item.id, subfolder);
    try {
        const downloadId = await browser.downloads.download({
            url,
            filename,
            conflictAction: 'overwrite',
            saveAs: false,
        });
        await waitForDownload(downloadId, META_DOWNLOAD_TIMEOUT_MS);
        return { filename, downloadId };
    } finally {
        URL.revokeObjectURL(url);
    }
}

export interface DownloadOutcome {
    filename: string;
    metaFilename: string;
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

    // Meta JSON first (small, fast) so the sidecar inbox watcher always sees
    // it next to the ZIP once the much-longer ZIP download lands.
    const meta = await writeMetaJson(item, format, subfolder);
    void log.info(`meta written: ${meta.filename}`);

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
    return { filename, metaFilename: meta.filename, signedUrl, downloadId };
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
                const reason = delta.error?.current ?? 'unknown';
                settle(() => reject(new Error(`download interrupted: ${reason}`)));
            }
        };
        browser.downloads.onChanged.addListener(onChanged);
    });
}
