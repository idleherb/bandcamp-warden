export interface QueueItem {
    id: number;
    bandName: string;
    itemTitle: string;
    downloadPageUrl: string;
    // Rich fields captured at refresh time. Optional so older stored queues
    // don't fail validation; sidecar's inbox watcher falls back to Fan API
    // when these are missing.
    itemUrl?: string;
    tralbumType?: string;
    featuredTrack?: string;
    purchasedAt?: string;
    addedAt?: string;
    artId?: number;
}

export interface FailedItem {
    id: number;
    error: string;
    lastTryAt: string;
    attempts: number;
    bandName?: string;
    itemTitle?: string;
    itemUrl?: string;
}

export type DownloadFormat =
    | 'flac'
    | 'mp3-v0'
    | 'mp3-320'
    | 'aac-hi'
    | 'vorbis'
    | 'alac'
    | 'wav'
    | 'aiff-lossless';

export type Transport = 'sidecar-upload' | 'browser-download';

export interface Config {
    dailyQuota: number;
    format: DownloadFormat;
    minDelaySec: number;
    maxDelaySec: number;
    circuitBreakerThreshold: number;
    circuitBreakerPauseSec: number;
    // Where the ZIP ends up.
    //   sidecar-upload: extension fetches from Bandcamp (browser identity),
    //     POSTs the bytes to the sidecar's /inbox/upload — no SMB involved.
    //   browser-download: legacy. Firefox writes via its default download
    //     dir (must be the SMB-mounted inbox to be useful).
    transport: Transport;
    sidecarBaseUrl: string;
    sidecarAuthToken: string;
    // Only used when transport = browser-download. Subfolder under
    // Firefox's default download directory.
    inboxSubfolder: string;
    // Hard cap for sidecar-upload; rejects tracks/albums that would
    // never fit anyway (defaults match the sidecar's max).
    maxUploadBytes: number;
}

export interface State {
    enabled: boolean;
    inFlight: number | null;
    lastRunAt: string | null;
    nextRunAt: string | null;
    todayDownloaded: number;
    todayResetAt: string;
    consecutiveFailures: number;
    pausedUntil: string | null;
    transientPausedUntil: string | null;
}

export type LogLevel = 'info' | 'warn' | 'error';

export interface LogEntry {
    ts: string;
    level: LogLevel;
    msg: string;
}
