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

export interface Config {
    dailyQuota: number;
    format: DownloadFormat;
    minDelaySec: number;
    maxDelaySec: number;
    circuitBreakerThreshold: number;
    circuitBreakerPauseSec: number;
    inboxSubfolder: string;
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
