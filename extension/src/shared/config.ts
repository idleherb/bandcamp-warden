import { makeStore } from './storage.js';
import type { Config } from './types.js';

export const DEFAULT_CONFIG: Config = {
    dailyQuota: 250,
    format: 'flac',
    minDelaySec: 60,
    maxDelaySec: 300,
    circuitBreakerThreshold: 5,
    circuitBreakerPauseSec: 3600,
    // Subfolder under Firefox's default download dir. Plan-canonical name
    // matching the sidecar inbox watcher's expected path. Point Firefox
    // at the parent (e.g. /Volumes/.../bandcamp/) and ZIPs land in
    // /Volumes/.../bandcamp/_inbox/bandcamp_<id>.zip.
    inboxSubfolder: '_inbox',
};

export const configStore = makeStore<Config>('config', DEFAULT_CONFIG);
