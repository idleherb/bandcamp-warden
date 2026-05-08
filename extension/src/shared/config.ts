import { makeStore } from './storage.js';
import type { Config } from './types.js';

export const DEFAULT_CONFIG: Config = {
    dailyQuota: 250,
    format: 'flac',
    minDelaySec: 60,
    maxDelaySec: 300,
    circuitBreakerThreshold: 5,
    circuitBreakerPauseSec: 3600,
    // Subfolder under Firefox's default download dir. For production use
    // empty string + point Firefox at /Volumes/.../bandcamp/_inbox; for
    // local dev use "warden-inbox" so test ZIPs don't litter ~/Downloads.
    inboxSubfolder: 'warden-inbox',
};

export const configStore = makeStore<Config>('config', DEFAULT_CONFIG);
