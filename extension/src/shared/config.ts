import { makeStore } from './storage.js';
import type { Config } from './types.js';

export const DEFAULT_CONFIG: Config = {
    dailyQuota: 250,
    format: 'flac',
    minDelaySec: 60,
    maxDelaySec: 300,
    circuitBreakerThreshold: 5,
    circuitBreakerPauseSec: 3600,
};

export const configStore = makeStore<Config>('config', DEFAULT_CONFIG);
