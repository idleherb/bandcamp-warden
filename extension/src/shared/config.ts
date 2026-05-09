import { makeStore } from './storage.js';
import type { Config } from './types.js';

export const DEFAULT_CONFIG: Config = {
    dailyQuota: 250,
    format: 'flac',
    minDelaySec: 60,
    maxDelaySec: 300,
    circuitBreakerThreshold: 5,
    circuitBreakerPauseSec: 3600,
    // Plan-E default: HTTP upload to the sidecar. SMB stays as a fallback
    // toggle but isn't the recommended path — Mac+SMB stability has been
    // the reason we built the upload endpoint in the first place.
    transport: 'sidecar-upload',
    sidecarBaseUrl: 'http://homeserver:31080',
    sidecarAuthToken: '',
    inboxSubfolder: '_inbox',
    maxUploadBytes: 2 * 1024 * 1024 * 1024,
};

export const configStore = makeStore<Config>('config', DEFAULT_CONFIG);
