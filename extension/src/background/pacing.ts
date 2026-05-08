import type { Config, State } from '../shared/types.js';

export type RunDecision = { run: true } | { run: false; reason: string };

export function todayKey(now: Date = new Date()): string {
    return now.toISOString().slice(0, 10);
}

export function dailyResetIfNeeded(state: State, now: Date = new Date()): State {
    const today = todayKey(now);
    if (state.todayResetAt === today) return state;
    return { ...state, todayResetAt: today, todayDownloaded: 0 };
}

export interface ShouldRunOptions {
    /** When true, skip the inter-download cooldown (manual force-tick). */
    ignoreCooldown?: boolean;
    /** When true, ignore the enabled flag (manual force-tick). */
    ignoreDisabled?: boolean;
}

export function shouldRunNow(
    state: State,
    config: Config,
    now: Date = new Date(),
    options: ShouldRunOptions = {},
): RunDecision {
    if (!options.ignoreDisabled && !state.enabled) {
        return { run: false, reason: 'disabled' };
    }
    if (state.inFlight !== null) {
        return { run: false, reason: `inFlight=${state.inFlight}` };
    }
    if (state.pausedUntil) {
        const until = new Date(state.pausedUntil);
        if (until > now) {
            return { run: false, reason: `paused-until=${state.pausedUntil}` };
        }
    }
    if (state.todayDownloaded >= config.dailyQuota) {
        return { run: false, reason: `quota-met (${state.todayDownloaded}/${config.dailyQuota})` };
    }
    if (!options.ignoreCooldown && state.nextRunAt) {
        const next = new Date(state.nextRunAt);
        if (next > now) {
            return { run: false, reason: `cooldown-until=${state.nextRunAt}` };
        }
    }
    return { run: true };
}

export function nextDelaySec(config: Config): number {
    const min = Math.max(0, config.minDelaySec);
    const max = Math.max(min, config.maxDelaySec);
    return min + Math.random() * (max - min);
}

export function nextRunAtIso(config: Config, now: Date = new Date()): string {
    return new Date(now.getTime() + nextDelaySec(config) * 1000).toISOString();
}

export interface CircuitBreakerOutcome {
    pausedUntil: string | null;
    tripped: boolean;
}

export function applyCircuitBreaker(
    consecutiveFailures: number,
    config: Config,
    currentPausedUntil: string | null,
    now: Date = new Date(),
): CircuitBreakerOutcome {
    if (consecutiveFailures < config.circuitBreakerThreshold) {
        return { pausedUntil: currentPausedUntil, tripped: false };
    }
    const until = new Date(now.getTime() + config.circuitBreakerPauseSec * 1000).toISOString();
    return { pausedUntil: until, tripped: true };
}
