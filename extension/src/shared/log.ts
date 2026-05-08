import { makeStore } from './storage.js';
import type { LogEntry, LogLevel } from './types.js';

const MAX_LINES = 500;

export const logStore = makeStore<LogEntry[]>('log', []);

function consoleMirror(level: LogLevel, msg: string): void {
    const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
    fn(`[warden] ${msg}`);
}

async function append(level: LogLevel, msg: string): Promise<void> {
    consoleMirror(level, msg);
    const entry: LogEntry = { ts: new Date().toISOString(), level, msg };
    await logStore.update((cur) => {
        const next = cur.length >= MAX_LINES ? [...cur.slice(-(MAX_LINES - 1)), entry] : [...cur, entry];
        return next;
    });
}

function errorMessage(err: unknown): string {
    if (err instanceof Error) return `${err.name}: ${err.message}`;
    if (typeof err === 'string') return err;
    try {
        return JSON.stringify(err);
    } catch {
        return String(err);
    }
}

export const log = {
    info: (msg: string): Promise<void> => append('info', msg),
    warn: (msg: string): Promise<void> => append('warn', msg),
    error: (msg: string | unknown): Promise<void> => append('error', typeof msg === 'string' ? msg : errorMessage(msg)),
};
