# Architecture

This document explains the design decisions behind bandcamp-warden — what each piece does and why it's shaped the way it is. Read `CLAUDE.md` first for the index; come here when you need to know *why*.

## Problem

The user has ~3110 purchased Bandcamp albums and wants them on their TrueNAS as FLAC. Their previous attempt — a self-written Python downloader — got the **user account** banned (not IP) at around 100 albums. Browsing/streaming kept working in a browser, but the script-issued auth could no longer pull downloads. Recovery took weeks.

The problem isn't downloading per se; multiple decent open-source tools exist. The problem is doing it slowly, predictably, and with safety rails that make a re-ban impossible. That's what this project is.

## Why not just run bandcampsync directly

[`meeb/bandcampsync`](https://github.com/meeb/bandcampsync) is well-maintained and does the actual work. But it's missing four things that the threat model requires:

1. **Per-day quota.** bandcampsync's Docker daemon mode (`RUN_DAILY_AT`) runs once per day and tries to download *everything* outstanding. For a fresh 3000-album collection, that means hitting Bandcamp's signed-download endpoint 3000 times in a row, which is exactly what got the user banned before.
2. **Stop-on-error.** bandcampsync retries with backoff, but it doesn't stop after N consecutive auth/rate failures. So if the cookie has been server-invalidated mid-run, it just keeps hammering — escalating an existing flag.
3. **Visibility.** stdout-only logging. No way to know "is it still alive at 14:00 today" without SSHing in.
4. **Cookie expiry warning.** Bandcamp's `identity` cookie expires server-set ~1 year out. Renewal is manual. Without warning, you find out by seeing 401s in the brake.

We could fork bandcampsync to add these, but they're cross-cutting concerns that don't belong inside the downloader. The sidecar pattern keeps bandcampsync vendor-pristine and lets us upgrade it independently.

## The sidecar pattern

```
TrueNAS host (Docker engine)
│
├── /var/run/docker.sock                   ← engine control
│
├── bandcamp-warden-sidecar (this repo)
│   FastAPI + APScheduler + SQLite
│   Mounts: docker.sock, /state, /config:ro, /downloads:rw
│   Runs continuously, restart=unless-stopped
│       │
│       │ docker.containers.run(image="meeb/bandcampsync", ...)
│       │  (only at scheduled time, or on POST /trigger)
│       ▼
└── bandcamp-warden-bandcampsync (ephemeral)
    Mounts: /config:rw, /downloads:rw
    Runs to completion or until sidecar stops it
    Container removed after each run
```

The sidecar holds the keys: scheduling, monitoring, state. The bandcampsync container is a one-shot worker that the sidecar spawns, watches, and reaps. Both bind-mount the same host directories, so bandcampsync's writes are immediately visible to the sidecar.

This pattern has trade-offs:
- **Pro:** bandcampsync image stays unmodified; easy to swap or upgrade.
- **Pro:** Watchtower auto-updating the sidecar gives us continuous deployment without ever touching bandcampsync.
- **Con:** Watchtower can't see bandcampsync (it's not running between sessions). bandcampsync version pinning is implicit via the env var `WARDEN_BANDCAMPSYNC_IMAGE`. To force a refresh of bandcampsync, run `docker pull` manually on the host, or add a pull call in `BandcampsyncController.start()` (5-line change).
- **Con:** Docker-socket access in the sidecar = effectively root on the host. Acceptable for a single-tenant home server; would need rethinking on a multi-user system.

## Components inside the sidecar

All in `sidecar/app.py`. ~700 lines, deliberately one file. Sections separated by `# ---------- Name ----------` banners.

### `Settings` (pydantic-settings)

Reads `WARDEN_*` env vars. Key surface:
- `host_downloads_path`, `host_config_path` — the host paths bandcampsync gets bind-mounted to. The sidecar passes these to `docker.containers.run()` as bind mounts.
- `bandcampsync_image`, `_concurrency`, `_max_retries`, `_retry_wait`, `_puid`, `_pgid` — passed into the spawned bandcampsync container.
- `daily_run_hour`, `timezone`, `ramp_quotas` — schedule + ramp.
- `anomaly_window`, `anomaly_threshold` — brake sensitivity.
- `cookies_path`, `cookie_warn_threshold_days`, `cookie_check_hour` — expiry monitor.
- `telegram_bot_token`, `telegram_chat_id` — autonomous channel; gracefully no-ops if empty.

`ramp_quotas` is a list parsed via pydantic's automatic JSON parsing of complex env vars (`[30,100,200]`).

### `State` (SQLite)

Two tables in `/state/state.db`:
- `warden` (single-row) — `started_on`, `emergency_stopped`, `last_emergency_at`, `last_emergency_reason`, `collection_complete`, `last_cookie_warning_on`. Schema migrations are additive `ALTER TABLE` wrapped in try/except OperationalError — works for both fresh and updated DBs.
- `daily_runs` (one row per day) — `quota`, `downloaded`, `started_at`, `finished_at`, `status`, `stop_reason`. `status` is in `{running, quota_hit, completed, emergency, failed_to_start}`.

Single-process, single-thread access. Short-lived connections per call, no connection pool. SQLite handles the rest.

### `Telegram`

Tiny httpx wrapper. `send(text)` does parse-mode Markdown, no preview. If token or chat-id missing, logs at WARNING and returns — doesn't raise. That keeps the rest of the system functioning even if Telegram is misconfigured.

### `BandcampsyncController`

Owns the lifecycle of the bandcampsync container.
- `start()` removes any stale container of the same name, then `client.containers.run(image=..., volumes={...}, environment={...}, detach=True, restart_policy={"Name": "no"})`.
- `stop(timeout)` issues a graceful stop.
- `is_running()` reloads container status from the engine.
- `stream_logs(container)` is a synchronous generator over `container.logs(stream=True, follow=True)`. The Orchestrator runs this in a thread and bridges to asyncio via a queue.

The controller does NOT pull the image proactively. Docker's `containers.run()` pulls if not cached; otherwise uses local. Implication: bandcampsync version is sticky once pulled. To upgrade bandcampsync, `docker pull` on the host or add an explicit `client.images.pull()` call.

### `Orchestrator`

The conductor. Three responsibilities:

1. **Scheduling decisions.** `compute_quota_for_day(day_index)` returns `ramp_quotas[min(day_index, len-1)]`. So with default `[30,100,200]`: day 0 → 30, day 1 → 100, day 2+ → 200. `started_on` is set on first kickoff, never reset.
2. **Daily orchestration.** `daily_kickoff()` is what APScheduler fires every morning. It checks emergency_stopped (skip + Telegram if true), checks collection_complete (skip silently if true), computes today's quota, baselines the album count, starts bandcampsync, calls `_monitor()`.
3. **Run monitoring.** `_monitor()` is the heart. See below.

### `_monitor` loop

Pseudocode:

```
recent_lines = deque(maxlen=anomaly_window)
deadline = now + 24h
last_count = baseline

start log-reader thread → puts each bandcampsync log line into a queue, sentinel None on EOF

while True:
    if past deadline: break with reason="deadline"
    line = queue.get(timeout=30s)
    if timeout:
        if not bandcampsync running: break "exited"
        recount albums; update today_run; if quota hit: break "quota_hit"
        continue
    if line is sentinel: break "exited"
    log_buffer.append(line); recent_lines.append(line)
    if is_anomaly_line(line):
        if count(is_anomaly_line(l) for l in recent_lines) >= threshold:
            break "emergency"

# post-loop:
stop bandcampsync if still running
final count
write run row + Telegram message based on stop_reason
```

`is_anomaly_line` skips `[INFO]` lines because album/artist names live there. Real auth/rate errors land at `[WARNING]`/`[ERROR]`.

The 30s timeout serves two purposes: drives periodic quota checks, and ensures we notice if bandcampsync exits silently without writing a final log line. The 24h deadline is a safety belt against a hung log stream.

### Cookie expiry monitor

Parses `cookies.txt` (Netscape format), finds the largest `expires` value among rows named `identity` on `bandcamp.com`. Returns `None` for session cookies (`expires=0`).

Daily check at noon (configurable). Warns once per day if days_remaining ≤ threshold (default 14). The "once per day" gate is `last_cookie_warning_on` in `warden`. Manual force via `POST /check-cookie` resets the gate to allow an immediate re-check.

This catches the **hard expiry** Bandcamp set when the cookie was issued. It does NOT catch spontaneous server-side invalidation (password change, "sign out everywhere", anti-abuse flag) — that path is the anomaly detector's job, fired on 401s in the next download attempt.

### Metadata enricher (added 2026-05-04)

Per-album `bandcamp_<item_id>.json` plus a central `/state/album_index.jsonl`. See `docs/ARCHITECTURE.md#metadata-enrichment` (this file's section below) for the design.

#### Why ID-prefixed filenames

The user's collection is heavily vaporwave with Japanese characters and similar styled artist names. bandcampsync's slugification can in theory collide; ID-keyed filenames eliminate that class of risk entirely. Item IDs are globally unique on Bandcamp.

#### How we map item_id → folder

We do NOT do post-hoc slug matching, which would be fragile. Instead, we observe bandcampsync's own log lines in real-time during `_monitor`:

- `New media item, will download: "X / Y" (id:NNN)` → record `current_item_id = NNN`
- `Moving extracted file: ".." to "/downloads/A/B/track.flac"` → record folder for `current_item_id` as `/downloads/A/B`

When the run ends, we have a complete `{item_id: folder}` mapping for everything bandcampsync touched this run.

#### Where the metadata comes from

Bandcamp's Fan API: `POST https://bandcamp.com/api/fancollection/1/collection_items` with the `identity` cookie. Returns paginated items with `tralbum_id`, `band_name`, `item_title`, `item_url`, `tralbum_type`, `release_date`, `featured_track_title`, etc. We paginate using the returned `last_token` until `more_available` is false.

For the fan_id we parse the `identity` cookie's embedded JSON (`{"id":NNN,"ex":0}`).

The enricher runs once at end of each daily run, AFTER bandcampsync has stopped. That keeps it cheap (one API sweep instead of per-album) and decoupled from the download flow (a Fan API hiccup doesn't affect downloads).

#### Atomic writes

`tmp + os.replace` so partial writes can't corrupt files even if the container crashes mid-write. Standard pattern.

#### Index file

`/state/album_index.jsonl` is append-only, one JSON-line per album. Easy `jq` queries, easy backups, recoverable if individual album JSON files are deleted. Also where the v2 Discogs lookup will write its `discogs_id` field.

## Notification taxonomy

Every Telegram message has a leading emoji to make scanning fast in a busy chat:
- 🟢 sidecar online (after every restart, including Watchtower updates)
- ▶ daily run starts
- ✅ daily run ends successfully (quota hit or natural completion partial day)
- 🎉 entire collection done — scheduler stops firing
- 🚨 emergency brake — anomaly threshold crossed
- ⏸ daily run skipped (emergency active)
- 🍪 cookie expiry warning
- ⌛ deadline hit (run ran 24h, unusual)
- ⚠️ unexpected exit (bandcampsync stopped without quota or completion signal)
- ❌ failure to start bandcampsync container

## Anti-features

These were considered and explicitly NOT built:
- **Higher concurrency.** `CONCURRENCY=2` would halve runtime, but Bandcamp's anti-abuse explicitly flags parallel downloads. Stays at 1.
- **Discogs lookup.** Out of scope for v1; planned as a separate enricher pass that reads from `album_index.jsonl` and adds `discogs_id` fields. Different API, different rate limits, different mapping problem.
- **Folder renaming to `Artist/YYYY - Album/`.** Would require carrying state about which renames happened. The `bandcamp_<id>.json` files give the canonical title without renaming directories — downstream tools can use them.
- **Webhook callbacks instead of Telegram.** `bandcampsync` exposes `NOTIFY_URL`; we don't pipe to it because Telegram is the user's preferred channel and adding two notification paths multiplies failure modes.
- **Authentication on the LAN endpoints.** They live on a private VLAN behind a NAT boundary. If the threat model changes (port forwarded, exposed via Tailscale Funnel), add a token check.
