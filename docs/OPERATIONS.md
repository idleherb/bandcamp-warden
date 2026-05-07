# Operations runbook

Day-to-day operations of bandcamp-warden. For architecture/design rationale see `ARCHITECTURE.md`. For first-time deployment see the README.

## Daily rhythm

The sidecar's APScheduler fires `daily_kickoff` at the configured hour (default 03:00 Europe/Berlin). Each fire:

1. Telegram: `▶ Tag N startet (YYYY-MM-DD), Quota heute: X Alben, Bisher gesamt: Y Alben`
2. Sidecar spawns a fresh bandcampsync container, streams its log
3. bandcampsync paginates the Fan API to discover all items (~30 seconds for a 3k-item collection)
4. bandcampsync downloads sequentially, one album at a time
5. After each successful album: bandcampsync appends the item ID to `/config/ignores.txt`
6. Sidecar re-counts ignores.txt every 30 seconds and after every log line; when `count − baseline ≥ quota`, stops bandcampsync
7. Telegram: `✅ Tag fertig, X/Y Alben heute, Z gesamt`

Typical timing per album: 3–5 minutes. Bandcamp throttles FLAC downloads server-side. So 30 albums ≈ 2h, 100 ≈ 5–6h, 200 ≈ 10–13h. The 03:00 kickoff was picked so the 200-quota days finish before midnight even on slow days.

## What `/status` tells you

```jsonc
{
  "bandcampsync_running": true|false,
  "started_on": "YYYY-MM-DD",     // first kickoff date; null before first run
  "day_number": 1|2|...|null,     // 1-indexed; null before first run
  "quota_today": 30|100|200|null,
  "ramp_quotas": [30, 100, 200],
  "today_run": {                   // null before first run of the day
    "quota": 30,
    "downloaded": 17,
    "started_at": "...",
    "finished_at": null|"...",
    "status": "running|quota_hit|completed|emergency|failed_to_start",
    "stop_reason": null|"daily quota reached"|...
  },
  "last_download_at": "...",
  "emergency_stopped": false,     // ⚠ true means scheduler is paused
  "last_emergency_at": null|"...",
  "last_emergency_reason": null|"...",
  "collection_complete": false,    // true means we're done; scheduler skips
  "total_complete": 1453,          // canonical count from /config/ignores.txt
  "cookie": {
    "present": true,
    "expires_at": "2026-06-29T...",
    "days_remaining": 56,
    "expired": false
  },
  "recent_runs": [/* last 14 daily runs */]
}
```

`total_complete` is the source of truth. If it agrees with the file count under `/mnt/storage/media/bandcamp/`, everything's healthy.

## Telegram message taxonomy

| Emoji | Meaning | What you do |
|---|---|---|
| 🟢 | Sidecar online (boot or restart) | Nothing; confirms it's alive |
| ▶ | Daily run started | Nothing; track quota for the day |
| ✅ | Daily run finished cleanly | Nothing; check `total_complete` if curious |
| 🎉 | Entire collection done | Celebrate; scheduler stops on its own |
| 🚨 | Emergency brake | **Investigate before resetting** — see below |
| ⏸ | Daily kickoff skipped (emergency still active) | Reset emergency or wait |
| 🍪 | Cookie expiry warning | Refresh cookies.txt (see below) |
| ⌛ | 24h deadline | Check `/logs`; bandcampsync probably hung |
| ⚠️ | Unexpected exit | Check `/logs` for the underlying error |
| ❌ | Failed to spawn bandcampsync | Container/network/permission issue on host |

## Investigating an emergency brake

The Telegram alert includes the last six log lines. Common causes:

**Cookie expired or revoked.**
Symptoms: 401 or 403 in the lines, often before any 100% download line. `cookie.expired` may be true on `/status`.
Fix: re-extract cookies from a fresh Firefox login (see "Cookie renewal" below), then `POST /reset-emergency`.

**Bandcamp rate-limited.**
Symptoms: 429 in the lines, possibly after a few albums had already started.
Fix: don't reset immediately. Wait at least 24h; ideally 48–72h. When you do reset, lower `RAMP_QUOTAS` (e.g. `[10, 30, 60]`) for at least a week.

**Bandcamp HTML/API change.**
Symptoms: stack traces, `JSONDecodeError`, missing-field errors. Not 401/403/429 directly, but our patterns may not catch this — emergency might NOT trip; check `/logs` periodically.
Fix: check bandcampsync GitHub issues; you may need to update the bandcampsync image (`docker pull ghcr.io/meeb/bandcampsync:latest` on the host, then re-trigger).

**False positive in our anomaly detector.**
Symptoms: 3+ matches but on `[INFO]` lines (artist names containing "forbidden" etc.).
Fix: shouldn't happen anymore — anomaly detection now skips INFO lines. If it does happen again, the regex needs tightening; file an issue and reset.

## Cookie renewal

Bandcamp's `identity` cookie has hard expiry at ~1 year out, but server-side invalidation can shorten that. The sidecar warns at ≤14 days remaining (configurable via `COOKIE_WARN_THRESHOLD_DAYS`).

Steps:
1. Open https://bandcamp.com in Firefox (normal profile, not incognito); log in fresh with "Remember me".
2. Use the **cookies.txt** Firefox extension (https://addons.mozilla.org/de/firefox/addon/cookies-txt/) → Current Site → Export. Saves a Netscape-format file.
3. The file at minimum needs a row with `identity` cookie for `.bandcamp.com` and a future expiry timestamp.
4. On TrueNAS web shell:
   ```
   sudo tee /mnt/apps/bandcamp-warden/config/cookies.txt > /dev/null <<'EOF'
   <paste contents>
   EOF
   sudo chown 568:568 /mnt/apps/bandcamp-warden/config/cookies.txt
   sudo chmod 600 /mnt/apps/bandcamp-warden/config/cookies.txt
   ```
5. Force a check: `curl -X POST http://homeserver:31080/check-cookie`. Telegram should confirm new days_remaining.
6. If the brake had tripped due to expiry: `curl -X POST http://homeserver:31080/reset-emergency`.

## Manual control

```sh
# Run now (respects today's quota; idempotent if already running)
curl -X POST http://homeserver:31080/trigger

# Stop the current bandcampsync mid-run (does NOT trip emergency, also
# cancels any pending auto-retry)
curl -X POST http://homeserver:31080/stop

# Reset emergency flag after investigation
curl -X POST http://homeserver:31080/reset-emergency

# Clear collection_complete (use if a transient failure was misclassified
# as 'caught up' and the scheduler is now skipping all kickoffs)
curl -X POST http://homeserver:31080/reset-completion

# Force a cookie check now (bypasses once-per-day debounce)
curl -X POST http://homeserver:31080/check-cookie

# Backfill metadata for every album folder that lacks bandcamp_<id>.json.
# Run this once after a sidecar upgrade or after a daily run failed to
# reach the Fan API. Add ?force=true to overwrite existing metadata files
# (use when new fields are added to the schema).
curl -X POST 'http://homeserver:31080/backfill-metadata'
curl -X POST 'http://homeserver:31080/backfill-metadata?force=true'
```

## Plan C: sidecar-side downloader (httpx + Range resume)

When the bandcampsync container's download mechanism keeps failing (curl-error-28 stalls, mystery SIGKILLs), the sidecar can take over the actual file download itself. It still uses `bandcampsync` as a library to authenticate, list purchases, and resolve signed download URLs — but the streaming, ZIP extraction, and ignores.txt append happen inside the sidecar, with httpx async + proper read-timeout + HTTP Range resume.

**Test before adopting**: `POST /test-sidecar-download` runs the new downloader for one (or N) album(s) without touching the daily-run mechanism. Returns success/failure + bytes + resume count + log events. If it works for a few albums, we flip the daily run over.

**YAML requirement**: `/config` mount must be RW for the sidecar (it appends item ids to `ignores.txt` directly). Change `:/config:ro` → `:/config` in the deployed compose.

### Endpoints

```sh
# One album, picks first not-yet-downloaded
curl -X POST 'http://homeserver:31080/test-sidecar-download'

# Specific item by id
curl -X POST 'http://homeserver:31080/test-sidecar-download?item_id=1234567'

# A few in a row to validate stability
curl -X POST 'http://homeserver:31080/test-sidecar-download?count=3'
```

### Tunables (env on the sidecar)

`WardenDownloader` has its own knobs but defaults are sensible: `connect_timeout=30s`, `read_timeout=60s`, `max_resumes_per_album=30`, `resume_delay=10s`, `between_albums=5s`. Override only if a daily run keeps stalling.

## Bandcampsync download patch

bandcampsync 0.7.0's downloader uses curl_cffi with `impersonate="chrome"`, which sets `LOW_SPEED_TIME=30` / `LOW_SPEED_LIMIT=1` — abort the stream after 30 seconds of less than 1 byte/sec. Bandcamp's edge sometimes pauses bursts mid-album, especially on big FLAC archives, and that triggers a curl-error-28 crash.

Browsers don't have this stall detection at all (the user could download the same album manually with no problem), so the fix is to be more patient on our side without making Bandcamp work harder.

The sidecar ships a patched `download.py` under `/app/patches/bandcampsync_download.py` and stages it under `<HOST_STATE_PATH>/patches/` on every boot. The bandcampsync container then bind-mounts it over the upstream module file at `/usr/local/lib/python3.13/dist-packages/bandcampsync/download.py:ro`. The patch only changes one thing: the curl session uses `LOW_SPEED_TIME=300` / `LOW_SPEED_LIMIT=1024`, so a stall has to last 5 minutes of <1KB/s before we give up.

Disable it (e.g. for an A/B test) by setting `WARDEN_BANDCAMPSYNC_PATCH_ENABLED=false` in the compose env. Verify it's active by looking for `[warden patch] download_file using patient curl options` in `/logs` after a daily run.

If you upgrade `WARDEN_BANDCAMPSYNC_IMAGE` to a future bandcampsync version where the module path differs (e.g. Python 3.14), update `WARDEN_BANDCAMPSYNC_PATCH_TARGET` too.

## Auto-retry on bandcampsync crashes

If bandcampsync exits with a non-zero code (network blip, ISP outage, server stall, etc.) the sidecar treats it as a **transient failure**, not a completion. It does not set `collection_complete`. It schedules an automatic retry per `WARDEN_RETRY_BACKOFFS_MINUTES` (default `[5, 15, 60]`), capped at `WARDEN_RETRY_MAX_PER_DAY` retries (default 3 → 1 initial + 3 retries = 4 attempts max). The baseline downloaded-count for the day is preserved across retries, so the daily quota is enforced over the whole day rather than reset on each attempt. Telegram says `🔁 Tag N Retry M/X` for each retry attempt.

Manual `POST /stop` cancels any pending retry; the scheduler resumes normally at 03:00 the next day.

## Updating

Two paths:

**Code change in this repo.** Push to `main`. GHA builds and pushes to GHCR. Watchtower polls and pulls within a few minutes; no action needed on TrueNAS. Telegram will fire the `🟢 online` message after restart.

**bandcampsync image refresh.** Watchtower can't see bandcampsync (it's spawned ad-hoc, not running between sessions). To upgrade:
```sh
sudo docker pull ghcr.io/meeb/bandcampsync:latest
```
Next daily run uses the new image.

**Force-update sidecar immediately.** In TrueNAS web shell:
```sh
sudo docker pull ghcr.io/idleherb/bandcamp-warden-sidecar:latest
```
Then in TrueNAS UI: Apps → bandcamp-warden → Edit → Save (no changes needed, just trigger recreation).

## When the cookie file is missing or wrong

The sidecar starts cleanly; the cookie check pushes a `🍪 Cookie file missing` style warning to Telegram (won't crash). bandcampsync, when triggered, will fail-fast on auth — anomaly detector trips quickly on the 401s, brake engages. So you'll know within seconds of trying.

## Performance considerations

- 3–5 min per album is normal; FLAC encoding+download is server-bound on Bandcamp's side. Don't troubleshoot this as if it were slow on our end.
- The folder rglob in `count_completed_albums` was replaced with reading `/config/ignores.txt`; counter cost is now O(albums-completed) line reads on a small text file, not a directory walk. No performance concerns even at 3000 albums.
- SQLite writes are short-lived and serialized in the orchestrator's single-thread async loop; no contention.
- Log buffer in memory is bounded at `WARDEN_LOG_BUFFER_SIZE` (default 2000 lines); old lines drop on overflow.

## When the user's collection is fully done

`collection_complete` flips to `true` in the warden state when bandcampsync exits naturally (no items remaining) and the daily downloaded count is below quota. Subsequent kickoffs see this flag and skip silently — the scheduler stays registered but does nothing. To re-enable downloads (e.g. after buying new albums), `UPDATE warden SET collection_complete = 0;` directly in `/state/state.db` via web shell, or POST to a future `/resume` endpoint if added.
