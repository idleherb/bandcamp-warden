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

# Stop the current bandcampsync mid-run (does NOT trip emergency)
curl -X POST http://homeserver:31080/stop

# Reset emergency flag after investigation
curl -X POST http://homeserver:31080/reset-emergency

# Force a cookie check now (bypasses once-per-day debounce)
curl -X POST http://homeserver:31080/check-cookie
```

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
