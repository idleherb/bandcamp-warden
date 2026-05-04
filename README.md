# bandcamp-warden

Rate-limited, ban-aware Bandcamp collection downloader for self-hosting on TrueNAS Scale.

Wraps [`meeb/bandcampsync`](https://github.com/meeb/bandcampsync) — the actual downloader — with a small Python sidecar that adds three things bandcampsync alone doesn't:

1. **Daily ramp-up quota** (default 30 → 100 → 200 albums/day). bandcampsync's defaults will happily try to drain a 3000-album collection in one go; that's how accounts get flagged.
2. **Anomaly detection with auto-stop.** Three 401/403/429 responses inside a short window → bandcampsync gets killed and a Telegram alert goes out. The previous tool that cost the user's account did not have this; this is the central reason this project exists.
3. **Observability.** Telegram for autonomous alerts, plus a LAN-only HTTP service on port 8080 (`/health`, `/status`, `/logs`) for live spot-checks.

Resume is inherited from bandcampsync: each successfully downloaded album gets a `bandcamp_item_id.txt` marker in its folder. The sidecar uses these markers as the canonical truth — it counts them at the start of each day to set a baseline, and again as bandcampsync runs to know when the daily quota is hit. If anything crashes mid-run, you lose at most one in-flight album; the rest of the queue picks up cleanly on the next run.

## Architecture

```
TrueNAS LAN-IP:8080  ←─── you / Claude (curl from a LAN machine)
        │
   ┌────┴───────────────────────────────────────┐
   │  warden sidecar (this repo)                │
   │   • FastAPI: /health /status /logs         │
   │   • APScheduler: daily kickoff             │
   │   • Spawns bandcampsync via Docker socket  │
   │   • Watches logs → anomaly auto-stop       │
   │   • Telegram bot pushes alerts             │
   │   • SQLite state in /state                 │
   └────┬───────────────────────────────────────┘
        │ docker.sock + bind mounts
        ▼
   ┌────────────────────────────────┐
   │  bandcampsync (one-shot)       │
   │   ghcr.io/meeb/bandcampsync    │
   │   spawned fresh each day,      │
   │   removed after run            │
   └────────┬───────────────────────┘
            │
            ▼
   /mnt/storage/media/bandcamp/      ← FLACs land here
       Artist Name/
           Album Name/
               01 Track.flac
               cover.jpg
               bandcamp_item_id.txt  ← resume marker (do NOT delete)
```

Telegram is the autonomous channel — it works whether or not anyone is watching. The HTTP endpoints are only reachable on the LAN; no tunnels, no DNS.

## Prerequisites

- TrueNAS Scale 24.10+ (tested target: 25.04 / Fangtooth) with the **Custom App** feature enabled.
- A Bandcamp account with a logged-in browser session (Firefox or Chromium-based) — needed once to extract a cookie file.
- A Telegram bot token + chat ID — see [setup below](#telegram-bot).
- ~50 GB free per ~500 albums (FLAC). Plan for the full collection size on the target dataset.

## Deployment

### 1. Create the two ZFS datasets

bandcamp-warden expects two locations on your TrueNAS:

| Purpose | Default path | Pool suggestion |
| --- | --- | --- |
| FLAC output (the music) | `/mnt/storage/media/bandcamp` | the big media pool |
| Sidecar + bandcampsync state (cookie, SQLite) | `/mnt/apps/bandcamp-warden` | the apps pool |

In TrueNAS UI: **Datasets** → select `storage/media` → **Add Dataset** → name `bandcamp` → leave defaults. Same for `apps` → **Add Dataset** → name `bandcamp-warden`. Then under `apps/bandcamp-warden` add two children: `config` and `state`.

If you prefer SSH:

```sh
zfs create storage/media/bandcamp
zfs create apps/bandcamp-warden
zfs create apps/bandcamp-warden/config
zfs create apps/bandcamp-warden/state
chown -R 568:568 /mnt/apps/bandcamp-warden /mnt/storage/media/bandcamp
```

The `568:568` is the standard `apps` user/group on TrueNAS Scale — bandcampsync writes as that user. If your media dataset has different ownership, adjust `BANDCAMPSYNC_PUID`/`PGID` in `.env` instead of changing the dataset.

### 2. Extract the Bandcamp cookie

`bandcampsync` authenticates via a `cookies.txt` file in Netscape format (the format `wget --load-cookies` expects).

The simplest reliable extractor is the **"cookies.txt" Firefox extension** ([source](https://github.com/lennonhill/cookies-txt)). Install it, log in to https://bandcamp.com in Firefox (use a fresh login — don't reuse anything from prior tooling attempts), open the extension, click **Export → Current Site**, save as `cookies.txt`.

Copy the file to the TrueNAS at `/mnt/apps/bandcamp-warden/config/cookies.txt`. Owner `568:568`, mode `600`:

```sh
scp cookies.txt root@truenas.local:/mnt/apps/bandcamp-warden/config/cookies.txt
ssh root@truenas.local 'chown 568:568 /mnt/apps/bandcamp-warden/config/cookies.txt && chmod 600 /mnt/apps/bandcamp-warden/config/cookies.txt'
```

The cookie file expires (Bandcamp rotates session cookies). When you see auth failures in Telegram, repeat this step.

### 3. Telegram bot

Already covered in conversation, repeated here for self-containment:

1. Telegram → chat with `@BotFather` → `/newbot` → name it (e.g. "Bandcamp Warden").
2. BotFather replies with a token like `123456789:ABC...` — save it.
3. Send your new bot any message (e.g. `/start` or `hi`).
4. Open `https://api.telegram.org/bot<TOKEN>/getUpdates` in a browser.
5. Find `"chat":{"id": <number>}` in the JSON — that's your chat ID.

### 4. Configure environment

Copy `.env.example` to the TrueNAS and edit:

```sh
ssh root@truenas.local
cd /mnt/apps/bandcamp-warden
curl -L https://raw.githubusercontent.com/idleherb/bandcamp-warden/main/.env.example -o .env
nano .env
```

Fill in `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. Adjust paths if you used different dataset names. The default ramp `[30, 100, 200]` is the agreed conservative profile — only change it if you've discussed the tradeoff.

### 5. Make the GHCR image pullable

GitHub publishes container images as **private** by default, even when the source repo is public. Before TrueNAS can pull the sidecar image you need to flip the package visibility once:

1. Open https://github.com/idleherb/bandcamp-warden/pkgs/container/bandcamp-warden-sidecar
2. **Package settings** (right sidebar) → scroll to **Danger Zone** → **Change visibility** → **Public**.

Skipping this step means TrueNAS will get `unauthorized` errors on `docker pull` and the sidecar will never start. Alternatively, if you'd rather keep the image private, generate a personal access token with `read:packages` scope and configure it as a registry credential in TrueNAS — but the public-flip is simpler.

### 6. Deploy as a TrueNAS Custom App

In the TrueNAS UI: **Apps** → **Discover Apps** → **Custom App** (top-right). Name it `bandcamp-warden`. Paste the contents of [`docker-compose.yaml`](./docker-compose.yaml) into the YAML editor. Important: **TrueNAS Custom Apps don't auto-load `.env` files** — you have two options:

- **Option A (recommended):** in the Custom App UI, expand "Environment Variables" and add each variable from `.env` directly. Tedious but simple.
- **Option B:** mount the `.env` file in and rely on Compose's interpolation. This works only if you launch the stack via `docker compose` directly on the host, not via the TrueNAS UI.

Click **Install**. The sidecar pulls its image from `ghcr.io/idleherb/bandcamp-warden-sidecar:latest`, starts up, and immediately sends `🟢 bandcamp-warden online` to your Telegram. If you don't see that message within ~30 seconds, something's wrong (see [troubleshooting](#troubleshooting)).

### 7. First-run smoke test

Don't wait until 03:00 the next morning to find out if it works. From your Mac (or any LAN machine):

```sh
# Replace with your TrueNAS LAN IP
TRUENAS=192.168.1.42
curl http://$TRUENAS:8080/health     # should return {"status":"ok"}
curl http://$TRUENAS:8080/status     # should return state JSON
curl -X POST http://$TRUENAS:8080/trigger  # forces an immediate run
```

`POST /trigger` fires the daily kickoff right now. You should see in Telegram, within ~10 seconds: `▶ Tag 1 startet, Quota heute: 30 Alben`. Watch `/status` and `/logs` to track progress. After 30 successful albums you should get the daily summary and bandcampsync stops.

If the first run gets through 30 albums without 401/403/429s, the cookie is good and the rate is safe. The scheduler will pick up Day 2 (100 albums) the next morning at 03:00 automatically.

## Operations

### Daily rhythm

- **03:00 server time:** sidecar fires daily kickoff. Telegram: `▶ Tag N startet, Quota heute: X Alben`.
- **Throughout the day:** bandcampsync downloads. Concurrency 1, 30 s wait between retries on errors.
- **When today's quota is reached:** sidecar stops bandcampsync. Telegram: `✅ Tag N fertig, X/Y Alben heute, Z gesamt`.
- **When the entire collection is done:** Telegram: `🎉 Collection vollständig`. Scheduler stops firing.

### What `/status` tells you

```jsonc
{
  "bandcampsync_running": false,        // is a download active right now
  "started_on": "2026-05-04",           // first kickoff date
  "day_number": 1,                       // ramp-up day (1-indexed)
  "quota_today": 30,
  "ramp_quotas": [30, 100, 200],
  "today_run": {                         // null before first run of the day
    "run_date": "2026-05-04",
    "quota": 30,
    "downloaded": 17,
    "started_at": "...",
    "status": "running"                  // running|quota_hit|completed|emergency
  },
  "last_download_at": "2026-05-04T...",
  "emergency_stopped": false,            // ⚠ true means scheduler is paused
  "total_complete": 17,                  // canonical count from bandcamp_item_id.txt files
  "collection_complete": false,
  "recent_runs": [ /* last 14 daily runs */ ]
}
```

### Emergency reset

If the sidecar trips its emergency brake, the daily scheduler will keep skipping kickoffs until you reset:

```sh
curl -X POST http://$TRUENAS:8080/reset-emergency
```

**But first investigate.** The Telegram alert includes the last 6 log lines. Likely causes:

- **Cookie expired.** Re-extract from a logged-in browser, replace `cookies.txt`, reset, retrigger.
- **Bandcamp rate-limited you.** Don't reset immediately; wait 24–48 h first. Then lower `RAMP_QUOTAS` (e.g. `[10, 30, 60]`) before resetting.
- **Bandcamp changed their HTML/API.** Check bandcampsync's GitHub issues. May need a newer image.

### Manual stop / start

```sh
curl -X POST http://$TRUENAS:8080/stop      # stop bandcampsync now (does NOT trip emergency)
curl -X POST http://$TRUENAS:8080/trigger   # start a run now (respects today's quota)
```

## Folder layout

bandcampsync writes:

```
/mnt/storage/media/bandcamp/
├── Artist Name/
│   └── Album Name/
│       ├── 01 Track.flac          (FLAC with embedded Vorbis tags)
│       ├── cover.jpg
│       └── bandcamp_item_id.txt   ← do NOT delete; resume depends on it
```

The user's preferred `Artist Name/YYYY - Album Name/` layout is **not implemented in v1**. bandcampsync writes `Artist/Album/` and we deliberately do not rename in this release — every rename is a chance to break the resume markers. A separate post-processor that reads FLAC tags (`date`, `albumartist`) and renames safely (carrying `bandcamp_item_id.txt` along) is the natural v2.

Discogs alias resolution (collapsing different stage names of the same artist into one folder) is explicitly out of scope for v1.

## Troubleshooting

**No Telegram message on startup.** Check the sidecar's own container logs in TrueNAS UI. Most likely: token or chat ID typo, or the bot was never sent an initial message (Telegram won't let bots message you until you message them first).

**`/health` returns nothing.** Container isn't up. Check it's running, check the port mapping in compose matches `8080:8080`, check no other app is on port 8080 on the TrueNAS.

**Telegram says "bandcampsync-Start fehlgeschlagen".** The sidecar couldn't spawn the bandcampsync container. Either the image isn't pullable (network issue), or the bind-mount paths don't exist on the host. Verify `/mnt/storage/media/bandcamp` and `/mnt/apps/bandcamp-warden/config` actually exist with `ls -la` over SSH.

**Telegram says "NOTBREMSE" within minutes of first trigger.** Cookie is bad or expired. Re-extract from a fresh browser login. Note: 401 right away is fine to recover from — your account is not banned, the cookie just doesn't work.

**Account banned again.** Stop everything (`POST /stop`, then disable the app in TrueNAS UI). Wait at least 2 weeks before any retry. When retrying, set `RAMP_QUOTAS=[5, 10, 20, 50]` and stay there for a week before increasing.

## Project layout

```
.
├── docker-compose.yaml       # the TrueNAS Custom App stack (only the sidecar)
├── .env.example              # template for production secrets/paths
├── sidecar/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                # ~450 lines, single file, all the logic
└── .github/workflows/build-sidecar.yaml   # auto-builds + pushes to GHCR
```

## License

Personal-use scaffolding for one user's Bandcamp collection. No license declared. If you fork it, expect to read all of `app.py` before trusting it with your account.
