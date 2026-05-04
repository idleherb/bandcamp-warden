# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

bandcamp-warden is a tiny safety harness around [`meeb/bandcampsync`](https://github.com/meeb/bandcampsync), running on the user's TrueNAS Scale home server. It exists because the user's Bandcamp **account** (not IP) was previously banned by an aggressive Python downloader at ~100 albums; this project's central feature is the things that prevent a repeat. Treat the ban-prevention logic as load-bearing — never weaken it without explicit user approval.

For the deeper why-and-how, see `docs/ARCHITECTURE.md`. For day-to-day operations, see `docs/OPERATIONS.md`. This file is the index.

## Repo at a glance

```
.
├── docker-compose.yaml          # the TrueNAS Custom App stack (sidecar only)
├── .env.example                 # env-var template
├── sidecar/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app.py                   # ~700 lines, single file, all the logic
├── docs/
│   ├── ARCHITECTURE.md          # design + decisions
│   └── OPERATIONS.md            # runbook
└── .github/workflows/build-sidecar.yaml   # auto-builds + pushes to GHCR on every push to main
```

`bandcampsync` itself is **not** declared in compose. The sidecar spawns it on demand via the Docker socket each daily run — fresh container, removed afterwards. If a future change requires bandcampsync to be running between runs, that's a substantial architecture change; flag it.

## Common commands

```bash
# Verify Python syntax of the sidecar
python3 -c "import ast; ast.parse(open('sidecar/app.py').read())"

# After pushing changes to main, watch the GHA build
gh run watch --repo idleherb/bandcamp-warden $(gh run list --repo idleherb/bandcamp-warden --limit 1 --json databaseId --jq '.[0].databaseId') --exit-status

# Hit the live sidecar from the user's Mac (LAN only — homeserver:31080)
curl http://homeserver:31080/healthz   # liveness + version/commit/channel
curl http://homeserver:31080/status | python3 -m json.tool
curl 'http://homeserver:31080/logs?lines=50' | python3 -m json.tool
curl -X POST http://homeserver:31080/trigger          # immediate run, respects today's quota
curl -X POST http://homeserver:31080/stop             # stop bandcampsync (no emergency flag)
curl -X POST http://homeserver:31080/reset-emergency  # clear the brake after investigation
curl -X POST http://homeserver:31080/check-cookie     # force cookie expiry check
```

There are no tests, no linter, no build step beyond Docker. The deployable artifact is the GHCR-published sidecar image; everything else lives in the user's TrueNAS state directory.

## Architecture in one paragraph

A FastAPI app called the "warden sidecar" runs permanently on TrueNAS, scheduled by APScheduler. Once a day at 03:00 it spawns a fresh `bandcampsync` container via the Docker socket, streams its log output, watches a sliding-window anomaly detector for clustered HTTP-401/403/429 (the documented ban precursor), counts completed albums by reading lines in `/config/ignores.txt` (bandcampsync's own canonical record in Docker mode — see sync.py:191-204), enforces a daily ramp-up quota (default 30/100/200), and stops the bandcampsync container when the quota is met or the brake trips. Telegram is the autonomous notification channel; LAN-only HTTP endpoints are for spot-checks. State (which day we're in, recent runs, emergency flag, cookie warnings) lives in SQLite at `/state/state.db`.

## Hard-won lessons (don't relearn these)

- **bandcampsync in Docker mode does NOT write per-album `bandcamp_item_id.txt` markers.** It uses a central `/config/ignores.txt` instead (see `sync.py:191-204` — it's `if ign_file_path: ignores.add() else: write_marker()`, never both). The completion counter must read ignores.txt.
- **Anomaly detection must skip `[INFO]` log lines.** Album/artist names like „forbidden cremme" appear in INFO and would otherwise trip the brake on first run. Real auth/rate errors land at `[WARNING]`/`[ERROR]`.
- **`sudo cat > /path` does NOT work** because shell redirect happens before sudo elevates. Use `sudo tee path > /dev/null <<'EOF' …` for heredocs as non-root.
- **GitHub publishes container packages as private by default**, even for public repos. Must flip via package settings UI; not API-doable with the standard token scopes.
- **TrueNAS Scale Custom Apps don't auto-load `.env`.** Env values must be entered in the UI's Environment Variables panel or inlined in the YAML. The `${VAR:-default}` form in compose only works when launching via `docker compose` directly on the host.
- **Watchtower can't see bandcampsync.** That image is spawned ad-hoc by the sidecar, never running between daily runs. If you want bandcampsync auto-updated, do it by adding a `docker pull` call in `BandcampsyncController.start()`, not by labeling.

## Things to remember when editing

- `app.py` runs as root inside the container. Necessary for Docker socket access.
- The bandcampsync container runs as `568:568` (TrueNAS apps user) so writes have correct ownership on the bind-mounted ZFS dataset.
- The sidecar mounts `/config` and `/downloads` read-only EXCEPT when the metadata enricher needs to write `bandcamp_<id>.json` files — then `/downloads` flips to rw. Check the actual compose for current state.
- Schema migrations on the SQLite go via the `ALTER TABLE ADD COLUMN` block in `State.__init__` — wrap each in try/except OperationalError, that's how additive migrations work.
- Pydantic-settings v2 parses `WARDEN_RAMP_QUOTAS=[30,100,200]` as JSON automatically. Don't try to be clever with comma-splitting.
- The TrueNAS hostname on the user's LAN is `homeserver`. SSH is disabled — for any host-side ops, write commands the user can paste into the TrueNAS web shell as `truenas_admin` with `sudo`.

## Key external pointers

- bandcampsync source: https://github.com/meeb/bandcampsync (Python, MIT)
- Bandcamp Fan API endpoint used: `POST https://bandcamp.com/api/fancollection/1/collection_items`
- TrueNAS Scale Custom App docs: see TrueNAS UI → Apps → Discover → Custom App; YAML is plain compose
