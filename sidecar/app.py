"""
bandcamp-warden sidecar.

Spawns one-shot bandcampsync containers on a daily schedule, watches their logs,
counts completed albums via the bandcamp_item_id.txt markers bandcampsync writes,
stops bandcampsync when the daily ramp-up quota is hit, and slams an emergency
brake when 401/403/429 responses cluster (the documented account-ban precursor).

State lives in SQLite under /state. Telegram is the autonomous notification
channel; the HTTP endpoints are for LAN-side spot-checks.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import re
import shutil
import sqlite3
import threading
import urllib.parse
import zipfile
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import curl_cffi.requests as curl_requests
import docker
import docker.errors
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# ---------- Config ----------

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="WARDEN_", env_file=None)

    # Bandcampsync container management
    bandcampsync_image: str = "ghcr.io/meeb/bandcampsync:latest"
    bandcampsync_container: str = "bandcamp-warden-bandcampsync"
    bandcampsync_concurrency: int = 1
    bandcampsync_max_retries: int = 3
    bandcampsync_retry_wait: int = 30  # seconds; aggressive backoff on 429
    bandcampsync_puid: int = 1000
    bandcampsync_pgid: int = 1000

    # Host paths (passed into bandcampsync container as bind mounts)
    host_downloads_path: str  # e.g. /mnt/storage/media/bandcamp
    host_config_path: str     # e.g. /mnt/apps/bandcamp-warden/config
    # Optional: host path of the sidecar's /state mount. If set, the
    # patched bandcampsync download.py is staged under <state>/patches/
    # and bind-mounted into the bandcampsync container at runtime.
    host_state_path: str = ""

    # In-sidecar paths (where the sidecar itself sees the data)
    state_path: str = "/state"
    downloads_view_path: str = "/downloads"  # read-only mount of host_downloads_path
    config_view_path: str = "/config"        # read-only mount of host_config_path

    # Schedule
    daily_run_hour: int = 3
    timezone: str = "Europe/Berlin"

    # Ramp-up: number of albums allowed on day N (last value repeats indefinitely).
    # Default matches the agreed plan: 30 day 1, 100 day 2, 200 day 3+.
    ramp_quotas: list[int] = Field(default_factory=lambda: [30, 100, 200])

    # Anomaly detection
    anomaly_window: int = 15        # how many recent log lines we keep
    anomaly_threshold: int = 3      # matches in the window → emergency stop
    log_buffer_size: int = 2000     # in-memory ring buffer exposed via /logs

    # bandcampsync patch: replaces the upstream download.py with a
    # version that tolerates 5 min of <1KB/s before aborting (vs the
    # default ~30s of <1B/s that was causing curl-error-28 crashes
    # mid-album on big Vaporwave releases).
    bandcampsync_patch_enabled: bool = True
    bandcampsync_patch_target: str = (
        "/usr/local/lib/python3.13/dist-packages/bandcampsync/download.py"
    )

    # Cookie expiry monitoring
    cookies_path: str = "/config/cookies.txt"
    cookie_warn_threshold_days: int = 14
    cookie_check_hour: int = 12     # daily check at midday — shows up at a sane time

    # Downloader strategy: "container" = spawn bandcampsync docker
    # container per run (legacy, has unfixable throttling problems);
    # "sidecar" = run downloads in-process via WardenDownloader
    # (uses curl_cffi+chrome+browser_headers, validated to bypass
    # Bandcamp's CDN throttling).
    downloader_strategy: str = "sidecar"

    # Resilience: auto-retry after bandcampsync crash (network blip, etc.)
    retry_max_per_day: int = 3
    retry_backoffs_minutes: list[int] = Field(default_factory=lambda: [5, 15, 60])

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

    # Inbox watcher (Phase 8b — picks up ZIPs the Plan-E Firefox extension
    # drops in <downloads>/_inbox/ and finalizes them: extract → Artist/Album/,
    # write bandcamp_<id>.json, append to ignores.txt, delete ZIP).
    # Off by default until validated end-to-end on the live system.
    inbox_watcher_enabled: bool = False
    inbox_subfolder: str = "_inbox"
    inbox_poll_seconds: int = 30
    # Quarantine sits inside the inbox so it's visible from the same SMB share.
    inbox_quarantine_subfolder: str = "_inbox/_quarantine"
    # Skip ZIPs newer than this — gives Firefox time to finish writing before
    # we try to read. Cheaper than a size-stability poll and good enough on
    # SMB where stat() can lie about size during in-flight writes.
    inbox_min_file_age_seconds: int = 30
    # Fan API metadata cache TTL. We refresh on cache miss anyway, so a
    # generous TTL just bounds memory use of stale rows.
    inbox_api_cache_ttl_seconds: int = 3600
    # When inbox watcher is the production path, the daily bandcampsync
    # kickoff should be off (extension owns the downloads now). Setting
    # this skips registering the daily_kickoff cron job.
    daily_kickoff_enabled: bool = True

    # Phase 8c — HTTP upload endpoint. When the token is set, the
    # POST /inbox/upload endpoint accepts streamed ZIPs from the Plan-E
    # browser extension and writes them directly to the inbox without
    # the SMB round-trip. Empty token disables the endpoint entirely
    # (it returns 503), so a default install can't be abused as an
    # open file drop.
    inbox_upload_auth_token: str = ""
    # Reject uploads larger than this (bytes). 2 GB is generous; biggest
    # Bandcamp lossless albums sit well under 1 GB.
    inbox_upload_max_bytes: int = 2 * 1024 * 1024 * 1024
    # How long .partial files may sit before the watcher's janitor sweeps
    # them. Covers extension-side aborts mid-upload. 1h is conservative.
    inbox_partial_max_age_seconds: int = 3600

    # Docker network the bandcampsync container should attach to (optional).
    docker_network: str | None = None


settings = Settings()


# ---------- Logging ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("warden")

# httpx INFO logs print the full request URL on every call. That includes
# the Telegram bot token in the path. Mute it — we still get failure
# details from the response handling in Telegram.send().
logging.getLogger("httpx").setLevel(logging.WARNING)


class _NoHealthzFilter(logging.Filter):
    """Drop uvicorn access records for /healthz (and the docker healthcheck
    that hits localhost). The healthcheck runs every 30s and floods the
    log with no useful information."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "/healthz" not in msg


logging.getLogger("uvicorn.access").addFilter(_NoHealthzFilter())
# uvicorn.access doesn't propagate to root by default — explicitly attach
# our ring buffer at startup once it's defined further down. (See
# _RingLogHandler attachment below; we'll add it to uvicorn.access too.)


class _RingLogHandler(logging.Handler):
    """In-memory ring buffer for the sidecar's own logs, so the
    /sidecar-logs endpoint can return them without docker access."""

    def __init__(self, capacity: int = 2000) -> None:
        super().__init__()
        self.buffer: deque[str] = deque(maxlen=capacity)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.buffer.append(self.format(record))
        except Exception:
            self.handleError(record)


_sidecar_log_buffer = _RingLogHandler(capacity=2000)
_sidecar_log_buffer.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
)
_sidecar_log_buffer.setLevel(logging.INFO)
# Attach to root + uvicorn.access (which doesn't propagate to root by
# default, so we'd otherwise miss every HTTP request log line).
logging.getLogger().addHandler(_sidecar_log_buffer)
logging.getLogger("uvicorn.access").addHandler(_sidecar_log_buffer)
logging.getLogger("uvicorn.error").addHandler(_sidecar_log_buffer)


# ---------- Patterns ----------

# Anomaly = the precursor pattern that previously cost the user's account.
# We're explicit and conservative: any HTTP-error code we'd expect on rate-limit
# or auth issues, plus the human-readable variants bandcampsync may print.
ANOMALY_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(401|403|429)\b"),
    re.compile(r"unauthori[sz]ed", re.IGNORECASE),
    re.compile(r"forbidden", re.IGNORECASE),
    re.compile(r"too many requests", re.IGNORECASE),
    re.compile(r"rate[- ]?limit", re.IGNORECASE),
)

# bandcampsync logs at standard Python levels: [INFO] for routine progress,
# [WARNING]/[ERROR] for actual problems. Album titles and artist names appear
# only in [INFO] lines (e.g. "Found item: forbidden cremme / opulence"). To
# avoid false-positive emergency stops on artist names that happen to contain
# words like "forbidden" or "unauthorized", we only run anomaly detection on
# non-INFO lines.
INFO_LINE = re.compile(r"\[INFO\]")


def is_anomaly_line(line: str) -> bool:
    if INFO_LINE.search(line):
        return False
    return any(p.search(line) for p in ANOMALY_PATTERNS)


# ---------- State (SQLite) ----------

class State:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as db:
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS warden (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    started_on TEXT,
                    emergency_stopped INTEGER NOT NULL DEFAULT 0,
                    last_emergency_at TEXT,
                    last_emergency_reason TEXT,
                    collection_complete INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS daily_runs (
                    run_date TEXT PRIMARY KEY,
                    quota INTEGER NOT NULL,
                    downloaded INTEGER NOT NULL DEFAULT 0,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,           -- running|quota_hit|completed|emergency|failed_to_start
                    stop_reason TEXT
                );
                INSERT OR IGNORE INTO warden (id) VALUES (1);
                """
            )
            # Best-effort additive schema migrations. Each ALTER is wrapped
            # because SQLite's IF NOT EXISTS doesn't apply to ADD COLUMN.
            for stmt in (
                "ALTER TABLE warden ADD COLUMN last_cookie_warning_on TEXT",
                "ALTER TABLE daily_runs ADD COLUMN baseline INTEGER",
                "ALTER TABLE daily_runs ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1",
                "ALTER TABLE daily_runs ADD COLUMN last_exit_code INTEGER",
            ):
                try:
                    db.execute(stmt)
                except sqlite3.OperationalError:
                    pass  # column already exists

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=10)
        c.row_factory = sqlite3.Row
        return c

    def get(self) -> dict:
        with self._conn() as db:
            row = db.execute("SELECT * FROM warden WHERE id = 1").fetchone()
        return dict(row)

    def update(self, **fields) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        with self._conn() as db:
            db.execute(f"UPDATE warden SET {sets} WHERE id = 1", list(fields.values()))

    def start_run(self, run_date: str, quota: int, baseline: int) -> dict:
        """Begin (or continue) a daily run. Returns the row, including the
        attempt number. baseline is set on first attempt and preserved
        across retries — that way the daily quota is enforced over the
        whole day, not reset on each retry."""
        now = datetime.now(timezone.utc).isoformat()
        with self._conn() as db:
            existing = db.execute(
                "SELECT * FROM daily_runs WHERE run_date = ?", (run_date,)
            ).fetchone()
            if existing is None:
                db.execute(
                    """INSERT INTO daily_runs
                       (run_date, quota, downloaded, baseline, attempt, started_at, status)
                       VALUES (?, ?, 0, ?, 1, ?, 'running')""",
                    (run_date, quota, baseline, now),
                )
            else:
                # Retry: bump attempt, keep baseline, clear terminal fields.
                # COALESCE(baseline, ?) backfills baseline for rows
                # created before the baseline column existed (older
                # rows have it NULL); rows that already have a value
                # keep it so quota is preserved across the whole day.
                db.execute(
                    """UPDATE daily_runs
                       SET attempt = attempt + 1,
                           baseline = COALESCE(baseline, ?),
                           status = 'running',
                           stop_reason = NULL,
                           finished_at = NULL,
                           last_exit_code = NULL
                       WHERE run_date = ?""",
                    (baseline, run_date),
                )
            row = db.execute(
                "SELECT * FROM daily_runs WHERE run_date = ?", (run_date,)
            ).fetchone()
        return dict(row)

    def update_run(self, run_date: str, **fields) -> None:
        if not fields:
            return
        sets = ", ".join(f"{k} = ?" for k in fields)
        with self._conn() as db:
            db.execute(
                f"UPDATE daily_runs SET {sets} WHERE run_date = ?",
                [*fields.values(), run_date],
            )

    def get_run(self, run_date: str) -> dict | None:
        with self._conn() as db:
            row = db.execute(
                "SELECT * FROM daily_runs WHERE run_date = ?", (run_date,)
            ).fetchone()
        return dict(row) if row else None

    def recent_runs(self, n: int = 14) -> list[dict]:
        with self._conn() as db:
            rows = db.execute(
                "SELECT * FROM daily_runs ORDER BY run_date DESC LIMIT ?", (n,)
            ).fetchall()
        return [dict(r) for r in rows]


# ---------- Completion counter ----------

# bandcampsync's own truth-of-record for "what's done" in Docker mode is the
# /config/ignores.txt file. Per bandcampsync/sync.py:191-204, the per-album
# bandcamp_item_id.txt marker only gets written when ign_file_path is unset
# — which is never, in Docker mode (entrypoint always sets it). So we count
# IDs in ignores.txt: each successful download appends one line.
def count_completed_albums(config_view: Path) -> int:
    ignores = config_view / "ignores.txt"
    if not ignores.exists():
        return 0
    count = 0
    for line in ignores.read_text(errors="replace").splitlines():
        # bandcampsync's format: comments starting with '#', plus IDs as
        # plain integers. Strip inline comments and whitespace; count the
        # non-empty remainder.
        stripped = line.split("#", 1)[0].strip()
        if stripped:
            count += 1
    return count


# ---------- Cookie expiry ----------

def cookie_identity_expiry(cookies_path: Path) -> datetime | None:
    """Read the bandcamp identity cookie's expiry from a Netscape cookies.txt.

    Returns None if the file is missing, unparseable, the cookie is absent,
    or it's a session cookie (expires=0). Server-side invalidation can still
    kill the cookie before this timestamp — that's caught by the anomaly
    detector, not here. This monitor only catches the hard server-set expiry.
    """
    if not cookies_path.exists():
        return None
    candidates: list[int] = []
    for line in cookies_path.read_text(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        domain, _flag, _path, _secure, expires, name, _value = parts[:7]
        if name != "identity" or "bandcamp.com" not in domain:
            continue
        try:
            ts = int(expires)
        except ValueError:
            continue
        if ts > 0:
            candidates.append(ts)
    if not candidates:
        return None
    return datetime.fromtimestamp(max(candidates), tz=timezone.utc)


# ---------- Telegram ----------

class Telegram:
    def __init__(self, token: str, chat_id: str) -> None:
        self.token = token
        self.chat_id = chat_id
        self.client = httpx.AsyncClient(timeout=15)

    @property
    def configured(self) -> bool:
        return bool(self.token and self.chat_id)

    async def send(self, text: str) -> None:
        if not self.configured:
            log.warning("Telegram not configured; would have sent: %s", text[:120])
            return
        try:
            r = await self.client.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                    "disable_web_page_preview": True,
                },
            )
            r.raise_for_status()
        except Exception as e:
            log.error("Telegram push failed: %s", e)


# ---------- Bandcampsync controller ----------

class BandcampsyncController:
    """Owns the lifecycle of one-shot bandcampsync containers.

    bandcampsync's own daemon mode (RUN_DAILY_AT) doesn't expose a per-run quota,
    which is exactly the safety knob we need. So we run bandcampsync as a fresh
    one-shot container each day and stop it when our quota is hit or when the
    log monitor screams. Each container is removed after use — state lives in
    the bind-mounted /config and /downloads volumes, not the container itself.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.client = docker.from_env()

    def _remove_existing(self) -> None:
        try:
            c = self.client.containers.get(self.settings.bandcampsync_container)
        except docker.errors.NotFound:
            return
        try:
            c.remove(force=True)
        except docker.errors.APIError as e:
            log.warning("Could not remove existing container: %s", e)

    def start(self):
        """Create + start a fresh bandcampsync container. Returns the container."""
        self._remove_existing()
        s = self.settings
        env = {
            "PUID": str(s.bandcampsync_puid),
            "PGID": str(s.bandcampsync_pgid),
            "TZ": s.timezone,
            "CONCURRENCY": str(s.bandcampsync_concurrency),
            "MAX_RETRIES": str(s.bandcampsync_max_retries),
            "RETRY_WAIT": str(s.bandcampsync_retry_wait),
            # Deliberately NOT setting RUN_DAILY_AT — keeps it one-shot.
        }
        volumes = {
            s.host_config_path: {"bind": "/config", "mode": "rw"},
            s.host_downloads_path: {"bind": "/downloads", "mode": "rw"},
        }
        # Mount the patched download.py over the upstream module file
        # so bandcampsync inherits our patient curl options.
        if s.bandcampsync_patch_enabled and s.host_state_path:
            patch_host = (
                f"{s.host_state_path.rstrip('/')}/patches/bandcampsync_download.py"
            )
            volumes[patch_host] = {
                "bind": s.bandcampsync_patch_target,
                "mode": "ro",
            }
            log.info(
                "Bandcampsync download.py patch active: %s → %s",
                patch_host, s.bandcampsync_patch_target,
            )
        elif s.bandcampsync_patch_enabled:
            log.warning(
                "bandcampsync_patch_enabled=true but host_state_path is "
                "empty — patch will NOT be applied"
            )

        kwargs: dict = dict(
            image=s.bandcampsync_image,
            name=s.bandcampsync_container,
            environment=env,
            volumes=volumes,
            detach=True,
            remove=False,  # we read logs after exit, so don't auto-remove
            restart_policy={"Name": "no"},
        )
        if s.docker_network:
            kwargs["network"] = s.docker_network
        c = self.client.containers.run(**kwargs)
        log.info("Started bandcampsync container %s (id=%s)", c.name, c.short_id)
        return c

    def stop(self, timeout: int = 30) -> None:
        try:
            c = self.client.containers.get(self.settings.bandcampsync_container)
        except docker.errors.NotFound:
            return
        if c.status == "running":
            try:
                c.stop(timeout=timeout)
                log.info("Stopped bandcampsync container")
            except docker.errors.APIError as e:
                log.warning("Stop failed: %s", e)

    def is_running(self) -> bool:
        try:
            c = self.client.containers.get(self.settings.bandcampsync_container)
            c.reload()
            return c.status == "running"
        except docker.errors.NotFound:
            return False

    def last_exit_code(self) -> int | None:
        """Exit code of the bandcampsync container's last run, or None
        if no container exists (yet) or status not exposed."""
        try:
            c = self.client.containers.get(self.settings.bandcampsync_container)
            c.reload()
            ec = c.attrs.get("State", {}).get("ExitCode")
            return int(ec) if ec is not None else None
        except docker.errors.NotFound:
            return None

    def stream_logs(self, container) -> Iterable[str]:
        """Yield decoded log lines as bandcampsync produces them."""
        for chunk in container.logs(stream=True, follow=True, tail=0):
            text = chunk.decode("utf-8", errors="replace")
            for line in text.splitlines():
                if line:
                    yield line


# ---------- Metadata enricher ----------

# Unambiguous mapping item_id → folder is built by parsing two bandcampsync
# log line shapes in real time. This is far safer than reverse-engineering
# bandcampsync's slug algorithm — we just record the path bandcampsync
# actually wrote to, paired with the id from the preceding "New media item"
# line. Per-album files are named bandcamp_<item_id>.json so even a freak
# folder collision can't make two albums fight over one filename.

NEW_ITEM_RE = re.compile(r'will download:.*?\(id:(\d+)\)')
MOVE_FILE_RE = re.compile(r'Moving extracted file: ".+?" to "([^"]+)"')

BANDCAMP_API_URL = "https://bandcamp.com/api/fancollection/1/collection_items"
BANDCAMP_USER_AGENT = (
    "bandcamp-warden/1.0 (+https://github.com/idleherb/bandcamp-warden)"
)


class MetadataEnricher:
    """Captures item_id → folder during a run, then fetches Bandcamp Fan API
    metadata at run-end and writes per-album JSON + a central JSONL index."""

    def __init__(
        self,
        config_view: Path,
        downloads_view: Path,
        state_path: Path,
        telegram: Telegram,
    ) -> None:
        self.config_view = config_view
        self.downloads_view = downloads_view
        self.index_path = state_path / "album_index.jsonl"
        self.orphan_path = state_path / "orphaned_metadata.jsonl"
        self.telegram = telegram
        self._reset_run_state()

    def _reset_run_state(self) -> None:
        self.id_to_folder: dict[int, Path] = {}
        self._current_id: int | None = None

    def begin_run(self) -> None:
        self._reset_run_state()

    def observe_log(self, line: str) -> None:
        """Hook for the orchestrator's log loop. Builds id→folder during the run."""
        m = NEW_ITEM_RE.search(line)
        if m:
            self._current_id = int(m.group(1))
            return
        m = MOVE_FILE_RE.search(line)
        if m and self._current_id is not None:
            file_in_container = Path(m.group(1))
            # bandcampsync's view: /downloads/<Artist>/<Album>/<file>
            # Sidecar's view: same path inside its own /downloads mount.
            try:
                rel = file_in_container.relative_to("/downloads")
            except ValueError:
                # bandcampsync logged a path outside /downloads — shouldn't
                # happen, but be defensive.
                return
            folder = self.downloads_view / rel.parent
            self.id_to_folder[self._current_id] = folder

    # ----- cookie + fan-id parsing -----

    def _read_all_bandcamp_cookies(self) -> dict[str, str]:
        """Return every cookie scoped to bandcamp.com from cookies.txt.
        Bandcamp's Fan API expects the full session cookie jar, not just
        identity — sending only identity returns 0 items because the
        request fails authentication / session validation."""
        cookies: dict[str, str] = {}
        p = self.config_view / "cookies.txt"
        if not p.exists():
            return cookies
        for line in p.read_text(errors="replace").splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            domain, _flag, _path, _secure, _expires, name, value = parts[:7]
            if "bandcamp.com" in domain:
                cookies[name] = value
        return cookies

    def _parse_identity_cookie(self) -> tuple[dict[str, str], int | None]:
        """Return (cookie_jar, fan_id). cookie_jar is every bandcamp.com
        cookie; fan_id is parsed from the identity cookie's embedded JSON
        blob (`{"id":<fan_id>,"ex":...}`)."""
        jar = self._read_all_bandcamp_cookies()
        identity = jar.get("identity")
        if not identity:
            return jar, None
        decoded = urllib.parse.unquote(identity)
        m = re.search(r'\{[^{}]*"id"\s*:\s*(\d+)', decoded)
        fan_id = int(m.group(1)) if m else None
        return jar, fan_id

    # ----- Fan API -----

    async def _fetch_collection(
        self, fan_id: int, cookie_jar: dict[str, str]
    ) -> dict[int, dict]:
        """Page through the user's full Fan-API collection. Return {item_id: row}.

        Bandcamp's Fan API gates non-Chrome TLS fingerprints; standard httpx
        gets blocked silently. curl_cffi.Session(impersonate="chrome") spoofs
        a real Chrome handshake — same library bandcampsync uses internally.
        The full bandcamp cookie jar is required, not just identity.
        """
        return await asyncio.to_thread(
            self._fetch_collection_sync, fan_id, cookie_jar
        )

    def _fetch_collection_sync(
        self, fan_id: int, cookie_jar: dict[str, str]
    ) -> dict[int, dict]:
        """Use bandcampsync's own Bandcamp client. It handles auth, TLS
        impersonation, cookie-jar plumbing, paginated load, and item
        de-duplication. fan_id and cookie_jar args are unused now (kept
        for signature compat) — bandcampsync extracts both from the
        cookies.txt content directly."""
        from bandcampsync.bandcamp import Bandcamp, BandcampError  # type: ignore

        cookies_path = self.config_view / "cookies.txt"
        if not cookies_path.exists():
            log.warning("Fan API: cookies.txt missing at %s", cookies_path)
            return {}
        cookies_str = cookies_path.read_text(errors="replace")
        try:
            bc = Bandcamp(cookies_str)
            # v0.7.0 API: verify_authentication primes the session AND
            # populates user_id from the homepage's pagedata blob.
            bc.verify_authentication()
            bc.load_purchases()
        except BandcampError as e:
            log.warning("Fan API (bandcampsync): %s", e)
            return {}
        except Exception as e:
            log.exception("Fan API (bandcampsync) raised %s", type(e).__name__)
            return {}

        out: dict[int, dict] = {}
        for purchase in getattr(bc, "purchases", []) or []:
            data = getattr(purchase, "_data", None)
            if not isinstance(data, dict):
                continue
            iid = data.get("item_id") or data.get("tralbum_id")
            if iid is None:
                continue
            out[int(iid)] = data
        log.info(
            "Fan API: collected %d items via bandcampsync (user_id=%s)",
            len(out), getattr(bc, "user_id", None),
        )
        return out

    # ----- writing -----

    @staticmethod
    def _atomic_write(path: Path, payload: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, path)

    def _build_record(self, item_id: int, folder: Path, api_row: dict | None) -> dict:
        rec = {
            "item_id": item_id,
            "folder": str(folder),
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
            "downloaded_format": "flac",
        }
        if api_row:
            for src, dst in (
                ("band_name", "band_name"),
                ("item_title", "item_title"),
                ("item_url", "item_url"),
                ("tralbum_type", "tralbum_type"),
                ("release_date", "release_date"),
                ("purchased", "purchased_at"),
                ("added", "added_at"),
                ("tralbum_genre", "genre"),
                ("featured_track_title", "featured_track"),
            ):
                v = api_row.get(src)
                if v is not None:
                    rec[dst] = v
        return rec

    async def enrich_run(self) -> dict:
        """Run after the daily monitor loop ends. Writes per-album JSON
        for every item we observed this run, plus appends to the central
        JSONL index. Returns counts for the run summary."""
        seen = dict(self.id_to_folder)  # snapshot
        if not seen:
            return {"observed": 0, "written": 0, "orphaned": 0, "api": False}

        cookie_jar, fan_id = self._parse_identity_cookie()
        api_map: dict[int, dict] = {}
        if cookie_jar and fan_id:
            api_map = await self._fetch_collection(fan_id, cookie_jar)
        else:
            log.warning(
                "Could not extract identity cookie or fan_id; "
                "writing log-derived metadata only"
            )

        written = 0
        orphaned = 0
        for item_id, folder in seen.items():
            api_row = api_map.get(item_id)
            record = self._build_record(item_id, folder, api_row)

            # Append to central index regardless of folder existence
            try:
                with self.index_path.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            except Exception as e:
                log.error("Failed appending to album_index.jsonl: %s", e)

            # Per-album file: write into the album folder if we can,
            # otherwise count as orphaned and append to the fallback jsonl.
            # "Orphaned" includes both no-such-folder and write-failed
            # (e.g. PermissionError when /downloads is mounted read-only).
            wrote_per_album = False
            if folder.exists() and folder.is_dir():
                try:
                    self._atomic_write(
                        folder / f"bandcamp_{item_id}.json",
                        json.dumps(record, ensure_ascii=False, indent=2),
                    )
                    written += 1
                    wrote_per_album = True
                except Exception as e:
                    log.error("Failed writing %s/bandcamp_%d.json: %s", folder, item_id, e)
            if not wrote_per_album:
                orphaned += 1
                try:
                    with self.orphan_path.open("a", encoding="utf-8") as f:
                        f.write(json.dumps(record, ensure_ascii=False) + "\n")
                except Exception as e:
                    log.error("Failed appending to orphaned_metadata.jsonl: %s", e)

        return {
            "observed": len(seen),
            "written": written,
            "orphaned": orphaned,
            "api": bool(api_map),
            "api_items": len(api_map),
        }

    async def backfill(self, only_missing: bool = True) -> dict:
        """Walk the existing /downloads tree, find every album folder, and
        write or refresh its bandcamp_<id>.json. The folder→item_id mapping
        comes from looking up each folder against the Fan API's item_url
        (which encodes the slug bandcampsync uses) — but easier: bandcampsync
        already wrote each ID into /config/ignores.txt in download order,
        so we cross-reference with the Fan API and try to match by
        (band_name slug, item_title slug) to actual folder paths.

        only_missing: if True, skip folders that already have a bandcamp_*.json.
        """
        cookie_jar, fan_id = self._parse_identity_cookie()
        if not cookie_jar or not fan_id:
            return {"error": "could not parse identity cookie / fan_id", "written": 0}
        api_map = await self._fetch_collection(fan_id, cookie_jar)
        if not api_map:
            return {
                "error": "Fan API returned no items — see sidecar logs for HTTP status",
                "written": 0,
                "fan_id": fan_id,
                "cookie_count": len(cookie_jar),
            }

        # Step 1: index existing album folders.
        existing_folders: list[Path] = []
        if self.downloads_view.exists():
            for artist_dir in self.downloads_view.iterdir():
                if not artist_dir.is_dir():
                    continue
                for album_dir in artist_dir.iterdir():
                    if album_dir.is_dir():
                        existing_folders.append(album_dir)

        # Step 2: replicate bandcampsync's folder sanitization exactly.
        # See bandcampsync/media.py LocalMedia._clean_path: NFKD-normalize
        # (turns Math-Bold/fullwidth/combining chars into plain ASCII)
        # then drop this exact set of punctuation chars. Whitespace
        # collapsing + lowercase + trim are our addition to absorb tiny
        # case/spacing drift between API titles and folder names.
        import unicodedata

        _bandcampsync_disallowed = '"#%\'*/?\\`:'

        def normalize(s: str) -> str:
            s = unicodedata.normalize("NFKD", s or "")
            # Drop format-category chars (zero-width spaces, joiners, etc.)
            # and combining marks. NFKD splits accented letters into base
            # + combining; we keep the base for matching purposes.
            s = "".join(
                c for c in s
                if c not in _bandcampsync_disallowed
                and unicodedata.category(c) not in ("Cf", "Mn")
            )
            s = re.sub(r"\s+", " ", s).strip().rstrip(". ").lower()
            return s

        # Step 3: build matching indexes. (band, title) is best, then a
        # title-only fallback for cases where bandcampsync collapsed an
        # exotic artist name differently from the API.
        folder_by_band_title: dict[tuple[str, str], Path] = {}
        folder_by_title: dict[str, list[Path]] = {}
        for f in existing_folders:
            folder_by_band_title[(normalize(f.parent.name), normalize(f.name))] = f
            folder_by_title.setdefault(normalize(f.name), []).append(f)

        # Step 4: prefer items in ignores.txt — those are provably the
        # ones bandcampsync downloaded. Falling back to all api_map keeps
        # behaviour sane if ignores.txt is missing.
        downloaded_ids: set[int] | None = None
        ignores_path = self.config_view / "ignores.txt"
        if ignores_path.exists():
            ids: set[int] = set()
            for line in ignores_path.read_text(errors="replace").splitlines():
                stripped = line.split("#", 1)[0].strip()
                if stripped.isdigit():
                    ids.add(int(stripped))
            if ids:
                downloaded_ids = ids

        candidates = (
            {iid: api_map[iid] for iid in downloaded_ids if iid in api_map}
            if downloaded_ids
            else api_map
        )

        written = 0
        skipped = 0
        unmatched_items: list[dict] = []
        for item_id, api_row in candidates.items():
            band = api_row.get("band_name", "")
            title = api_row.get("item_title", "")
            band_n = normalize(band)
            title_n = normalize(title)
            folder = folder_by_band_title.get((band_n, title_n))
            if folder is None:
                # Title-only fallback (avoid ambiguous matches).
                hits = folder_by_title.get(title_n) or []
                if len(hits) == 1:
                    folder = hits[0]
            if folder is None:
                if len(unmatched_items) < 50:
                    unmatched_items.append({
                        "item_id": item_id,
                        "band_name": band,
                        "item_title": title,
                        "expected_band_norm": band_n,
                        "expected_title_norm": title_n,
                    })
                continue
            target = folder / f"bandcamp_{item_id}.json"
            if only_missing and target.exists():
                skipped += 1
                continue
            record = self._build_record(item_id, folder, api_row)
            try:
                self._atomic_write(
                    target,
                    json.dumps(record, ensure_ascii=False, indent=2),
                )
                written += 1
            except Exception as e:
                log.error("Backfill write failed for %s: %s", target, e)
        return {
            "api_items": len(api_map),
            "ignores_ids": len(downloaded_ids) if downloaded_ids else None,
            "candidates": len(candidates),
            "existing_folders": len(existing_folders),
            "written": written,
            "skipped_already_present": skipped,
            "unmatched_count": len(unmatched_items),
            "unmatched_examples": unmatched_items,
        }


# ---------- Inbox Watcher (Phase 8b) ----------

# bandcampsync's filesystem character set, replicated for output sanitization.
# Same disallowed set the existing 295 album folders were named with — keeps
# new arrivals collision-free against backfill_metadata's matcher.
_BANDCAMPSYNC_FORBIDDEN = '"#%\'*/?\\`:'


def clean_path_component(name: str) -> str:
    """Filesystem-safe component matching bandcampsync's LocalMedia._clean_path.

    The non-obvious bit: bandcampsync NFKD-normalizes before writing, which
    decomposes Mathematical Bold / Double-struck / Fullwidth Latin into
    plain ASCII (𝔸𝕧𝕣𝕚𝕝 → Avril), then drops combining marks (Mn) and
    format characters (Cf — zero-width joiners, etc.). CJK ideographs
    pass through untouched because NFKD doesn't decompose them.

    If we don't replicate this exactly, my output collides with neither
    the existing 295 album folders nor what bandcampsync would have
    written, producing a parallel namespace and breaking the Idempotency
    check in _process_one. Verified empirically on item 2392412674
    (𝔸𝕧𝕣𝕚𝕝 — 𝔸𝕧𝕣𝕚𝕝 𝟚) — must collapse to "Avril/Avril 2".
    """
    import unicodedata
    decomposed = unicodedata.normalize("NFKD", name or "")
    cleaned = "".join(
        c for c in decomposed
        if c not in _BANDCAMPSYNC_FORBIDDEN
        and unicodedata.category(c) not in ("Cf", "Mn")
    )
    # Only strip whitespace, NOT trailing periods — bandcampsync keeps
    # them. Verified against existing folder "猫 シ Corp." in user's
    # library (/downloads/猫 シ Corp./Blueberries on Mars).
    cleaned = cleaned.strip()
    return cleaned or '_unknown'


_AUDIO_EXTS = {'.flac', '.mp3', '.m4a', '.ogg', '.wav', '.aiff', '.aif', '.opus'}


def _detect_zip_root_prefix(names: list[str]) -> str:
    """If every entry in the ZIP shares a common top-level folder prefix,
    return it (with trailing slash). Otherwise return ''. Bandcamp ZIPs vary
    — sometimes flat, sometimes wrapped in '<Artist> - <Album>/'."""
    if not names:
        return ''
    first = names[0].split('/', 1)[0]
    if not first or first == names[0]:
        return ''
    prefix = first + '/'
    for n in names:
        if not n.startswith(prefix):
            return ''
    return prefix


class InboxWatcher:
    """Polls <downloads>/_inbox/ for ZIPs the Plan-E extension dropped (or
    POSTed via /inbox/upload), looks each item up via the Fan API metadata
    cache, extracts into <Artist>/<Album>/, writes bandcamp_<id>.json,
    appends to ignores.txt, deletes the ZIP."""

    _FILENAME_RE = re.compile(r'^bandcamp_(\d+)\.zip$')

    def __init__(
        self,
        config_view: Path,
        downloads_view: Path,
        metadata_enricher: MetadataEnricher,
        telegram: Telegram,
    ) -> None:
        self.config_view = config_view
        self.downloads_view = downloads_view
        self.metadata_enricher = metadata_enricher
        self.telegram = telegram
        self.inbox_path = downloads_view / settings.inbox_subfolder
        self.quarantine_path = downloads_view / settings.inbox_quarantine_subfolder
        self._stop = asyncio.Event()
        self._api_cache: dict[int, dict] = {}
        self._api_cache_at: datetime | None = None
        self._processed_count = 0
        self._quarantined_count = 0
        self._last_processed_at: datetime | None = None
        self._last_error: str | None = None

    def status(self) -> dict:
        """Snapshot for /inbox-status endpoint."""
        try:
            pending = sorted(p.name for p in self.inbox_path.glob('bandcamp_*.zip'))
        except OSError as e:
            pending = []
            self._last_error = f'pending listing failed: {e}'
        try:
            partials = sorted(
                p.name for p in self.inbox_path.glob('bandcamp_*.zip.partial')
            )
        except OSError:
            partials = []
        try:
            quarantined = sorted(
                p.name for p in self.quarantine_path.glob('bandcamp_*.zip')
            )
        except OSError:
            quarantined = []
        return {
            'enabled': settings.inbox_watcher_enabled,
            'inbox_path': str(self.inbox_path),
            'quarantine_path': str(self.quarantine_path),
            'pending': pending,
            'partials': partials,
            'quarantined': quarantined,
            'pending_count': len(pending),
            'partials_count': len(partials),
            'quarantined_count': len(quarantined),
            'processed_total': self._processed_count,
            'quarantined_total': self._quarantined_count,
            'last_processed_at': (
                self._last_processed_at.isoformat() if self._last_processed_at else None
            ),
            'api_cache_size': len(self._api_cache),
            'api_cache_at': (
                self._api_cache_at.isoformat() if self._api_cache_at else None
            ),
            'last_error': self._last_error,
        }

    async def run_loop(self) -> None:
        """Poll forever until stopped. Errors are caught per-iteration so
        a transient hiccup (e.g. SMB blip on the dataset) doesn't kill
        the loop. Slowing or speeding the cadence happens via the
        inbox_poll_seconds setting; default 30s."""
        log.info('inbox watcher starting (path=%s)', self.inbox_path)
        try:
            self.inbox_path.mkdir(parents=True, exist_ok=True)
            self.quarantine_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            log.error('inbox watcher setup failed: %s', e)
            self._last_error = str(e)
            return
        while not self._stop.is_set():
            try:
                await self._sweep_partials()
                await self._process_pending()
            except Exception as e:
                self._last_error = f'{type(e).__name__}: {e}'
                log.exception('inbox watcher sweep raised')
            try:
                await asyncio.wait_for(
                    self._stop.wait(), timeout=settings.inbox_poll_seconds
                )
            except asyncio.TimeoutError:
                pass
        log.info('inbox watcher stopped')

    def stop(self) -> None:
        self._stop.set()

    async def _sweep_partials(self) -> None:
        """Delete .partial files older than the configured TTL — they're
        leftovers from upload-aborts and won't be completed."""
        cutoff = datetime.now(timezone.utc).timestamp() - settings.inbox_partial_max_age_seconds
        try:
            partials = list(self.inbox_path.glob('bandcamp_*.zip.partial'))
        except OSError:
            return
        for p in partials:
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
                    log.info('inbox: swept stale .partial: %s', p.name)
            except OSError:
                continue

    async def _process_pending(self) -> None:
        """One sweep of the inbox. Reads the directory, picks ZIPs that
        are old enough to be considered fully written, processes them
        sequentially. Sequential is intentional — extracting multiple
        500MB ZIPs in parallel would thrash any home-NAS disk."""
        try:
            zips = sorted(self.inbox_path.glob('bandcamp_*.zip'))
        except OSError as e:
            self._last_error = f'inbox listing failed: {e}'
            return
        if not zips:
            return
        cutoff = datetime.now(timezone.utc).timestamp() - settings.inbox_min_file_age_seconds
        ready: list[tuple[int, Path]] = []
        for zp in zips:
            try:
                mtime = zp.stat().st_mtime
            except OSError:
                continue
            if mtime > cutoff:
                continue
            m = self._FILENAME_RE.match(zp.name)
            if not m:
                log.warning('inbox: filename does not match pattern, ignoring: %s', zp.name)
                continue
            ready.append((int(m.group(1)), zp))
        if not ready:
            return

        # If any ID isn't in our cached API map, refresh the cache once.
        await self._ensure_api_cache(needed_ids=[iid for iid, _ in ready])

        for item_id, zp in ready:
            api_row = self._api_cache.get(item_id)
            try:
                await asyncio.to_thread(
                    self._process_one, item_id, zp, api_row
                )
            except Exception as e:
                self._last_error = f'item {item_id}: {type(e).__name__}: {e}'
                log.exception('inbox: processing item %d raised', item_id)

    async def _ensure_api_cache(self, needed_ids: list[int]) -> None:
        """Refresh the cache if any needed_id is missing or TTL expired."""
        now = datetime.now(timezone.utc)
        ttl_ok = (
            self._api_cache_at is not None
            and (now - self._api_cache_at).total_seconds()
            < settings.inbox_api_cache_ttl_seconds
        )
        all_present = ttl_ok and all(iid in self._api_cache for iid in needed_ids)
        if all_present:
            return
        log.info(
            'inbox: refreshing Fan API cache (needed_ids=%d, cache_size=%d)',
            len(needed_ids), len(self._api_cache),
        )
        try:
            api_map = await asyncio.to_thread(
                self.metadata_enricher._fetch_collection_sync, 0, {}
            )
        except Exception as e:
            log.exception('inbox: Fan API refresh failed')
            self._last_error = f'fan api refresh: {type(e).__name__}: {e}'
            return
        if api_map:
            self._api_cache = api_map
            self._api_cache_at = now

    def _process_one(self, item_id: int, zip_path: Path, api_row: dict | None) -> None:
        """Synchronous core: extract one ZIP, write JSON, update ignores,
        delete original. Runs in a worker thread."""
        if api_row is None:
            log.warning(
                'inbox: no Fan API row for item %d, quarantining', item_id
            )
            self._quarantine(zip_path, reason='no_api_row')
            return

        band = clean_path_component(api_row.get('band_name', ''))
        title = clean_path_component(api_row.get('item_title', ''))
        if band == '_unknown' or title == '_unknown':
            log.warning(
                'inbox: API row for item %d missing band/title, quarantining', item_id
            )
            self._quarantine(zip_path, reason='blank_band_or_title')
            return
        target_folder = self.downloads_view / band / title
        json_marker = target_folder / f'bandcamp_{item_id}.json'

        # Idempotency — if the canonical JSON marker already exists, this
        # item was finalized previously. Just clean the inbox copy.
        if json_marker.exists():
            log.info(
                'inbox: item %d already finalized at %s, removing inbox copy',
                item_id, target_folder,
            )
            zip_path.unlink(missing_ok=True)
            self._processed_count += 1
            self._last_processed_at = datetime.now(timezone.utc)
            return

        # Extract into a staging folder first, then rename to the final
        # name. If extract fails partway, the half-baked folder doesn't
        # collide with bandcampsync's existing layout.
        staging = target_folder.with_name(target_folder.name + '.warden-partial')
        try:
            if staging.exists():
                shutil.rmtree(staging)
            staging.mkdir(parents=True, exist_ok=True)
            self._extract_zip(zip_path, staging, item_id)
            audio_files = [
                p for p in staging.iterdir()
                if p.is_file() and p.suffix.lower() in _AUDIO_EXTS
            ]
            if not audio_files:
                log.error(
                    'inbox: no audio files after extracting item %d, quarantining',
                    item_id,
                )
                shutil.rmtree(staging, ignore_errors=True)
                self._quarantine(zip_path, reason='no_audio_after_extract')
                return

            # Move staging into final position. If target already exists
            # (rare — we checked json_marker above; but bandcampsync may
            # have written a folder without our JSON marker), merge files
            # in rather than clobber.
            target_folder.parent.mkdir(parents=True, exist_ok=True)
            if target_folder.exists():
                for src in staging.iterdir():
                    dst = target_folder / src.name
                    if dst.exists():
                        log.warning(
                            'inbox: skipping overwrite of existing %s', dst
                        )
                        src.unlink()
                    else:
                        shutil.move(str(src), str(dst))
                shutil.rmtree(staging, ignore_errors=True)
            else:
                staging.rename(target_folder)

            # Write canonical JSON marker (matches MetadataEnricher format).
            record = self.metadata_enricher._build_record(
                item_id, target_folder, api_row
            )
            self.metadata_enricher._atomic_write(
                json_marker, json.dumps(record, ensure_ascii=False, indent=2)
            )

            # Append to ignores.txt so any future bandcampsync run skips
            # this item. Ignores append is idempotent — duplicate lines
            # are harmless to bandcampsync.
            self._append_to_ignores(item_id)

            zip_path.unlink(missing_ok=True)
            self._processed_count += 1
            self._last_processed_at = datetime.now(timezone.utc)
            log.info(
                'inbox: processed item %d → %s (audio_files=%d)',
                item_id, target_folder, len(audio_files),
            )
        except zipfile.BadZipFile:
            log.error('inbox: bad ZIP for item %d, quarantining', item_id)
            shutil.rmtree(staging, ignore_errors=True)
            self._quarantine(zip_path, reason='bad_zip')
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def _extract_zip(
        self, zip_path: Path, target_folder: Path, item_id: int
    ) -> None:
        """Extract files from ZIP into target_folder, flat. Strips a single
        common root folder if Bandcamp wrapped everything in one. Skips
        directories. Path traversal attempts (../) are rejected."""
        with zipfile.ZipFile(zip_path) as zf:
            names = [n for n in zf.namelist() if not n.endswith('/')]
            if not names:
                raise ValueError(f'item {item_id}: zip has no files')
            root_prefix = _detect_zip_root_prefix(names)
            for name in names:
                # Reject anything that tries to escape via .. — zipfile
                # doesn't sanitize this and a malicious ZIP could write
                # outside the target.
                if '..' in Path(name).parts:
                    log.warning(
                        'inbox: rejecting traversal entry %s in item %d',
                        name, item_id,
                    )
                    continue
                if root_prefix and name.startswith(root_prefix):
                    rel = name[len(root_prefix):]
                else:
                    rel = name
                if not rel:
                    continue
                # Flatten — we don't expect subdirs in Bandcamp ZIPs, but
                # if there are any, take the leaf name.
                leaf = Path(rel).name
                if not leaf:
                    continue
                dest = target_folder / leaf
                with zf.open(name) as src, dest.open('wb') as out:
                    shutil.copyfileobj(src, out, length=64 * 1024)

    def _append_to_ignores(self, item_id: int) -> None:
        ignores_path = self.config_view / 'ignores.txt'
        try:
            with ignores_path.open('a', encoding='utf-8') as f:
                f.write(f'{item_id}\n')
        except OSError as e:
            log.warning(
                'inbox: could not append item %d to ignores.txt: %s', item_id, e
            )

    def _quarantine(self, zip_path: Path, reason: str) -> None:
        try:
            self.quarantine_path.mkdir(parents=True, exist_ok=True)
            target = self.quarantine_path / zip_path.name
            # Don't blow away an existing quarantined copy; suffix instead.
            counter = 0
            while target.exists():
                counter += 1
                target = self.quarantine_path / (
                    zip_path.stem + f'.{counter}' + zip_path.suffix
                )
            shutil.move(str(zip_path), str(target))
            self._quarantined_count += 1
            log.warning(
                'inbox: quarantined %s (reason=%s) → %s',
                zip_path.name, reason, target,
            )
            asyncio.create_task(
                self.telegram.send(
                    f'⚠️ *warden inbox quarantine*\n'
                    f'`{zip_path.name}`\nreason: `{reason}`'
                )
            )
        except OSError as e:
            log.error(
                'inbox: failed to quarantine %s (reason=%s): %s',
                zip_path.name, reason, e,
            )


# ---------- Orchestrator ----------

class Orchestrator:
    def __init__(
        self,
        state: State,
        controller: BandcampsyncController,
        telegram: Telegram,
        config_view: Path,
        settings: Settings,
        enricher: MetadataEnricher,
    ) -> None:
        self.state = state
        self.controller = controller
        self.telegram = telegram
        self.config_view = config_view
        self.settings = settings
        self.enricher = enricher
        self.log_buffer: deque[str] = deque(maxlen=settings.log_buffer_size)
        self._run_lock = asyncio.Lock()
        self._last_download_at: str | None = None
        # Set by /stop to suppress the auto-retry that would otherwise fire
        # when the bandcampsync container exits with a non-zero code due to
        # being SIGTERM'd. Cleared at the start of every kickoff.
        self._user_stop_requested: bool = False
        # Track the asyncio task scheduling the next retry, so /stop or a
        # repeat /trigger can cancel it cleanly.
        self._retry_task: asyncio.Task | None = None

    def quota_for_day(self, day_index: int) -> int:
        ramps = self.settings.ramp_quotas or [200]
        return ramps[min(day_index, len(ramps) - 1)]

    def days_in(self, started_on: str | None) -> int:
        if not started_on:
            return 0
        return (date.today() - date.fromisoformat(started_on)).days

    def cookie_status(self) -> dict:
        """Snapshot of identity-cookie expiry for /status."""
        expiry = cookie_identity_expiry(Path(self.settings.cookies_path))
        if not expiry:
            return {"present": False}
        delta = expiry - datetime.now(timezone.utc)
        return {
            "present": True,
            "expires_at": expiry.isoformat(),
            "days_remaining": max(0, delta.days),
            "expired": delta.total_seconds() <= 0,
        }

    async def check_cookie_expiry(self) -> None:
        """Daily cookie freshness check. Warns at most once per day under threshold."""
        info = self.cookie_status()
        if not info["present"]:
            log.info("Cookie file missing or unparseable; skipping expiry check")
            return
        days = info["days_remaining"]
        if not info["expired"] and days > self.settings.cookie_warn_threshold_days:
            return  # plenty of time, stay quiet

        today = date.today().isoformat()
        s = self.state.get()
        if s.get("last_cookie_warning_on") == today:
            return  # already warned today, no spam
        self.state.update(last_cookie_warning_on=today)

        if info["expired"]:
            head = "🍪 *Cookie ABGELAUFEN*"
        elif days <= 3:
            head = f"🍪 *Cookie läuft in {days} Tagen ab* — bitte heute erneuern"
        else:
            head = f"🍪 *Cookie läuft in {days} Tagen ab*"
        await self.telegram.send(
            f"{head}\n"
            f"Ablauf: `{info['expires_at']}`\n\n"
            "Erneuern:\n"
            "1. Frischer Firefox-Login auf bandcamp.com\n"
            "2. cookies.txt mit der Browser-Extension exportieren\n"
            "3. Datei nach `/mnt/apps/bandcamp-warden/config/cookies.txt` kopieren\n"
            "(Resume bleibt erhalten — die Marker liegen in den Album-Ordnern.)"
        )

    async def daily_kickoff(self, override_quota: int | None = None) -> None:
        """Top-level entry point. APScheduler fires this once per day."""
        if self._run_lock.locked():
            log.warning("Kickoff skipped — previous run still in progress")
            return
        async with self._run_lock:
            await self._do_kickoff(override_quota=override_quota)

    async def _do_kickoff(self, override_quota: int | None = None) -> None:
        s = self.state.get()
        run_date = date.today().isoformat()

        if s["emergency_stopped"]:
            log.warning("Emergency stop active — skipping kickoff")
            await self.telegram.send(
                "⏸ *Tag übersprungen*\n"
                f"Notbremse aktiv seit `{s['last_emergency_at']}`.\n"
                f"Grund: {s['last_emergency_reason']}\n\n"
                "Reset: `POST /reset-emergency` am Sidecar."
            )
            return

        if s["collection_complete"]:
            log.info("Collection already marked complete — skipping kickoff")
            return

        # Initialise start date on first run.
        if not s["started_on"]:
            self.state.update(started_on=run_date)
            s = self.state.get()

        day_index = self.days_in(s["started_on"])
        quota = (
            override_quota
            if override_quota is not None and override_quota > 0
            else self.quota_for_day(day_index)
        )

        # Baseline must be set ONCE per day and reused across retries —
        # otherwise a retry resets the count and we'd download up to
        # `quota` more albums per attempt instead of `quota` total today.
        existing_run = self.state.get_run(run_date)
        if existing_run and existing_run.get("baseline") is not None:
            baseline = existing_run["baseline"]
        else:
            baseline = count_completed_albums(self.config_view)

        run_row = self.state.start_run(run_date, quota, baseline)
        attempt = run_row.get("attempt", 1)
        is_retry = attempt > 1

        kickoff_msg = (
            f"▶ *Tag {day_index + 1} startet* (`{run_date}`)\n"
            f"Quota heute: *{quota}* Alben\n"
            f"Bisher gesamt: *{baseline}* Alben"
        )
        if is_retry:
            kickoff_msg = (
                f"🔁 *Tag {day_index + 1} Retry {attempt}/{self.settings.retry_max_per_day + 1}*"
                f" (`{run_date}`)\n"
                f"Bisher heute geschafft: *{count_completed_albums(self.config_view) - baseline}*/{quota}\n"
                f"Gesamt: *{count_completed_albums(self.config_view)}*"
            )
        await self.telegram.send(kickoff_msg)

        # Reset user-stop signal at the start of every new attempt.
        self._user_stop_requested = False

        if self.settings.downloader_strategy == "sidecar":
            await self._kickoff_sidecar(run_date, baseline, quota, attempt)
            return

        # Legacy: spawn bandcampsync container.
        try:
            container = self.controller.start()
        except Exception as e:
            log.exception("Failed to start bandcampsync")
            self.state.update_run(
                run_date,
                status="failed_to_start",
                stop_reason=str(e),
                finished_at=datetime.now(timezone.utc).isoformat(),
            )
            await self.telegram.send(f"❌ bandcampsync-Start fehlgeschlagen:\n```\n{e}\n```")
            return

        self.enricher.begin_run()
        await self._monitor(run_date, baseline, quota, container, attempt)

    async def _kickoff_sidecar(
        self, run_date: str, baseline: int, quota: int, attempt: int,
    ) -> None:
        """Plan C kickoff: download albums in-process via WardenDownloader.

        No bandcampsync container spawn, no log-stream parsing — direct
        Python control of the download flow with curl_cffi+chrome+
        browser_headers (the combination that empirically gets Bandcamp
        to serve files at MB/s rather than throttling to 0).
        """
        from downloader import WardenDownloader  # local module

        # WardenDownloader writes ignores entries to /state (always RW)
        # if /config is mounted RO; reads union of both. So no compose
        # change required.
        downloads_root = Path(self.settings.downloads_view_path)
        config_dir = Path(self.settings.config_view_path)
        state_dir = Path(self.settings.state_path)
        dl = WardenDownloader(
            downloads_root=downloads_root,
            config_dir=config_dir,
            state_dir=state_dir,
            format_name="flac",
        )

        # Wire the downloader's cancel signal to our user-stop flag.
        # /stop sets self._user_stop_requested; we'd ideally observe it
        # in a callback. WardenDownloader checks dl.cancel_requested
        # between albums, so we just propagate.
        async def _watch_user_stop():
            while not dl.cancel_requested:
                if self._user_stop_requested:
                    dl.cancel_requested = True
                    return
                await asyncio.sleep(2)
        watcher = asyncio.create_task(_watch_user_stop())

        # log_event callback: emit warden-namespaced log lines so the
        # ring buffer captures them, plus push selected lines to the
        # log_buffer so /logs continues to surface activity.
        def _log(s: str) -> None:
            log.info("daily-run: %s", s)
            self.log_buffer.append(s)

        try:
            summary = await dl.run_daily(
                quota=quota, baseline_count=baseline, log_event=_log,
            )
        finally:
            watcher.cancel()
            try:
                await watcher
            except (asyncio.CancelledError, Exception):
                pass

        # Persist + telegram.
        finished_at = summary.finished_at
        downloaded_today = summary.downloaded
        final = summary.final_count

        if summary.stop_reason == "quota_hit":
            self.state.update_run(
                run_date, downloaded=downloaded_today, status="quota_hit",
                stop_reason="daily quota reached", finished_at=finished_at,
            )
            msg = (
                f"✅ *Tag fertig* (`{run_date}`)\n"
                f"Heute: *{downloaded_today}*/{quota}\nGesamt: *{final}*"
            )
        elif summary.stop_reason == "cancelled":
            self.state.update_run(
                run_date, downloaded=downloaded_today, status="completed",
                stop_reason="user-initiated stop", finished_at=finished_at,
            )
            msg = (
                f"⏹ *Manuell gestoppt* (`{run_date}`)\n"
                f"Heute: *{downloaded_today}*/{quota}, Gesamt: *{final}*"
            )
        elif summary.stop_reason == "failed_to_start":
            self.state.update_run(
                run_date, downloaded=downloaded_today, status="failed_to_start",
                stop_reason=summary.last_error, finished_at=finished_at,
            )
            msg = (
                f"❌ *Start fehlgeschlagen*\n```\n{summary.last_error}\n```"
            )
        elif summary.stop_reason == "circuit_break":
            # N consecutive failures — likely Bandcamp throttle. Don't
            # keep hammering. Mark today as crashed so the auto-retry
            # logic backs off, and surface the issue loudly.
            self.state.update_run(
                run_date, downloaded=downloaded_today, status="crashed",
                stop_reason="circuit breaker (consecutive failures)",
                finished_at=finished_at,
            )
            msg = (
                f"⛔ *Circuit Breaker* (`{run_date}`)\n"
                f"Mehrere Alben hintereinander gescheitert. "
                f"Heute: *{downloaded_today}*/{quota}, "
                f"Gesamt: *{final}*.\n"
                f"Letzter Fehler: `{(summary.last_error or '')[:200]}`\n\n"
                f"Bandcamp drosselt vermutlich. "
                f"Auto-Retry pausiert, nächster Versuch erst morgen 03:00."
            )
        else:
            # Reached end of purchase list without hitting quota.
            self.state.update_run(
                run_date, downloaded=downloaded_today, status="completed",
                stop_reason="no more items", finished_at=finished_at,
            )
            if downloaded_today == 0 and summary.skipped > 0:
                # All purchases already in ignores.txt → done.
                self.state.update(collection_complete=1)
                msg = (
                    f"🎉 *Collection vollständig*\n"
                    f"Alle {summary.skipped} Items schon lokal.\n"
                    f"Gesamt: *{final}*"
                )
            elif downloaded_today > 0:
                self.state.update(collection_complete=1)
                msg = (
                    f"🎉 *Collection vollständig*\n"
                    f"Heute zuletzt: *{downloaded_today}* Alben.\n"
                    f"Gesamt: *{final}*"
                )
            else:
                msg = (
                    f"⚠️ *Tag mit 0 Alben beendet* (`{run_date}`)\n"
                    f"Failures: *{len(summary.failures)}*\n"
                    f"Letzter Fehler: `{summary.last_error}`"
                )

        # Append failure summary if any.
        if summary.failures:
            msg += f"\n\n❗ *{len(summary.failures)}* Fehler"
            if summary.last_error:
                msg += f": `{summary.last_error[:200]}`"

        await self.telegram.send(msg)

    async def _monitor(self, run_date: str, baseline: int, quota: int, container, attempt: int) -> None:
        recent: deque[str] = deque(maxlen=self.settings.anomaly_window)
        loop = asyncio.get_event_loop()
        line_queue: asyncio.Queue[str | None] = asyncio.Queue()

        def reader_thread() -> None:
            try:
                for line in self.controller.stream_logs(container):
                    asyncio.run_coroutine_threadsafe(line_queue.put(line), loop)
            except Exception as e:
                log.warning("Log reader exited: %s", e)
            finally:
                asyncio.run_coroutine_threadsafe(line_queue.put(None), loop)

        threading.Thread(target=reader_thread, daemon=True).start()

        stop_reason: str | None = None
        anomaly_hits = 0
        last_count = baseline
        last_quota_check = 0.0

        # 24h hard cap on a single daily run so a stuck log stream can't pin us forever.
        deadline = loop.time() + 24 * 3600

        def check_quota() -> tuple[bool, int]:
            nonlocal last_count, last_quota_check
            last_quota_check = loop.time()
            count = count_completed_albums(self.config_view)
            if count != last_count:
                self._last_download_at = datetime.now(timezone.utc).isoformat()
                last_count = count
            self.state.update_run(run_date, downloaded=count - baseline)
            return (count - baseline >= quota), count

        while True:
            if loop.time() >= deadline:
                stop_reason = "deadline"
                break

            try:
                line = await asyncio.wait_for(line_queue.get(), timeout=30)
            except asyncio.TimeoutError:
                # Periodic checks even if bandcampsync is quiet.
                if not self.controller.is_running():
                    stop_reason = "exited"
                    break
                hit, _ = check_quota()
                if hit:
                    stop_reason = "quota_hit"
                    break
                continue

            if line is None:
                stop_reason = stop_reason or "exited"
                break

            self.log_buffer.append(line)
            recent.append(line)
            # Real-time mapping for the metadata enricher.
            self.enricher.observe_log(line)

            # Anomaly detection runs only on non-INFO lines so artist/album
            # names containing words like "forbidden" don't trigger false
            # positives. Real auth/rate errors land at WARNING/ERROR.
            if is_anomaly_line(line):
                anomaly_hits = sum(1 for ln in recent if is_anomaly_line(ln))
                if anomaly_hits >= self.settings.anomaly_threshold:
                    stop_reason = "emergency"
                    break

            # Debounced quota check during active log flow — we want
            # /status today_run.downloaded to track close to reality even
            # when bandcampsync is chatty (one log line per ~5–30 s during
            # a download). 5 s is well below the per-album time and well
            # above the cost of reading ignores.txt.
            if loop.time() - last_quota_check >= 5.0:
                hit, _ = check_quota()
                if hit:
                    stop_reason = "quota_hit"
                    break

        # Make sure bandcampsync is actually stopped, then capture its exit
        # code while the container still exists.
        self.controller.stop()
        exit_code = self.controller.last_exit_code()
        final = count_completed_albums(self.config_view)
        downloaded_today = final - baseline
        finished_at = datetime.now(timezone.utc).isoformat()

        # Decide branch: build the user-facing message, persist run state.
        # We do NOT send Telegram or return yet — enrichment is appended to
        # the message so the user sees the metadata-write summary in the
        # same notification.
        msg: str
        will_retry = False
        retry_delay_min: int | None = None
        if stop_reason == "emergency":
            recent_tail = "\n".join(list(recent)[-6:])
            self.state.update(
                emergency_stopped=1,
                last_emergency_at=finished_at,
                last_emergency_reason=(
                    f"{anomaly_hits} Anomalien in letzten {self.settings.anomaly_window} Zeilen"
                ),
            )
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="emergency",
                stop_reason=f"{anomaly_hits} anomalies",
                finished_at=finished_at,
            )
            msg = (
                "🚨 *NOTBREMSE*\n"
                f"`{anomaly_hits}` Auth-/Rate-Fehler in den letzten "
                f"{self.settings.anomaly_window} Log-Zeilen.\n"
                f"Container gestoppt. Heute geschafft: *{downloaded_today}*/{quota}, "
                f"Gesamt: *{final}*.\n\n"
                f"Letzte Zeilen:\n```\n{recent_tail[:800]}\n```\n\n"
                "Bitte prüfen, dann `POST /reset-emergency` am Sidecar."
            )
        elif stop_reason == "quota_hit":
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="quota_hit",
                stop_reason="daily quota reached",
                finished_at=finished_at,
            )
            msg = (
                f"✅ *Tag fertig* (`{run_date}`)\n"
                f"Heute: *{downloaded_today}*/{quota}\n"
                f"Gesamt: *{final}*"
            )
        elif stop_reason == "deadline":
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="completed",
                stop_reason="24h deadline hit",
                finished_at=finished_at,
            )
            msg = (
                "⌛ *24h-Deadline erreicht*\n"
                f"Heute: *{downloaded_today}*/{quota}, Gesamt: *{final}*.\n"
                "Run war ungewöhnlich lang. Bitte `/logs` prüfen."
            )
        elif self._user_stop_requested:
            # POST /stop. SIGTERM gives a non-zero exit code that's not a
            # crash — we suppress retry and don't touch collection_complete.
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="completed",
                stop_reason="user-initiated stop",
                last_exit_code=exit_code,
                finished_at=finished_at,
            )
            msg = (
                f"⏹ *Manuell gestoppt* (`{run_date}`)\n"
                f"Heute: *{downloaded_today}*/{quota}, Gesamt: *{final}*"
            )
        elif exit_code is not None and exit_code != 0:
            # Crash — non-zero exit, not user-initiated. Treat as transient
            # (network blip, server stall, etc.). Schedule auto-retry up to
            # retry_max_per_day; do NOT set collection_complete.
            recent_tail = "\n".join(list(recent)[-6:])
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="crashed",
                stop_reason=f"bandcampsync exit code {exit_code}",
                last_exit_code=exit_code,
                finished_at=finished_at,
            )
            attempts_done = attempt
            attempts_max = self.settings.retry_max_per_day + 1  # 1 initial + N retries
            if attempts_done < attempts_max:
                backoffs = self.settings.retry_backoffs_minutes or [10]
                # attempt=1 means first crash → use backoffs[0], attempt=2 → backoffs[1], …
                retry_delay_min = backoffs[min(attempts_done - 1, len(backoffs) - 1)]
                will_retry = True
                msg = (
                    f"⚠️ *bandcampsync gecrasht* (Exit `{exit_code}`)\n"
                    f"Heute: *{downloaded_today}*/{quota}, Gesamt: *{final}*.\n"
                    f"Auto-Retry in *{retry_delay_min} Min* "
                    f"(Versuch {attempts_done + 1}/{attempts_max}).\n\n"
                    f"Letzte Zeilen:\n```\n{recent_tail[:800]}\n```"
                )
            else:
                msg = (
                    f"❌ *bandcampsync gecrasht* (Exit `{exit_code}`)\n"
                    f"Max Retries ({attempts_max}) heute erreicht. "
                    f"Heute: *{downloaded_today}*/{quota}, Gesamt: *{final}*.\n"
                    f"Nächster Versuch morgen 03:00.\n\n"
                    f"Letzte Zeilen:\n```\n{recent_tail[:800]}\n```"
                )
        else:
            # exit_code == 0 (or unknown). Clean exit means bandcampsync
            # walked the whole collection and found nothing else outstanding.
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="completed",
                stop_reason="bandcampsync exited cleanly",
                last_exit_code=exit_code,
                finished_at=finished_at,
            )
            if downloaded_today > 0 and downloaded_today < quota:
                # Caught up before hitting quota → collection complete.
                self.state.update(collection_complete=1)
                msg = (
                    "🎉 *Collection vollständig*\n"
                    f"bandcampsync hat sich beendet — keine ausstehenden Alben mehr.\n"
                    f"Gesamt: *{final}*"
                )
            elif downloaded_today >= quota:
                msg = (
                    f"✅ *Tag fertig* (`{run_date}`)\n"
                    f"bandcampsync exakt an Quota: *{downloaded_today}*/{quota}\n"
                    f"Gesamt: *{final}*"
                )
            else:
                # Clean exit with 0 downloads — cookie problem, empty
                # collection, or already-fully-synced. Surface for review.
                msg = (
                    "ℹ️ *bandcampsync sauber beendet, 0 Alben*\n"
                    f"Cookie OK? Sammlung leer? Schon vollständig?\n"
                    f"Gesamt: *{final}*"
                )

        # Enrich whatever we managed to download this run, and append the
        # summary to the Telegram message. Failures are logged but never
        # block the user-visible notification.
        try:
            er = await self.enricher.enrich_run()
        except Exception as e:
            log.exception("Enrichment failed")
            er = {"observed": 0, "written": 0, "orphaned": 0, "api": False, "error": str(e)}

        if er.get("observed"):
            api_note = "API ✓" if er.get("api") else "API ✗ (log-derived only)"
            orphan_note = (
                f", *{er['orphaned']}* orphaned" if er.get("orphaned") else ""
            )
            msg += (
                f"\n\n📋 Metadaten: *{er['written']}*/{er['observed']} geschrieben"
                f"{orphan_note} ({api_note})"
            )

        await self.telegram.send(msg)

        # Auto-retry after a crash. We fire-and-forget an asyncio task that
        # waits, then re-enters daily_kickoff. The kickoff logic itself
        # checks _run_lock + emergency_stopped + collection_complete, so
        # the retry is safe even if state changes meanwhile.
        if will_retry and retry_delay_min is not None:
            self._schedule_retry(retry_delay_min * 60)

    def _schedule_retry(self, delay_seconds: int) -> None:
        """Schedule a single retry of daily_kickoff after delay_seconds."""
        # Cancel any previously-pending retry to avoid stacking.
        if self._retry_task and not self._retry_task.done():
            self._retry_task.cancel()

        async def _retry():
            try:
                await asyncio.sleep(delay_seconds)
            except asyncio.CancelledError:
                return
            log.info("Auto-retry firing after %ds", delay_seconds)
            await self.daily_kickoff()

        self._retry_task = asyncio.create_task(_retry())

    def status(self) -> dict:
        s = self.state.get()
        run_date = date.today().isoformat()
        day_index = self.days_in(s["started_on"])
        return {
            "version": 1,
            "now": datetime.now(timezone.utc).isoformat(),
            "bandcampsync_running": self.controller.is_running(),
            "started_on": s["started_on"],
            "day_number": day_index + 1 if s["started_on"] else None,
            "quota_today": self.quota_for_day(day_index) if s["started_on"] else None,
            "ramp_quotas": self.settings.ramp_quotas,
            "today_run": self.state.get_run(run_date),
            "last_download_at": self._last_download_at,
            "emergency_stopped": bool(s["emergency_stopped"]),
            "last_emergency_at": s["last_emergency_at"],
            "last_emergency_reason": s["last_emergency_reason"],
            "collection_complete": bool(s["collection_complete"]),
            "total_complete": count_completed_albums(self.config_view),
            "cookie": self.cookie_status(),
            "recent_runs": self.state.recent_runs(14),
        }


# ---------- Wiring ----------

state = State(Path(settings.state_path) / "state.db")
controller = BandcampsyncController(settings)
telegram = Telegram(settings.telegram_bot_token, settings.telegram_chat_id)
enricher = MetadataEnricher(
    config_view=Path(settings.config_view_path),
    downloads_view=Path(settings.downloads_view_path),
    state_path=Path(settings.state_path),
    telegram=telegram,
)
orchestrator = Orchestrator(
    state, controller, telegram, Path(settings.config_view_path), settings, enricher,
)
inbox_watcher = InboxWatcher(
    config_view=Path(settings.config_view_path),
    downloads_view=Path(settings.downloads_view_path),
    metadata_enricher=enricher,
    telegram=telegram,
)
scheduler = AsyncIOScheduler(timezone=settings.timezone)


def _stage_bandcampsync_patch() -> None:
    """Copy the in-image patched download.py to /state/patches/ so the
    bandcampsync container can bind-mount it. Idempotent — runs every
    sidecar boot, picks up patch updates automatically when sidecar is
    rebuilt."""
    src = Path("/app/patches/bandcampsync_download.py")
    if not src.exists():
        log.warning(
            "bandcampsync patch source missing at %s; patch disabled", src
        )
        return
    dst_dir = Path(settings.state_path) / "patches"
    dst = dst_dir / "bandcampsync_download.py"
    try:
        dst_dir.mkdir(parents=True, exist_ok=True)
        # shutil.copyfile does what we want — overwrite, preserve content.
        import shutil as _sh
        _sh.copyfile(src, dst)
        log.info("Staged bandcampsync patch at %s", dst)
    except Exception as e:
        log.error("Failed to stage bandcampsync patch: %s", e)


def _cleanup_orphan_bandcampsync() -> None:
    """If a bandcampsync container is still running from a previous
    sidecar incarnation, mark today's run as crashed so the new sidecar
    can decide what to do. Doesn't kill the container — just records.
    Useful after a Watchtower restart that orphaned the running download.
    """
    try:
        c = controller.client.containers.get(settings.bandcampsync_container)
        c.reload()
    except Exception:
        return
    if c.status != "running":
        return
    log.warning(
        "Found orphan bandcampsync container at startup (status=%s); "
        "stopping it to reclaim a clean slate", c.status,
    )
    try:
        controller.stop()
    except Exception as e:
        log.warning("Could not stop orphan: %s", e)


@asynccontextmanager
async def lifespan(_: FastAPI):
    _cleanup_orphan_bandcampsync()
    if settings.bandcampsync_patch_enabled:
        _stage_bandcampsync_patch()
    if settings.daily_kickoff_enabled:
        scheduler.add_job(
            orchestrator.daily_kickoff,
            CronTrigger(hour=settings.daily_run_hour, minute=0),
            id="daily_kickoff",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600,
        )
    scheduler.add_job(
        orchestrator.check_cookie_expiry,
        CronTrigger(hour=settings.cookie_check_hour, minute=0),
        id="cookie_check",
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    log.info(
        "Sidecar online. Daily kickoff: %s. Ramp quotas: %s. Cookie check: %02d:00. Inbox watcher: %s.",
        f"{settings.daily_run_hour:02d}:00 {settings.timezone}" if settings.daily_kickoff_enabled else "disabled",
        settings.ramp_quotas,
        settings.cookie_check_hour,
        "enabled" if settings.inbox_watcher_enabled else "disabled",
    )
    await telegram.send(
        "🟢 *bandcamp-warden online*\n"
        + (
            f"Daily-Kickoff: `{settings.daily_run_hour:02d}:00 {settings.timezone}`\n"
            if settings.daily_kickoff_enabled
            else "Daily-Kickoff: `disabled (Plan E mode)`\n"
        )
        + f"Inbox-Watcher: `{'enabled' if settings.inbox_watcher_enabled else 'disabled'}`\n"
        f"Ramp-Quotas: `{settings.ramp_quotas}`"
    )
    # Run a cookie check at startup so the user gets immediate feedback if the
    # cookie file is missing or already near expiry, instead of waiting until
    # tomorrow noon.
    asyncio.create_task(orchestrator.check_cookie_expiry())
    inbox_task: asyncio.Task | None = None
    if settings.inbox_watcher_enabled:
        inbox_task = asyncio.create_task(inbox_watcher.run_loop())
    try:
        yield
    finally:
        if inbox_task is not None:
            inbox_watcher.stop()
            try:
                await asyncio.wait_for(inbox_task, timeout=10)
            except asyncio.TimeoutError:
                inbox_task.cancel()
        scheduler.shutdown(wait=False)


app = FastAPI(title="bandcamp-warden", lifespan=lifespan)

# CORS for the Plan-E browser extension. Firefox enforces CORS on
# extension background-script fetches when the destination origin was
# granted via runtime-requested optional_permissions (as opposed to
# manifest's mandatory permissions). The sidecar is auth-protected via
# the X-Warden-Auth header — credentials/cookies aren't part of the
# auth model — so allow_origins=["*"] is safe here. Don't enable
# allow_credentials: that would couple CORS to cookies and require a
# concrete origin allowlist instead of "*".
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)


@app.get("/healthz")
async def healthz() -> dict:
    """Liveness + build provenance. Build args injected by GHA at image build."""
    return {
        "ok": True,
        "version": os.getenv("WARDEN_BUILD_DATE", "unknown"),
        "channel": os.getenv("WARDEN_GIT_BRANCH", "unknown"),
        "commit": os.getenv("WARDEN_GIT_COMMIT", "unknown")[:7],
    }


@app.get("/status")
async def status_endpoint() -> dict:
    return orchestrator.status()


@app.get("/logs")
async def logs(lines: int = 200) -> dict:
    """bandcampsync's container logs (per-album download progress)."""
    n = max(1, min(lines, settings.log_buffer_size))
    return {"lines": list(orchestrator.log_buffer)[-n:]}


@app.get("/sidecar-logs")
async def sidecar_logs(lines: int = 200) -> dict:
    """Sidecar's own logs (warden, uvicorn, apscheduler) from a ring
    buffer. Lets us debug without container shell access."""
    n = max(1, min(lines, 2000))
    return {"lines": list(_sidecar_log_buffer.buffer)[-n:]}


@app.post("/trigger")
async def trigger_now(quota: int | None = None) -> dict:
    """Manually fire the daily kickoff. Useful for first-deploy smoke test.

    Optional `quota` parameter overrides today's ramp-up quota for this
    one run. Use small values (e.g. quota=1) to validate the entire
    pipeline end-to-end without burning a 200-album daily budget."""
    if orchestrator._run_lock.locked():
        raise HTTPException(409, "A run is already in progress")
    asyncio.create_task(orchestrator.daily_kickoff(override_quota=quota))
    return {"triggered": True, "quota_override": quota}


@app.post("/stop")
async def stop_now(request: Request) -> dict:
    """Force-stop bandcampsync. Suppresses any auto-retry that would
    otherwise fire (since SIGTERM gives a non-zero exit code)."""
    client = getattr(request.client, "host", "unknown")
    log.warning(
        "POST /stop from %s — bandcampsync will be terminated", client
    )
    orchestrator._user_stop_requested = True
    if orchestrator._retry_task and not orchestrator._retry_task.done():
        orchestrator._retry_task.cancel()
    controller.stop()
    return {"stopped": True, "caller_ip": client}


@app.post("/check-cookie")
async def check_cookie_now() -> dict:
    """Force a cookie expiry check and Telegram-warn if applicable."""
    # Reset today's warning flag so the check can actually push, even if
    # something already warned today.
    state.update(last_cookie_warning_on=None)
    await orchestrator.check_cookie_expiry()
    return orchestrator.cookie_status()


@app.post("/reset-emergency")
async def reset_emergency() -> dict:
    state.update(
        emergency_stopped=0,
        last_emergency_at=None,
        last_emergency_reason=None,
    )
    return {"reset": True}


@app.post("/reset-completion")
async def reset_completion() -> dict:
    """Clear the collection_complete flag so the daily scheduler resumes.
    Useful if a transient failure (network, crash) was misclassified as
    'caught up'."""
    state.update(collection_complete=0)
    return {"reset": True}


@app.post("/cleanup-stale-ignores")
async def cleanup_stale_ignores(dry_run: bool = True) -> dict:
    """Resync ignores.txt with what's actually downloaded.

    Walks every '<id>  # band / title' line in the bandcampsync
    /config/ignores.txt and the warden /state/ignores_warden.txt.
    Looks up the corresponding album folder under /downloads using
    the same normalize() rule as the metadata enricher. If the folder
    doesn't exist or contains zero audio files (FLAC/MP3/etc.), the
    line is stale: bandcampsync wrote it but the album is incomplete.

    dry_run=true: report only. dry_run=false: rewrite the files,
    removing stale entries so the next daily run picks them up again.
    """
    return await asyncio.to_thread(_cleanup_stale_ignores_sync, dry_run)


def _cleanup_stale_ignores_sync(dry_run: bool) -> dict:
    import unicodedata as _ud

    _disallowed = '"#%\'*/?\\`:'

    def _norm(s: str) -> str:
        s = _ud.normalize("NFKD", s or "")
        s = "".join(c for c in s if c not in _disallowed)
        s = "".join(
            c for c in s
            if _ud.category(c) not in ("Cf", "Mn", "Cc")
        )
        s = re.sub(r"\s+", " ", s).strip().rstrip(". ").lower()
        return s

    audio_exts = {".flac", ".mp3", ".wav", ".aiff", ".alac", ".ogg"}
    downloads = Path(settings.downloads_view_path)

    # Index actual album dirs by (norm_band, norm_title) → has_audio?
    folder_status: dict[tuple[str, str], bool] = {}
    if downloads.exists():
        for artist_dir in downloads.iterdir():
            if not artist_dir.is_dir():
                continue
            for album_dir in artist_dir.iterdir():
                if not album_dir.is_dir():
                    continue
                has_audio = any(
                    p.suffix.lower() in audio_exts
                    for p in album_dir.rglob("*") if p.is_file()
                )
                folder_status[(_norm(artist_dir.name), _norm(album_dir.name))] = has_audio

    # Process each ignores file independently.
    candidate_paths = [
        Path(settings.config_view_path) / "ignores.txt",
        Path(settings.state_path) / "ignores_warden.txt",
    ]
    per_file: list[dict] = []
    total_stale = 0
    line_re = re.compile(r"^\s*(\d+)\s*#\s*(.+?)\s*/\s*(.+?)\s*$")

    for path in candidate_paths:
        if not path.exists():
            per_file.append({"path": str(path), "exists": False})
            continue
        original_lines = path.read_text(errors="replace").splitlines(keepends=True)
        kept: list[str] = []
        stale: list[dict] = []
        for line in original_lines:
            m = line_re.match(line)
            if not m:
                kept.append(line)
                continue
            iid = int(m.group(1))
            band = m.group(2)
            title = m.group(3)
            key = (_norm(band), _norm(title))
            ok = folder_status.get(key, False)
            if ok:
                kept.append(line)
            else:
                stale.append({
                    "item_id": iid, "band": band, "item_title": title,
                    "folder_known": key in folder_status,
                })
        per_file.append({
            "path": str(path),
            "exists": True,
            "total_lines": len(original_lines),
            "stale_count": len(stale),
            "stale_examples": stale[:10],
        })
        total_stale += len(stale)
        if not dry_run and stale:
            try:
                # Atomic rewrite: write to .tmp then rename.
                tmp = path.with_suffix(path.suffix + ".tmp")
                tmp.write_text("".join(kept), encoding="utf-8")
                os.replace(tmp, path)
                per_file[-1]["rewrote"] = True
            except Exception as e:
                per_file[-1]["rewrite_error"] = f"{type(e).__name__}: {e}"

    return {
        "dry_run": dry_run,
        "total_stale": total_stale,
        "files": per_file,
    }


@app.get("/sample-metadata")
async def sample_metadata(count: int = 5) -> dict:
    """Read up to N bandcamp_<id>.json files from /downloads and return
    their parsed content. Lets us verify backfill quality without shell
    access — what fields are present, what fields are null."""
    samples: list[dict] = []
    field_coverage: dict[str, int] = {}
    n = max(1, min(count, 50))
    base = Path(settings.downloads_view_path)
    if base.exists():
        for path in base.rglob("bandcamp_*.json"):
            try:
                content = json.loads(path.read_text(encoding="utf-8"))
                if len(samples) < n:
                    samples.append({
                        "path": str(path),
                        "content": content,
                    })
                for k in content.keys():
                    field_coverage[k] = field_coverage.get(k, 0) + 1
            except Exception as e:
                if len(samples) < n:
                    samples.append({"path": str(path), "error": str(e)})
    return {
        "files_total": sum(field_coverage.values()) // (len(field_coverage) or 1),
        "field_coverage": dict(sorted(field_coverage.items(), key=lambda kv: -kv[1])),
        "samples": samples,
    }


class _ProbeURLBody(BaseModel):
    url: str
    sample_bytes: int = 5_000_000


@app.post("/probe-url")
async def probe_url(body: _ProbeURLBody) -> dict:
    """Hit a SPECIFIC URL the user pasted (e.g. the popplers5.bandcamp.com
    URL their browser just used at 39 MB/s) and measure throughput from
    the sidecar. Lets us isolate: is the slowdown about request shape
    (then this URL crawls) or about URL freshness/auth (then this URL
    flies)?

    The user pastes the EXACT URL from their browser's address bar or
    network tab. We send it with full Firefox-mimicry headers and the
    cookies.txt cookie jar."""
    if controller.is_running():
        return {"error": "bandcampsync container running"}
    return await asyncio.to_thread(_probe_url_sync, body.url, body.sample_bytes)


def _probe_url_sync(url: str, sample_bytes: int) -> dict:
    import time as _time
    cookies_path = Path(settings.config_view_path) / "cookies.txt"
    cookies = {}
    if cookies_path.exists():
        for line in cookies_path.read_text(errors="replace").splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 7 and "bandcamp.com" in parts[0]:
                cookies[parts[5]] = parts[6]

    headers = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/png,image/svg+xml,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "DNT": "1",
        "Sec-GPC": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Priority": "u=0, i",
        "Referer": "https://bandcamp.com/",
        "Connection": "keep-alive",
    }

    start = _time.time()
    bytes_dl = 0
    first_chunk_after = None
    error = None
    http_status = None
    try:
        with curl_requests.Session(impersonate="firefox133") as s:
            r = s.get(
                url, headers=headers, cookies=cookies,
                stream=True, timeout=120,
            )
            http_status = r.status_code
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    if first_chunk_after is None:
                        first_chunk_after = round(_time.time() - start, 2)
                    bytes_dl += len(chunk)
                    if bytes_dl >= sample_bytes:
                        break
                if _time.time() - start > 120:
                    break
            r.close()
    except Exception as e:
        error = f"{type(e).__name__}: {e}"

    duration = max(0.001, _time.time() - start)
    return {
        "http_status": http_status,
        "duration_seconds": round(duration, 2),
        "first_chunk_after_seconds": first_chunk_after,
        "bytes": bytes_dl,
        "MB_per_s": round((bytes_dl / 1_000_000) / duration, 2),
        "error": error,
        "cookie_count": len(cookies),
    }


@app.post("/test-range-support")
async def test_range_support(item_id: int | None = None) -> dict:
    """Probe whether Bandcamp's signed download URL accepts HTTP Range
    requests. If it returns 206 Partial Content with the right
    Content-Range header, our resume strategy works. If it returns 200
    (ignoring the Range header), Range-resume is dead in the water and
    we need a different concept (full sidecar-side downloader)."""
    if controller.is_running():
        return {"error": "bandcampsync running, would compete for bandwidth"}
    return await asyncio.to_thread(_test_range_sync, item_id)


def _test_range_sync(item_id: int | None) -> dict:
    from bandcampsync.bandcamp import Bandcamp  # type: ignore

    cookies_path = Path(settings.config_view_path) / "cookies.txt"
    if not cookies_path.exists():
        return {"error": "cookies.txt missing"}
    try:
        bc = Bandcamp(cookies_path.read_text(errors="replace"))
        bc.verify_authentication()
        bc.load_purchases()
    except Exception as e:
        return {"error": f"bandcampsync init: {type(e).__name__}: {e}"}

    target = None
    if item_id is not None:
        target = next((p for p in bc.purchases if p.item_id == item_id), None)
    elif bc.purchases:
        target = bc.purchases[0]
    if target is None:
        return {"error": "no purchase to test"}

    try:
        url = bc.get_download_file_url(target, encoding="flac")
    except Exception as e:
        return {"error": f"URL resolution: {type(e).__name__}: {e}"}

    out: dict = {"item_id": target.item_id, "band_name": target.band_name}

    # Step 1: HEAD-like — small initial GET to learn content-length and
    # whether server advertises range support.
    try:
        with curl_requests.Session(impersonate="chrome") as s:
            r = s.get(url, headers={"Range": "bytes=0-1023"}, timeout=30)
            out["initial_status"] = r.status_code
            cr = r.headers.get("Content-Range") or r.headers.get("content-range")
            out["initial_content_range"] = cr
            out["initial_accept_ranges"] = (
                r.headers.get("Accept-Ranges") or r.headers.get("accept-ranges")
            )
            out["initial_content_length"] = r.headers.get("Content-Length")
            out["initial_bytes_first_16"] = (r.content or b"")[:16].hex()
            r.close()
    except Exception as e:
        out["initial_error"] = f"{type(e).__name__}: {e}"
        return out

    # Step 2: ranged GET from somewhere mid-file. If the server gives us
    # 206 + correct Content-Range, Range works. If 200, ignored.
    try:
        with curl_requests.Session(impersonate="chrome") as s:
            r = s.get(
                url, headers={"Range": "bytes=1048576-1049599"}, timeout=30,
            )
            out["range_status"] = r.status_code
            out["range_content_range"] = (
                r.headers.get("Content-Range") or r.headers.get("content-range")
            )
            out["range_content_length"] = r.headers.get("Content-Length")
            r.close()
    except Exception as e:
        out["range_error"] = f"{type(e).__name__}: {e}"

    # Verdict
    if out.get("range_status") == 206:
        out["verdict"] = "RANGE_SUPPORTED"
    elif out.get("range_status") == 200:
        out["verdict"] = "RANGE_IGNORED"
    else:
        out["verdict"] = "INCONCLUSIVE"
    return out


@app.post("/test-browser-download")
async def test_browser_download(
    item_id: int | None = None,
    browser: str = "firefox",
) -> dict:
    """Plan D smoke test: download ONE album using a real Playwright
    browser (chromium or firefox). The user proved their browser
    downloads fast while our HTTP scripts get throttled to 0 — this
    validates whether automating a real browser also gets fast
    download speeds, or whether Bandcamp also detects Playwright."""
    if controller.is_running():
        return {"error": "bandcampsync container is running"}

    from bandcampsync.bandcamp import Bandcamp  # type: ignore
    from browser_downloader import BrowserDownloader

    cookies_path = Path(settings.config_view_path) / "cookies.txt"
    if not cookies_path.exists():
        return {"error": "cookies.txt missing"}
    try:
        bc = Bandcamp(cookies_path.read_text(errors="replace"))
        bc.verify_authentication()
        bc.load_purchases()
    except Exception as e:
        return {"error": f"bandcampsync init: {type(e).__name__}: {e}"}

    target = None
    if item_id is not None:
        target = next((p for p in bc.purchases if p.item_id == item_id), None)
    else:
        # First not-yet-downloaded item.
        completed = set()
        ig = Path(settings.config_view_path) / "ignores.txt"
        if ig.exists():
            for line in ig.read_text(errors="replace").splitlines():
                stripped = line.split("#", 1)[0].strip()
                if stripped.isdigit():
                    completed.add(int(stripped))
        for p in bc.purchases:
            if p.item_id not in completed:
                target = p
                break
    if target is None:
        return {"error": "no eligible target"}

    log_events: list[str] = []

    def _log(s: str) -> None:
        log_events.append(s)
        log.info("test-browser-download: %s", s)

    bd = BrowserDownloader(
        downloads_root=Path(settings.downloads_view_path),
        config_dir=Path(settings.config_view_path),
        state_dir=Path(settings.state_path),
        format_name="flac",
    )
    outcome = await bd.download_one(target, browser_name=browser, log_event=_log)
    return {
        "success": outcome.success,
        "item_id": outcome.item_id,
        "band_name": outcome.band_name,
        "item_title": outcome.item_title,
        "bytes_written": outcome.bytes_written,
        "duration_seconds": round(outcome.duration_seconds, 2),
        "MB_per_s": (
            round((outcome.bytes_written / 1_000_000) / outcome.duration_seconds, 2)
            if outcome.duration_seconds > 0 else None
        ),
        "folder": str(outcome.folder) if outcome.folder else None,
        "browser_used": outcome.browser_used,
        "error": outcome.error,
        "log_events": log_events,
    }


@app.post("/test-browser-headers")
async def test_browser_headers(
    item_id: int | None = None, sample_bytes: int = 5_000_000,
) -> dict:
    """Diagnose: download a slice of an album using FULL browser-faithful
    headers (Referer, Sec-Fetch-*, Accept, Accept-Language, etc.) — the
    set a real Chrome navigation would emit. Compare throughput with the
    plain-headers variant to determine whether Bandcamp's CDN
    throttles non-browser-shaped requests.

    sample_bytes: how many bytes to grab (default 5MB) to avoid pulling
    a whole album per call. Result reports MB/s; <0.5 MB/s likely means
    we're being throttled regardless of headers.
    """
    if controller.is_running():
        return {"error": "bandcampsync container is running"}
    return await asyncio.to_thread(
        _test_browser_headers_sync, item_id, sample_bytes,
    )


def _test_browser_headers_sync(item_id: int | None, sample_bytes: int) -> dict:
    from bandcampsync.bandcamp import Bandcamp  # type: ignore

    cookies_path = Path(settings.config_view_path) / "cookies.txt"
    if not cookies_path.exists():
        return {"error": "cookies.txt missing"}
    try:
        bc = Bandcamp(cookies_path.read_text(errors="replace"))
        bc.verify_authentication()
        bc.load_purchases()
    except Exception as e:
        return {"error": f"bandcampsync init: {type(e).__name__}: {e}"}

    target = None
    if item_id is not None:
        target = next((p for p in bc.purchases if p.item_id == item_id), None)
    elif bc.purchases:
        target = bc.purchases[0]
    if target is None:
        return {"error": "no purchase to test"}

    try:
        signed_url = bc.get_download_file_url(target, encoding="flac")
    except Exception as e:
        return {"error": f"URL: {type(e).__name__}: {e}"}
    if not signed_url:
        return {"error": "URL resolution returned empty"}

    # The page that would normally have linked to this download.
    referer = getattr(target, "download_url", "") or "https://bandcamp.com/"

    cookies = {}
    for line in cookies_path.read_text(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) >= 7 and "bandcamp.com" in parts[0]:
            cookies[parts[5]] = parts[6]

    # ----- variant A: minimal headers (what bandcampsync does) -----
    a_result = _measure_throughput(
        signed_url, headers={}, cookies=cookies, sample_bytes=sample_bytes,
        impersonate="chrome",
    )
    a_result["variant"] = "minimal_chrome_impersonate"

    # ----- variant B: full browser headers + referer -----
    full_headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not.A/Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"macOS"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-site",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Referer": referer,
        "Connection": "keep-alive",
    }
    b_result = _measure_throughput(
        signed_url, headers=full_headers, cookies=cookies,
        sample_bytes=sample_bytes, impersonate="chrome",
    )
    b_result["variant"] = "full_browser_headers_with_referer"

    return {
        "item_id": target.item_id,
        "band_name": target.band_name,
        "item_title": target.item_title,
        "referer_used": referer,
        "sample_bytes_target": sample_bytes,
        "results": [a_result, b_result],
    }


def _measure_throughput(
    url: str, headers: dict, cookies: dict, sample_bytes: int,
    impersonate: str = "chrome",
) -> dict:
    import time as _time
    start = _time.time()
    bytes_dl = 0
    http_status = None
    error = None
    first_chunk_after = None
    try:
        with curl_requests.Session(impersonate=impersonate) as s:
            r = s.get(
                url, headers=headers, cookies=cookies,
                stream=True, timeout=120,
            )
            http_status = r.status_code
            if r.status_code != 200:
                return {
                    "status": "fail",
                    "http_status": r.status_code,
                    "duration_seconds": round(_time.time() - start, 2),
                    "bytes": 0,
                    "error": f"HTTP {r.status_code}",
                }
            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    if first_chunk_after is None:
                        first_chunk_after = round(_time.time() - start, 2)
                    bytes_dl += len(chunk)
                    if bytes_dl >= sample_bytes:
                        break
                if _time.time() - start > 120:
                    break
            r.close()
    except Exception as e:
        error = f"{type(e).__name__}: {e}"

    duration = max(0.001, _time.time() - start)
    return {
        "status": "ok" if not error and bytes_dl > 0 else "fail",
        "http_status": http_status,
        "duration_seconds": round(duration, 2),
        "first_chunk_after_seconds": first_chunk_after,
        "bytes": bytes_dl,
        "mbps": round((bytes_dl * 8 / 1_000_000) / duration, 2),
        "MB_per_s": round((bytes_dl / 1_000_000) / duration, 2),
        "error": error,
    }


@app.post("/test-sidecar-download")
async def test_sidecar_download(
    item_id: int | None = None, count: int = 1,
) -> dict:
    """Plan C smoke test: download N albums using the in-sidecar
    WardenDownloader (httpx + Range resume), bypassing the bandcampsync
    container entirely. Used to validate the new approach works before
    flipping it on for the daily run."""
    if controller.is_running():
        return {"error": "bandcampsync container is running; stop it first"}

    from downloader import WardenDownloader  # local module

    dl = WardenDownloader(
        downloads_root=Path(settings.downloads_view_path),
        config_dir=Path(settings.config_view_path),
        state_dir=Path(settings.state_path),
        format_name="flac",
    )

    # Reuse bandcampsync to load and pick one purchase
    from bandcampsync.bandcamp import Bandcamp  # type: ignore

    cookies_path = Path(settings.config_view_path) / "cookies.txt"
    if not cookies_path.exists():
        return {"error": "cookies.txt missing"}
    try:
        bc = Bandcamp(cookies_path.read_text(errors="replace"))
        bc.verify_authentication()
        bc.load_purchases()
    except Exception as e:
        return {"error": f"bandcampsync init: {type(e).__name__}: {e}"}

    if not bc.purchases:
        return {"error": "no purchases"}

    completed = dl._read_ignores()
    targets = []
    if item_id is not None:
        item = next((p for p in bc.purchases if p.item_id == item_id), None)
        if item is not None:
            targets = [item]
    else:
        # Pick the first not-yet-downloaded item
        for p in bc.purchases:
            if p.item_id not in completed:
                targets.append(p)
                if len(targets) >= max(1, min(count, 5)):
                    break

    if not targets:
        return {"error": "no eligible target"}

    results: list[dict] = []
    log_events: list[str] = []

    def _log(s: str) -> None:
        log_events.append(s)
        log.info("test-sidecar-download: %s", s)

    for item in targets:
        try:
            url = bc.get_download_file_url(item, encoding="flac")
            outcome = await dl._fetch_extract_and_record(item, url, _log)
            results.append({
                "item_id": outcome.item_id,
                "band_name": outcome.band_name,
                "item_title": outcome.item_title,
                "success": outcome.success,
                "bytes_written": outcome.bytes_written,
                "resumes": outcome.resumes,
                "folder": str(outcome.folder) if outcome.folder else None,
                "error": outcome.error,
            })
            if outcome.success:
                dl._append_ignore(outcome.item_id, outcome.band_name, outcome.item_title)
        except Exception as e:
            results.append({
                "item_id": item.item_id,
                "error": f"{type(e).__name__}: {e}",
            })
    return {"results": results, "log_events": log_events}


@app.post("/test-download")
async def test_download(
    max_seconds: int = 1800,
    max_bytes: int = 0,
    item_id: int | None = None,
) -> dict:
    """Probe a Bandcamp download URL with multiple curl_cffi timeout
    configs. Tests the hypothesis that curl-error-28 (server-stall
    detection) is what's killing daily runs — and that a more patient
    config fixes it without making Bandcamp throttle us harder.

    Three variants run sequentially against the same album:
      * default_chrome_impersonate — what bandcampsync ships with
      * patient_5min_1kb — abort only if <1KB/s sustained for 5 min
      * no_stall_check — never abort on slow transfer

    max_seconds: wall-clock cap per variant (default 1800 = 30 min)
    max_bytes:   stop each variant after N bytes (0 = full file)
    item_id:     specific item to test, otherwise picks first purchase
    """
    if controller.is_running():
        return {
            "error": "bandcampsync is currently running; the test would "
                     "compete with it for bandwidth and produce confusing "
                     "results. Wait until status.bandcampsync_running is false."
        }
    return await asyncio.to_thread(
        _test_download_sync, item_id, max_seconds, max_bytes,
    )


def _test_download_sync(
    item_id: int | None, max_seconds: int, max_bytes: int,
) -> dict:
    from bandcampsync.bandcamp import Bandcamp  # type: ignore

    cookies_path = Path(settings.config_view_path) / "cookies.txt"
    if not cookies_path.exists():
        return {"error": "cookies.txt missing"}

    try:
        bc = Bandcamp(cookies_path.read_text(errors="replace"))
        bc.verify_authentication()
        bc.load_purchases()
    except Exception as e:
        return {"error": f"bandcampsync init: {type(e).__name__}: {e}"}

    target = None
    if item_id is not None:
        for p in bc.purchases:
            if p.item_id == item_id:
                target = p
                break
        if target is None:
            return {"error": f"item_id {item_id} not in purchases"}
    elif bc.purchases:
        target = bc.purchases[0]
    if target is None:
        return {"error": "no purchases available"}

    variants = [
        ("default_chrome_impersonate", {}),
        ("patient_5min_1kb", {"LOW_SPEED_TIME": 300, "LOW_SPEED_LIMIT": 1024}),
        ("no_stall_check", {"LOW_SPEED_TIME": 0, "LOW_SPEED_LIMIT": 0}),
    ]

    results: list[dict] = []
    for variant_name, options in variants:
        try:
            url = bc.get_download_file_url(target, encoding="flac")
        except Exception as e:
            results.append({
                "variant": variant_name,
                "error": f"URL resolution: {type(e).__name__}: {e}",
            })
            continue
        if not url:
            results.append({
                "variant": variant_name,
                "error": "URL resolution returned empty",
            })
            continue
        result = _probe_download(url, options, max_seconds, max_bytes)
        result["variant"] = variant_name
        results.append(result)

    return {
        "item_id": target.item_id,
        "band_name": target.band_name,
        "item_title": target.item_title,
        "test_max_seconds": max_seconds,
        "test_max_bytes": max_bytes,
        "results": results,
    }


def _probe_download(
    url: str, named_options: dict, max_seconds: int, max_bytes: int,
) -> dict:
    """Stream bytes from `url`, discard them, report what happened."""
    import time as _time

    try:
        from curl_cffi import CurlOpt as _CurlOpt  # type: ignore
    except ImportError:
        try:
            from curl_cffi.const import CurlOpt as _CurlOpt  # type: ignore
        except ImportError:
            _CurlOpt = None  # type: ignore

    curl_options: dict = {}
    if _CurlOpt is not None:
        for name, val in named_options.items():
            opt = getattr(_CurlOpt, name, None)
            if opt is not None:
                curl_options[opt] = val
    options_resolved = bool(curl_options) == bool(named_options)

    start = _time.time()
    bytes_dl = 0
    content_length = 0
    completed = False
    early_stop: str | None = None
    error: str | None = None
    http_status: int | None = None
    last_chunk_at = start
    max_gap = 0.0

    def make_session():
        # curl_cffi's API surface for setting raw libcurl options has
        # moved between versions: some accept curl_options on Session(),
        # some on per-request, some only via subclass. Try the easiest
        # paths first; fall back to subclassing _set_curl_options.
        try:
            return curl_requests.Session(
                impersonate="chrome",
                curl_options=curl_options or None,
            )
        except TypeError:
            pass

        class _Patient(curl_requests.Session):  # type: ignore
            def _set_curl_options(self, curl, *a, **kw):  # type: ignore
                parent = getattr(super(), "_set_curl_options", None)
                if parent:
                    parent(curl, *a, **kw)
                for k, v in (curl_options or {}).items():
                    try:
                        curl.setopt(k, v)
                    except Exception as exc:
                        log.warning("setopt(%s) failed: %s", k, exc)

        return _Patient(impersonate="chrome")

    try:
        with make_session() as session:
            r = session.get(url, stream=True, timeout=max_seconds + 60)
            http_status = r.status_code
            if r.status_code != 200:
                return {
                    "status": "fail",
                    "http_status": r.status_code,
                    "duration_seconds": round(_time.time() - start, 1),
                    "error": f"HTTP {r.status_code}",
                    "options_resolved": options_resolved,
                }
            try:
                content_length = int(r.headers.get("content-length", "0") or "0")
            except Exception:
                content_length = 0

            for chunk in r.iter_content(chunk_size=64 * 1024):
                if chunk:
                    now = _time.time()
                    gap = now - last_chunk_at
                    if gap > max_gap:
                        max_gap = gap
                    last_chunk_at = now
                    bytes_dl += len(chunk)
                    if max_bytes and bytes_dl >= max_bytes:
                        early_stop = "max_bytes"
                        break
                    if now - start > max_seconds:
                        early_stop = "max_seconds"
                        break
            else:
                completed = True
    except Exception as e:
        error = f"{type(e).__name__}: {e}"

    duration = max(0.001, _time.time() - start)
    return {
        "status": "ok" if not error and (completed or early_stop) else "fail",
        "completed": completed,
        "early_stop": early_stop,
        "http_status": http_status,
        "content_length": content_length,
        "bytes_downloaded": bytes_dl,
        "percent_complete": (
            round(bytes_dl / content_length * 100, 1) if content_length else None
        ),
        "duration_seconds": round(duration, 1),
        "avg_mbps": round((bytes_dl * 8 / 1_000_000) / duration, 2),
        "max_gap_between_chunks_s": round(max_gap, 1),
        "options_applied": list(named_options.keys()) if options_resolved else [],
        "options_resolved": options_resolved,
        "error": error,
    }


@app.post("/backfill-metadata")
async def backfill_metadata(force: bool = False) -> dict:
    """Walk the existing /downloads tree and write bandcamp_<id>.json for
    every album folder that doesn't already have one. Pulls fresh data
    from the Bandcamp Fan API. Use force=true to overwrite existing
    metadata files (e.g. after upgrading what fields we capture).

    Run this manually after the Fan API was unreachable during a daily
    run, or after deploying a sidecar version with new fields."""
    return await orchestrator.enricher.backfill(only_missing=not force)


def _safe_cookie_names(cookies_obj) -> list[str]:
    """curl_cffi's cookie iterators can yield strings or Cookie objects
    depending on the version; both shapes appear in the wild. Be defensive."""
    out: list[str] = []
    try:
        for c in cookies_obj or []:
            if isinstance(c, str):
                out.append(c)
            else:
                name = getattr(c, "name", None)
                if name:
                    out.append(name)
                else:
                    out.append(str(c))
    except Exception:
        pass
    return sorted(set(out))


@app.post("/diagnose-fan-api")
async def diagnose_fan_api() -> dict:
    """Run a single Fan-API call with full diagnostics in the response.
    No need to read container logs: you get pre-flight status, response
    headers, body snippet, and parsed result all in one JSON. Use this
    to debug why the Fan API returns 0 items.

    Each phase has its own try/except so a crash in one phase doesn't
    erase the diagnostics from previous phases."""
    enricher = orchestrator.enricher
    cookie_jar, fan_id = enricher._parse_identity_cookie()
    out: dict = {
        "fan_id": fan_id,
        "cookie_count": len(cookie_jar),
        "cookie_names": sorted(cookie_jar.keys()),
    }
    if not fan_id or not cookie_jar:
        out["error"] = "no fan_id or no cookies"
        return out

    def run() -> dict:
        result: dict = {"preflight": {}, "api": {}}
        try:
            session = curl_requests.Session(impersonate="chrome")
        except Exception as e:
            result["raised_session"] = f"{type(e).__name__}: {e}"
            return result

        # ----- pre-flight -----
        try:
            pf = session.get(
                "https://bandcamp.com/", cookies=cookie_jar, timeout=30
            )
            result["preflight"]["status"] = pf.status_code
            result["preflight"]["body_bytes"] = len(pf.content or b"")
            result["preflight"]["set_cookie_names"] = _safe_cookie_names(
                getattr(pf, "cookies", None)
            )
            text = getattr(pf, "text", "") or ""
            result["preflight"]["html_has_pagedata"] = 'id="pagedata"' in text
            result["preflight"]["html_has_homepage_app"] = 'id="HomepageApp"' in text
            result["preflight"]["html_has_fan_id"] = (str(fan_id) in text)
            # Try to extract fan_id from the homepage's pagedata blob.
            m = re.search(
                r'"identity"\s*:\s*\{[^{}]*"id"\s*:\s*(\d+)', text
            )
            result["preflight"]["pagedata_fan_id"] = int(m.group(1)) if m else None
            # If our fan_id appears anywhere in the HTML, snapshot the
            # surrounding 300 chars so we can see what shape the identity
            # blob has (the regex may need updating).
            idx = text.find(str(fan_id))
            if idx > 0:
                start = max(0, idx - 100)
                result["preflight"]["fan_id_html_context"] = text[start:idx + 200]
            # Also: snapshot the first 500 chars where "pagedata" appears
            # so we can see Bandcamp's current data shape.
            pd_idx = text.find("id=\"pagedata\"")
            if pd_idx > 0:
                result["preflight"]["pagedata_snippet"] = text[pd_idx:pd_idx + 500]
            # Headers tell us if curl_cffi sent our identity cookie.
            try:
                req = pf.request
                hdrs = dict(getattr(req, "headers", None) or {})
                cookie_hdr = hdrs.get("Cookie") or hdrs.get("cookie")
                if cookie_hdr:
                    # Mask the actual identity value, keep the prefix to confirm shape.
                    masked = re.sub(
                        r"identity=([^;]*)",
                        lambda m: f"identity={m.group(1)[:25]}…(len={len(m.group(1))})",
                        cookie_hdr,
                    )
                    result["preflight"]["request_cookie_header"] = masked
            except Exception:
                pass
            # And the response Set-Cookie headers — if Bandcamp replaces
            # our identity with a fresh one, that tells us auth was rejected.
            try:
                set_cookie_headers = pf.headers.get_list("Set-Cookie") if hasattr(pf.headers, "get_list") else []
            except Exception:
                set_cookie_headers = []
            result["preflight"]["response_sets_identity"] = any(
                "identity=" in (h or "") for h in set_cookie_headers
            )
        except Exception as e:
            result["preflight"]["raised"] = f"{type(e).__name__}: {e}"

        # ----- session cookies after pre-flight -----
        try:
            result["session_cookies_after_preflight"] = _safe_cookie_names(
                session.cookies
            )
        except Exception as e:
            result["session_cookies_raised"] = f"{type(e).__name__}: {e}"

        # ----- API call: try multiple variants to find what works -----
        now_ts = int(datetime.now(timezone.utc).timestamp())
        token = f"{now_ts}:0:a::"
        variants = [
            ("int_fan_id", {"fan_id": fan_id, "older_than_token": token, "count": 100}),
            ("str_fan_id", {"fan_id": str(fan_id), "older_than_token": token, "count": 100}),
            ("with_xhr_header", {"fan_id": fan_id, "older_than_token": token, "count": 100}),
        ]
        for variant_name, body in variants:
            try:
                headers = {
                    "Origin": "https://bandcamp.com",
                    "Referer": "https://bandcamp.com/",
                }
                if variant_name == "with_xhr_header":
                    headers["X-Requested-With"] = "XMLHttpRequest"
                ar = session.post(
                    BANDCAMP_API_URL,
                    json=body,
                    cookies=cookie_jar,
                    headers=headers,
                    timeout=30,
                )
                v_result: dict = {
                    "status": ar.status_code,
                    "body_first_300": (getattr(ar, "text", "") or "")[:300],
                }
                try:
                    data = ar.json()
                    v_result["items_count"] = len(data.get("items") or [])
                    v_result["more_available"] = data.get("more_available")
                    # Check if any of the secondary maps have content even
                    # when items is empty — that would tell us auth is OK
                    # but query params are off.
                    for k in ("redownload_urls", "item_lookup", "purchase_infos", "tracklists", "collectors"):
                        v = data.get(k)
                        v_result[f"{k}_count"] = len(v) if isinstance(v, dict) else (
                            len(v) if isinstance(v, list) else None
                        )
                except Exception as e:
                    v_result["json_error"] = f"{type(e).__name__}: {e}"
                result["api"][variant_name] = v_result
            except Exception as e:
                result["api"][variant_name] = {"raised": f"{type(e).__name__}: {e}"}

        try:
            session.close()
        except Exception:
            pass
        return result

    out.update(await asyncio.to_thread(run))
    return out


# ---------- Inbox endpoints (Phase 8b/8c) ----------


@app.get("/inbox-status")
async def inbox_status() -> dict:
    """Snapshot of the inbox watcher state — pending ZIPs, partial uploads,
    quarantine, processing counters, last error. LAN-only, no auth."""
    return inbox_watcher.status()


@app.post("/inbox/upload")
async def inbox_upload(
    item_id: int,
    request: Request,
    x_warden_auth: str = Header(default=""),
) -> dict:
    """Accept a streamed ZIP from the Plan-E browser extension and write
    it directly to <downloads>/_inbox/bandcamp_<item_id>.zip — no SMB
    round trip from the user's Mac. Auth via shared secret in the
    X-Warden-Auth header. Body is streamed chunk-by-chunk to a .partial
    file and atomically renamed on success, so the watcher never reads
    a half-written ZIP.
    """
    expected = settings.inbox_upload_auth_token
    if not expected:
        raise HTTPException(
            status_code=503,
            detail="inbox upload disabled (set WARDEN_INBOX_UPLOAD_AUTH_TOKEN to enable)",
        )
    # constant-time compare so a timing attack can't probe the token
    if not hmac.compare_digest(x_warden_auth, expected):
        raise HTTPException(status_code=401, detail="bad auth")
    if item_id <= 0:
        raise HTTPException(status_code=400, detail="invalid item_id")

    inbox = Path(settings.downloads_view_path) / settings.inbox_subfolder
    try:
        inbox.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"inbox mkdir: {e}") from e

    target = inbox / f"bandcamp_{item_id}.zip"
    partial = inbox / f"bandcamp_{item_id}.zip.partial"

    if target.exists():
        # Idempotent re-upload: report the existing file's size and bail.
        # The watcher will pick it up on its next sweep regardless.
        return {
            "ok": True,
            "already_present": True,
            "size": target.stat().st_size,
        }

    written = 0
    max_bytes = settings.inbox_upload_max_bytes
    try:
        with partial.open("wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
                written += len(chunk)
                if written > max_bytes:
                    raise HTTPException(
                        status_code=413,
                        detail=f"upload exceeds max_bytes={max_bytes}",
                    )
                f.write(chunk)
        if written == 0:
            raise HTTPException(status_code=400, detail="empty body")
        os.replace(partial, target)
    except HTTPException:
        partial.unlink(missing_ok=True)
        raise
    except OSError as e:
        partial.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=f"write failed: {e}") from e
    except Exception as e:
        partial.unlink(missing_ok=True)
        log.exception("inbox upload failed for item %d", item_id)
        raise HTTPException(
            status_code=500, detail=f"{type(e).__name__}: {e}"
        ) from e

    log.info("inbox upload: item %d, %d bytes → %s", item_id, written, target)
    return {"ok": True, "item_id": item_id, "size": written, "path": str(target)}
