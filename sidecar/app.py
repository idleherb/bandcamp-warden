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
import json
import logging
import os
import re
import sqlite3
import threading
import urllib.parse
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
from fastapi import FastAPI, HTTPException
from pydantic import Field
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

    # Cookie expiry monitoring
    cookies_path: str = "/config/cookies.txt"
    cookie_warn_threshold_days: int = 14
    cookie_check_hour: int = 12     # daily check at midday — shows up at a sane time

    # Resilience: auto-retry after bandcampsync crash (network blip, etc.)
    retry_max_per_day: int = 3
    retry_backoffs_minutes: list[int] = Field(default_factory=lambda: [5, 15, 60])

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""

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
# Attach to the root logger so we capture warden, uvicorn, apscheduler etc.
logging.getLogger().addHandler(_sidecar_log_buffer)


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
                db.execute(
                    """UPDATE daily_runs
                       SET attempt = attempt + 1,
                           status = 'running',
                           stop_reason = NULL,
                           finished_at = NULL,
                           last_exit_code = NULL
                       WHERE run_date = ?""",
                    (run_date,),
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
        kwargs: dict = dict(
            image=s.bandcampsync_image,
            name=s.bandcampsync_container,
            environment=env,
            volumes={
                s.host_config_path: {"bind": "/config", "mode": "rw"},
                s.host_downloads_path: {"bind": "/downloads", "mode": "rw"},
            },
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

    async def daily_kickoff(self) -> None:
        """Top-level entry point. APScheduler fires this once per day."""
        if self._run_lock.locked():
            log.warning("Kickoff skipped — previous run still in progress")
            return
        async with self._run_lock:
            await self._do_kickoff()

    async def _do_kickoff(self) -> None:
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
        quota = self.quota_for_day(day_index)

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
scheduler = AsyncIOScheduler(timezone=settings.timezone)


@asynccontextmanager
async def lifespan(_: FastAPI):
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
        "Sidecar online. Daily kickoff: %02d:00 %s. Ramp quotas: %s. Cookie check: %02d:00.",
        settings.daily_run_hour, settings.timezone, settings.ramp_quotas,
        settings.cookie_check_hour,
    )
    await telegram.send(
        "🟢 *bandcamp-warden online*\n"
        f"Daily-Kickoff: `{settings.daily_run_hour:02d}:00 {settings.timezone}`\n"
        f"Ramp-Quotas: `{settings.ramp_quotas}`"
    )
    # Run a cookie check at startup so the user gets immediate feedback if the
    # cookie file is missing or already near expiry, instead of waiting until
    # tomorrow noon.
    asyncio.create_task(orchestrator.check_cookie_expiry())
    try:
        yield
    finally:
        scheduler.shutdown(wait=False)


app = FastAPI(title="bandcamp-warden", lifespan=lifespan)


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
async def trigger_now() -> dict:
    """Manually fire the daily kickoff. Useful for first-deploy smoke test."""
    if orchestrator._run_lock.locked():
        raise HTTPException(409, "A run is already in progress")
    asyncio.create_task(orchestrator.daily_kickoff())
    return {"triggered": True}


@app.post("/stop")
async def stop_now() -> dict:
    """Force-stop bandcampsync. Suppresses any auto-retry that would
    otherwise fire (since SIGTERM gives a non-zero exit code)."""
    orchestrator._user_stop_requested = True
    if orchestrator._retry_task and not orchestrator._retry_task.done():
        orchestrator._retry_task.cancel()
    controller.stop()
    return {"stopped": True}


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
