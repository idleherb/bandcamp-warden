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
import logging
import os
import re
import sqlite3
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

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

    def start_run(self, run_date: str, quota: int) -> None:
        with self._conn() as db:
            db.execute(
                """INSERT OR REPLACE INTO daily_runs
                   (run_date, quota, downloaded, started_at, status)
                   VALUES (?, ?, 0, ?, 'running')""",
                (run_date, quota, datetime.now(timezone.utc).isoformat()),
            )

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

    def stream_logs(self, container) -> Iterable[str]:
        """Yield decoded log lines as bandcampsync produces them."""
        for chunk in container.logs(stream=True, follow=True, tail=0):
            text = chunk.decode("utf-8", errors="replace")
            for line in text.splitlines():
                if line:
                    yield line


# ---------- Orchestrator ----------

class Orchestrator:
    def __init__(
        self,
        state: State,
        controller: BandcampsyncController,
        telegram: Telegram,
        config_view: Path,
        settings: Settings,
    ) -> None:
        self.state = state
        self.controller = controller
        self.telegram = telegram
        self.config_view = config_view
        self.settings = settings
        self.log_buffer: deque[str] = deque(maxlen=settings.log_buffer_size)
        self._run_lock = asyncio.Lock()
        self._last_download_at: str | None = None

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
        baseline = count_completed_albums(self.config_view)
        self.state.start_run(run_date, quota)

        await self.telegram.send(
            f"▶ *Tag {day_index + 1} startet* (`{run_date}`)\n"
            f"Quota heute: *{quota}* Alben\n"
            f"Bisher gesamt: *{baseline}* Alben"
        )

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

        await self._monitor(run_date, baseline, quota, container)

    async def _monitor(self, run_date: str, baseline: int, quota: int, container) -> None:
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

        # 24h hard cap on a single daily run so a stuck log stream can't pin us forever.
        deadline = loop.time() + 24 * 3600

        def check_quota() -> tuple[bool, int]:
            nonlocal last_count
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
                # Periodic checks even if bandcampsync is quiet. The folder
                # rglob is the only place we re-count, so it can't dominate
                # CPU even on big trees.
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

            # Anomaly detection runs only on non-INFO lines so artist/album
            # names containing words like "forbidden" don't trigger false
            # positives. Real auth/rate errors land at WARNING/ERROR.
            if is_anomaly_line(line):
                anomaly_hits = sum(1 for ln in recent if is_anomaly_line(ln))
                if anomaly_hits >= self.settings.anomaly_threshold:
                    stop_reason = "emergency"
                    break

        # Make sure bandcampsync is actually stopped.
        self.controller.stop()
        final = count_completed_albums(self.config_view)
        downloaded_today = final - baseline
        finished_at = datetime.now(timezone.utc).isoformat()

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
            await self.telegram.send(
                "🚨 *NOTBREMSE*\n"
                f"`{anomaly_hits}` Auth-/Rate-Fehler in den letzten "
                f"{self.settings.anomaly_window} Log-Zeilen.\n"
                f"Container gestoppt. Heute geschafft: *{downloaded_today}*/{quota}, "
                f"Gesamt: *{final}*.\n\n"
                f"Letzte Zeilen:\n```\n{recent_tail[:800]}\n```\n\n"
                "Bitte prüfen, dann `POST /reset-emergency` am Sidecar."
            )
            return

        if stop_reason == "quota_hit":
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="quota_hit",
                stop_reason="daily quota reached",
                finished_at=finished_at,
            )
            await self.telegram.send(
                f"✅ *Tag fertig* (`{run_date}`)\n"
                f"Heute: *{downloaded_today}*/{quota}\n"
                f"Gesamt: *{final}*"
            )
            return

        if stop_reason == "deadline":
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="completed",
                stop_reason="24h deadline hit",
                finished_at=finished_at,
            )
            await self.telegram.send(
                "⌛ *24h-Deadline erreicht*\n"
                f"Heute: *{downloaded_today}*/{quota}, Gesamt: *{final}*.\n"
                "Run war ungewöhnlich lang. Bitte `/logs` prüfen."
            )
            return

        # bandcampsync exited on its own. Two interpretations: collection done,
        # or a hard error. If we made progress and there are no recent anomalies
        # in the buffer, assume done. Otherwise flag.
        had_anomaly = any(is_anomaly_line(ln) for ln in recent)
        if downloaded_today > 0 and not had_anomaly:
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="completed",
                stop_reason="bandcampsync exited (likely caught up)",
                finished_at=finished_at,
            )
            self.state.update(collection_complete=1 if downloaded_today < quota else 0)
            msg = (
                "🎉 *Collection vollständig*\n"
                f"bandcampsync hat sich beendet, keine ausstehenden Alben mehr.\n"
                f"Gesamt: *{final}*"
                if downloaded_today < quota
                else f"✅ *Tag fertig* (`{run_date}`)\n"
                f"bandcampsync hat exakt die Quota geliefert: *{downloaded_today}*/{quota}\n"
                f"Gesamt: *{final}*"
            )
            await self.telegram.send(msg)
        else:
            self.state.update_run(
                run_date,
                downloaded=downloaded_today,
                status="completed",
                stop_reason="bandcampsync exited unexpectedly",
                finished_at=finished_at,
            )
            recent_tail = "\n".join(list(recent)[-6:])
            await self.telegram.send(
                "⚠️ *bandcampsync hat unerwartet beendet*\n"
                f"Heute: *{downloaded_today}*/{quota}, Gesamt: *{final}*.\n"
                f"Letzte Zeilen:\n```\n{recent_tail[:800]}\n```"
            )

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
orchestrator = Orchestrator(
    state, controller, telegram, Path(settings.config_view_path), settings
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


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/status")
async def status_endpoint() -> dict:
    return orchestrator.status()


@app.get("/logs")
async def logs(lines: int = 200) -> dict:
    n = max(1, min(lines, settings.log_buffer_size))
    return {"lines": list(orchestrator.log_buffer)[-n:]}


@app.post("/trigger")
async def trigger_now() -> dict:
    """Manually fire the daily kickoff. Useful for first-deploy smoke test."""
    if orchestrator._run_lock.locked():
        raise HTTPException(409, "A run is already in progress")
    asyncio.create_task(orchestrator.daily_kickoff())
    return {"triggered": True}


@app.post("/stop")
async def stop_now() -> dict:
    """Force-stop bandcampsync (does NOT trip the emergency flag)."""
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
