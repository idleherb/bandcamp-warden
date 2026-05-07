"""Sidecar-side downloader. Plan C of the bandcamp-warden project.

Replaces the bandcampsync container's internal sync flow. We still use
the bandcampsync library to:
  * authenticate (TLS-impersonation matters here)
  * resolve item → signed download URL
  * call check_download_stat to keep the URL fresh

But the actual file download, ZIP extract, and ignores.txt management
happen in the sidecar with proper Python-side timeouts and HTTP Range
resume. This is the structural fix for the curl-error-28 problem and
the mystery SIGKILL events: there's no separate container to be killed,
and we control the entire download lifecycle.

Why this works where the bandcampsync patch didn't:
  * httpx's read timeout fires reliably (proven with stdlib socket-level
    SO_RCVTIMEO under the hood) — curl_cffi's LOW_SPEED_TIME was being
    silently ignored under impersonate="chrome".
  * Range: bytes=N- on the same signed URL lets us resume after a
    stall instead of wasting the bandwidth that already arrived.
  * Bandcamp's CDN (popplers5.bandcamp.com / bcbits) is regular
    CloudFront — no TLS-fingerprint gating like the Fan API has.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import shutil
import threading
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import AsyncIterator, Callable
from zipfile import ZipFile

import curl_cffi.requests as curl_requests
from curl_cffi.const import CurlOpt


log = logging.getLogger("warden.downloader")


# Mimic the user's Firefox browser exactly — that's the configuration
# they proved gets full bandwidth from Bandcamp (120MB album in 2 s
# locally). curl_cffi's impersonate="firefox133" handles TLS fingerprint
# + HTTP/2 settings; we add Firefox-specific headers (DNT, Sec-GPC,
# Priority — none of which Chrome sends) so the request is consistent
# end-to-end. User-Agent is left to impersonate's default so it matches
# the TLS fingerprint version automatically.
_BROWSER_IMPERSONATE = "firefox133"
_BROWSER_HEADERS = {
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


# ---------- Path sanitization ----------

# Same rule bandcampsync uses (LocalMedia._clean_path), so albums land
# at exactly the same paths upstream would have used. NFKD normalize +
# strip the disallowed punctuation set.
_DISALLOWED_PATH_CHARS = '"#%\'*/?\\`:'


def clean_path_component(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    s = "".join(c for c in s if c not in _DISALLOWED_PATH_CHARS)
    # Filesystems also choke on bare control chars and certain
    # zero-width / format chars; strip them.
    s = "".join(
        c for c in s if unicodedata.category(c) not in ("Cf", "Mn", "Cc")
    )
    return s.strip().rstrip(". ") or "_"


# ---------- Result types ----------

@dataclass
class DownloadOutcome:
    success: bool
    item_id: int
    band_name: str
    item_title: str
    folder: Path | None
    bytes_written: int
    resumes: int
    error: str | None = None


@dataclass
class RunSummary:
    started_at: str
    finished_at: str
    quota: int
    baseline_count: int
    final_count: int
    downloaded: int
    successes: list[DownloadOutcome]
    failures: list[DownloadOutcome]
    skipped: int  # already in ignores
    stop_reason: str
    last_error: str | None


# ---------- Downloader ----------

class WardenDownloader:
    """Owns one daily run from start to finish."""

    def __init__(
        self,
        downloads_root: Path,
        config_dir: Path,
        state_dir: Path | None = None,
        format_name: str = "flac",
        connect_timeout: float = 30.0,
        read_timeout: float = 60.0,
        max_resumes_per_album: int = 30,
        resume_delay_seconds: float = 10.0,
        between_albums_seconds: float = 5.0,
        max_consecutive_failures: int = 3,
    ) -> None:
        self.downloads_root = downloads_root
        self.config_dir = config_dir
        # state_dir is the sidecar's RW state mount. We use it as the
        # primary location for ignores tracking because /config might
        # be read-only (legacy compose). When config_dir IS writable,
        # we also append to /config/ignores.txt to keep the file in
        # sync for any future container-strategy fallback.
        self.state_dir = state_dir
        self.format_name = format_name
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_resumes_per_album = max_resumes_per_album
        self.resume_delay_seconds = resume_delay_seconds
        self.between_albums_seconds = between_albums_seconds
        self.max_consecutive_failures = max_consecutive_failures
        # Cancellation flag. Set externally (e.g. from /stop) to break
        # out between albums; an in-flight download will continue until
        # the current attempt finishes or its read-timeout fires.
        self.cancel_requested = False

    # ----- helpers -----

    def _read_cookie_jar(self) -> dict[str, str]:
        """Parse cookies.txt into a name→value dict scoped to bandcamp.com."""
        out: dict[str, str] = {}
        path = self.config_dir / "cookies.txt"
        if not path.exists():
            return out
        for line in path.read_text(errors="replace").splitlines():
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) >= 7 and "bandcamp.com" in parts[0]:
                out[parts[5]] = parts[6]
        return out

    async def _warmup_browse(
        self,
        item_url: str,
        cookie_jar: dict[str, str],
        log_event: Callable[[str], None],
    ) -> None:
        """Visit the album's bandcamp page (e.g.
        valyri.bandcamp.com/album/saturnfall) before kicking off the
        download. Mimics a real user clicking into the album. We
        discard the response body — we only need Bandcamp's session-
        side state to register the visit."""
        delay = random.uniform(3.0, 8.0)
        log_event(f"  ⋯ warmup-browse {item_url[:80]} (delay {delay:.1f}s)")
        await asyncio.sleep(delay)

        def _do_get() -> tuple[int | None, int]:
            try:
                with curl_requests.Session(
                    impersonate=_BROWSER_IMPERSONATE,
                ) as s:
                    headers = dict(_BROWSER_HEADERS)
                    headers["Referer"] = "https://bandcamp.com/"
                    r = s.get(
                        item_url, headers=headers, cookies=cookie_jar,
                        timeout=30,
                    )
                    body = r.content or b""
                    r.close()
                    return r.status_code, len(body)
            except Exception as e:
                return None, 0

        status, body_len = await asyncio.to_thread(_do_get)
        log_event(f"  ⋯ warmup status={status} body={body_len} bytes")

    # ----- public entry point -----

    async def run_daily(
        self,
        quota: int,
        baseline_count: int,
        log_event: Callable[[str], None] | None = None,
    ) -> RunSummary:
        """Run until quota albums are done, or no more items remain."""
        from bandcampsync.bandcamp import Bandcamp, BandcampError  # type: ignore

        started_at = datetime.now(timezone.utc).isoformat()

        cookies_path = self.config_dir / "cookies.txt"
        if not cookies_path.exists():
            return self._failed_summary(
                started_at, quota, baseline_count, "cookies.txt missing"
            )

        try:
            bc = Bandcamp(cookies_path.read_text(errors="replace"))
            bc.verify_authentication()
            bc.load_purchases()
        except Exception as e:
            return self._failed_summary(
                started_at, quota, baseline_count,
                f"bandcampsync init: {type(e).__name__}: {e}",
            )

        completed_ids = self._read_ignores()
        successes: list[DownloadOutcome] = []
        failures: list[DownloadOutcome] = []
        skipped = 0
        stop_reason = "completed_loop"
        last_error: str | None = None
        consecutive_failures: int | None = 0

        _log = log_event or (lambda s: None)
        _log(
            f"Daily run start: {len(bc.purchases)} purchases known, "
            f"{len(completed_ids)} already done, quota={quota}"
        )

        # Cookie jar for warmup browse + downloads. Loaded once.
        cookie_jar = self._read_cookie_jar()

        for item in bc.purchases:
            if self.cancel_requested:
                stop_reason = "cancelled"
                break
            if len(successes) >= quota:
                stop_reason = "quota_hit"
                break
            iid = item.item_id
            if iid in completed_ids:
                skipped += 1
                continue

            band = (item.band_name or "?")
            title = (item.item_title or "?")
            _log(f"→ {band} / {title} (id:{iid})")

            # Warmup browse — GET the album's main page like a human
            # who clicked into the album before downloading. Real users
            # navigate album-page → download-page → cdn-fetch; bandcampsync
            # by default jumps straight from API to cdn-fetch, missing
            # the album-page step. Bandcamp's anti-bot may flag the
            # short sequence as bot-like; the warmup smooths it out.
            item_url = getattr(item, "item_url", None) or item._data.get("item_url")
            if item_url:
                try:
                    await self._warmup_browse(item_url, cookie_jar, _log)
                except Exception as e:
                    _log(f"  ! warmup browse failed (non-fatal): {e}")

            try:
                signed_url = bc.get_download_file_url(item, encoding=self.format_name)
                if not signed_url:
                    raise BandcampError("URL resolution returned empty")
            except Exception as e:
                err = f"URL resolution: {type(e).__name__}: {e}"
                _log(f"  ! {err}")
                failures.append(DownloadOutcome(
                    success=False, item_id=iid, band_name=band, item_title=title,
                    folder=None, bytes_written=0, resumes=0, error=err,
                ))
                last_error = err
                continue

            outcome = await self._fetch_extract_and_record(
                item, signed_url, log_event=_log, cookie_jar=cookie_jar,
            )
            if outcome.success:
                successes.append(outcome)
                completed_ids.add(iid)
                self._append_ignore(iid, band, title)
                consecutive_failures = 0
            else:
                failures.append(outcome)
                last_error = outcome.error
                consecutive_failures = (
                    1 if consecutive_failures is None
                    else consecutive_failures + 1
                )
                if consecutive_failures >= self.max_consecutive_failures:
                    _log(
                        f"  ⛔ {consecutive_failures} consecutive failures — "
                        f"circuit breaker tripped, bailing out of run "
                        f"to protect the account"
                    )
                    stop_reason = "circuit_break"
                    break

            await asyncio.sleep(self.between_albums_seconds)

        finished_at = datetime.now(timezone.utc).isoformat()
        return RunSummary(
            started_at=started_at,
            finished_at=finished_at,
            quota=quota,
            baseline_count=baseline_count,
            final_count=baseline_count + len(successes),
            downloaded=len(successes),
            successes=successes,
            failures=failures,
            skipped=skipped,
            stop_reason=stop_reason,
            last_error=last_error,
        )

    # ----- per-album orchestration -----

    async def _fetch_extract_and_record(
        self,
        item,
        signed_url: str,
        log_event: Callable[[str], None],
        cookie_jar: dict[str, str] | None = None,
    ) -> DownloadOutcome:
        iid = item.item_id
        band = item.band_name or "?"
        title = item.item_title or "?"
        # Per-item Referer matches what a real browser would send: the
        # bandcamp.com/download?... page where the user clicked the
        # FLAC link.  Static `bandcamp.com/` Referer (our previous
        # default) is detectable as bot-like.
        referer = (
            getattr(item, "download_url", None)
            or item._data.get("download_url")
            or "https://bandcamp.com/"
        )

        import time as _time
        dl_started = _time.time()
        with TemporaryDirectory(prefix="warden_dl_") as td:
            tmp_zip = Path(td) / f"item_{iid}.bin"
            try:
                bytes_written, resumes, content_type = await self._stream_to_file(
                    signed_url, tmp_zip, log_event,
                    cookie_jar=cookie_jar, referer=referer,
                )
            except Exception as e:
                err = f"download: {type(e).__name__}: {e}"
                log_event(f"  ✗ {err}")
                return DownloadOutcome(
                    success=False, item_id=iid, band_name=band, item_title=title,
                    folder=None, bytes_written=0, resumes=0, error=err,
                )
            dl_seconds = max(0.001, _time.time() - dl_started)
            mbps = (bytes_written / 1_000_000) / dl_seconds
            log_event(
                f"  ↓ {bytes_written / 1_000_000:.1f} MB in "
                f"{dl_seconds:.1f}s = {mbps:.2f} MB/s"
            )

            artist_dir = self.downloads_root / clean_path_component(band)
            album_dir = artist_dir / clean_path_component(title)
            album_dir.mkdir(parents=True, exist_ok=True)

            try:
                if _looks_like_zip(tmp_zip):
                    self._extract_zip(tmp_zip, album_dir, log_event)
                else:
                    # Single-track download — drop it directly.
                    ext = self._guess_ext(content_type, signed_url)
                    dest = album_dir / clean_path_component(f"{title}.{ext}")
                    shutil.move(str(tmp_zip), dest)
                    log_event(f"  · single-track moved to {dest.name}")
            except Exception as e:
                err = f"extract: {type(e).__name__}: {e}"
                log_event(f"  ✗ {err}")
                return DownloadOutcome(
                    success=False, item_id=iid, band_name=band, item_title=title,
                    folder=album_dir, bytes_written=bytes_written, resumes=resumes,
                    error=err,
                )

            # Verify extract actually produced audio files. bandcampsync
            # had a long-standing bug here: it would write item_id to
            # ignores.txt even if the unzip produced an empty folder
            # (corrupt zip, exception mid-move, etc.), making a 're-
            # download all the broken ones' impossible without manually
            # editing ignores.txt. We refuse to mark an album as done
            # unless at least one audio file is on disk.
            audio_extensions = {".flac", ".mp3", ".wav", ".aiff", ".alac", ".ogg"}
            files_with_audio = [
                p for p in album_dir.rglob("*")
                if p.is_file() and p.suffix.lower() in audio_extensions
            ]
            if not files_with_audio:
                err = "no audio files in album dir after extract"
                log_event(f"  ✗ {err}")
                return DownloadOutcome(
                    success=False, item_id=iid, band_name=band, item_title=title,
                    folder=album_dir, bytes_written=bytes_written, resumes=resumes,
                    error=err,
                )

        log_event(
            f"  ✓ done ({bytes_written} bytes, {resumes} resumes, "
            f"{len(files_with_audio)} audio files)"
        )
        return DownloadOutcome(
            success=True, item_id=iid, band_name=band, item_title=title,
            folder=album_dir, bytes_written=bytes_written, resumes=resumes,
        )

    # ----- the actual streaming download with Range resume -----

    async def _stream_to_file(
        self,
        url: str,
        path: Path,
        log_event: Callable[[str], None],
        cookie_jar: dict[str, str] | None = None,
        referer: str | None = None,
    ) -> tuple[int, int, str]:
        """Stream `url` to `path` with Range-resume on stall.

        Uses curl_cffi.Session(impersonate="firefox133") because Bandcamp's
        CDN TLS-fingerprints clients in addition to checking headers.
        cookie_jar carries the user's bandcamp cookies; referer should be
        the bandcamp.com/download?... page so the request looks like a
        click from there (rather than navigation from bandcamp.com root).

        Runs in a thread because curl_cffi's API is sync.
        """
        return await asyncio.to_thread(
            self._stream_to_file_sync, url, path, log_event,
            cookie_jar, referer,
        )

    def _stream_to_file_sync(
        self, url: str, path: Path, log_event: Callable[[str], None],
        cookie_jar: dict[str, str] | None = None,
        referer: str | None = None,
    ) -> tuple[int, int, str]:
        bytes_written = 0
        resumes = 0
        content_length_total: int | None = None
        content_type: str = ""
        last_log_pct = 0
        # Python-side stall timeout. curl_cffi's CurlOpt.LOW_SPEED_TIME
        # was empirically observed NOT to fire under impersonate=chrome,
        # leaving us at the mercy of TCP keepalive (effectively forever).
        # This is our backstop: a watchdog thread closes the response
        # forcefully if no bytes arrive for N seconds.
        STALL_SECONDS = max(int(self.read_timeout), 30)

        with path.open("ab") as fh:
            while True:
                headers: dict[str, str] = dict(_BROWSER_HEADERS)
                if referer:
                    headers["Referer"] = referer
                is_resume = bytes_written > 0
                if is_resume:
                    headers["Range"] = f"bytes={bytes_written}-"

                try:
                    with curl_requests.Session(
                        impersonate=_BROWSER_IMPERSONATE,
                        curl_options={
                            CurlOpt.LOW_SPEED_TIME: 60,
                            CurlOpt.LOW_SPEED_LIMIT: 1024,
                        },
                    ) as session:
                        r = session.get(
                            url, headers=headers, stream=True,
                            cookies=cookie_jar or None,
                            timeout=self.connect_timeout + self.read_timeout,
                        )

                        # ---- watchdog ----
                        watchdog_state = {
                            "last_progress_at": time.time(),
                            "bytes_seen": 0,
                            "killed": False,
                        }
                        watchdog_stop = threading.Event()

                        def _watchdog() -> None:
                            while not watchdog_stop.is_set():
                                if watchdog_stop.wait(timeout=5):
                                    return
                                idle = time.time() - watchdog_state["last_progress_at"]
                                if idle > STALL_SECONDS:
                                    watchdog_state["killed"] = True
                                    log_event(
                                        f"  ⚠ watchdog: no bytes for "
                                        f"{int(idle)}s, closing connection"
                                    )
                                    try:
                                        r.close()
                                    except Exception:
                                        pass
                                    return

                        wd_thread = threading.Thread(target=_watchdog, daemon=True)
                        wd_thread.start()

                        try:
                            if is_resume and r.status_code == 200:
                                log_event("  ! server ignored Range, restarting from 0")
                                fh.seek(0)
                                fh.truncate()
                                bytes_written = 0
                                last_log_pct = 0
                            elif is_resume and r.status_code == 416:
                                if (
                                    content_length_total is not None
                                    and bytes_written >= content_length_total
                                ):
                                    log_event("  · 416 with full content, OK")
                                    return bytes_written, resumes, content_type
                                raise RuntimeError("416 with incomplete content")
                            elif r.status_code not in (200, 206):
                                raise RuntimeError(
                                    f"unexpected status {r.status_code}"
                                )

                            if content_length_total is None:
                                cr = (
                                    r.headers.get("content-range")
                                    or r.headers.get("Content-Range")
                                )
                                if cr:
                                    m = re.match(r"bytes\s+\d+-\d+/(\d+)", cr)
                                    if m:
                                        content_length_total = int(m.group(1))
                                else:
                                    cl = (
                                        r.headers.get("content-length")
                                        or r.headers.get("Content-Length")
                                    )
                                    if cl:
                                        content_length_total = int(cl)
                            if not content_type:
                                content_type = (
                                    (r.headers.get("content-type")
                                     or r.headers.get("Content-Type") or "")
                                    .split(";")[0].strip()
                                )

                            for chunk in r.iter_content(chunk_size=64 * 1024):
                                if not chunk:
                                    continue
                                fh.write(chunk)
                                bytes_written += len(chunk)
                                # watchdog progress beat
                                watchdog_state["last_progress_at"] = time.time()
                                watchdog_state["bytes_seen"] = bytes_written
                                if content_length_total:
                                    pct = int(
                                        bytes_written / content_length_total * 100
                                    )
                                    if pct >= last_log_pct + 10:
                                        last_log_pct = (pct // 10) * 10
                                        log_event(f"  … {last_log_pct}%")
                            # If the watchdog killed the response, surface
                            # that as an exception so the resume path runs.
                            if watchdog_state["killed"]:
                                raise RuntimeError("watchdog stall close")
                        finally:
                            watchdog_stop.set()
                            try:
                                r.close()
                            except Exception:
                                pass

                        if (
                            content_length_total
                            and bytes_written < content_length_total
                        ):
                            raise RuntimeError("premature EOF before content-length")
                        return bytes_written, resumes, content_type

                except Exception as e:
                    if resumes >= self.max_resumes_per_album:
                        log_event(
                            f"  ✗ giving up after {resumes} resumes "
                            f"({type(e).__name__}: {e})"
                        )
                        raise
                    resumes += 1
                    log_event(
                        f"  ↺ {type(e).__name__} at offset "
                        f"{bytes_written}/{content_length_total} — "
                        f"resume {resumes}/{self.max_resumes_per_album}"
                    )
                    time.sleep(self.resume_delay_seconds)
                    continue

    # ----- ZIP handling -----

    @staticmethod
    def _extract_zip(zip_path: Path, dest: Path, log_event) -> None:
        with ZipFile(zip_path) as z:
            for member in z.namelist():
                # Defang absolute / parent-relative paths.
                clean_name = "/".join(
                    clean_path_component(p) for p in member.split("/") if p
                )
                target = dest / clean_name
                target.parent.mkdir(parents=True, exist_ok=True)
                with z.open(member) as src, target.open("wb") as out:
                    shutil.copyfileobj(src, out)
        log_event(f"  · extracted into {dest}")

    @staticmethod
    def _guess_ext(content_type: str, url: str) -> str:
        ct = (content_type or "").lower()
        if "flac" in ct:
            return "flac"
        if "mpeg" in ct or "mp3" in ct:
            return "mp3"
        # Fall back to URL hint
        m = re.search(r"\.([a-zA-Z0-9]{2,4})(?:\?|$)", url)
        if m:
            return m.group(1)
        return "bin"

    # ----- ignores.txt -----
    #
    # Reading: union of both paths (sidecar's /state and bandcampsync's
    # /config) so neither tool re-downloads what the other did.
    # Writing: prefer /config (legacy bandcampsync location) when it's
    # writable; fall back to /state. This way Plan C works even when
    # the user's compose still mounts /config:ro.

    def _ignore_paths(self) -> list[Path]:
        paths = [self.config_dir / "ignores.txt"]
        if self.state_dir is not None:
            paths.append(self.state_dir / "ignores_warden.txt")
        return paths

    def _read_ignores(self) -> set[int]:
        out: set[int] = set()
        for path in self._ignore_paths():
            if not path.exists():
                continue
            for line in path.read_text(errors="replace").splitlines():
                stripped = line.split("#", 1)[0].strip()
                if stripped.isdigit():
                    out.add(int(stripped))
        return out

    def _append_ignore(self, item_id: int, band: str, title: str) -> None:
        line = f"{item_id}  # {band} / {title}\n"
        # Try /config first (bandcampsync convention). If RO, fall
        # back to /state. Worst case: log warning, still no crash.
        for path in self._ignore_paths():
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as f:
                    f.write(line)
                return
            except Exception as e:
                log.warning("append %s failed: %s", path, e)
                continue
        log.error(
            "could not append ignore entry for item %d to any path",
            item_id,
        )

    # ----- helpers -----

    @staticmethod
    def _failed_summary(
        started_at: str, quota: int, baseline_count: int, error: str,
    ) -> RunSummary:
        finished_at = datetime.now(timezone.utc).isoformat()
        return RunSummary(
            started_at=started_at, finished_at=finished_at,
            quota=quota, baseline_count=baseline_count,
            final_count=baseline_count, downloaded=0,
            successes=[], failures=[], skipped=0,
            stop_reason="failed_to_start", last_error=error,
        )


def _looks_like_zip(path: Path) -> bool:
    try:
        with path.open("rb") as f:
            head = f.read(4)
        return head[:2] == b"PK"
    except Exception:
        return False
