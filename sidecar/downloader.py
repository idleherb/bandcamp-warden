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
import re
import shutil
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


# Empirically validated (see /test-browser-headers): Bandcamp's CDN
# throttles non-browser-shaped requests to ~0.28 MB/s but serves
# browser-shaped requests at 1.44+ MB/s. So every download request
# from this module sends the same headers a real Chrome navigation
# would emit. Referer is a generic bandcamp.com URL — per-album
# Referer would be marginally better but requires more plumbing.
_BROWSER_HEADERS = {
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-Ch-Ua": (
        '"Chromium";v="124", "Google Chrome";v="124", "Not.A/Brand";v="99"'
    ),
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-site",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://bandcamp.com/",
    "Connection": "keep-alive",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
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
        format_name: str = "flac",
        connect_timeout: float = 30.0,
        read_timeout: float = 60.0,
        max_resumes_per_album: int = 30,
        resume_delay_seconds: float = 10.0,
        between_albums_seconds: float = 5.0,
    ) -> None:
        self.downloads_root = downloads_root
        self.config_dir = config_dir
        self.format_name = format_name
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_resumes_per_album = max_resumes_per_album
        self.resume_delay_seconds = resume_delay_seconds
        self.between_albums_seconds = between_albums_seconds
        # Cancellation flag. Set externally (e.g. from /stop) to break
        # out between albums; an in-flight download will continue until
        # the current attempt finishes or its read-timeout fires.
        self.cancel_requested = False

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

        _log = log_event or (lambda s: None)
        _log(
            f"Daily run start: {len(bc.purchases)} purchases known, "
            f"{len(completed_ids)} already done, quota={quota}"
        )

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
                item, signed_url, log_event=_log,
            )
            if outcome.success:
                successes.append(outcome)
                completed_ids.add(iid)
                self._append_ignore(iid, band, title)
            else:
                failures.append(outcome)
                last_error = outcome.error
                # Don't keep trying if we hit a hard auth issue; a
                # series of these is exactly the ban-precursor we want
                # to bail on. The orchestrator's anomaly detection
                # observes log_event output, so it'll see and brake.

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
    ) -> DownloadOutcome:
        iid = item.item_id
        band = item.band_name or "?"
        title = item.item_title or "?"

        with TemporaryDirectory(prefix="warden_dl_") as td:
            tmp_zip = Path(td) / f"item_{iid}.bin"
            try:
                bytes_written, resumes, content_type = await self._stream_to_file(
                    signed_url, tmp_zip, log_event,
                )
            except Exception as e:
                err = f"download: {type(e).__name__}: {e}"
                log_event(f"  ✗ {err}")
                return DownloadOutcome(
                    success=False, item_id=iid, band_name=band, item_title=title,
                    folder=None, bytes_written=0, resumes=0, error=err,
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

        log_event(f"  ✓ done ({bytes_written} bytes, {resumes} resumes)")
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
    ) -> tuple[int, int, str]:
        """Stream `url` to `path` with Range-resume on stall.

        Uses curl_cffi.Session(impersonate="chrome") because Bandcamp's
        CDN appears to TLS-fingerprint clients in addition to checking
        request headers; httpx with all the right headers still gets
        throttled (validated empirically: same headers via httpx hung
        for 10 minutes, via curl_cffi got 1.44 MB/s on the same URL).

        Runs in a thread because curl_cffi's API is sync.
        """
        return await asyncio.to_thread(
            self._stream_to_file_sync, url, path, log_event,
        )

    def _stream_to_file_sync(
        self, url: str, path: Path, log_event: Callable[[str], None],
    ) -> tuple[int, int, str]:
        bytes_written = 0
        resumes = 0
        content_length_total: int | None = None
        content_type: str = ""
        last_log_pct = 0

        with path.open("ab") as fh:
            while True:
                headers: dict[str, str] = dict(_BROWSER_HEADERS)
                is_resume = bytes_written > 0
                if is_resume:
                    headers["Range"] = f"bytes={bytes_written}-"

                try:
                    with curl_requests.Session(
                        impersonate="chrome",
                        curl_options={
                            CurlOpt.LOW_SPEED_TIME: 60,
                            CurlOpt.LOW_SPEED_LIMIT: 1024,
                        },
                    ) as session:
                        r = session.get(
                            url, headers=headers, stream=True,
                            timeout=self.connect_timeout + self.read_timeout,
                        )
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
                                if content_length_total:
                                    pct = int(
                                        bytes_written / content_length_total * 100
                                    )
                                    if pct >= last_log_pct + 10:
                                        last_log_pct = (pct // 10) * 10
                                        log_event(f"  … {last_log_pct}%")
                        finally:
                            r.close()

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

    def _read_ignores(self) -> set[int]:
        path = self.config_dir / "ignores.txt"
        out: set[int] = set()
        if not path.exists():
            return out
        for line in path.read_text(errors="replace").splitlines():
            stripped = line.split("#", 1)[0].strip()
            if stripped.isdigit():
                out.add(int(stripped))
        return out

    def _append_ignore(self, item_id: int, band: str, title: str) -> None:
        path = self.config_dir / "ignores.txt"
        # Match bandcampsync's existing format: id  # band / title
        line = f"{item_id}  # {band} / {title}\n"
        try:
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
        except Exception as e:
            log.warning("failed to append ignores.txt: %s", e)

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
