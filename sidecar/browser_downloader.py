"""Plan D: Playwright-driven download.

The user's browser downloads a 120MB album in 2 seconds; our scripts
get 0 bytes for an hour from the same signed URL. Bandcamp's CDN does
session-level throttling that we can't trick with HTTP headers — the
durable answer is to actually drive a real browser.

Design:
  * One Playwright context per "session", cookies loaded from
    /config/cookies.txt as a Netscape file via add_cookies()
  * Browser rotated per run (Chromium / Firefox) to look less like
    one specific bot
  * Stealth: disable navigator.webdriver, set realistic User-Agent,
    timezone, locale; per-album random delays.
  * For each album: navigate to the download page (not the API URL),
    click the FLAC link, intercept download via page.on("download").
  * Save zip to /downloads, extract, write metadata.
  * Item id → ignores.txt append on success (same convention as
    bandcampsync, so the existing counter still works).

This module is intentionally separate from downloader.py (httpx-based)
so we can A/B them or fall back if the browser approach has its own
problems.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Awaitable, Callable
from zipfile import ZipFile

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError,
)


log = logging.getLogger("warden.browser")


# Same path-cleaning rule downloader.py uses, so albums end up at the
# same on-disk locations regardless of which downloader did the work.
import unicodedata as _ud
_DISALLOWED = '"#%\'*/?\\`:'


def clean_path_component(s: str) -> str:
    s = _ud.normalize("NFKD", s or "")
    s = "".join(c for c in s if c not in _DISALLOWED)
    s = "".join(c for c in s if _ud.category(c) not in ("Cf", "Mn", "Cc"))
    return s.strip().rstrip(". ") or "_"


@dataclass
class BrowserDownloadOutcome:
    success: bool
    item_id: int
    band_name: str
    item_title: str
    folder: Path | None
    bytes_written: int
    duration_seconds: float
    browser_used: str
    error: str | None = None


# Chrome/Firefox UAs roughly current to 2026-05; intentionally one
# version behind the latest so we don't accidentally use a UA Bandcamp
# hasn't seen yet.
_USER_AGENTS = {
    "chromium": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "firefox": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
}


# Stealth tweaks applied to every page before any site script runs.
# Removes the most obvious automation tells; not bulletproof, but
# sufficient for sites that just check `navigator.webdriver`.
_STEALTH_INIT_SCRIPT = r"""
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
window.chrome = window.chrome || { runtime: {} };
const originalQuery = window.navigator.permissions && window.navigator.permissions.query;
if (originalQuery) {
    window.navigator.permissions.query = (params) => (
        params && params.name === 'notifications'
            ? Promise.resolve({ state: Notification.permission })
            : originalQuery(params)
    );
}
"""


def _parse_netscape_cookies(path: Path) -> list[dict]:
    """Convert a Netscape cookies.txt to Playwright add_cookies() input."""
    out: list[dict] = []
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            continue
        domain, _flag, _path, secure, expires, name, value = parts[:7]
        if "bandcamp.com" not in domain:
            continue
        try:
            expires_int = int(expires)
        except ValueError:
            expires_int = 0
        cookie = {
            "name": name,
            "value": value,
            "domain": domain,
            "path": _path or "/",
            "secure": secure.upper() == "TRUE",
        }
        if expires_int > 0:
            cookie["expires"] = expires_int
        out.append(cookie)
    return out


class BrowserDownloader:
    def __init__(
        self,
        downloads_root: Path,
        config_dir: Path,
        state_dir: Path | None = None,
        format_name: str = "flac",
        between_albums_min_s: float = 5.0,
        between_albums_max_s: float = 15.0,
        download_timeout_s: float = 600.0,
        nav_timeout_s: float = 60.0,
    ) -> None:
        self.downloads_root = downloads_root
        self.config_dir = config_dir
        # state_dir is the always-RW sidecar volume. If /config is
        # mounted RO (legacy compose), we still write the warden ignores
        # file to /state and read both at startup.
        self.state_dir = state_dir
        self.format_name = format_name
        self.between_min = between_albums_min_s
        self.between_max = between_albums_max_s
        self.download_timeout = download_timeout_s
        self.nav_timeout = nav_timeout_s

    async def _make_context(self, pw, browser_name: str) -> tuple[Browser, BrowserContext]:
        """Launch browser and return (browser, context) seeded with cookies."""
        ua = _USER_AGENTS.get(browser_name, _USER_AGENTS["chromium"])
        if browser_name == "firefox":
            browser = await pw.firefox.launch(headless=True)
        else:
            browser = await pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ],
            )
        context = await browser.new_context(
            user_agent=ua,
            locale="en-US",
            timezone_id="Europe/Berlin",
            viewport={"width": 1440, "height": 900},
        )
        cookies = _parse_netscape_cookies(self.config_dir / "cookies.txt")
        if cookies:
            await context.add_cookies(cookies)
        await context.add_init_script(_STEALTH_INIT_SCRIPT)
        return browser, context

    async def download_one(
        self,
        item,  # bandcampsync BandcampItem
        browser_name: str = "firefox",
        log_event: Callable[[str], None] | None = None,
    ) -> BrowserDownloadOutcome:
        """Drive a real browser to download one album and save it under
        /downloads/Artist/Album/. Uses item.download_url (the Bandcamp
        download page) — same URL the Fan API gives us; just navigate
        to it and click."""
        _log = log_event or (lambda s: None)
        start = datetime.now(timezone.utc).timestamp()
        iid = item.item_id
        band = item.band_name or "?"
        title = item.item_title or "?"
        page_url = getattr(item, "download_url", None)
        if not page_url:
            return BrowserDownloadOutcome(
                success=False, item_id=iid, band_name=band, item_title=title,
                folder=None, bytes_written=0, duration_seconds=0,
                browser_used=browser_name,
                error="no item.download_url",
            )

        async with async_playwright() as pw:
            browser, ctx = await self._make_context(pw, browser_name)
            try:
                return await self._download_via_context(
                    ctx, item, page_url, browser_name, _log, start,
                )
            finally:
                await ctx.close()
                await browser.close()

    async def _download_via_context(
        self, ctx, item, page_url, browser_name, _log, start_ts,
    ) -> BrowserDownloadOutcome:
        iid = item.item_id
        band = item.band_name or "?"
        title = item.item_title or "?"
        page = await ctx.new_page()
        page.set_default_navigation_timeout(self.nav_timeout * 1000)

        try:
            _log(f"navigate → {page_url}")
            await page.goto(page_url, wait_until="domcontentloaded")

            # Bandcamp's download page lists encodings as radio + a
            # "Download" link. Pick the FLAC option, then click.
            # The page sometimes pre-selects mp3-v0; we change first.
            #
            # The selector below was the one the official site used in
            # mid-2025; if Bandcamp redesigns, this is the brittle bit.
            try:
                # Click the format selector dropdown.
                await page.click("button.fmt-toggle", timeout=15000)
            except PlaywrightTimeoutError:
                # Some pages use a <select> instead of toggle button.
                pass

            # Try to pick FLAC by visible text.
            try:
                await page.click(
                    "ul.fmt-list >> text=/^FLAC/i", timeout=10000,
                )
                _log("FLAC selected")
            except PlaywrightTimeoutError:
                _log("could not click FLAC menu — assuming default")

            # Wait for the download link to be clickable.
            dl_button_selector = "a.item-button:has-text('Download')"
            await page.wait_for_selector(dl_button_selector, timeout=15000)

            # Trigger the download.
            with TemporaryDirectory(prefix="warden_browser_") as td:
                td_path = Path(td)
                async with page.expect_download(
                    timeout=self.download_timeout * 1000,
                ) as dl_info:
                    await page.click(dl_button_selector)
                download = await dl_info.value
                _log(f"download started: {download.suggested_filename}")
                tmp_path = td_path / "album.bin"
                await download.save_as(str(tmp_path))
                size = tmp_path.stat().st_size
                _log(f"saved {size} bytes to {tmp_path.name}")

                artist_dir = self.downloads_root / clean_path_component(band)
                album_dir = artist_dir / clean_path_component(title)
                album_dir.mkdir(parents=True, exist_ok=True)

                if _looks_like_zip(tmp_path):
                    self._extract_zip(tmp_path, album_dir)
                    _log(f"extracted into {album_dir}")
                else:
                    fname = clean_path_component(
                        f"{title}.{self.format_name}"
                    )
                    shutil.move(str(tmp_path), album_dir / fname)
                    _log(f"single-track moved to {fname}")

                # Verify at least one audio file exists post-extract.
                audio_exts = {".flac", ".mp3", ".wav", ".aiff", ".alac", ".ogg"}
                audio_count = sum(
                    1 for p in album_dir.rglob("*")
                    if p.is_file() and p.suffix.lower() in audio_exts
                )
                if audio_count == 0:
                    return BrowserDownloadOutcome(
                        success=False, item_id=iid, band_name=band, item_title=title,
                        folder=album_dir, bytes_written=size,
                        duration_seconds=datetime.now(timezone.utc).timestamp() - start_ts,
                        browser_used=browser_name,
                        error="no audio files after extract",
                    )

                # Record success in ignores.txt for the existing counter.
                self._append_ignore(iid, band, title)

                return BrowserDownloadOutcome(
                    success=True, item_id=iid, band_name=band, item_title=title,
                    folder=album_dir, bytes_written=size,
                    duration_seconds=datetime.now(timezone.utc).timestamp() - start_ts,
                    browser_used=browser_name,
                )

        except Exception as e:
            return BrowserDownloadOutcome(
                success=False, item_id=iid, band_name=band, item_title=title,
                folder=None, bytes_written=0,
                duration_seconds=datetime.now(timezone.utc).timestamp() - start_ts,
                browser_used=browser_name,
                error=f"{type(e).__name__}: {e}",
            )

    def _extract_zip(self, zip_path: Path, dest: Path) -> None:
        with ZipFile(zip_path) as z:
            for member in z.namelist():
                clean_name = "/".join(
                    clean_path_component(p) for p in member.split("/") if p
                )
                target = dest / clean_name
                target.parent.mkdir(parents=True, exist_ok=True)
                with z.open(member) as src, target.open("wb") as out:
                    shutil.copyfileobj(src, out)

    def _append_ignore(self, item_id: int, band: str, title: str) -> None:
        line = f"{item_id}  # {band} / {title}\n"
        # Try /config first (bandcampsync convention). If RO, fall
        # back to /state.
        candidates = [self.config_dir / "ignores.txt"]
        if self.state_dir is not None:
            candidates.append(self.state_dir / "ignores_warden.txt")
        for path in candidates:
            try:
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as f:
                    f.write(line)
                return
            except Exception as e:
                log.warning("append %s failed: %s", path, e)
                continue
        log.error("could not append ignore for item %d", item_id)


def _looks_like_zip(p: Path) -> bool:
    try:
        with p.open("rb") as f:
            return f.read(2) == b"PK"
    except Exception:
        return False
