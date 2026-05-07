"""bandcamp-warden patch for bandcampsync's download.py.

Drop-in replacement of bandcampsync==0.7.0's download.py with HTTP Range
resume on stall, so a TCP stall at 70% of a 1GB album doesn't waste 70%
of bandwidth — we reconnect with `Range: bytes=N-` and continue from
where we left off.

WHY: bandcampsync's vanilla downloader has no resume. Bandcamp's edge
sometimes briefly stops sending bytes mid-stream. Default curl_cffi
aborts after 30s of <1B/s with curl-error-28; even with patient
timeouts (LOW_SPEED_TIME=300) we either hang forever or the connection
goes truly dead. Browsers don't have this problem because they keep
the same TCP socket alive; we can't replicate that, but we CAN do the
next best thing: detect stalls fast, abandon the dead socket, open a
new one with a Range header pointing at our current byte offset, and
keep going.

This file is bind-mounted by the warden sidecar over
/usr/local/lib/python3.13/dist-packages/bandcampsync/download.py.
"""

import math
import os
import shutil
import time
from zipfile import ZipFile
from curl_cffi import requests
from curl_cffi.const import CurlOpt
from .logger import get_logger


log = get_logger("download")


# Tunables — environment overrides for ops debugging.
def _env_int(name: str, default: int) -> int:
    try:
        v = os.environ.get(name)
        return int(v) if v is not None else default
    except ValueError:
        return default


# Abort the connection at 60s of <1KB/s sustained — fast enough to
# trigger a Range resume, slow enough to tolerate normal Bandcamp
# slow-start patterns.
_LOW_SPEED_TIME = _env_int("WARDEN_DOWNLOAD_LOW_SPEED_TIME", 60)
_LOW_SPEED_LIMIT = _env_int("WARDEN_DOWNLOAD_LOW_SPEED_LIMIT", 1024)
# Max number of resume attempts per album. Each resume picks up where
# we left off, so this bounds total wall-clock per album, not bandwidth.
_MAX_RESUMES = _env_int("WARDEN_DOWNLOAD_MAX_RESUMES", 30)
# Wait between a stall and the resume request.
_RESUME_DELAY_SECONDS = _env_int("WARDEN_DOWNLOAD_RESUME_DELAY", 10)


_PATIENT_OPTIONS = {
    CurlOpt.LOW_SPEED_TIME: _LOW_SPEED_TIME,
    CurlOpt.LOW_SPEED_LIMIT: _LOW_SPEED_LIMIT,
}


def mask_sig(url):
    if "&sig=" not in url:
        return url
    url_parts = url.split("&")
    for i, url_part in enumerate(url_parts):
        if url_part[:4] == "sig=":
            url_parts[i] = "sig=[masked]"
        elif url_part[:6] == "token=":
            url_parts[i] = "token=[masked]"
    return "&".join(url_parts)


class DownloadBadStatusCode(ValueError):
    pass


class DownloadInvalidContentType(ValueError):
    pass


def _open_stream(session, url, byte_offset):
    """Open a streaming GET, optionally with Range: bytes=N-. Returns
    the response. Caller is responsible for closing it."""
    headers = {}
    if byte_offset > 0:
        headers["Range"] = f"bytes={byte_offset}-"
    return session.get(url, stream=True, headers=headers)


def download_file(
    url,
    target,
    mode="wb",
    chunk_size=8192,
    logevery=10,
    disallow_content_type="text/html",
):
    """Stream a download to an open target file handle in chunks.

    PATCHED for bandcamp-warden: tolerates and recovers from
    curl_cffi.RequestException via HTTP Range resume.
    """
    text = True if "t" in mode else False
    bytes_total = 0  # what we've successfully written to target
    last_log = 0
    content_length_first_seen = 0
    resumes = 0

    log.info(
        "[warden patch] download_file: low_speed_time=%ds, "
        "low_speed_limit=%dB/s, max_resumes=%d",
        _LOW_SPEED_TIME, _LOW_SPEED_LIMIT, _MAX_RESUMES,
    )

    while True:
        with requests.Session(
            impersonate="chrome",
            curl_options=_PATIENT_OPTIONS,
        ) as session:
            resume_attempt = bytes_total > 0
            try:
                r = _open_stream(session, url, bytes_total)
            except Exception as e:
                # Connection error before stream even started.
                if resumes >= _MAX_RESUMES:
                    raise
                resumes += 1
                log.warning(
                    "[warden patch] connect failed (%s), resume %d/%d "
                    "in %ds at offset %d",
                    type(e).__name__, resumes, _MAX_RESUMES,
                    _RESUME_DELAY_SECONDS, bytes_total,
                )
                time.sleep(_RESUME_DELAY_SECONDS)
                continue

            try:
                expected_status = 206 if resume_attempt else 200
                if r.status_code != expected_status:
                    # 200 on a Range request means server ignored Range —
                    # fall back to seeking into the response stream
                    # (rare but possible). 416 means our offset is past
                    # the end, treat as success only if we actually
                    # finished previously.
                    if r.status_code == 416 and content_length_first_seen and bytes_total >= content_length_first_seen:
                        log.info(
                            "[warden patch] 416 Range Not Satisfiable but "
                            "we already wrote full content (%d bytes) — "
                            "treating as success",
                            bytes_total,
                        )
                        return True
                    if resume_attempt and r.status_code == 200:
                        log.warning(
                            "[warden patch] server ignored Range header, "
                            "got 200 — discarding our %d bytes and "
                            "starting over",
                            bytes_total,
                        )
                        target.seek(0)
                        target.truncate()
                        bytes_total = 0
                    elif r.status_code != 200:
                        raise DownloadBadStatusCode(
                            f"Got status {r.status_code} on attempt "
                            f"(resume={resume_attempt}, offset={bytes_total})"
                        )

                try:
                    content_type = r.headers.get("Content-Type", "")
                except (ValueError, KeyError):
                    content_type = ""
                content_type_parts = content_type.split(";")
                major_content_type = content_type_parts[0].strip()
                if not resume_attempt and major_content_type == disallow_content_type:
                    raise DownloadInvalidContentType(
                        f"Invalid content type: {major_content_type}"
                    )

                # Determine total expected size. On the initial 200,
                # Content-Length is the full thing. On a 206, it's the
                # remaining bytes — sum with bytes_total to get total.
                try:
                    cl = int(r.headers.get("Content-Length", "0"))
                except (ValueError, KeyError):
                    cl = 0
                if cl > 0:
                    if resume_attempt:
                        total_expected = bytes_total + cl
                    else:
                        total_expected = cl
                    if not content_length_first_seen:
                        content_length_first_seen = total_expected
                else:
                    total_expected = content_length_first_seen

                # Stream chunks until done or stall.
                got_any_chunk_this_attempt = False
                try:
                    for chunk in r.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue
                        got_any_chunk_this_attempt = True
                        if text:
                            chunk = chunk.decode()
                        target.write(chunk)
                        bytes_total += len(chunk)
                        if total_expected > 0 and logevery > 0:
                            percent_complete = math.floor(
                                (bytes_total / total_expected) * 100
                            )
                            if (
                                percent_complete % logevery == 0
                                and percent_complete > last_log
                            ):
                                log.info(
                                    f"Downloading {mask_sig(url)}: {percent_complete}%"
                                )
                                last_log = percent_complete
                except requests.exceptions.RequestException as e:
                    # Stall or connection drop. If we got at least one
                    # chunk this attempt, we made forward progress and
                    # the resume counter shouldn't tick up too fast;
                    # if we got nothing, count it harder.
                    if resumes >= _MAX_RESUMES:
                        log.error(
                            "[warden patch] giving up after %d resumes at "
                            "offset %d/%d (%s)",
                            resumes, bytes_total, total_expected,
                            type(e).__name__,
                        )
                        raise
                    resumes += 1
                    log.warning(
                        "[warden patch] mid-stream %s at offset %d/%d "
                        "after writing %s this attempt — resume %d/%d "
                        "in %ds",
                        type(e).__name__,
                        bytes_total,
                        total_expected,
                        "some" if got_any_chunk_this_attempt else "ZERO bytes",
                        resumes,
                        _MAX_RESUMES,
                        _RESUME_DELAY_SECONDS,
                    )
                    time.sleep(_RESUME_DELAY_SECONDS)
                    continue
            finally:
                try:
                    r.close()
                except Exception:
                    pass

            # Stream finished without exception.
            if total_expected > 0 and bytes_total < total_expected:
                # Hit EOF before content_length — treat as a stall and
                # try resume.
                if resumes >= _MAX_RESUMES:
                    raise DownloadBadStatusCode(
                        f"Stream EOF early at {bytes_total}/{total_expected}"
                    )
                resumes += 1
                log.warning(
                    "[warden patch] stream EOF at %d/%d before "
                    "Content-Length — resume %d/%d",
                    bytes_total, total_expected, resumes, _MAX_RESUMES,
                )
                time.sleep(_RESUME_DELAY_SECONDS)
                continue

            log.info(
                "[warden patch] download complete: %d bytes, %d resumes",
                bytes_total, resumes,
            )
            return True


def is_zip_file(file_path):
    try:
        with ZipFile(file_path) as z:
            z.infolist()
        return True
    except Exception:
        return False


def unzip_file(decompress_from, decompress_to):
    with ZipFile(decompress_from) as z:
        z.extractall(decompress_to)
    return True


def move_file(src, dst):
    return shutil.move(src, dst)


def copy_file(src, dst):
    return shutil.copyfile(src, dst)
