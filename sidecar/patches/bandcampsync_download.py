"""bandcamp-warden patch for bandcampsync's download.py.

Drop-in replacement of bandcampsync==0.7.0's download.py. Identical behaviour
EXCEPT for one change: the Bandcamp file download uses an explicit
curl_cffi Session with `LOW_SPEED_TIME=300` / `LOW_SPEED_LIMIT=1024`, so
that brief Bandcamp-side stream-stalls don't trigger curl-error-28
("Operation too slow, less than 1 byte/sec for 30 seconds").

Without this patch our daily runs crashed 4× per day on big Vaporwave
albums when Bandcamp's edge briefly stopped sending bytes; auto-retry
recovered each time but at the cost of partial progress and Telegram
noise. The browser doesn't have this stall detection at all and the
user could download manually with no problems, which proved this is a
client-side timeout, not a server-side throttle.

This file is mounted into the bandcampsync container at
/usr/local/lib/python3.13/dist-packages/bandcampsync/download.py
by the warden sidecar.
"""

import math
import shutil
from zipfile import ZipFile
from curl_cffi import requests
from curl_cffi.const import CurlOpt
from .logger import get_logger


log = get_logger("download")

# Patient curl options — tolerate up to 5 minutes of <1KB/s before aborting
# the stream. Bandcamp's edge sometimes pauses bursts mid-album; we'd
# rather wait it out than reset.
_WARDEN_PATIENT_CURL_OPTIONS = {
    CurlOpt.LOW_SPEED_TIME: 300,
    CurlOpt.LOW_SPEED_LIMIT: 1024,
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


def download_file(
    url,
    target,
    mode="wb",
    chunk_size=8192,
    logevery=10,
    disallow_content_type="text/html",
):
    """
    Attempts to stream a download to an open target file handle in chunks. If the
    request returns a disallowed content type then return a failed state with the
    response content.

    PATCHED for bandcamp-warden: uses explicit Session with patient
    LOW_SPEED options so brief mid-stream stalls don't kill the run.
    """
    text = True if "t" in mode else False
    data_streamed = 0
    last_log = 0
    log.info("[warden patch] download_file using patient curl options")
    with requests.Session(
        impersonate="chrome",
        curl_options=_WARDEN_PATIENT_CURL_OPTIONS,
    ) as session:
        r = session.get(url, stream=True)
        try:
            if r.status_code != 200:
                raise DownloadBadStatusCode(f"Got non-200 status code: {r.status_code}")
            try:
                content_type = r.headers.get("Content-Type", "")
            except (ValueError, KeyError):
                content_type = ""
            content_type_parts = content_type.split(";")
            major_content_type = content_type_parts[0].strip()
            if major_content_type == disallow_content_type:
                raise DownloadInvalidContentType(
                    f"Invalid content type: {major_content_type}"
                )
            try:
                content_length = int(r.headers.get("Content-Length", "0"))
            except (ValueError, KeyError):
                content_length = 0
            for chunk in r.iter_content(chunk_size=chunk_size):
                data_streamed += len(chunk)
                if text:
                    chunk = chunk.decode()
                target.write(chunk)
                if content_length > 0 and logevery > 0:
                    percent_complete = math.floor((data_streamed / content_length) * 100)
                    if percent_complete % logevery == 0 and percent_complete > last_log:
                        log.info(f"Downloading {mask_sig(url)}: {percent_complete}%")
                        last_log = percent_complete
        finally:
            r.close()
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
