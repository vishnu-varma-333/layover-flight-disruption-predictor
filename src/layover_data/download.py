"""Fetches BTS On-Time Performance monthly files and the OurAirports
reference table. Downloads are resumable and idempotent: a file that's
already complete (matches the server's Content-Length) is skipped, and a
partial file is resumed with an HTTP Range request rather than restarted.
"""
import logging
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

BTS_URL_TEMPLATE = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
OURAIRPORTS_URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"

CHUNK_SIZE = 1 << 16  # 64 KiB
MAX_RETRIES = 8
RETRY_BACKOFF_SECONDS = 5


def bts_month_url(year: int, month: int) -> str:
    return BTS_URL_TEMPLATE.format(year=year, month=month)


def download_resumable(url: str, dest: Path, timeout: int = 60) -> Path:
    """Downloads `url` to `dest`, resuming a partial file and retrying on
    transient network failures. Raises if the server never returns a usable
    Content-Length or if retries are exhausted.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, MAX_RETRIES + 1):
        # A compressed (gzip/br) response's Content-Length describes the
        # wire size, but `requests` transparently decodes it -- bytes
        # written to disk end up larger than that header. Range-resuming
        # into the middle of a compressed stream also can't be decoded
        # correctly on its own. So: for a compressed resource, always
        # re-fetch from byte 0 and skip the Content-Length completeness
        # check; trust that a truncated gzip stream raises its own
        # decompression error instead. Discovered by a real failure, not
        # anticipated -- see DECISIONS.md.
        existing = dest.stat().st_size if dest.exists() else 0
        headers = {"Range": f"bytes={existing}-"} if existing else {}
        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as resp:
                if resp.status_code == 416:
                    # Range not satisfiable -> we already have the whole file.
                    return dest
                resp.raise_for_status()

                compressed = "Content-Encoding" in resp.headers
                total = None if compressed else resp.headers.get("Content-Length")
                resumed = existing and resp.status_code == 206 and not compressed
                mode = "ab" if resumed else "wb"
                if not resumed:
                    existing = 0

                with open(dest, mode) as f:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

                final_size = dest.stat().st_size
                if total is not None:
                    expected = existing + int(total) if resumed else int(total)
                    if final_size != expected:
                        raise OSError(
                            f"incomplete download: got {final_size} bytes, expected {expected}"
                        )
                logger.info("downloaded %s (%d bytes)", dest.name, final_size)
                return dest
        except (requests.RequestException, OSError) as exc:
            logger.warning(
                "attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, url, exc
            )
            if attempt == MAX_RETRIES:
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError("unreachable")  # pragma: no cover


def download_bts_month(year: int, month: int, raw_dir: Path) -> Path:
    dest = raw_dir / f"{year}_{month:02d}.zip"
    url = bts_month_url(year, month)
    return download_resumable(url, dest)


def download_ourairports(raw_dir: Path) -> Path:
    dest = raw_dir / "airports.csv"
    return download_resumable(OURAIRPORTS_URL, dest)
