#!/usr/bin/env python3
"""Downloads the 12 months of BTS On-Time Performance data (Aug 2025 -
Jul 2026 -- the most recent full year published as of 2026-10) plus the
OurAirports reference table, into data/raw/. Safe to interrupt and rerun:
download_resumable() picks up where it left off.
"""
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_data.download import download_bts_month, download_ourairports  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"

# Aug 2025 through Jul 2026 -- see DECISIONS.md for why this window.
MONTHS = [(2025, m) for m in range(8, 13)] + [(2026, m) for m in range(1, 8)]


def main():
    download_ourairports(RAW_DIR / "reference")
    for year, month in MONTHS:
        download_bts_month(year, month, RAW_DIR / "bts")
    print("all downloads complete")


if __name__ == "__main__":
    main()
