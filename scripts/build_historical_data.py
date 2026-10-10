#!/usr/bin/env python3
"""Milestone 2 pipeline: turns downloaded BTS zips into cleaned, UTC-
timestamped Parquet (the Flight entity) plus a Rotation table built from
tail numbers. Run scripts/fetch_historical_data.py first.

Reads each month's CSV directly out of its zip (no disk extraction needed)
to avoid doubling the ~2GB of raw data on disk.
"""
import logging
import sys
import time
import zipfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_data.airports import build_airport_tz_map  # noqa: E402
from layover_data.clean import clean_month  # noqa: E402
from layover_data.rotations import build_rotations  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"


def _csv_member(zf: zipfile.ZipFile) -> str:
    csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if len(csv_names) != 1:
        raise ValueError(f"expected exactly one CSV in zip, found {csv_names}")
    return csv_names[0]


def clean_all_months(zip_paths: list, airport_tz_map: dict) -> pd.DataFrame:
    frames = []
    for zip_path in sorted(zip_paths):
        t0 = time.time()
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(_csv_member(zf)) as f:
                month_df = clean_month(f, airport_tz_map)
        frames.append(month_df)
        logger.info(
            "%s: %d flights cleaned in %.1fs",
            zip_path.name,
            len(month_df),
            time.time() - t0,
        )
    return pd.concat(frames, ignore_index=True)


def main():
    t_start = time.time()

    airport_tz_map = build_airport_tz_map(RAW_DIR / "reference" / "airports.csv")
    logger.info("loaded timezones for %d US airports", len(airport_tz_map))

    zip_paths = sorted((RAW_DIR / "bts").glob("*.zip"))
    if not zip_paths:
        raise SystemExit("no BTS zip files found in data/raw/bts -- run fetch_historical_data.py")

    flights = clean_all_months(zip_paths, airport_tz_map)
    logger.info("total cleaned flights: %d", len(flights))

    rotations = build_rotations(flights)
    logger.info("total rotations (tail_number x day): %d", len(rotations))

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    flights_path = PROCESSED_DIR / "flights.parquet"
    rotations_path = PROCESSED_DIR / "rotations.parquet"

    flights.to_parquet(flights_path, index=False)
    # flight_ids is a list column -- pyarrow handles it natively, but
    # explode would lose the ordering that *is* the point of a rotation,
    # so it's kept as a list in the parquet file as-is.
    rotations.to_parquet(rotations_path, index=False)

    elapsed = time.time() - t_start
    logger.info(
        "wrote %s (%.1f MB) and %s (%.1f MB) in %.1fs",
        flights_path.name,
        flights_path.stat().st_size / 1e6,
        rotations_path.name,
        rotations_path.stat().st_size / 1e6,
        elapsed,
    )

    with_tail = flights["tail_number"].notna().sum()
    multi_leg_rotations = (rotations["n_legs"] > 1).sum()
    total_chain_breaks = rotations["chain_breaks"].sum()
    print("\n--- summary ---")
    print(f"flights: {len(flights)}")
    print(f"date range: {flights['fl_date'].min()} to {flights['fl_date'].max()}")
    print(f"flights with tail_number: {with_tail} ({with_tail / len(flights):.1%})")
    print(f"rotations: {len(rotations)}")
    print(
        f"multi-leg rotations: {multi_leg_rotations} "
        f"({multi_leg_rotations / len(rotations):.1%})"
    )
    print(f"chain breaks (gap/ferry between legs): {total_chain_breaks}")
    print(f"total pipeline time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
