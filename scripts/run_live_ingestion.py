#!/usr/bin/env python3
"""Milestone 5: runs the three live ingestors (OpenSky positions, NOAA
weather, FAA airport status) concurrently, publishing real current data
into Kafka's `live.events` topic.

Usage: run_live_ingestion.py [--duration SECONDS] [--opensky-interval S]
[--weather-interval S] [--status-interval S]

With no --duration, runs forever (the real deployment mode). A bounded
--duration is for live verification -- proving real data actually flows
end-to-end -- without running indefinitely or, for OpenSky specifically,
spending more of the shared anonymous daily credit budget than a short
verification needs.
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_ingest import airport_status, opensky, weather  # noqa: E402
from layover_ingest.kafka_io import make_producer  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duration", type=float, default=None, help="seconds; default: run forever"
    )
    parser.add_argument("--opensky-interval", type=float, default=3600)
    parser.add_argument("--weather-interval", type=float, default=900)
    parser.add_argument("--status-interval", type=float, default=300)
    args = parser.parse_args()

    producer = make_producer()
    try:
        await asyncio.gather(
            opensky.run(
                producer,
                duration_seconds=args.duration,
                cycle_interval_seconds=args.opensky_interval,
            ),
            weather.run(
                producer, duration_seconds=args.duration, interval_seconds=args.weather_interval
            ),
            airport_status.run(
                producer, duration_seconds=args.duration, interval_seconds=args.status_interval
            ),
        )
    finally:
        producer.flush(30)


if __name__ == "__main__":
    asyncio.run(main())
