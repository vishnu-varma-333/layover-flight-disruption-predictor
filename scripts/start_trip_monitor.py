#!/usr/bin/env python3
"""Milestone 8: creates a real Trip (two real connecting flights from
the Milestone 2 dataset, the same 2026-07-15 demo day Milestones 5-7
already use) and starts its `TripMonitorWorkflow` against the real
Temporal server.

The demo connection is real, not scripted to look dramatic: United 539
(ORD->DFW) departed 115 minutes late in the actual historical data;
American 1402 (DFW->AUS) was scheduled to leave DFW only 52 minutes
after 539's scheduled arrival. A genuine, already-tight connection that
real data happens to put at real risk.

Usage: start_trip_monitor.py [--check-interval-seconds N] [--to-email ADDR]
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

from temporalio.client import Client

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_trip.trip_service import create_and_start_trip, load_flights_df  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent

LEG_1_FLIGHT_ID = "2026-07-15-UA539-ORD-DFW"
LEG_2_FLIGHT_ID = "2026-07-15-AA1402-DFW-AUS"
MIN_CONNECTION_MINUTES = 45  # a documented demo-scope simplification --
# the spec itself anticipates "a manual table for demo airports" rather
# than a real minimum-connection-time data source; see DECISIONS.md.


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-interval-seconds", type=int, default=20)
    parser.add_argument("--to-email", default="traveler@example.com")
    parser.add_argument("--trip-id", default=None)
    args = parser.parse_args()

    flights_df = load_flights_df(REPO_ROOT)
    client = await Client.connect("localhost:7233", namespace="default")
    trip_id = await create_and_start_trip(
        client,
        flights_df,
        user_id="demo-user",
        to_email=args.to_email,
        legs=[
            {"flight_id": LEG_1_FLIGHT_ID, "min_connection_min": MIN_CONNECTION_MINUTES},
            {"flight_id": LEG_2_FLIGHT_ID, "min_connection_min": None},
        ],
        check_interval_seconds=args.check_interval_seconds,
        trip_id=args.trip_id,
    )
    logger.info("trip %s created and monitoring started", trip_id)
    print(trip_id)


if __name__ == "__main__":
    asyncio.run(main())
