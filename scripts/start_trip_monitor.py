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
import uuid
from pathlib import Path

import pandas as pd
from temporalio.client import Client
from temporalio.exceptions import WorkflowAlreadyStartedError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_trip import db, workflow  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
TASK_QUEUE = "trip-monitoring"

LEG_1_FLIGHT_ID = "2026-07-15-UA539-ORD-DFW"
LEG_2_FLIGHT_ID = "2026-07-15-AA1402-DFW-AUS"
MIN_CONNECTION_MINUTES = 45  # a documented demo-scope simplification --
# the spec itself anticipates "a manual table for demo airports" rather
# than a real minimum-connection-time data source; see DECISIONS.md.


def load_leg_schedule(flight_id: str) -> dict:
    flights = pd.read_parquet(REPO_ROOT / "data" / "processed" / "flights.parquet")
    row = flights[flights["flight_id"] == flight_id].iloc[0]
    return {
        "sched_dep_epoch": row["sched_dep_utc"].timestamp(),
        "sched_arr_epoch": row["sched_arr_utc"].timestamp(),
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check-interval-seconds", type=int, default=20)
    parser.add_argument("--to-email", default="traveler@example.com")
    parser.add_argument("--trip-id", default=None)
    args = parser.parse_args()

    trip_id = args.trip_id or f"trip-{uuid.uuid4().hex[:8]}"
    leg1 = load_leg_schedule(LEG_1_FLIGHT_ID)
    leg2 = load_leg_schedule(LEG_2_FLIGHT_ID)

    conn = db.connect()
    try:
        db.create_trip(
            conn,
            trip_id,
            user_id="demo-user",
            legs=[
                {"flight_id": LEG_1_FLIGHT_ID, "min_connection_min": MIN_CONNECTION_MINUTES},
                {"flight_id": LEG_2_FLIGHT_ID, "min_connection_min": None},
            ],
        )
        legs = db.get_trip_legs(conn, trip_id)
    finally:
        conn.close()
    logger.info("created trip %s with %d legs", trip_id, len(legs))

    workflow_input = workflow.TripMonitorInput(
        trip_id=trip_id,
        to_email=args.to_email,
        check_interval_seconds=args.check_interval_seconds,
        legs=[
            workflow.TripLegInput(
                trip_leg_id=legs[0]["id"],
                flight_id=legs[0]["flight_id"],
                sched_arr_epoch=leg1["sched_arr_epoch"],
                next_leg_sched_dep_epoch=leg2["sched_dep_epoch"],
                min_connection_min=legs[0]["min_connection_min"],
            ),
            workflow.TripLegInput(
                trip_leg_id=legs[1]["id"],
                flight_id=legs[1]["flight_id"],
                sched_arr_epoch=leg2["sched_arr_epoch"],
            ),
        ],
    )

    client = await Client.connect("localhost:7233", namespace="default")
    try:
        handle = await client.start_workflow(
            workflow.TripMonitorWorkflow.run,
            workflow_input,
            id=f"trip-monitor-{trip_id}",
            task_queue=TASK_QUEUE,
        )
        logger.info("started workflow %s (run_id=%s)", handle.id, handle.result_run_id)
    except WorkflowAlreadyStartedError:
        logger.info("workflow trip-monitor-%s is already running", trip_id)

    print(trip_id)


if __name__ == "__main__":
    asyncio.run(main())
