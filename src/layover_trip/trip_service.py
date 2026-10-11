"""Shared trip-creation logic: turns a list of real flight_ids (plus
per-leg minimum connection times) into a persisted Trip + TripLegs and a
running `TripMonitorWorkflow`. One implementation of "what does adding a
trip actually do," used by both `scripts/start_trip_monitor.py`
(Milestone 8's demo/kill-test script, a fixed two-leg connection) and
Milestone 9's web API (an arbitrary number of real legs a user picked).
"""
import uuid

import pandas as pd
from temporalio.exceptions import WorkflowAlreadyStartedError

from layover_trip import db, workflow

TASK_QUEUE = "trip-monitoring"


def load_leg_schedule(flights_df: pd.DataFrame, flight_id: str) -> dict:
    row = flights_df[flights_df["flight_id"] == flight_id].iloc[0]
    return {
        "sched_dep_epoch": row["sched_dep_utc"].timestamp(),
        "sched_arr_epoch": row["sched_arr_utc"].timestamp(),
    }


async def create_and_start_trip(
    temporal_client,
    flights_df: pd.DataFrame,
    user_id: str,
    to_email: str,
    legs: list,
    check_interval_seconds: int = 900,
    trip_id: str = None,
) -> str:
    """`legs`: ordered list of {"flight_id", "min_connection_min"} dicts,
    already in trip order (first leg to last). `next_leg_sched_dep_epoch`
    for each workflow leg is derived automatically from the *following*
    leg's own schedule -- the caller only needs to know the flights, not
    duplicate the connection-buffer wiring Milestone 8 already built.
    """
    trip_id = trip_id or f"trip-{uuid.uuid4().hex[:8]}"
    schedules = [load_leg_schedule(flights_df, leg["flight_id"]) for leg in legs]

    conn = db.connect()
    try:
        db.create_trip(conn, trip_id, user_id=user_id, to_email=to_email, legs=legs)
        db_legs = db.get_trip_legs(conn, trip_id)
    finally:
        conn.close()

    workflow_legs = []
    for i, (db_leg, sched) in enumerate(zip(db_legs, schedules)):
        next_dep = schedules[i + 1]["sched_dep_epoch"] if i + 1 < len(schedules) else None
        workflow_legs.append(
            workflow.TripLegInput(
                trip_leg_id=db_leg["id"],
                flight_id=db_leg["flight_id"],
                sched_arr_epoch=sched["sched_arr_epoch"],
                next_leg_sched_dep_epoch=next_dep,
                min_connection_min=db_leg["min_connection_min"],
            )
        )

    workflow_input = workflow.TripMonitorInput(
        trip_id=trip_id,
        to_email=to_email,
        check_interval_seconds=check_interval_seconds,
        legs=workflow_legs,
    )

    try:
        await temporal_client.start_workflow(
            workflow.TripMonitorWorkflow.run,
            workflow_input,
            id=f"trip-monitor-{trip_id}",
            task_queue=TASK_QUEUE,
        )
    except WorkflowAlreadyStartedError:
        pass

    return trip_id


def load_flights_df(repo_root) -> pd.DataFrame:
    return pd.read_parquet(repo_root / "data" / "processed" / "flights.parquet")
