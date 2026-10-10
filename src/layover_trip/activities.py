"""Temporal Activities: every piece of non-deterministic I/O the trip-
monitoring workflow needs (HTTP calls to the Milestone 7 prediction
service, database reads/writes, sending notifications) lives here, never
in `workflow.py` -- Temporal workflow code must be deterministic (it
gets replayed from history after a worker restart), and only Activities
are allowed to do real-world I/O. See DECISIONS.md.
"""
import dataclasses
from typing import Optional

import requests
from temporalio import activity

from layover_trip import db, notify, risk

PREDICTION_SERVICE_URL = "http://localhost:8000"
DEFAULT_MIN_CONNECTION_MINUTES = 45


@dataclasses.dataclass
class LegAssessmentInput:
    trip_leg_id: str
    flight_id: str
    next_leg_sched_dep_epoch: Optional[float] = None
    sched_arr_epoch: Optional[float] = None
    min_connection_min: Optional[int] = None


@dataclasses.dataclass
class LegAssessmentResult:
    trip_leg_id: str
    flight_id: str
    predicted_delay: Optional[float] = None
    p_miss: Optional[float] = None
    model_version: Optional[str] = None
    explanation: list = dataclasses.field(default_factory=list)
    available: bool = True


@activity.defn
async def fetch_leg_assessment(inp: LegAssessmentInput) -> LegAssessmentResult:
    """Calls the real Milestone 7 prediction service for this leg's
    current risk, computes this *connection's* p_miss from it (Milestone
    7's own `p_miss` is null -- it's a single-flight endpoint with no
    notion of a connection; this activity is the actual home for a
    connection-specific probability, matching the data model's
    `RiskAssessment.p_miss` being keyed by `trip_leg_id`, not by flight),
    and persists the assessment so it has a durable history independent
    of the workflow's own (replayable, but not externally queryable by
    SQL) state.
    """
    resp = requests.get(f"{PREDICTION_SERVICE_URL}/flights/{inp.flight_id}/risk", timeout=10)
    if resp.status_code == 404:
        return LegAssessmentResult(
            trip_leg_id=inp.trip_leg_id, flight_id=inp.flight_id, available=False
        )
    resp.raise_for_status()
    body = resp.json()

    p_miss = None
    if inp.next_leg_sched_dep_epoch is not None and inp.sched_arr_epoch is not None:
        buffer_minutes = (inp.next_leg_sched_dep_epoch - inp.sched_arr_epoch) / 60 - (
            inp.min_connection_min or DEFAULT_MIN_CONNECTION_MINUTES
        )
        p_miss = risk.connection_p_miss(body["predicted_delay"], body["model_name"], buffer_minutes)

    conn = db.connect()
    try:
        db.insert_risk_assessment(
            conn,
            inp.trip_leg_id,
            body["ts"],
            body["model_version"],
            p_miss,
            body["predicted_delay"],
            body["explanation"],
        )
    finally:
        conn.close()

    return LegAssessmentResult(
        trip_leg_id=inp.trip_leg_id,
        flight_id=inp.flight_id,
        predicted_delay=body["predicted_delay"],
        p_miss=p_miss,
        model_version=body["model_version"],
        explanation=body["explanation"],
    )


@dataclasses.dataclass
class AlertInput:
    trip_id: str
    dedup_key: str
    to_email: str
    push_subscription: dict
    subject: str
    body: str


@activity.defn
async def send_alert(inp: AlertInput) -> dict:
    """The actual exactly-once guarantee (Temporal's own retries are
    only at-least-once -- see DECISIONS.md): reserve `dedup_key` in the
    real `alerts` table first; only the attempt that wins the reservation
    (a brand-new key, or a worker retrying its own still-uncommitted
    attempt) sends anything. `dedup_key` is derived from durable workflow
    state (see `workflow.py`), so a replay recomputes the identical key
    rather than minting a fresh one that would defeat this entirely.
    """
    alert_id = f"alert-{inp.dedup_key}"
    conn = db.connect()
    try:
        reserved = db.try_record_alert(
            conn, alert_id, inp.trip_id, inp.dedup_key, "email+push", "sent"
        )
    finally:
        conn.close()

    if not reserved:
        return {"sent": False, "reason": "already sent (dedup_key reserved by a prior attempt)"}

    results = {"email": notify.send_email(inp.to_email, inp.subject, inp.body)}
    if inp.push_subscription:
        results["push"] = notify.send_push(inp.push_subscription, inp.body)
    return {"sent": True, "results": results}


@dataclasses.dataclass
class MarkTripStatusInput:
    trip_id: str
    status: str


@activity.defn
async def mark_trip_status(inp: MarkTripStatusInput) -> None:
    conn = db.connect()
    try:
        db.update_trip_status(conn, inp.trip_id, inp.status)
    finally:
        conn.close()
