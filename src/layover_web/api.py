"""Milestone 9: the backend API Next.js calls server-side (never directly
from the browser -- see DECISIONS.md for why the web app owns the user
session and this service stays an internal, unauthenticated-but-trusted
bridge). Reuses every real piece already built: Temporal for starting/
signaling trip monitors (Milestone 8), TimescaleDB for trip/risk
persistence (Milestone 8), the real VAPID identity for Web Push
(Milestone 8), and the Milestone 2 historical dataset for flight lookup
and the aircraft-chain view.
"""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from temporalio.client import Client

from layover_trip import db
from layover_trip.notify import vapid_public_key_b64url
from layover_trip.trip_service import create_and_start_trip, load_flights_df
from layover_web.flights import get_aircraft_chain, get_flight, search_flight

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
# Overridable so the same image runs in a container, where these are
# service names, not localhost -- see DECISIONS.md (Milestone 10).
PREDICTION_SERVICE_URL = os.environ.get("PREDICTION_SERVICE_URL", "http://localhost:8000")
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
WEB_ORIGIN = os.environ.get("WEB_ORIGIN", "http://localhost:3001")
state = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["temporal"] = await Client.connect(TEMPORAL_ADDRESS, namespace="default")
    state["flights"] = load_flights_df(REPO_ROOT)
    conn = db.connect()
    db.apply_schema(conn)
    conn.close()
    logger.info("layover_web API ready")
    yield
    state.clear()


app = FastAPI(title="Layover web API", lifespan=lifespan)
# Only Next.js's own dev/prod origin ever calls this -- never the browser
# directly -- but CORS is still enabled narrowly for local development
# convenience (e.g. hitting this API directly from a browser tab while
# debugging) rather than left wide open.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[WEB_ORIGIN],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/vapid-public-key")
def get_vapid_public_key():
    return {"key": vapid_public_key_b64url()}


@app.get("/flights/search")
def flights_search(carrier: str, flight_number: int, fl_date: str):
    results = search_flight(carrier, flight_number, fl_date)
    if not results:
        raise HTTPException(status_code=404, detail="no flight found for that carrier/number/date")
    return results


@app.get("/flights/{flight_id}")
def flight_detail(flight_id: str):
    flight = get_flight(flight_id)
    if flight is None:
        raise HTTPException(status_code=404, detail="unknown flight_id")

    risk = None
    try:
        resp = requests.get(f"{PREDICTION_SERVICE_URL}/flights/{flight_id}/risk", timeout=5)
        if resp.status_code == 200:
            risk = resp.json()
    except requests.RequestException:
        pass  # the prediction service covers only the live demo day; a
        # flight outside it (or the service being down) just means no
        # risk to show, not an error for this page

    return {"flight": flight, "chain": get_aircraft_chain(flight_id), "risk": risk}


class LegIn(BaseModel):
    flight_id: str
    min_connection_min: Optional[int] = None


class TripIn(BaseModel):
    user_id: str
    to_email: str
    legs: list[LegIn]
    check_interval_seconds: int = 900


@app.post("/trips")
async def create_trip(body: TripIn):
    if len(body.legs) < 1:
        raise HTTPException(status_code=400, detail="a trip needs at least one leg")
    trip_id = await create_and_start_trip(
        state["temporal"],
        state["flights"],
        user_id=body.user_id,
        to_email=body.to_email,
        legs=[leg.model_dump() for leg in body.legs],
        check_interval_seconds=body.check_interval_seconds,
    )
    return {"trip_id": trip_id}


@app.get("/trips")
def list_trips(user_id: str):
    conn = db.connect()
    try:
        return db.list_trips(conn, user_id)
    finally:
        conn.close()


@app.get("/trips/{trip_id}")
def trip_detail(trip_id: str):
    conn = db.connect()
    try:
        trip = db.get_trip(conn, trip_id)
        if trip is None:
            raise HTTPException(status_code=404, detail="unknown trip")
        legs = db.get_trip_legs(conn, trip_id)
        for leg in legs:
            leg["flight"] = get_flight(leg["flight_id"])
            leg["risk"] = db.latest_risk_assessment(conn, leg["id"])
        trip["legs"] = legs
        return trip
    finally:
        conn.close()


class PushSubscriptionIn(BaseModel):
    subscription: dict


@app.post("/trips/{trip_id}/push-subscription")
async def set_push_subscription(trip_id: str, body: PushSubscriptionIn):
    conn = db.connect()
    try:
        if db.get_trip(conn, trip_id) is None:
            raise HTTPException(status_code=404, detail="unknown trip")
        db.update_push_subscription(conn, trip_id, body.subscription)
    finally:
        conn.close()

    handle = state["temporal"].get_workflow_handle(f"trip-monitor-{trip_id}")
    try:
        await handle.signal("update_push_subscription", body.subscription)
    except Exception as exc:  # noqa: BLE001 -- trip already stopped monitoring is fine
        logger.warning("could not signal workflow for %s: %s", trip_id, exc)
    return {"status": "ok"}


@app.post("/trips/{trip_id}/stop")
async def stop_trip(trip_id: str):
    handle = state["temporal"].get_workflow_handle(f"trip-monitor-{trip_id}")
    await handle.signal("stop_watching")
    return {"status": "ok"}


@app.delete("/trips/{trip_id}")
async def delete_trip(trip_id: str):
    handle = state["temporal"].get_workflow_handle(f"trip-monitor-{trip_id}")
    try:
        await handle.signal("stop_watching")
    except Exception as exc:  # noqa: BLE001 -- already stopped/completed is fine
        logger.warning("could not signal workflow for %s before delete: %s", trip_id, exc)

    conn = db.connect()
    try:
        existed = db.delete_trip(conn, trip_id)
    finally:
        conn.close()
    if not existed:
        raise HTTPException(status_code=404, detail="unknown trip")
    return {"status": "deleted"}
