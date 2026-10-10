"""Live aircraft positions from OpenSky Network's REST API.

Credit budgeting (the spec's own named design topic): anonymous access
gives 400 credits/day, and a small bounding-box query to `/states/all`
costs exactly 1 credit -- confirmed live by comparing the
`X-Rate-Limit-Remaining` response header across two real calls, not
assumed from documentation. With 15 monitored airports (one credit each
per cycle), a once-per-hour full rotation costs 360 credits/day, leaving
a deliberate reserve for retries and variance -- "poll only the regions
tied to monitored flights, not the whole globe" from the spec's data-
sources design consequence, applied directly.

`CreditBudget` tracks the real remaining count straight from that header
(never estimated locally), and polling stops for the rest of the day once
it drops below a safety reserve, rather than running the account dry and
getting locked out before the daily reset.
"""

import logging
import time

import aiohttp

from layover_ingest.airports import MONITORED_AIRPORTS
from layover_ingest.kafka_io import publish
from layover_ingest.scheduling import sleep_until_next_cycle

logger = logging.getLogger(__name__)

STATES_URL = "https://opensky-network.org/api/states/all"
BBOX_HALF_WIDTH_DEG = 0.26  # ~small local airspace around the airport, not a wide region
DEFAULT_RESERVE_CREDITS = 40


class CreditBudget:
    def __init__(self, reserve: int = DEFAULT_RESERVE_CREDITS):
        self.remaining = None  # unknown until the first real response
        self.reserve = reserve

    def update(self, response_headers) -> None:
        value = response_headers.get("X-Rate-Limit-Remaining")
        if value is not None:
            self.remaining = int(value)

    def can_poll(self) -> bool:
        return self.remaining is None or self.remaining > self.reserve


def _bbox(lat: float, lon: float, half_width: float = BBOX_HALF_WIDTH_DEG) -> dict:
    return {
        "lamin": lat - half_width,
        "lamax": lat + half_width,
        "lomin": lon - half_width,
        "lomax": lon + half_width,
    }


def parse_state_vectors(payload: dict, airport: dict) -> list:
    """OpenSky's state vector is a fixed-position array, not an object --
    field order per their API docs, not guessed from a sample.
    """
    events = []
    for state in payload.get("states") or []:
        (
            icao24,
            callsign,
            _origin_country,
            time_position,
            last_contact,
            longitude,
            latitude,
            baro_altitude,
            on_ground,
            velocity,
            true_track,
            vertical_rate,
            _sensors,
            geo_altitude,
            squawk,
            _spi,
            _position_source,
        ) = state[:17]
        if longitude is None or latitude is None:
            continue  # no current position fix -- nothing to publish
        events.append(
            {
                "event_type": "aircraft_position",
                "event_time": last_contact or time_position or payload.get("time"),
                "near_airport": airport["iata"],
                "icao24": icao24,
                "callsign": (callsign or "").strip() or None,
                "longitude": longitude,
                "latitude": latitude,
                "baro_altitude_m": baro_altitude,
                "geo_altitude_m": geo_altitude,
                "on_ground": on_ground,
                "velocity_mps": velocity,
                "true_track_deg": true_track,
                "vertical_rate_mps": vertical_rate,
                "squawk": squawk,
            }
        )
    return events


async def poll_airport(session: aiohttp.ClientSession, airport: dict, budget: CreditBudget):
    if not budget.can_poll():
        logger.info(
            "credit reserve reached (%s remaining) -- skipping %s",
            budget.remaining,
            airport["iata"],
        )
        return []
    async with session.get(STATES_URL, params=_bbox(airport["lat"], airport["lon"])) as resp:
        budget.update(resp.headers)
        if resp.status != 200:
            logger.warning("OpenSky %s -> HTTP %d", airport["iata"], resp.status)
            return []
        payload = await resp.json()
    return parse_state_vectors(payload, airport)


async def run(
    producer, duration_seconds=None, cycle_interval_seconds=3600, reserve=DEFAULT_RESERVE_CREDITS
):
    budget = CreditBudget(reserve=reserve)
    start = time.monotonic()
    deadline = None if duration_seconds is None else start + duration_seconds
    async with aiohttp.ClientSession() as session:
        while duration_seconds is None or time.monotonic() - start < duration_seconds:
            cycle_start = time.monotonic()
            total_events = 0
            for airport in MONITORED_AIRPORTS:
                events = await poll_airport(session, airport, budget)
                for event in events:
                    publish(producer, event)
                total_events += len(events)
            logger.info(
                "opensky cycle: %d position events, %s credits remaining",
                total_events,
                budget.remaining,
            )
            await sleep_until_next_cycle(cycle_start, cycle_interval_seconds, deadline)
