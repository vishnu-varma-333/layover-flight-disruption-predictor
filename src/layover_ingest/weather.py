"""Live airport weather (METAR/TAF) from NOAA's Aviation Weather Center
API -- public, keyless, no observed rate limit, unlike OpenSky. One call
covers every monitored airport at once (`ids` takes a comma-separated
list), so there's no credit-budgeting problem to solve here: the
engineering question this ingestor answers is normalization and parsing,
not scarcity.
"""

import logging
import time

import aiohttp

from layover_ingest.airports import MONITORED_AIRPORTS
from layover_ingest.kafka_io import publish
from layover_ingest.scheduling import sleep_until_next_cycle

logger = logging.getLogger(__name__)

METAR_URL = "https://aviationweather.gov/api/data/metar"
TAF_URL = "https://aviationweather.gov/api/data/taf"


def _icao_list() -> str:
    return ",".join(a["icao"] for a in MONITORED_AIRPORTS)


def parse_metars(payload: list) -> list:
    events = []
    for m in payload:
        events.append(
            {
                "event_type": "airport_weather",
                "report_type": "metar",
                "event_time": m.get("reportTime"),
                "icao": m.get("icaoId"),
                "temp_c": m.get("temp"),
                "dewpoint_c": m.get("dewp"),
                "wind_dir_deg": m.get("wdir"),
                "wind_speed_kt": m.get("wspd"),
                "visibility_sm": m.get("visib"),
                "altimeter_hpa": m.get("altim"),
                "flight_category": m.get("fltCat"),
                "raw": m.get("rawOb"),
            }
        )
    return events


def parse_tafs(payload: list) -> list:
    events = []
    for t in payload:
        events.append(
            {
                "event_type": "airport_weather",
                "report_type": "taf",
                "event_time": t.get("issueTime"),
                "icao": t.get("icaoId"),
                "valid_from": t.get("validTimeFrom"),
                "valid_to": t.get("validTimeTo"),
                "raw": t.get("rawTAF"),
            }
        )
    return events


async def poll_weather(session: aiohttp.ClientSession) -> list:
    events = []
    for url, parser in ((METAR_URL, parse_metars), (TAF_URL, parse_tafs)):
        async with session.get(url, params={"ids": _icao_list(), "format": "json"}) as resp:
            if resp.status != 200:
                logger.warning("aviationweather %s -> HTTP %d", url, resp.status)
                continue
            payload = await resp.json()
        events.extend(parser(payload))
    return events


async def run(producer, duration_seconds=None, interval_seconds=900):
    start = time.monotonic()
    deadline = None if duration_seconds is None else start + duration_seconds
    async with aiohttp.ClientSession() as session:
        while duration_seconds is None or time.monotonic() - start < duration_seconds:
            cycle_start = time.monotonic()
            events = await poll_weather(session)
            for event in events:
                publish(producer, event)
            logger.info("weather cycle: %d events", len(events))
            await sleep_until_next_cycle(cycle_start, interval_seconds, deadline)
