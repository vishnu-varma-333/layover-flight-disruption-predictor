"""Live airport status (ground stops, ground delay programs, closures)
from the FAA's public NAS Status XML feed -- one global endpoint covering
every US airport at once, filtered down to the monitored set. No credits,
no per-airport query: the only real engineering question here is parsing
a schema that varies by delay type.

The feed's structure: one <Delay_type> element per category (e.g.
"Ground Stop", "Airport Closures"), each wrapping a list element (name
varies by category) of <Airport> entries whose own child fields also vary
by category (a closure has Reason/Start/Reopen; a ground stop has
different fields again). Parsed generically -- every child tag under an
<Airport> element becomes a dict key -- rather than hardcoding each
category's known fields, since the FAA could add fields or categories
without this ingestor needing a code change to still capture them.
"""

import logging
import re
import time
from datetime import datetime, timezone
from xml.etree import ElementTree

import aiohttp

from layover_ingest.airports import MONITORED_AIRPORTS
from layover_ingest.kafka_io import publish
from layover_ingest.scheduling import sleep_until_next_cycle

logger = logging.getLogger(__name__)

STATUS_URL = "https://nasstatus.faa.gov/api/airport-status-information"
MONITORED_CODES = {a["iata"] for a in MONITORED_AIRPORTS}


def _parse_update_time(text: str):
    # e.g. "Sat Oct 10 15:02:24 2026 GMT" -- always GMT per the feed.
    match = re.match(r"\w+ (\w+ \d+ \d+:\d+:\d+ \d+)", text or "")
    if not match:
        return None
    dt = datetime.strptime(match.group(1), "%b %d %H:%M:%S %Y").replace(tzinfo=timezone.utc)
    return dt.isoformat()


def parse_nas_status(xml_text: str) -> list:
    root = ElementTree.fromstring(xml_text)
    update_time = _parse_update_time(root.findtext("Update_Time"))

    events = []
    for delay_type in root.findall("Delay_type"):
        category = delay_type.findtext("Name")
        list_el = next((c for c in delay_type if c.tag.endswith("_List")), None)
        if list_el is None:
            continue
        for airport_el in list_el.findall("Airport"):
            code = airport_el.findtext("ARPT")
            if code not in MONITORED_CODES:
                continue
            fields = {child.tag: child.text for child in airport_el if child.tag != "ARPT"}
            events.append(
                {
                    "event_type": "airport_status",
                    "event_time": update_time,
                    "iata": code,
                    "delay_category": category,
                    **fields,
                }
            )
    return events


async def poll_status(session: aiohttp.ClientSession) -> list:
    async with session.get(STATUS_URL) as resp:
        if resp.status != 200:
            logger.warning("FAA NAS status -> HTTP %d", resp.status)
            return []
        xml_text = await resp.text()
    return parse_nas_status(xml_text)


async def run(producer, duration_seconds=None, interval_seconds=300):
    start = time.monotonic()
    deadline = None if duration_seconds is None else start + duration_seconds
    async with aiohttp.ClientSession() as session:
        while duration_seconds is None or time.monotonic() - start < duration_seconds:
            cycle_start = time.monotonic()
            events = await poll_status(session)
            for event in events:
                publish(producer, event)
            logger.info("airport-status cycle: %d events", len(events))
            await sleep_until_next_cycle(cycle_start, interval_seconds, deadline)
