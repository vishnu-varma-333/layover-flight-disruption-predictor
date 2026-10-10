#!/usr/bin/env python3
"""Live checks against the real live.events topic: consumes for a bounded
window and asserts each expected event type actually arrived with a
sane, correctly-typed schema -- not mocked payloads, whatever the real
ingestors (run separately, e.g. via `make live-ingest`) actually
published.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from confluent_kafka import Consumer  # noqa: E402

from layover_ingest.airports import MONITORED_AIRPORTS  # noqa: E402

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "live.events"
CONSUME_SECONDS = 20

FAILURES = []


def check(name):
    def decorator(fn):
        def wrapper(events):
            try:
                fn(events)
                print(f"[PASS] {name}")
            except AssertionError as exc:
                print(f"[FAIL] {name}: {exc}")
                FAILURES.append(name)

        return wrapper

    return decorator


@check("at least one aircraft_position event, with a real-looking lat/lon")
def check_positions(events):
    positions = [e for e in events if e["event_type"] == "aircraft_position"]
    assert positions, "no aircraft_position events received"
    p = positions[0]
    assert -180 <= p["longitude"] <= 180, f"implausible longitude {p['longitude']}"
    assert -90 <= p["latitude"] <= 90, f"implausible latitude {p['latitude']}"
    assert p["icao24"], "missing icao24"


@check("at least one airport_weather event (metar or taf) with a raw report")
def check_weather(events):
    weather = [e for e in events if e["event_type"] == "airport_weather"]
    assert weather, "no airport_weather events received"
    w = weather[0]
    assert w["report_type"] in ("metar", "taf")
    assert w["raw"], "missing raw report text"
    assert w["icao"].startswith("K"), f"unexpected icao code {w['icao']}"


@check("airport_status events (if any) are all for monitored airports")
def check_status(events):
    status = [e for e in events if e["event_type"] == "airport_status"]
    if not status:
        print("  (none this window -- not a failure, FAA feed may be quiet)")
        return
    monitored = {a["iata"] for a in MONITORED_AIRPORTS}
    for s in status:
        assert s["iata"] in monitored, f"unexpected airport {s['iata']} in status feed"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"verify-live-{int(time.time())}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    events = []
    deadline = time.time() + CONSUME_SECONDS
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        events.append(json.loads(msg.value()))
    consumer.close()

    print(f"consumed {len(events)} events in {CONSUME_SECONDS}s\n")
    if not events:
        sys.exit(
            "no events at all -- is `make live-ingest` (or run_live_ingestion.py) running?"
        )

    for fn in (check_positions, check_weather, check_status):
        fn(events)

    if FAILURES:
        print(f"\n{len(FAILURES)} check(s) failed: {', '.join(FAILURES)}")
        sys.exit(1)
    print("\nAll live-ingestion checks passed.")


if __name__ == "__main__":
    main()
