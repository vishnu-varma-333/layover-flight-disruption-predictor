from datetime import date

import pandas as pd

from layover_backtest.events import build_event_stream


def _flight(flight_id, tail, fl_date, origin, dest, sched_dep, actual_dep=None, actual_arr=None,
            dep_delay=None, arr_delay=None, cancelled=False, diverted=False):
    return {
        "flight_id": flight_id,
        "tail_number": tail,
        "fl_date": fl_date,
        "carrier": "UA",
        "origin": origin,
        "dest": dest,
        "sched_dep_utc": pd.Timestamp(sched_dep, tz="UTC"),
        "actual_dep_utc": pd.Timestamp(actual_dep, tz="UTC") if actual_dep else pd.NaT,
        "actual_arr_utc": pd.Timestamp(actual_arr, tz="UTC") if actual_arr else pd.NaT,
        "dep_delay_min": dep_delay,
        "arr_delay_min": arr_delay,
        "cancelled": cancelled,
        "diverted": diverted,
    }


def test_normal_flight_produces_three_ordered_events():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00",
                actual_dep="2026-01-15T08:05:00", actual_arr="2026-01-15T10:30:00",
                dep_delay=5.0, arr_delay=15.0,
            )
        ]
    )
    stream = build_event_stream(flights)
    assert list(stream["event_type"]) == ["schedule", "departed", "landed"]
    assert stream.iloc[0]["event_time"] == pd.Timestamp("2026-01-14T08:00:00", tz="UTC")
    assert stream.iloc[1]["event_time"] == pd.Timestamp("2026-01-15T08:05:00", tz="UTC")
    assert stream.iloc[2]["event_time"] == pd.Timestamp("2026-01-15T10:30:00", tz="UTC")


def test_cancelled_flight_only_produces_schedule_event():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [_flight("f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00", cancelled=True)]
    )
    stream = build_event_stream(flights)
    assert list(stream["event_type"]) == ["schedule"]


def test_diverted_flight_has_no_landed_event():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00",
                actual_dep="2026-01-15T08:05:00", dep_delay=5.0, diverted=True,
            )
        ]
    )
    stream = build_event_stream(flights)
    assert list(stream["event_type"]) == ["schedule", "departed"]


def test_multiple_flights_are_globally_sorted_by_event_time():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight("f2", "N2", d, "ORD", "LAX", "2026-01-15T14:00:00"),
            _flight("f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00"),
        ]
    )
    stream = build_event_stream(flights)
    # Both are schedule-only events (no actual times given); f1's schedule
    # fires first since 08:00 - 24h < 14:00 - 24h.
    assert list(stream["flight_id"]) == ["f1", "f2"]
