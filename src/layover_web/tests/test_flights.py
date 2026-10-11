from datetime import date

import pandas as pd

from layover_web import flights


def _flight(flight_id, carrier, flight_number, tail, origin, dest, sched_dep, sched_arr):
    return {
        "flight_id": flight_id,
        "fl_date": date(2026, 7, 15),
        "carrier": carrier,
        "flight_number": flight_number,
        "tail_number": tail,
        "origin": origin,
        "dest": dest,
        "sched_dep_utc": pd.Timestamp(sched_dep, tz="UTC"),
        "sched_arr_utc": pd.Timestamp(sched_arr, tz="UTC"),
        "cancelled": False,
    }


def _set_flights(monkeypatch, rows):
    monkeypatch.setattr(flights, "_flights", pd.DataFrame(rows))


def test_search_returns_every_leg_when_a_flight_number_covers_more_than_one(monkeypatch):
    # Regression test for a real bug: UA539 on 2026-07-15 is genuinely
    # two different legs under the same flight number (ORD->DFW, then
    # DFW->IAH) -- a naive "first match" search silently returned the
    # wrong half of a real connection. See DECISIONS.md.
    rows = [
        _flight("f1", "UA", 539, "N1", "ORD", "DFW", "2026-07-15T12:15:00", "2026-07-15T14:58:00"),
        _flight("f2", "UA", 539, "N1", "DFW", "IAH", "2026-07-15T16:59:00", "2026-07-15T18:17:00"),
    ]
    _set_flights(monkeypatch, rows)
    results = flights.search_flight("UA", 539, "2026-07-15")
    assert [r["flight_id"] for r in results] == ["f1", "f2"]  # chronological order


def test_search_uppercases_carrier(monkeypatch):
    rows = [
        _flight("f1", "UA", 100, "N1", "ORD", "DFW", "2026-07-15T12:00:00", "2026-07-15T14:00:00")
    ]
    _set_flights(monkeypatch, rows)
    assert len(flights.search_flight("ua", 100, "2026-07-15")) == 1


def test_search_no_match_returns_empty_list(monkeypatch):
    rows = [
        _flight("f1", "UA", 100, "N1", "ORD", "DFW", "2026-07-15T12:00:00", "2026-07-15T14:00:00")
    ]
    _set_flights(monkeypatch, rows)
    assert flights.search_flight("UA", 999, "2026-07-15") == []


def test_aircraft_chain_includes_every_leg_same_tail_same_day_in_schedule_order(monkeypatch):
    rows = [
        _flight("f3", "UA", 300, "N1", "SFO", "SAN", "2026-07-15T21:20:00", "2026-07-15T23:07:00"),
        _flight("f1", "UA", 100, "N1", "ORD", "DFW", "2026-07-15T12:15:00", "2026-07-15T14:58:00"),
        _flight("f2", "UA", 200, "N1", "DFW", "SFO", "2026-07-15T16:02:00", "2026-07-15T19:51:00"),
        # different tail, same day -- must NOT appear in N1's chain
        _flight(
            "other", "UA", 400, "N2", "LAX", "JFK", "2026-07-15T08:00:00", "2026-07-15T16:00:00"
        ),
    ]
    _set_flights(monkeypatch, rows)
    chain = flights.get_aircraft_chain("f1")
    assert [f["flight_id"] for f in chain] == ["f1", "f2", "f3"]


def test_aircraft_chain_handles_missing_tail_number_gracefully(monkeypatch):
    row = _flight("f1", "UA", 100, None, "ORD", "DFW", "2026-07-15T12:00:00", "2026-07-15T14:00:00")
    _set_flights(monkeypatch, [row])
    chain = flights.get_aircraft_chain("f1")
    assert [f["flight_id"] for f in chain] == ["f1"]
