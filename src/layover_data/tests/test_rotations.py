from datetime import date

import pandas as pd

from layover_data.rotations import build_rotations


def _flight(flight_id, tail, fl_date, origin, dest, sched_dep_hour):
    return {
        "flight_id": flight_id,
        "tail_number": tail,
        "fl_date": fl_date,
        "origin": origin,
        "dest": dest,
        "sched_dep_utc": pd.Timestamp(f"{fl_date}T{sched_dep_hour:02d}:00:00", tz="UTC"),
    }


def test_build_rotations_orders_legs_by_scheduled_departure():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight("f3", "N1", d, "ORD", "DEN", 20),
            _flight("f1", "N1", d, "DEN", "LAX", 8),
            _flight("f2", "N1", d, "LAX", "ORD", 14),
        ]
    )
    rot = build_rotations(flights)
    assert len(rot) == 1
    row = rot.iloc[0]
    assert row["flight_ids"] == ["f1", "f2", "f3"]
    assert row["n_legs"] == 3
    assert row["chain_breaks"] == 0


def test_build_rotations_detects_chain_break():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight("f1", "N2", d, "DEN", "LAX", 8),
            # break: this leg departs from ORD, not LAX where f1 landed
            _flight("f2", "N2", d, "ORD", "JFK", 14),
        ]
    )
    rot = build_rotations(flights)
    assert rot.iloc[0]["chain_breaks"] == 1


def test_build_rotations_excludes_flights_with_no_tail_number():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight("f1", None, d, "DEN", "LAX", 8),
            _flight("f2", "N3", d, "LAX", "ORD", 14),
        ]
    )
    rot = build_rotations(flights)
    assert len(rot) == 1
    assert rot.iloc[0]["tail_number"] == "N3"


def test_build_rotations_separates_by_date():
    flights = pd.DataFrame(
        [
            _flight("f1", "N4", date(2026, 1, 15), "DEN", "LAX", 8),
            _flight("f2", "N4", date(2026, 1, 16), "LAX", "ORD", 8),
        ]
    )
    rot = build_rotations(flights)
    assert len(rot) == 2
    assert set(rot["n_legs"]) == {1, 1}
