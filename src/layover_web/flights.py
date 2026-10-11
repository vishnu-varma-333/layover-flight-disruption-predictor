"""Flight lookup and the aircraft-chain view, both read directly from the
Milestone 2 historical Parquet -- the same dataset every other milestone's
"live" demo already replays from (2026-07-15), not a separate live
schedule source (there isn't one; see DECISIONS.md for why "add a trip by
flight number and date" searches this dataset rather than a real GDS).

Loaded once at API startup and kept in memory (same pattern as
`layover_predict.service`'s `categories` and `layover_backtest`'s
`airport_tz_map`), not re-read per request -- this table doesn't change
while the process is running.
"""
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FLIGHTS_PARQUET = REPO_ROOT / "data" / "processed" / "flights.parquet"

_flights: pd.DataFrame = None


def load_flights() -> pd.DataFrame:
    global _flights
    if _flights is None:
        _flights = pd.read_parquet(FLIGHTS_PARQUET)
    return _flights


def _row_to_dict(row) -> dict:
    return {
        "flight_id": row["flight_id"],
        "carrier": row["carrier"],
        "flight_number": int(row["flight_number"]),
        "tail_number": row["tail_number"] if pd.notna(row["tail_number"]) else None,
        "origin": row["origin"],
        "dest": row["dest"],
        "sched_dep_utc": row["sched_dep_utc"].isoformat(),
        "sched_arr_utc": row["sched_arr_utc"].isoformat(),
        "cancelled": bool(row["cancelled"]),
    }


def search_flight(carrier: str, flight_number: int, fl_date: str) -> list:
    """`fl_date`: an ISO date string ("2026-07-15"). Returns every
    matching leg, not just the first.

    Found live, not assumed: a carrier+flight_number+date is NOT always
    unique in the real BTS data. UA539 on 2026-07-15 is genuinely two
    different legs (ORD->DFW, then separately DFW->IAH) under the same
    flight number -- a real through-flight/equipment-swap pattern, not a
    data error. Silently returning `.iloc[0]` picked ORD->DFW or DFW->IAH
    depending on row order alone, with no way for a caller to know which
    one it got -- confirmed by searching for it and getting the wrong
    half of the connection this project's own demo trip depends on. The
    caller (the web app) shows every match and lets the user pick the
    right segment, the same way a real airline search would when a
    flight number covers more than one leg.
    """
    flights = load_flights()
    matches = flights[
        (flights["carrier"] == carrier.upper())
        & (flights["flight_number"] == int(flight_number))
        & (flights["fl_date"].astype(str) == fl_date)
    ].sort_values("sched_dep_utc")
    return [_row_to_dict(row) for _, row in matches.iterrows()]


def get_flight(flight_id: str):
    flights = load_flights()
    matches = flights[flights["flight_id"] == flight_id]
    if matches.empty:
        return None
    return _row_to_dict(matches.iloc[0])


def get_aircraft_chain(flight_id: str) -> list:
    """The spec's "Trip timeline: a live view of the aircraft's chain of
    flights and where delays are building" -- every flight the SAME
    physical aircraft (tail number) flies on the SAME day, in schedule
    order, not just this trip's own legs. This is what actually lets a
    user see *why* their flight might be late: an earlier leg on the same
    airframe running behind.
    """
    flights = load_flights()
    target = flights[flights["flight_id"] == flight_id]
    if target.empty:
        return []
    tail_number = target.iloc[0]["tail_number"]
    fl_date = target.iloc[0]["fl_date"]
    if pd.isna(tail_number):
        return [_row_to_dict(target.iloc[0])]

    chain = flights[(flights["tail_number"] == tail_number) & (flights["fl_date"] == fl_date)]
    chain = chain.sort_values("sched_dep_utc")
    return [_row_to_dict(row) for _, row in chain.iterrows()]
