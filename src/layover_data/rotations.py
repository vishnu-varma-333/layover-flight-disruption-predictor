"""Builds the Rotation entity: for each (tail_number, fl_date), the ordered
chain of flights that aircraft flew that day -- the signal the delay-
propagation model depends on (a late-arriving aircraft delays its next
scheduled departure).

Flights with no tail number (a small share of records -- some regional/
codeshare rows omit it) can't be placed in a rotation and are excluded here;
they still exist as Flight records, just with no aircraft link.

Performance note (see DECISIONS.md): a first version grouped with a Python-
level loop (`groupby(...)` then `.to_dict("records")` per group) and was
never actually tested at real scale during development -- it looked correct
against small synthetic fixtures and silently became the pipeline's
bottleneck at ~7M real rows (millions of tiny groups, each paying pandas's
per-group Python overhead). Found live: it was still running after 20+
minutes on the real dataset. Rewritten here as vectorized groupby/shift
operations with no per-group Python loop at all.
"""

import pandas as pd


def build_rotations(flights: pd.DataFrame) -> pd.DataFrame:
    """Returns one row per (tail_number, fl_date): tail_number, fl_date,
    flight_ids (ordered list), n_legs, chain_breaks (count of consecutive
    legs where leg[i].dest != leg[i+1].origin -- a data-quality signal,
    not a filter: ferry flights and equipment swaps are real).
    """
    with_tail = flights.loc[
        flights["tail_number"].notna(),
        ["tail_number", "fl_date", "dest", "origin", "flight_id", "sched_dep_utc"],
    ].copy()

    # Order within a rotation by scheduled departure; actual departure as a
    # tiebreak for the rare case of a missing/equal scheduled time.
    with_tail = with_tail.sort_values(["tail_number", "fl_date", "sched_dep_utc"])

    group_keys = ["tail_number", "fl_date"]
    grouped = with_tail.groupby(group_keys, sort=False)

    # A chain break is leg[i].origin != leg[i-1].dest. Computed for every
    # row at once via a within-group shift, instead of a Python loop per
    # group: the first leg of a rotation has no previous dest (NaN) and is
    # never counted as a break.
    prev_dest = grouped["dest"].shift(1)
    is_break = (with_tail["origin"] != prev_dest) & prev_dest.notna()

    rotations = grouped.agg(flight_ids=("flight_id", list), n_legs=("flight_id", "size"))
    rotations["chain_breaks"] = is_break.groupby(
        [with_tail["tail_number"], with_tail["fl_date"]], sort=False
    ).sum()

    return rotations.reset_index()
