"""Builds the feature table the delay-propagation models train on, from the
Milestone 2 Flight records.

Two feature groups:

- Schedule-only: carrier, origin, dest (categorical -- LightGBM learns
  airport- and route-specific effects from these directly via categorical
  splits, which is this milestone's "airport features": there is no live
  weather/FAA-status data yet -- that's Milestone 5's ingestion, sequenced
  after this one in the spec's own milestone order -- so a real airport
  condition signal isn't available to train on yet. Recurring time-of-day/
  seasonal congestion is captured instead via local departure hour, day of
  week and month), local departure hour/day-of-week/month.
- Rotation/turnaround: the inbound aircraft's previous leg outcome, the
  scheduled turnaround time, a tight-turnaround flag, and the tail's
  cumulative delay so far that day -- the actual "delay propagation"
  signal this whole project is about. This feature set (previous-leg
  delay via a tail-number-partitioned lag, turnaround time, a tight-
  turnaround indicator, cumulative same-day delay) follows published
  reactionary-delay-prediction research (e.g. arXiv:2605.07364, which
  reports these as the dominant driver of model gain, schedule-only ->
  rotation-chain features taking AUC 0.732 -> 0.875), not just intuition
  -- see DECISIONS.md for the research pass and what was deliberately
  left out (airport-network-centrality features, meta-models).
- TIGHT_TURNAROUND_MINUTES=45 follows that same literature's cited
  standard minimum practical narrow-body domestic turnaround under normal
  gate operations -- below it, a flight can't absorb even a moderate
  upstream delay.

Local hour-of-day is computed by grouping on the airport's IANA timezone
(not per-row) -- only ~15 distinct zones across all US-domestic airports,
so this is one vectorized `tz_convert` per zone, not millions of per-row
lookups. (Milestone 2 found, the hard way, what per-row/per-tiny-group
pandas operations cost at this dataset's scale -- see DECISIONS.md.)
"""

import pandas as pd

TIGHT_TURNAROUND_MINUTES = 45

SCHEDULE_FEATURES = ["carrier", "origin", "dest", "month", "day_of_week", "dep_hour_local"]
ROTATION_FEATURES = [
    "leg_index",
    "has_inbound",
    "prev_arr_delay_min",
    "prev_dep_delay_min",
    "turnaround_minutes",
    "tight_turnaround",
    "chain_break",
    "cum_delay_today",
]
CATEGORICAL_FEATURES = ["carrier", "origin", "dest"]


def _local_dep_hour_dow_month(flights: pd.DataFrame, airport_tz_map: dict) -> pd.DataFrame:
    tz_name = flights["origin"].map(airport_tz_map)
    local_dep = pd.Series(pd.NaT, index=flights.index, dtype="datetime64[ns]")
    for tz, idx in flights.groupby(tz_name, sort=False).groups.items():
        local_dep.loc[idx] = (
            flights.loc[idx, "sched_dep_utc"].dt.tz_convert(tz).dt.tz_localize(None)
        )
    return pd.DataFrame(
        {
            "dep_hour_local": local_dep.dt.hour,
            "day_of_week": local_dep.dt.dayofweek,
            "month": local_dep.dt.month,
        }
    )


def rotation_features(flights: pd.DataFrame) -> pd.DataFrame:
    sorted_flights = flights.sort_values(["tail_number", "fl_date", "sched_dep_utc"]).copy()
    # A column that happens to be all-NaT (e.g. scoring future flights for
    # pre-departure prediction -- none of them have landed yet) can lose its
    # tz-aware dtype during construction, which breaks the UTC subtraction
    # below with a naive-vs-aware TypeError. Force it back explicitly rather
    # than let that depend on what values happened to be present.
    sorted_flights["actual_arr_utc"] = pd.to_datetime(sorted_flights["actual_arr_utc"], utc=True)
    # Same reasoning: an all-missing delay column (no flight in the batch
    # has landed yet) can come in as object dtype, which breaks the
    # groupby cumsum below.
    for col in ("arr_delay_min", "dep_delay_min"):
        sorted_flights[col] = pd.to_numeric(sorted_flights[col], errors="coerce")
    grouped = sorted_flights.groupby(["tail_number", "fl_date"], sort=False)

    leg_index = grouped.cumcount()
    prev_dest = grouped["dest"].shift(1)
    prev_actual_arr_utc = grouped["actual_arr_utc"].shift(1)
    prev_arr_delay_min = grouped["arr_delay_min"].shift(1)
    prev_dep_delay_min = grouped["dep_delay_min"].shift(1)

    turnaround_minutes = (
        sorted_flights["sched_dep_utc"] - prev_actual_arr_utc
    ).dt.total_seconds() / 60
    # NaN (no inbound leg) stays NaN, not False -- "no data" and "turnaround
    # wasn't tight" are different things, and conflating them would teach
    # the model a false signal for every first-leg-of-the-day row. Kept as
    # float 0.0/1.0/NaN rather than Python bool + NaN mixed together: the
    # latter collapses to pandas "object" dtype, which LightGBM rejects
    # outright (found live, not assumed -- see DECISIONS.md).
    tight_turnaround = (turnaround_minutes < TIGHT_TURNAROUND_MINUTES).astype(float)
    tight_turnaround = tight_turnaround.where(turnaround_minutes.notna())
    chain_break = (sorted_flights["origin"] != prev_dest) & prev_dest.notna()
    # Running total of the tail's OWN already-landed legs today, computed
    # from the same shifted (no-leakage) series: at leg i this sums legs
    # 0..i-1's actual arrival delay, never leg i's own.
    cum_delay_today = prev_arr_delay_min.groupby(
        [sorted_flights["tail_number"], sorted_flights["fl_date"]], sort=False
    ).cumsum()

    out = pd.DataFrame(
        {
            "leg_index": leg_index,
            "has_inbound": leg_index > 0,
            "prev_arr_delay_min": prev_arr_delay_min,
            "prev_dep_delay_min": prev_dep_delay_min,
            "turnaround_minutes": turnaround_minutes,
            "tight_turnaround": tight_turnaround,
            "chain_break": chain_break,
            "cum_delay_today": cum_delay_today,
        },
        index=sorted_flights.index,
    )
    # Realign to the original row order -- the grouping/sort above was only
    # needed to compute these columns correctly, not to reorder the table.
    return out.reindex(flights.index)


def build_feature_table(flights: pd.DataFrame, airport_tz_map: dict) -> pd.DataFrame:
    """One row per flight: engineered features, both delay targets, and the
    columns needed to split/filter (fl_date, cancelled, diverted).

    Rows with no tail number, or the first leg of an aircraft's day, get
    NaN rotation features -- left as NaN (not imputed) for LightGBM's
    native missing-value handling, not filled with a misleading default.
    """
    time_feats = _local_dep_hour_dow_month(flights, airport_tz_map)
    rotation_feats = rotation_features(flights)

    out = pd.concat(
        [
            flights[["flight_id", "fl_date", "carrier", "origin", "dest", "cancelled", "diverted"]],
            time_feats,
            rotation_feats,
            flights[["dep_delay_min", "arr_delay_min"]],
        ],
        axis=1,
    )
    for col in CATEGORICAL_FEATURES:
        out[col] = out[col].astype("category")
    return out
