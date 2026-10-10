"""Turns Flight records into the chronological stream of events a live
system would actually see, for replay through Kafka.

Three event types per flight, each carrying only what's genuinely known at
that moment -- this is what makes the replay point-in-time correct rather
than just replaying the already-computed (and therefore leaky) Milestone 3
feature table:

- "schedule": the flight's schedule becomes known. BTS gives no real
  schedule-publish timestamp, so this is simulated as a fixed
  `schedule_lead_hours` (default 24h) before `sched_dep_utc` -- a
  documented simplification. It doesn't affect the rotation-driven lead
  time this milestone is actually measuring, since that's governed by
  when the *inbound* aircraft's "landed" event fires, which is always
  tied to real data.
- "departed": this flight's own actual departure, with its now-known
  `dep_delay_min`. Skipped for cancelled flights (never departed).
- "landed": this flight's own actual arrival, with its now-known
  `arr_delay_min` -- this is simultaneously (a) the ground-truth label for
  evaluating predictions made on this flight, and (b) the event that
  makes this flight's outcome available as *rotation* information for
  whatever flies next on the same tail. Skipped for cancelled/diverted
  flights (no comparable arrival outcome), matching Milestone 3's
  supervised-row filter.

Events are returned sorted by event_time ascending -- the order they're
produced to a single Kafka partition in, so a single consumer reading that
partition sees them in true chronological order. This is a deliberate
scope boundary: a live system (Milestone 6) must handle genuinely
out-of-order, multi-source event arrival with Flink watermarks; here, the
producer controls emission order because it's replaying known history, so
single-partition producer-side sorting is sufficient and correct for a
backtest specifically -- see DECISIONS.md.
"""

import pandas as pd

EVENT_COLUMNS = [
    "event_time",
    "event_type",
    "flight_id",
    "tail_number",
    "fl_date",
    "carrier",
    "origin",
    "dest",
    "sched_dep_utc",
    "cancelled",
    "dep_delay_min",
    "arr_delay_min",
]


def build_event_stream(flights: pd.DataFrame, schedule_lead_hours: int = 24) -> pd.DataFrame:
    base_cols = [
        "flight_id",
        "tail_number",
        "fl_date",
        "carrier",
        "origin",
        "dest",
        "sched_dep_utc",
        "cancelled",
    ]

    schedule_events = flights[base_cols].copy()
    schedule_events["event_time"] = flights["sched_dep_utc"] - pd.Timedelta(
        hours=schedule_lead_hours
    )
    schedule_events["event_type"] = "schedule"
    schedule_events["dep_delay_min"] = float("nan")
    schedule_events["arr_delay_min"] = float("nan")

    departed = flights[~flights["cancelled"] & flights["actual_dep_utc"].notna()]
    departed_events = departed[base_cols].copy()
    departed_events["event_time"] = departed["actual_dep_utc"]
    departed_events["event_type"] = "departed"
    departed_events["dep_delay_min"] = departed["dep_delay_min"].astype(float)
    departed_events["arr_delay_min"] = float("nan")

    landed = flights[
        ~flights["cancelled"] & ~flights["diverted"] & flights["actual_arr_utc"].notna()
    ]
    landed_events = landed[base_cols].copy()
    landed_events["event_time"] = landed["actual_arr_utc"]
    landed_events["event_type"] = "landed"
    landed_events["dep_delay_min"] = landed["dep_delay_min"].astype(float)
    landed_events["arr_delay_min"] = landed["arr_delay_min"].astype(float)

    stream = pd.concat([schedule_events, departed_events, landed_events], ignore_index=True)[
        EVENT_COLUMNS
    ]
    return stream.sort_values("event_time", kind="stable").reset_index(drop=True)
