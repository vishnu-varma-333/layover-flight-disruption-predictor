"""Pure, Flink-independent rotation-linking logic -- the same per-tail
state machine as Milestone 4's `BacktestScorer`, factored out so it's
testable without a Flink runtime. `rotation_job.py`'s `KeyedProcessFunction`
holds the actual state in Flink's `ValueState`/`MapState` (so it
checkpoints and partitions correctly across a real cluster) and calls
these pure functions with plain Python values extracted from that state.

Two things this version adds beyond Milestone 4's Python state machine,
both needed because this milestone's input is deliberately messy, not
producer-sorted:

- `landed_event_time` on tail state, and a last-event-time-wins guard: an
  out-of-order "landed" event for the same tail/day that's *older* than
  one already applied is a no-op, not a stale overwrite.
- Explicit duplicate detection is the caller's job (a seen-keys set
  keyed by (flight_id, event_type)) -- these functions assume they're
  only ever called once per genuine event, so dedup happens before they
  do, not inside them.
"""

TIGHT_TURNAROUND_MINUTES = 45


def build_feature_update(
    flight_id: str, tail_number: str, fl_date, inbound, source_event_type: str, event_time
) -> dict:
    turnaround_minutes = None
    if inbound is not None and inbound.get("landed_at") is not None:
        turnaround_minutes = (
            (event_time - inbound["landed_at"]) / 60
            if isinstance(event_time, (int, float))
            else None
        )

    return {
        "flight_id": flight_id,
        "tail_number": tail_number,
        "fl_date": fl_date,
        "source_event_type": source_event_type,
        "computed_at_event_time": event_time,
        "has_inbound": inbound is not None,
        "prev_arr_delay_min": inbound["arr_delay_min"] if inbound else None,
        "prev_dep_delay_min": inbound["dep_delay_min"] if inbound else None,
        "turnaround_minutes": turnaround_minutes,
        "tight_turnaround": (
            (turnaround_minutes < TIGHT_TURNAROUND_MINUTES)
            if turnaround_minutes is not None
            else None
        ),
        "cum_delay_today": inbound["cum_delay_today"] if inbound else None,
    }


def resolve_inbound(tail_state, fl_date):
    """None if there's no tail state yet, or it's from a different day
    (rotations don't cross day boundaries -- Milestone 2's own scope)."""
    if tail_state is None or tail_state["fl_date"] != fl_date:
        return None
    return tail_state


def merge_landed_event(tail_state, event: dict) -> dict:
    """Last-event-time-wins: an older landed event arriving after a newer
    one has already been applied must not overwrite it with stale data.
    """
    event_time = event["event_time"]
    if tail_state is not None and tail_state["fl_date"] == event["fl_date"]:
        if tail_state.get("landed_event_time", float("-inf")) >= event_time:
            return tail_state  # a newer (or identical) update already won

    cum_delay = event["arr_delay_min"]
    if (
        tail_state is not None
        and tail_state["fl_date"] == event["fl_date"]
        and tail_state.get("cum_delay_today") is not None
    ):
        cum_delay = tail_state["cum_delay_today"] + event["arr_delay_min"]

    return {
        "fl_date": event["fl_date"],
        "dest": event["dest"],
        "arr_delay_min": event["arr_delay_min"],
        "dep_delay_min": event["dep_delay_min"],
        "landed_at": event_time,
        "landed_event_time": event_time,
        "cum_delay_today": cum_delay,
    }
