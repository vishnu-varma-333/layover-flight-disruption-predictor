"""The stateful, point-in-time-correct scorer: consumes events one at a
time (as a live Kafka consumer would) and maintains just enough state to
reproduce the Milestone 3 feature set *as it becomes known*, never
peeking at a flight's own future outcome.

State kept, per tail number:
- the most recently *landed* leg's destination, arrival delay, departure
  delay and landing time, for the same fl_date only (matching Milestone
  2's per-day rotation scope) -- this is exactly "prev_*" from
  layover_model.features, but built incrementally from events instead of
  a batch groupby/shift.
- a running cumulative same-day delay total.
- a leg counter, and which of the tail's scheduled-but-not-yet-departed
  flights are "pending" -- so that when an inbound leg lands, the very
  next leg (if already scheduled) can be immediately re-scored with the
  news, which is exactly what produces a non-trivial lead time.

Three performance lessons, all found by actually running this against
real data rather than assumed from the design, are baked into this
module's shape -- the same root lesson as Milestone 2's rotation-building
bug, applied three more times:

1. Feature capture vs. model inference are decoupled. Capturing a point-
   in-time-correct feature snapshot must happen online, one event at a
   time, because it depends on state that only exists at that moment. But
   *running* the model against that snapshot doesn't have to happen
   immediately -- a prediction's value never feeds back into the state
   machine (only real ground-truth "landed"/"departed" events do) -- so
   `handle_event` returns unscored requests and a separate
   `score_requests` batches them by model. Measured: a single-row
   `model.predict()` call costs ~2.6ms of fixed per-call overhead
   regardless of row count (confirmed by profiling a 500-row batch at the
   same ~2.6ms *total*).
2. `handle_event` never builds a pandas object. It returns a plain Python
   dict of raw feature values -- building even a single-row DataFrame
   (plus the categorical/numeric dtype coercion a model needs) measured
   at ~2.4ms *per call*, the same order of overhead as problem 1, and for
   the same reason: pandas is built for one bulk vectorized operation, not
   many tiny ones. All the DataFrame construction and dtype coercion
   happens exactly once, in `score_requests`, over every accumulated
   request at once.
3. Pending flights are indexed by (tail_number, fl_date), not scanned.
   A first version kept one flat dict of every scheduled-but-not-departed
   flight and, on each "landed" event, scanned *all* of it filtering for
   the matching tail/date. Cancelled flights never get a "departed" event
   to remove them again, so they silently accumulated in that dict for
   the rest of the replay -- harmless at a 2-day pilot's scale (a few
   hundred leaked entries), but by a full month's end there were
   thousands, scanned on every one of ~600K landed events. Confirmed live:
   a full-month run's consumer throughput was *half* the 2-day pilot's,
   not just proportionally slower. Fixed two ways: cancelled flights are
   never added to `pending` at all (there's nothing to ever remove them),
   and `pending` itself is keyed by (tail_number, fl_date) so a landed
   event's lookup touches only that tail's same-day flights -- typically
   0-2 entries -- never the whole replay's pending set.
"""
from zoneinfo import ZoneInfo

import pandas as pd

from layover_model.features import ROTATION_FEATURES, SCHEDULE_FEATURES

TIGHT_TURNAROUND_MINUTES = 45

PRE_DEPARTURE_COLS = SCHEDULE_FEATURES + ROTATION_FEATURES
POST_DEPARTURE_COLS = PRE_DEPARTURE_COLS + ["dep_delay_min"]

NUMERIC_COLS = [
    "month", "day_of_week", "dep_hour_local", "leg_index",
    "prev_arr_delay_min", "prev_dep_delay_min", "turnaround_minutes",
    "tight_turnaround", "cum_delay_today", "dep_delay_min",
]
CATEGORICAL_COLS = ["carrier", "origin", "dest"]

MODEL_FEATURE_COLS = {
    "rotation": PRE_DEPARTURE_COLS,
    "schedule_only": SCHEDULE_FEATURES,
    "post_departure": POST_DEPARTURE_COLS,
}


class BacktestScorer:
    def __init__(self, airport_tz_map):
        self.airport_tz_map = airport_tz_map

        self.tail_state = {}  # tail_number -> dict of last-landed info
        # (tail_number, fl_date) -> {flight_id -> scheduled-but-not-departed
        # info}, so a landed event's re-score lookup is O(flights on that
        # tail that day), not O(every pending flight in the whole replay).
        self.pending = {}
        self.leg_index = {}  # (tail_number, fl_date) -> count of schedule events seen

    def _local_time_parts(self, origin: str, sched_dep_utc: pd.Timestamp):
        tz = self.airport_tz_map.get(origin)
        if tz is None:
            return None, None, None
        local = sched_dep_utc.to_pydatetime().astimezone(ZoneInfo(tz))
        return local.hour, local.weekday(), local.month

    def _feature_dict(self, flight, inbound) -> dict:
        """Plain Python dict of raw feature values -- no pandas object
        construction here; see the module docstring for why.

        `flight["leg_index"]` -- NOT `self.leg_index` -- is this flight's
        leg index. Found during Milestone 7's research pass: this used to
        re-read `self.leg_index.get(key, 0)` here, the *live* counter,
        instead of the value `handle_event` already froze onto `flight`
        at schedule time. Since the live counter keeps advancing as this
        tail's *other* legs get scheduled later the same day, every
        departed/landed-triggered re-score (and even the schedule-time
        call itself, which runs after the counter's already been bumped
        for the next leg) read a larger, wrong value -- a real feature
        bug that's been live since Milestone 4, invisible in the
        headline recall/lead-time/MAE numbers because leg_index is a
        weak, mostly-redundant auxiliary signal (has_inbound/
        prev_arr_delay_min/turnaround_minutes already carry the real
        rotation signal), not something that showed up until an exact
        field-level comparison against ground truth in Milestone 7. See
        DECISIONS.md.
        """
        dep_hour, dow, month = self._local_time_parts(flight["origin"], flight["sched_dep_utc"])

        turnaround_minutes = None
        if inbound is not None and inbound.get("landed_at") is not None:
            turnaround_minutes = (
                flight["sched_dep_utc"] - inbound["landed_at"]
            ).total_seconds() / 60

        return {
            "carrier": flight["carrier"],
            "origin": flight["origin"],
            "dest": flight["dest"],
            "month": month,
            "day_of_week": dow,
            "dep_hour_local": dep_hour,
            "leg_index": flight["leg_index"],
            "has_inbound": inbound is not None,
            "prev_arr_delay_min": inbound["arr_delay_min"] if inbound else None,
            "prev_dep_delay_min": inbound["dep_delay_min"] if inbound else None,
            "turnaround_minutes": turnaround_minutes,
            "tight_turnaround": (
                (turnaround_minutes < TIGHT_TURNAROUND_MINUTES)
                if turnaround_minutes is not None
                else None
            ),
            "chain_break": (inbound["dest"] != flight["origin"]) if inbound else False,
            "cum_delay_today": inbound["cum_delay_today"] if inbound else None,
            "dep_delay_min": flight.get("dep_delay_min"),
        }

    def handle_event(self, event: dict) -> list:
        """Returns zero or more *unscored* requests: dicts with flight_id,
        event_time, model (name), and the raw feature values flattened in
        -- actual DataFrame construction and inference happen later, in
        score_requests().
        """
        etype = event["event_type"]
        tail = event["tail_number"]
        key = (tail, event["fl_date"])

        if etype == "schedule":
            idx = self.leg_index.get(key, 0)
            self.leg_index[key] = idx + 1

            inbound = self.tail_state.get(tail)
            if inbound is not None and inbound["fl_date"] != event["fl_date"]:
                inbound = None  # rotations don't cross day boundaries (Milestone 2 scope)

            flight = dict(event, leg_index=idx)
            features = self._feature_dict(flight, inbound)
            # Cancelled flights never get a "departed" event to remove
            # them again -- never add them here at all, rather than leak
            # an entry for the rest of the replay. Found live: this scan
            # growing unboundedly over a full month (thousands of leaked
            # cancelled-flight entries, scanned on every single "landed"
            # event) was the dominant cost, not model inference -- see
            # DECISIONS.md.
            if not event["cancelled"]:
                self.pending.setdefault(key, {})[event["flight_id"]] = {
                    **event,
                    "leg_index": idx,
                    "inbound": inbound,
                }
            base = {"flight_id": event["flight_id"], "event_time": event["event_time"]}
            return [
                {**base, "model": "rotation", **features},
                {**base, "model": "schedule_only", **features},
            ]

        if etype == "departed":
            same_day = self.pending.get(key)
            pending = same_day.pop(event["flight_id"], None) if same_day else None
            if pending is None:
                return []
            flight = dict(pending, dep_delay_min=event["dep_delay_min"])
            features = self._feature_dict(flight, pending["inbound"])
            return [
                {
                    "flight_id": event["flight_id"],
                    "event_time": event["event_time"],
                    "model": "post_departure",
                    **features,
                }
            ]

        if etype == "landed":
            prior = self.tail_state.get(tail)
            cum_delay = event["arr_delay_min"]
            if (
                prior is not None
                and prior["fl_date"] == event["fl_date"]
                and prior["cum_delay_today"] is not None
            ):
                cum_delay = prior["cum_delay_today"] + event["arr_delay_min"]
            self.tail_state[tail] = {
                "fl_date": event["fl_date"],
                "dest": event["dest"],
                "arr_delay_min": event["arr_delay_min"],
                "dep_delay_min": event["dep_delay_min"],
                "landed_at": event["event_time"],
                "cum_delay_today": cum_delay,
            }

            # Re-score whatever's already scheduled on this tail, same day
            # (almost always just the immediate next leg) now that this
            # news exists -- this is what actually produces a non-trivial
            # lead time. O(1) lookup by (tail, fl_date), not a scan of
            # every pending flight in the whole replay.
            requests = []
            for fid, pending in self.pending.get(key, {}).items():
                inbound = self.tail_state[tail]
                features = self._feature_dict(dict(pending), inbound)
                pending["inbound"] = inbound
                requests.append(
                    {
                        "flight_id": fid,
                        "event_time": event["event_time"],
                        "model": "rotation",
                        **features,
                    }
                )
            return requests

        raise ValueError(f"unknown event_type {etype!r}")


def score_requests(requests: list, models: dict, categories: dict) -> pd.DataFrame:
    """Batches every accumulated request by model and predicts once per
    batch. All DataFrame construction and dtype coercion happens here,
    exactly once over the full accumulated set -- see the module
    docstring for why that matters at this dataset's scale.

    `categories`: {"carrier": [...], "origin": [...], "dest": [...]} --
    the same fixed category lists used at Milestone 3 training time, so a
    one-row batch doesn't silently narrow a column's categories.
    """
    if not requests:
        return pd.DataFrame(columns=["flight_id", "event_time", "model", "predicted_delay"])

    df = pd.DataFrame(requests)
    for col in CATEGORICAL_COLS:
        df[col] = pd.Categorical(df[col], categories=categories[col])
    df[NUMERIC_COLS] = df[NUMERIC_COLS].apply(pd.to_numeric, errors="coerce")

    results = []
    for model_name, group in df.groupby("model"):
        preds = models[model_name].predict(group[MODEL_FEATURE_COLS[model_name]])
        # Built from the group's own tz-aware Series, not `.values` --
        # `.values` on a tz-aware column silently returns tz-naive numpy
        # datetime64, which breaks every later comparison against the
        # (still tz-aware) ground truth timestamps. Found live, not
        # assumed.
        out = group[["flight_id", "event_time"]].reset_index(drop=True)
        out["model"] = model_name
        out["predicted_delay"] = preds
        results.append(out)
    return pd.concat(results, ignore_index=True)
