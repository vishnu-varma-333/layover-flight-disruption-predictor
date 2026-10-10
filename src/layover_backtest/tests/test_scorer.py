from datetime import date

import pandas as pd

from layover_backtest.events import build_event_stream
from layover_backtest.scorer import BacktestScorer, score_requests

TZ_MAP = {"DEN": "America/Denver", "ORD": "America/Chicago", "LAX": "America/Los_Angeles"}
CATEGORIES = {"carrier": ["UA"], "origin": ["DEN", "ORD", "LAX"], "dest": ["DEN", "ORD", "LAX"]}


class _StubModel:
    """Returns prev_arr_delay_min (0 if absent) for rotation-style
    features, or a fixed constant -- whichever the test needs to tell
    apart "used the inbound signal" from "didn't"."""

    def __init__(self, column=None, constant=None):
        self.column = column
        self.constant = constant

    def predict(self, X):
        if self.constant is not None:
            return [self.constant] * len(X)
        return [0.0 if pd.isna(v) else float(v) for v in X[self.column]]


def _flight(flight_id, tail, fl_date, origin, dest, sched_dep, actual_dep=None, actual_arr=None,
            dep_delay=None, arr_delay=None, cancelled=False):
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
        "diverted": False,
    }


def _models():
    return {
        "rotation": _StubModel(column="prev_arr_delay_min"),
        "schedule_only": _StubModel(constant=5.0),
        "post_departure": _StubModel(column="dep_delay_min"),
    }


def _run(flights):
    stream = build_event_stream(flights)
    scorer = BacktestScorer(airport_tz_map=TZ_MAP)
    requests = []
    for event in stream.to_dict("records"):
        requests.extend(scorer.handle_event(event))
    return score_requests(requests, _models(), CATEGORIES)


def test_first_leg_of_day_has_no_inbound_signal():
    d = date(2026, 1, 15)
    flights = pd.DataFrame([_flight("f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00")])
    preds = _run(flights)
    schedule_pred = preds[(preds["flight_id"] == "f1") & (preds["model"] == "rotation")]
    assert schedule_pred.iloc[0]["predicted_delay"] == 0.0  # no inbound yet


def test_inbound_landing_triggers_rescore_of_pending_next_leg():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00",
                actual_dep="2026-01-15T08:00:00", actual_arr="2026-01-15T10:30:00",
                dep_delay=0.0, arr_delay=22.0,
            ),
            _flight("f2", "N1", d, "ORD", "LAX", "2026-01-15T12:00:00"),
        ]
    )
    preds = _run(flights)
    f2_rotation_preds = preds[(preds["flight_id"] == "f2") & (preds["model"] == "rotation")]

    # Before f1 lands: no inbound signal yet.
    assert f2_rotation_preds.iloc[0]["predicted_delay"] == 0.0
    # After f1 lands (22 min late): f2 gets re-scored with that news.
    assert f2_rotation_preds.iloc[-1]["predicted_delay"] == 22.0
    assert len(f2_rotation_preds) == 2  # exactly one re-score, not a storm of them


def test_schedule_only_model_never_changes_on_rescore():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00",
                actual_dep="2026-01-15T08:00:00", actual_arr="2026-01-15T10:30:00",
                dep_delay=0.0, arr_delay=22.0,
            ),
            _flight("f2", "N1", d, "ORD", "LAX", "2026-01-15T12:00:00"),
        ]
    )
    preds = _run(flights)
    f2_schedule_only = preds[(preds["flight_id"] == "f2") & (preds["model"] == "schedule_only")]
    assert len(f2_schedule_only) == 1  # scored once, at its own schedule event, never again
    assert f2_schedule_only.iloc[0]["predicted_delay"] == 5.0


def test_post_departure_uses_this_flights_own_departure_delay():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00",
                actual_dep="2026-01-15T08:40:00", actual_arr="2026-01-15T10:30:00",
                dep_delay=40.0, arr_delay=22.0,
            ),
        ]
    )
    preds = _run(flights)
    post = preds[(preds["flight_id"] == "f1") & (preds["model"] == "post_departure")]
    assert post.iloc[0]["predicted_delay"] == 40.0


def test_rotation_state_does_not_cross_a_day_boundary():
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", date(2026, 1, 15), "DEN", "ORD", "2026-01-15T08:00:00",
                actual_dep="2026-01-15T08:00:00", actual_arr="2026-01-15T10:30:00",
                dep_delay=0.0, arr_delay=22.0,
            ),
            # same tail, NEXT day -- must not inherit yesterday's inbound delay
            _flight("f2", "N1", date(2026, 1, 16), "ORD", "LAX", "2026-01-16T12:00:00"),
        ]
    )
    preds = _run(flights)
    f2_rotation = preds[(preds["flight_id"] == "f2") & (preds["model"] == "rotation")]
    assert f2_rotation.iloc[0]["predicted_delay"] == 0.0


def test_cancelled_flight_never_leaks_into_pending_state():
    # Regression test for a real bug: cancelled flights never get a
    # "departed" event to remove them from pending, so a version that
    # added every scheduled flight to pending leaked one entry per
    # cancellation for the rest of the replay -- harmless at small scale,
    # but the dominant cost of a full month's run (hundreds of thousands
    # of landed-event re-score lookups, each scanning every leaked entry).
    # See DECISIONS.md.
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [_flight("f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00", cancelled=True)]
    )
    stream = build_event_stream(flights)
    scorer = BacktestScorer(airport_tz_map=TZ_MAP)
    for event in stream.to_dict("records"):
        scorer.handle_event(event)
    assert scorer.pending == {}


def test_leg_index_stays_frozen_at_schedule_time_through_later_legs():
    # Regression test for a real bug (found in Milestone 7): _feature_dict
    # used to re-read the *live* leg_index counter instead of the value
    # handle_event already froze onto `flight` at schedule time. Since the
    # live counter keeps advancing as this tail's later legs get their own
    # schedule events, f1's leg_index would have been reported as 1 (or
    # higher) by the time f2's schedule event bumped the shared counter --
    # it must stay 0, the value true at f1's own schedule time, through
    # every later re-score. See DECISIONS.md.
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", d, "DEN", "ORD", "2026-01-15T08:00:00",
                actual_dep="2026-01-15T08:00:00", actual_arr="2026-01-15T10:30:00",
                dep_delay=0.0, arr_delay=22.0,
            ),
            _flight("f2", "N1", d, "ORD", "LAX", "2026-01-15T12:00:00"),
        ]
    )
    stream = build_event_stream(flights)
    scorer = BacktestScorer(airport_tz_map=TZ_MAP)
    f1_leg_indices = []
    f2_leg_indices = []
    for event in stream.to_dict("records"):
        for request in scorer.handle_event(event):
            if request["model"] == "schedule_only":
                continue  # same feature dict as "rotation" for this event, skip the duplicate
            if request["flight_id"] == "f1":
                f1_leg_indices.append(request["leg_index"])
            elif request["flight_id"] == "f2":
                f2_leg_indices.append(request["leg_index"])

    assert f1_leg_indices == [0, 0]  # schedule, then departed -- never bumped by f2's schedule
    assert f2_leg_indices == [1, 1]  # schedule, then landed-triggered rescore


def test_score_requests_empty_input_returns_empty_frame():
    result = score_requests([], _models(), CATEGORIES)
    assert len(result) == 0
    assert list(result.columns) == ["flight_id", "event_time", "model", "predicted_delay"]
