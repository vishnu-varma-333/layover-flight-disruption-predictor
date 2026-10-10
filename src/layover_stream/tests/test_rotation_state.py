from layover_stream.rotation_state import build_feature_update, merge_landed_event, resolve_inbound


def test_resolve_inbound_none_when_no_tail_state():
    assert resolve_inbound(None, "2026-01-15") is None


def test_resolve_inbound_none_across_day_boundary():
    tail_state = {"fl_date": "2026-01-14", "arr_delay_min": 10}
    assert resolve_inbound(tail_state, "2026-01-15") is None


def test_resolve_inbound_returns_same_day_state():
    tail_state = {"fl_date": "2026-01-15", "arr_delay_min": 10}
    assert resolve_inbound(tail_state, "2026-01-15") == tail_state


def test_merge_landed_event_first_ever():
    event = {
        "fl_date": "2026-01-15",
        "dest": "ORD",
        "arr_delay_min": 15.0,
        "dep_delay_min": 5.0,
        "event_time": 1000,
    }
    state = merge_landed_event(None, event)
    assert state["arr_delay_min"] == 15.0
    assert state["cum_delay_today"] == 15.0
    assert state["landed_event_time"] == 1000


def test_merge_landed_event_accumulates_same_day():
    first = merge_landed_event(
        None,
        {
            "fl_date": "2026-01-15",
            "dest": "ORD",
            "arr_delay_min": 10.0,
            "dep_delay_min": 0.0,
            "event_time": 1000,
        },
    )
    second = merge_landed_event(
        first,
        {
            "fl_date": "2026-01-15",
            "dest": "LAX",
            "arr_delay_min": 20.0,
            "dep_delay_min": 5.0,
            "event_time": 2000,
        },
    )
    assert second["cum_delay_today"] == 30.0  # 10 + 20, not just the latest leg's own delay


def test_merge_landed_event_out_of_order_arrival_does_not_overwrite_newer_state():
    # A later (by event_time) landed event is applied first (simulating
    # out-of-order delivery), then an *older* one arrives after it --
    # the older one must be a no-op, not clobber the newer state.
    newer = merge_landed_event(
        None,
        {
            "fl_date": "2026-01-15",
            "dest": "LAX",
            "arr_delay_min": 20.0,
            "dep_delay_min": 5.0,
            "event_time": 2000,
        },
    )
    older_arriving_late = merge_landed_event(
        newer,
        {
            "fl_date": "2026-01-15",
            "dest": "ORD",
            "arr_delay_min": 10.0,
            "dep_delay_min": 0.0,
            "event_time": 1000,
        },
    )
    assert older_arriving_late == newer  # unchanged -- the stale update was rejected


def test_merge_landed_event_resets_across_day_boundary():
    yesterday = merge_landed_event(
        None,
        {
            "fl_date": "2026-01-14",
            "dest": "ORD",
            "arr_delay_min": 10.0,
            "dep_delay_min": 0.0,
            "event_time": 1000,
        },
    )
    today = merge_landed_event(
        yesterday,
        {
            "fl_date": "2026-01-15",
            "dest": "LAX",
            "arr_delay_min": 20.0,
            "dep_delay_min": 5.0,
            "event_time": 2000,
        },
    )
    assert today["cum_delay_today"] == 20.0  # not 30 -- yesterday's total doesn't carry over


def test_build_feature_update_no_inbound():
    f = build_feature_update("f1", "N1", "2026-01-15", None, "schedule", 1000)
    assert f["has_inbound"] is False
    assert f["prev_arr_delay_min"] is None
    assert f["turnaround_minutes"] is None


def test_build_feature_update_with_inbound_computes_turnaround_and_tight_flag():
    inbound = {
        "arr_delay_min": 15.0,
        "dep_delay_min": 5.0,
        "landed_at": 1000,
        "cum_delay_today": 15.0,
    }
    f = build_feature_update("f2", "N1", "2026-01-15", inbound, "schedule", 1000 + 20 * 60)
    assert f["has_inbound"] is True
    assert f["prev_arr_delay_min"] == 15.0
    assert f["turnaround_minutes"] == 20.0
    assert f["tight_turnaround"] is True  # 20 min < 45 min threshold


def test_build_feature_update_loose_turnaround_not_flagged_tight():
    inbound = {"arr_delay_min": 0.0, "dep_delay_min": 0.0, "landed_at": 0, "cum_delay_today": 0.0}
    f = build_feature_update("f3", "N1", "2026-01-15", inbound, "schedule", 60 * 60)
    assert f["turnaround_minutes"] == 60.0
    assert f["tight_turnaround"] is False
