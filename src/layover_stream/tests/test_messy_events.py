import pandas as pd

from layover_stream.messy_events import inject_messiness


def _clean_stream(n=100):
    return pd.DataFrame(
        [
            {"flight_id": f"f{i}", "event_type": "schedule", "event_time": i, "value": i}
            for i in range(n)
        ]
    )


def test_zero_rates_preserves_order_and_content():
    stream = _clean_stream(20)
    out = inject_messiness(
        stream, shuffle_window=1, duplicate_rate=0, very_late_rate=0, missing_rate=0
    )
    assert out["flight_id"].tolist() == stream["flight_id"].tolist()
    assert len(out) == len(stream)


def test_missing_rate_drops_events_entirely():
    stream = _clean_stream(200)
    out = inject_messiness(stream, duplicate_rate=0, very_late_rate=0, missing_rate=0.1)
    # ~10% dropped, none duplicated, none added -- net count shrinks
    assert len(out) < len(stream)
    assert len(out) >= len(stream) * 0.85  # roughly 10% missing, some tolerance


def test_duplicate_rate_produces_exact_repeats():
    stream = _clean_stream(200)
    out = inject_messiness(stream, duplicate_rate=0.1, very_late_rate=0, missing_rate=0)
    # more rows than the input (duplicates added, nothing dropped)
    assert len(out) > len(stream)
    dup_counts = out["flight_id"].value_counts()
    assert (dup_counts > 1).sum() > 0  # at least some flight_ids appear more than once


def test_very_late_events_end_up_near_the_end_of_produce_order():
    stream = _clean_stream(200)
    out = inject_messiness(
        stream, shuffle_window=4, duplicate_rate=0, very_late_rate=0.02, missing_rate=0
    )
    # the lowest event_time values (earliest, i.e. most likely picked as
    # "very late" since they're displaced furthest from their natural
    # position) should not all be clustered at the start anymore
    first_quarter = out.iloc[: len(out) // 4]
    last_quarter = out.iloc[-len(out) // 4 :]
    # with pure local shuffling only (window=4), an event's position can't
    # move more than ~4 slots -- so any very-early event_time found in the
    # last quarter must have come from the deliberate "very late" path
    assert (last_quarter["event_time"] < len(out) // 4).any()
    assert len(first_quarter) > 0  # sanity: quarters are non-trivial


def test_out_of_order_shuffle_stays_within_local_window():
    stream = _clean_stream(100)
    out = inject_messiness(
        stream, shuffle_window=5, duplicate_rate=0, very_late_rate=0, missing_rate=0
    )
    # every event's produce position must be within its own window of 5
    # (no event teleports far from where it started) since very_late is
    # disabled here
    for produce_pos, event_time in enumerate(out["event_time"]):
        original_window = event_time // 5
        produce_window = produce_pos // 5
        assert abs(original_window - produce_window) <= 1
