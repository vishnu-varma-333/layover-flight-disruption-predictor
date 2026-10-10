from datetime import date

import pandas as pd

from layover_model.features import build_feature_table

TZ_MAP = {"DEN": "America/Denver", "ORD": "America/Chicago"}


def _flight(
    flight_id,
    tail,
    fl_date,
    carrier,
    origin,
    dest,
    sched_dep_utc,
    actual_arr_utc=None,
    dep_delay_min=None,
    arr_delay_min=None,
    cancelled=False,
    diverted=False,
):
    return {
        "flight_id": flight_id,
        "fl_date": fl_date,
        "carrier": carrier,
        "tail_number": tail,
        "origin": origin,
        "dest": dest,
        "sched_dep_utc": pd.Timestamp(sched_dep_utc, tz="UTC"),
        "actual_arr_utc": pd.Timestamp(actual_arr_utc, tz="UTC") if actual_arr_utc else pd.NaT,
        "dep_delay_min": dep_delay_min,
        "arr_delay_min": arr_delay_min,
        "cancelled": cancelled,
        "diverted": diverted,
    }


def test_local_hour_dow_month_match_stdlib_zoneinfo():
    from zoneinfo import ZoneInfo

    d = date(2026, 1, 15)
    flights = pd.DataFrame([_flight("f1", "N1", d, "UA", "DEN", "ORD", "2026-01-15T15:00:00")])
    out = build_feature_table(flights, TZ_MAP)

    expected_local = pd.Timestamp("2026-01-15T15:00:00", tz="UTC").astimezone(
        ZoneInfo("America/Denver")
    )
    row = out.iloc[0]
    assert row["dep_hour_local"] == expected_local.hour
    assert row["day_of_week"] == expected_local.weekday()
    assert row["month"] == expected_local.month


def test_rotation_features_first_leg_has_no_inbound():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [_flight("f1", "N1", d, "UA", "DEN", "ORD", "2026-01-15T08:00:00")]
    )
    out = build_feature_table(flights, TZ_MAP)
    row = out.iloc[0]
    assert row["leg_index"] == 0
    assert not row["has_inbound"]
    assert pd.isna(row["prev_arr_delay_min"])
    assert pd.isna(row["turnaround_minutes"])


def test_rotation_features_second_leg_sees_first_legs_outcome_not_its_own():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N1", d, "UA", "DEN", "ORD", "2026-01-15T08:00:00",
                actual_arr_utc="2026-01-15T10:30:00", arr_delay_min=15.0,
            ),
            _flight(
                "f2", "N1", d, "UA", "ORD", "LAX", "2026-01-15T12:00:00",
                actual_arr_utc="2026-01-15T14:00:00", arr_delay_min=-5.0,
            ),
        ]
    )
    out = build_feature_table(flights, TZ_MAP)
    leg2 = out[out["flight_id"] == "f2"].iloc[0]

    assert leg2["leg_index"] == 1
    assert leg2["has_inbound"]
    # leg2 sees leg1's outcome (15.0), never its own (-5.0) -- no leakage
    assert leg2["prev_arr_delay_min"] == 15.0
    assert leg2["arr_delay_min"] == -5.0
    # turnaround = leg2 sched dep (12:00) - leg1 actual arr (10:30) = 90 min
    assert leg2["turnaround_minutes"] == 90.0
    assert not leg2["chain_break"]


def test_chain_break_detected_when_origin_does_not_match_previous_dest():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N2", d, "UA", "DEN", "ORD", "2026-01-15T08:00:00",
                actual_arr_utc="2026-01-15T10:30:00", arr_delay_min=15.0,
            ),
            # break: departs from JFK, not ORD where f1 landed
            _flight(
                "f2", "N2", d, "UA", "JFK", "LAX", "2026-01-15T12:00:00",
                actual_arr_utc="2026-01-15T14:00:00", arr_delay_min=-5.0,
            ),
        ]
    )
    out = build_feature_table(flights, TZ_MAP)
    leg2 = out[out["flight_id"] == "f2"].iloc[0]
    assert leg2["chain_break"]


def test_tight_turnaround_flag_set_by_threshold_not_missing():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N4", d, "UA", "DEN", "ORD", "2026-01-15T08:00:00",
                actual_arr_utc="2026-01-15T10:00:00", arr_delay_min=0.0,
            ),
            # turnaround = 30 min -- below the 45-min threshold -> tight
            _flight(
                "f2", "N4", d, "UA", "ORD", "LAX", "2026-01-15T10:30:00",
                actual_arr_utc="2026-01-15T13:00:00", arr_delay_min=0.0,
            ),
            # turnaround = 60 min -- above the threshold -> not tight
            _flight("f3", "N4", d, "UA", "LAX", "SEA", "2026-01-15T14:00:00"),
        ]
    )
    out = build_feature_table(flights, TZ_MAP)
    assert out[out["flight_id"] == "f1"].iloc[0]["tight_turnaround"] != out[
        out["flight_id"] == "f1"
    ].iloc[0]["tight_turnaround"]  # NaN != NaN -- no inbound leg
    assert out[out["flight_id"] == "f2"].iloc[0]["tight_turnaround"]
    assert not out[out["flight_id"] == "f3"].iloc[0]["tight_turnaround"]


def test_cumulative_delay_sums_prior_legs_only_not_its_own():
    d = date(2026, 1, 15)
    flights = pd.DataFrame(
        [
            _flight(
                "f1", "N5", d, "UA", "DEN", "ORD", "2026-01-15T08:00:00",
                actual_arr_utc="2026-01-15T10:00:00", arr_delay_min=10.0,
            ),
            _flight(
                "f2", "N5", d, "UA", "ORD", "LAX", "2026-01-15T11:00:00",
                actual_arr_utc="2026-01-15T13:00:00", arr_delay_min=20.0,
            ),
            _flight(
                "f3", "N5", d, "UA", "LAX", "SEA", "2026-01-15T14:00:00",
                arr_delay_min=999.0,  # must never leak into its own cum_delay_today
            ),
        ]
    )
    out = build_feature_table(flights, TZ_MAP)
    assert pd.isna(out[out["flight_id"] == "f1"].iloc[0]["cum_delay_today"])
    assert out[out["flight_id"] == "f2"].iloc[0]["cum_delay_today"] == 10.0
    assert out[out["flight_id"] == "f3"].iloc[0]["cum_delay_today"] == 30.0


def test_output_preserves_original_row_order():
    d = date(2026, 1, 15)
    # Deliberately out of chronological order -- build_feature_table sorts
    # internally for the rotation computation but must return rows in the
    # order they came in.
    flights = pd.DataFrame(
        [
            _flight("f2", "N3", d, "UA", "ORD", "LAX", "2026-01-15T12:00:00"),
            _flight("f1", "N3", d, "UA", "DEN", "ORD", "2026-01-15T08:00:00"),
        ]
    )
    out = build_feature_table(flights, TZ_MAP)
    assert list(out["flight_id"]) == ["f2", "f1"]
