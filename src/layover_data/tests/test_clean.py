from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from layover_data.clean import USECOLS, clean_month, local_to_utc, parse_hhmm

TZ_MAP = {"DEN": "America/Denver", "ORD": "America/Chicago", "LAX": "America/Los_Angeles"}


def _row(
    fl_date,
    carrier,
    fl_num,
    tail,
    origin,
    dest,
    crs_dep,
    dep,
    crs_elapsed,
    actual_elapsed,
    dep_delay,
    arr_delay,
    cancelled,
    diverted,
):
    return dict(
        zip(
            USECOLS,
            [
                fl_date,
                carrier,
                fl_num,
                tail,
                origin,
                dest,
                crs_dep,
                dep,
                crs_elapsed,
                actual_elapsed,
                dep_delay,
                arr_delay,
                cancelled,
                diverted,
            ],
        )
    )


def _write_rows(tmp_path, rows):
    df = pd.DataFrame(rows, columns=USECOLS)
    path = tmp_path / "sample.csv"
    df.to_csv(path, index=False)
    return path


def test_parse_hhmm_normal():
    assert parse_hhmm(805) == (8, 5, 0)
    assert parse_hhmm(1430) == (14, 30, 0)


def test_parse_hhmm_midnight_2400_rolls_to_next_day():
    # BTS's own convention: 2400 = end of the current FL_DATE, i.e. 00:00
    # the *next* day -- not 00:00 the same day. Off-by-one-day if missed.
    assert parse_hhmm(2400) == (0, 0, 1)


def test_parse_hhmm_missing_is_none():
    assert parse_hhmm(float("nan")) is None


def test_local_to_utc_basic_conversion():
    # Jan 15 is winter -> Denver is on MST (UTC-7), unambiguous.
    result = local_to_utc(datetime(2026, 1, 15).date(), 800, "America/Denver")
    assert result == pd.Timestamp("2026-01-15T15:00:00", tz="UTC")


def test_local_to_utc_2400_rollover_crosses_into_next_utc_day():
    # CRS_DEP_TIME=2400 on Jan 10 in Chicago (CST, UTC-6) -> Jan 11 00:00
    # local -> Jan 11 06:00 UTC, not Jan 10 anything.
    result = local_to_utc(datetime(2026, 1, 10).date(), 2400, "America/Chicago")
    assert result == pd.Timestamp("2026-01-11T06:00:00", tz="UTC")


def test_local_to_utc_matches_stdlib_on_ambiguous_dst_fallback_time():
    # Nov 1 2026, America/Chicago falls back at 2:00 AM -> 1:30 AM local
    # occurs twice. local_to_utc uses fold=0 (the pre-transition/earlier
    # offset) -- documented in clean.py, not an accident. This test pins
    # that behavior against Python's own zoneinfo semantics directly,
    # rather than a hand-computed UTC offset that could itself be wrong.
    expected_naive = datetime(2026, 11, 1, 1, 30)
    expected = expected_naive.replace(tzinfo=ZoneInfo("America/Chicago"), fold=0)
    result = local_to_utc(datetime(2026, 11, 1).date(), 130, "America/Chicago")
    assert result == pd.Timestamp(expected.astimezone(ZoneInfo("UTC")))


def test_clean_month_normal_flight(tmp_path):
    path = _write_rows(
        tmp_path,
        [
            _row(
                "2026-01-15",
                "UA",
                100,
                "N12345",
                "DEN",
                "ORD",
                800,
                805,
                150,
                145,
                5,
                -5,
                0,
                0,
            ),
        ],
    )
    df = clean_month(path, TZ_MAP)
    assert len(df) == 1
    row = df.iloc[0]
    assert row["sched_dep_utc"] == pd.Timestamp("2026-01-15T15:00:00", tz="UTC")
    assert row["sched_arr_utc"] == pd.Timestamp("2026-01-15T17:30:00", tz="UTC")
    assert row["actual_dep_utc"] == pd.Timestamp("2026-01-15T15:05:00", tz="UTC")
    assert row["actual_arr_utc"] == pd.Timestamp("2026-01-15T17:30:00", tz="UTC")
    assert not row["cancelled"]
    assert not row["diverted"]


def test_clean_month_cancelled_flight_has_no_actual_times(tmp_path):
    path = _write_rows(
        tmp_path,
        [
            [
                "2026-01-10",
                "UA",
                101,
                "N67890",
                "ORD",
                "DEN",
                2400,
                None,
                120,
                None,
                None,
                None,
                1,
                0,
            ],
        ],
    )
    df = clean_month(path, TZ_MAP)
    row = df.iloc[0]
    assert row["cancelled"]
    assert row["actual_dep_utc"] is None
    assert row["actual_arr_utc"] is None
    # scheduled times still exist for a cancelled flight -- the 2400 rollover
    # still applies to them
    assert row["sched_dep_utc"] == pd.Timestamp("2026-01-11T06:00:00", tz="UTC")


def test_clean_month_negative_elapsed_time_leaves_arrival_null(tmp_path):
    # Found live against real BTS data: a small number of real rows carry a
    # negative CRSElapsedTime (a source data defect -- BTS, not this
    # pipeline). Computing sched_arr_utc from it would produce an arrival
    # before departure; left null instead, same as any other missing value.
    path = _write_rows(
        tmp_path,
        [_row("2026-01-15", "WN", 500, "N55555", "DEN", "ORD", 2105, 2050, -85, -80, 5, 3, 0, 0)],
    )
    df = clean_month(path, TZ_MAP)
    row = df.iloc[0]
    assert row["sched_dep_utc"] is not None
    assert row["sched_arr_utc"] is None
    assert row["actual_dep_utc"] is not None
    assert row["actual_arr_utc"] is None


def test_clean_month_diverted_flight_drops_actual_arrival(tmp_path):
    # A diverted flight's ACTUAL_ELAPSED_TIME (if present at all) describes
    # time to the diversion airport, not DEST -- using it to compute an
    # arrival-at-DEST time would be actively wrong, not just missing.
    path = _write_rows(
        tmp_path,
        [
            [
                "2026-01-15",
                "UA",
                200,
                "N11111",
                "DEN",
                "ORD",
                800,
                805,
                150,
                90,
                5,
                None,
                0,
                1,
            ],
        ],
    )
    df = clean_month(path, TZ_MAP)
    row = df.iloc[0]
    assert row["diverted"]
    assert row["actual_dep_utc"] is not None
    assert row["actual_arr_utc"] is None


def test_clean_month_deduplicates_repeated_flight_id(tmp_path):
    path = _write_rows(
        tmp_path,
        [
            ["2026-01-15", "UA", 300, "N22222", "DEN", "LAX", 900, 905, 90, 88, 5, 3, 0, 0],
            # same carrier+number+route+date (equipment swap mid-day, same
            # flight_id) -- a known v1 limitation, not a crash
            ["2026-01-15", "UA", 300, "N33333", "DEN", "LAX", 1500, 1510, 90, 85, 10, 5, 0, 0],
        ],
    )
    df = clean_month(path, TZ_MAP)
    assert len(df) == 1
    assert df.iloc[0]["tail_number"] == "N22222"


def test_clean_month_unknown_airport_is_skipped_not_crashed(tmp_path):
    path = _write_rows(
        tmp_path,
        [
            ["2026-01-15", "UA", 400, "N44444", "ZZZ", "DEN", 800, 805, 60, 58, 5, 3, 0, 0],
            ["2026-01-15", "UA", 401, "N44444", "DEN", "LAX", 1100, 1105, 90, 88, 5, 3, 0, 0],
        ],
    )
    df = clean_month(path, TZ_MAP)
    assert len(df) == 1
    assert df.iloc[0]["origin"] == "DEN"
