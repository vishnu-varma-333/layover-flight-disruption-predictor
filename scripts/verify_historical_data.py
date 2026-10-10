#!/usr/bin/env python3
"""Live data-quality checks against the processed Parquet output, run with
DuckDB (the spec's chosen offline-data query engine) -- not a unit test
against a fixture, an actual query over the real cleaned dataset.
"""
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"

FAILURES = []


def check(name):
    def decorator(fn):
        def wrapper(con):
            try:
                fn(con)
                print(f"[PASS] {name}")
            except AssertionError as exc:
                print(f"[FAIL] {name}: {exc}")
                FAILURES.append(name)

        return wrapper

    return decorator


@check("no null required fields (flight_id, carrier, origin, dest, fl_date)")
def check_required_fields(con):
    n = con.execute(
        """
        SELECT count(*) FROM flights
        WHERE flight_id IS NULL OR carrier IS NULL OR origin IS NULL
           OR dest IS NULL OR fl_date IS NULL
        """
    ).fetchone()[0]
    assert n == 0, f"{n} rows missing a required field"


@check("flight_id is unique")
def check_flight_id_unique(con):
    n = con.execute(
        "SELECT count(*) - count(DISTINCT flight_id) FROM flights"
    ).fetchone()[0]
    assert n == 0, f"{n} duplicate flight_id(s)"


@check("every non-null scheduled arrival is after scheduled departure")
def check_sched_arrival_after_departure(con):
    n = con.execute(
        """
        SELECT count(*) FROM flights
        WHERE sched_dep_utc IS NOT NULL AND sched_arr_utc IS NOT NULL
          AND sched_arr_utc <= sched_dep_utc
        """
    ).fetchone()[0]
    assert n == 0, f"{n} flights with scheduled arrival at/before scheduled departure"


@check("every non-null actual arrival is after actual departure")
def check_actual_arrival_after_departure(con):
    n = con.execute(
        """
        SELECT count(*) FROM flights
        WHERE actual_dep_utc IS NOT NULL AND actual_arr_utc IS NOT NULL
          AND actual_arr_utc <= actual_dep_utc
        """
    ).fetchone()[0]
    assert n == 0, f"{n} flights with actual arrival at/before actual departure"


@check("cancelled flights have no actual departure/arrival times")
def check_cancelled_has_no_actuals(con):
    n = con.execute(
        """
        SELECT count(*) FROM flights
        WHERE cancelled = true AND (actual_dep_utc IS NOT NULL OR actual_arr_utc IS NOT NULL)
        """
    ).fetchone()[0]
    assert n == 0, f"{n} cancelled flights unexpectedly have actual times"


@check("rotation leg lists reference real flight_ids")
def check_rotation_flight_ids_exist(con):
    n = con.execute(
        """
        SELECT count(*) FROM (
            SELECT UNNEST(flight_ids) AS fid FROM rotations
        ) r
        LEFT JOIN flights f ON f.flight_id = r.fid
        WHERE f.flight_id IS NULL
        """
    ).fetchone()[0]
    assert n == 0, f"{n} rotation leg(s) reference a flight_id not present in flights"


@check("rotation leg count matches the aircraft's actual flight count that day")
def check_rotation_leg_counts_consistent(con):
    n = con.execute(
        """
        WITH actual_counts AS (
            SELECT tail_number, fl_date, count(*) AS n
            FROM flights
            WHERE tail_number IS NOT NULL
            GROUP BY tail_number, fl_date
        )
        SELECT count(*) FROM rotations r
        JOIN actual_counts a ON a.tail_number = r.tail_number AND a.fl_date = r.fl_date
        WHERE a.n != r.n_legs
        """
    ).fetchone()[0]
    assert n == 0, f"{n} rotation(s) with n_legs not matching actual flight count"


def main():
    flights_path = PROCESSED_DIR / "flights.parquet"
    rotations_path = PROCESSED_DIR / "rotations.parquet"
    if not flights_path.exists() or not rotations_path.exists():
        sys.exit(
            "processed Parquet files not found -- run scripts/build_historical_data.py first"
        )

    con = duckdb.connect()
    con.execute(f"CREATE VIEW flights AS SELECT * FROM read_parquet('{flights_path}')")
    con.execute(f"CREATE VIEW rotations AS SELECT * FROM read_parquet('{rotations_path}')")

    n_flights = con.execute("SELECT count(*) FROM flights").fetchone()[0]
    n_rotations = con.execute("SELECT count(*) FROM rotations").fetchone()[0]
    print(f"loaded {n_flights} flights, {n_rotations} rotations\n")

    for fn in (
        check_required_fields,
        check_flight_id_unique,
        check_sched_arrival_after_departure,
        check_actual_arrival_after_departure,
        check_cancelled_has_no_actuals,
        check_rotation_flight_ids_exist,
        check_rotation_leg_counts_consistent,
    ):
        fn(con)

    if FAILURES:
        print(f"\n{len(FAILURES)} check(s) failed: {', '.join(FAILURES)}")
        sys.exit(1)
    print("\nAll historical-data checks passed.")


if __name__ == "__main__":
    main()
