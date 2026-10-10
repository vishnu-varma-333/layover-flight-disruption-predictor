"""Cleans a raw BTS On-Time Performance monthly CSV into UTC-timestamped
flight records matching the Flight entity in the data model.

Key correctness decision (see DECISIONS.md): BTS gives scheduled/actual
times as local HHMM integers on the flight's departure date (FlightDate), but
never gives an explicit arrival *date* -- a flight landing after midnight
local time is still just "some HHMM" with no day marker. Rather than
inferring a date rollover by comparing HHMM values (ambiguous across a
timezone change, and outright wrong near DST transitions), arrival UTC is
derived as departure UTC + elapsed minutes. BTS reports elapsed time
directly (CRSElapsedTime / ActualElapsedTime) and it's immune to all of
that ambiguity.

BTS's "2400" convention (a schedule ending exactly at midnight is coded as
hour 24 on the *current* FlightDate, not hour 0 on the next day) is handled
explicitly -- it is easy to silently mis-parse as 00:00 same-day, which is
off by exactly one day.
"""
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

USECOLS = [
    "FlightDate",
    "Reporting_Airline",
    "Flight_Number_Reporting_Airline",
    "Tail_Number",
    "Origin",
    "Dest",
    "CRSDepTime",
    "DepTime",
    "CRSElapsedTime",
    "ActualElapsedTime",
    "DepDelay",
    "ArrDelay",
    "Cancelled",
    "Diverted",
]

_tz_cache: dict = {}


def _zoneinfo(tz_name: str) -> ZoneInfo:
    zi = _tz_cache.get(tz_name)
    if zi is None:
        zi = ZoneInfo(tz_name)
        _tz_cache[tz_name] = zi
    return zi


def parse_hhmm(raw) -> "tuple[int, int, int] | None":
    """BTS HHMM -> (hour, minute, day_offset). None for missing/invalid.

    "2400" means midnight at the *end* of FlightDate, i.e. 00:00 the next day
    -- day_offset=1 signals that to the caller.
    """
    if pd.isna(raw):
        return None
    v = int(raw)
    if v == 2400:
        return (0, 0, 1)
    if not (0 <= v <= 2359):
        return None
    hour, minute = divmod(v, 100)
    if minute > 59:
        return None
    return (hour, minute, 0)


def local_to_utc(flight_date: date, hhmm_raw, tz_name: str) -> "pd.Timestamp | None":
    parsed = parse_hhmm(hhmm_raw)
    if parsed is None or tz_name is None:
        return None
    hour, minute, day_offset = parsed
    naive = datetime.combine(flight_date + timedelta(days=day_offset), time(hour, minute))
    # fold=0 (default): on the ~2.5 hours/year of an ambiguous DST
    # fall-back local time, this resolves to the earlier (pre-transition)
    # UTC offset. A known, documented approximation -- not silently assumed.
    localized = naive.replace(tzinfo=_zoneinfo(tz_name))
    return pd.Timestamp(localized.astimezone(timezone.utc))


def clean_month(csv_path, airport_tz_map: dict) -> pd.DataFrame:
    raw = pd.read_csv(
        csv_path,
        usecols=USECOLS,
        dtype={
            "Reporting_Airline": "string",
            "Tail_Number": "string",
            "Origin": "string",
            "Dest": "string",
        },
        parse_dates=["FlightDate"],
    )

    rows = []
    unresolved_airports = set()
    non_positive_elapsed_count = 0
    for r in raw.itertuples(index=False):
        origin_tz = airport_tz_map.get(r.Origin)
        if origin_tz is None:
            unresolved_airports.add(r.Origin)
            continue
        fl_date = r.FlightDate.date()

        # BTS's own data has a small number of rows (confirmed live: 14 out
        # of ~7M, all with a negative CRSElapsedTime) where elapsed time is
        # zero or negative -- a source data defect, not a timezone issue.
        # Treated like a missing value rather than propagated into a
        # logically-impossible arrival-before-departure timestamp.
        sched_dep_utc = local_to_utc(fl_date, r.CRSDepTime, origin_tz)
        sched_arr_utc = (
            sched_dep_utc + timedelta(minutes=r.CRSElapsedTime)
            if sched_dep_utc is not None and pd.notna(r.CRSElapsedTime) and r.CRSElapsedTime > 0
            else None
        )
        if sched_dep_utc is not None and pd.notna(r.CRSElapsedTime) and r.CRSElapsedTime <= 0:
            non_positive_elapsed_count += 1

        cancelled = bool(r.Cancelled)
        diverted = bool(r.Diverted)
        actual_dep_utc = (
            local_to_utc(fl_date, r.DepTime, origin_tz) if not cancelled else None
        )
        # A diverted flight's ActualElapsedTime (when present) describes
        # time to the diversion airport, not Dest -- using it here would be
        # actively wrong, not just missing. Same non-positive-elapsed guard
        # as sched_arr_utc above.
        actual_arr_utc = (
            actual_dep_utc + timedelta(minutes=r.ActualElapsedTime)
            if actual_dep_utc is not None
            and pd.notna(r.ActualElapsedTime)
            and r.ActualElapsedTime > 0
            and not diverted
            else None
        )

        flight_id = (
            f"{fl_date.isoformat()}-{r.Reporting_Airline}{int(r.Flight_Number_Reporting_Airline)}"
            f"-{r.Origin}-{r.Dest}"
        )

        rows.append(
            {
                "flight_id": flight_id,
                "fl_date": fl_date,
                "carrier": r.Reporting_Airline,
                "flight_number": int(r.Flight_Number_Reporting_Airline),
                "tail_number": r.Tail_Number if pd.notna(r.Tail_Number) and r.Tail_Number else None,
                "origin": r.Origin,
                "dest": r.Dest,
                "sched_dep_utc": sched_dep_utc,
                "sched_arr_utc": sched_arr_utc,
                "actual_dep_utc": actual_dep_utc,
                "actual_arr_utc": actual_arr_utc,
                "dep_delay_min": r.DepDelay if pd.notna(r.DepDelay) else None,
                "arr_delay_min": r.ArrDelay if pd.notna(r.ArrDelay) else None,
                "cancelled": cancelled,
                "diverted": diverted,
            }
        )

    if unresolved_airports:
        import logging

        logging.getLogger(__name__).warning(
            "%d row(s) skipped: no timezone for airport code(s) %s",
            len(unresolved_airports),
            sorted(unresolved_airports),
        )

    if non_positive_elapsed_count:
        import logging

        logging.getLogger(__name__).warning(
            "%d row(s) had a zero/negative CRSElapsedTime in the source data -- "
            "sched_arr_utc left null rather than computed from it",
            non_positive_elapsed_count,
        )

    df = pd.DataFrame(rows)
    before = len(df)
    df = df.drop_duplicates(subset="flight_id", keep="first")
    if len(df) != before:
        import logging

        logging.getLogger(__name__).warning(
            "%d duplicate flight_id(s) collapsed (same carrier+number+route+date "
            "operated more than once -- known v1 limitation)",
            before - len(df),
        )
    return df
