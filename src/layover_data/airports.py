"""Builds an IATA-code -> (lat, lon, IANA timezone) lookup for airports BTS
treats as US domestic.

OurAirports' CSV has no timezone column, so it's derived from lat/lon via
timezonefinder (offline, no API calls -- matters since this gets called
once per airport, cached, never per-flight).

Two real gaps found by running this against actual BTS data, not assumed:

1. BTS's "US domestic" On-Time Performance data includes Puerto Rico, Guam,
   American Samoa, the Northern Mariana Islands and the US Virgin Islands
   (e.g. JFK-SJU is a real, scheduled route in the data) -- but OurAirports
   assigns each its own ISO 3166-1 country code, distinct from "US". A
   naive `iso_country == "US"` filter silently drops every flight to/from
   them. US_DOMESTIC_ISO_COUNTRIES makes that inclusion decision explicit.

2. OurAirports is a *current* snapshot; BTS's historical data uses whatever
   IATA code was valid *at the time*. Found live: Palm Beach International
   is listed today under a new name and a new IATA code ("DJT"), with its
   old code "PBI" preserved only in the free-text `keywords` column -- not
   as its `iata_code`. A code-only join against the current table silently
   drops every historical flight still coded under an airport's old
   identifier. Resolved by also indexing 3-letter alias tokens out of
   `keywords`, but only for an alias no primary `iata_code` row already
   claims -- so a rename never shadows a real, currently-assigned code.
"""
import re
from pathlib import Path

import pandas as pd
from timezonefinder import TimezoneFinder

_tf = TimezoneFinder()

# ISO 3166-1 codes OurAirports uses for US territories that BTS's "domestic"
# data includes as ordinary routes.
US_DOMESTIC_ISO_COUNTRIES = ["US", "PR", "GU", "AS", "MP", "VI"]

_IATA_ALIAS_RE = re.compile(r"\b[A-Z]{3}\b")


def load_us_airports(airports_csv: Path) -> pd.DataFrame:
    """Returns one row per US-domestic airport with a non-empty IATA code:
    iata_code, latitude_deg, longitude_deg, tz_name.
    """
    df = pd.read_csv(
        airports_csv,
        usecols=["iso_country", "iata_code", "latitude_deg", "longitude_deg", "type"],
        dtype={"iata_code": "string"},
    )
    df = df[
        df["iso_country"].isin(US_DOMESTIC_ISO_COUNTRIES)
        & df["iata_code"].notna()
        & (df["iata_code"] != "")
    ]
    # A handful of IATA codes are reused across multiple OurAirports rows
    # (e.g. a decommissioned record alongside the active one) -- keep the
    # first, which is consistently the currently-active airport in practice.
    df = df.drop_duplicates(subset="iata_code", keep="first").reset_index(drop=True)

    df["tz_name"] = df.apply(
        lambda row: _tf.timezone_at(lat=row["latitude_deg"], lng=row["longitude_deg"]),
        axis=1,
    )

    unresolved = df[df["tz_name"].isna()]
    if len(unresolved):
        raise ValueError(
            f"timezonefinder could not resolve a timezone for {len(unresolved)} US "
            f"airport(s): {unresolved['iata_code'].tolist()}"
        )

    return df[["iata_code", "latitude_deg", "longitude_deg", "tz_name"]]


def _historical_code_aliases(airports_csv: Path, known_codes: set) -> dict:
    """iata_code-shaped tokens found in `keywords` for US-domestic rows,
    mapped to that row's own lat/lon -- for an alias not already a primary
    iata_code (so a rename can never shadow a currently-assigned code).
    """
    df = pd.read_csv(
        airports_csv,
        usecols=["iso_country", "keywords", "latitude_deg", "longitude_deg"],
        dtype={"keywords": "string"},
    )
    df = df[df["iso_country"].isin(US_DOMESTIC_ISO_COUNTRIES) & df["keywords"].notna()]

    aliases = {}
    for row in df.itertuples(index=False):
        for token in _IATA_ALIAS_RE.findall(row.keywords):
            if token not in known_codes and token not in aliases:
                aliases[token] = (row.latitude_deg, row.longitude_deg)
    return aliases


def build_airport_tz_map(airports_csv: Path) -> dict:
    """iata_code (plus recovered historical aliases) -> IANA timezone name,
    for every airport BTS's domestic data covers.
    """
    df = load_us_airports(airports_csv)
    tz_map = dict(zip(df["iata_code"], df["tz_name"]))

    for code, (lat, lon) in _historical_code_aliases(airports_csv, set(tz_map)).items():
        tz_map[code] = _tf.timezone_at(lat=lat, lng=lon)

    return tz_map
