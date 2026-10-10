from pathlib import Path

from layover_data.airports import build_airport_tz_map, load_us_airports

FIXTURE = Path(__file__).parent / "fixtures" / "airports_sample.csv"


def test_known_hub_timezones():
    tz_map = build_airport_tz_map(FIXTURE)
    assert tz_map["DEN"] == "America/Denver"
    assert tz_map["ORD"] == "America/Chicago"
    assert tz_map["JFK"] == "America/New_York"
    assert tz_map["LAX"] == "America/Los_Angeles"


def test_alaska_and_hawaii_not_mapped_to_continental_zones():
    tz_map = build_airport_tz_map(FIXTURE)
    assert tz_map["ANC"] == "America/Anchorage"
    assert tz_map["HNL"] == "Pacific/Honolulu"


def test_duplicate_iata_code_keeps_first_row():
    df = load_us_airports(FIXTURE)
    den_rows = df[df["iata_code"] == "DEN"]
    assert len(den_rows) == 1
    # the first DEN row in the fixture is the real Denver International
    assert den_rows.iloc[0]["latitude_deg"] == 39.861698


def test_us_territory_included_as_domestic():
    # San Juan (PR) is a real route in BTS's "domestic" data despite having
    # its own ISO country code in OurAirports -- must not be dropped.
    tz_map = build_airport_tz_map(FIXTURE)
    assert tz_map["SJU"] == "America/Puerto_Rico"


def test_renamed_airport_old_code_recovered_from_keywords():
    # This fixture mirrors a real gap found live: an airport renamed in the
    # current OurAirports snapshot (new iata_code "DJT") whose old code
    # ("PBI") only survives in the free-text keywords column. Historical
    # BTS data still uses "PBI".
    tz_map = build_airport_tz_map(FIXTURE)
    assert tz_map["DJT"] == "America/New_York"
    assert tz_map["PBI"] == "America/New_York"


def test_keyword_alias_never_shadows_a_real_primary_code():
    # "KPBI" also appears in DJT's keywords but is a 4-letter ICAO code, not
    # a 3-letter IATA one -- must not be picked up as an alias at all.
    tz_map = build_airport_tz_map(FIXTURE)
    assert "KPBI" not in tz_map
