from layover_ingest.weather import parse_metars, parse_tafs

# Real responses captured live from aviationweather.gov during development
# (see DECISIONS.md), not hand-invented.
SAMPLE_METAR = [
    {
        "icaoId": "KDEN",
        "receiptTime": "2026-10-10T14:56:24.571Z",
        "obsTime": 1791643980,
        "reportTime": "2026-10-10T15:00:00.000Z",
        "temp": 20,
        "dewp": 1.1,
        "wdir": 210,
        "wspd": 12,
        "visib": "10+",
        "altim": 1012.6,
        "fltCat": "VFR",
        "rawOb": "METAR KDEN 101453Z 21012KT 10SM FEW090 SCT120 20/01 A2990 RMK AO2 SLP052",
    }
]

SAMPLE_TAF = [
    {
        "icaoId": "KDEN",
        "issueTime": "2026-10-10T14:59:00.000Z",
        "validTimeFrom": 1791644400,
        "validTimeTo": 1791741600,
        "rawTAF": "TAF KDEN 101459Z 1015/1118 22012KT P6SM FEW090 SCT220",
    }
]


def test_parse_metars():
    events = parse_metars(SAMPLE_METAR)
    assert len(events) == 1
    e = events[0]
    assert e["event_type"] == "airport_weather"
    assert e["report_type"] == "metar"
    assert e["icao"] == "KDEN"
    assert e["temp_c"] == 20
    assert e["flight_category"] == "VFR"
    assert e["raw"].startswith("METAR KDEN")


def test_parse_tafs():
    events = parse_tafs(SAMPLE_TAF)
    assert len(events) == 1
    e = events[0]
    assert e["event_type"] == "airport_weather"
    assert e["report_type"] == "taf"
    assert e["icao"] == "KDEN"
    assert e["valid_from"] == 1791644400
    assert e["raw"].startswith("TAF KDEN")
