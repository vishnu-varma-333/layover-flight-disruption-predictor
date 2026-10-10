from layover_ingest.airport_status import parse_nas_status

# Structure (tag names, nesting, date format) captured live from the real
# FAA feed during development (see DECISIONS.md), not hand-invented.
# PNS is deliberately NOT in MONITORED_AIRPORTS -- it must be filtered out;
# LAX is, and must survive.
SAMPLE_XML = """<?xml version="1.0"?>
<AIRPORT_STATUS_INFORMATION>
<Update_Time>Sat Oct 10 15:02:24 2026 GMT</Update_Time>
<Delay_type>
<Name>Airport Closures</Name>
<Airport_Closure_List>
<Airport>
<ARPT>PNS</ARPT>
<Reason>!PNS 10/060 PNS AD AP CLSD EXC HEL 2HR</Reason>
<Start>Oct 10 at 13:39 UTC.</Start>
<Reopen>Oct 10 at 22:00 UTC.</Reopen>
</Airport>
<Airport>
<ARPT>LAX</ARPT>
<Reason>!LAX 05/277 LAX AD AP CLSD TO NON SKED TRANSIENT GA ACFT</Reason>
<Start>May 27 at 18:26 UTC.</Start>
<Reopen>May 28 at 16:00 UTC.</Reopen>
</Airport>
</Airport_Closure_List>
</Delay_type>
</AIRPORT_STATUS_INFORMATION>
"""


def test_filters_to_monitored_airports_only():
    events = parse_nas_status(SAMPLE_XML)
    codes = {e["iata"] for e in events}
    assert codes == {"LAX"}  # PNS is not monitored, must not appear


def test_captures_category_specific_fields_generically():
    events = parse_nas_status(SAMPLE_XML)
    lax = events[0]
    assert lax["event_type"] == "airport_status"
    assert lax["delay_category"] == "Airport Closures"
    assert lax["Start"] == "May 27 at 18:26 UTC."
    assert lax["Reopen"] == "May 28 at 16:00 UTC."
    assert "CLSD" in lax["Reason"]


def test_update_time_parsed_to_iso_utc():
    events = parse_nas_status(SAMPLE_XML)
    assert events[0]["event_time"] == "2026-10-10T15:02:24+00:00"


def test_no_monitored_airports_in_feed_returns_empty():
    xml = SAMPLE_XML.replace("LAX", "ZZZ")
    events = parse_nas_status(xml)
    assert events == []
