from layover_ingest.opensky import CreditBudget, parse_state_vectors

AIRPORT = {"iata": "DEN", "icao": "KDEN", "lat": 39.86, "lon": -104.67}

# A trimmed real response captured live from /states/all during development
# (see DECISIONS.md) -- not hand-invented, so it matches OpenSky's actual
# field order and null-handling.
SAMPLE_PAYLOAD = {
    "time": 1791644568,
    "states": [
        [
            "a40cd9", "FFT1230 ", "United States", 1791644567, 1791644567,
            -104.685, 39.8731, None, True, 13.89, 180, None, None, None,
            "3334", False, 0,
        ],
        # no position fix yet (longitude/latitude null) -- must be skipped
        [
            "abc123", "TEST1   ", "United States", None, 1791644500,
            None, None, None, False, None, None, None, None, None,
            None, False, 0,
        ],
    ],
}


def test_parses_real_shaped_state_vector():
    events = parse_state_vectors(SAMPLE_PAYLOAD, AIRPORT)
    assert len(events) == 1  # the null-position row is skipped
    event = events[0]
    assert event["event_type"] == "aircraft_position"
    assert event["icao24"] == "a40cd9"
    assert event["callsign"] == "FFT1230"  # whitespace-padded in the API, stripped here
    assert event["near_airport"] == "DEN"
    assert event["longitude"] == -104.685
    assert event["latitude"] == 39.8731
    assert event["on_ground"] is True
    assert event["event_time"] == 1791644567


def test_null_callsign_becomes_none_not_empty_string():
    payload = {
        "time": 1,
        "states": [
            [
                "x", "        ", "US", 1, 1, 1.0, 2.0, None, False, None,
                None, None, None, None, None, False, 0,
            ]
        ],
    }
    events = parse_state_vectors(payload, AIRPORT)
    assert events[0]["callsign"] is None


def test_credit_budget_allows_polling_until_reserve_hit():
    budget = CreditBudget(reserve=10)
    assert budget.can_poll()  # unknown remaining -- optimistic until first response

    budget.update({"X-Rate-Limit-Remaining": "50"})
    assert budget.can_poll()

    budget.update({"X-Rate-Limit-Remaining": "10"})
    assert not budget.can_poll()  # at the reserve, not above it

    budget.update({"X-Rate-Limit-Remaining": "9"})
    assert not budget.can_poll()


def test_credit_budget_ignores_missing_header():
    budget = CreditBudget(reserve=10)
    budget.update({})
    assert budget.remaining is None
    assert budget.can_poll()
