from layover_trip.alerts import ALERT_THRESHOLD, ESCALATION_DELTA, decide_alert


def test_below_threshold_never_alerts():
    should, state = decide_alert(ALERT_THRESHOLD - 0.01, None)
    assert should is False
    assert state is None


def test_first_crossing_above_threshold_alerts():
    should, state = decide_alert(ALERT_THRESHOLD + 0.05, None)
    assert should is True
    assert state == ALERT_THRESHOLD + 0.05


def test_staying_elevated_without_meaningful_change_does_not_re_alert():
    # Already alerted at 0.40; risk ticks up slightly to 0.45 on the next
    # check -- still "elevated," not "changed meaningfully" -- must not
    # spam a second alert for every periodic re-check.
    should, state = decide_alert(0.45, 0.40)
    assert should is False
    assert state == 0.40  # unchanged -- still anchored to the original alert


def test_meaningful_escalation_re_alerts():
    should, state = decide_alert(0.40 + ESCALATION_DELTA, 0.40)
    assert should is True
    assert state == 0.40 + ESCALATION_DELTA


def test_dropping_below_threshold_resets_state():
    should, state = decide_alert(ALERT_THRESHOLD - 0.05, 0.60)
    assert should is False
    assert state is None


def test_drop_then_recross_is_a_fresh_alert_not_a_muted_continuation():
    # Risk was high, dropped below threshold (reset), then crossed back
    # above it at a *lower* level than before -- this must still alert,
    # since it's a genuinely new event, not an echo of the old one.
    _, state = decide_alert(ALERT_THRESHOLD - 0.05, 0.60)
    should, new_state = decide_alert(ALERT_THRESHOLD + 0.01, state)
    assert should is True
    assert new_state == ALERT_THRESHOLD + 0.01
