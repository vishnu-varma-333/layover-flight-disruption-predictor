"""The alert policy: pure functions, no I/O, no Temporal -- testable
without a workflow runtime, same philosophy as `rotation_state.py`
(Milestone 6) and `scorer.py` (Milestone 4).

The spec's own wording is the policy: "Notifications only when risk
crosses a threshold or changes meaningfully; never duplicated, never
spammed." That's a threshold-plus-hysteresis state machine, not a
stateless "p_miss > X" check -- a stateless check would re-fire on every
periodic re-evaluation for as long as risk stays elevated, which is
exactly the spam the spec rules out.

`decide_alert` is pure: the caller (the workflow, which durably owns
`last_alerted_p_miss` per leg across its whole run -- see workflow.py)
feeds it the current state and gets back whether to alert and what the
new state should be. It never does I/O and never generates an alert id
or dedup key itself -- that's the workflow's job (see `workflow.py` for
why a dedup key must come from deterministic workflow state, not a
freshly generated id, to survive a replay).
"""

ALERT_THRESHOLD = 0.30
ESCALATION_DELTA = 0.15


def decide_alert(p_miss: float, last_alerted_p_miss):
    """Returns `(should_alert, new_last_alerted_p_miss)`.

    - Below threshold: never alerts, and resets state to `None` -- a
      later re-crossing above threshold is a fresh event, not a muted
      continuation of a stale one (hysteresis cuts both ways).
    - At/above threshold, never alerted yet: alerts (the first
      crossing).
    - At/above threshold, already alerted: alerts again only if risk
      climbed by at least `ESCALATION_DELTA` since the last alert --
      "changes meaningfully," not "is still elevated" (which would fire
      on every periodic check while risk hovers near the same level).
    """
    if p_miss < ALERT_THRESHOLD:
        return False, None
    if last_alerted_p_miss is None:
        return True, p_miss
    if p_miss >= last_alerted_p_miss + ESCALATION_DELTA:
        return True, p_miss
    return False, last_alerted_p_miss
