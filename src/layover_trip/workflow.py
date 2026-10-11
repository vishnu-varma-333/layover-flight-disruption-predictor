"""Milestone 8: the durable, multi-day trip-monitoring workflow -- the
spec's own description ("Each trip gets a long-running workflow that
watches its flights for days, survives restarts, and stops after the
trip ends") implemented directly against Temporal's actual guarantees
(see DECISIONS.md for the research pass this was built from), not
approximated with a cron job and a database flag.

Workflow code here is deterministic control flow only: every real-world
operation (HTTP calls to the Milestone 7 prediction service, database
writes, sending notifications) is delegated to an Activity in
`activities.py`. The one duration-sensitive exception,
`workflow.wait_condition(..., timeout=...)`, is Temporal's own durable-
timer-plus-signal-wake primitive, not `asyncio.sleep` -- a bare sleep
would be lost on a worker restart; this is tracked server-side and
survives one.

Why this, not a cron-and-database approach (a real design-decision
point the spec names): a cron job re-reading "which trips need
checking" from a database loses in-flight state on every restart (what
was the last alerted risk level? did the last notification actually
send?) and has to rebuild that from scratch every tick. A Temporal
workflow IS that state, durably, for the whole life of the trip -- no
separate "resume" logic needed, because resuming *is* just a worker
picking the workflow's next task back up.
"""
import asyncio
import dataclasses
from datetime import timedelta
from typing import Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

from layover_trip import activities
from layover_trip.alerts import decide_alert

ACTIVITY_TIMEOUT = timedelta(seconds=30)
ACTIVITY_RETRY_POLICY = RetryPolicy(maximum_attempts=5, backoff_coefficient=2.0)


@dataclasses.dataclass
class TripLegInput:
    trip_leg_id: str
    flight_id: str
    sched_arr_epoch: float
    # None on the trip's last leg -- there's no next connection to protect,
    # so the workflow never even calls the prediction service for it (see
    # the loop below): monitoring an unmonitorable connection would be
    # pure overhead, not a real check.
    next_leg_sched_dep_epoch: Optional[float] = None
    min_connection_min: Optional[int] = None


@dataclasses.dataclass
class TripMonitorInput:
    trip_id: str
    to_email: str
    legs: list[TripLegInput] = dataclasses.field(default_factory=list)
    push_subscription: dict = dataclasses.field(default_factory=dict)
    # Production default would be the spec's "days"-scale cadence (e.g.
    # 15 minutes); overridable so a kill test or demo doesn't have to
    # run for real days to observe a check cycle.
    check_interval_seconds: int = 900


@workflow.defn
class TripMonitorWorkflow:
    def __init__(self) -> None:
        self._stopped = False
        self._latest: dict = {}
        self._last_alerted_p_miss: dict = {}
        self._alert_count: dict = {}
        self._push_subscription: dict = {}

    @workflow.signal
    def stop_watching(self) -> None:
        self._stopped = True

    @workflow.signal
    def update_push_subscription(self, subscription: dict) -> None:
        """Milestone 9: a trip's monitoring workflow is usually already
        running by the time its owner opens the web app and grants
        notification permission (the workflow starts the moment a trip is
        added; subscribing is a later, separate user action) -- a signal,
        not a fresh start parameter, is how a long-running workflow learns
        about something that happens after it's already underway. The
        subscription is also persisted to `trips.push_subscription` by the
        API layer that sends this signal, so a *future* trip (or a worker
        that's re-reading this trip from scratch) picks it up without
        needing a repeat signal."""
        self._push_subscription = subscription

    @workflow.query
    def get_latest_assessments(self) -> dict:
        """Queries never touch workflow state or get written to event
        history -- a cheap, read-only way to ask "what does this trip's
        monitor currently think?" without waiting for the next check
        cycle or affecting it."""
        return self._latest

    @workflow.query
    def get_alert_counts(self) -> dict:
        return dict(self._alert_count)

    @workflow.run
    async def run(self, inp: TripMonitorInput) -> None:
        self._push_subscription = inp.push_subscription
        interval = timedelta(seconds=inp.check_interval_seconds)
        while not self._stopped:
            for leg in inp.legs:
                if leg.next_leg_sched_dep_epoch is None:
                    continue
                await self._check_leg(inp, leg)

            # The durable timer: if a worker is down when this deadline
            # passes, it fires as soon as one resumes polling -- not
            # skipped, not restarted from zero. `stop_watching()` wakes
            # this immediately instead of waiting out the full interval.
            #
            # Found live: on Python 3.9, `asyncio.TimeoutError` is NOT
            # the same class as the builtin `TimeoutError` (they only
            # became aliases in Python 3.11) -- `wait_condition` raises
            # the asyncio one, so a bare `except TimeoutError` silently
            # never catches it. That let the exception escape run()
            # entirely, which Temporal treats as the *workflow itself*
            # failing -- not a timeout being handled, the whole trip
            # monitor crashing after its very first interval. See
            # DECISIONS.md.
            try:
                await workflow.wait_condition(lambda: self._stopped, timeout=interval)
            except asyncio.TimeoutError:
                pass

        await workflow.execute_activity(
            activities.mark_trip_status,
            activities.MarkTripStatusInput(trip_id=inp.trip_id, status="completed"),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )

    async def _check_leg(self, inp: TripMonitorInput, leg: TripLegInput) -> None:
        result = await workflow.execute_activity(
            activities.fetch_leg_assessment,
            activities.LegAssessmentInput(
                trip_leg_id=leg.trip_leg_id,
                flight_id=leg.flight_id,
                next_leg_sched_dep_epoch=leg.next_leg_sched_dep_epoch,
                sched_arr_epoch=leg.sched_arr_epoch,
                min_connection_min=leg.min_connection_min,
            ),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )
        if not result.available or result.p_miss is None:
            return
        self._latest[leg.trip_leg_id] = dataclasses.asdict(result)

        last_alerted = self._last_alerted_p_miss.get(leg.trip_leg_id)
        should_alert, new_last_alerted = decide_alert(result.p_miss, last_alerted)
        self._last_alerted_p_miss[leg.trip_leg_id] = new_last_alerted
        if not should_alert:
            return

        # The dedup key is derived from durable workflow state (this
        # counter), not freshly generated -- a replay re-executing this
        # same logical check recomputes the identical key, so the
        # Activity's own DB-side reservation (see activities.py) can
        # actually catch a duplicate. A fresh uuid4() per attempt would
        # defeat that entirely.
        count = self._alert_count.get(leg.trip_leg_id, 0) + 1
        self._alert_count[leg.trip_leg_id] = count
        dedup_key = f"{leg.trip_leg_id}:{count}"

        await workflow.execute_activity(
            activities.send_alert,
            activities.AlertInput(
                trip_id=inp.trip_id,
                dedup_key=dedup_key,
                to_email=inp.to_email,
                push_subscription=self._push_subscription,
                subject="Your connection may be at risk",
                body=_alert_message(leg, result),
            ),
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=ACTIVITY_RETRY_POLICY,
        )


def _alert_message(leg: TripLegInput, result) -> str:
    pct = round(result.p_miss * 100)
    lines = [f"Risk of missing your connection after {leg.flight_id}: {pct}%."]
    lines.extend(result.explanation[:3])
    return "\n".join(lines)
