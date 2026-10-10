#!/usr/bin/env python3
"""Milestone 8: runs a real Temporal Worker -- the process that actually
executes `TripMonitorWorkflow` and its Activities, polling the real
Temporal server (`localhost:7233`) for work.

This is the process a kill test kills and restarts: a worker going down
mid-monitoring is exactly the scenario the spec's own test asks for
("restart Temporal workers and services mid-monitoring; no trip stops
being watched, no duplicate alert") -- see
`scripts/verify_trip_monitoring.py` and DECISIONS.md.
"""
import asyncio
import logging
import sys
from pathlib import Path

from temporalio.client import Client
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_trip import activities, workflow  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TASK_QUEUE = "trip-monitoring"


async def main():
    client = await Client.connect("localhost:7233", namespace="default")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[workflow.TripMonitorWorkflow],
        activities=[
            activities.fetch_leg_assessment,
            activities.send_alert,
            activities.mark_trip_status,
        ],
        # `workflow.py` imports `activities` (for its dataclass input/
        # output types and to pass the activity functions by reference)
        # -- Temporal's deterministic-code sandbox tries to statically
        # validate everything that import transitively pulls in, which
        # includes `requests`/`psycopg2`/`pywebpush`'s real I/O code.
        # Found live: `requests` -> `urllib3` fails the sandbox's static
        # check on `http.client.IncompleteRead.__mro_entries__` outright.
        # These modules only ever actually run inside an Activity (never
        # inside the workflow's own deterministic replay), so they're
        # marked passthrough -- the officially supported way to tell the
        # sandbox "trust this module, don't restrict it" -- rather than
        # restructuring the whole package to avoid the import. See
        # DECISIONS.md.
        workflow_runner=SandboxedWorkflowRunner(
            restrictions=SandboxRestrictions.default.with_passthrough_modules(
                "requests",
                "psycopg2",
                "pywebpush",
                "py_vapid",
                "cryptography",
                "numpy",
                "layover_trip.activities",
                "layover_trip.db",
                "layover_trip.notify",
                "layover_trip.risk",
            )
        ),
    )
    logger.info("worker polling task queue %r", TASK_QUEUE)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
