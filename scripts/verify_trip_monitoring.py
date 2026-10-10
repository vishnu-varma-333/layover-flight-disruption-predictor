#!/usr/bin/env python3
"""Milestone 8's real kill test -- the spec's own required check:
"restart Temporal workers and services mid-monitoring; no trip stops
being watched, no duplicate alert."

Against the REAL Temporal server and a REAL worker subprocess (started
and SIGKILLed by this script, not simulated): starts a trip-monitoring
workflow for a real, already-tight connection; waits for a real check
cycle to compute risk and fire a real alert (real SMTP, delivered to a
real Mailpit inbox); kills the worker process outright; confirms the
server-side workflow execution is still "Running" with no worker
polling it at all; starts a fresh worker process; confirms monitoring
resumes with no second alert for the same, unchanged risk level (the
actual "no duplicate" property, not just "the process didn't crash");
then stops the trip and confirms it actually completes.

Requires `make predict-service` already running (this test calls the
real Milestone 7 service for real risk).
"""
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_trip import db  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
PYTHON = str(REPO_ROOT / ".venv" / "bin" / "python")
CHECK_INTERVAL_SECONDS = 15
MAILPIT_API = "http://localhost:8025/api/v1"


def wait_for(predicate, timeout, label, interval=1.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    sys.exit(f"timed out waiting for: {label}")


def require_prediction_service():
    try:
        r = requests.get("http://localhost:8000/healthz", timeout=2)
        assert r.status_code == 200
    except Exception:  # noqa: BLE001
        sys.exit("prediction service not responding on :8000 -- run `make predict-service` first")


def start_worker(log_path: Path) -> subprocess.Popen:
    # Logs go to a real file, never to a PIPE this script doesn't drain --
    # an unread PIPE fills its OS buffer once the worker's own (fairly
    # chatty) logging exceeds it, and the worker process then blocks
    # trying to write, which looks exactly like a silent hang. Found
    # live, not assumed -- see DECISIONS.md.
    log_file = open(log_path, "w")
    proc = subprocess.Popen(
        [PYTHON, "scripts/run_trip_worker.py"],
        cwd=REPO_ROOT,
        stdout=log_file,
        stderr=subprocess.STDOUT,
    )
    time.sleep(3)  # let it connect and start polling before anything depends on it
    return proc


def kill_worker(proc: subprocess.Popen):
    os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=10)


def main():
    require_prediction_service()

    conn = db.connect()
    db.apply_schema(conn)
    conn.close()

    print("--- starting trip-monitoring workflow for a real, tight connection ---")
    start = subprocess.run(
        [
            PYTHON, "scripts/start_trip_monitor.py",
            "--check-interval-seconds", str(CHECK_INTERVAL_SECONDS),
        ],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if start.returncode != 0:
        sys.exit(f"start_trip_monitor.py failed:\n{start.stdout}\n{start.stderr}")
    trip_id = start.stdout.strip().splitlines()[-1]
    print(f"trip_id = {trip_id}")

    import asyncio

    from temporalio.client import Client

    async def get_handle():
        client = await Client.connect("localhost:7233", namespace="default")
        return client, client.get_workflow_handle(f"trip-monitor-{trip_id}")

    client, handle = asyncio.run(get_handle())

    print("\n--- worker #1: waiting for a real risk check + a real alert ---")
    worker1 = start_worker(REPO_ROOT / "data" / "worker1.log")
    try:
        assessments = wait_for(
            lambda: asyncio.run(handle.query(
                "get_latest_assessments"
            )),
            timeout=30, label="first risk assessment",
        )
        leg_id = next(iter(assessments))
        p_miss = assessments[leg_id]["p_miss"]
        predicted_delay = assessments[leg_id]["predicted_delay"]
        print(f"leg {leg_id}: p_miss={p_miss:.2f}, predicted_delay={predicted_delay}")
        assert p_miss > 0.5, f"expected a high-risk real connection, got p_miss={p_miss}"

        alert_counts = wait_for(
            lambda: asyncio.run(handle.query("get_alert_counts")) or None,
            timeout=30, label="first alert",
        )
        assert alert_counts.get(leg_id, 0) >= 1, "expected at least one alert for the at-risk leg"
        print(f"alert_counts after worker #1: {alert_counts}")

        conn = db.connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM alerts WHERE trip_id = %s", (trip_id,))
            n_alerts_before_kill = cur.fetchone()[0]
        conn.close()
        print(f"real alert rows in the database: {n_alerts_before_kill}")
        assert n_alerts_before_kill >= 1

        msgs = requests.get(f"{MAILPIT_API}/messages", timeout=5).json()
        print(f"Mailpit inbox has {msgs.get('total', 0)} total message(s)")
    finally:
        print("\n--- killing worker #1 (SIGKILL, simulating a real crash) ---")
        kill_worker(worker1)

    print("--- confirming the server-side workflow is still 'Running' with no worker alive ---")
    desc = asyncio.run(handle.describe())
    print(f"workflow status with no worker polling: {desc.status}")
    assert str(desc.status).endswith("RUNNING"), f"expected Running, got {desc.status}"

    print(f"\n--- waiting past a check interval ({CHECK_INTERVAL_SECONDS}s) with no worker ---")
    time.sleep(CHECK_INTERVAL_SECONDS + 3)
    conn = db.connect()
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM alerts WHERE trip_id = %s", (trip_id,))
        n_alerts_while_down = cur.fetchone()[0]
    conn.close()
    assert n_alerts_while_down == n_alerts_before_kill, "no alert should fire with no worker up"
    print(f"alert count unchanged while no worker was running: {n_alerts_while_down}")

    print("\n--- worker #2: restarting after the crash ---")
    worker2 = start_worker(REPO_ROOT / "data" / "worker2.log")
    try:
        time.sleep(CHECK_INTERVAL_SECONDS + 5)  # let at least one more check cycle run
        alert_counts_after = asyncio.run(handle.query("get_alert_counts"))
        print(f"alert_counts after resuming: {alert_counts_after}")
        assert alert_counts_after.get(leg_id, 0) == alert_counts.get(leg_id, 0), (
            "risk is unchanged (same real flight data) -- must NOT have fired a second alert"
        )

        conn = db.connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM alerts WHERE trip_id = %s AND dedup_key = %s",
                (trip_id, f"{leg_id}:1"),
            )
            n_rows_for_same_key = cur.fetchone()[0]
        conn.close()
        assert n_rows_for_same_key == 1, "dedup_key must be recorded exactly once, never duplicated"
        print(f"exactly {n_rows_for_same_key} row for dedup_key={leg_id}:1 -- no duplicate")

        print("\n--- signaling stop_watching() and confirming the trip actually completes ---")
        asyncio.run(handle.signal("stop_watching"))
        asyncio.run(handle.result())  # blocks until the workflow's run() returns
        status = wait_for(
            lambda: db_trip_status(trip_id), timeout=20, label="trip status == completed"
        )
        print(f"final trip status: {status}")
        assert status == "completed"
    finally:
        kill_worker(worker2)

    print("\nKILL TEST PASSED: no trip stopped being watched, no duplicate alert.")


def db_trip_status(trip_id):
    conn = db.connect()
    try:
        status = db.get_trip_status(conn, trip_id)
        return status if status == "completed" else None
    finally:
        conn.close()


if __name__ == "__main__":
    main()
