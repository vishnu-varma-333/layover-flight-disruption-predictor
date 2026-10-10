#!/usr/bin/env python3
"""Milestone 7: live checks against the real, running prediction service
(not mocks) -- assumes `make predict-service` is already up and
`make sync-features` has populated Redis for the demo day
(2026-07-15, from the Milestone 6 pipeline).

Two kinds of check:
1. HTTP-level sanity on every real flight sampled from Redis: valid
   shape, a numeric predicted_delay, a non-empty evidence-backed
   explanation.
2. A direct cross-check, bypassing HTTP entirely, that re-derives one
   flight's prediction in-process from the exact same Redis record and
   MLflow model the service itself used -- proving the HTTP response is
   the real model's real output, not just a well-shaped stub.
"""
import json
import sys
import time
from pathlib import Path

import mlflow
import mlflow.lightgbm
import pandas as pd
import redis
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_backtest.scorer import MODEL_FEATURE_COLS, prepare_frame  # noqa: E402
from layover_predict.feature_store import KEY_PREFIX  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
SERVICE_URL = "http://localhost:8000"
SAMPLE_SIZE = 15


def wait_for_service(timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{SERVICE_URL}/healthz", timeout=2)
            if r.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(1)
    sys.exit(f"prediction service not responding at {SERVICE_URL} -- run `make predict-service`")


def sample_flight_ids(r: redis.Redis, n: int) -> list:
    ids = []
    for key in r.scan_iter(match=f"{KEY_PREFIX}*", count=100):
        ids.append(key[len(KEY_PREFIX) :])
        if len(ids) >= n:
            break
    return ids


def main():
    wait_for_service()
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    if not r.ping():
        sys.exit("Redis not reachable")

    flight_ids = sample_flight_ids(r, SAMPLE_SIZE)
    if not flight_ids:
        sys.exit("no flights in the online feature store -- run `make sync-features` first")
    print(f"sampled {len(flight_ids)} real flight_ids from Redis")

    results = []
    for fid in flight_ids:
        resp = requests.get(f"{SERVICE_URL}/flights/{fid}/risk", timeout=5)
        if resp.status_code != 200:
            print(f"  FAIL {fid}: HTTP {resp.status_code} {resp.text}")
            continue
        body = resp.json()
        assert isinstance(body["predicted_delay"], (int, float))
        assert body["model_version"]
        assert isinstance(body["explanation"], list) and len(body["explanation"]) > 0
        results.append(body)

    print(f"{len(results)}/{len(flight_ids)} flights returned a valid risk assessment")
    for body in results[:3]:
        print(f"\n{body['flight_id']} ({body['source_event_type']}):")
        model_short = body["model_version"][:8]
        print(f"  predicted_delay: {body['predicted_delay']} min  (model {model_short})")
        for line in body["explanation"]:
            print(f"  - {line}")

    # Cross-check: re-derive the first flight's prediction in-process,
    # straight from Redis + MLflow, bypassing the HTTP layer entirely.
    cross_check_fid = flight_ids[0]
    record = json.loads(r.get(f"{KEY_PREFIX}{cross_check_fid}"))
    model_name = "post_departure" if record.get("dep_delay_min") is not None else "rotation"
    run_name = {
        "rotation": "arr_delay_pre_departure_full",
        "post_departure": "arr_delay_post_departure_full",
    }[model_name]

    mlflow.set_tracking_uri(f"file://{REPO_ROOT}/mlruns")
    client = mlflow.tracking.MlflowClient()
    runs = client.search_runs(
        client.get_experiment_by_name("layover-delay-models").experiment_id,
        filter_string=f"tags.mlflow.runName = '{run_name}'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    model = mlflow.lightgbm.load_model(f"runs:/{runs[0].info.run_id}/model")

    flights = pd.read_parquet(REPO_ROOT / "data" / "processed" / "flights.parquet")
    categories = {
        col: sorted(flights[col].dropna().unique().tolist())
        for col in ("carrier", "origin", "dest")
    }
    request = dict(
        record,
        event_time=record.get("computed_at_event_time"),
        model=model_name,
        dep_delay_min=record.get("dep_delay_min"),
    )
    df = prepare_frame([request], categories)
    expected = float(model.predict(df[MODEL_FEATURE_COLS[model_name]])[0])

    served = next(b for b in results if b["flight_id"] == cross_check_fid)
    print(
        f"\ncross-check {cross_check_fid}: "
        f"service={served['predicted_delay']} direct={expected:.1f}"
    )
    if abs(served["predicted_delay"] - round(expected, 1)) > 1e-6:
        sys.exit("MISMATCH: service's HTTP response does not match a direct re-derivation")
    print("cross-check passed: HTTP response matches a direct, independent re-derivation")


if __name__ == "__main__":
    main()
