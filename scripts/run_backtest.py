#!/usr/bin/env python3
"""Milestone 4: replays a real month of flights through Redpanda, scoring
each event as it arrives with the Milestone 3 models, and measures the
headline numbers: warning lead time and detection recall, plus a MAE
cross-check against Milestone 3's offline evaluation.

Usage: run_backtest.py [--days N]  (default: the full July 2026 holdout
month; --days N replays only the first N calendar days, for a fast
correctness check before committing to the full run.)
"""

import argparse
import json
import logging
import sys
import time
import uuid
from pathlib import Path

import mlflow
import pandas as pd
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_backtest.events import build_event_stream  # noqa: E402
from layover_backtest.metrics import (  # noqa: E402
    lead_time_and_recall,
    lead_time_by_leg_position,
    mae_cross_check,
)
from layover_backtest.scorer import BacktestScorer, score_requests  # noqa: E402
from layover_data.airports import build_airport_tz_map  # noqa: E402
from layover_model.features import rotation_features  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
TEST_START_DATE = pd.Timestamp("2026-07-01").date()
BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "backtest.flight_events"


def load_latest_model(client, label: str):
    runs = client.search_runs(
        client.get_experiment_by_name("layover-delay-models").experiment_id,
        filter_string=f"tags.mlflow.runName = '{label}'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise SystemExit(
            f"no MLflow run found with name {label!r} -- run scripts/train_model.py first"
        )
    return mlflow.lightgbm.load_model(f"runs:/{runs[0].info.run_id}/model")


def produce_events(stream: pd.DataFrame):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    # Each run is a clean replay of exactly `len(stream)` events -- delete
    # any leftover topic from a prior (possibly partial/failed) run first,
    # so consume_and_score's event count can never be contaminated by
    # stale messages.
    for f in admin.delete_topics([TOPIC]).values():
        try:
            f.result()
        except Exception:  # noqa: BLE001 -- "topic doesn't exist yet" is fine
            pass
    time.sleep(1)
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for row in stream.to_dict("records"):
        row["event_time"] = row["event_time"].isoformat()
        row["fl_date"] = row["fl_date"].isoformat()
        row["sched_dep_utc"] = row["sched_dep_utc"].isoformat()
        # Without this, librdkafka's local outstanding-message queue (cap
        # 100,000 by default) fills up mid-loop and produce() raises
        # BufferError -- found live at ~121K events, not assumed. poll(0)
        # services delivery callbacks non-blockingly so the queue drains
        # continuously instead of only at the final flush().
        while True:
            try:
                producer.produce(TOPIC, json.dumps(row, default=str).encode())
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    producer.flush(60)


def consume_and_score(
    n_events: int, scorer: BacktestScorer, models: dict, categories: dict
) -> pd.DataFrame:
    """Consumes every event (maintaining state, capturing point-in-time-
    correct feature snapshots as it goes -- this part is inherently
    sequential) but defers actual model inference to one batched call per
    model at the very end: a single-row predict() call costs ~2.6ms purely
    from fixed per-call overhead, confirmed by profiling, regardless of
    row count -- calling it once per event instead of once per event
    batch is the difference between finishing in seconds vs. tens of
    minutes. See DECISIONS.md and scorer.py's module docstring.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"backtest-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    all_requests = []
    received = 0
    # Generous: a prior version's 600s was based on a since-fixed O(n)
    # pending-state scan (see DECISIONS.md) and silently timed out on a
    # full month's replay, having consumed only 21.5% of events -- yet
    # still printed "final" metrics against the full month's ground
    # truth, making recall look catastrophically wrong rather than
    # failing loudly. Never again: an incomplete consume is now a hard
    # error below, not a warning the final numbers quietly ignore.
    deadline = time.time() + 3600
    while received < n_events and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        event = json.loads(msg.value())
        event["event_time"] = pd.Timestamp(event["event_time"])
        event["fl_date"] = pd.Timestamp(event["fl_date"]).date()
        event["sched_dep_utc"] = pd.Timestamp(event["sched_dep_utc"])
        all_requests.extend(scorer.handle_event(event))
        received += 1
    consumer.close()

    if received < n_events:
        raise SystemExit(
            f"consumer timed out: only received {received}/{n_events} events -- "
            "refusing to report metrics against an incomplete replay"
        )
    return score_requests(all_requests, models, categories)


def build_ground_truth(flights: pd.DataFrame) -> pd.DataFrame:
    rotation = rotation_features(flights)
    gt = flights[["flight_id", "sched_dep_utc", "arr_delay_min", "cancelled", "diverted"]].copy()
    gt["leg_index"] = rotation["leg_index"]
    return gt[~gt["cancelled"] & ~gt["diverted"] & gt["arr_delay_min"].notna()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=None)
    args = parser.parse_args()

    mlflow.set_tracking_uri(f"file://{REPO_ROOT}/mlruns")
    client = mlflow.tracking.MlflowClient()

    logger.info("loading models from MLflow")
    models = {
        "rotation": load_latest_model(client, "arr_delay_pre_departure_full"),
        "schedule_only": load_latest_model(client, "arr_delay_pre_departure_schedule_only"),
        "post_departure": load_latest_model(client, "arr_delay_post_departure_full"),
    }

    all_flights = pd.read_parquet(PROCESSED_DIR / "flights.parquet")
    categories = {
        col: sorted(all_flights[col].dropna().unique().tolist())
        for col in ("carrier", "origin", "dest")
    }

    test_month = all_flights[all_flights["fl_date"] >= TEST_START_DATE]
    if args.days:
        cutoff = TEST_START_DATE + pd.Timedelta(days=args.days)
        test_month = test_month[test_month["fl_date"] < cutoff]
    logger.info(
        "replaying %d flights (%s)",
        len(test_month),
        "full month" if not args.days else f"first {args.days} days",
    )

    airport_tz_map = build_airport_tz_map(REPO_ROOT / "data" / "raw" / "reference" / "airports.csv")

    t0 = time.time()
    stream = build_event_stream(test_month)
    logger.info("built %d events in %.1fs", len(stream), time.time() - t0)

    t0 = time.time()
    produce_events(stream)
    produce_time = time.time() - t0
    logger.info("produced %d events to Kafka in %.1fs", len(stream), produce_time)

    scorer = BacktestScorer(airport_tz_map=airport_tz_map)
    t0 = time.time()
    predictions = consume_and_score(len(stream), scorer, models, categories)
    consume_time = time.time() - t0
    logger.info(
        "consumed + scored %d events in %.1fs (%.0f events/sec)",
        len(stream),
        consume_time,
        len(stream) / consume_time,
    )

    ground_truth = build_ground_truth(test_month)

    print("\n--- lead time and detection recall (15+ min delay) ---")
    for model in ("rotation", "schedule_only"):
        r = lead_time_and_recall(predictions, ground_truth, model)
        print(
            f"\n{model}: {r['n_detected']}/{r['n_disrupted']} detected (recall {r['recall']:.1%})"
        )
        print(f"  median lead time (detected only): {r['median_lead_time_min']:.1f} min")
        print(f"  mean lead time (detected only): {r['mean_lead_time_min']:.1f} min")

    print("\n--- lead time by rotation position (rotation model) ---")
    seg = lead_time_by_leg_position(predictions, ground_truth, "rotation")
    for name, r in seg.items():
        print(
            f"{name}: recall {r['recall']:.1%}, "
            f"median lead {r['median_lead_time_min']:.1f} min, n={r['n_disrupted']}"
        )

    print("\n--- MAE cross-check vs Milestone 3 offline evaluation ---")
    for model in ("rotation", "schedule_only", "post_departure"):
        m = mae_cross_check(predictions, ground_truth, model)
        print(f"{model}: MAE {m['mae']:.2f} min (n={m['n']})")

    print(f"\nreplay throughput: {len(stream) / consume_time:.0f} events/sec")


if __name__ == "__main__":
    main()
