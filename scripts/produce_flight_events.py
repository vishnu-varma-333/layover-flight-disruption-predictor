#!/usr/bin/env python3
"""Milestone 6, step 1: loads one real day of flights, computes a ground-
truth final feature value per flight by processing the *clean* (sorted,
complete) event stream through the same pure rotation_state logic the
Flink job uses, then produces the *deliberately messy* version of that
same stream to Kafka for the real Flink job to consume.

Ground truth is written to data/processed/rotation_ground_truth.json for
verify_rotation_job.py to compare the Flink job's actual output against.
"""
import json
import logging
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_backtest.events import build_event_stream  # noqa: E402
from layover_backtest.scorer import BacktestScorer  # noqa: E402
from layover_data.airports import build_airport_tz_map  # noqa: E402
from layover_stream.messy_events import inject_messiness  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
TARGET_DATE = pd.Timestamp("2026-07-15").date()
BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "stream.flight_events"


def compute_ground_truth(clean_stream: pd.DataFrame, airport_tz_map: dict) -> dict:
    """Reference implementation: drive the same `BacktestScorer` Milestone
    4 already validated against a real month of historical data (it
    reproduces the exact Milestone 3 training feature schema -- schedule
    features included) over the clean, sorted stream, keeping only each
    flight's LAST feature update -- what a correct system should converge
    to regardless of delivery order.

    Milestone 6 originally computed ground truth with a narrower, ad hoc
    reimplementation (`rotation_state.build_feature_update` alone, called
    directly from this function). That's why its "95.8% match" result
    never caught that the real Flink job was silently missing
    `leg_index`, `chain_break` and every schedule feature the models are
    actually trained on: that reference only ever checked the fields it
    itself computed. Found during Milestone 7's research pass (serving
    predictions from these features is what exposed the gap) -- fixed by
    reusing BacktestScorer here, so the ground truth is the same
    already-proven feature logic the trained models expect, not a second,
    independently-written approximation of it. See DECISIONS.md.
    """
    scorer = BacktestScorer(airport_tz_map=airport_tz_map)
    ground_truth = {}
    for event in clean_stream.to_dict("records"):
        for request in scorer.handle_event(event):
            if request["model"] == "schedule_only":
                continue  # identical feature values to the "rotation" request for this event
            ground_truth[request["flight_id"]] = request
    return ground_truth


def produce_events(stream: pd.DataFrame):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for f in admin.delete_topics([TOPIC]).values():
        try:
            f.result()
        except Exception:  # noqa: BLE001 -- "topic doesn't exist yet" is fine
            pass
    time.sleep(1)
    admin.create_topics([NewTopic(TOPIC, num_partitions=1, replication_factor=1)])

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for row in stream.to_dict("records"):
        row["event_time"] = row["event_time"].timestamp()
        row["fl_date"] = row["fl_date"].isoformat()
        while True:
            try:
                producer.produce(TOPIC, json.dumps(row, default=str).encode())
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)
    producer.flush(60)


def main():
    flights = pd.read_parquet(PROCESSED_DIR / "flights.parquet")
    day = flights[flights["fl_date"] == TARGET_DATE]
    logger.info("loaded %d flights for %s", len(day), TARGET_DATE)

    clean_stream = build_event_stream(day)
    logger.info("built %d clean events", len(clean_stream))

    airport_tz_map = build_airport_tz_map(REPO_ROOT / "data" / "raw" / "reference" / "airports.csv")
    ground_truth = compute_ground_truth(clean_stream, airport_tz_map)
    gt_path = PROCESSED_DIR / "rotation_ground_truth.json"
    gt_path.write_text(json.dumps(ground_truth, default=str))
    logger.info("wrote ground truth for %d flights to %s", len(ground_truth), gt_path)

    messy_stream = inject_messiness(clean_stream)
    logger.info(
        "messy stream: %d events (clean had %d) -- includes duplicates, missing, "
        "reordered and deliberately-late events",
        len(messy_stream),
        len(clean_stream),
    )

    produce_events(messy_stream)
    logger.info("produced %d messy events to %s", len(messy_stream), TOPIC)


if __name__ == "__main__":
    main()
