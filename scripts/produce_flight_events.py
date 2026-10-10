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
from layover_stream.messy_events import inject_messiness  # noqa: E402
from layover_stream.rotation_state import (  # noqa: E402
    build_feature_update,
    merge_landed_event,
    resolve_inbound,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
TARGET_DATE = pd.Timestamp("2026-07-15").date()
BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "stream.flight_events"


def compute_ground_truth(clean_stream: pd.DataFrame) -> dict:
    """Reference implementation: process the clean, sorted stream in
    plain Python through the exact same pure functions the Flink job
    uses, keeping only each flight's LAST feature update -- what a
    correct system should converge to regardless of delivery order.

    event_time is converted to epoch seconds (float) up front, matching
    exactly what the Flink job sees after a JSON round-trip over Kafka --
    otherwise this reference would compare pandas Timestamps while Flink
    compares floats, and turnaround_minutes (which only computes when
    event_time is numeric) would silently never match.
    """
    tail_state = {}
    pending = {}
    ground_truth = {}

    rows = clean_stream.to_dict("records")
    for event in rows:
        event["event_time"] = event["event_time"].timestamp()

    for event in rows:
        tail = event["tail_number"]
        fl_date = event["fl_date"]
        etype = event["event_type"]

        if etype == "schedule":
            inbound = resolve_inbound(tail_state.get(tail), fl_date)
            ground_truth[event["flight_id"]] = build_feature_update(
                event["flight_id"], tail, fl_date, inbound, "schedule", event["event_time"]
            )
            pending[event["flight_id"]] = event

        elif etype == "departed":
            if event["flight_id"] not in pending:
                continue
            pending.pop(event["flight_id"])
            inbound = resolve_inbound(tail_state.get(tail), fl_date)
            features = build_feature_update(
                event["flight_id"], tail, fl_date, inbound, "departed", event["event_time"]
            )
            features["dep_delay_min"] = event["dep_delay_min"]
            ground_truth[event["flight_id"]] = features

        elif etype == "landed":
            tail_state[tail] = merge_landed_event(tail_state.get(tail), event)
            for fid, p in pending.items():
                if p["tail_number"] != tail or p["fl_date"] != fl_date:
                    continue
                inbound = resolve_inbound(tail_state[tail], fl_date)
                ground_truth[fid] = build_feature_update(
                    fid, tail, fl_date, inbound, "landed", event["event_time"]
                )

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

    ground_truth = compute_ground_truth(clean_stream)
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
