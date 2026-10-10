#!/usr/bin/env python3
"""Milestone 6, step 3: consumes the real Flink job's output
(stream.flight_features, stream.late_events) and compares the final
feature value it converged to for each flight against the ground truth
computed from the clean, sorted stream.

A non-trivial mismatch rate is *expected*, not a bug: `missing_rate` and
`very_late_rate` in the messy stream (see messy_events.py) deliberately
drop/discard some events entirely, and a flight whose inbound leg's
"landed" event was one of them genuinely cannot know what this reference
implementation knows. The useful number is the match rate *among flights
whose relevant upstream events all survived* -- that isolates whether the
job handles messiness correctly from whether messiness has in-principle-
unrecoverable consequences (it does, by design).
"""

import json
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:19092"
FEATURES_TOPIC = "stream.flight_features"
LATE_TOPIC = "stream.late_events"
CONSUME_IDLE_TIMEOUT_SECONDS = 15

REPO_ROOT = Path(__file__).resolve().parent.parent
GROUND_TRUTH_PATH = REPO_ROOT / "data" / "processed" / "rotation_ground_truth.json"

TOLERANCE = 1e-6


def consume_all(topic: str) -> list:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"verify-rotation-{int(time.time())}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    records = []
    last_message_at = time.time()
    while time.time() - last_message_at < CONSUME_IDLE_TIMEOUT_SECONDS:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        records.append(json.loads(msg.value()))
        last_message_at = time.time()
    consumer.close()
    return records


def last_per_flight(records: list) -> dict:
    out = {}
    for r in records:
        out[r["flight_id"]] = r  # later records overwrite -- keep the last
    return out


def close_enough(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(a - b) < TOLERANCE
    return a == b


FEATURE_FIELDS = [
    "has_inbound",
    "prev_arr_delay_min",
    "prev_dep_delay_min",
    "turnaround_minutes",
    "tight_turnaround",
    "cum_delay_today",
    # Added for Milestone 7: the online job originally only emitted the
    # rotation-state subset above; these are the rest of the real training
    # schema (layover_model.features.SCHEDULE_FEATURES plus leg_index/
    # chain_break from ROTATION_FEATURES) that a prediction service
    # actually needs to score a flight -- see DECISIONS.md.
    "leg_index",
    "chain_break",
    "carrier",
    "origin",
    "dest",
    "month",
    "day_of_week",
    "dep_hour_local",
]


def main():
    if not GROUND_TRUTH_PATH.exists():
        sys.exit("no ground truth found -- run scripts/produce_flight_events.py first")
    ground_truth = json.loads(GROUND_TRUTH_PATH.read_text())

    print(
        f"consuming {FEATURES_TOPIC} and {LATE_TOPIC} "
        f"(idle timeout {CONSUME_IDLE_TIMEOUT_SECONDS}s)..."
    )
    feature_records = consume_all(FEATURES_TOPIC)
    late_records = consume_all(LATE_TOPIC)
    flink_output = last_per_flight(feature_records)

    print(f"\nground truth: {len(ground_truth)} flights")
    print(
        f"flink feature updates received: {len(feature_records)} "
        f"(covering {len(flink_output)} distinct flights)"
    )
    print(f"late events side-outputted: {len(late_records)}")

    missing_from_output = set(ground_truth) - set(flink_output)
    matched = 0
    mismatched = []
    for flight_id, gt in ground_truth.items():
        actual = flink_output.get(flight_id)
        if actual is None:
            continue
        if all(close_enough(gt.get(f), actual.get(f)) for f in FEATURE_FIELDS):
            matched += 1
        else:
            mismatched.append(flight_id)

    present = len(ground_truth) - len(missing_from_output)
    print(f"\nflights with no Flink output at all: {len(missing_from_output)}")
    print(f"flights present in both: {present}")
    print(f"  matching on all feature fields: {matched} ({matched / present:.1%} of present)")
    print(f"  mismatched: {len(mismatched)} ({len(mismatched) / present:.1%} of present)")
    if mismatched[:5]:
        print(f"  sample mismatched flight_ids: {mismatched[:5]}")


if __name__ == "__main__":
    main()
