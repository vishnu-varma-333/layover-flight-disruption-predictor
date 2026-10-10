#!/usr/bin/env python3
"""Milestone 7: syncs the real Flink job's output (`stream.flight_features`)
into Redis as the online feature store the prediction service reads from
-- the spec's own "Online feature state: Redis... latest aircraft and
airport state for low-latency predictions" layer.

A plain Kafka consumer calling SET, not a Flink sink, by deliberate
choice: it's simple, independently restartable, and keeps the already
complex rotation-linking job (Milestone 6) focused on correctness rather
than also owning a Redis client and its failure modes -- see
DECISIONS.md.

Usage: sync_features_to_redis.py [--duration N]  (default: runs
forever, consuming new feature updates as the real job produces them;
--duration N stops after N seconds, for verification runs.)
"""
import argparse
import json
import logging
import sys
import time
from pathlib import Path

import redis
from confluent_kafka import Consumer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_predict.feature_store import write_features  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:19092"
FEATURES_TOPIC = "stream.flight_features"
REDIS_HOST = "localhost"
REDIS_PORT = 6379


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=None)
    args = parser.parse_args()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    r.ping()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": "feature-sync",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([FEATURES_TOPIC])

    deadline = time.time() + args.duration if args.duration else None
    synced = 0
    try:
        while deadline is None or time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            record = json.loads(msg.value())
            write_features(r, record)
            synced += 1
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    logger.info("synced %d feature updates to Redis", synced)


if __name__ == "__main__":
    main()
