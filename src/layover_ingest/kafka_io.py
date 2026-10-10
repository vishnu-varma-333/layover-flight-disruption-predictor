"""Shared Kafka wiring for the live ingestors: one topic, an event_type
discriminator -- the same "normalized into one event stream" shape the
spec asks for, and the same single-topic-with-discriminator pattern
Milestone 4's backtest replay already used (for the same reason: simplest
thing that lets Milestone 6's Flink job subscribe to one place for every
signal it needs).
"""

import json
import logging

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "live.events"


def make_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})


def _delivery_report(err, msg):
    if err is not None:
        logger.warning("delivery failed: %s", err)


def publish(producer: Producer, event: dict) -> None:
    producer.produce(TOPIC, json.dumps(event, default=str).encode(), callback=_delivery_report)
    producer.poll(0)
