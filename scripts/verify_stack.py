#!/usr/bin/env python3
"""Live round-trip checks against every Milestone 1 infra service.

Not a unit test against mocks: each check talks to the real container over
the real port, the same way a future ingestor/consumer/service would.

Kafka bootstrap uses localhost:19092 (Redpanda's "external" listener) because
this script runs on the host, not inside the compose network. A service
running as a container on the `layover` network instead uses `redpanda:9092`
(the "internal" listener) -- see infra/docker-compose.yml.
"""
import sys
import time
import uuid

import psycopg2
import redis
import requests
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

FAILURES = []


def check(name):
    def decorator(fn):
        def wrapper():
            try:
                fn()
                print(f"[PASS] {name}")
            except Exception as exc:  # noqa: BLE001
                print(f"[FAIL] {name}: {exc}")
                FAILURES.append(name)

        return wrapper

    return decorator


@check("redpanda: produce + consume round trip")
def check_kafka():
    topic = f"layover-verify-{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": "localhost:19092"})
    admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])[
        topic
    ].result(timeout=15)

    producer = Producer({"bootstrap.servers": "localhost:19092"})
    sent = b"layover-foundations-check"
    producer.produce(topic, sent)
    producer.flush(10)

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": f"verify-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([topic])
    msg = None
    deadline = time.time() + 15
    while time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is not None and not msg.error():
            break
    consumer.close()
    if msg is None or msg.error():
        raise RuntimeError("no message received within timeout")
    if msg.value() != sent:
        raise RuntimeError(f"round-trip mismatch: sent {sent!r}, got {msg.value()!r}")


@check("timescaledb: connect + create hypertable")
def check_timescaledb():
    conn = psycopg2.connect(
        host="localhost", port=5432, user="layover", password="layover", dbname="layover"
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1

            cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

            table = f"verify_{uuid.uuid4().hex[:8]}"
            cur.execute(
                f"CREATE TABLE {table} (ts TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION)"
            )
            cur.execute(f"SELECT create_hypertable('{table}', 'ts')")
            cur.execute(f"INSERT INTO {table} VALUES (now(), 1.0)")
            cur.execute(f"SELECT count(*) FROM {table}")
            assert cur.fetchone()[0] == 1
            cur.execute(f"DROP TABLE {table}")
    finally:
        conn.close()


@check("redis: set + get round trip")
def check_redis():
    r = redis.Redis(host="localhost", port=6379, socket_timeout=5)
    key = f"verify:{uuid.uuid4().hex[:8]}"
    r.set(key, "ok", ex=30)
    assert r.get(key) == b"ok"
    r.delete(key)


@check("otel-collector: health endpoint")
def check_otel():
    resp = requests.get("http://localhost:13133/", timeout=5)
    resp.raise_for_status()


@check("prometheus: healthy + collector scrape target up")
def check_prometheus():
    resp = requests.get("http://localhost:9090/-/healthy", timeout=5)
    resp.raise_for_status()

    deadline = time.time() + 30
    last = None
    while time.time() < deadline:
        resp = requests.get("http://localhost:9090/api/v1/targets", timeout=5)
        resp.raise_for_status()
        targets = resp.json()["data"]["activeTargets"]
        otel_targets = [t for t in targets if t["labels"].get("job") == "otel-collector"]
        last = otel_targets
        if otel_targets and all(t["health"] == "up" for t in otel_targets):
            return
        time.sleep(2)
    raise RuntimeError(f"otel-collector scrape target never came up: {last}")


@check("grafana: health endpoint + provisioned Prometheus datasource")
def check_grafana():
    resp = requests.get("http://localhost:3000/api/health", timeout=5)
    resp.raise_for_status()

    resp = requests.get(
        "http://localhost:3000/api/datasources",
        auth=("admin", "admin"),
        timeout=5,
    )
    resp.raise_for_status()
    names = [ds["type"] for ds in resp.json()]
    assert "prometheus" in names, f"no prometheus datasource provisioned, found {names}"


def main():
    for fn in (
        check_kafka,
        check_timescaledb,
        check_redis,
        check_otel,
        check_prometheus,
        check_grafana,
    ):
        fn()

    if FAILURES:
        print(f"\n{len(FAILURES)} check(s) failed: {', '.join(FAILURES)}")
        sys.exit(1)
    print("\nAll foundations checks passed.")


if __name__ == "__main__":
    main()
