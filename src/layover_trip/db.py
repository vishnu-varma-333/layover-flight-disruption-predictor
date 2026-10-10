"""Trip/TripLeg/Alert/RiskAssessment persistence -- the spec's relational
entities, in the real TimescaleDB instance Milestone 1 already provisioned
(and that's sat unused for real application data until now; Milestones
2-7 all worked through Parquet, Kafka, and Redis instead).

`risk_assessments` is a hypertable (the spec's own "positions and
conditions as time series" pattern, extended to risk history -- the data
model explicitly calls `RiskAssessment` a "versioned history of every
assessment"). `trips`/`trip_legs`/`alerts` are plain relational tables:
low volume, keyed lookups, no time-series access pattern.
"""
import json

import psycopg2
import psycopg2.extras

CONNECTION_KWARGS = dict(
    host="localhost", port=5432, user="layover", password="layover", dbname="layover"
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS trips (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS trip_legs (
    id TEXT PRIMARY KEY,
    trip_id TEXT NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    flight_id TEXT NOT NULL,
    leg_order INT NOT NULL,
    min_connection_min INT,
    UNIQUE (trip_id, leg_order)
);

CREATE TABLE IF NOT EXISTS alerts (
    id TEXT PRIMARY KEY,
    trip_id TEXT NOT NULL REFERENCES trips(id) ON DELETE CASCADE,
    dedup_key TEXT NOT NULL UNIQUE,
    channel TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_assessments (
    trip_leg_id TEXT NOT NULL REFERENCES trip_legs(id) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL,
    model_version TEXT NOT NULL,
    p_miss DOUBLE PRECISION,
    predicted_delay DOUBLE PRECISION,
    explanation JSONB,
    PRIMARY KEY (trip_leg_id, ts)
);
SELECT create_hypertable('risk_assessments', 'ts', if_not_exists => TRUE);
"""


def connect():
    return psycopg2.connect(**CONNECTION_KWARGS)


def apply_schema(conn) -> None:
    with conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")
        cur.execute(SCHEMA_SQL)


def create_trip(conn, trip_id: str, user_id: str, legs: list) -> None:
    """`legs`: ordered list of {"flight_id", "min_connection_min"} dicts --
    `leg_order` is each one's position in this list, not a caller-supplied
    field, so it's always a dense 0..n-1 sequence matching the trip's real
    leg order."""
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO trips (id, user_id, status) VALUES (%s, %s, 'active')", (trip_id, user_id)
        )
        psycopg2.extras.execute_batch(
            cur,
            "INSERT INTO trip_legs (id, trip_id, flight_id, leg_order, min_connection_min) "
            "VALUES (%s, %s, %s, %s, %s)",
            [
                (f"{trip_id}-leg{i}", trip_id, leg["flight_id"], i, leg.get("min_connection_min"))
                for i, leg in enumerate(legs)
            ],
        )


def get_trip_legs(conn, trip_id: str) -> list:
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, flight_id, leg_order, min_connection_min FROM trip_legs "
            "WHERE trip_id = %s ORDER BY leg_order",
            (trip_id,),
        )
        return [dict(row) for row in cur.fetchall()]


def get_trip_status(conn, trip_id: str):
    with conn, conn.cursor() as cur:
        cur.execute("SELECT status FROM trips WHERE id = %s", (trip_id,))
        row = cur.fetchone()
        return row[0] if row else None


def update_trip_status(conn, trip_id: str, status: str) -> None:
    with conn, conn.cursor() as cur:
        cur.execute("UPDATE trips SET status = %s WHERE id = %s", (status, trip_id))


def insert_risk_assessment(
    conn, trip_leg_id: str, ts, model_version: str, p_miss, predicted_delay: float, explanation
) -> None:
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO risk_assessments (trip_leg_id, ts, model_version, p_miss, "
            "predicted_delay, explanation) VALUES (%s, to_timestamp(%s), %s, %s, %s, %s) "
            "ON CONFLICT (trip_leg_id, ts) DO NOTHING",
            (trip_leg_id, ts, model_version, p_miss, predicted_delay, json.dumps(explanation)),
        )


def latest_risk_assessment(conn, trip_leg_id: str):
    with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT ts, model_version, p_miss, predicted_delay, explanation FROM risk_assessments "
            "WHERE trip_leg_id = %s ORDER BY ts DESC LIMIT 1",
            (trip_leg_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def try_record_alert(
    conn, alert_id: str, trip_id: str, dedup_key: str, channel: str, status: str
) -> bool:
    """The actual exactly-once guarantee for notifications (Temporal's own
    retries are at-least-once, never exactly-once -- see DECISIONS.md):
    a single atomic `INSERT ... ON CONFLICT (dedup_key) DO NOTHING`
    against the real unique constraint on `alerts.dedup_key`. Returns
    True the first time a given dedup_key is recorded (the caller should
    then actually send the notification), False on every subsequent
    attempt with the same key (already sent -- the caller must not send
    again). Atomic even under concurrent attempts, unlike a separate
    SELECT-then-INSERT.
    """
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO alerts (id, trip_id, dedup_key, channel, status) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT (dedup_key) DO NOTHING",
            (alert_id, trip_id, dedup_key, channel, status),
        )
        return cur.rowcount == 1
