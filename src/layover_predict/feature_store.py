"""The Redis-backed online feature store: the spec's own "online feature
state" layer ("latest aircraft and airport state for low-latency
predictions"). Holds exactly one record per flight -- the latest feature
update `rotation_job.py` emitted for it (synced in by
`scripts/sync_features_to_redis.py`) -- so the prediction service can
look a flight up in O(1) instead of re-deriving its state from Kafka on
every request.

A TTL, not a manual eviction job: a flight's features stop being useful
well before the key would ever matter again (there's no reason to query
a flight from a prior day), so letting Redis expire it is simpler than
tracking cleanup separately.
"""
import json

KEY_PREFIX = "layover:features:"
TTL_SECONDS = 48 * 3600


def feature_key(flight_id: str) -> str:
    return f"{KEY_PREFIX}{flight_id}"


def write_features(redis_client, record: dict) -> None:
    redis_client.set(feature_key(record["flight_id"]), json.dumps(record), ex=TTL_SECONDS)


def read_features(redis_client, flight_id: str):
    raw = redis_client.get(feature_key(flight_id))
    return json.loads(raw) if raw is not None else None
