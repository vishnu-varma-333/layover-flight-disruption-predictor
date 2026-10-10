"""Milestone 7: FastAPI service serving each flight's current predicted
delay and evidence-backed explanation from the online feature state
Redis holds (synced from the real Flink job by
`scripts/sync_features_to_redis.py`) -- the spec's own description of
this layer: "Prediction service... serves the current model; returns
risk plus explanation."

`RiskAssessment.p_miss` (missed-connection probability) genuinely needs
a `TripLeg` with a minimum connection time to mean anything -- that
entity doesn't exist until Milestone 8. This service reports
`predicted_delay` and `explanation` now and leaves `p_miss` null rather
than fabricate one -- the same sequencing-dependency pattern Milestone 4
already used for the identical gap (see DECISIONS.md), not a dropped
requirement: the spec's own Build milestones list assigns connection
risk and whole-trip risk to later work.
"""
import logging
from contextlib import asynccontextmanager
from pathlib import Path

import mlflow
import mlflow.lightgbm
import pandas as pd
import redis
from fastapi import FastAPI, HTTPException

from layover_backtest.scorer import MODEL_FEATURE_COLS, prepare_frame
from layover_predict.explain import explain
from layover_predict.feature_store import read_features

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Mirrors scripts/run_backtest.py's own run-name labels -- this service
# serves the identical models the backtest already validated, not a
# separately-trained copy.
MODEL_RUN_NAMES = {
    "rotation": "arr_delay_pre_departure_full",
    "post_departure": "arr_delay_post_departure_full",
}

state = {}


def _load_latest_model(client, run_name: str):
    runs = client.search_runs(
        client.get_experiment_by_name("layover-delay-models").experiment_id,
        filter_string=f"tags.mlflow.runName = '{run_name}'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise RuntimeError(f"no MLflow run found with name {run_name!r} -- run train_model.py")
    model = mlflow.lightgbm.load_model(f"runs:/{runs[0].info.run_id}/model")
    return model, runs[0].info.run_id


@asynccontextmanager
async def lifespan(app: FastAPI):
    mlflow.set_tracking_uri(f"file://{REPO_ROOT}/mlruns")
    client = mlflow.tracking.MlflowClient()

    models, model_versions = {}, {}
    for name, run_name in MODEL_RUN_NAMES.items():
        model, run_id = _load_latest_model(client, run_name)
        models[name] = model
        model_versions[name] = run_id
    state["models"] = models
    state["model_versions"] = model_versions

    flights = pd.read_parquet(REPO_ROOT / "data" / "processed" / "flights.parquet")
    state["categories"] = {
        col: sorted(flights[col].dropna().unique().tolist())
        for col in ("carrier", "origin", "dest")
    }
    state["redis"] = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    logger.info("prediction service ready: model_versions=%s", model_versions)
    yield
    state.clear()


app = FastAPI(title="Layover prediction service", lifespan=lifespan)


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.get("/flights/{flight_id}/risk")
def flight_risk(flight_id: str):
    record = read_features(state["redis"], flight_id)
    if record is None:
        raise HTTPException(status_code=404, detail="no online features for this flight yet")

    # Once this flight has its own known departure delay, the
    # post-departure model (trained with that as an extra feature) is
    # strictly more informed -- matching the spec's "updated as new
    # events arrive" and Milestone 3/4's own model selection.
    model_name = "post_departure" if record.get("dep_delay_min") is not None else "rotation"
    model = state["models"][model_name]
    feature_cols = MODEL_FEATURE_COLS[model_name]

    # Only the "departed" stage sets "dep_delay_min" at all (it's the
    # only stage where it's genuinely known) -- `prepare_frame` expects
    # the column to exist regardless, the same convention
    # BacktestScorer's feature dicts always follow (present, possibly
    # None), so it's filled in here rather than changing the Flink job's
    # own output schema for a consumer-specific requirement.
    request = dict(
        record,
        event_time=record.get("computed_at_event_time"),
        model=model_name,
        dep_delay_min=record.get("dep_delay_min"),
    )
    df = prepare_frame([request], state["categories"])
    features = df[feature_cols]

    predicted_delay = float(model.predict(features)[0])
    # pred_contrib: one exact TreeSHAP contribution per input feature,
    # plus a final "expected value" column -- see explain.py's docstring
    # for why this, not a heuristic, is this service's explanation source.
    raw_contributions = model.predict(features, pred_contrib=True)[0]
    contributions = dict(zip(feature_cols, raw_contributions[:-1]))

    return {
        "flight_id": flight_id,
        "model_version": state["model_versions"][model_name],
        "ts": record.get("computed_at_event_time"),
        "source_event_type": record.get("source_event_type"),
        "predicted_delay": round(predicted_delay, 1),
        "p_miss": None,
        "explanation": explain(record, contributions),
    }
