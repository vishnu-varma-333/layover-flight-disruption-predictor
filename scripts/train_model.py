#!/usr/bin/env python3
"""Milestone 3: trains delay-propagation models and logs them to MLflow.

Three models, all LightGBM, sharing one feature-engineering pipeline:

1. dep_delay_min -- pre-departure features (schedule + rotation/turnaround).
2. arr_delay_min, pre-departure -- same feature set. This is the headline
   model (arrival delay drives missed-connection risk), so it's also
   compared against a schedule-only ablation of the *same* algorithm, to
   isolate what the rotation/turnaround signal specifically contributes
   rather than conflating it with a difference between model families.
3. arr_delay_min, post-departure -- adds this flight's own now-known
   dep_delay_min as a feature, matching the spec's "updated as new events
   arrive": once a flight has actually left the gate, its own departure
   delay sharpens the arrival-delay estimate.

Every model is also compared against a naive floor: predicting the
training set's median delay for every test row.

Evaluated on a strict time-based holdout -- the last calendar month in the
dataset (July 2026) -- not a random row split, so test is genuinely unseen
future data relative to train. This also previews how Milestone 4's replay
backtest will evaluate: by date, not by row.
"""

import logging
import sys
from pathlib import Path

import lightgbm as lgb
import mlflow
import mlflow.lightgbm
import pandas as pd
from sklearn.metrics import mean_absolute_error

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_data.airports import build_airport_tz_map  # noqa: E402
from layover_model.features import (  # noqa: E402
    CATEGORICAL_FEATURES,
    ROTATION_FEATURES,
    SCHEDULE_FEATURES,
    build_feature_table,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
TEST_START_DATE = pd.Timestamp("2026-07-01").date()

LGBM_PARAMS = {
    # L1 (MAE) objective, not LightGBM's default L2/squared-error: delay is
    # right-skewed (most flights near zero, a long tail of large delays),
    # so an L2 model targets the conditional *mean* while MAE is minimized
    # by the conditional *median* -- that mismatch is large enough on this
    # data that an L2-trained schedule-only model actually scored *worse*
    # than the naive median baseline on held-out data (confirmed by
    # training both and comparing; not assumed). L1 makes the comparison
    # apples-to-apples with the MAE metric itself.
    "objective": "regression_l1",
    "n_estimators": 300,
    "num_leaves": 63,
    "learning_rate": 0.05,
    "min_child_samples": 50,
    "random_state": 42,
}


def load_features() -> pd.DataFrame:
    flights = pd.read_parquet(PROCESSED_DIR / "flights.parquet")
    tz_map = build_airport_tz_map(REPO_ROOT / "data" / "raw" / "reference" / "airports.csv")
    return build_feature_table(flights, tz_map)


def supervised_rows(features: pd.DataFrame, target: str) -> pd.DataFrame:
    # Cancelled flights never have a meaningful delay outcome; diverted
    # flights landed somewhere other than the scheduled dest, so their
    # "arrival delay" isn't comparable either. Documented v1 simplification
    # -- see DECISIONS.md.
    return features[~features["cancelled"] & ~features["diverted"] & features[target].notna()]


def time_split(df: pd.DataFrame):
    return df[df["fl_date"] < TEST_START_DATE], df[df["fl_date"] >= TEST_START_DATE]


def fit_lightgbm(train, test, feature_cols, target):
    model = lgb.LGBMRegressor(**LGBM_PARAMS)
    model.fit(
        train[feature_cols],
        train[target],
        categorical_feature=[c for c in CATEGORICAL_FEATURES if c in feature_cols],
    )
    preds = model.predict(test[feature_cols])
    return model, mean_absolute_error(test[target], preds)


def run_target(features: pd.DataFrame, label: str, target: str, feature_cols, run_ablation=False):
    rows = supervised_rows(features, target)
    train, test = time_split(rows)

    naive_pred = train[target].median()
    naive_mae = mean_absolute_error(test[target], [naive_pred] * len(test))

    with mlflow.start_run(run_name=f"{label}_full"):
        mlflow.log_params({"target": target, "features": feature_cols, **LGBM_PARAMS})
        mlflow.log_metric("naive_median_mae", naive_mae)
        model, mae = fit_lightgbm(train, test, feature_cols, target)
        mlflow.log_metric("mae", mae)
        mlflow.lightgbm.log_model(model, "model")

    result = {
        "label": label,
        "n_train": len(train),
        "n_test": len(test),
        "naive_mae": naive_mae,
        "full_mae": mae,
    }

    if run_ablation:
        with mlflow.start_run(run_name=f"{label}_schedule_only"):
            mlflow.log_params({"target": target, "features": SCHEDULE_FEATURES, **LGBM_PARAMS})
            ablation_model, ablation_mae = fit_lightgbm(train, test, SCHEDULE_FEATURES, target)
            mlflow.log_metric("mae", ablation_mae)
            # Saved (not just scored) so Milestone 4's replay can score with
            # the real schedule-only model for its lead-time comparison,
            # rather than retraining a second copy of it.
            mlflow.lightgbm.log_model(ablation_model, "model")
        result["schedule_only_mae"] = ablation_mae

    return result


def main():
    mlflow.set_tracking_uri(f"file://{REPO_ROOT}/mlruns")
    mlflow.set_experiment("layover-delay-models")

    features = load_features()
    logger.info("feature table: %d rows", len(features))

    pre_departure_cols = SCHEDULE_FEATURES + ROTATION_FEATURES
    post_departure_cols = pre_departure_cols + ["dep_delay_min"]

    results = [
        run_target(features, "dep_delay", "dep_delay_min", pre_departure_cols),
        run_target(
            features,
            "arr_delay_pre_departure",
            "arr_delay_min",
            pre_departure_cols,
            run_ablation=True,
        ),
        run_target(features, "arr_delay_post_departure", "arr_delay_min", post_departure_cols),
    ]

    print("\n--- results (MAE in minutes, lower is better) ---")
    for r in results:
        print(f"\n{r['label']}  (train={r['n_train']}, test={r['n_test']})")
        print(f"  naive median baseline: {r['naive_mae']:.2f}")
        if "schedule_only_mae" in r:
            print(f"  schedule-only LightGBM: {r['schedule_only_mae']:.2f}")
            improvement = (r["schedule_only_mae"] - r["full_mae"]) / r["schedule_only_mae"]
            print(f"  full (rotation) LightGBM: {r['full_mae']:.2f}  ({improvement:.1%} better)")
        else:
            print(f"  full LightGBM: {r['full_mae']:.2f}")


if __name__ == "__main__":
    main()
