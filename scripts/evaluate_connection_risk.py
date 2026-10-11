#!/usr/bin/env python3
"""Milestone 10: measures what the spec's "Metrics and benchmarks" table
asks for and no earlier milestone measured directly -- precision, recall
and calibration of `p_miss` itself (Milestone 4 measures single-flight
delay-detection recall; Milestone 8 measures only the residual quantiles
`p_miss` is built from, not how well the resulting probability tracks
real outcomes).

BTS has no passenger itineraries, so there is no real "did this traveler
make their connection" label to evaluate against. Connections are
synthetically constructed instead: every pair of real flights at the same
Milestone 5 hub airport, same day, where one lands and the other departs
20-180 minutes apart by schedule -- a plausible connection window, not an
actual booking. This is a real limitation (noted in the model card), not
a shortcut taken to avoid building the real thing: no booking data exists
to build the real thing from.

Uses the same July 2026 holdout month, the same trained "rotation"
pre-departure model, and the same feature pipeline as Milestones 3/4/8 --
and the exact `connection_p_miss` function the production trip-monitoring
workflow calls, not a reimplementation of it.
"""
import logging
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import mlflow  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from layover_data.airports import build_airport_tz_map  # noqa: E402
from layover_model.features import (  # noqa: E402
    ROTATION_FEATURES,
    SCHEDULE_FEATURES,
    build_feature_table,
)
from layover_trip.activities import DEFAULT_MIN_CONNECTION_MINUTES  # noqa: E402
from layover_trip.alerts import ALERT_THRESHOLD  # noqa: E402
from layover_trip.risk import connection_p_miss  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
TEST_START_DATE = pd.Timestamp("2026-07-01").date()
PRE_DEPARTURE_COLS = SCHEDULE_FEATURES + ROTATION_FEATURES
# Same 15 hubs Milestone 5's live ingestion monitors -- keeps this
# evaluation on the same demo scope as the rest of the project rather
# than every one of BTS's ~300 airports.
HUB_AIRPORTS = [
    "ATL", "BOS", "CLT", "DEN", "DFW", "IAH", "JFK", "LAS", "LAX",
    "MCO", "MIA", "ORD", "PHX", "SEA", "SFO",
]
MIN_GAP_MIN, MAX_GAP_MIN = 20, 180
OUT_DIR = REPO_ROOT / "docs" / "assets"


def load_rotation_model(client):
    runs = client.search_runs(
        client.get_experiment_by_name("layover-delay-models").experiment_id,
        filter_string="tags.mlflow.runName = 'arr_delay_pre_departure_full'",
        order_by=["start_time DESC"],
        max_results=1,
    )
    if not runs:
        raise SystemExit("no MLflow run found -- run scripts/train_model.py first")
    return mlflow.lightgbm.load_model(f"runs:/{runs[0].info.run_id}/model")


def build_connections(flights: pd.DataFrame) -> pd.DataFrame:
    """A full cross-join per (airport, day) group is the obvious way to
    write this and was tried first -- it blew past minutes of runtime
    because a busy hub's day has ~800-900 flights, so one day's
    arrivals x departures alone is ~700K candidate pairs before any
    filtering, x15 hubs x31 days. Rewritten to use a sorted-array
    `searchsorted` window lookup per (airport, day) group instead: cost
    is proportional to the number of connections actually in the 20-180
    minute window, not every possible pair.
    """
    legs = flights[~flights["cancelled"] & ~flights["diverted"]].copy()
    arrivals = legs[legs["dest"].isin(HUB_AIRPORTS)][
        ["flight_id", "dest", "fl_date", "sched_arr_utc", "actual_arr_utc", "predicted_arr_delay"]
    ]
    departures = legs[legs["origin"].isin(HUB_AIRPORTS)][
        ["flight_id", "origin", "fl_date", "sched_dep_utc"]
    ]

    lo_offset = np.timedelta64(MIN_GAP_MIN, "m")
    hi_offset = np.timedelta64(MAX_GAP_MIN, "m")
    out_rows = []
    for airport in HUB_AIRPORTS:
        arr_a = arrivals[arrivals["dest"] == airport]
        dep_a = departures[departures["origin"] == airport]
        for date, arr_day in arr_a.groupby("fl_date", sort=False):
            dep_day = dep_a[dep_a["fl_date"] == date].sort_values("sched_dep_utc")
            if dep_day.empty:
                continue
            # tz-aware datetime Series.to_numpy() yields an object array of
            # Timestamps (not a real datetime64), which numpy can't do
            # timedelta arithmetic on -- drop to naive UTC first (every
            # timestamp here is already UTC).
            dep_times = dep_day["sched_dep_utc"].dt.tz_localize(None).to_numpy()
            dep_ids = dep_day["flight_id"].to_numpy()

            t_a = arr_day["sched_arr_utc"].dt.tz_localize(None).to_numpy()
            i0 = np.searchsorted(dep_times, t_a + lo_offset, side="left")
            i1 = np.searchsorted(dep_times, t_a + hi_offset, side="right")

            arr_ids = arr_day["flight_id"].to_numpy()
            actual_arr = arr_day["actual_arr_utc"].dt.tz_localize(None).to_numpy()
            sched_arr = t_a
            pred_delay = arr_day["predicted_arr_delay"].to_numpy()

            for k in range(len(arr_day)):
                for j in range(i0[k], i1[k]):
                    if arr_ids[k] == dep_ids[j]:
                        continue
                    out_rows.append((
                        arr_ids[k], dep_ids[j], airport, sched_arr[k], actual_arr[k],
                        pred_delay[k], dep_times[j],
                    ))

    merged = pd.DataFrame(
        out_rows,
        columns=[
            "flight_id_a", "flight_id_b", "airport", "sched_arr_utc", "actual_arr_utc",
            "predicted_arr_delay", "sched_dep_utc_b",
        ],
    )
    gap = merged["sched_dep_utc_b"] - merged["sched_arr_utc"]
    merged["gap_min"] = gap.dt.total_seconds() / 60
    return merged


def main():
    mlflow.set_tracking_uri(f"file://{REPO_ROOT}/mlruns")
    client = mlflow.tracking.MlflowClient()

    logger.info("loading rotation model from MLflow")
    model = load_rotation_model(client)

    all_flights = pd.read_parquet(PROCESSED_DIR / "flights.parquet")
    categories = {
        col: sorted(all_flights[col].dropna().unique().tolist())
        for col in ("carrier", "origin", "dest")
    }
    test_month = all_flights[all_flights["fl_date"] >= TEST_START_DATE].copy()
    logger.info("evaluating on %d July 2026 holdout flights", len(test_month))

    airport_tz_map = build_airport_tz_map(REPO_ROOT / "data" / "raw" / "reference" / "airports.csv")
    feature_table = build_feature_table(test_month, airport_tz_map)
    for col, cats in categories.items():
        feature_table[col] = pd.Categorical(feature_table[col], categories=cats)

    test_month["predicted_arr_delay"] = model.predict(feature_table[PRE_DEPARTURE_COLS])

    connections = build_connections(test_month)
    logger.info(
        "%d synthetic connections across %d hub airports, July 2026",
        len(connections), len(HUB_AIRPORTS),
    )

    buffer_min = connections["gap_min"] - DEFAULT_MIN_CONNECTION_MINUTES
    connections["p_miss"] = [
        connection_p_miss(pred, "rotation", buf)
        for pred, buf in zip(connections["predicted_arr_delay"], buffer_min)
    ]
    actual_gap_min = (
        connections["sched_dep_utc_b"] - connections["actual_arr_utc"]
    ).dt.total_seconds() / 60
    connections["actual_missed"] = actual_gap_min < DEFAULT_MIN_CONNECTION_MINUTES

    base_rate = connections["actual_missed"].mean()
    predicted_alert = connections["p_miss"] >= ALERT_THRESHOLD
    tp = int((predicted_alert & connections["actual_missed"]).sum())
    fp = int((predicted_alert & ~connections["actual_missed"]).sum())
    fn = int((~predicted_alert & connections["actual_missed"]).sum())
    tn = int((~predicted_alert & ~connections["actual_missed"]).sum())
    precision = tp / (tp + fp) if (tp + fp) else float("nan")
    recall = tp / (tp + fn) if (tp + fn) else float("nan")

    print(f"\n--- connection-risk evaluation ({len(connections)} synthetic connections) ---")
    print(f"base rate (actually missed): {base_rate:.2%}")
    print(f"alert threshold: p_miss >= {ALERT_THRESHOLD}")
    print(f"TP={tp} FP={fp} FN={fn} TN={tn}")
    print(f"precision: {precision:.1%}")
    print(f"recall: {recall:.1%}")

    print("\n--- calibration (decile bins of predicted p_miss) ---")
    connections["bin"] = pd.qcut(connections["p_miss"], 10, duplicates="drop")
    calib = connections.groupby("bin").agg(
        n=("p_miss", "size"),
        mean_predicted=("p_miss", "mean"),
        observed_rate=("actual_missed", "mean"),
    )
    print(calib.to_string(float_format=lambda x: f"{x:.3f}"))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(5, 5))
    ax.plot([0, 1], [0, 1], "k--", linewidth=1, label="perfect calibration")
    ax.plot(
        calib["mean_predicted"], calib["observed_rate"], "o-", color="#4f46e5",
        label="measured (decile bins)",
    )
    ax.set_xlabel("mean predicted p_miss")
    ax.set_ylabel("observed miss rate")
    ax.set_title("Connection-risk calibration\n(July 2026 holdout, synthetic connections)")
    ax.legend()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(OUT_DIR / "p_miss_calibration.png", dpi=150)
    print(f"\ncalibration plot written to {OUT_DIR / 'p_miss_calibration.png'}")


if __name__ == "__main__":
    main()
