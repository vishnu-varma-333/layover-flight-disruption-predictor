"""Computes the backtest's headline numbers from the replay's recorded
prediction snapshots plus ground truth.

Two kinds of validation happen here:

1. Lead time and detection recall (this milestone's actual deliverable) --
   reported *together*, not lead time alone: a system with a great median
   lead time but low recall is mostly not detecting real disruptions at
   all, which lead time by itself hides (this is a known pitfall in
   early-warning-system evaluation literature -- see DECISIONS.md).
2. A MAE cross-check against Milestone 3's offline sklearn-style
   evaluation, using the exact same trained models. If this replay
   harness is implemented correctly, scoring the identical models through
   a genuinely different code path (event-driven, incremental state)
   should reproduce the same accuracy -- this is a real correctness check
   on the harness itself, not just on the model.
"""

import pandas as pd

ALERT_THRESHOLD_MIN = 15


def _pre_departure_predictions(predictions: pd.DataFrame, model: str) -> pd.DataFrame:
    return predictions[(predictions["model"] == model)]


def lead_time_and_recall(predictions: pd.DataFrame, ground_truth: pd.DataFrame, model: str) -> dict:
    """ground_truth: one row per flight that actually landed, with
    flight_id, sched_dep_utc, arr_delay_min, leg_index.
    """
    disrupted = ground_truth[ground_truth["arr_delay_min"] >= ALERT_THRESHOLD_MIN]
    preds = _pre_departure_predictions(predictions, model)
    alerts = preds[preds["predicted_delay"] >= ALERT_THRESHOLD_MIN]
    earliest_alert = alerts.groupby("flight_id")["event_time"].min()

    merged = disrupted.join(earliest_alert.rename("alert_time"), on="flight_id")
    detected = merged["alert_time"].notna()
    lead_minutes = (
        merged.loc[detected, "sched_dep_utc"] - merged.loc[detected, "alert_time"]
    ).dt.total_seconds() / 60

    return {
        "model": model,
        "n_disrupted": len(merged),
        "n_detected": int(detected.sum()),
        "recall": float(detected.mean()) if len(merged) else float("nan"),
        "median_lead_time_min": float(lead_minutes.median()) if len(lead_minutes) else float("nan"),
        "mean_lead_time_min": float(lead_minutes.mean()) if len(lead_minutes) else float("nan"),
    }


def lead_time_by_leg_position(
    predictions: pd.DataFrame, ground_truth: pd.DataFrame, model: str
) -> dict:
    """Same as lead_time_and_recall, split by whether the flight had any
    inbound rotation signal at all (leg_index > 0) -- honest about where
    this system's core value proposition can and can't apply (a first
    leg of the day has no inbound aircraft to track, by definition).
    """
    first_leg = lead_time_and_recall(
        predictions, ground_truth[ground_truth["leg_index"] == 0], model
    )
    later_leg = lead_time_and_recall(
        predictions, ground_truth[ground_truth["leg_index"] > 0], model
    )
    return {"first_leg_of_day": first_leg, "later_leg": later_leg}


def mae_cross_check(predictions: pd.DataFrame, ground_truth: pd.DataFrame, model: str) -> dict:
    """The LAST pre-departure (or, for post_departure, the only) prediction
    made for each flight, compared to its actual outcome -- same metric
    Milestone 3 reported, computed independently through the replay path.
    """
    preds = predictions[predictions["model"] == model].sort_values("event_time")
    last_pred = preds.groupby("flight_id").last()["predicted_delay"]
    merged = ground_truth.set_index("flight_id")[["arr_delay_min"]].join(last_pred, how="inner")
    mae = (merged["predicted_delay"] - merged["arr_delay_min"]).abs().mean()
    return {"model": model, "n": len(merged), "mae": float(mae)}
