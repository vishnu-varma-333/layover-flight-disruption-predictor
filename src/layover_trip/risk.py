"""Connection-risk calculation: turns a leg's point-prediction
(`predicted_delay`, from the Milestone 7 prediction service) into
`p_miss`, the probability of missing the next leg given this leg's
scheduled connection buffer and the airport's minimum connection time.

A LightGBM regressor gives a point estimate (trained to minimize MAE,
so roughly the conditional median), not a distribution -- turning that
into an actual probability needs a real error model, not a made-up one.
Research pass (per the standing instruction to do this properly for
anything feature/model-shaped, not just intuit it): measured the
empirical residual distribution (actual arrival delay minus predicted)
on the real July 2026 holdout Milestone 3/4 already use, rather than
assuming a parametric (e.g. Gaussian) error model -- delay is heavily
right-skewed (confirmed already in Milestone 3's objective-mismatch
finding), so a normal-distribution assumption would badly understate
tail risk. `p_miss = P(residual > buffer - predicted_delay)`, read off
this empirical distribution via linear interpolation between measured
percentile points, not a closed-form formula.

The two tables below are measured, not estimated: 101-point (0th-100th
percentile) empirical quantiles of (actual_arr_delay_min -
predicted_arr_delay_min) on the real, non-cancelled/non-diverted July
2026 test month (n=612,038 for both), computed 2026-10-11 using the
exact trained models and feature pipeline from Milestones 3/4. One
table per model, because the post-departure model (which also knows
this flight's own departure delay) is both more accurate and has a
tighter error distribution -- using the pre-departure table for a
post-departure prediction would overstate uncertainty.
"""
import numpy as np

PERCENTILE_LEVELS = np.arange(0, 101) / 100.0

ROTATION_RESIDUAL_QUANTILES = np.array([
    -361.73, -31.67, -26.65, -23.83, -21.86, -20.34, -19.08, -18.00, -17.06, -16.24,
    -15.48, -14.78, -14.14, -13.52, -12.94, -12.40, -11.88, -11.40, -10.93, -10.47,
    -10.03, -9.61, -9.20, -8.80, -8.40, -8.01, -7.64, -7.27, -6.91, -6.54,
    -6.19, -5.84, -5.50, -5.15, -4.80, -4.47, -4.13, -3.80, -3.47, -3.13,
    -2.79, -2.46, -2.13, -1.79, -1.46, -1.12, -0.77, -0.44, -0.08, 0.26,
    0.61, 0.97, 1.32, 1.69, 2.06, 2.44, 2.83, 3.24, 3.64, 4.05,
    4.47, 4.91, 5.35, 5.81, 6.28, 6.77, 7.27, 7.81, 8.37, 8.93,
    9.52, 10.16, 10.80, 11.47, 12.18, 12.95, 13.76, 14.62, 15.56, 16.55,
    17.64, 18.80, 20.08, 21.51, 23.07, 24.80, 26.74, 28.96, 31.49, 34.38,
    37.76, 41.85, 46.71, 52.89, 60.63, 70.76, 84.72, 104.87, 138.12, 215.40,
    3436.95,
])

POST_DEPARTURE_RESIDUAL_QUANTILES = np.array([
    -189.71, -25.65, -21.57, -19.23, -17.57, -16.28, -15.22, -14.30, -13.52, -12.83,
    -12.21, -11.64, -11.10, -10.61, -10.14, -9.70, -9.30, -8.91, -8.54, -8.17,
    -7.82, -7.49, -7.16, -6.85, -6.54, -6.24, -5.96, -5.67, -5.40, -5.13,
    -4.86, -4.60, -4.34, -4.09, -3.83, -3.59, -3.34, -3.10, -2.85, -2.62,
    -2.38, -2.14, -1.90, -1.66, -1.42, -1.18, -0.94, -0.70, -0.46, -0.21,
    0.03, 0.28, 0.52, 0.77, 1.02, 1.28, 1.54, 1.80, 2.06, 2.33,
    2.61, 2.88, 3.17, 3.47, 3.77, 4.08, 4.40, 4.72, 5.06, 5.41,
    5.76, 6.13, 6.49, 6.88, 7.30, 7.73, 8.17, 8.63, 9.11, 9.62,
    10.17, 10.73, 11.34, 12.01, 12.71, 13.46, 14.26, 15.14, 16.11, 17.16,
    18.36, 19.75, 21.34, 23.19, 25.43, 28.14, 31.69, 36.45, 43.88, 58.51,
    1897.13,
])

RESIDUAL_QUANTILES = {
    "rotation": ROTATION_RESIDUAL_QUANTILES,
    "post_departure": POST_DEPARTURE_RESIDUAL_QUANTILES,
}


def p_exceeds(threshold: float, model_name: str) -> float:
    """P(residual > threshold) under the empirical distribution for the
    given model -- a reverse lookup into the measured quantile table via
    linear interpolation (`np.interp` needs increasing x, which the
    quantile table already is by construction)."""
    quantiles = RESIDUAL_QUANTILES[model_name]
    percentile = np.interp(threshold, quantiles, PERCENTILE_LEVELS * 100.0)
    return float(max(0.0, min(1.0, 1.0 - percentile / 100.0)))


def connection_p_miss(predicted_delay: float, model_name: str, buffer_minutes: float) -> float:
    """`buffer_minutes`: scheduled time between this leg's arrival and
    the next leg's departure, minus the airport's minimum connection
    time -- the slack actually available before a connection is missed.
    Missing it means this leg's REAL arrival delay exceeds that slack,
    i.e. the residual (real delay minus the point prediction already
    baked into `buffer_minutes` having `predicted_delay` subtracted from
    it below) exceeds `buffer_minutes - predicted_delay`.
    """
    threshold = buffer_minutes - predicted_delay
    return p_exceeds(threshold, model_name)
