# Replay and load-test report

The spec's own "For recruiters" ask: a backtest results page proving the
headline numbers with real measurements, not claims. Every number here
was produced by actually running the code against real data — see
`docs/BENCHMARKS.md` for full methodology and raw output, and
`docs/MODEL_CARD.md` for what the model does and doesn't do well.

All numbers: real July 2026 BTS On-Time Performance holdout month
(631,970 flights, never seen in training), replayed through the real
Kafka/Redpanda stack with the real trained LightGBM models — not a
separate "demo mode" pipeline. Re-measured 2026-10-11 for this report
(`make backtest`); see `docs/BENCHMARKS.md` Milestone 4 and Milestone 10
for the two runs this draws from.

## Headline numbers

| Metric | Result | Baseline comparison |
| --- | --- | --- |
| 15+ min delay detection recall | 64.9% (73.9% on legs with a rotation signal) | 11.0% (schedule-only) |
| Median warning lead time (detected disruptions) | 219 minutes before scheduled departure | n/a — baseline fires at T-0, not a real warning |
| Arrival-delay MAE | 21.4-22.1 min | 29.82 min (26-28% better) |
| Connection-risk precision @ alert threshold | 67.7% | — (no prior baseline exists for this metric) |
| Connection-risk recall @ alert threshold | 72.5% | — |
| Monitoring durability | 0 trips dropped, 0 duplicate alerts across a real `SIGKILL` + restart | — |

## Load test: replay speed

The spec's production-readiness bar: "replay historical days at 10x to
100x speed." Measured by replaying the entire July 2026 month (31 real
calendar days, 1,859,448 individual flight-lifecycle events) through the
live Kafka -> consumer -> scoring pipeline and timing wall-clock start
to finish — not a synthetic throughput benchmark against an idle queue.

| Stage | Measurement |
| --- | --- |
| Event stream construction | 1,859,448 events in 1.1s |
| Kafka produce | 1,859,448 events in 27.5s |
| Consume + score (real point-in-time-correct feature capture + batched model inference) | 1,859,448 events in 100.5s |
| **Sustained throughput** | **18,504 events/sec** |
| **Total wall-clock time, full month replay** | **~128s (produce + consume/score)** |
| **Real month span this replay compresses** | 31 days = 2,678,400 seconds |
| **Effective replay speedup** | **~20,900x real time** |

Far past the spec's 10-100x target — the production bottleneck in a real
deployment would be the live data sources' own rate limits (OpenSky's
daily credit budget, see Milestone 5), not this pipeline's throughput.

## Backtest-as-regression-test

The spec requires "a new model ships only if it matches or beats the
current one." This project's version of that: Milestone 4's backtest
harness replays the exact production code path (same Kafka topics, same
feature pipeline, same scorer) against a fixed holdout month, so any
model or pipeline change gets measured against these same numbers before
being called an improvement — not a separate offline-only check that
could drift from what's actually served.

## What this does and doesn't prove

Proves: the pipeline correctly reproduces Milestone 3's offline model
behavior end-to-end through real Kafka transport (MAE exact-matches for
the schedule-only and post-departure models — see `docs/BENCHMARKS.md`
Milestone 4's MAE cross-check), handles a full month of real messy data
without dropping or duplicating events, and sustains far more than the
required replay speed.

Doesn't prove: real-world alert quality (the spec's own "share of alerts
followed by a real disruption" metric needs live production traffic over
time to measure honestly, not a single backtest), or generalization
beyond US domestic hub airports. See `docs/MODEL_CARD.md`'s Limitations
section.
