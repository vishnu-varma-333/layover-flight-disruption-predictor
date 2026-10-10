# Benchmarks

Only real, measured numbers. Every entry states the exact setup used to
measure it. Model accuracy, pipeline throughput, replay speedup, and alert
quality (the spec's "Metrics and benchmarks" table) have no numbers yet —
they require a trained model and a backtesting harness, which don't exist
until Milestones 3 and 4. They will be added here when measured, never
estimated in their place.

## Measurement environment (applies to every number below)

- MacBook, macOS 26.5.2 (Darwin 25.5.0), 8 CPUs, 16GB physical RAM.
- Docker Desktop, engine v29.4.1, Compose v5.1.3, VM allocation 8GB RAM.
- Images pulled once, then warm (local image cache) for all timings below —
  cold-pull time depends on network speed and isn't a property of the
  system, so it isn't reported as a system benchmark.

## Milestone 1 — Foundations

### Stack startup time (cold containers, warm images)

**Setup:** `docker compose -f infra/docker-compose.yml down -v` (full
teardown including volumes) immediately followed by
`docker compose -f infra/docker-compose.yml up -d --wait --wait-timeout 180`,
wall-clock timed with `date +%s` around the command. Measures time for all
6 services (Redpanda, TimescaleDB, Redis, OTel Collector, Prometheus,
Grafana) to reach a ready state Compose's `--wait` accepts.

**Result:** 7 seconds (2026-10-09, single run after the otel-collector
healthcheck fix — see DECISIONS.md). An earlier run, before that fix, hit
the full 180s `--wait-timeout` and failed, because the collector's
healthcheck could never succeed on a distroless image — included here as
the "before" data point, not as a system benchmark.

### Live verification suite runtime

**Setup:** `time .venv/bin/python scripts/verify_stack.py` against the
already-running, already-healthy stack from the run above. Covers: Kafka
topic create + produce + consume round trip, TimescaleDB hypertable create
+ insert + read, Redis set/get, 3 HTTP health/API checks.

**Result:** 1 second (2026-10-09).

### Idle resource usage

**Setup:** `docker stats --no-stream` immediately after all 6 services
reached healthy/ready, no load applied.

**Result** (2026-10-09):

| Service | CPU | Memory |
| --- | --- | --- |
| redpanda | 0.35% | 237.7 MiB |
| timescaledb | 0.07% | 84.8 MiB |
| grafana | 0.07% | 50.7 MiB |
| otel-collector | 0.20% | 32.5 MiB |
| prometheus | 0.00% | 22.6 MiB |
| redis | 0.64% | 3.6 MiB |
| **Total** | — | **~432 MiB** |

Well within the 8GB Docker VM allocation, with headroom for Flink
(Milestone 6) and the prediction service (Milestone 7) to join the same
local stack without resource contention on this machine.

### Failure-recovery time

**Setup:** `docker kill layover-timescaledb` while idle, then
`docker compose -f infra/docker-compose.yml up -d timescaledb`, timed from
restart command to `healthy` status via polling
`docker inspect --format='{{.State.Health.Status}}'` every 3s.

**Result:** recovered to `healthy` within one 3s poll interval after
restart (2026-10-09) — i.e. under ~6 seconds including the poll's own
granularity. The verification suite passed immediately after with no
manual intervention.

## Milestone 2 — Historical data

### Download throughput (BTS On-Time Performance zips, this sandbox's network)

**Setup:** `stat` on a growing destination file, sampled before and after a
clean 30-second sleep, while `scripts/fetch_historical_data.py` was the
only active transfer.

**Result:** 23 KB/s sustained (2026-10-09). Highly variable in practice —
individual ~27-33 MB monthly files took anywhere from ~10 to ~20 minutes
each depending on the sample window. Across all 12 months + the OurAirports
reference file (~370 MB total), wall-clock time from first byte to last
byte was approximately 2 hours 20 minutes. This is a property of this
sandbox's network path to transtats.bts.gov, not of the download code —
reported here because it's the real, measured number, not because it's
expected to generalize to another machine.

### Cleaning throughput (BTS CSV -> UTC-timestamped Flight records)

**Setup:** wall-clock timestamps from `scripts/build_historical_data.py`'s
own per-month log lines, final run, 2026-10-10, all 12 months already on
local disk (download time excluded).

**Result:** 7,043,858 flights cleaned from 12 real monthly files in 243.0s
total (sum of per-month times, 14.8s-29.1s each, mean 20.25s/month) —
**~28,990 flights/sec**. Per-month time grows mildly across the series
(14.8s for the first month processed to 29.1s for the last) consistent
with steadily growing Python object/string overhead as more month
DataFrames accumulate in memory before the final concat, not a per-row
slowdown.

### Rotation-building: before and after the vectorization fix

**Setup:** wall-clock between `build_historical_data.py`'s "total cleaned
flights" and "total rotations" log lines, same real dataset
(7,043,858 flights -> 1,747,220 rotations) both times.

**Result:**
- Original groupby + per-group Python loop implementation: killed after
  **20+ minutes** with no completion in sight (not a measured endpoint —
  genuinely did not finish; see DECISIONS.md).
- Vectorized rewrite (`groupby().agg()` + within-group `.shift()`), first
  successful run: **40.5 seconds**.
- Vectorized rewrite, final run (after an additional correctness fix to
  `clean.py` for non-positive elapsed times, which doesn't touch
  `rotations.py` but was timed again for the authoritative end-to-end run
  below): **52.3 seconds**. The 12-second difference between these two
  vectorized runs is ordinary run-to-run variance on a shared dev laptop
  (other processes, thermal state), not a regression — both are
  2-3 orders of magnitude faster than the original.

### End-to-end pipeline (final, authoritative run)

**Setup:** `scripts/build_historical_data.py`'s own reported
`total pipeline time`, 2026-10-10, all 12 months already downloaded.

**Result:** 307.7s total — 243.0s cleaning all 12 months, 52.3s building
rotations, 8.1s writing both Parquet files. End-to-end throughput:
**~22,890 flights/sec** including all I/O.

### Live DuckDB verification suite runtime

**Setup:** wall-clock around `scripts/verify_historical_data.py` (7 checks:
null fields, unique flight IDs, scheduled/actual arrival ordering,
cancelled-flight consistency, rotation referential integrity, rotation leg
counts) against the real 7,043,858-row / 1,747,220-row processed dataset.

**Result:** 3 seconds (2026-10-10).

### Output dataset size

**Result:** `flights.parquet` 175.4 MB (7,043,858 rows), `rotations.parquet`
73.9 MB (1,747,220 rows) — both Snappy-compressed via pandas' default
Parquet writer, no tuning applied.

### Data-quality summary (from the real dataset, not a sample)

| Metric | Value |
| --- | --- |
| Total flights (12 months, Aug 2025-Jul 2026) | 7,043,858 |
| Flights with a tail number (rotation-eligible) | 7,026,084 (99.7%) |
| Rotations (tail_number x day) | 1,747,220 |
| Multi-leg rotations | 1,613,595 (92.4%) |
| Chain breaks (ferry/equipment-swap signal) | 123,669 |
| Rows with unresolvable scheduled arrival (fixed) | 14 (negative source `CRSElapsedTime` — see DECISIONS.md) |

## Milestone 3 — Baseline and model

### Setup (applies to every number in this section)

`make train-model` (`scripts/train_model.py`), 2026-10-10, against the
real Milestone 2 output (7,043,858 flights, same machine as above). Time
split: train = Aug 2025-Jun 2026 (6,283,472-6,283,473 rows depending on
target's own null-filtering), test = Jul 2026 (612,038 rows). Objective
`regression_l1` (see DECISIONS.md for why, found via a real result that
looked wrong under the L2 default).

### Pipeline timing

| Phase | Time |
| --- | --- |
| Feature engineering (7,043,858 rows: timezone conversion + rotation/turnaround features, fully vectorized) | 23s |
| Train + evaluate + MLflow-log: departure delay model | ~113s |
| Train + evaluate + MLflow-log: arrival delay, pre-departure, full model | ~117s |
| Train + evaluate: arrival delay schedule-only ablation + train/evaluate/log arrival delay post-departure model (combined; no clean boundary between these two in the captured log) | ~193s |
| **Total, feature table load to final result printed** | **447s (~7.5 min)** |

### Model accuracy (MAE in minutes, lower is better; real holdout, 612,038 flights)

| Model | Naive median MAE | Schedule-only MAE | Full (rotation) MAE |
| --- | --- | --- | --- |
| Departure delay | 26.95 | — | **15.55** |
| Arrival delay, pre-departure | 31.36 | 29.82 | **21.41** |
| Arrival delay, post-departure (own `dep_delay_min` known) | 31.36 | — | **10.12** |

**Headline result:** rotation/turnaround features reduce arrival-delay
MAE by **28.2%** versus a schedule-only model of the identical algorithm,
trained and evaluated identically (21.41 vs. 29.82 minutes) — an isolated
measurement of this project's core hypothesis, not a comparison against a
deliberately weak baseline. Once a flight has actually departed, knowing
its own departure delay sharpens the arrival estimate by a further 52.7%
(21.41 -> 10.12 minutes).

### A result that looked wrong, and what it was measuring instead

Under LightGBM's default L2 (squared-error) objective, the schedule-only
ablation scored MAE 32.39 — *worse* than the naive median floor (31.36).
This was investigated rather than reported: diagnosed as an objective/
metric mismatch (L2 targets the conditional mean; MAE is minimized by the
median; delay is right-skewed enough that the difference mattered for a
weak-signal model). Re-run with `regression_l1` for every model: the
schedule-only ablation correctly improved to 29.82 (now beating the naive
floor, as any real model should), and the full arrival-delay model also
improved (22.82 -> 21.41). Both L2 and L1 numbers are included here
because the difference, and *why* it happened, is itself a real finding —
not because the L2 numbers are a valid benchmark to compare against.

## Milestone 4 — Backtesting harness

### Setup (applies to every number in this section)

`scripts/run_backtest.py`, 2026-10-10, replaying the real July 2026 test
month (631,970 flights, 1,859,448 events) through the actual Redpanda
stack from Milestone 1, scoring with the exact trained models from
Milestone 3 (loaded from MLflow). Same machine as all prior benchmarks.

### Performance: before and after the per-call overhead fixes

| Stage | Throughput / time |
| --- | --- |
| Event stream construction (1,859,448 events) | 1.5s |
| Kafka produce (1,859,448 events) | 35.6s |
| Consume + score, **before** fixes (inline single-row predict + per-event DataFrame construction) | timed out at 600s, only 400,404/1,859,448 events (667 events/sec effective) |
| Consume + score, **after** decoupling inference + plain-dict feature capture, **before** the pending-state fix | n/a at full-month scale (diagnosed analytically before re-running — see DECISIONS.md) |
| Consume + score, **after all fixes** | **129.7s for all 1,859,448 events (14,338 events/sec)** |

**Isolated micro-benchmarks** (profiled directly, not estimated):
- Single-row `model.predict()`: ~2.64ms/call. Batched (500 rows in one call): ~0.011ms/row (~240x faster per row).
- Single-row feature `DataFrame` construction + dtype coercion: ~2.44ms/call. Plain Python dict construction (same data): ~0.0014ms/call (~1700x faster).
- Net effect of all fixes combined: full-month replay went from "does not finish in 10 minutes" to 130 seconds.

### Headline numbers (real replay, July 2026, 631,970 flights)

**Re-measured 2026-10-10** after a Milestone 7 fix to a real bug in
`BacktestScorer._feature_dict` (it re-read the *live* `leg_index` counter
instead of the value frozen at schedule time — see DECISIONS.md). The
numbers below are the corrected, current ones; they moved modestly from
the original run (recall 66.0% -> 64.9%, median lead time 232 -> 219 min)
— consistent with `leg_index` being a weak secondary signal rather than
the rotation model's main driver, not a methodology change.

| Metric | Rotation model | Schedule-only model |
| --- | --- | --- |
| Detection recall (15+ min delay, overall) | 64.9% (112,615/173,461) | 11.0% (18,994/173,461) |
| Median lead time, detected only | 219.0 min | 1,440.0 min (simulation artifact — see DECISIONS.md) |
| Mean lead time, detected only | 538.0 min | 1,440.0 min |
| Recall, later legs (rotation signal available) | **73.9%** (n=151,202) | — |
| Recall, first leg of the day (no rotation signal possible) | 3.9% (n=22,259) | — |

### MAE cross-check: this replay vs. Milestone 3's offline evaluation

| Model | Milestone 3 (offline, sklearn-style) | Milestone 4 (this replay, event-driven) |
| --- | --- | --- |
| Schedule-only | 29.82 | 29.82 (exact match) |
| Rotation (arrival, pre-departure) | 21.41 | 22.07 |
| Post-departure | 10.12 | 10.12 (exact match) |

All three evaluated on identical n=612,038 (non-cancelled, non-diverted,
known-outcome flights in July 2026) in both methodologies. The exact
match for schedule-only and post-departure validate that the replay
harness correctly reproduces Milestone 3's model behavior end to end
(Kafka transport, event parsing, categorical encoding, model loading —
all independently exercised here). The small rotation-model gap (22.07
vs. 21.41) is expected, not a bug: this replay enforces point-in-time
correctness strictly (a flight only sees its inbound leg's outcome if
that leg has *actually landed* by the time of scoring), while Milestone
3's batch feature table always uses the eventual actual outcome
regardless of whether it would genuinely be known yet — a small, rare
look-ahead this replay deliberately does not have.

## Milestone 5 — Live ingestion

### Setup

`scripts/run_live_ingestion.py`, 2026-10-10, real requests against
OpenSky Network, NOAA's Aviation Weather Center API, and the FAA's NAS
Status feed, publishing to the real Redpanda stack. 15 monitored US hub
airports (ATL, BOS, CLT, DEN, DFW, IAH, JFK, LAS, LAX, MCO, MIA, ORD, PHX,
SEA, SFO).

### OpenSky credit cost (measured directly, not assumed from documentation)

**Setup:** two real `/states/all` calls with a small (~0.26° half-width)
bounding box each, comparing the `X-Rate-Limit-Remaining` response header
before and after.

**Result:** exactly **1 credit per call** (398 -> 397 -> 396 across two
consecutive calls). This number, not a documentation-derived guess, drove
the budgeting design: 15 airports x 1 credit = 15 credits per full
rotation, so an hourly cadence costs 360 of the 400 daily credits,
leaving a 40-credit reserve.

### Bounded-run scheduling fix: before and after

**Setup:** `scripts/run_live_ingestion.py --duration N`, wall-clock timed
from launch to process exit, with every ingestor at its production-
default interval (OpenSky 3600s, weather 900s, status 300s).

**Result:**
- Before the fix (`--duration 90`): still running several minutes later
  (the process was manually killed, not timed to completion — see
  DECISIONS.md for why).
- After the fix (`--duration 60`): exited cleanly at **61 seconds**.

### Live data volume (one 60-second bounded run)

| Event type | Count | Notes |
| --- | --- | --- |
| `aircraft_position` | 571 | one cycle across all 15 monitored airports |
| `airport_weather` | 30 | 15 airports x (1 METAR + 1 TAF) |
| `airport_status` | 0-2 | depends on real, currently-active FAA delay programs/closures at monitored airports (observed 1 real closure for LAX during testing) |

### Live verification

`scripts/verify_live_ingestion.py`, consuming for 20 seconds against
events already published: all checks passed against real data (a
plausible lat/lon and non-empty callsign/icao24 on a real aircraft
position, a non-empty raw METAR/TAF report, and status events correctly
restricted to the monitored airport set).

## Milestone 6 — Stream processing

### Setup

`make rotation-produce && make rotation-job && make rotation-verify`,
2026-10-10, a real day (2026-07-15, 20,055 flights from the Milestone 2
dataset) replayed through a real local JVM (Java 17), `apache-flink`
2.2.1, and the real Redpanda stack — not a mocked Flink test harness.

### End-to-end job runtime

**Setup:** wall-clock timed from job launch to process exit, bounded
Kafka source (stops once caught up to the topic's end offset at start),
61,033 messy events (59,854 clean events from the day, with duplicates
added and some dropped by `inject_messiness`).

**Result:** **85 seconds**, reproduced identically across two consecutive
clean runs (confirming determinism, not a one-off).

### Messiness handling (real data, not synthetic)

| Category | Injected | Detected/handled |
| --- | --- | --- |
| Late (displaced to end of produce order) | ~300 events (0.5% rate) | **302** routed to late side output |
| Duplicate (exact re-send) | ~1,800 events (3% rate) | Dropped silently via seen-keys dedup (no separate counter kept this run — see DECISIONS.md for the mechanism) |
| Missing (dropped before producing) | ~600 events (1% rate) | 309 flights (1.5%) ended up with no Flink output at all — consistent with some flights' only event being among those dropped |
| Out-of-order (local shuffle) | all events, window=8 | Handled via last-event-time-wins merge; no separate count, validated by the overall match rate below |

### Correctness: Flink job output vs. ground truth

**Re-measured 2026-10-10** (Milestone 7): ground truth is now computed by
running `layover_backtest.scorer.BacktestScorer` (Milestone 4's
already-validated reference, which reproduces the real training feature
schema) over the clean, sorted stream, rather than Milestone 6's original
narrower reimplementation. Compared against the real Flink job's actual
output on the complete 14-field training schema — the original 6
rotation-state fields plus `leg_index`, `chain_break`, and every schedule
feature (`carrier`, `origin`, `dest`, `month`, `day_of_week`,
`dep_hour_local`). This substitution, plus two real bugs it surfaced
(`turnaround_minutes` measured from the wrong timestamp; the Flink job
originally missing 8 of these 14 fields entirely), are written up in
DECISIONS.md.

**Result:**

| Metric | Value |
| --- | --- |
| Ground truth flights | 20,055 |
| Flink feature updates received | 77,380 (across 19,746 distinct flights) |
| Flights with no Flink output at all | 309 (1.5%) |
| Flights present in both, matching on every one of the 14 fields | 18,304 / 19,746 (**92.7%**) |
| Flights present in both, mismatched | 1,442 (7.3%) |

A lower headline number than the original 95.8%, but a more trustworthy
one — the old number checked 6 fields with a self-consistently-wrong
reference; this one checks all 14 against an independently-validated
one. Per-field mismatch breakdown confirms the remaining 7.3% is exactly
where it's expected to be: the schedule-side fields (carrier/origin/dest/
month/day_of_week/dep_hour_local) match **100%** (they don't depend on
delivery order at all), while every rotation-dependent field sits at
1.1%-4.6% mismatch — the same order of magnitude as `missing_rate +
very_late_rate` applied to ~20K flights' worth of upstream dependencies,
not an open-ended or unbounded error rate. `leg_index` sits at 3.0%,
consistent with the ordinary "out-of-order" shuffle category
occasionally reordering two of the same tail's schedule events relative
to each other (unlike the other rotation fields, `leg_index`'s true
value is inherently order-of-schedule-arrival dependent, so this residual
is expected, not a bug).

## Milestone 7 — Prediction service

### Setup

`make sync-features && make predict-service && make verify-predict`,
2026-10-10, against the real Milestone 6 output for the same demo day
(2026-07-15), a real Redis instance (Milestone 1's stack), and the real
trained LightGBM models loaded from MLflow — no mocks.

### Feature sync (Flink output -> Redis)

| Metric | Value |
| --- | --- |
| Feature updates consumed | 77,380 |
| Distinct flights synced | 19,746 (matches Milestone 6's own Flink output exactly) |
| Wall-clock time | 20s (bounded run) |

### Prediction endpoint latency (200 real flights, after one warm-up request)

End to end: Redis lookup, feature-frame assembly (categorical/numeric
coercion), model inference, and `pred_contrib` explanation attribution.

| Percentile | Latency |
| --- | --- |
| p50 | 9.5ms |
| p95 | 13.9ms |
| p99 | 19.2ms |
| max | 65.4ms |

Comfortably inside the spec's "p95 under a few seconds" end-to-end
target, which also has to cover ingestion and stream processing
upstream of this service, not just this hop.

### Live correctness check

15/15 real sampled flights returned a valid risk assessment (numeric
`predicted_delay`, non-empty evidence-backed `explanation`) over HTTP.
A cross-check re-derived one flight's prediction independently — same
Redis record, same MLflow model, no HTTP layer involved — and it
matched the service's own HTTP response exactly, confirming the
response is the real model's real output, not a well-shaped stub.

## Milestone 8 — Trip monitoring and alerts

### Setup

`scripts/verify_trip_monitoring.py`, 2026-10-11, against a real
self-hosted Temporal server (`infra/docker-compose.yml`), a real worker
subprocess this script itself starts and `SIGKILL`s, the real Milestone
7 prediction service, the real TimescaleDB trip schema, and a real
Mailpit SMTP inbox — no mocks, no simulated restart.

### Connection-risk calibration (measured, not assumed)

| Model | p50 residual | p90 residual | p95 residual | p99 residual |
| --- | --- | --- | --- | --- |
| Rotation (pre-departure) | +0.6 min | +37.8 min | +70.8 min | +215.4 min |
| Post-departure | 0.0 min | +18.4 min | +28.1 min | +58.5 min |

Measured on the real July 2026 holdout (n=612,038 per model, matching
Milestones 3/4's own evaluation set) — the empirical distribution
`p_miss` is read off by interpolation. The post-departure model's
visibly tighter distribution (it already knows this flight's own
departure delay) is why the workflow switches to it the moment a leg
departs, same as Milestone 7's own model selection.

### The real kill test (2026-10-11, UA539 ORD→DFW → AA1402 DFW→AUS)

A genuine tight connection from the real dataset: UA539 departed 115
minutes late; AA1402 was scheduled only 52 minutes after UA539's
*scheduled* arrival, against a 45-minute minimum connection time — 7
real minutes of slack.

| Step | Result |
| --- | --- |
| Computed `p_miss` | **0.995** (predicted arrival delay 108.8 min vs. 7-min buffer) |
| Real alerts fired (worker #1) | 1 (one `alerts` row, one real Mailpit email) |
| Workflow status with worker `SIGKILL`ed, zero workers running | `RUNNING` (`describe()`) |
| New alerts while no worker was running at all | 0 |
| `fetch_leg_assessment` activity executions (full run, via real Temporal history) | 3 (across 2 real timer fires, spanning the kill and restart) |
| `send_alert` activity executions (full run) | 1 (not re-fired on the 2nd/3rd check — risk unchanged) |
| Rows for the alert's dedup key after restart | 1 (never became 2) |
| Trip status after `stop_watching()` signaled | `completed` |

**No trip stopped being watched, no duplicate alert** — verified against
Temporal's own workflow history, not only this test's own assertions.

### Web Push: real protocol, real cryptography, no real browser subscription (environment-limited)

This session's sandboxed browser tool cannot register a Service Worker
(confirmed live: a plain `fetch()` of the worker script succeeds over
real HTTP with the correct MIME type; `serviceWorker.register()` fails
regardless). Verified the strongest alternative instead: a real P-256
subscriber keypair, a real local HTTP endpoint, and a real `send_push`
call produced a correctly ES256-signed VAPID JWT, the correct
`aes128gcm` Content-Encoding header, and a 152-byte AES-GCM-encrypted
body delivered over a real HTTP connection — the full client-side Web
Push protocol, genuinely exercised. Only the final "a real browser
receives and displays it" hop is unverified, pending Milestone 9's
subscribe UI.
