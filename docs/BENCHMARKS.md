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

| Metric | Rotation model | Schedule-only model |
| --- | --- | --- |
| Detection recall (15+ min delay, overall) | 66.0% (114,500/173,461) | 11.0% (18,994/173,461) |
| Median lead time, detected only | 232.0 min | 1,440.0 min (simulation artifact — see DECISIONS.md) |
| Mean lead time, detected only | 623.3 min | 1,440.0 min |
| Recall, flights with any inbound aircraft (leg_index > 0) | **74.9%** (n=151,202) | — |
| Recall, first leg of the day (leg_index == 0, no rotation signal possible) | 5.9% (n=22,259) | — |

### MAE cross-check: this replay vs. Milestone 3's offline evaluation

| Model | Milestone 3 (offline, sklearn-style) | Milestone 4 (this replay, event-driven) |
| --- | --- | --- |
| Schedule-only | 29.82 | 29.82 (exact match) |
| Rotation (arrival, pre-departure) | 21.41 | 22.09 |
| Post-departure | 10.12 | 10.14 |

All three evaluated on identical n=612,038 (non-cancelled, non-diverted,
known-outcome flights in July 2026) in both methodologies. The exact
match for schedule-only and near-exact match for post-departure validate
that the replay harness correctly reproduces Milestone 3's model
behavior end to end (Kafka transport, event parsing, categorical
encoding, model loading — all independently exercised here). The small
rotation-model gap (22.09 vs. 21.41) is expected, not a bug: this replay
enforces point-in-time correctness strictly (a flight only sees its
inbound leg's outcome if that leg has *actually landed* by the time of
scoring), while Milestone 3's batch feature table always uses the
eventual actual outcome regardless of whether it would genuinely be known
yet — a small, rare look-ahead this replay deliberately does not have.

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
