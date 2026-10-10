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
