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
