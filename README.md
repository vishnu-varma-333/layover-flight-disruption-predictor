# Layover — Flight Disruption Predictor

Layover is a real-time system that warns travelers they will miss a
connection before the airline says so, by tracking the physical aircraft
behind each flight instead of trusting the schedule alone.

Full design, data sources, architecture, tech-stack rationale, data model,
and milestone plan live in [`docs/MILESTONES.md`](docs/MILESTONES.md) and
[`DECISIONS.md`](DECISIONS.md).

## Status

Milestone 1 (Foundations) in progress. See `docs/MILESTONES.md` for
current state.

## Local development

Requires Docker and Python 3.9+.

```bash
make up        # start Kafka (Redpanda), TimescaleDB, Redis, OTel/Prometheus/Grafana
make verify    # run live health + round-trip checks against every service
make down      # tear down
```

## Repository layout

```
infra/      local dev stack (docker-compose) + observability config
scripts/    operational and verification scripts
docs/       milestone tracking and benchmarks
DECISIONS.md   design-decision log
```
