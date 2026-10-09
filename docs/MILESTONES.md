# Build milestones

Each milestone ends with something running and tested — not just code
committed. Checkboxes track completion; progress notes record what actually
happened, including bugs found during live verification (not just what was
planned).

- [x] **1. Foundations** — Repository, CI, local Kubernetes or VM, Kafka,
      PostgreSQL with TimescaleDB, Redis, observability.
- [ ] **2. Historical data** — Download and clean US on-time records into
      Parquet; build rotation history from tail numbers.
- [ ] **3. Baseline and model** — A simple baseline, then the LightGBM model
      with rotation, turnaround and airport features; tracked in MLflow.
- [ ] **4. Backtesting harness** — Replay historical days through Kafka and
      measure lead time and accuracy. First headline numbers.
- [ ] **5. Live ingestion** — OpenSky, weather and airport-status ingestors
      with credit budgeting.
- [ ] **6. Stream processing** — Flink jobs for event-time handling,
      rotation linking and feature updates.
- [ ] **7. Prediction service** — FastAPI serving risk and explanations
      from online features.
- [ ] **8. Trip monitoring and alerts** — Temporal workflows, deduplicated
      push and email alerts, kill tests.
- [ ] **9. Web app** — Trips, live risk, aircraft chain view,
      notifications.
- [ ] **10. Ship it** — AWS deploy, load and replay reports, model card,
      demo video, write-up.
- [ ] **11. Version 2** — Email import, whole-trip risk, airport-level
      disruption signals.

---

## Milestone 1 — Foundations

**Goal:** bring up every piece of infrastructure every later milestone
depends on, and prove each piece actually works by talking to it over the
network — not by reading documentation or trusting a healthcheck alone.

### What was built

- Git repo with a repo-scoped (local, not global) commit identity.
- `infra/docker-compose.yml` — Redpanda (Kafka-API compatible), TimescaleDB
  (Postgres 16 + Timescale extension), Redis 7, OpenTelemetry Collector,
  Prometheus, Grafana (pre-provisioned with a Prometheus datasource).
- `scripts/verify_stack.py` — live round-trip checks, not mocks:
  - Kafka: creates a real topic, produces a message, consumes it back,
    asserts byte-for-byte equality.
  - TimescaleDB: connects, creates the `timescaledb` extension, creates an
    actual hypertable, inserts and reads a row, drops the table.
  - Redis: sets and gets a real key with TTL.
  - OTel Collector: hits its health-check extension endpoint.
  - Prometheus: hits `/-/healthy`, then polls `/api/v1/targets` until the
    Collector's scrape target reports `up` (not just "process started").
  - Grafana: hits `/api/health`, then calls the datasources API and asserts
    a Prometheus datasource was actually provisioned (not just that Grafana
    is reachable).
- `.github/workflows/ci.yml` — two jobs: `lint` (ruff), and
  `foundations-stack`, which brings up the *exact same* `docker-compose.yml`
  on a GitHub Actions runner, runs the live verification script against it,
  and dumps logs on failure. CI exercises the real stack, not a stub.
- `Makefile` — `make up` / `make verify` / `make down` / `make lint` for
  local dev parity with CI.
- Root `pyproject.toml` — pinned dependencies, `venv`-based (no Homebrew,
  no new package manager installed).

### Bugs found during live verification (not unit tests)

**Bug 1 — Redpanda single listener.** Redpanda's first configuration advertised a single Kafka listener
(`redpanda:9092`) to all clients. The verify script, running on the host
(not inside the Compose network), failed with a DNS resolution error —
`Failed to resolve 'redpanda:9092'` — not a connection refusal. Root cause:
Kafka's protocol requires a second, separate connection after the initial
bootstrap, to whatever address the broker's metadata response advertises;
that address only resolves inside Docker's internal network DNS.

Fixed with Redpanda's standard dual-listener pattern: an `internal` listener
advertised as `redpanda:9092` (for other containers — Flink, future
ingestors) and an `external` listener advertised as `localhost:19092` (for
anything on the host). Full writeup and trade-offs in
[`DECISIONS.md`](../DECISIONS.md).

This is exactly the kind of bug a healthcheck or `docker compose ps` would
never catch — only an actual produce/consume round trip from outside the
container network surfaced it, which is why Milestone 1's bar is "live
round trip," not "port is open."

**Bug 2 — OTel Collector healthcheck can never pass.** The
`otel-collector-contrib` image is distroless: no `/bin/sh`, no `wget`, no
`curl`, nothing but the collector binary. A `CMD-SHELL` healthcheck exec
fails on every container, regardless of whether the collector itself is
working — confirmed by checking the collector's own logs (fully healthy,
serving) against its healthcheck failure history (`exec: "/bin/sh": ...
no such file or directory`). Fixed by removing the container-level
healthcheck and relying on `verify_stack.py`'s real HTTP call to the
collector's health endpoint, which is a more meaningful check anyway. Full
reasoning in [`DECISIONS.md`](../DECISIONS.md).

### Failure mode exercised live

Killed the TimescaleDB container (`docker kill`) mid-verification-run to
confirm:
1. The verify script fails fast and cleanly (exit code 1, clear error,
   no hang) rather than blocking indefinitely on a dead connection.
2. Restarting the container (`docker compose up -d timescaledb`) brings it
   back to a `healthy` state and the full verification suite passes again
   with no manual cleanup needed.

### Progress notes

- All 6 services verified healthy and round-trip tested locally
  (2026-10-09).
- Lint (`ruff check .`) passes clean with zero findings.
- Deferred nothing in this milestone's own scope — every item the spec
  names for Foundations (repo, CI, local orchestration, Kafka, Postgres+
  Timescale, Redis, observability) is built and live-verified.
