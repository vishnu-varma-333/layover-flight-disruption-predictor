# Build milestones

Each milestone ends with something running and tested — not just code
committed. Checkboxes track completion; progress notes record what actually
happened, including bugs found during live verification (not just what was
planned).

- [x] **1. Foundations** — Repository, CI, local Kubernetes or VM, Kafka,
      PostgreSQL with TimescaleDB, Redis, observability.
- [x] **2. Historical data** — Download and clean US on-time records into
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

## Milestone 2 — Historical data

**Goal:** turn BTS's public On-Time Performance data into UTC-correct,
analysis-ready Parquet, plus the aircraft rotation chains (tail number ->
ordered flights per day) the delay-propagation model depends on — and
prove it on the real dataset, not a sample.

### What was built

- `src/layover_data/download.py` + `scripts/fetch_historical_data.py` —
  resumable, retrying downloader for 12 months of BTS data (Aug 2025-Jul
  2026, the most recent full year published) plus the OurAirports
  reference table.
- `src/layover_data/airports.py` — IATA-code -> IANA-timezone lookup via
  `timezonefinder` (offline, lat/lon-based — OurAirports carries no
  timezone column), covering US states plus Puerto Rico, Guam, American
  Samoa, the Northern Mariana Islands and the US Virgin Islands (BTS's own
  domestic scope), with historical-code recovery for renamed airports.
- `src/layover_data/clean.py` — BTS row -> UTC-timestamped Flight record.
  Arrival time derived from departure + elapsed minutes (BTS gives no
  arrival date at all); BTS's "2400 = midnight next day" convention handled
  explicitly; cancelled/diverted flights and non-positive elapsed times
  guarded against rather than silently computed into nonsense.
- `src/layover_data/rotations.py` — vectorized rotation builder: for every
  (tail_number, date), the ordered flight chain plus a chain-break count
  (data-quality signal, not a filter).
- `scripts/build_historical_data.py` — orchestrates download -> clean ->
  rotations -> Parquet, reading each month's CSV straight out of its zip
  (no double-storage on disk).
- `scripts/verify_historical_data.py` — 7 live DuckDB checks against the
  real processed Parquet: no null required fields, unique flight IDs,
  scheduled/actual arrival strictly after departure, cancelled flights
  carry no actual times, every rotation leg references a real flight,
  rotation leg counts match actual per-aircraft-per-day flight counts.
- 22 unit tests covering BTS's real quirks (2400 rollover, DST-ambiguous
  local times pinned against Python's own `zoneinfo` semantics, cancelled/
  diverted handling, duplicate flight IDs, territory inclusion, renamed-
  airport alias recovery, non-positive elapsed time).

### Bugs found during live verification (not unit tests)

**Bug 1 — gzip Content-Length mismatch in the downloader.** The OurAirports
CSV fetch failed every time with "got more bytes than expected." The
GitHub Pages CDN serves it gzip-encoded; `Content-Length` describes the
compressed wire size, but `requests` transparently decompresses it. Fixed
by skipping the length check (and Range-resume) for any gzip-encoded
response. Full writeup in [`DECISIONS.md`](../DECISIONS.md).

**Bug 2 — assumed BTS column names were wrong for this endpoint.** `clean.py`
was first written against remembered column names
(`FL_DATE`, `OP_UNIQUE_CARRIER`, ...). Downloading one real file and
printing its actual header before running against all 12 months showed
the real names (`FlightDate`, `Reporting_Airline`, ...) — a different
naming convention entirely. Rewritten against the real schema.

**Bug 3 — 9 airports silently unresolvable, two different root causes.**
`BQN, GUM, PPG, PSE, SJU, SPN, STT, STX` are US territories (Puerto Rico,
Guam, American Samoa, Northern Mariana Islands, US Virgin Islands) that
OurAirports assigns distinct ISO country codes to, even though BTS counts
them as domestic — a scope decision, now made explicit. `PBI` (Palm Beach
International) is listed in OurAirports' *current* snapshot under a new
name and IATA code (`DJT`); the old code survived only in a `keywords`
field. Recovered via a keyword-alias scan that can never shadow a real
primary code. Combined fix restored 5,867 flights for August 2025 alone.

**Bug 4 — rotation building hung for 20+ minutes at real scale.** A
groupby-then-Python-loop implementation passed every unit test instantly
against small fixtures and then never finished against the real ~7M-row,
~1.75M-group dataset — confirmed genuinely stuck (not a logging artifact)
by running unbuffered and watching it stay pinned near 100% CPU for 20+
minutes with no completion. Rewritten as fully vectorized pandas
(`groupby().agg()` + a within-group `.shift()` for chain-break detection,
no per-group Python loop at all). Same unit tests pass unchanged; real
runtime dropped to **41 seconds**. Full root-cause writeup in
[`DECISIONS.md`](../DECISIONS.md) — the general lesson (any transform whose
tests only exercise single-digit row counts gets a real-scale timing check
before being called done) now applies project-wide.

**Bug 5 — 14 flights with scheduled arrival at/before departure.** Live
DuckDB verification caught this; investigating the actual rows (not just
patching the check) found a genuine BTS source-data defect — negative
`CRSElapsedTime` in the raw data for these 14 rows, not a bug in this
project's UTC derivation. Fixed by treating non-positive elapsed time as
a missing value (null arrival) rather than propagating a known-bad number
into a logically-impossible timestamp, with a logged count so it's
auditable, never silent.

### Progress notes

- Final run (2026-10-10): 7,043,858 flights cleaned from 12 real BTS
  monthly files, 1,747,220 rotations built, all 7 live DuckDB checks pass.
- 99.7% of flights carry a tail number (rotation-eligible); 92.4% of
  rotations are multi-leg; 123,669 chain breaks logged as a data-quality
  signal (ferries/equipment swaps — not filtered out).
- Lint and all 22 unit tests pass clean.
- Deferred nothing in this milestone's own scope. The 12-month window
  (vs. "all years" of BTS history) is a documented, reversible scope
  choice — see DECISIONS.md — not a dropped requirement.
