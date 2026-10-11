# Build milestones

Each milestone ends with something running and tested — not just code
committed. Checkboxes track completion; progress notes record what actually
happened, including bugs found during live verification (not just what was
planned).

- [x] **1. Foundations** — Repository, CI, local Kubernetes or VM, Kafka,
      PostgreSQL with TimescaleDB, Redis, observability.
- [x] **2. Historical data** — Download and clean US on-time records into
      Parquet; build rotation history from tail numbers.
- [x] **3. Baseline and model** — A simple baseline, then the LightGBM model
      with rotation, turnaround and airport features; tracked in MLflow.
- [x] **4. Backtesting harness** — Replay historical days through Kafka and
      measure lead time and accuracy. First headline numbers.
- [x] **5. Live ingestion** — OpenSky, weather and airport-status ingestors
      with credit budgeting.
- [x] **6. Stream processing** — Flink jobs for event-time handling,
      rotation linking and feature updates.
- [x] **7. Prediction service** — FastAPI serving risk and explanations
      from online features.
- [x] **8. Trip monitoring and alerts** — Temporal workflows, deduplicated
      push and email alerts, kill tests.
- [x] **9. Web app** — Trips, live risk, aircraft chain view,
      notifications.
- [x] **10. Ship it** — AWS deploy, load and replay reports, model card,
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

## Milestone 3 — Baseline and model

**Goal:** turn the Milestone 2 Flight/Rotation data into a feature table,
and prove the central hypothesis of this whole project — that tracking
the inbound aircraft meaningfully improves delay prediction over a
schedule-only model — with a real, controlled comparison on real data,
not an assumed result.

### What was built

- `src/layover_model/features.py` — one feature table covering schedule
  features (carrier, origin, dest, local departure hour/day-of-week/
  month) and rotation/turnaround features (previous-leg delay, turnaround
  minutes, a tight-turnaround flag, chain-break flag, cumulative same-day
  delay), all computed with vectorized pandas groupby/shift/cumsum —
  deliberately, learning from Milestone 2's rotation-building performance
  bug, never a per-row or per-tiny-group Python loop.
- A short literature pass (see DECISIONS.md) before finalizing the
  feature set, which directly added two features (`tight_turnaround`,
  `cum_delay_today`) the original design hadn't included, and produced a
  deliberate, documented decision to *not* build graph-based airport-
  centrality features or meta-models for v1.
- `scripts/train_model.py` — trains three LightGBM models (departure
  delay; arrival delay pre-departure; arrival delay post-departure, once
  this flight's own departure delay is known — matching the spec's
  "updated as new events arrive"), each logged to MLflow with params and
  metrics. The headline arrival-delay model is additionally compared
  against a schedule-only ablation of the *same* algorithm (isolating the
  rotation signal's own contribution) and a naive median-delay floor.
- A strict time-based holdout (train on Aug 2025-Jun 2026, test on Jul
  2026) rather than a random row split — avoiding same-day leakage across
  train/test and previewing Milestone 4's by-date replay methodology.
- 7 unit tests on synthetic fixtures covering local-hour correctness
  (pinned against Python's own `zoneinfo`), no-leakage rotation shifting,
  chain-break detection, the tight-turnaround threshold, and cumulative
  same-day delay.

### Bugs and a methodology error found during live verification

**Bug 1 — environment: LightGBM can't load on macOS without Homebrew.**
`import lightgbm` failed looking for `libomp.dylib` only at Homebrew/
MacPorts paths. Fixed by pointing `DYLD_LIBRARY_PATH` at scikit-learn's
own bundled (and ABI-compatible) copy instead of installing anything new
— `make train-model` resolves this automatically. Full writeup in
DECISIONS.md.

**Bug 2 — `tight_turnaround` became an unusable `object`-dtype column.**
Mixing float NaN with Python `bool` values in one pandas Series silently
produces `object` dtype, which LightGBM rejects outright. Fixed by
keeping the column float64 throughout (0.0/1.0/NaN) instead of NaN mixed
with real booleans.

**Methodology error — L2 objective vs. an MAE metric.** The schedule-only
ablation model scored *worse* than a naive median baseline under
LightGBM's default objective — a red flag, since the median is the best
possible constant predictor specifically under MAE. Diagnosed as an
objective/metric mismatch (L2 targets the mean; delay is right-skewed, so
the mean overshoots on the many near-zero-delay flights) rather than
patched over or reported as-is. Switching every model to `regression_l1`
fixed it: the schedule-only model then correctly beat the naive floor,
and the ablation margin became trustworthy. Full writeup in DECISIONS.md
— this is the kind of check the user explicitly asked be done thoroughly
for feature engineering and modeling decisions, not rushed.

### Results (real holdout, Jul 2026, 612,038 test flights)

| Model | Naive median MAE | Schedule-only MAE | Full (rotation) MAE |
| --- | --- | --- | --- |
| Departure delay | 26.95 min | — | **15.55 min** |
| Arrival delay, pre-departure | 31.36 min | 29.82 min | **21.41 min** (28.2% better than schedule-only) |
| Arrival delay, post-departure | 31.36 min | — | **10.12 min** |

The headline number: tracking the inbound aircraft's rotation cuts
arrival-delay prediction error by **28.2%** versus a schedule-only model
of the identical algorithm, trained and evaluated identically — isolating
the value of this project's core idea, not just showing a model beats a
weak strawman. Once a flight has actually departed, the arrival-delay
estimate sharpens further (21.41 -> 10.12 minutes), confirming the
"updated as new events arrive" design is working as intended.

### Progress notes

- Full pipeline (load 7,043,858-row feature table + train/eval/log 4
  LightGBM models) runs in ~7.5 minutes on this machine — ~23s for
  feature engineering, ~7 minutes for training and MLflow logging. Real
  numbers and methodology in docs/BENCHMARKS.md.
- Lint and all 29 unit tests (22 from Milestone 2 + 7 new) pass clean.
- Deferred, by deliberate and documented decision, not oversight: graph-
  based airport-centrality features and meta-models (considered during
  the literature pass, scoped out of v1 — see DECISIONS.md); hyperparameter
  tuning (reasonable defaults used, formal tuning not required by the
  spec for this milestone). Real airport-condition features (live
  weather/FAA status) remain blocked on Milestone 5's ingestion, per the
  spec's own milestone order — not a skipped requirement, a sequencing
  dependency.

## Milestone 4 — Backtesting harness

**Goal:** replay a real month of flights through the actual Kafka
infrastructure from Milestone 1 — event by event, in the order
information genuinely becomes available — scoring each flight with the
Milestone 3 models as that information arrives, and measure the headline
numbers: warning lead time and detection recall.

### What was built

- `src/layover_backtest/events.py` — turns Flight records into three
  event types per flight (schedule known, departed, landed), each
  carrying only what's genuinely known at that moment; globally sorted by
  event time for producer-side, single-partition chronological replay.
- `src/layover_backtest/scorer.py` — a stateful, point-in-time-correct
  scorer: per-tail rotation state built incrementally from events (never
  from the already-computed, and therefore leaky, Milestone 3 batch
  feature table), re-scoring the next scheduled leg the instant its
  inbound aircraft lands. Feature capture and model inference are
  deliberately decoupled for performance (see Bugs below).
- `src/layover_backtest/metrics.py` — lead time and detection recall
  (reported together, per early-warning-evaluation research — see
  DECISIONS.md), segmented by whether a flight had any inbound rotation
  signal at all, plus an MAE cross-check against Milestone 3's offline
  evaluation using the identical trained models.
- `scripts/run_backtest.py` — orchestrates: loads the three Milestone 3
  models from MLflow, builds the event stream, produces it to Redpanda,
  consumes and scores it, computes and prints every metric.
- A research pass (point-in-time correctness principles; early-warning
  recall/lead-time evaluation practice) done before designing the replay,
  not after something looked wrong — see DECISIONS.md.
- 11 new unit tests (plain-dict event stream construction, the scorer's
  state machine using stub models — no real Kafka or LightGBM needed to
  verify the logic) plus the full project test suite still passing.

### Bugs found during live verification — six, across two full-month attempts

A real performance saga, documented in full in DECISIONS.md:

1. **Kafka producer `BufferError`** — missing periodic `poll(0)` during a
   tight produce loop let librdkafka's local queue fill up.
2. **Single-row `model.predict()` costs ~2.6ms purely from fixed per-call
   overhead** (profiled: a 500-row batch costs the same ~2.6ms *total*).
   Fixed by decoupling feature capture from model inference — the scorer
   returns unscored requests, batched and predicted once at the end.
3. **Building even a single-row pandas DataFrame costs ~2.4ms/call**, the
   same order of overhead as #2 and the same root cause: pandas is built
   for bulk operations, not thousands of tiny ones — a direct echo of
   Milestone 2's rotation-building bug. Fixed by returning plain Python
   dicts from the hot path (measured: ~0.0014ms/call, ~1700x faster) and
   doing all DataFrame construction once, in bulk, at the end.
4. **`.values` on a timezone-aware column silently strips the timezone**,
   crashing the lead-time calculation with a tz-naive-vs-aware subtraction
   error. Fixed by building results from the Series directly.
5. **The real dominant cost at full-month scale**: cancelled flights
   leaked into a "pending" dict with no event ever removing them,
   scanned in full on every one of ~600K landed events. Fixed by indexing
   pending flights by `(tail_number, fl_date)` and never adding cancelled
   flights to it at all. Measured: full-month throughput went from 667
   events/sec (timing out before finishing) to **14,338 events/sec**.
6. **A partial, timed-out consume was reported as a final result** — the
   first full-month run silently processed only 21.5% of events, then
   printed "final" metrics against the full month's ground truth anyway,
   making recall look catastrophically wrong in a way that read as a real
   finding. Fixed by making an incomplete consume a hard error.

### The headline result, with the caveat it needs

**Updated 2026-10-10 (Milestone 7):** re-measured after fixing a real
bug in `BacktestScorer._feature_dict` (it re-read the *live* `leg_index`
counter instead of the value frozen at schedule time, feeding the
trained models a subtly wrong value at serving time throughout this
milestone's original run — see DECISIONS.md). The numbers below are the
corrected, current ones.

**64.9% recall, 219-minute median lead time** for detecting a real
15+-minute delay before scheduled departure, using the rotation-aware
model — rising to **73.9% recall** for flights that have any inbound
aircraft to track at all (vs. 3.9% for a tail's first leg of the day,
where no rotation signal can possibly exist). The schedule-only model's
nominal "1,440-minute lead time" is a simulation artifact (it only ever
scores once, 24h ahead, by construction) masking just an 11.0% recall —
reported deliberately alongside recall, not lead time alone, per the
early-warning-evaluation research above. (Original, pre-fix numbers:
66.0% / 232 min / 74.9% / 5.9% — the leg_index fix moved these modestly,
not dramatically, consistent with leg_index being a weak secondary
signal rather than the rotation model's main driver.)

The MAE cross-check validates the whole harness against Milestone 3's
offline numbers, using the identical trained models: schedule-only MAE
29.82 (exact match to Milestone 3's 29.82), post-departure MAE 10.12
(exact match), rotation MAE 22.07 (vs. 21.41 — a small, expected, and
*more* correct divergence from this harness's stricter point-in-time
discipline, not an error).

### Progress notes

- Final run (2026-10-10): 631,970 flights replayed as 1,859,448 events
  through the real Redpanda stack, full month processed in 130 seconds
  after the performance fixes (down from a first attempt that never
  finished within a 600-second timeout).
- Lint and all 40 unit tests (29 from Milestones 1-3 + 11 new) pass clean.
- Deferred nothing in this milestone's own scope. Connection-risk
  probability (precision/recall/calibration of `p_miss`) is explicitly
  out of scope here — it needs a Trip/TripLeg entity with a minimum
  connection time that doesn't exist yet (Milestones 8-9), a sequencing
  dependency like Milestone 3's airport-condition features, not a
  dropped requirement.

## Milestone 5 — Live ingestion

**Goal:** stop being purely historical. Build real, continuously-running
ingestors for aircraft positions (OpenSky), airport weather (NOAA) and
airport status (FAA), normalized into one Kafka event stream, with
credit-aware polling for the one source that's actually rate-limited.

### What was built

- `src/layover_ingest/opensky.py` — polls OpenSky's `/states/all` for 15
  monitored US hub airports' local airspace, tracking real remaining
  daily credits directly from the API's own response header (never
  estimated locally) and stopping for the day once a configurable reserve
  is reached.
- `src/layover_ingest/weather.py` — polls NOAA's Aviation Weather API for
  METAR and TAF reports, all 15 monitored airports in two calls total
  (not per-airport) since this source isn't credit-limited.
- `src/layover_ingest/airport_status.py` — polls the FAA's public NAS
  Status XML feed (one global endpoint covering every US airport),
  parsing generically by delay category so new FAA fields or categories
  don't need a code change to be captured, filtered to the monitored set.
- `src/layover_ingest/scheduling.py` — the shared cycle-sleep logic every
  ingestor uses (see the bug below).
- `scripts/run_live_ingestion.py` — runs all three concurrently via
  `asyncio.gather`, sharing one Kafka producer, for either a bounded
  `--duration` (verification) or indefinitely (real deployment).
- `scripts/verify_live_ingestion.py` — live checks against real consumed
  events (plausible lat/lon, a non-empty raw weather report, status
  events restricted to monitored airports), not fixtures.
- A verification-first research pass before building anything: confirmed
  live that OpenSky's anonymous tier still exists and measured its real
  per-call credit cost empirically (exactly 1 credit for a small bounding
  box — two real calls, comparing the rate-limit header before and
  after), rather than trusting assumptions or documentation alone.
- 13 new unit tests, all offline against real-shaped fixture data
  (captured live during development, not hand-invented): OpenSky state-
  vector parsing and credit-budget logic, METAR/TAF parsing, FAA XML
  parsing (including that an unmonitored airport like PNS is correctly
  filtered out), and the cycle-scheduling fix.

### Bug found during live verification

A `--duration 90` test run was still running several minutes later.
Root cause: each ingestor's sleep between cycles considered only its own
interval (OpenSky defaults to 3600s), never the overall run's duration
budget — so after its first real cycle, it would sleep for the better
part of an hour before ever checking whether the 90-second budget had
elapsed. Fixed with one shared `sleep_until_next_cycle` helper that caps
the sleep at whatever's left of the overall deadline. Verified live: a
`--duration 60` run with production-default intervals now exits at 61
seconds, not whenever its longest configured interval happens to elapse.

### Live verification (real data, real external APIs)

A bounded 60-second run produced, and `verify_live_ingestion.py`
confirmed via direct Kafka consumption: 571 real `aircraft_position`
events (live flights near the 15 monitored hubs, including a real United
flight's callsign, position and ground state at SFO), 30 real
`airport_weather` events (METAR + TAF for all 15 airports), and a real
`airport_status` event (an actual current FAA closure notice for LAX,
correctly excluding a simultaneous closure at an unmonitored airport).
Credits dropped by exactly 15 (one per monitored airport), consistent
with the measured 1-credit-per-call cost.

### Progress notes

- Lint and all 53 unit tests (40 from Milestones 1-4 + 13 new) pass clean.
- Live ingestion is deliberately not part of automated CI — unlike prior
  milestones' CI jobs (which exercise this project's own local infra),
  these ingestors call real third-party APIs against a real, shared,
  metered daily budget. Running them on every push would burn OpenSky's
  anonymous quota unpredictably; CI still runs every offline unit test.
- Deferred nothing in this milestone's own scope. Trip-linked targeted
  polling (narrowing OpenSky queries to aircraft tied to a specific
  monitored trip, rather than a fixed demo-airport list) is a natural
  extension once Milestones 8-9 introduce the Trip entity — a sequencing
  dependency, not a dropped requirement, exactly like Milestone 3's
  deferred live-weather features were resolved by this milestone.

## Milestone 6 — Stream processing

**Goal:** replace Milestone 4's Python, producer-sorted-order assumption
with real Apache Flink stream processing — genuine event-time watermarks,
Flink-managed keyed state, and explicit handling of late, out-of-order,
duplicate and missing events, exactly as the spec names them.

### What was built

- A full PyFlink environment, installed entirely user-locally (no
  Homebrew): Java 17 (Eclipse Temurin, direct download), `apache-flink`
  2.2.1, and a separately-downloaded Kafka connector JAR matched to it —
  with two real pip dependency conflicts (a missing `pkg_resources` during
  `apache-beam`'s build, and a `pyarrow` version clash) diagnosed and
  fixed at the root cause rather than worked around.
- `src/layover_stream/rotation_state.py` — the pure, Flink-independent
  rotation-linking logic (last-event-time-wins merge for out-of-order
  "landed" events, the same feature computation as Milestones 3-4),
  testable without a Flink runtime.
- `src/layover_stream/rotation_job.py` — the real Flink job: a
  `KeyedProcessFunction` keyed by tail number, with Flink `ValueState`/
  `MapState` for rotation tracking, an explicit seen-keys dedup set, and
  watermark-driven late-event detection routing to a side output.
- `src/layover_stream/messy_events.py` — deliberately reorders,
  duplicates, delays and drops events from Milestone 4's clean event
  generator, independently controlling all four categories the spec names
  verbatim ("late, out-of-order, duplicate and missing messages").
- `scripts/produce_flight_events.py` / `verify_rotation_job.py` — produces
  a real day's messy event stream plus a ground-truth reference (computed
  by running the same pure logic on the *clean* stream in plain Python),
  then compares the real Flink job's output against it.
- 15 new unit tests (the pure rotation-state logic, the messiness
  injector's four categories) plus a minimal isolated repro test that
  diagnosed the watermark-propagation bug below.

### Bugs found during live verification — five, all fixed, one substantial

1. A `max(1, ...)` floor forced a "very late" event even when the rate
   was explicitly 0 — caught by a unit test before touching live data.
2. Ground truth and the real job initially compared different
   `event_time` representations (pandas `Timestamp` vs. post-Kafka-JSON
   float), which would have made every turnaround-time comparison look
   like a mismatch regardless of whether the job was actually correct.
3. `KafkaSource`/`KafkaSink` needs an actual `SimpleStringSchema()`
   instance for (de)serialization, not a `Types.STRING()` type
   descriptor — caught immediately on first run.
4. PyFlink's Python API has no `ctx.output()` for side outputs (unlike
   Java Flink) — a side output is a `yield (OutputTag, value)` tuple.
5. **The substantial one**: binding a watermark strategy via
   `from_source(source, watermark_strategy, ...)` silently fails to
   propagate the assigned event-time to `ctx.timestamp()` for a Python
   `KeyedProcessFunction` reading a `KafkaSource` — every event's
   timestamp was reporting wall-clock "now" instead of its real July 2026
   flight time, making the late-detection path dead code that looked
   like it worked. Diagnosed with temporary instrumentation and a minimal
   3-line isolated repro, not assumed; fixed by attaching the real
   watermark strategy via an explicit `.assign_timestamps_and_watermarks()`
   call on the stream instead of through `from_source`'s built-in binding.

Full root-cause writeups for all five in DECISIONS.md.

### Results (real data: 2026-07-15, 20,055 flights, 61,033 messy events)

**Updated 2026-10-10 (Milestone 7):** the research pass for the
prediction service traced this job's feature output against the actual
training schema and found it was missing 8 of 14 required fields
entirely, plus a `turnaround_minutes` reference-point bug — both fixed,
along with a related bug in Milestone 4's `BacktestScorer` found by the
same pass. Full writeup, and why the match rate below reads lower than
the original 95.8% (a stricter, more trustworthy comparison, not a
regression), in DECISIONS.md.

- **302 late events** correctly routed to a side output (the messy
  stream displaced ~300 — confirms the mechanism isn't over- or
  under-triggering).
- **92.7% of flights present in both ground truth and Flink's output
  match on every one of the real 14 training-schema fields** (up from
  checking only 6 fields, against a partly-wrong reference, before this
  fix). The remaining 7.3% is the expected consequence of deliberately
  dropped/very-late data genuinely being unrecoverable information, not
  a bug — a system can't know what it was never told and correctly
  discarded as too late to trust. Schedule-side fields (carrier/origin/
  dest/month/day_of_week/dep_hour_local) match 100%, since they don't
  depend on delivery order.
- Full bounded run (real JVM, real connector, real Redpanda): **85
  seconds**, unchanged by the fix.

### Progress notes

- Lint and all 68 unit tests (53 from Milestones 1-5 + 15 new) pass clean.
- PyFlink's live verification is a deliberate manual step
  (`make rotation-produce && make rotation-job && make rotation-verify`),
  not part of automated CI — the local JVM/connector setup is heavier
  than this project's other CI jobs warrant provisioning fresh on every
  push. CI still runs every offline unit test on every push.
- Deferred nothing in this milestone's own scope. Full timer-based global
  event-time reordering (Flink's most idiomatic, but substantially more
  complex, answer to out-of-order per-key processing) was considered and
  deliberately not built — documented as a real trade-off in DECISIONS.md,
  not a shortcut that went unmentioned; the simpler watermark-plus-last-
  write-wins design correctly handles every messiness category this
  milestone actually tests, confirmed live.

## Milestone 7 — Prediction service

**Goal:** serve each flight's current predicted delay and an
evidence-backed explanation from the online feature state Milestone 6's
real Flink job produces — a genuine FastAPI service reading a real
Redis-backed feature store, not a stub.

### A research pass that found real correctness problems before any service code was written

Tracing whether `stream.flight_features` actually carried the full
training feature schema (SCHEDULE_FEATURES + ROTATION_FEATURES) — the
first step, per the standing instruction to research feature-engineering
work properly rather than start coding — found it didn't: 8 of 14
required fields were missing entirely, `turnaround_minutes` was measured
from the wrong timestamp, and (found by switching Milestone 6's ground
truth to Milestone 4's independently-validated `BacktestScorer`) that
scorer itself had a real `leg_index` bug live since Milestone 4. All
three fixed, with Milestone 4's backtest and Milestone 6's verification
both re-run to get corrected real numbers. Full writeup in DECISIONS.md;
the corrected numbers are reflected in Milestones 4 and 6 above and in
docs/BENCHMARKS.md.

### What was built

- `src/layover_predict/feature_store.py` — the Redis-backed online
  feature store: one JSON record per flight, 48h TTL, keyed for O(1)
  lookup.
- `scripts/sync_features_to_redis.py` — a dedicated Kafka consumer
  syncing `stream.flight_features` into Redis, deliberately separate
  from the Flink job itself (see DECISIONS.md for why).
- `src/layover_predict/service.py` — the FastAPI service
  (`GET /flights/{flight_id}/risk`): looks up a flight's latest online
  features, picks the pre- or post-departure model based on whether its
  own departure delay is known yet (mirroring Milestone 3's own model
  design), serves the real trained LightGBM model from MLflow, and
  returns `predicted_delay` plus an evidence-backed `explanation`.
  `p_miss` is returned null — it needs a `TripLeg` with a minimum
  connection time that doesn't exist until Milestone 8, the same
  sequencing gap Milestone 4 already named, not a dropped requirement.
- `src/layover_predict/explain.py` — turns LightGBM's native
  `pred_contrib` (exact, TreeSHAP-consistent per-prediction feature
  attribution — the tech-stack table's own stated reason for choosing
  LightGBM) into ranked, evidence-backed sentences ("inbound aircraft's
  previous leg landed 107 min late"), not a generic feature-importance
  ranking or an LLM-generated explanation.
- `src/layover_backtest/scorer.py` — gained a `prepare_frame` helper
  (the DataFrame construction and dtype coercion already inside
  `score_requests`, factored out) so the prediction service reuses the
  exact same, already-tested categorical/numeric handling Milestone 4
  validated, rather than a second implementation of it.
- `scripts/verify_prediction_service.py` — live checks against the real
  running service: every sampled real flight gets a valid, non-empty
  explanation, and one flight's prediction is independently re-derived
  (same Redis record, same MLflow model, no HTTP layer) and checked
  against the service's own HTTP response.
- 7 new unit tests for `explain.py`'s ranking and sentence logic
  (synthetic rows/contributions, no Redis or MLflow needed).

### Bugs found during live verification (two, both fixed)

1. `prepare_frame` assumed every request dict always carries a
   `dep_delay_min` key (true of `BacktestScorer`'s own feature dicts,
   not of the Flink job's pre-departure-stage records) — raised a
   `KeyError` the first time a flight that hadn't departed yet was
   queried. Fixed by filling the key in explicitly before scoring.
2. A genuinely very-late inbound aircraft can push `turnaround_minutes`
   negative (its landing is already past this flight's own scheduled
   departure) — read as a reassuring "a tight turnaround" instead of
   what it actually is. Fixed with an explicit negative-value case in
   the explanation text.

Full root-cause writeups for both, and for the three research-pass bugs
above, in DECISIONS.md.

### Results (same 2026-07-15 demo day, 19,746 flights)

- All 77,380 real Flink feature updates synced to Redis in a single
  20-second bounded run, converging to 19,746 distinct keys.
- Every one of 15 real sampled flights returned a valid risk assessment
  with a non-empty, evidence-backed explanation over HTTP.
- Prediction endpoint latency (200 real flights, Redis lookup + feature
  assembly + model inference + `pred_contrib`, end to end): **p50
  9.5ms, p95 13.9ms, p99 19.2ms**.
- A live cross-check: one flight's prediction, re-derived independently
  with no HTTP layer, matched the service's own HTTP response exactly.

### Progress notes

- Lint and all 77 unit tests (70 from Milestones 1–6 + 7 new) pass clean.
- Deferred nothing in this milestone's own scope. `p_miss` is a named
  sequencing dependency on Milestone 8's `TripLeg` entity, exactly like
  Milestone 4's identical gap — not a dropped requirement.

## Milestone 8 — Trip monitoring and alerts

**Goal:** a long-running Temporal workflow per trip that watches its
flights for days, survives a real restart, and alerts only when
connection risk crosses a threshold or changes meaningfully — real
Temporal, a real database, real notification channels, and a real kill
test, not a simulation of any of them.

### A research pass before any workflow code

Five concrete questions answered before writing `workflow.py`: how to
run a real (non-dev-server) Temporal server locally without Homebrew;
which `temporalio` version this project's Python 3.9 can install
(1.18.1 — 1.19.0+ needs 3.10, a real named tradeoff); the correct
workflow/activity/signal/query shapes; what Temporal's retries actually
guarantee (at-least-once, never exactly-once — the real dedup has to be
application-level, which is exactly what the spec's `Alert.dedup_key`
field is for); and what a Workflow ID actually prevents. Full writeup,
and why each answer shaped the design, in DECISIONS.md.

### What was built

- `infra/docker-compose.yml` — a real self-hosted Temporal server
  (`temporalio/auto-setup`, sharing this project's existing Postgres
  rather than a second container), `temporal-ui`, and Mailpit (a real
  local SMTP catcher for the email channel).
- `src/layover_trip/db.py` — the real `trips`/`trip_legs`/`alerts`/
  `risk_assessments` schema in TimescaleDB (the spec's own data model,
  applied for the first time — Milestone 1 provisioned this database but
  nothing used it as real application storage until now).
  `risk_assessments` is a hypertable, matching the spec's "positions and
  conditions as time series" pattern extended to risk history.
- `src/layover_trip/risk.py` — turns a Milestone 7 point-prediction into
  `p_miss` using empirically measured residual-quantile tables (101
  points, n=612,038, one per model) rather than an assumed Gaussian
  error model — the same research-before-implementing discipline
  Milestone 3's feature design used.
- `src/layover_trip/alerts.py` — the threshold-plus-hysteresis alert
  policy (`decide_alert`): pure, no I/O, pinned by 6 unit tests covering
  first-crossing, non-spam-on-staying-elevated, meaningful-escalation,
  and reset-on-drop-then-fresh-recross.
- `src/layover_trip/workflow.py` / `activities.py` — the actual
  `TripMonitorWorkflow`: a durable check loop
  (`workflow.wait_condition(..., timeout=...)`, not `asyncio.sleep`),
  a `stop_watching` signal, `get_latest_assessments`/`get_alert_counts`
  queries, and activities for the real I/O (calling the Milestone 7
  prediction service, persisting risk assessments, sending alerts).
- `src/layover_trip/notify.py` — real SMTP to Mailpit; real Web Push
  (VAPID-signed, AES128GCM-encrypted via `pywebpush`) with a
  self-generated VAPID identity (no external account needed).
- `scripts/run_trip_worker.py`, `start_trip_monitor.py`,
  `verify_trip_monitoring.py` — the worker process, a real trip-creation
  script using a genuine tight connection from the Milestone 2 dataset,
  and the full live kill test.
- 13 new unit tests (6 for the alert policy, 7 for the risk calibration)
  plus a 7-test regression suite entry for a real upstream bug found
  while fixing this milestone's own `BacktestScorer` reuse (see below).

### Bugs found building and live-testing this milestone (five, all fixed)

1. `py_vapid==1.9.2`'s `generate_keys()` is broken against
   `cryptography==46.0.0` (passes a class where an instance is required)
   — a confirmed upstream bug, worked around directly.
2. Temporal's workflow-determinism sandbox statically validates every
   module a workflow file imports, transitively — `requests` and this
   project's own `notify.py` (a module-level `Path.resolve()` call)
   both tripped it, even though neither ever runs inside the workflow's
   own deterministic replay. Fixed via Temporal's own sandbox-passthrough
   mechanism.
3. Temporal's strict dataclass converter rejects `None` for a field
   typed as plain `float`/`int`, not `Optional[...]` — broke workflow-
   input deserialization for a trip's last leg (which genuinely has no
   next connection). Fixed by typing every nullable field `Optional`.
4. An unread `subprocess.Popen(stdout=PIPE)` deadlocks once a chatty
   child process's output fills the OS pipe buffer — looked exactly
   like a hung worker. Fixed by logging to a real file instead.
5. **The substantial one**: on Python 3.9, `asyncio.TimeoutError` is a
   *different class* from the builtin `TimeoutError` (unified only in
   3.11) — `workflow.wait_condition`'s timeout exception escaped an
   `except TimeoutError:` entirely, which Temporal correctly treated as
   the whole workflow crashing, not a timeout being handled. This
   silently invalidated an earlier "passing" kill-test run (the workflow
   had already crashed before the kill even happened); re-confirmed
   after the fix using Temporal's own workflow history, not just the
   test's assertions.

Full root-cause writeups for all five in DECISIONS.md.

### The real kill test (2026-10-11, a genuine tight connection from real data)

UA539 (ORD→DFW) departed 115 minutes late in the real historical data;
AA1402 (DFW→AUS) was scheduled only 52 minutes after UA539's *scheduled*
arrival — a 45-minute minimum connection time leaving 7 real minutes of
slack. Not scripted to look dramatic; the dataset already contains it.

- Real `p_miss = 0.995` computed; exactly one real alert fired (one
  `alerts` row, one real email in Mailpit).
- Worker `SIGKILL`ed outright. Server-side workflow status: still
  `RUNNING`, confirmed via `describe()`, with zero workers polling.
- A full check interval elapsed with no worker running at all: zero new
  alerts (confirmed directly against the database).
- A fresh worker resumed monitoring — confirmed via Temporal's own
  workflow history, not just this test's assertions: `fetch_leg_assessment`
  was genuinely scheduled a second and third time across two real timer
  fires. `send_alert` was **not** scheduled again (risk hadn't changed),
  and the single `alerts` row for the connection's dedup key never
  became two.
- `stop_watching()` signaled; the workflow's `run()` returned for real
  and the trip's database row transitioned to `status='completed'`.

**No trip stopped being watched, no duplicate alert** — the spec's own
kill-test wording, demonstrated against a real crash.

### Web Push: real protocol, one documented environment limitation

The real Web Push protocol (VAPID + AES128GCM) is fully implemented and
exercised with a real generated subscriber keypair and a real HTTP
delivery (confirmed: correct VAPID JWT, correct encryption, correct
headers). A literal browser-issued subscription needs Milestone 9's
subscribe UI (or a real external browser) — this session's own sandboxed
browser tool has Service Worker registration disabled at the engine
level, confirmed live, not assumed (a plain `fetch()` of the identical
URL succeeds; `serviceWorker.register()` does not). A named, re-openable
gap, not a silent one — full writeup in DECISIONS.md.

### Progress notes

- Lint and all 90 unit tests (77 from Milestones 1–7 + 13 new) pass
  clean.
- Deferred nothing buildable in this milestone's own scope. Email uses a
  real local SMTP catcher (Mailpit) rather than Amazon SES — a named,
  reversible substitution (identical protocol, different
  host/credentials) made because SES needs a real AWS account this
  project doesn't have at this stage, not a shortcut around the alerting
  logic itself. Full Web Push browser verification is a sequencing
  dependency on Milestone 9's subscribe UI, exactly like Milestone 4's
  TripLeg gap and Milestone 7's `p_miss` gap before it.

## Milestone 9 — Web app

**Goal:** a real Next.js PWA — add a trip by flight number and date, see
each leg's live risk and explanation, watch the aircraft's chain of
flights for the day, enable push notifications, and share a public
flight page — backed entirely by the real Temporal workflow and
prediction service already built, not a mock UI.

### What was built

- A small internal FastAPI service, `layover_web.api`, is the real
  backend the web app talks to server-side only (never from the
  browser) — reusing every real piece already built: Temporal
  (starting/signaling trip monitors), TimescaleDB (trip/risk
  persistence), the real VAPID identity (Web Push), and the Milestone 2
  dataset (flight lookup and the aircraft-chain view). See DECISIONS.md
  for why this split, not a TypeScript rewrite of the backend logic.
- `src/layover_trip/trip_service.py` — trip-creation logic factored out
  of Milestone 8's demo script so both it and the new web API share one
  implementation of "what does adding a trip actually do."
- A real, lightweight sign-in (email, HMAC-signed cookie — no OAuth, no
  external identity provider credentials available) that Next.js owns
  directly, not the Python API.
- Pages: sign-in/trip list, add-a-trip (flight search with real
  disambiguation when a flight number covers more than one leg), trip
  detail (live risk, explanation, aircraft chain, push-subscribe,
  stop-monitoring), and a public, no-auth flight status page.
- A real PWA: manifest, installable icons, and a service worker that
  handles genuine Web Push events (not a cache-the-app-shell worker —
  this is a live-data app, so there's deliberately no offline caching
  that could show stale risk data as current).
- 5 new Python unit tests for flight search and the aircraft-chain
  lookup (pure logic against synthetic data, same pattern as every other
  milestone's pure-logic tests).

### Two real bugs found building and live-testing this (both fixed)

1. A carrier+flight_number+date is not always unique — UA539 on
   2026-07-15 is genuinely two different legs under the same flight
   number (a real through-flight/equipment-swap pattern in the BTS
   data). The original search silently returned the wrong half of this
   project's own demo connection; fixed by returning every match and
   letting the UI disambiguate, the way a real airline search would.
2. `navigator.serviceWorker.ready` hangs forever (not rejects) if
   Service Worker registration itself fails — found live, clicking the
   real push-subscribe button in the real running app, which hung on
   "Enabling..." indefinitely in this session's Service-Worker-disabled
   browser environment instead of failing visibly. Fixed by registering
   explicitly inside the subscribe flow so a real failure is caught and
   shown, not silently stuck.

Full root-cause writeups for both in DECISIONS.md.

### Live verification (real browser, real backend stack, 2026-10-11)

Signed in, created a real trip on the same tight UA539→AA1402 connection
Milestone 8's kill test used, and watched the trip detail page render a
real 99% "High risk" badge, a real +109 min predicted delay, the real
evidence-backed explanation, and the real aircraft-chain view — all
sourced live from the pipeline Milestones 6-8 already built, not mock
data. Confirmed the public flight page renders the same data with no
sign-in required, and that the push-subscribe button degrades gracefully
(not silently, not by hanging) with email alerts still working either
way. `npx tsc --noEmit`, `npm run lint`, and `npm run build` (a real
production build, not just dev mode) all pass clean.

### Progress notes

- Lint and all 95 Python unit tests (90 from Milestones 1–8 + 5 new) pass
  clean.
- Deferred nothing buildable in this milestone's own scope. OAuth/real
  identity verification is a named, deliberate scope boundary (no
  external provider credentials available, and the spec's own data model
  only needs a `user_id` string) — not a dropped requirement.

## Milestone 10 — Ship it

**Goal:** AWS deploy (as code), load and replay reports, a model card,
a demo video, a write-up — the spec's own "production readiness" bar
made real: containers for every service, least-privilege IAM, real
deletion on request, and every benchmark honestly labeled.

### What was built

- **Terraform for AWS** (`infra/terraform/`): one EC2 host running every
  service as a container (matching the spec's own cost-conscious "one
  modest VM" guidance), S3 for model/data artifacts, Secrets Manager for
  the session secret and DB password (real random values, never
  committed), least-privilege IAM (scoped to exactly one bucket and two
  secrets, not account-wide access). `terraform validate` and
  `terraform fmt` both clean. **Not applied** — no AWS account exists in
  this environment; see DECISIONS.md for why that's a stop-and-ask
  external blocker, not a shortcut.
- **Containers for every always-on service**: a shared `Dockerfile` for
  the prediction service, internal web API and Temporal trip worker
  (differentiated by command), a separate `web/Dockerfile` for the
  Next.js app (multi-stage, `output: "standalone"`), and
  `infra/docker-compose.prod.yml` wiring them to the existing
  Milestone 1 infra stack. Built and **live-tested together** — all four
  containers started, signed in and created a trip through the
  containerized web app, confirmed a cross-container HTTP call
  (web-api → predict-service) returned real risk data.
- **Portability fixes** this surfaced: every hardcoded `localhost` in
  the backend (db, Redis, Temporal, the prediction service) became an
  overridable env var (defaulting to the old hardcoded value, so local
  `make` development is unaffected); a real MLflow bug where model
  loading failed inside the container because MLflow bakes an absolute
  host path into a run's own metadata.
- **Connection-risk precision/recall/calibration**
  (`scripts/evaluate_connection_risk.py`): measured for the first time —
  67.7% precision / 72.5% recall at the production alert threshold, a
  calibration plot, on 23.5M synthetic connections (no real booking data
  exists to build a real evaluation from — documented as a limitation).
- **Data deletion**: a real hard-delete endpoint and confirm-before-
  delete UI control, closing a genuine gap against the spec's "deletion
  on request" privacy requirement.
- **Load and replay reports** (`docs/REPLAY_REPORT.md`): re-ran the real
  Milestone 4 backtest harness fresh — 18,504 events/sec, a full month
  of real data replayed ~20,900x faster than real time, far past the
  spec's 10-100x target.
- **Model card** (`docs/MODEL_CARD.md`), **runbook**
  (`docs/RUNBOOK.md`), **postmortem** (`docs/POSTMORTEM.md`, for a real
  incident hit during this milestone's own work), **demo script**
  (`docs/DEMO_SCRIPT.md`), and **write-up** (`WRITEUP.md`).

### A real incident, documented rather than quietly fixed

Freeing a port during container testing killed the host's Docker daemon
itself (not the intended process), stopping every running container.
No durable data was lost (TimescaleDB has a real volume; Redis/Kafka
deliberately don't). Full timeline and root cause in
`docs/POSTMORTEM.md` — written the same way every other real bug in this
project gets handled: documented, not hidden.

### Live verification

Built both Docker images (`docker build --platform linux/amd64`, the
real EC2 deploy target — `apache-flink` has no prebuilt `linux/arm64`
wheel, found live rather than assumed). Brought up all four app
containers alongside the real infra stack (`docker compose -p layover
-f infra/docker-compose.yml -f infra/docker-compose.prod.yml up -d`),
confirmed `/healthz` on both Python services, confirmed a real
cross-container flight-risk lookup returned actual model output (not a
stub), and signed in + reached the trips page through the containerized
Next.js app. Live-created and live-deleted a real trip through the
production delete endpoint, confirming via a direct TimescaleDB query
that the trip, its legs, and its risk-assessment history were all
actually gone, not just hidden from the UI. `terraform validate` clean
against the real (if unapplied) AWS provider.

### Progress notes

- Lint and all 95 Python unit tests still pass clean after every
  env-var/portability change. `npx tsc --noEmit`, `npm run lint`, and
  `npm run build` all clean after the new delete-trip route/component.
- Deferred nothing buildable in this milestone's own scope. The one
  genuine external blocker (a real AWS account to deploy against) was
  surfaced to the user before any work started, not discovered partway
  through and quietly worked around — the agreed resolution (write and
  validate the Terraform, don't apply it) is reflected everywhere this
  milestone is referenced, never silently upgraded to "deployed."
- The demo video itself is a script (`docs/DEMO_SCRIPT.md`), not a
  rendered file — recording needs real screen capture and voiceover,
  which this environment can't produce; named honestly rather than
  claimed as done.
