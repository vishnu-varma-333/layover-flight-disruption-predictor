# Design decisions

Each entry: the options considered, what was chosen, why, and what it cost.
Written as interview talking points, not a changelog.

## Milestone 1 — Foundations

### Local dev orchestration: Docker Compose, not local Kubernetes

**Options:** kind/minikube (matches production topology more closely) vs.
Docker Compose (lighter, faster iteration).

**Chose:** Docker Compose for local dev. Production targets AWS via
Terraform with EC2 or EKS (see Tech stack) — that's where the "real"
Kubernetes decision gets made, informed by actual load-test results from
later milestones.

**Why:** At this stage there's no workload yet to justify Kubernetes's
scheduling/networking overhead on a single dev machine. Compose gives
healthchecks, a single `docker compose up`, and per-service logs — enough
to prove the stack works end to end. The cost is that local dev doesn't
exercise k8s-specific failure modes (pod eviction, rolling restarts) — those
get tested against the real EKS deployment at the "Ship it" milestone, not
simulated locally.

**What it cost:** if a bug only reproduces under k8s scheduling behavior, it
won't surface until deploy. Mitigated by the Temporal workflow-kill tests
(restarting the actual services, not just containers) planned for the "Trip
monitoring and alerts" milestone.

### Kafka: Redpanda, not Apache Kafka, for local/dev

**Options:** real Apache Kafka (what some production deployments use) vs.
Redpanda (Kafka-API-compatible, single binary, no ZooKeeper/JVM).

**Chose:** Redpanda — this matches the spec's own tech-stack table
("Kafka (Redpanda locally)").

**Why:** Redpanda speaks the Kafka wire protocol, so every client library
(`confluent-kafka`, Flink's Kafka connector) works unmodified. It starts in
seconds, needs no JVM/ZooKeeper, and uses a fraction of the memory — material
on an 8GB Docker VM that will also need to run Flink and Postgres. Trade-off:
Redpanda isn't byte-for-byte identical to Kafka under extreme load (different
internal storage engine), so any production Kafka-specific tuning
(partition reassignment behavior, broker-level quotas) would need
re-validation against real Kafka before a non-demo production deploy.

### Redpanda: dual listener (internal vs. external) required — found via live test, not assumed

**What happened:** first pass configured a single advertised address,
`redpanda:9092`, because that's the only name other *containers* on the
Compose network need. Running the verify script from the host failed with
`Failed to resolve 'redpanda:9092'` — not a connection refusal, a DNS
failure, because Kafka's protocol is two-step: the client's initial
bootstrap connection succeeds against whatever port is mapped, but the
broker's metadata response then tells the client "actually, send your
produce/consume traffic to `<advertised address>`" — and `redpanda` only
resolves inside the Docker network's internal DNS, not from the host.

**Fix:** two named listeners — `internal://redpanda:9092` for other
containers on the `layover` network (Flink jobs, future ingestors), and
`external://localhost:19092` for anything running on the host (this verify
script now, and any service run locally outside Compose during development).
Both map to the same broker; it's purely an advertised-address split.

**Why this is worth recording:** it's the single most common
Kafka/Redpanda footgun in any mixed host+container dev setup, and it is
invisible from the config alone — it only shows up when something actually
tries to produce/consume from outside the container network. This is exactly
why Milestone 1 does live round-trip checks instead of just checking "is the
port open."

### OTel Collector: no container-level healthcheck — distroless image has no shell

**What happened:** first pass gave the `otel-collector` service a
`CMD-SHELL` healthcheck (`wget -q -O- http://localhost:13133/`). On a clean
`down -v` + `up` cycle it came back `unhealthy`, even though the
collector's own logs showed `Everything is ready. Begin running and
processing data.` Inspecting the container's healthcheck history showed the
real error: `exec: "/bin/sh": stat /bin/sh: no such file or directory`.
Checked directly — `docker exec` into the container for `sh`, `wget`,
`curl`, `busybox`, `which`: none exist. The `otel/opentelemetry-collector-
contrib` image ships only the collector binary itself; there is no shell to
run a `CMD-SHELL` healthcheck in, so the check was failing for a reason
that had nothing to do with whether the collector was healthy.

**Fix:** removed the container-level healthcheck entirely rather than
chase a workaround (e.g. installing a shell via a custom image, which adds
build/maintenance cost for no real benefit). Health is instead verified
from the host, where tools exist, by `scripts/verify_stack.py` making a
real HTTP request to the collector's `health_check` extension endpoint —
which is the check that actually matters (can anything *talk* to the
collector), not whether a shell can be execed inside it.

**Consequence:** `docker compose up --wait` now treats this service as
ready as soon as it's running (no healthcheck = running is ready), so a
genuinely crashed-but-still-running collector wouldn't be caught by
Compose's own wait — only by the live verification script actually hitting
its endpoint. This is consistent with the rest of Milestone 1's philosophy:
`docker compose ps` showing "healthy" was never trusted as sufficient on
its own; every service's real test is `verify_stack.py` making a real call
to it.

### TimescaleDB over plain PostgreSQL

**Chose:** `timescale/timescaledb:latest-pg16` — Postgres with the
TimescaleDB extension, per the spec's tech-stack table.

**Why:** aircraft position reports and airport weather are naturally
time-series (append-heavy, queried by time range, one table growing
indefinitely). Timescale's hypertables give automatic time-based
partitioning without hand-rolling partition management, while `Flight`,
`Trip`, `Alert` etc. stay ordinary relational tables in the *same* database
— no second datastore, no cross-database joins between relational and
time-series data. Verified live: `CREATE EXTENSION timescaledb` +
`create_hypertable()` both succeed against the running container, not just
assumed from documentation.

### Observability stood up before any application code

**Chose:** OpenTelemetry Collector + Prometheus + Grafana, wired together
(Collector exports a Prometheus-scrapeable endpoint, Prometheus scrapes it,
Grafana is pre-provisioned with Prometheus as a datasource) before a single
ingestor or service exists.

**Why:** every later milestone's "ends with something running and tested"
bar includes metrics (consumer lag, prediction latency, etc. — see Metrics
and benchmarks). Retrofitting observability after services exist always
costs more than building the pipe first and having each service emit into
an already-working destination. Cost: three extra containers at this stage
emit nothing of substance yet — the value is entirely in the plumbing being
proven to work (verified: Collector health endpoint, Prometheus target
scrape marked "up", Grafana's datasource provisioning actually present via
its API), not in any dashboard content.

### Python dependency management: stdlib `venv` + `pyproject.toml`, not Poetry/uv

**Chose:** plain `venv` + `pip install -e ".[dev]"` against a
`pyproject.toml`.

**Why:** no Homebrew is allowed on this machine for this project, and `uv`
isn't installed. The system Python 3.9.6 already ships `venv` and `pip`, so
this needed zero new tooling installed outside the project. Revisit if a
later milestone's dependency (e.g. PyFlink's Python version constraints)
needs something `venv` can't express — then a user-local Python install
(not Homebrew) becomes the fix, not a package-manager swap.

### Kafka client: `confluent-kafka`, not `kafka-python`

**Chose:** `confluent-kafka` (librdkafka bindings).

**Why:** it's the client Flink's Kafka connectors and most production Kafka
tooling standardize on, has prebuilt wheels for macOS (installed cleanly via
pip with no Homebrew-installed `librdkafka` needed), and its admin API
(`AdminClient`) was needed anyway to create the verify script's throwaway
topic. `kafka-python` is pure-Python and lighter, but diverging from the
client the rest of the stack will use would just mean re-learning two APIs.

## Milestone 2 — Historical data

### Scope: 12 months (Aug 2025 - Jul 2026), not "all years"

**Options:** download BTS's full history back to 1987 (richest possible
training signal) vs. a bounded recent window.

**Chose:** the most recent 12 full months BTS has published as of this
build (confirmed live: the latest available month is 2026-07, a ~3-month
publication lag). Verified programmatically by probing the BTS endpoint
month-by-month rather than assuming a lag length.

**Why:** 12 months covers a full seasonal cycle (winter storm delays,
summer thunderstorm delays) which matters more for a disruption predictor
than raw row count, while keeping first-pass download/processing bounded
on a single dev machine. TAIL_NUM reporting, carrier codes, and schedule
conventions have also all changed over decades of BTS history — a bounded
recent window avoids quietly training on data whose labeling conventions
drifted. This is explicitly called out as adjustable: if backtesting
(Milestone 4) shows the model needs more history or more seasonal cycles,
widening the window is a one-line change to `MONTHS` in
`scripts/fetch_historical_data.py`, not a rewrite.

**What it cost:** real download time. This sandbox's network throughput to
transtats.bts.gov measured at 20-60 KB/s (see docs/BENCHMARKS.md) — a
~27 MB monthly file took 10-20 minutes, not seconds. Downloads are resumable
(HTTP Range) and run as a detached background process specifically because
of this constraint, not for its own sake.

### Arrival time derived from departure + elapsed minutes, not parsed from an arrival HHMM field

**Options:** parse CRS_ARR_TIME/ARR_TIME (the local HHMM BTS reports for
arrival) and infer whether it rolled into the next calendar day by
comparing it against the departure HHMM, vs. deriving arrival UTC
arithmetically from departure UTC + CRS_ELAPSED_TIME/ACTUAL_ELAPSED_TIME.

**Chose:** elapsed-time arithmetic.

**Why:** BTS gives a flight's departure date (FL_DATE) but never an
explicit arrival date — a flight landing after midnight local time is
reported as some HHMM value with no day marker. Inferring the rollover by
comparing HHMM values breaks the moment origin and destination are in
different timezones with different UTC offsets (comparing "23:50" to
"02:10" tells you nothing about which one is later in absolute time without
also knowing both timezones and whether DST differs between them that day).
Elapsed-time arithmetic sidesteps the ambiguity entirely: departure UTC is
unambiguous (FL_DATE + CRS_DEP_TIME + origin timezone), and
`arrival_utc = departure_utc + elapsed_minutes` needs no date-rollover
inference at all. Diverted flights are the one case this breaks: their
`ACTUAL_ELAPSED_TIME`, when present, measures time to the diversion
airport, not to `DEST` — so `actual_arr_utc` is deliberately left null for
any diverted flight (see `clean.py`) rather than silently computing a wrong
arrival time at the wrong airport.

### BTS's "2400" convention handled explicitly, not as a parsing edge case found later

BTS encodes a schedule time of exactly midnight as `2400` on the *current*
`FL_DATE` (meaning 00:00 the *next* day), not `0000` same-day. The two are
one day apart. This is a well-known BTS quirk, but it's exactly the kind of
thing that passes silently in an `int(hhmm) // 100` parse and only surfaces
as a mysteriously-shifted rotation months later. Handled explicitly in
`parse_hhmm()` with its own unit test (`test_parse_hhmm_midnight_2400_rolls_to_next_day`)
rather than trusted to "probably not come up much."

### DST-ambiguous local times resolved via Python's `fold=0` default — documented, not silently inherited

Local-to-UTC conversion uses `zoneinfo` with the default `fold=0`, which
matters for exactly the ~1-2 hours/year a civil clock repeats during a
fall-back DST transition (e.g. 1:30 AM America/Chicago occurs twice on the
first Sunday in November). `fold=0` resolves to the earlier
(pre-transition) UTC offset for that ambiguous hour. This is a real,
bounded approximation — a handful of flights per year, per timezone, that
depart or arrive in that one repeated hour — not a correctness guarantee.
Pinned with a test that checks the function's output against Python's own
`zoneinfo` semantics directly (`test_local_to_utc_matches_stdlib_on_ambiguous_dst_fallback_time`),
so a future change to this behavior is a deliberate decision, not a silent
regression.

### Airport timezone from lat/lon via `timezonefinder`, not a timezone column or API

**Options:** a hosted timezone API (e.g. one keyed by lat/lon) vs.
`timezonefinder` (offline, shapefile-based, pure Python).

**Chose:** `timezonefinder`.

**Why:** OurAirports' reference CSV — the spec's own named source for
"airport codes, locations, timezones" — turns out to carry no timezone
column at all, only lat/lon (confirmed by downloading and inspecting the
real file, not assumed from its description). An API would add a network
dependency and rate limits for a lookup that only needs to run once per
airport (~2,000 US airports with IATA codes), ever, then gets cached in the
build — exactly the kind of call that doesn't justify a live dependency.
Verified against six known hub airports spanning four different US
timezones, including Alaska and Hawaii, which are the cases a
continental-US-only mental model gets wrong.

### Downloader bug: a gzip-encoded response's Content-Length is not the decoded byte count

**What happened:** the resumable downloader (`download_resumable`) checked
that bytes written to disk matched the server's `Content-Length` header,
to detect truncated downloads. The OurAirports CSV fetch failed every time
with "got 12,743,186 bytes, expected 3,894,647" — *more* bytes than
expected, which is the signature of this bug, not a truncation. The
GitHub Pages/Fastly CDN serving that file sends `Content-Encoding: gzip`
with `Content-Length: 3894647` describing the *compressed* wire size, but
`requests`/`urllib3` transparently decompresses the stream — so the
decompressed bytes written to disk (12.7 MB) will never match a
compressed-size header. Confirmed directly with a raw `curl -I` showing
both headers together before writing the fix.

**Fix:** when a response carries `Content-Encoding`, skip the
Content-Length equality check entirely (a truncated gzip stream reliably
raises its own decompression error partway through, which is a sufficient
completeness signal) and always re-fetch from byte 0 rather than attempting
a Range-resume — resuming a decompression mid-stream from a byte offset
into the *compressed* representation doesn't work without replaying
decoder state anyway. The BTS zip files carry no `Content-Encoding` (confirmed via headers), so
they keep the original strict-equality + Range-resume path, which matters
for them since they're 10x larger.

**Why this belongs here and not just in a code comment:** it's a trap any
HTTP client logic hits the first time it meets a CDN-gzipped resource
behind a completeness check — correct-looking code, wrong threat model for
what "Content-Length" measures.

### Real schema mismatch: BTS's actual column names weren't what was assumed from memory

**What happened:** `clean.py` was first written against remembered BTS
column names (`FL_DATE`, `OP_UNIQUE_CARRIER`, `TAIL_NUM`, `CRS_DEP_TIME`,
...) — a plausible-looking schema for "the BTS On-Time Performance
dataset," but wrong for *this specific* public download endpoint. Caught
before running against all 12 months by downloading one real file first
and printing its actual header: the real columns are `FlightDate`,
`Reporting_Airline`, `Tail_Number`, `CRSDepTime`, etc. — a different naming
convention (mixed case, inconsistent underscore use) than the SQL-style
assumption. Fixed by rewriting `USECOLS` and every field reference against
the real header, not by patching around a guess.

**Why this belongs here:** it's the single most important validation step
in this whole milestone — assumed-correct code that would have silently
produced an empty or malformed DataFrame (pandas's `usecols` raises if a
named column doesn't exist, so this specific mistake would have failed
loudly rather than silently — but a near-miss, like a column that exists
under both names in different dataset variants, might not have). The
broader lesson applied throughout this project: verify a real sample of
any external data source before writing logic against its assumed shape.

### US territories included as domestic; a rename gap recovered from OurAirports' `keywords` column

**What happened:** after wiring the real schema, `clean_month` logged 9
skipped rows for unresolved airport codes: `BQN, GUM, PBI, PPG, PSE, SJU,
SPN, STT, STX`. Investigated each individually rather than assuming one
root cause, and found two distinct, unrelated issues:

1. `BQN, GUM, PPG, PSE, SJU, SPN, STT, STX` are Puerto Rico, Guam, American
   Samoa, the Northern Mariana Islands and the US Virgin Islands — BTS's
   "US domestic" On-Time Performance data includes them as ordinary routes
   (confirmed: real flight rows with these as Origin/Dest), but OurAirports
   assigns each its own ISO 3166-1 country code, distinct from "US". The
   airport loader's `iso_country == "US"` filter was silently dropping
   every flight touching any of them.
2. `PBI` (Palm Beach International) resolved to nothing because
   OurAirports' *current* snapshot lists that airport under a new name and
   a new IATA code (`DJT`), with the old code preserved only in a free-text
   `keywords` column, not as `iata_code`. BTS's 2025 historical data still
   uses `PBI`, because that was the airport's code when those flights
   actually happened.

**Chose:**
1. Explicitly widen the country filter to `US_DOMESTIC_ISO_COUNTRIES = ["US", "PR", "GU", "AS", "MP", "VI"]`
   rather than quietly excluding territories BTS itself calls domestic.
2. Recover historical aliases by scanning `keywords` for 3-letter,
   all-caps tokens not already claimed by a primary `iata_code` row, and
   mapping them to that row's own coordinates (so a rename can never
   shadow a real, currently-assigned code — the "not already claimed"
   guard is what makes this safe).

**What it cost / known limitation:** the keywords-alias heuristic is a
targeted fix for the specific failure mode found (a stale code trapped in
free text), not a general historical-identifier resolution system. It
could in principle misattribute an unrelated 3-letter token that happens
to appear in another airport's keywords (checked by hand for this
dataset: no such collision occurs among codes BTS actually uses). A more
rigorous fix would use a dedicated IATA-code-history table; not worth
building for a handful of affected rows out of 600K+ per month. Result:
the 2025-08 recovery alone restored 5,867 flights that would otherwise
have been silently dropped (602,378 vs. 596,511 rows for the same month).

### Performance bug: rotation building looked correct on fixtures, hung for 20+ minutes at real scale

**What happened:** `build_rotations()` was first written as
`flights.groupby(["tail_number", "fl_date"]).groupby(...)` followed by a
Python-level loop calling `.to_dict("records")` per group and summing
chain breaks with a Python `for` loop over each group's legs. It passed
all unit tests instantly against small synthetic fixtures (a handful of
rows, a handful of groups) and looked done. Running the actual Milestone 2
pipeline against the real 12-month, ~7 million row dataset, the script
finished cleaning all 12 months in under 4 minutes, then appeared to hang
indefinitely at the rotation-building step — confirmed genuinely stuck
(not a logging/buffering illusion) by: running with unbuffered output
(`python -u`, no pipe), watching the process stay pinned near 100% CPU
with growing runtime, and killing it after 20+ minutes with still no
"total rotations" log line.

**Root cause:** ~7M flight rows with a tail number group into ~1.75M
(tail_number, date) rotations — the overwhelming majority 1-3 legs each.
pandas's `groupby().to_dict()`-per-group pattern pays substantial
per-group Python/pandas overhead (object creation, index bookkeeping) on
every single one of 1.75M groups; that overhead, trivial at fixture scale,
dominates completely at real scale. This is a known pandas anti-pattern
(a groupby-then-Python-loop instead of a vectorized groupby-aggregate),
but it is invisible until you actually run it against production-scale
data — exactly why this project's rule is "verify live against the real
thing," not "unit tests pass."

**Fix:** rewrote with no per-group Python loop at all: a single
`groupby(...).agg(flight_ids=("flight_id", list), n_legs=("flight_id", "size"))`
for the aggregation, and chain-break detection via a vectorized
within-group `.shift(1)` comparison (every row's origin compared to the
previous row's destination *in the same group*, computed for all rows at
once) summed per group with a second `groupby().sum()`. Both are standard
pandas vectorized idioms — no conceptual change to what's being computed,
only how.

**What it cost / measured improvement:** the original version was killed
after 20+ minutes with no completion in sight (effectively unusable). The
vectorized rewrite builds all 1,747,220 rotations from 7,043,858 flights in
**40.5-52.3 seconds** across repeated runs (see docs/BENCHMARKS.md for
both measurements and why they differ) — two to three orders of magnitude
faster either way. Unit tests
needed no changes — same output shape, same assertions, all still passing
— because the *contract* didn't change, only the implementation. Lesson
generalized to the rest of this project: any transformation whose unit
tests only exercise single-digit row counts gets a real-scale timing check
before being called done, not just a correctness check.

### Fourteen flights with scheduled arrival at/before scheduled departure — a BTS source-data defect, not a derivation bug

**What happened:** the live DuckDB verification (`verify_historical_data.py`)
failed one check against the real dataset: 14 of 7,043,858 flights had
`sched_arr_utc <= sched_dep_utc`. Investigated by querying the actual rows
rather than assuming the elapsed-time-arithmetic design (see the earlier
"Arrival time derived from departure + elapsed minutes" decision) was
wrong: all 14 had a negative *implied* elapsed time (-14 to -99 minutes),
and checking back against the source data confirmed `CRSElapsedTime`
itself was negative in BTS's own raw data for these rows (13 of 14 on
Southwest Airlines, no other common factor) — a small, known class of data
entry defect in the public dataset, not a timezone or arithmetic error on
this project's side.

**Chose:** treat a zero/negative `CRSElapsedTime` (and, symmetrically,
`ActualElapsedTime`) the same as a missing value — `sched_arr_utc` /
`actual_arr_utc` left `null` — rather than silently computing a
logically-impossible timestamp from a value already known to be wrong.
Logged with a row count (`clean.py`), not silently dropped: 14 rows
across 12 months is traceable and auditable, never just absent.

**Why this belongs here:** it's a good example of a bug that is *correct
to not fully explain away* — the honest fix isn't to guess the real
elapsed time, it's to say "this specific field is known-bad, don't trust
a value derived from it." The alternative (clamping to some minimum
elapsed time, or inferring from distance/aircraft type) would quietly
fabricate data the source never actually provided.

## Milestone 3 — Baseline and model

### Feature design grounded in a literature pass, not just intuition

Before writing `features.py`, searched for published work on reactionary/
delay-propagation prediction rather than designing the feature set from
first principles alone. Found arXiv:2605.07364 (verified real via a direct
fetch of the paper, not taken on faith from a search snippet — it's a very
recently submitted paper, so that verification mattered), which reports
11 delay-propagation features derived from tail-number rotation chains
(previous-flight arrival delay, turnaround time, a *tight-turnaround
indicator*, cumulative same-day aircraft delay) producing the dominant
performance gain in their study (AUC 0.732 -> 0.875, schedule-only ->
rotation-chain features). Two features this project's original design
pass hadn't included were added as a direct result:

- **`tight_turnaround`**: a binary flag for turnaround time under 45
  minutes (the paper's cited standard minimum practical narrow-body
  domestic turnaround under normal gate operations), not just the raw
  continuous `turnaround_minutes` value. A GBM *can* approximate a
  threshold split on its own, but an explicit indicator for a genuinely
  operationally-meaningful threshold is a known-useful practice when that
  threshold is domain knowledge, not something to hope the model
  rediscovers from scratch.
- **`cum_delay_today`**: the tail's running total of already-landed legs'
  delay so far that day (not just the immediately preceding leg) —
  captures that a day with several compounding small delays is a
  different risk profile than one bad leg, which a previous-leg-only
  feature can't see.

**Considered and deliberately deferred:** the same paper (and other
search results) describe graph-based airport-network-centrality features
(e.g. PageRank over the route network) and meta-models (a model whose
output feeds another model, e.g. predicting upstream flight duration as
an input feature). Not built for v1: a GBM already learns airport-specific
effects directly from categorical origin/dest splits, so centrality
features are largely redundant signal dressed up as novelty at this
project's scale; a meta-model adds real training/serving complexity
(versioning and keeping two models in sync) for a project whose actual
differentiators are the rotation-tracking data engineering and production
rigor, not feature-engineering novelty. Revisit only if Milestone 4's
backtest shows the categorical airport features are under-capturing
airport-level effects.

### Vectorized feature engineering, deliberately, informed by the Milestone 2 bug

Both the local-timezone conversion and the rotation/turnaround features
are written as vectorized groupby operations from the start (grouping by
the airport's IANA timezone -- only ~15 distinct zones across all US
airports -- for the time features, and by (tail_number, fl_date) with
`.shift()`/`.cumsum()` for the rotation features) rather than any
per-row or per-tiny-group Python loop. This is a direct, deliberate
application of the lesson from Milestone 2's rotation-building
performance bug (see above): confirmed on the real 7,043,858-row dataset,
building the full feature table (local time + all 8 rotation/turnaround
features) took **~23 seconds** — no repeat of that earlier mistake.

### Model objective: L1 (MAE), not LightGBM's L2 default — found by comparing results, not assumed

**What happened:** the schedule-only ablation model for arrival delay
(trained with LightGBM's default `regression` objective, which minimizes
squared error) scored a test MAE of 32.39 minutes — *worse* than simply
predicting the training set's median delay for every row (31.36 minutes).
That's a red flag precisely because the median is the single best
constant predictor *under MAE specifically* (it minimizes MAE by
definition) — a model with real, if weak, information should still beat
it.

**Root cause:** LightGBM's default L2 objective targets the conditional
*mean*; MAE is minimized by the conditional *median*. Flight delay is
heavily right-skewed (most flights near zero or early, a long tail of
large delays), so the mean sits well above the median, and a mean-
targeting model systematically overshoots on the large share of near-zero
flights — directly hurting MAE. The schedule-only model's features are
weak enough that this mismatch dominated its performance; the full
model's much stronger rotation signal was enough to overcome the same
mismatch, which is exactly why the bug stayed hidden until the ablation
comparison surfaced it.

**Fix:** switched every model to `objective: "regression_l1"` (LightGBM
trains directly against MAE). Re-running confirmed the diagnosis: the
schedule-only model's MAE improved to 29.82 (now correctly beating the
31.36 naive floor), and the full model's arrival-delay MAE also improved
(22.82 -> 21.41). The *qualitative* conclusion (rotation features help a
lot) was never in doubt, but the quantitative ablation margin was wrong
under L2 and only became a trustworthy, apples-to-apples number once both
models were evaluated under the same loss that matches the reported
metric.

**Why this belongs here:** it's a textbook objective/metric mismatch, the
kind of thing that's easy to state abstractly ("match your loss to your
metric") but easy to actually miss until a result looks wrong and gets
investigated rather than reported as-is.

### `tight_turnaround` dtype bug: NaN + Python bool silently becomes an unusable column

**What happened:** training crashed immediately with
`ValueError: pandas dtypes must be int, float or bool. Fields with bad
pandas dtypes: tight_turnaround: object` — on the real dataset, not in
any unit test (the test fixtures happened to always have at least one
non-missing value in a way that didn't trigger it reliably at small
scale).

**Root cause:** `tight_turnaround` was built with
`turnaround_minutes.where(turnaround_minutes.isna(), turnaround_minutes < 45)`
— keep NaN where missing, else a boolean comparison. Mixing a float NaN
with native Python `True`/`False` values in one pandas Series has no
clean single numpy dtype, so pandas silently falls back to `object`
dtype, which LightGBM flatly rejects.

**Fix:** build it as `(turnaround_minutes < 45).astype(float)` first (a
clean float64 column of 0.0/1.0), then `.where(turnaround_minutes.notna())`
to punch the NaNs back in — float NaN mixed with float 0.0/1.0 stays
float64 throughout, no object-dtype fallback.

### Baseline design: an ablation of the same model, plus a naive floor

Two baselines, not one: (1) the simplest possible floor — predict the
training set's median delay for every test row, a sanity check any real
model should clear; (2) a schedule-only LightGBM (carrier, origin, dest,
month, day-of-week, local departure hour — no rotation/turnaround
features), trained and evaluated identically to the full model. (2) is
what the spec's own benchmark table means by "compare against a simple
baseline that ignores the inbound aircraft" — using the *same algorithm*
with a *subset of features* isolates what the rotation signal specifically
contributes, rather than conflating it with a difference between model
families (e.g. linear regression vs. gradient boosting would confound
"simpler model" with "no rotation data").

### Three targets, not one — matching the spec's "updated as new events arrive"

Trained separately: departure delay (pre-departure features only),
arrival delay pre-departure (schedule + rotation/turnaround, no knowledge
of this flight's own actual departure yet), and arrival delay
post-departure (the same features plus this flight's own now-known
`dep_delay_min`). This mirrors how the system actually operates: before a
flight leaves the gate, only upstream/schedule signal exists; once it has
actually departed, its own departure delay sharpens the arrival estimate
considerably. Measured on the real holdout: pre-departure arrival-delay
MAE 21.41 minutes vs. post-departure 10.12 minutes — confirming this is a
real, substantial refinement, not a redundant extra model.

### Time-based holdout (last calendar month), not a random row split

Train = Aug 2025-Jun 2026 (11 months), test = Jul 2026 (the most recent
full month) — the same reasoning as Milestone 2's BTS-window decision,
applied to model evaluation: a random row split would let the model see
rows from the same days it's tested on (e.g. a different flight on the
same disrupted day), which leaks same-day conditions across the split in
a way a real deployed model, predicting forward in time, never gets. This
also previews exactly how Milestone 4's replay backtest will evaluate —
by date, not by row — so this milestone's numbers and Milestone 4's are
produced by a consistent methodology, not two different ones that happen
to both be called "accuracy."

### LightGBM over alternatives (gradient boosting vs. linear/deep models)

**Chose:** LightGBM, per the spec's own tech-stack table.

**Why:** tabular data with a mix of categorical (carrier/origin/dest) and
numeric (turnaround minutes, cumulative delay) features, strong
nonlinear interactions (e.g. a tight turnaround matters far more when the
inbound delay is already large — a multiplicative, not additive, effect)
is exactly gradient-boosted trees' strength over linear models. Over a
neural network: far less training data engineering (no embedding layers,
scaling, or architecture search needed for ~7M rows of mixed tabular
data), materially faster to train and iterate on a laptop, and — the
feature that matters most for this project's "explain every alert with
evidence" requirement — LightGBM exposes per-prediction feature
contributions directly (used later by the prediction service to answer
"why is this flight at risk," not just "how at risk"), which a black-box
deep model would need a separate explainability layer (SHAP, etc.) bolted
on to match.

### LightGBM on macOS without Homebrew: borrow scikit-learn's bundled `libomp`

**What happened:** `import lightgbm` crashed with `Library not loaded:
@rpath/libomp.dylib`, searching only Homebrew (`/opt/homebrew/opt/libomp`)
and MacPorts (`/opt/local/lib/libomp`) paths — neither of which exist,
and Homebrew is off-limits for this project.

**Chose:** point the dynamic linker at scikit-learn's own bundled
`libomp.dylib` (`sklearn/.dylibs/libomp.dylib` — confirmed ABI-compatible:
both report OpenMP compatibility version 5.0.0) via `DYLD_LIBRARY_PATH`,
rather than downloading or installing anything new. `make train-model`
resolves this path dynamically from the active venv's installed
scikit-learn rather than hardcoding a Python-version-specific path, so it
keeps working across a `venv` rebuild.

**Why:** this is precisely the "install tooling user-locally, not via
Homebrew" instruction applied to a dependency conflict rather than a
missing tool — the runtime library already exists on disk (pip installed
it as part of scikit-learn's wheel), it just wasn't where LightGBM's
hardcoded rpath looked for it. No new download, no modification to any
installed package's files, fully reversible (unset one environment
variable).

## Milestone 4 — Backtesting harness

### Research pass: point-in-time correctness and early-warning evaluation practice

Before designing the replay, searched for how streaming ML systems
actually handle backtesting and how early-warning systems are properly
evaluated, rather than inventing a methodology from scratch:

- **Point-in-time correctness**: a training/evaluation sample for an
  event at time t must only use feature values known at or before t;
  replay must advance chronologically and never let a later observation
  rewrite an earlier prediction snapshot. This directly shaped the
  event-driven design (below) rather than reusing Milestone 3's batch
  feature table for replay, which would have been leaky by construction
  (batch mode's `prev_arr_delay_min` always uses the eventual actual
  outcome, regardless of whether that outcome would genuinely be known
  yet relative to the current flight's own schedule).
- **Early Warning Recall / Mean-Time-to-Alert**: lead time must be
  reported *alongside* detection recall, never alone — a system can have
  an impressive median lead time while still missing most real events,
  which lead time by itself hides. This directly caught a real
  presentation trap in this project's own results (see below).

### Replay architecture: one Kafka topic, three event types, producer-sorted

Three event types per flight -- "schedule" (simulated 24h before
`sched_dep_utc`; BTS gives no real schedule-publish time, and the offset
doesn't affect the rotation-driven lead time this milestone measures,
since that's governed by the *inbound* aircraft's real "landed" event),
"departed" (this flight's own actual departure), "landed" (this flight's
own actual arrival — simultaneously the ground-truth label for this
flight and the rotation information the *next* flight on the same tail
needs). All three flow through a single Kafka topic/partition, with the
producer emitting them in sorted event-time order.

**Why single-partition, producer-sorted, not Flink-style watermarking:**
a live system (Milestone 6) must handle genuinely out-of-order,
multi-source arrival with real watermarks — that's explicitly Flink's job
later. This is a *backtest*, replaying known history: the producer
controls emission order because it already has the full timeline, so
single-partition sorted order is sufficient and correct here specifically.
Documented as a deliberate scope boundary, not a shortcut that got missed.

### Feature capture and model inference are deliberately decoupled, and `handle_event` never touches pandas

The scorer's state machine (per-tail rotation tracking, pending-flight
bookkeeping) runs in plain Python dicts; it returns *unscored* requests,
and a separate `score_requests` batches every accumulated request by
model before calling `predict()` once per model. This was not the first
design — seeing why it had to change is itself the finding (see Bugs 2-3
below).

### Bugs found during live verification (six, across two full-month run attempts)

**Bug 1 — Kafka producer `BufferError: Queue full`.** Producing ~1.86M
events in a tight loop without ever calling `producer.poll(0)` let
librdkafka's local outstanding-message queue (default cap 100,000) fill
up mid-run. Fixed by polling non-blockingly after every `produce()` call,
the standard confluent-kafka high-throughput pattern.

**Bug 2 — single-row `model.predict()` costs ~2.6ms purely from fixed
per-call overhead.** Profiled directly rather than assumed: 500 individual
single-row `predict()` calls took 1.32s total; one 500-row batched call
took 0.0057s — ~240x faster per row, confirming the cost is fixed
per-call overhead (thread dispatch, data validation), not actual
computation. At this project's event volumes (hundreds of thousands to
low millions of predict calls for a full month), scoring inline inside
`handle_event` meant a 2-day pilot alone took 15+ minutes with no
end in sight. Fixed by decoupling capture from inference (see above):
`handle_event` returns requests, `score_requests` batches them by model
at the end. Correctness is unaffected because a prediction's value never
feeds back into the state machine — only real ground-truth events do.

**Bug 3 — building even a single-row DataFrame costs ~2.4ms per call, the
same order of overhead as Bug 2, for the same underlying reason.**
Profiled directly: even after fixing Bug 2, a 2-day pilot still took 11+
minutes with climbing memory. Isolated the remaining cost to
`_feature_row`'s per-call DataFrame construction plus categorical/numeric
dtype coercion — pandas is built for one bulk vectorized operation, not
tens of thousands of tiny ones, a direct echo of Milestone 2's rotation-
building bug in a different spot. Fixed by having `handle_event` return a
*plain Python dict* of raw feature values (measured: ~0.0014ms/call, a
~1700x improvement) and moving all DataFrame construction and dtype
coercion into `score_requests`, run exactly once over every accumulated
request. Combined with Bug 2's fix, the 2-day pilot dropped from 11+
minutes (not finished) to 90 seconds.

**Bug 4 — `.values` on a timezone-aware column silently returns
timezone-*naive* numpy `datetime64`.** After fixing Bugs 2-3, the first
full pilot run crashed computing lead time:
`TypeError: Cannot subtract tz-naive and tz-aware datetime-like objects`.
Root cause: `score_requests`'s result construction used
`group["event_time"].values` to build the output DataFrame — a
well-known pandas gotcha where `.values` on tz-aware data strips the
timezone entirely, producing a naive column that can no longer be
compared against the (still tz-aware) ground-truth timestamps. Fixed by
building the result from the group's own tz-aware Series
(`group[["flight_id","event_time"]]`) rather than round-tripping through
raw numpy arrays.

**Bug 5 — the real dominant cost at full-month scale: an O(n) scan over
a silently-growing "pending" dict.** Even after Bugs 2-4 were fixed, the
first full-month attempt only consumed 400,404 of 1,859,448 events in
600 seconds and silently timed out (see Bug 6). Diagnosed analytically
before re-running: cancelled flights are added to `pending` at their
"schedule" event but never removed (no "departed" event ever fires for a
flight that never departed), so they leaked into the dict for the rest
of the replay — a few hundred by a 2-day pilot's end (harmless), but
thousands by a full month's end. The "landed" handler scanned *all* of
`pending` on every one of ~600K landed events, filtering for the matching
tail/date -- back-of-envelope, ~600,000 events x an average few thousand
leaked entries x per-comparison overhead lands squarely in the observed
slowdown. Fixed two ways: cancelled flights are never added to `pending`
at all (added "cancelled" to the event schema so the scorer can check at
schedule-event time), and `pending` is now keyed by `(tail_number,
fl_date)` so a landed event's re-score lookup is O(flights on that tail
that day) — typically 0-2 — never O(every pending flight in the replay).
Measured result: full-month consumer throughput went from 667 events/sec
(timing out) to **14,338 events/sec** — a ~21x improvement — finishing in
130 seconds.

**Bug 6 — an incomplete consume was reported as if it were a final
result.** The original code logged a `WARNING` when the consumer hit its
timeout short of `n_events`, then proceeded to compute and print "final"
metrics anyway, using ground truth from the *full* month but predictions
from only the ~21.5% of events actually processed. This made recall look
catastrophically wrong (13.2%) in a way that read as a genuine finding
rather than a broken run — until the MAE cross-check's `n=145,623` (not
the expected 612,038) gave it away. Fixed by raising a hard `SystemExit`
on an incomplete consume instead of a warning: a partial replay must
never silently produce a number that looks like a complete one.

### A result that needed its own caveat, caught by the research pass above

`schedule_only`'s "lead time" (median 1,440 minutes = exactly 24 hours,
every time) is a simulation artifact, not a real signal: that model is
scored exactly once, at its single simulated "schedule known" event, so
*any* detection it makes trivially shows the full 24h offset as its lead
time. Reported side-by-side with `rotation`'s lead time without comment,
this would misleadingly suggest schedule-only has a *longer* lead time
than the rotation model. The number that actually matters is recall:
schedule-only catches 11.0% of real 15+-minute delays; the rotation model
catches 66.0% overall and 74.9% of flights that have any inbound aircraft
to track at all. This is precisely the "lead time without recall" pitfall
the early-warning-evaluation research (above) warned about, applied to
this project's own numbers before they were reported as final.

### Segmented, not just averaged: first-leg-of-day vs. later legs

Reported recall and lead time separately for flights with no possible
rotation signal (`leg_index == 0`, the first flight of the tail's day —
5.9% recall, nothing to track) vs. flights with a trackable inbound
aircraft (`leg_index > 0` — 74.9% recall, 232-minute median lead time).
Averaging these together would understate the system's real value where
it actually applies, and overstate it where it structurally can't (a
first leg has no inbound aircraft, by definition, no matter how good the
model is). Honest segmentation beats a single flattering-but-misleading
average.

## Milestone 5 — Live ingestion

### Verified real API access before designing anything, not assumed from memory

OpenSky's REST API changed its auth model since this project's spec was
likely last checked against documentation — it now exclusively uses
OAuth2 client-credentials, not basic auth. Fetched the current docs
directly rather than relying on memory, and found anonymous access is
still available (400 credits/day on `/states/all`, 10-second position
resolution) — matching the spec's own "free for research and
non-commercial use with a daily credit limit" framing exactly, so no
external blocker (a paid key or registered account) actually exists here.
Confirmed NOAA's Aviation Weather API and the FAA's NAS Status feed are
both fully public and keyless by making real live requests, not reading
documentation alone (`aviationweather.gov/api/data/{metar,taf}`,
`nasstatus.faa.gov/api/airport-status-information` — both returned real
current data for real airports on the first try).

### OpenSky's real credit cost measured empirically, not assumed from docs

Made two real, small-bounding-box calls back to back and compared the
`X-Rate-Limit-Remaining` response header before and after: it dropped by
exactly 1 each time. A small per-airport bounding box (~0.26° half-width)
costs **1 credit per call**, confirmed live rather than trusted from
OpenSky's own documentation (which describes a grid-based cost model that
could plausibly charge more for a given area without testing it
directly). This number drove the actual budgeting design: 15 monitored
airports × 1 credit, once per hour, costs 360 of the 400 daily credits,
leaving a 40-credit reserve — not a number picked arbitrarily.

### Credit-aware scheduling: read the real remaining count from the API, never estimate it locally

`CreditBudget` tracks `remaining` directly from each response's
`X-Rate-Limit-Remaining` header — it has no independent counter of its
own that could drift from what OpenSky actually thinks the account has
used. Polling stops for the rest of the day once `remaining` drops to the
configured reserve (default 40), rather than spending the account to zero
and risking a lockout before the next UTC reset. This is the spec's own
named design topic ("Credit budgeting: polling a rate-limited API
intelligently") applied directly, not left as a vague future concern.

### One Kafka topic, an event_type discriminator — same pattern as Milestone 4, for the same reason

All three ingestors (aircraft positions, airport weather, airport status)
publish to a single `live.events` topic rather than three separate
topics. This matches the spec's own description ("normalized into one
event stream") and reuses Milestone 4's backtest replay topic design:
Milestone 6's Flink job subscribes to one place for every signal it
needs, rather than joining across several topics itself.

### Monitored-airport list hardcoded, not read from the Milestone 2 data file at ingestor startup

The 15 demo airports (major US hubs) are a fixed Python list in
`layover_ingest/airports.py`, with coordinates sourced from the same
OurAirports reference data Milestone 2 downloaded — but not read from
that file at runtime. That file is gitignored, multi-hundred-MB, and has
no reason to exist on a machine that's only running live ingestion (e.g.
a future production ingestor deployment) rather than the full historical
pipeline. A fixed list for a fixed, rarely-changing set of major hubs is
a reasonable simplification; a dynamic, growing monitored-airport set (if
trip-linked polling is added later, per the spec's "poll only regions tied
to monitored trips" framing) would need a real lookup service instead —
noted as a natural extension point when Milestones 8-9 introduce Trips.

### Async per the spec's own tech-stack table, not a default choice

Used `asyncio` + `aiohttp` for all three ingestors, matching the spec's
tech-stack table verbatim ("Ingestors: Python (async)"). The three
sources have genuinely different access patterns (OpenSky: many small
per-airport credit-metered calls; weather: one call covers every airport;
FAA: one global call, filtered client-side) and run fully concurrently via
`asyncio.gather`, each on its own independent interval, sharing one Kafka
producer.

### Bug: a bounded `--duration` test run didn't actually stop on time

**What happened:** a verification run with `--duration 90` was still
running several minutes later. Root cause: each ingestor's sleep between
cycles used only its own cycle interval (`interval_seconds - elapsed`),
with no awareness of the overall run's duration budget. OpenSky's default
interval is 3600 seconds, so after its one real cycle (which happens
immediately, duration-budget or not), it would sleep for ~3,593 seconds
before ever re-checking whether the 90-second budget had elapsed —
meaning a "bounded" test run would actually run for up to an hour before
naturally exiting on its own.

**Fix:** extracted the sleep logic into one shared
`sleep_until_next_cycle(cycle_start, interval_seconds, deadline)` used by
all three ingestors, which caps the sleep at whatever time remains until
the overall deadline (when one is set) — not just the cycle interval.
Verified live: a `--duration 60` run with default intervals now exits in
61 seconds, not "whenever the longest configured interval happens to
elapse."

### Live-verified, not CI-automated

`scripts/verify_live_ingestion.py` consumes real events from `live.events`
for a bounded window and asserts real schema/plausibility properties
(lat/lon in range, a non-empty raw METAR/TAF string, status events only
for monitored airports) — run manually (`make live-ingest` +
`make verify-live`), not added to the GitHub Actions CI pipeline. Unlike
Milestones 1, 2 and 4's CI jobs (which exercise this project's *own*
local infrastructure — Redpanda, Postgres — safe and free to run on every
push), these ingestors call real third-party APIs with a real, shared,
metered daily credit budget. Running them on every CI push would burn
through OpenSky's anonymous quota on an unpredictable schedule and make
test runs depend on three external services' uptime — a deliberate scope
boundary, not an oversight. CI still runs the offline, fixture-based unit
tests for all the parsing and scheduling logic on every push.

## Milestone 6 — Stream processing

### PyFlink environment: a real, non-trivial, user-local install (no Homebrew)

PyFlink needs a real JVM (Java 17+ for Flink 2.x — Java 11 support was
dropped) and a separately-downloaded Kafka connector JAR (the DataStream
API's Python bindings don't bundle one). All installed user-locally, no
Homebrew, per standing project policy:

- **Java**: Eclipse Temurin 17.0.20.1, downloaded directly from Adoptium's
  GitHub releases (checksum-verified) to `~/.local/jdk` — not a system
  install, not Homebrew's `openjdk` formula.
- **PyFlink**: `apache-flink==2.2.1`, chosen (not the newest 2.3.0) because
  Maven Central had a confirmed-matching Kafka connector build tagged
  `-2.2` for it; 2.3.0 had no matching connector release yet at build
  time. Checked directly on Maven Central rather than assumed.
- **Kafka connector**: `flink-sql-connector-kafka-5.0.0-2.2.jar`,
  downloaded directly from Maven Central to `~/.local/flink-connectors`
  and loaded via `env.add_jars()` — this is the standard Flink pattern for
  Python DataStream jobs that need a connector not on PyPI.

### Dependency resolution: two real pip conflicts, fixed at the root cause

**`pkg_resources` missing during `apache-beam`'s build** (a transitive
dependency of `apache-flink`): pip's isolated build environment installs
the latest `setuptools`, which no longer bundles `pkg_resources` by
default, and `apache-beam`'s legacy `setup.py` imports it directly at
build time with no explicit version constraint declared. Fixed by pinning
`setuptools==75.6.0` (last version confirmed to still include it) in the
project's own venv and installing with `--no-build-isolation`, so the
build step reuses that pinned setuptools instead of fetching a fresh one.

**`pyarrow` version conflict**: `apache-beam` 2.54-2.61 all require
`pyarrow<17.0.0`, but this project had pinned exactly `pyarrow==17.0.0`
since Milestone 2. Relaxed to `pyarrow==16.1.0` — satisfies every
dependent package simultaneously (`mlflow<18`, `apache-flink<21`,
`apache-beam<17`) — and confirmed the entire existing test suite (53
tests spanning Milestones 1-5) still passes unchanged after the
downgrade, since nothing in this project depends on anything pyarrow
added between 16.1 and 17.0.

### Event-time correctness via watermarks + last-write-wins, not full timer-based reordering

A `KeyedProcessFunction.process_element` in Flink is invoked in *arrival*
order per key, not event-time order — Flink's fully idiomatic answer to
this (buffering each key's events in state and firing via registered
event-time timers once the watermark passes) was considered and
deliberately not built for v1: it's meaningfully more complex, and this
job's actual correctness need is narrower than "process everything in
strict global order." Instead:

- **Late** events (event time more than `ALLOWED_LATENESS_SECONDS` behind
  the current watermark) are routed to a side output via Flink's own
  watermark mechanism — genuinely dropped from normal processing, not
  silently absorbed.
- **Ordinary out-of-order jitter** (within the bounded-out-of-orderness
  window) is handled by a last-event-time-wins merge specifically on
  `tail_state` updates (`merge_landed_event` in `rotation_state.py`): an
  older "landed" event arriving after a newer one has already been
  applied is a no-op, not a stale overwrite.
- **Duplicates** are handled separately and explicitly via a seen-keys
  `MapState` keyed by `(flight_id, event_type)` — dropped before they can
  touch state at all, not relied on being naturally idempotent.

Documented as a real trade-off, not hidden: this doesn't guarantee strict
global event-time ordering the way timer-based buffering would, but it
correctly handles every category of messiness this milestone actually
tests (confirmed live — see results below), with substantially less
implementation complexity.

### Messiness injection: four independently-controlled categories, matching the spec's own words

`layover_stream/messy_events.py` reuses Milestone 4's clean event
generator as the source of truth for *what* events exist, and separately
controls delivery order/completeness: **out-of-order** (small local
shuffle windows — ordinary network/multi-partition jitter), **late**
(a few events deliberately displaced to the very end of the produce
sequence — guaranteed to exceed any reasonable watermark lateness bound),
**duplicate** (exact re-sends a few positions later — an at-least-once
producer retry), **missing** (dropped entirely, never sent). Matches the
spec's own phrase verbatim ("late, out-of-order, duplicate and missing
messages") rather than one blanket "shuffle everything" knob, so each
category's handling can be verified independently.

### Bugs found during live verification (five, all real, all fixed)

**Bug 1 — `max(1, ...)` floor forced a "very late" event even at a 0%
rate.** `inject_messiness`'s very-late count used
`max(1, int(n * very_late_rate))` intending "at least one, if the rate is
positive" but applying the floor unconditionally — a caller explicitly
asking for zero late events still got one. Caught by a unit test
(`test_zero_rates_preserves_order_and_content`) before it ever reached
live data. Fixed by gating the floor on `very_late_rate > 0`.

**Bug 2 — ground truth and the Flink job compared different `event_time`
representations.** The reference implementation (`compute_ground_truth`)
initially ran on the clean stream's own pandas `Timestamp` objects, while
the real Flink job only ever sees JSON-serialized floats (Unix seconds)
after a Kafka round-trip. `rotation_state.build_feature_update` guards
`turnaround_minutes` with `isinstance(event_time, (int, float))` — so
ground truth would have silently computed `None` for every single flight
while Flink computed real values, making every comparison involving
turnaround time look like a mismatch regardless of whether the job was
actually correct. Fixed by converting to epoch seconds up front in
`compute_ground_truth`, before any pure-function logic runs — the same
representation the real job uses, by construction, not by coincidence.

**Bug 3 — `KafkaSource`/`KafkaSink` serializer API misuse.**
`set_value_only_deserializer(Types.STRING())` and
`set_value_serialization_schema(Types.STRING())` both crashed with
`AttributeError: 'BasicTypeInfo' object has no attribute
'_j_deserialization_schema'` — these methods need an actual
`DeserializationSchema`/`SerializationSchema` instance
(`SimpleStringSchema()`), not a `TypeInformation` descriptor. An earlier,
separate Kafka-connectivity smoke test (run before writing the real job)
had already used `SimpleStringSchema()` correctly for the source side,
which made this an inconsistency to catch, not a fresh unknown.

**Bug 4 — PyFlink's Python API has no `ctx.output()` for side outputs.**
The first real late event to reach that code path crashed with
`AttributeError: 'InternalKeyedProcessFunctionContext' object has no
attribute 'output'`. Unlike Java Flink, PyFlink's Python `ProcessFunction`
API emits to a side output by yielding a `(OutputTag, value)` tuple from
`process_element` itself, not via a context method — confirmed against
Flink's own documented Python example before fixing, not guessed.

**Bug 5 — the big one: binding a watermark strategy via
`from_source(source, watermark_strategy, ...)` doesn't propagate the
assigned event-time to `ctx.timestamp()` for a Python `KeyedProcessFunction`
reading from a `KafkaSource`.** Diagnosed, not assumed: added temporary
instrumentation printing `ctx.timestamp()` against the watermark on every
call, and found every single processed event showed `ctx.timestamp()`
equal to approximately *wall-clock "now"* (the moment the job was run) —
not the July 2026 flight date actually encoded in each message's
`event_time` field (independently confirmed correct by reading the raw
Kafka messages directly). This made the watermark track "now minus the
bounded-out-of-orderness," so no amount of event-time displacement in the
*data* could ever look late relative to it — the late-detection path was
silently dead code despite looking correct. Isolated with a minimal 3-line
repro: a trivial `KeyedProcessFunction` printing `ctx.timestamp()` against
a single known-old timestamp, fed via `env.from_collection(...)` +
`.assign_timestamps_and_watermarks(...)` *called explicitly on the
resulting stream* — which worked correctly, pointing squarely at
`from_source`'s own built-in watermark-binding path for a `KafkaSource`
specifically. Fixed by never relying on it: attach `WatermarkStrategy.no_watermarks()`
inside `from_source()` and bind the real strategy via an explicit, separate
`.assign_timestamps_and_watermarks()` call on the resulting stream instead.
Confirmed fixed by the same diagnostic instrumentation, which then showed
watermarks correctly spanning the real flight day and one event with a
genuine 45-hour gap being correctly routed to the late side output.

### Final live results (2026-07-15, 20,055 real flights, 61,033 messy events)

- **302 late events** correctly routed to the side output (the messy
  stream deliberately displaced ~300 events to trigger this — an almost
  exact match, confirming the mechanism isn't over- or under-triggering).
- **95.8% of flights present in both ground truth and Flink's output
  match on every feature field** (18,922 of 19,746). The remaining 4.2%
  is the expected, legitimate consequence of `missing_rate` and
  `very_late_rate` genuinely discarding information this reference
  implementation still has access to — not a correctness bug in the job.
- 309 flights (1.5%) have no Flink output at all — consistent with
  `missing_rate=0.01` occasionally dropping a flight's only recorded
  event entirely.
- End-to-end bounded run: **85 seconds** for 61,033 events through the
  real JVM, connector, and Redpanda stack.

### Not part of automated CI — same reasoning as Milestone 5's live ingestors

PyFlink's setup (a downloaded JDK, a downloaded connector JAR, specific
environment variables) is heavier than this project's other CI jobs and
not something to provision fresh on every GitHub Actions run. Unlike
Milestone 5's ingestors, this isn't about a shared external rate limit —
it's that reproducing this exact local-tooling setup in CI would need its
own dedicated provisioning step disproportionate to this milestone's
goal. CI continues to run the offline, fixture-based unit tests for
`messy_events.py` and `rotation_state.py` (15 tests) on every push; the
live Flink+Kafka verification is a deliberate manual step
(`make rotation-produce && make rotation-job && make rotation-verify`),
documented in the README.

## Milestone 7 — Prediction service

### Research pass before writing the service: tracing the feature schema end to end

Per the standing instruction to research feature-engineering-adjacent work
properly before implementing, the first step wasn't writing the FastAPI
app -- it was tracing, field by field, whether `stream.flight_features`
(Milestone 6's Flink output) actually carries everything
`layover_model.features.SCHEDULE_FEATURES` + `ROTATION_FEATURES` (what
the Milestone 3 models were trained on) requires. It didn't: the Flink
job only ever emitted the narrow rotation-state subset
`rotation_state.build_feature_update` computes
(`has_inbound`/`prev_arr_delay_min`/`prev_dep_delay_min`/
`turnaround_minutes`/`tight_turnaround`/`cum_delay_today`), missing
`leg_index`, `chain_break`, and every schedule feature (carrier, origin,
dest, month, day_of_week, dep_hour_local) entirely. A prediction service
can't serve a model from features it was never given, so this had to be
fixed before anything else in this milestone could be built on solid
ground.

That tracing exercise led to re-deriving Milestone 6's ground truth from
`layover_backtest.scorer.BacktestScorer` (the Milestone 4 reference
implementation that already reproduces training's exact feature schema)
instead of Milestone 6's own narrower, ad hoc reimplementation --
reasoning that re-using an independently-validated reference is stronger
evidence than extending the same function that's also under test. That
substitution is what actually surfaced the three real bugs below: the
old ground truth was *self-consistently* comparing the Flink job against
its own (also wrong, in one case) assumptions, which is exactly how a
mismatch this size went unnoticed through Milestone 6's "95.8% match."

### Three real bugs found by that substitution, none visible from the headline metrics alone

1. **Missing fields (the gap above).** Fixed by having `rotation_job.py`
   assemble the complete feature record itself (`_full_feature_update`):
   `leg_index`/`chain_break` threaded through from the already-tracked
   per-leg state, and the schedule-side fields computed from the raw
   event's own carrier/origin/dest/sched_dep_utc plus the same
   `airport_tz_map`-based local-time derivation every other script
   already uses (loaded once in `open()`, not per event).

2. **`turnaround_minutes` measured from the wrong timestamp.**
   `rotation_state.build_feature_update` measured turnaround from the
   *triggering event's own* `event_time`, not the flight's scheduled
   departure. That's only correct by accident: a "schedule" event fires
   24h before its own flight's departure, so turnaround measured from it
   was off by ~24h; a "departed"/"landed" recompute was off by this
   flight's own delay or flight duration. Training's real definition
   (`layover_model.features.rotation_features`) is always
   `sched_dep_utc - prev_actual_arr_utc`, independent of which event
   triggers the recompute. Fixed by giving `build_feature_update` an
   explicit `sched_dep_epoch` parameter instead of overloading
   `event_time` for two different purposes. Masked in Milestone 6
   because its ground truth used the same (wrong) reference point --
   caught only once BacktestScorer's *correct* definition was used as
   the comparison target.

3. **`BacktestScorer._feature_dict` re-read the live `leg_index` counter
   instead of the frozen value `handle_event` already attached to the
   flight.** Since that counter keeps advancing as a tail's *other* legs
   get their own schedule events later the same day, every
   departed/landed-triggered re-score (and even the schedule-time call
   itself, which runs after the counter's already bumped for next time)
   read a larger, wrong value. This is a Milestone 4 bug, not a
   Milestone 6 or 7 one -- it's been live since the real backtest that
   produced this project's first headline recall/lead-time numbers, and
   stayed invisible because `leg_index` is a weak, mostly-redundant
   auxiliary signal (`has_inbound`, `prev_arr_delay_min`,
   `turnaround_minutes` already carry the real rotation signal), not
   something that shows up in aggregate MAE/recall unless checked field
   by field. Fixed by reading `flight["leg_index"]` directly. A
   regression test (`test_leg_index_stays_frozen_at_schedule_time_through_later_legs`)
   locks the frozen-value semantics in.

A fourth, purely operational bug surfaced during re-verification: neither
`rotation_job.py` nor `produce_flight_events.py` ever reset the *output*
topics (`stream.flight_features`, `stream.late_events`) between runs, so
repeated verification runs kept accumulating messages in them --
inflating counts and occasionally resolving "last update per flight" to
a stale prior run. Fixed by having the bounded (verification) path
delete and recreate both output topics at job start, the same way
`produce_flight_events.py` already resets the input topic.

### Milestone 4 and 6 numbers, re-measured after the fixes

The leg_index bug fed the already-trained models a subtly wrong value at
serving time throughout Milestone 4's real backtest. Re-running it after
the fix (same real July 2026 month, 631,970 flights, identical trained
models) changed the headline numbers modestly, not dramatically --
consistent with leg_index being a weak secondary signal, not the
rotation model's main driver:

| Metric | Before fix | After fix |
| --- | --- | --- |
| Detection recall, overall | 66.0% | 64.9% |
| Median lead time, detected only | 232.0 min | 219.0 min |
| Recall, later legs (rotation signal available) | 74.9% | 73.9% |
| Recall, first leg of day (no rotation signal possible) | 5.9% | 3.9% |
| MAE, rotation model | 22.09 min | 22.07 min |

`docs/BENCHMARKS.md` and `docs/MILESTONES.md` are updated to the
after-fix numbers, with this entry as the record of why they changed.
Milestone 6's own correctness check, re-run with the complete 14-field
schema (not just the original 6) and the fixes above, moved from 95.8%
match on a narrower, partly-wrong comparison to **92.7% match on the
full, correct schema** -- a lower headline number than before, but a
more trustworthy one, and the right one to build Milestone 7 on.

### Online feature store: a dedicated Redis-syncing consumer, not a Flink sink

The spec names Redis as the "online feature state" layer ("latest
aircraft and airport state for low-latency predictions"). Two ways to
get Milestone 6's Flink output into it: have the Flink job write to
Redis directly (a custom sink inside `rotation_job.py`), or add a small,
separate Kafka consumer that reads `stream.flight_features` and upserts
into Redis. Chose the separate consumer (`scripts/sync_features_to_redis.py`):

- The Flink job is already the most complex, hardest-to-debug component
  in this project (watermarks, keyed state, a Python UDF subprocess,
  JVM/connector setup). Adding a stateful Redis client inside it couples
  two different failure domains (stream processing correctness and
  online-store availability) for no benefit -- a Redis outage shouldn't
  risk the rotation-linking job's own checkpointing, and a plain
  consumer can be restarted or scaled independently.
- Kafka already gives this exactly the delivery guarantee it needs:
  `stream.flight_features` is the durable log; a consumer group offset
  is how the sync step tracks what it's applied, the same pattern this
  project already uses everywhere else (the backtest scorer, the
  rotation job itself).

A fixed TTL (48h) on every Redis key, not a separate cleanup job: a
flight's online features stop mattering long before that, so an unused
key disappearing on its own is simpler than tracking deletion.

### Model selection: dep_delay_min's presence, not a separate model-version field

The online feature record doesn't carry an explicit "which model should
score this" flag -- the service infers it the same way
`layover_backtest.scorer.MODEL_FEATURE_COLS` already implies it: a
record with a known `dep_delay_min` came from (or was re-scored after)
this flight's own "departed" event, so the post-departure model (which
was trained with that as an extra feature) is strictly more informed
and should be used; otherwise the pre-departure rotation model serves
it. This mirrors Milestone 3's own three-model design ("updated as new
events arrive") without introducing a new field.

### Explanations: real LightGBM `pred_contrib`, not a feature-importance heatmap or an LLM

The tech-stack table's own stated reason for choosing LightGBM is
"interpretable feature contributions for explanations" -- `pred_contrib`
gives an exact, TreeSHAP-consistent decomposition of *this specific
prediction* into each input feature's contribution, which is what makes
an explanation like "inbound aircraft landed 107 min late" true of this
flight specifically, not a generic "this feature matters on average"
statement (a global feature-importance ranking would say the same thing
for every flight and miss the point). Deliberately not routed through
an LLM to turn numbers into prose: the sentence templates in
`explain.py` are a direct, auditable mapping from a real number to a
real sentence, with no risk of a model inventing a plausible-sounding
but wrong reason -- appropriate for a safety-adjacent "why should I
trust this delay prediction" answer.

Sentences are ranked by each prediction's own contribution magnitude
(largest first, top 3), not a fixed priority order -- the same flight's
dominant evidence changes by stage (its own departure delay dominates
once it's departed; the inbound aircraft's delay and turnaround dominate
before that), which a fixed order would misrepresent.

### `p_miss` left null, not fabricated -- the same sequencing gap Milestone 4 already named

`RiskAssessment.p_miss` only means something relative to a connection's
minimum connection time, which lives on `TripLeg` -- an entity the spec
itself doesn't introduce until Milestone 8 (Trip monitoring and alerts).
Milestone 4 already hit this exact gap ("connection-risk probability...
needs a Trip/TripLeg entity that doesn't exist yet") and named it a
sequencing dependency, not a dropped requirement; this service follows
the same precedent rather than inventing a placeholder probability that
would look real but mean nothing.

### Bugs found live, building and verifying the actual service

1. **`prepare_frame` assumed every request dict always carries a
   `dep_delay_min` key.** True for `BacktestScorer`'s own feature dicts
   (always present, possibly `None`), but the Flink job only ever sets
   it on the "departed"-stage record -- a real flight's "schedule"- or
   "landed"-stage online feature record simply doesn't have the key at
   all. `df[NUMERIC_COLS]` raised `KeyError` the first time a
   pre-departure flight was queried. Fixed in the service (and the
   verification script's cross-check) by filling the key in explicitly
   before calling `prepare_frame`, rather than changing the Flink job's
   own output schema for one consumer's column-selection requirement.
2. **A negative `turnaround_minutes` read as a reassuring "tight
   turnaround."** A genuinely very-late inbound aircraft can push this
   value negative (its actual landing is already past this flight's own
   scheduled departure) -- training's `tight_turnaround = (turnaround <
   45 min)` is still technically `True` there, but "a tight turnaround"
   undersells what's actually an overdue aircraft, and the generic
   minutes formatter's `abs()` hid the sign entirely (e.g. "turnaround
   is 463 min -- a tight turnaround" instead of "already overdue by
   463 min"). Found by deliberately looking at a real flight with a
   large inbound delay, not assumed. Fixed with an explicit negative-
   value branch in `explain.py`'s turnaround explainer.

### Real numbers (2026-07-15 demo day, 19,746 flights synced to Redis)

- Feature sync: all 77,380 Flink feature updates consumed and upserted
  in a single 20-second bounded run, converging to 19,746 distinct keys
  (matching the Flink job's own output -- see Milestone 6).
- Prediction endpoint latency, 200 real flights after a warm-up request
  (Redis lookup + feature assembly + model inference +
  `pred_contrib`, end to end): **p50 9.5ms, p95 13.9ms, p99 19.2ms**
  -- comfortably inside the spec's "p95 under a few seconds"
  end-to-end target, which also has to cover ingestion and stream
  processing upstream of this service.
- Live cross-check: a flight's prediction re-derived independently (same
  Redis record, same MLflow model, no HTTP layer) matched the service's
  own HTTP response exactly.

## Milestone 8 — Trip monitoring and alerts

### Research pass: Temporal's actual guarantees, not assumed ones

Before any workflow code, a research pass answered five concrete
questions: how to run a real (not single-process dev-server) Temporal
server locally without Homebrew; which `temporalio` version this
project's Python 3.9 can actually install; the correct
workflow/activity/signal/query shapes; what "exactly-once" really means
in Temporal (it doesn't exist -- Temporal's own guarantee is
at-least-once Activity execution, full stop); and what a Workflow ID
actually guarantees. Two results changed the design directly:

- **`temporalio==1.18.1` is the newest SDK release that still supports
  Python 3.9** (1.19.0+ requires 3.10). Pinned deliberately rather than
  bumping this project's Python version this late, to avoid
  re-validating the whole PyFlink/lightgbm/mlflow stack (Milestone 6's
  environment took real, nontrivial effort to get working) for one new
  milestone's SDK currency. A real, named tradeoff, not an oversight.
- **Temporal's own retries are at-least-once, never exactly-once.**
  "Activity ID" and retry bookkeeping are about which attempt matched
  which completion, not about deduping a downstream side effect (an
  email, a push notification). The actual exactly-once guarantee has to
  be application-level: reserve a dedup key in a real database *before*
  sending, atomically, so a retried or replayed attempt that reaches the
  same logical decision reliably finds the key already taken. This is
  exactly what the spec's own `Alert.dedup_key` field is for --
  confirmed, not assumed, this is the right design before
  `activities.py`/`db.py` were written.

### Infra: a real self-hosted Temporal server, sharing this project's Postgres

`infra/docker-compose.yml` gained `temporal` (the actual
`temporalio/auto-setup` server image, not the single-process
`temporal server start-dev` CLI dev-server -- the dev-server's SQLite
persistence is explicitly flagged by Temporal's own docs as not
guaranteed compatible across CLI versions, and it's not the shape a real
kill test should exercise), `temporal-ui`, and `mailpit` (a real local
SMTP catcher for the email alert channel -- see below). Temporal shares
this project's existing `timescaledb` container rather than running a
second Postgres: `auto-setup` creates its own `temporal` and
`temporal_visibility` databases inside it on first boot, using the same
`layover`/`layover` credentials. Elasticsearch (Temporal's "advanced
visibility" search) is skipped -- default visibility is enough to query
by workflow ID/status, and the whole stack already has enough moving
parts without it.

### Why Temporal, not a cron-and-database approach (a named design-decision point)

A cron job re-reading "which trips need checking" from a database loses
all in-flight state on every restart -- what was the last alerted risk
level, did the last notification actually send -- and has to rebuild
that from scratch on every tick, usually by adding more database columns
to track it. A Temporal workflow *is* that state, durably, for the whole
life of the trip: `last_alerted_p_miss` and `alert_count` live as plain
Python dict fields on the workflow instance, replayed from Temporal's
own event history on restart -- no separate "resume" logic to write or
get wrong, because resuming *is* a worker picking the workflow's next
task back up. The kill test below demonstrates this is a real property,
not a claim.

### Connection risk (`p_miss`): measured residual quantiles, not an assumed error model

The Milestone 7 models give a point prediction (trained on MAE, roughly
the conditional median) -- turning that into an actual missed-connection
*probability* needs a real error distribution, not a guess. Per the
standing instruction to research this kind of thing properly: measured
the empirical distribution of (actual arrival delay minus predicted
delay) on the real July 2026 holdout Milestones 3/4 already use (101
percentile points, n=612,038, one table per model), rather than assuming
a Gaussian error model. Delay is heavily right-skewed (already
established in Milestone 3's L1-vs-L2 finding), so a normal-distribution
assumption would badly understate tail risk -- confirmed by the measured
numbers themselves: the rotation model's p95 residual is +70.8 min, p99
is +215.4 min, far outside what a normal distribution with the same
mean/std (11.4 / 60.0) would predict. `p_miss` is read off this
empirical distribution by interpolation (`np.interp`), separately for
the pre- and post-departure models (the post-departure model's
distribution is measurably tighter, since it already knows this flight's
own departure delay -- confirmed live by `test_post_departure_has_tighter_distribution_than_rotation`).

### Alert policy: threshold plus hysteresis, not a stateless check

A stateless "p_miss > 0.30 -> alert" check re-fires on every periodic
re-check for as long as risk stays elevated -- exactly the spam the spec
rules out ("never duplicated, never spammed"). `decide_alert` (pure,
`alerts.py`) only fires on a first crossing above `ALERT_THRESHOLD`
(0.30) or a *further* rise of at least `ESCALATION_DELTA` (0.15) past
the level that triggered the last alert; dropping back below threshold
resets the state so a later re-crossing is a fresh event, not a muted
echo. The workflow owns this state per leg for the trip's whole life
(see above) -- the policy function itself never touches a database or a
clock.

### Email via a real local SMTP catcher, not SES -- a named, reversible substitution

The spec's deployment choice is Amazon SES; this milestone sends real
SMTP to Mailpit instead, for one reason: SES needs a real AWS account
and credentials, which don't exist for this project at this stage, and
`smtplib.SMTP` speaks the identical protocol to both -- swapping the
host/port/credentials at deploy time is the entire migration, not a
rewrite. Verified live, not assumed: real messages land in Mailpit's
real inbox (confirmed via its REST API during the kill test below).

### Web Push: the real protocol, a real generated VAPID identity, and a real cryptographic send -- with one documented environment limitation

`notify.send_push` uses `pywebpush` (VAPID-signed, AES128GCM-encrypted,
RFC 8291/8292) against a real generated VAPID keypair -- no external
account needed, since VAPID is a self-signed server identity, not a
third-party credential. A real end-to-end test needs a real
`PushSubscription` (endpoint + keys) from a browser's
`pushManager.subscribe()` call, which there's no web app to issue until
Milestone 9. Rather than wait, tried getting a real one directly: built
a minimal static page (`scripts/push_test/`) with a service worker,
served over real HTTP (Service Workers need a secure context; localhost
qualifies). **Found live**: this session's own sandboxed browser tool
has Service Worker registration disabled or broken at the browser-engine
level (`navigator.serviceWorker.register()` fails with "an unknown error
occurred when fetching the script" even though a plain `fetch()` of the
identical URL succeeds, and the registration request never appears in
the network log at all) -- a genuine environment limitation of this
automation context, not a bug in the test page or the server (confirmed:
correct `Content-Type: text/javascript`, 200 response, byte-correct
body, a secure context by the browser's own `isSecureContext` check).

Rather than leave this unverified, ran the strongest verification
possible without a real browser: generated a real P-256 subscriber
keypair and a real random auth secret (exactly what a browser would
generate), pointed a real `PushSubscription`'s endpoint at a local HTTP
server this script controls, and called `send_push` for real. The
result: a correctly-formed `vapid` Authorization header with a real
ES256-signed JWT (`aud`/`exp`/`sub` claims), a real `aes128gcm`
Content-Encoding header, and a 152-byte AES-GCM-encrypted body --
the complete client-side Web Push protocol, genuinely executed and
genuinely received over a real HTTP connection. The only unverified hop
is delivery to an actual browser's push service, which needs Milestone
9's subscribe UI (or the user's own Chrome, outside this sandbox, via
`scripts/push_test/`) to close -- a named, re-openable gap, not a
silent one.

### Five real bugs found building and live-testing this milestone

1. **`py_vapid==1.9.2`'s own `generate_keys()` is broken against
   `cryptography==46.0.0`** (installed here): it passes the
   `ec.SECP256R1` *class* instead of an *instance* to
   `generate_private_key()`, which newer `cryptography` rejects outright
   with `TypeError: curve must be an EllipticCurve instance`. A real,
   confirmed upstream incompatibility, not a guess -- worked around by
   generating the key directly (with a real curve instance) and
   assigning it through `Vapid`'s own `private_key` setter (which
   correctly derives the public key), rather than pinning an old,
   unmaintained `cryptography` or waiting on an upstream fix.
2. **Temporal's workflow-determinism sandbox statically validates
   everything a workflow module imports, transitively, including
   third-party I/O libraries the *activities* module needs** (`requests`
   -> `urllib3` failed the sandbox's static check on
   `http.client.IncompleteRead.__mro_entries__` outright, and separately,
   `layover_trip.notify`'s own module-level `Path(__file__).resolve()`
   tripped a restricted-filesystem-access check). Both only ever
   actually execute inside an Activity, never inside the workflow's own
   deterministic replay -- fixed by marking the specific modules
   (`requests`, `psycopg2`, `pywebpush`, `py_vapid`, `cryptography`,
   `numpy`, and this project's own `layover_trip.activities`/`db`/
   `notify`/`risk`) as sandbox passthrough, the officially supported
   mechanism for exactly this situation, in `run_trip_worker.py`.
3. **Temporal's strict dataclass converter rejects `None` for a field
   typed as plain `float`/`int`/`str`** (not `Optional[...]`) --
   `next_leg_sched_dep_epoch: float = None` on `TripLegInput` (the last
   leg of a trip genuinely has no next connection) failed workflow-input
   deserialization with `TypeError: Expected value to be int|float, was
   NoneType`. Fixed by typing every field that can legitimately be
   `None` as `Optional[...]`, across both `workflow.py` and
   `activities.py`'s dataclasses.
4. **An unread `subprocess.Popen(..., stdout=subprocess.PIPE)` silently
   deadlocks once the child's output exceeds the OS pipe buffer** -- the
   kill test's worker subprocess (fairly chatty: Temporal's own SDK logs
   a WARNING per sandboxed import) blocked trying to write once its
   output filled the buffer, which looked exactly like a hung worker
   (every query against it timed out at 30s) rather than a process stuck
   on a `write()` syscall. Fixed by redirecting worker subprocess output
   to a real log file instead of an unread pipe.
5. **The most substantial one**: `workflow.wait_condition(...,
   timeout=...)` raises `asyncio.TimeoutError` on timeout -- and on
   Python 3.9, `asyncio.TimeoutError` is a *different class* from the
   builtin `TimeoutError` (they were only unified into one alias
   starting Python 3.11). `except TimeoutError:` therefore never caught
   it, the exception escaped the workflow's `run()` coroutine entirely,
   and Temporal correctly treated an uncaught exception as the
   **workflow itself failing** -- the trip monitor silently crashed
   after its very first check interval elapsed, not after any kill test
   action. This fully explained an earlier, misleading "passing" test
   run: the workflow had already crashed (confirmed independently --
   attempting to `terminate()` it afterward returned "workflow execution
   already completed") before the kill test's own `kill_worker()` call,
   so every assertion after that point was unknowingly checking a frozen
   final state rather than genuine ongoing monitoring. Fixed by catching
   `asyncio.TimeoutError` explicitly; re-ran the full kill test after
   the fix and independently confirmed (via Temporal's own workflow
   history, not just the test's own assertions) that `fetch_leg_assessment`
   was genuinely scheduled three separate times (across two real
   timer fires spanning the kill and restart) with `send_alert` firing
   only on the first.

### The real kill test (2026-10-11, a real UA539 ORD->DFW -> AA1402 DFW->AUS connection)

The real historical data for 2026-07-15: UA539 departed ORD 115 minutes
late; AA1402 was scheduled to leave DFW only 52 minutes after UA539's
*scheduled* arrival, a 45-minute minimum connection time leaving only 7
minutes of real slack -- not a scripted scenario, a genuine tight
connection the dataset already contains.

- Worker #1 computed `p_miss = 0.995` (correctly: predicted arrival
  delay 108.8 min against a 7-minute buffer) and fired exactly one real
  alert: one row in the real `alerts` table, one real email delivered to
  Mailpit.
- Worker #1 killed outright (`SIGKILL`, not a graceful shutdown). The
  server-side workflow execution status: still `RUNNING`, confirmed via
  `describe()`, with zero workers polling it at all.
- A full check interval elapsed with no worker running at all: zero new
  alerts, confirmed directly against the database (not just the
  workflow's own state).
- Worker #2 started fresh. Temporal's own workflow history (not just
  this test's assertions) shows `fetch_leg_assessment` scheduled a
  second and third time, across two real timer fires -- genuine ongoing
  monitoring resumed, not a restart artifact. `send_alert` was **not**
  scheduled again: risk hadn't changed, so `decide_alert` correctly
  declined to re-fire, and the single `alerts` row for this connection's
  dedup key never became two.
- `stop_watching()` signaled; the workflow's `run()` returned for real
  (not just a status flip) and the trip's real database row transitioned
  to `status='completed'`.

**No trip stopped being watched, no duplicate alert** -- the spec's own
kill-test wording, demonstrated against a real crash, not simulated.
