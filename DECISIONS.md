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
