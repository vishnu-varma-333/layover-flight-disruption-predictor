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
