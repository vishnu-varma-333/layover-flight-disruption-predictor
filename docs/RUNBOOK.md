# Runbook

Operational reference for running and recovering Layover. Pairs with
`docs/MODEL_CARD.md` (what the model does) and `docs/BENCHMARKS.md`
(what's been measured). Local-dev commands below; a real deployment
runs the same containers via `infra/docker-compose.prod.yml` on the host
`infra/terraform` provisions (see that directory's README comment block
in `main.tf`).

## Starting everything

```bash
make up              # infra: Kafka, TimescaleDB, Redis, Temporal, observability
make verify           # confirms every infra service is actually healthy
make sync-features     # Flink output -> Redis (needed before predict-service has data)
make predict-service    # port 8000
make web-api              # port 8001
make trip-worker           # Temporal worker
make web                    # port 3001 (cd web && npm install once, first time)
```

Or, containerized (Milestone 10):

```bash
docker compose -p layover -f infra/docker-compose.yml -f infra/docker-compose.prod.yml up -d
```

## Stopping everything

```bash
make down    # tears down infra containers AND their volumes (-v) -- see "Data loss" below
```

For just the app processes (leaving infra running, the project's usual
between-session state): stop each `make` target's process, or for the
containerized form, `docker compose ... stop predict-service web-api
trip-worker web` (no `-v`, no `down`).

## Common failures

### "risk" comes back null for a flight that should have one

The online feature store (Redis) only has what the last `sync-features`
run put there, and it's an in-memory cache, not durable storage (no
volume in `infra/docker-compose.yml` by design -- it's a cache, not a
system of record; TimescaleDB is the durable store). Any Redis restart
empties it. Fix: `make sync-features` again (re-reads the Milestone 6
Flink job's Kafka output topic, ~20s). If that topic is also empty
(e.g. Redpanda itself lost data), regenerate it first:
`make rotation-produce && make rotation-job`.

### Redis/Kafka data missing after the host's Docker daemon restarted

**This happened for real during Milestone 10's own container testing** --
worth documenting exactly because it's the kind of thing that looks like
an application bug but isn't one. Freeing a port that turned out to be
held by Docker Desktop's own process (not the container using it)
killed the daemon entirely, taking every running container down with
it -- including Redis and Redpanda, neither of which persist their data
to a volume. TimescaleDB *did* keep its data (it has a named volume,
`timescale_data`) -- trips, alerts and risk history all survived; only
the derived/cache state (Redis features, in-flight Kafka topics) needed
regenerating.

Recovery, in order:
1. Confirm the daemon is actually back: `docker ps`.
2. `make up` -- brings infra back, TimescaleDB's volume reattaches
   automatically, no data loss there.
3. `make rotation-produce && make rotation-job && make sync-features`
   -- regenerates the derived state that had no volume.
4. Re-verify: `make verify-predict` or a direct `curl localhost:8000/
   flights/<id>/risk` for a known flight.

Lesson for next time: never `kill` a PID found via `lsof -ti:<port>`
without checking what process it actually is first (`ps -p <pid>`) --
a port can be held by something other than the service you meant to
stop.

### Trip monitoring seems to have stopped

Check the real Temporal server state, not just application logs --
Temporal's own history is the source of truth for whether a workflow is
still running:

```bash
# via the Temporal UI (make up includes it)
open http://localhost:8080
# or
scripts/verify_trip_monitoring.py   # the real kill-test script; safe to re-run read-only pieces
```

A worker crashing mid-monitoring is an *expected, tested* scenario (see
`docs/BENCHMARKS.md` Milestone 8's real kill test) -- the workflow stays
`RUNNING` in Temporal's history and resumes once any worker restarts
(`make trip-worker`), with no dropped trip and no duplicate alert.

### Container builds apache-flink from source and fails

Only happens building for `linux/arm64` (e.g. natively on an Apple
Silicon Mac) -- `apache-flink`'s PyPI wheel has no pre-built arm64 Linux
artifact, so pip falls back to a source build that isn't set up to
succeed in this image. Force the real deployment target instead:
`docker build --platform linux/amd64 ...` (or `DOCKER_DEFAULT_PLATFORM=
linux/amd64` for `docker compose`), matching the EC2 instance
`infra/terraform` actually provisions.

## Data deletion (user request)

`DELETE /trips/{id}` on the internal API (wired through the web app's
own "Delete this trip" button) hard-deletes the trip and -- via real
`ON DELETE CASCADE` foreign keys, not application-level cleanup code --
every `trip_leg`, `alert` and `risk_assessment` row that references it.
Confirmed live by deleting a real trip and querying TimescaleDB directly
for leftover rows (Milestone 10; see DECISIONS.md).
