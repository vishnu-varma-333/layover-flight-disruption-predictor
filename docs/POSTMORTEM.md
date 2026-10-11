# Postmortem: Docker daemon killed while testing Milestone 10 containers

**Date:** 2026-10-11
**Severity:** Low (local dev environment only; no production system, no
user-facing impact, no data loss for durably-stored data)
**Status:** Resolved

## Summary

While live-testing the Milestone 10 Docker images, a command intended to
free TCP port 3001 (held by a stale `npm run dev` process) instead
killed the Docker Desktop daemon itself, stopping every running
container -- the project's own infra stack plus two unrelated container
groups sharing the same machine. Redis and Kafka (Redpanda) lost their
in-memory/non-persisted state; TimescaleDB's data survived (it has a
durable volume). Recovered within minutes by restarting Docker Desktop
and regenerating the two derived caches that had no volume.

## Timeline (all times approximate, same session)

1. Four new app containers (`predict-service`, `web-api`, `trip-worker`,
   `web`) built successfully and three started; `web` failed with "port
   3001 already in use" -- a leftover host-side `npm run dev` process
   from earlier in the session, not stopped when the containerized
   version was meant to take over.
2. Ran `lsof -ti:3001 | xargs kill` to free the port, plus the same for
   port 8001. This is a command that's correct *in general* -- but it
   kills whatever process actually holds the port, not necessarily the
   one the operator has in mind.
3. Port 3001 was, by coincidence, also where Docker Desktop's own proxy
   process had a listener. `kill` took down Docker Desktop's main
   process, not just the Next.js dev server.
4. Every running container stopped immediately, confirmed by the very
   next `docker ps` failing with "cannot connect to the Docker daemon."
5. Restarted Docker Desktop (`open -a Docker`), waited ~10s for the
   socket to come back.
6. `make up` brought the project's own infra stack back; TimescaleDB's
   named volume reattached with all data intact (verified: the trip
   deleted earlier in the same session, and its cascade, were still
   correctly reflected -- nothing resurrected, nothing extra).
7. Redis and Kafka had no persisted data to restore (by design -- see
   "Why this didn't lose real data" below). Regenerated: `make
   rotation-produce && make rotation-job && make sync-features`
   (~2.5 minutes total), then re-verified a known flight's risk endpoint
   returned real data again.
8. Resumed the container testing that had been interrupted.

## Impact

- No production system exists yet (this is pre-launch development), so
  no user ever saw downtime.
- No durable data was lost: TimescaleDB (trips, alerts, risk history)
  persisted because it has a real volume. This is the system of record
  for anything a user would be upset to lose.
- ~5-10 minutes of session time spent on recovery.
- Two unrelated container groups on the same machine (`proving-ground-*`,
  `kind-registry-*`) were also stopped and had to come back on their own
  -- outside this project's scope to fix, but worth naming as blast
  radius: killing a shared daemon affects everything using it, not just
  the one thing you meant to touch.

## Why this didn't lose real data

Not luck -- a design choice made back in Milestone 1 and followed since:
TimescaleDB has a named Docker volume (`timescale_data`); Redis and
Redpanda don't. That's deliberate, not an oversight -- Redis here is
explicitly an *online feature cache*, rebuilt from Kafka on a schedule
(`sync-features`), and Redpanda's demo-day topics are themselves rebuilt
from the Milestone 2 Parquet dataset on demand
(`rotation-produce`/`rotation-job`). Neither was ever the durable system
of record; TimescaleDB is. This incident is the first time that design
decision actually got exercised under a real failure, and it held.

## Root cause

Using `lsof -ti:<port> | xargs kill` assumes the process bound to a port
is the one you expect. It killed Docker Desktop's own process because
that process, not the target `npm` process, happened to be the one
listening on port 3001 at that moment (the `npm run dev` process from
earlier had already exited or rebound; Docker Desktop's proxy had taken
the port in the interim). The command had no step that confirmed *which*
process it was about to kill before killing it.

## Corrective actions

- [x] `docs/RUNBOOK.md` now documents this exact failure mode and its
      recovery steps, so a repeat (by a human operator or a future
      agent) resolves in under a minute instead of being diagnosed from
      scratch.
- [ ] Prefer `lsof -i :<port> -sTCP:LISTEN` (inspect first) over
      `lsof -ti:<port> | xargs kill` (kill blind) when freeing a port in
      any shared-daemon environment -- noted here as the actual process
      change; not yet enforced by tooling (no pre-commit hook or similar
      exists to enforce a *shell habit*, and shouldn't -- this is a
      judgment-call practice, not a code-level fix to any importable
      module).
- [ ] Not pursued: making Redis/Kafka durable across daemon restarts.
      Deliberately out of scope -- doing so would contradict the
      Milestone 1 design decision this postmortem's own "Why this didn't
      lose real data" section just validated. The fix for this incident
      is operator care and documentation, not re-architecting a cache to
      survive a failure mode it was never meant to survive.
