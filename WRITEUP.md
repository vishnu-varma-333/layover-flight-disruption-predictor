# Layover: write-up

A real-time flight disruption predictor, built end to end as a portfolio
project -- data engineering, streaming, ML, a durable workflow engine
and a shipped web app, each piece live-verified against real systems and
real data rather than mocked. This document is the narrative; the
details live in `docs/MILESTONES.md` (what was built, milestone by
milestone), `DECISIONS.md` (every real design decision and its
tradeoffs), `docs/BENCHMARKS.md` (every measured number), `docs/
MODEL_CARD.md` (what the model does and doesn't do well), and `docs/
REPLAY_REPORT.md` (the headline results).

## The problem

Airlines announce delays late. The signal that your connection is at
risk often exists hours earlier: the physical aircraft assigned to your
flight is already running behind on an earlier leg somewhere else in the
country. Layover tracks that aircraft, not just your flight's own
schedule, and warns you before the airline does.

## What it does

Streams live aircraft positions, airport weather and FAA status;
links each flight to its aircraft and that aircraft's full chain of
flights for the day; predicts arrival delay as that chain's delays
propagate; turns a predicted delay into a connection-miss probability
against your specific layover's buffer; watches each trip for days
through a durable workflow that survives restarts; and alerts you --
with evidence, not just a number -- only when the risk actually changes
enough to matter.

## Architecture, briefly

Live data and historical replays enter the *same* Kafka topics, so a
backtest exercises the exact production code path, not a separate
offline evaluation harness that could silently drift from what's
actually served. Flink keeps aircraft rotations and online features
current; a FastAPI service serves the latest model's risk and
explanation from that online state; Temporal workflows decide when to
alert, with the durability guarantees a hand-rolled cron-and-database
approach doesn't give for free. A Next.js PWA is the only thing a user
or their browser ever talks to directly -- it owns the session and calls
an internal, unauthenticated-but-trusted backend-for-frontend, which is
the only thing that talks to Temporal, TimescaleDB and the prediction
service. See `DECISIONS.md` for why each of these choices, specifically,
and what was considered and rejected.

## What's real here, not staged

Every number in this write-up and in `docs/REPLAY_REPORT.md` comes from
actually running the code against real data -- the July 2026 BTS
On-Time Performance holdout month the model never trained on, replayed
through the real Kafka stack, scored by the real trained LightGBM
models. The project's own working discipline (documented because it
shaped a lot of what got built and fixed): verify everything live, not
just with unit tests -- and when live verification turns up a real bug,
fix it and write it down rather than quietly patching around it. Several
of the more interesting entries in `DECISIONS.md` are bugs found exactly
this way: a backtest consumer that silently reported wrong metrics after
timing out instead of failing loudly; a real flight-number collision
(two different physical legs sharing `UA539` on the same day) that a
naive lookup would have resolved to the wrong one; an MLflow model-
loading path that worked on a developer machine and broke the moment it
ran inside a container, because MLflow bakes an absolute host path into
a run's own metadata.

## Results

| Metric | Result |
| --- | --- |
| 15+ min delay detection recall | 64.9% (73.9% on legs with a rotation signal), vs. 11.0% for a schedule-only baseline |
| Median warning lead time | 219 minutes before scheduled departure |
| Arrival-delay accuracy | 21.4-22.1 min MAE, vs. 29.82 min baseline (~26-28% better) |
| Connection-risk precision / recall at the alert threshold | 67.7% / 72.5% |
| Monitoring durability | 0 trips dropped, 0 duplicate alerts across a real forced worker kill and restart |
| Replay/load-test throughput | 18,504 events/sec; a full month of real data replayed ~20,900x faster than real time |

Full methodology for every number: `docs/BENCHMARKS.md` and `docs/
REPLAY_REPORT.md`.

## What's deliberately not done yet

Honest scope boundaries, not dropped requirements:

- **Real AWS deployment.** Full Terraform (EC2, S3, Secrets Manager,
  least-privilege IAM) is written and validated (`infra/terraform`,
  `terraform validate` clean) and every service is containerized and
  build/run-tested locally, but nothing is applied against a real AWS
  account in this environment -- no AWS account was available to apply
  against. The honest-labeling discipline that shaped every benchmark in
  this project applies here too: "written and validated" is what
  actually happened; "deployed" would be a claim this project can't back
  up yet.
- **Amazon SES** is a named, reversible substitution for a real local
  SMTP catcher (Mailpit) -- same protocol, different endpoint -- for the
  same reason.
- **Real passenger itinerary data** doesn't exist publicly, so
  connection-risk evaluation (`docs/MODEL_CARD.md`) uses synthetically
  constructed connections from real flight pairs, not real bookings --
  documented as a limitation, not hidden.
- **Version 2** (email itinerary import via an LLM, whole-trip risk
  across multi-leg journeys, airport-level disruption signals) is
  explicitly sequenced after v1 ships, per the spec's own plan.

## Try it

`make up && make sync-features && make predict-service && make web-api
&& make trip-worker && make web`, then `localhost:3001`. `docs/
DEMO_SCRIPT.md` has a shot-by-shot walkthrough of the same real UA539
connection used throughout this project's own testing.
