# Demo video script (2-3 minutes)

Not produced as a video file here -- recording needs a real screen
capture + voiceover, which is outside what this environment can do.
This is the exact walkthrough to record, timed, against the real running
stack (no staged data, no mocked responses -- everything below is a real
request against the real pipeline, the same path used throughout this
project's own live verification).

## Setup (before recording)

```bash
make up && make sync-features && make predict-service && make web-api && make trip-worker && make web
```

Confirm `curl localhost:8000/healthz`, `localhost:8001/healthz`, and
`localhost:3001` all respond before rolling. Have two browser tabs ready:
the Temporal UI (`localhost:8080`) and the app (`localhost:3001`).

## Shot list

**0:00-0:20 -- Cold open on the problem.**
Voiceover over the landing page: "Airlines tell you about a delay once
it's official. Often the real signal exists hours earlier -- the plane
assigned to your flight is already running late on an earlier leg."
Show the landing page's own one-line pitch on screen.

**0:20-0:45 -- Add a real trip.**
Sign in, click "Add a trip," search `UA 539` for `2026-07-15` -- let the
disambiguation picker actually show up (this flight number covers two
real legs that day, ORD->DFW and DFW->IAH -- a genuine data quirk, not a
staged example; narrate it as "the system catching something that
actually trips up naive flight-number lookups"). Pick the ORD->DFW leg,
add `AA 1402` DFW->AUS as the connection.

**0:45-1:30 -- The real risk, live.**
Land on the trip detail page. This is the real UA539/AA1402 connection
from the real July 2026 holdout data: UA539 lands 115 minutes late
against only a 7-minute real buffer over the 45-minute minimum
connection time. Show, and narrate:
- `p_miss = 99%`, `predicted delay +109 min` -- not placeholder numbers,
  the actual model output for this actual flight.
- The evidence list ("already departed 115 min late," "historical
  delay patterns on the ORD-DFW route") -- every alert explains itself.
- Scroll to the aircraft-chain view: N76502's whole day, so a viewer
  sees *why* -- this physical airframe has flights before and after this
  one, and its lateness here is the thing actually driving the risk
  number, not a black box.

**1:30-2:00 -- Durability, proven, not claimed.**
Cut to a terminal: run (or show pre-recorded output of)
`scripts/verify_trip_monitoring.py`'s kill test -- `SIGKILL` a running
worker mid-monitoring, show the Temporal UI's workflow history: status
stays `RUNNING` through the kill, one alert fires, no duplicate after
restart. Narrate: "Real workflow engine, really killed and restarted,
zero dropped trips, zero duplicate alerts -- not a mocked test."

**2:00-2:30 -- The numbers that back it up.**
Cut to `docs/REPLAY_REPORT.md` or a terminal running `make backtest`'s
tail output: 64.9% recall on 15+ minute delays (vs. 11% for a
schedule-only baseline), a 219-minute median warning lead time, and the
load-test number -- a full month of real flight data replayed through
the live pipeline in about two minutes, ~20,900x faster than real time.

**2:30-2:50 -- Close.**
Back to the web app: show the public flight page (no sign-in needed) and
the "Delete this trip" control -- narrate briefly that user data is a
real, deletable thing, not an afterthought. End on the one-line pitch
again: "Know you'll miss your connection before the airline tells you."

## Notes for whoever records this

- Every number named above is reproducible by re-running the referenced
  script against this repo's own data -- rehearse once against the real
  stack before recording so the risk numbers you narrate match what's
  on screen (they're deterministic given the same trained model and
  data, but confirm rather than assume).
- If Service Worker push notifications don't work in the recording
  environment, that's expected in some sandboxed/incognito setups (see
  DECISIONS.md, Milestone 9) -- the UI degrades gracefully to "email
  alerts will still be sent" rather than hanging, which is itself worth
  showing rather than editing around.
