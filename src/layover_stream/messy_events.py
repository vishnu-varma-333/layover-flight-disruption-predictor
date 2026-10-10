"""Deliberately reorders, duplicates, delays and drops events from an
otherwise-clean, sorted event stream -- reusing Milestone 4's event
generation (`layover_backtest.events.build_event_stream`) as the source
of truth for *what* events exist, while this module controls the *order
and completeness* they're actually produced in.

This exists because Milestone 4 explicitly deferred "genuinely out-of-
order, multi-source arrival" to this milestone (a backtest, replaying
known history with a producer it controls, could legitimately assume
sorted single-partition delivery; a real system can't). The four
categories match the spec's own phrase verbatim: "late, out-of-order,
duplicate and missing messages."

Each category is controlled independently and is a real, distinct test of
the Flink job's handling, not a single blanket "shuffle everything":

- **out-of-order**: events are shuffled only *within* small local windows
  (a few seconds/positions of each other) -- the ordinary jitter a real
  network or multi-partition topic produces, which a bounded-out-of-
  orderness watermark strategy is specifically designed to absorb.
- **late**: a small number of events are pulled out and appended at the
  very end of the produce sequence, guaranteeing they arrive long after
  the watermark has advanced past their event time -- proving the
  explicit "this arrived too late" path, not just the ordinary-jitter
  path above.
- **duplicate**: a small number of events are re-sent a few positions
  after their original -- an exact repeat, the way an at-least-once
  producer retry actually behaves, not a delayed one.
- **missing**: a small number of events are dropped from the produce
  sequence entirely and never sent -- the rotation-linking logic must
  degrade to "no inbound information" gracefully, not fail.

Event *content* (the `event_time` field) is never altered -- only
delivery order and completeness are. That's the whole point: event-time
correctness must come from watermarks acting on the data's own
timestamps, not from assuming the transport delivers it in order.
"""
import random

import pandas as pd


def inject_messiness(
    stream: pd.DataFrame,
    seed: int = 42,
    shuffle_window: int = 8,
    duplicate_rate: float = 0.03,
    very_late_rate: float = 0.005,
    missing_rate: float = 0.01,
) -> pd.DataFrame:
    rng = random.Random(seed)
    rows = stream.to_dict("records")
    n = len(rows)

    # Missing: drop entirely, before any reordering.
    missing_n = int(n * missing_rate)
    dropped = set(rng.sample(range(n), missing_n)) if missing_n else set()
    kept_rows = [r for i, r in enumerate(rows) if i not in dropped]
    n = len(kept_rows)

    # Out-of-order: shuffle within small non-overlapping windows.
    order = list(range(n))
    for start in range(0, n, shuffle_window):
        window = order[start : start + shuffle_window]
        rng.shuffle(window)
        order[start : start + shuffle_window] = window

    # Late: pull a few out of the shuffled order and append at the very end.
    # (max(1, ...) only applies when the rate is actually positive -- a
    # rate of exactly 0 must mean exactly zero, not "at least one anyway".)
    very_late_n = max(1, int(n * very_late_rate)) if (n and very_late_rate > 0) else 0
    very_late_idx = set(rng.sample(order, very_late_n)) if very_late_n else set()
    produce_order = [i for i in order if i not in very_late_idx] + [
        i for i in order if i in very_late_idx
    ]
    produced = [kept_rows[i] for i in produce_order]

    # Duplicate: re-insert a copy a few positions after the original.
    dup_n = int(n * duplicate_rate)
    with_dups = list(produced)
    if dup_n:
        dup_source_positions = sorted(rng.sample(range(len(produced)), dup_n))
        offset = 0
        for pos in dup_source_positions:
            source_pos = pos + offset
            insert_at = min(source_pos + rng.randint(1, 5), len(with_dups))
            with_dups.insert(insert_at, with_dups[source_pos])
            offset += 1

    return pd.DataFrame(with_dups)
