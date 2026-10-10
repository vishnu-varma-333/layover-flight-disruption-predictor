import asyncio
import time

from layover_ingest.scheduling import sleep_until_next_cycle


def test_sleep_capped_at_remaining_duration_not_full_interval():
    # Regression test for a real bug: a 90-second bounded run with
    # OpenSky's 3600-second default interval kept running for the better
    # part of an hour, because the sleep never considered the overall
    # duration budget -- only the cycle interval. See DECISIONS.md.
    cycle_start = time.monotonic()
    deadline = time.monotonic() + 0.05  # the run's overall budget almost up
    started = time.monotonic()
    asyncio.run(sleep_until_next_cycle(cycle_start, interval_seconds=3600, deadline=deadline))
    elapsed = time.monotonic() - started
    assert elapsed < 0.5  # capped at ~the remaining budget, not 3600s


def test_no_deadline_sleeps_the_full_interval():
    cycle_start = time.monotonic()
    started = time.monotonic()
    asyncio.run(sleep_until_next_cycle(cycle_start, interval_seconds=0.05, deadline=None))
    elapsed = time.monotonic() - started
    assert elapsed >= 0.04


def test_cycle_already_took_longer_than_interval_does_not_sleep():
    cycle_start = time.monotonic() - 10  # cycle itself took 10s already
    started = time.monotonic()
    asyncio.run(sleep_until_next_cycle(cycle_start, interval_seconds=1, deadline=None))
    elapsed = time.monotonic() - started
    assert elapsed < 0.1
