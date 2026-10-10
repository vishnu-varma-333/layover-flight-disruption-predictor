"""Shared cycle-sleep logic for the three ingestors.

Found live, not assumed: a naive `sleep(interval - elapsed)` ignores the
overall run's `--duration` budget entirely. With OpenSky's default
interval (3600s) and a 90-second bounded verification run, the process
kept running for the better part of an hour after its single real cycle,
because it slept for ~3,593 seconds before ever re-checking whether the
90-second duration had elapsed. Capping the sleep at whatever's left of
the overall deadline (when one is set) fixes it.
"""
import asyncio
import time


async def sleep_until_next_cycle(cycle_start: float, interval_seconds: float, deadline) -> None:
    sleep_for = interval_seconds - (time.monotonic() - cycle_start)
    if deadline is not None:
        sleep_for = min(sleep_for, deadline - time.monotonic())
    if sleep_for > 0:
        await asyncio.sleep(sleep_for)
