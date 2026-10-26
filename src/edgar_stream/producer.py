"""Publish new-filing events, optionally paced, with injected duplicates and disorder."""

from __future__ import annotations

import random
import time

from .bus import Bus
from .events import FilingEvent


def plan_stream(
    events: list[FilingEvent], dup_rate: float = 0.0, shuffle_window: int = 0, seed: int = 0
) -> list[FilingEvent]:
    """Order events as an unreliable feed would deliver them.

    dup_rate: share of events delivered a second time later (at-least-once delivery).
    shuffle_window: events may move up to this many places (out-of-order delivery).
    """
    rng = random.Random(seed)
    out = list(events)
    for e in events:
        if rng.random() < dup_rate:
            out.insert(rng.randint(out.index(e) + 1, len(out)), e)
    if shuffle_window > 1:
        keyed = [(i + rng.uniform(0, shuffle_window), e) for i, e in enumerate(out)]
        out = [e for _, e in sorted(keyed, key=lambda t: t[0])]
    return out


def publish_all(bus: Bus, events: list[FilingEvent], rate_per_s: float | None = None) -> int:
    """Stamp each event with its publish time and send it. rate_per_s=None sends a burst."""
    start = time.monotonic()
    for n, e in enumerate(events):
        if rate_per_s:
            wait = start + n / rate_per_s - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        bus.publish(e.stamped(int(time.time() * 1000)))
    bus.flush()
    return len(events)
