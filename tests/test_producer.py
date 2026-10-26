from collections import Counter

from conftest import make_event
from hypothesis import given, settings
from hypothesis import strategies as st

from edgar_stream.bus import MemoryBus
from edgar_stream.producer import plan_stream, publish_all

EVENTS = [make_event(f"0000000001-26-{n:06d}") for n in range(30)]


@settings(max_examples=40, deadline=None)
@given(dup=st.floats(0, 1), window=st.integers(0, 15), seed=st.integers(0, 999))
def test_plan_never_loses_an_event_and_only_adds_repeats(dup, window, seed):
    out = plan_stream(EVENTS, dup, window, seed)
    counts = Counter(e.key for e in out)
    assert set(counts) == {e.key for e in EVENTS}
    assert all(c >= 1 for c in counts.values()) and len(out) >= len(EVENTS)


def test_plan_is_deterministic_for_a_seed():
    assert plan_stream(EVENTS, 0.2, 5, 3) == plan_stream(EVENTS, 0.2, 5, 3)


def test_publish_stamps_event_time_and_paces():
    bus = MemoryBus()
    publish_all(bus, EVENTS[:5], rate_per_s=100)
    msgs = bus.poll(10, 0.1)
    assert len(msgs) == 5 and all(m.event.event_ts_ms > 1 for m in msgs)
    assert msgs[-1].event.event_ts_ms - msgs[0].event.event_ts_ms >= 35
