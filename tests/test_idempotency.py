"""Property-style tests: any delivery order, with repeats, ends in the same warehouse state."""

import random

import pytest
from conftest import make_event, make_submission
from hypothesis import given, settings
from hypothesis import strategies as st

from edgar_stream import warehouse as wh
from edgar_stream.parse import parse_submission

PERIODS = ["2026-03-31", "2026-06-30"]


def build(cik: int, period: str, version: int, serial: int, day: int):
    form = "10-Q/A" if version else "10-Q"
    acc = f"{cik:010d}-26-{serial:06d}"
    filed = f"2026-07-{day:02d}"
    return parse_submission(
        make_submission(acc, cik=cik, form=form, filed=filed, period=period),
        make_event(acc, cik=cik, form=form, filed=filed),
    )


@st.composite
def universe(draw):
    """A few companies, each with an original and 0 to 2 amendments of the same period."""
    out, serial = [], 1
    for cik in draw(st.lists(st.integers(1, 5), min_size=1, max_size=3, unique=True)):
        period = draw(st.sampled_from(PERIODS))
        out.append(build(cik, period, 0, serial, 1))
        for n in range(draw(st.integers(0, 2))):
            serial += 1
            out.append(build(cik, period, 1, serial, 2 + n))
        serial += 1
    return out


def state(conn):
    cur = conn.execute("select filing_key, accession, version from filing_current order by 1")
    current = cur.fetchall()
    hist = conn.execute("select accession, version from stg_filings order by 1, 2").fetchall()
    return current, hist


def expected_current(filings):
    best = {}
    for p in filings:
        if p.filing_key not in best or p.order_key > best[p.filing_key].order_key:
            best[p.filing_key] = p
    return sorted((p.filing_key, p.accession, p.version) for p in best.values())


@settings(max_examples=40, deadline=None)
@given(filings=universe(), seed=st.integers(0, 10_000), dup_share=st.floats(0, 1))
def test_any_order_with_duplicates_gives_the_same_state(filings, seed, dup_share):
    rng = random.Random(seed)
    delivered = filings + [p for p in filings if rng.random() < dup_share]
    rng.shuffle(delivered)

    ordered, shuffled = wh.connect(":memory:"), wh.connect(":memory:")
    wh.apply_batch(ordered, filings)
    # same events, hostile order, split into random batches
    i = 0
    while i < len(delivered):
        n = rng.randint(1, 4)
        wh.apply_batch(shuffled, delivered[i : i + n])
        i += n

    assert state(shuffled) == state(ordered)
    assert state(shuffled)[0] == expected_current(filings)
    assert len(state(shuffled)[1]) == len(filings)


def test_older_version_arriving_late_never_replaces_the_newer_one(conn):
    original, amendment = build(1, "2026-03-31", 0, 1, 1), build(1, "2026-03-31", 1, 2, 5)
    wh.apply_batch(conn, [amendment])
    outcomes = wh.apply_batch(conn, [original])
    assert outcomes[0].outcome == wh.STALE
    assert conn.execute("select accession, version from filing_current").fetchall() == [
        (amendment.accession, 1)
    ]
    assert conn.execute("select count(*) from stg_filings").fetchone()[0] == 2  # history kept


def test_same_event_twice_in_one_batch_loads_once(conn):
    p = build(1, "2026-03-31", 0, 1, 1)
    outcomes = [o.outcome for o in wh.apply_batch(conn, [p, p])]
    assert outcomes == [wh.LOADED, wh.DUPLICATE]


def test_a_failing_batch_rolls_back_completely(conn, monkeypatch):
    good, other = build(1, "2026-03-31", 0, 1, 1), build(2, "2026-03-31", 0, 2, 1)
    real = wh._row

    def boom(p, now):
        if p.cik == 2:
            raise RuntimeError("disk full")
        return real(p, now)

    monkeypatch.setattr(wh, "_row", boom)
    with pytest.raises(RuntimeError):
        wh.apply_batch(conn, [good, other])
    assert conn.execute("select count(*) from stg_filings").fetchone()[0] == 0
