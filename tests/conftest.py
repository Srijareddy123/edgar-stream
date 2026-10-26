from __future__ import annotations

import json
from pathlib import Path

import pytest

from edgar_stream import warehouse as wh
from edgar_stream.events import FilingEvent, base_form, version_of
from edgar_stream.quality import Validator
from edgar_stream.replay import DaySource
from edgar_stream.store import LocalStore


def make_submission(
    accession: str,
    cik: int = 1234,
    form: str = "8-K",
    filed: str = "2026-01-05",
    period: str = "2026-01-02",
    body: str | None = None,
) -> bytes:
    """A small but structurally real EDGAR submission text file."""
    d = filed.replace("-", "")
    if body is None:
        body = (
            "<div>Item 2.02 Results of Operations</div><div>Revenue grew.</div>"
            "<div>Item 9.01 Financial Statements and Exhibits</div><div>Exhibit list.</div>"
        )
    return (
        f"<SEC-DOCUMENT>{accession}.txt : {d}\n<SEC-HEADER>{accession}.hdr.sgml : {d}\n"
        f"<ACCEPTANCE-DATETIME>{d}120000\nACCESSION NUMBER:\t\t{accession}\n"
        f"CONFORMED SUBMISSION TYPE:\t{form}\nCONFORMED PERIOD OF REPORT:\t{period.replace('-', '')}\n"
        f"FILED AS OF DATE:\t\t{d}\nFILER:\n\tCOMPANY DATA:\n"
        f"\t\tCOMPANY CONFORMED NAME:\t\t\tACME CORP\n\t\tCENTRAL INDEX KEY:\t\t\t{cik:010d}\n"
        f"\t\tSTATE OF INCORPORATION:\t\t\tDE\n</SEC-HEADER>\n<DOCUMENT>\n<TYPE>{form}\n"
        f"<SEQUENCE>1\n<TEXT>\n<html><body>{body}</body></html>\n</TEXT>\n</DOCUMENT>\n"
        "</SEC-DOCUMENT>\n"
    ).encode()


def make_event(accession: str, cik: int = 1234, form: str = "8-K", filed: str = "2026-01-05"):
    return FilingEvent(
        accession=accession,
        version=version_of(form),
        cik=cik,
        company="ACME CORP",
        form=form,
        filed_date=filed,
        url_path=f"edgar/data/{cik}/{accession}.txt",
        event_ts_ms=1,
    )


def write_day(root: Path, day: str, items: list[tuple[FilingEvent, bytes | None]]) -> DaySource:
    (root / day / "raw").mkdir(parents=True, exist_ok=True)
    for e, raw in items:
        if raw is not None:
            (root / day / "raw" / f"{e.accession}.txt").write_bytes(raw)
    meta = {"events": [json.loads(e.to_json()) for e, _ in items]}
    (root / day / "index.json").write_text(json.dumps(meta))
    return DaySource(root)


@pytest.fixture(scope="session")
def validator() -> Validator:
    return Validator()


@pytest.fixture
def conn():
    c = wh.connect(":memory:")
    yield c
    c.close()


@pytest.fixture
def store(tmp_path):
    return LocalStore(tmp_path / "objects")


__all__ = ["base_form", "make_event", "make_submission", "write_day"]
