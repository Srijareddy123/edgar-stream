"""The new-filing event that travels through the stream."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace

TARGET_FORMS = ("10-K", "10-K/A", "10-Q", "10-Q/A", "8-K", "8-K/A")


def base_form(form: str) -> str:
    return form[:-2] if form.endswith("/A") else form


def version_of(form: str) -> int:
    """0 for an original filing, 1 for an amendment (form ends with /A)."""
    return 1 if form.endswith("/A") else 0


@dataclass(frozen=True)
class FilingEvent:
    accession: str
    version: int
    cik: int
    company: str
    form: str
    filed_date: str  # YYYY-MM-DD
    url_path: str  # path under https://www.sec.gov/Archives/
    event_ts_ms: int = 0  # set by the producer at publish time

    @property
    def key(self) -> str:
        return f"{self.accession}:{self.version}"

    def stamped(self, ts_ms: int) -> FilingEvent:
        return replace(self, event_ts_ms=ts_ms)

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))

    @staticmethod
    def from_json(text: str | bytes) -> FilingEvent:
        return FilingEvent(**json.loads(text))
