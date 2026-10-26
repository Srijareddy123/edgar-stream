"""Saved days on disk: the event list and the raw submission files."""

from __future__ import annotations

import json
from pathlib import Path

from .events import FilingEvent


class MissingRaw(Exception):
    """The raw file for an event is not in the saved day (the fetch failed)."""


class DaySource:
    def __init__(self, days_dir: Path) -> None:
        self.days_dir = Path(days_dir)
        self._day_of: dict[str, str] = {}  # accession -> day directory it was delivered on

    def available_days(self) -> list[str]:
        return sorted(p.parent.name for p in self.days_dir.glob("*/index.json"))

    def events(self, day: str) -> list[FilingEvent]:
        meta = json.loads((self.days_dir / day / "index.json").read_text())
        events = [FilingEvent(**e) for e in meta["events"]]
        self._day_of.update({e.accession: day for e in events})
        return events

    def get(self, event: FilingEvent) -> bytes:
        # The index of one day can list a filing whose "Date Filed" is an earlier day.
        day = self._day_of.get(event.accession, event.filed_date)
        path = self.days_dir / day / "raw" / f"{event.accession}.txt"
        if not path.is_file():
            found = next(self.days_dir.glob(f"*/raw/{event.accession}.txt"), None)
            if found is None:
                raise MissingRaw(event.accession)
            path = found
        return path.read_bytes()
