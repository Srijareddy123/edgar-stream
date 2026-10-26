"""EDGAR daily index parsing and a client that respects SEC fair access."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from datetime import date
from pathlib import Path

import requests

from .config import PLACEHOLDER_AGENT
from .events import TARGET_FORMS, FilingEvent, version_of

ARCHIVES = "https://www.sec.gov/Archives/"
MIN_INTERVAL_S = 0.11  # about 9 requests a second, under the SEC limit of 10
INDEX_ROW = re.compile(
    r"^(?P<form>\S+)\s+(?P<company>.+?)\s+(?P<cik>\d{1,10})\s+(?P<date>\d{8})\s+"
    r"(?P<path>edgar/\S+)\s*$"
)


class NoIndexForDay(Exception):
    """EDGAR has no daily index for this date (weekend, holiday or future)."""


def parse_form_index(text: str) -> list[FilingEvent]:
    """Parse the text of a form.YYYYMMDD.idx file into events (all forms)."""
    events: list[FilingEvent] = []
    for line in text.splitlines():
        m = INDEX_ROW.match(line)
        if not m:
            continue
        d = m["date"]
        accession = Path(m["path"]).stem
        events.append(
            FilingEvent(
                accession=accession,
                version=version_of(m["form"]),
                cik=int(m["cik"]),
                company=m["company"].strip(),
                form=m["form"],
                filed_date=f"{d[:4]}-{d[4:6]}-{d[6:]}",
                url_path=m["path"],
            )
        )
    return events


def select_events(events: list[FilingEvent], max_n: int) -> list[FilingEvent]:
    """Keep target forms, drop repeats, and take a stable pseudo-random sample."""
    seen: dict[str, FilingEvent] = {}
    for e in events:
        if e.form in TARGET_FORMS:
            seen.setdefault(e.key, e)
    ranked = sorted(seen.values(), key=lambda e: hashlib.sha1(e.accession.encode()).hexdigest())
    return sorted(ranked[:max_n], key=lambda e: e.accession)


def index_url(day: date) -> str:
    quarter = (day.month - 1) // 3 + 1
    return f"{ARCHIVES}edgar/daily-index/{day.year}/QTR{quarter}/form.{day:%Y%m%d}.idx"


class EdgarClient:
    """Throttled HTTP client. Needs a real contact string in the User-Agent."""

    def __init__(self, user_agent: str, session: requests.Session | None = None) -> None:
        if not user_agent or user_agent == PLACEHOLDER_AGENT:
            raise ValueError(
                "Set EDGAR_USER_AGENT in .env to 'Your Name your.email@example.com'. "
                "The SEC requires it for automated access."
            )
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip"})
        self._lock = threading.Lock()
        self._last = 0.0
        self.requests_made = 0

    def _wait(self) -> None:
        with self._lock:
            gap = MIN_INTERVAL_S - (time.monotonic() - self._last)
            if gap > 0:
                time.sleep(gap)
            self._last = time.monotonic()
            self.requests_made += 1

    def _get(self, url: str, retries: int = 4) -> requests.Response:
        for attempt in range(retries):
            self._wait()
            resp = self._session.get(url, stream=True, timeout=30)
            if resp.status_code in (429, 500, 502, 503, 504):
                resp.close()
                time.sleep(2.0 * (attempt + 1))
                continue
            return resp
        raise RuntimeError(f"giving up on {url} after {retries} tries")

    def get_index(self, day: date) -> str:
        resp = self._get(index_url(day))
        if resp.status_code in (403, 404):
            resp.close()
            raise NoIndexForDay(day.isoformat())
        resp.raise_for_status()
        return resp.content.decode("latin-1")

    def get_submission(self, url_path: str, max_bytes: int) -> bytes:
        """Download a submission text file, stopping after max_bytes."""
        resp = self._get(ARCHIVES + url_path)
        try:
            resp.raise_for_status()
            buf = bytearray()
            for chunk in resp.iter_content(65536):
                buf += chunk
                if len(buf) >= max_bytes:
                    break
            return bytes(buf[:max_bytes])
        finally:
            resp.close()


def fetch_day(
    client: EdgarClient, day: date, days_dir: Path, max_per_day: int, max_bytes: int
) -> dict:
    """Save one day to disk: the selected events plus raw submission files."""
    all_events = parse_form_index(client.get_index(day))
    chosen = select_events(all_events, max_per_day)
    out = days_dir / day.isoformat()
    (out / "raw").mkdir(parents=True, exist_ok=True)
    failed: list[str] = []
    for e in chosen:
        target = out / "raw" / f"{e.accession}.txt"
        if target.exists():
            continue
        try:
            target.write_bytes(client.get_submission(e.url_path, max_bytes))
        except (requests.RequestException, RuntimeError):
            failed.append(e.accession)
    meta = {
        "date": day.isoformat(),
        "index_rows": len(all_events),
        "target_form_rows": sum(1 for e in all_events if e.form in TARGET_FORMS),
        "selected": len(chosen),
        "fetch_failed": failed,
        "max_bytes": max_bytes,
        "events": [json.loads(e.to_json()) for e in chosen],
    }
    (out / "index.json").write_text(json.dumps(meta, indent=1))
    return {k: v for k, v in meta.items() if k != "events"}
