"""Consumer side: fetch, parse, quality-gate, land, quarantine and load micro-batches."""

from __future__ import annotations

import json
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime

import duckdb

from . import warehouse as wh
from .bus import Bus, Message
from .events import FilingEvent
from .parse import PARSER_VERSION, ParsedFiling, ParseError, parse_submission
from .quality import Validator, gate_batch
from .replay import DaySource, MissingRaw
from .store import ObjectStore

RAW, QUARANTINE = "raw", "quarantine"
LINGER_S = 0.2  # how long a micro-batch waits for more events before it loads


def raw_key(day: str, accession: str) -> str:
    return f"{RAW}/dt={day}/{accession}.txt"


def quarantine_key(day: str, accession: str, ext: str) -> str:
    return f"{QUARANTINE}/dt={day}/{accession}.{ext}"


def _row_for_gate(p: ParsedFiling) -> dict:
    return {
        "event_key": p.key, "accession": p.accession, "cik": p.cik, "company": p.company,
        "form": p.form, "filed_date": p.filed_date, "n_sections": len(p.sections),
        "n_chars": sum(s.n_chars for s in p.sections),
    }  # fmt: skip


@dataclass
class Stats:
    outcomes: Counter = field(default_factory=Counter)
    reasons: Counter = field(default_factory=Counter)
    batches: int = 0


class Loader:
    def __init__(
        self,
        conn: duckdb.DuckDBPyConnection,
        store: ObjectStore,
        source: DaySource,
        run_id: str,
        validator: Validator | None = None,
    ) -> None:
        self.conn, self.store, self.source, self.run_id = conn, store, source, run_id
        self.validator = validator or Validator()
        self.stats = Stats()

    # quarantine -----------------------------------------------------------------------
    def _quarantine(self, event: FilingEvent, raw: bytes | None, reason: str, detail: str) -> None:
        if raw is not None:
            self.store.put(quarantine_key(event.filed_date, event.accession, "txt"), raw)
        meta = {
            "reason": reason,
            "detail": detail,
            "event": json.loads(event.to_json()),
            "parser_version": PARSER_VERSION,
            "quarantined_at": datetime.now(UTC).isoformat(),
            "has_raw": raw is not None,
        }
        self.store.put(
            quarantine_key(event.filed_date, event.accession, "json"),
            json.dumps(meta, indent=1).encode(),
        )

    # one micro-batch --------------------------------------------------------------------
    def load_events(
        self, items: list[tuple[FilingEvent, bytes | None]], reprocess: bool = False
    ) -> None:
        """items: (event, raw bytes or None when the raw file could not be fetched)."""
        log: list[tuple] = []
        parsed: dict[str, tuple[FilingEvent, ParsedFiling, bytes]] = {}
        in_batch: set[str] = set()
        known = wh.known_keys(self.conn, [e.key for e, _ in items])

        def record(e: FilingEvent, outcome: str, reason: str | None, done: int | None = None):
            self.stats.outcomes[outcome] += 1
            if reason:
                self.stats.reasons[reason] += 1
            log.append((self.run_id, e.accession, e.version, e.form, e.filed_date, outcome,
                        reason, e.event_ts_ms, done or int(time.time() * 1000)))  # fmt: skip

        for event, raw in items:
            if event.key in known or event.key in in_batch:
                record(event, wh.DUPLICATE, None)
                continue
            in_batch.add(event.key)
            if raw is None:
                self._quarantine(event, None, "fetch_failed", "raw file missing")
                record(event, "quarantined", "fetch_failed")
                continue
            try:
                parsed[event.key] = (event, parse_submission(raw, event), raw)
            except ParseError as exc:
                self._quarantine(event, raw, exc.reason, exc.detail)
                record(event, "quarantined", exc.reason)

        gate = gate_batch(self.validator, [_row_for_gate(p) for _, p, _ in parsed.values()])
        self.conn.execute(
            "insert into dq_results (run_id, scope, target, n_expectations, n_failed, success) "
            "values (?,?,?,?,?,?)",
            [self.run_id, "batch_gate", "stg_filings", gate.n_expectations, gate.n_failed,
             gate.success],
        )  # fmt: skip
        good: list[ParsedFiling] = []
        for key, (event, p, raw) in parsed.items():
            if key in gate.bad_rows:
                reason = "ge:" + gate.bad_rows[key][0]
                self._quarantine(event, raw, reason, "; ".join(gate.bad_rows[key]))
                record(event, "quarantined", reason)
            else:
                self.store.put(raw_key(event.filed_date, event.accession), raw)
                good.append(p)

        by_key = {p.key: parsed[p.key][0] for p in good}
        for applied in wh.apply_batch(self.conn, good):
            outcome = applied.outcome
            if reprocess and outcome == wh.LOADED:
                outcome = "reprocessed"
            record(by_key[applied.key], outcome, None, applied.done_ts_ms)
        wh.log_events(self.conn, log)
        self.stats.batches += 1

    # consume loop -----------------------------------------------------------------------
    def process_messages(self, msgs: list[Message]) -> None:
        items: list[tuple[FilingEvent, bytes | None]] = []
        for m in msgs:
            try:
                items.append((m.event, self.source.get(m.event)))
            except MissingRaw:
                items.append((m.event, None))
        self.load_events(items)

    def consume(
        self, bus: Bus, expected: int, batch_size: int = 50, idle_timeout_s: float = 20.0
    ) -> None:
        """Poll until `expected` messages (duplicates included) were handled or the bus is idle."""
        handled, last_data = 0, time.monotonic()
        while handled < expected and time.monotonic() - last_data < idle_timeout_s:
            msgs = bus.poll(batch_size, LINGER_S)
            if not msgs:
                continue
            last_data = time.monotonic()
            self.process_messages(msgs)
            bus.commit(msgs)
            handled += len(msgs)

    def _try_source(self, event: FilingEvent) -> bytes | None:
        try:
            return self.source.get(event)
        except MissingRaw:
            return None

    # reprocess ----------------------------------------------------------------------------
    def reprocess_quarantine(self, prefix: str = QUARANTINE, batch_size: int = 50) -> dict:
        """Re-run quarantined files through the current parser; delete only what loads."""
        metas = [k for k in self.store.list(prefix) if k.endswith(".json")]
        before = len(metas)
        for i in range(0, len(metas), batch_size):
            chunk, items, keys = metas[i : i + batch_size], [], []
            for mk in chunk:
                meta = json.loads(self.store.get(mk))
                event = FilingEvent(**meta["event"])
                txt = mk[:-5] + ".txt"
                held = meta.get("has_raw") and self.store.exists(txt)
                raw = self.store.get(txt) if held else self._try_source(event)
                items.append((event, raw))
                keys.append((mk, txt))
            self.load_events(items, reprocess=True)
            for (mk, txt), (event, _) in zip(keys, items, strict=True):
                if event.key in wh.known_keys(self.conn, [event.key]):
                    self.store.delete(mk)
                    self.store.delete(txt)
                else:  # still failing: keep the files (load_events rewrote the reason)
                    pass
        after = len([k for k in self.store.list(prefix) if k.endswith(".json")])
        return {"quarantined_before": before, "quarantined_after": after}
