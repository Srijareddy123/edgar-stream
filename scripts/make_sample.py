"""Build the small committed replay sample from days saved with `edgar-stream fetch`.

It keeps a real original and amendment pair (so a replay can deliver them out of order), a set
of small real 8-K filings, and three labelled faults used to show the quarantine path.
Run: python scripts/make_sample.py
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path

from edgar_stream.events import FilingEvent
from edgar_stream.parse import ParseError, parse_submission

ROOT = Path(__file__).resolve().parents[1]
SRC, DST = ROOT / "data" / "days", ROOT / "sample" / "days"
PAIR = ("0001104659-26-116990", "0001104659-26-117835")  # an 8-K and its 8-K/A
MAX_RAW = 150_000
N_PLAIN = 26
# Strings that must never reach the committed sample, written in pieces so this file stays clean.
BANNED = tuple(
    "".join(parts)
    for parts in (("S&", "P"), ("KP", "IT"), ("Sri", "ja"), ("@gm", "ail.com"), ("/Us", "ers/"))
)


def load_all() -> dict[str, tuple[FilingEvent, Path]]:
    out = {}
    for idx in sorted(SRC.glob("*/index.json")):
        for e in json.loads(idx.read_text())["events"]:
            ev = FilingEvent(**e)
            raw = idx.parent / "raw" / f"{ev.accession}.txt"
            if raw.exists():
                out[ev.accession] = (ev, raw)
    return out


def clean(path: Path) -> bool:
    text = path.read_text(errors="replace")
    return not any(b in text for b in BANNED)


def main() -> None:
    allf = load_all()
    chosen: list[str] = list(PAIR)
    for acc, (ev, path) in sorted(allf.items()):
        if len(chosen) >= N_PLAIN + len(PAIR):
            break
        if acc in chosen or ev.form != "8-K" or path.stat().st_size > MAX_RAW:
            continue
        try:
            parse_submission(path.read_bytes(), ev)
        except ParseError:
            continue
        if clean(path):
            chosen.append(acc)
    assert all(clean(allf[a][1]) for a in PAIR), "pair contains a banned string"

    shutil.rmtree(DST, ignore_errors=True)
    by_day: dict[str, list[FilingEvent]] = {}
    for acc in chosen:
        ev, path = allf[acc]
        (DST / ev.filed_date / "raw").mkdir(parents=True, exist_ok=True)
        shutil.copy(path, DST / ev.filed_date / "raw" / path.name)
        by_day.setdefault(ev.filed_date, []).append(ev)

    # Faults, applied to copies of real files and labelled in faults.json.
    plain = [a for a in chosen if a not in PAIR]
    faults = {}
    day = sorted(by_day)[-1]
    victims = [e for e in by_day[day] if e.accession in plain][:3]
    kinds = ["cik_mismatch", "empty_file", "fetch_failed"]
    for ev, kind in zip(victims, kinds, strict=True):
        raw = DST / day / "raw" / f"{ev.accession}.txt"
        if kind == "cik_mismatch":
            text = raw.read_text(errors="replace")
            raw.write_text(re.sub(r"(CENTRAL INDEX KEY:\s+)\d+", r"\g<1>0000000042", text, count=1))
        elif kind == "empty_file":
            raw.write_bytes(b"")
        else:
            raw.unlink()
        faults[ev.accession] = kind
    for d, events in by_day.items():
        meta = {"date": d, "events": [json.loads(e.to_json()) for e in events]}
        (DST / d / "index.json").write_text(json.dumps(meta, indent=1))
    (ROOT / "sample" / "faults.json").write_text(json.dumps(faults, indent=1) + "\n")
    size = sum(p.stat().st_size for p in DST.rglob("*") if p.is_file())
    print(f"{len(chosen)} filings on {sorted(by_day)}, {size / 1e6:.1f} MB, faults {faults}")


if __name__ == "__main__":
    main()
