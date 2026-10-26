"""Command line entry point: fetch, run, reprocess, transform, report, demo, measure."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

from . import warehouse as wh
from .bus import Bus, KafkaBus, MemoryBus
from .config import Settings, get_settings
from .edgar import EdgarClient, NoIndexForDay, fetch_day
from .pipeline import Loader
from .replay import DaySource
from .report import build_site
from .runner import new_run_id, run_days
from .store import LocalStore, ObjectStore, S3Store
from .transform import transform

SKIP_EXIT_CODE = 99  # Airflow BashOperator treats this as "skipped"


def _dates(args: argparse.Namespace) -> list[str]:
    if getattr(args, "dates", None):
        return list(args.dates)
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    return [(start + timedelta(n)).isoformat() for n in range((end - start).days + 1)]


def _store(s: Settings, kind: str) -> ObjectStore:
    if kind == "s3":
        return S3Store(s.s3_endpoint, s.s3_access_key, s.s3_secret_key, s.s3_bucket)
    return LocalStore(s.objects_dir)


def _bus(s: Settings, kind: str) -> Bus:
    return KafkaBus(s.kafka_bootstrap) if kind == "kafka" else MemoryBus()


def cmd_fetch(args: argparse.Namespace, s: Settings) -> int:
    client = EdgarClient(s.user_agent)
    status = SKIP_EXIT_CODE
    for d in _dates(args):
        try:
            info = fetch_day(client, date.fromisoformat(d), s.days_dir, args.max_per_day,
                             args.max_bytes)  # fmt: skip
            print(json.dumps(info))
            status = 0
        except NoIndexForDay:
            print(f"{d}: no daily index (weekend or holiday)")
    return status if len(_dates(args)) == 1 else 0


def _source(args: argparse.Namespace, s: Settings) -> DaySource:
    return DaySource(Path(args.days_dir) if args.days_dir else s.days_dir)


def cmd_run(args: argparse.Namespace, s: Settings) -> int:
    source = _source(args, s)
    days = _dates(args) if (args.dates or args.start) else source.available_days()
    if args.descending:
        days = sorted(days, reverse=True)
    missing = [d for d in days if d not in source.available_days()]
    if missing:
        print(f"no saved day for {missing}; run fetch first", file=sys.stderr)
        return SKIP_EXIT_CODE
    conn = wh.connect(s.warehouse_path)
    bus = _bus(s, args.bus)
    try:
        m = run_days(days=days, source=source, conn=conn, store=_store(s, args.store), bus=bus,
                     label=args.label, rate_per_s=args.rate, dup_rate=args.dup_rate,
                     shuffle_window=args.shuffle, batch_size=args.batch_size)  # fmt: skip
    finally:
        bus.close()
        conn.close()
    print(json.dumps(m, indent=1))
    return 0


def cmd_reprocess(args: argparse.Namespace, s: Settings) -> int:
    conn = wh.connect(s.warehouse_path)
    try:
        loader = Loader(conn, _store(s, args.store), _source(args, s), new_run_id("reprocess"))
        print(json.dumps(loader.reprocess_quarantine()))
    finally:
        conn.close()
    return 0


def cmd_transform(args: argparse.Namespace, s: Settings) -> int:
    print(json.dumps(transform(s.warehouse_path, new_run_id("transform"))))
    return 0


def cmd_report(args: argparse.Namespace, s: Settings) -> int:
    conn = wh.connect(s.warehouse_path)
    try:
        out = build_site(conn, _store(s, args.store), s.site_dir)
    finally:
        conn.close()
    print(f"dashboard written to {out}")
    return 0


def cmd_demo(args: argparse.Namespace, s: Settings) -> int:
    """Everything on the committed sample: no network, no keys, no containers."""
    for p in (s.warehouse_path, s.objects_dir):
        shutil.rmtree(p, ignore_errors=True) if p.is_dir() else p.unlink(missing_ok=True)
    args.days_dir, args.store, args.bus = "sample/days", "local", "memory"
    args.dates = args.start = None
    args.label, args.rate, args.descending = "demo", None, True
    args.dup_rate, args.shuffle, args.batch_size = 0.1, 10, 25
    if (code := cmd_run(args, s)) != 0:
        return code
    cmd_transform(args, s)
    return cmd_report(args, s)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="edgar-stream")
    sub = p.add_subparsers(dest="cmd", required=True)

    def dates(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--dates", nargs="*")
        sp.add_argument("--start")
        sp.add_argument("--end")

    f = sub.add_parser("fetch", help="save EDGAR days (index plus raw files) to data/days")
    dates(f)
    f.add_argument("--max-per-day", type=int, default=200)
    f.add_argument("--max-bytes", type=int, default=1_500_000)
    f.set_defaults(fn=cmd_fetch)

    r = sub.add_parser("run", help="publish saved days to the bus and load them")
    dates(r)
    r.add_argument("--days-dir")
    r.add_argument("--bus", choices=["memory", "kafka"], default="memory")
    r.add_argument("--store", choices=["local", "s3"], default="local")
    r.add_argument("--label", default="run")
    r.add_argument("--rate", type=float, default=None, help="events per second (default burst)")
    r.add_argument("--dup-rate", type=float, default=0.0)
    r.add_argument("--shuffle", type=int, default=0)
    r.add_argument("--batch-size", type=int, default=50)
    r.add_argument("--descending", action="store_true", help="replay newest day first")
    r.set_defaults(fn=cmd_run)

    q = sub.add_parser("reprocess", help="re-run quarantined files through the current parser")
    q.add_argument("--store", choices=["local", "s3"], default="local")
    q.add_argument("--days-dir")
    q.set_defaults(fn=cmd_reprocess)

    sub.add_parser("transform", help="dbt build plus Great Expectations").set_defaults(
        fn=cmd_transform
    )
    rp = sub.add_parser("report", help="write the HTML dashboard to site/")
    rp.add_argument("--store", choices=["local", "s3"], default="local")
    rp.set_defaults(fn=cmd_report)
    sub.add_parser("demo", help="run the committed sample end to end").set_defaults(fn=cmd_demo)
    return p


def main(argv: list[str] | None = None) -> int:
    os.environ.setdefault("TQDM_DISABLE", "1")
    args = build_parser().parse_args(argv)
    return args.fn(args, get_settings())


if __name__ == "__main__":
    raise SystemExit(main())
