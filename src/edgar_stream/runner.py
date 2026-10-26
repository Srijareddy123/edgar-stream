"""Run one measured replay: publish saved days to the bus and load them with the consumer."""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime

import duckdb
import numpy as np

from .bus import Bus
from .pipeline import Loader
from .producer import plan_stream, publish_all
from .replay import DaySource
from .store import ObjectStore

QUERYABLE = ("loaded", "loaded_older_version", "reprocessed")


def new_run_id(label: str) -> str:
    return f"{label}-{datetime.now(UTC):%Y%m%dT%H%M%S}"


def run_metrics(conn: duckdb.DuckDBPyConnection, run_id: str, wall_s: float) -> dict:
    counts = dict(
        conn.execute(
            "select outcome, count(*) from event_log where run_id=? group by 1", [run_id]
        ).fetchall()
    )
    lat = [
        r[0]
        for r in conn.execute(
            "select done_ts_ms - event_ts_ms from event_log where run_id=? and outcome in "
            "('loaded','loaded_older_version','reprocessed')",
            [run_id],
        ).fetchall()
    ]
    reasons = dict(
        conn.execute(
            "select reason, count(distinct accession || version) from event_log "
            "where run_id=? and outcome='quarantined' group by 1 order by 2 desc",
            [run_id],
        ).fetchall()
    )
    n_msgs = sum(counts.values())
    p50, p95 = (float(np.percentile(lat, q)) for q in (50, 95)) if lat else (0.0, 0.0)
    return {
        "run_id": run_id,
        "messages": n_msgs,
        "loaded": counts.get("loaded", 0),
        "loaded_older_version": counts.get("loaded_older_version", 0),
        "duplicates_dropped": counts.get("duplicate", 0),
        "quarantined": sum(reasons.values()),
        "quarantine_reasons": reasons,
        "wall_seconds": round(wall_s, 2),
        "throughput_per_s": round(n_msgs / wall_s, 1) if wall_s else 0.0,
        "freshness_ms_p50": round(p50),
        "freshness_ms_p95": round(p95),
    }


def run_days(
    *,
    days: list[str],
    source: DaySource,
    conn: duckdb.DuckDBPyConnection,
    store: ObjectStore,
    bus: Bus,
    label: str,
    rate_per_s: float | None = None,
    dup_rate: float = 0.0,
    shuffle_window: int = 0,
    seed: int = 7,
    batch_size: int = 50,
    params: dict | None = None,
) -> dict:
    events = [e for d in days for e in source.events(d)]
    stream = plan_stream(events, dup_rate, shuffle_window, seed)
    run_id = new_run_id(label)
    loader = Loader(conn, store, source, run_id)
    start = time.monotonic()
    producer = threading.Thread(target=publish_all, args=(bus, stream, rate_per_s))
    producer.start()
    loader.consume(bus, expected=len(stream), batch_size=batch_size)
    producer.join()
    metrics = run_metrics(conn, run_id, time.monotonic() - start)
    metrics.update({"label": label, "days": days, "params": params or {}})
    metrics["params"].update(
        {"rate_per_s": rate_per_s, "dup_rate": dup_rate, "shuffle_window": shuffle_window,
         "seed": seed, "batch_size": batch_size}
    )  # fmt: skip
    conn.execute(
        "insert into run_metrics (run_id, label, payload) values (?,?,?)",
        [run_id, label, json.dumps(metrics)],
    )
    return metrics
