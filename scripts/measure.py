"""Measured replay of real EDGAR days through Redpanda and MinIO.

Writes results/replay_results.json.

Needs the stack running (`make stack-up`) and EDGAR_USER_AGENT in .env when days are not cached.
Run: python scripts/measure.py --days 2026-10-19 2026-10-20 2026-10-21 2026-10-22 2026-10-23
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / "data" / "eval"
PACED_RATE = 25.0


def cli(*args: str, env: dict | None = None) -> str:
    out = subprocess.run(
        [sys.executable, "-m", "edgar_stream.cli", *args],
        check=True, capture_output=True, text=True, cwd=ROOT, env={**os.environ, **(env or {})},
    )  # fmt: skip
    return out.stdout


def payloads(path: Path) -> list[dict]:
    c = duckdb.connect(str(path), read_only=True)
    try:
        return [json.loads(r[0]) for r in c.execute("select payload from run_metrics").fetchall()]
    finally:
        c.close()


def day_facts(days: list[str]) -> list[dict]:
    facts = []
    for d in days:
        meta = json.loads((ROOT / "data" / "days" / d / "index.json").read_text())
        facts.append({k: meta[k] for k in ("date", "index_rows", "target_form_rows", "selected")})
        facts[-1]["fetch_failed"] = len(meta["fetch_failed"])
    return facts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--max-per-day", type=int, default=200)
    args = ap.parse_args()

    missing = [d for d in args.days if not (ROOT / "data" / "days" / d / "index.json").exists()]
    if missing:
        cli("fetch", "--dates", *missing, "--max-per-day", str(args.max_per_day))
    shutil.rmtree(WORK, ignore_errors=True)
    WORK.mkdir(parents=True)
    common = ["--bus", "kafka", "--store", "s3", "--dup-rate", "0.05", "--shuffle", "20"]
    paced_env = {"WAREHOUSE_PATH": str(WORK / "paced.duckdb"), "S3_BUCKET": "edgar-eval-paced"}
    main_env = {"WAREHOUSE_PATH": str(WORK / "burst.duckdb"), "S3_BUCKET": "edgar-eval-burst"}

    cli("run", "--dates", *args.days, "--label", "paced", "--rate", str(PACED_RATE), *common,
        env=paced_env)  # fmt: skip
    cli("run", "--dates", *args.days, "--label", "burst", "--descending", *common, env=main_env)
    paced = payloads(WORK / "paced.duckdb")[0]
    c = duckdb.connect(str(WORK / "burst.duckdb"))
    c.execute("insert into run_metrics (run_id, label, payload) values (?,?,?)",
              [paced["run_id"], "paced", json.dumps(paced)])  # fmt: skip
    c.close()
    transform = json.loads(cli("transform", env=main_env))
    held = json.loads(cli("reprocess", "--store", "s3", env=main_env))
    cli("report", "--store", "s3", env={**main_env, "SITE_DIR": str(ROOT / "site")})

    c = duckdb.connect(str(WORK / "burst.duckdb"), read_only=True)
    star = {
        t: c.execute(f"select count(*) from star.{t}").fetchone()[0]
        for t in ("fact_filings", "fact_sections", "dim_companies", "dim_forms", "dim_dates")
    }
    stale = c.execute("select count(*) from star.fact_filings where not is_latest").fetchone()[0]
    gates = c.execute(
        "select count(*), count(*) filter (where not success) from dq_results "
        "where scope='batch_gate'"
    ).fetchone()
    c.close()
    mem = subprocess.run(
        ["docker", "stats", "--no-stream", "--format", "{{.Name}}: {{.MemUsage}}",
         "edgar-stream-redpanda-1", "edgar-stream-minio-1"],
        capture_output=True, text=True, check=False,
    ).stdout.split("\n")  # fmt: skip
    result = {
        "container_memory_after_run": [ln for ln in mem if ln],
        "days": day_facts(args.days),
        "stream": {"max_bytes_per_filing": 1_500_000, "paced_rate_per_s": PACED_RATE},
        "runs": [p for p in payloads(WORK / "burst.duckdb")],
        "star_rows": star,
        "older_versions_kept_in_history": stale,
        "quality_gate_batches": {"total": gates[0], "failed": gates[1]},
        "dbt_and_great_expectations": transform,
        "reprocess_of_quarantine": held,
    }
    out = ROOT / "results" / "replay_results.json"
    out.write_text(json.dumps(result, indent=1) + "\n")
    print(json.dumps(result, indent=1))


if __name__ == "__main__":
    main()
