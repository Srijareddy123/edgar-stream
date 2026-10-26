"""Build the star schema with dbt, then check the result with Great Expectations."""

from __future__ import annotations

import os
import time
from pathlib import Path

from . import warehouse as wh
from .quality import Validator, warehouse_suite

DBT_DIR = Path(__file__).resolve().parents[2] / "dbt"


class TransformError(Exception):
    pass


def run_dbt(warehouse_path: Path) -> dict:
    """Run `dbt build` (models and tests). Returns timing and node count."""
    from dbt.cli.main import dbtRunner

    os.environ["WAREHOUSE_PATH"] = str(Path(warehouse_path).resolve())
    start = time.monotonic()
    res = dbtRunner().invoke(
        ["build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "-q"]
    )
    nodes = {r.node.name: str(r.status) for r in (res.result.results if res.result else [])}
    if not res.success:
        bad = {k: v for k, v in nodes.items() if v not in ("success", "pass")}
        raise TransformError(f"dbt build failed: {bad or res.exception}")
    return {"seconds": round(time.monotonic() - start, 2), "nodes": len(nodes)}


def check_warehouse(conn, run_id: str) -> list[dict]:
    """Great Expectations on each star table. Results are stored in dq_results."""
    validator, out = Validator(), []
    for table, expectations in warehouse_suite().items():
        df = conn.execute(f"select * from star.{table}").df()
        res = validator.run(df, expectations)
        failed = sum(1 for r in res.results if not r.success)
        conn.execute(
            "insert into dq_results (run_id, scope, target, n_expectations, n_failed, success) "
            "values (?,?,?,?,?,?)",
            [run_id, "warehouse", table, len(res.results), failed, failed == 0],
        )
        out.append({"table": table, "expectations": len(res.results), "failed": failed})
    return out


def transform(warehouse_path: Path, run_id: str) -> dict:
    dbt = run_dbt(warehouse_path)
    conn = wh.connect(warehouse_path)
    try:
        ge = check_warehouse(conn, run_id)
    finally:
        conn.close()
    if any(t["failed"] for t in ge):
        raise TransformError(f"Great Expectations failed: {ge}")
    return {"dbt": dbt, "great_expectations": ge}
