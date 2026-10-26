"""Airflow DAGs: a daily run and a date-range backfill. Tasks call the edgar-stream CLI.

The CLI runs in its own virtualenv (EDGAR_BIN) so its pinned packages stay apart from Airflow's.
Exit code 99 means "nothing to do for this date" (weekend, holiday) and marks the task skipped.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

from airflow import DAG

BIN = os.environ.get("EDGAR_BIN", "edgar-stream")
SKIP = 99
RETRIES = {"retries": 3, "retry_delay": timedelta(seconds=30), "retry_exponential_backoff": True}
BUS = os.environ.get("EDGAR_BUS", "kafka")
STORE = os.environ.get("EDGAR_STORE", "s3")


def fetch(dates: str) -> str:
    """Live mode downloads the days from EDGAR. Replay mode uses days already on disk."""
    return f'if [ "${{EDGAR_MODE:-replay}}" = live ]; then {BIN} fetch {dates}; fi'


def tasks(dag: DAG, dates: str, run_flags: str = "") -> None:
    t_fetch = BashOperator(
        task_id="fetch_days", bash_command=fetch(dates), skip_on_exit_code=SKIP, dag=dag
    )
    t_run = BashOperator(
        task_id="stream_and_load",
        bash_command=f"{BIN} run {dates} --bus {BUS} --store {STORE} --label airflow {run_flags}",
        skip_on_exit_code=SKIP,
        dag=dag,
    )
    t_dbt = BashOperator(task_id="dbt_and_expectations", bash_command=f"{BIN} transform", dag=dag)
    t_report = BashOperator(
        task_id="dashboard", bash_command=f"{BIN} report --store {STORE}", dag=dag
    )
    t_fetch >> t_run >> t_dbt >> t_report


with DAG(
    dag_id="edgar_daily",
    description="Load one day of EDGAR filings: fetch, stream, load, dbt, checks, dashboard",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # DuckDB allows one writer
    default_args=RETRIES,
    tags=["edgar"],
) as daily:
    tasks(daily, "--dates {{ ds }}")

with DAG(
    dag_id="edgar_backfill",
    description="Backfill a date range, newest day first so late days arrive out of order",
    schedule=None,
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=RETRIES,
    params={
        "start": Param("2026-10-19", type="string", description="first day, YYYY-MM-DD"),
        "end": Param("2026-10-23", type="string", description="last day, YYYY-MM-DD"),
    },
    tags=["edgar"],
) as backfill:
    tasks(backfill, "--start {{ params.start }} --end {{ params.end }}", "--descending")
