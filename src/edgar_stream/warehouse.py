"""DuckDB staging tables and the idempotent, order-safe batch upsert."""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from .parse import ParsedFiling

SCHEMA = """
create table if not exists stg_filings (
  accession varchar, version integer, cik bigint, company varchar, form varchar,
  base_form varchar, is_amendment boolean, filed_date date, period date,
  acceptance_ts timestamp, sic varchar, state_inc varchar, fiscal_year_end varchar,
  items_declared varchar, n_docs integer, n_sections integer, n_chars bigint,
  truncated boolean, raw_bytes bigint, parser_version varchar, filing_key varchar,
  order_key varchar, loaded_ts_ms bigint, primary key (accession, version));
create table if not exists stg_sections (
  accession varchar, version integer, item varchar, title varchar, ord integer,
  n_chars integer, text varchar);
create table if not exists filing_current (
  filing_key varchar primary key, accession varchar, version integer, order_key varchar);
create table if not exists event_log (
  run_id varchar, accession varchar, version integer, form varchar, filed_date date,
  outcome varchar, reason varchar, event_ts_ms bigint, done_ts_ms bigint);
create table if not exists dq_results (
  run_id varchar, scope varchar, target varchar, n_expectations integer, n_failed integer,
  success boolean, checked_at timestamp default current_timestamp);
create table if not exists run_metrics (
  run_id varchar primary key, label varchar, payload json,
  created_at timestamp default current_timestamp);
"""

LOADED, STALE, DUPLICATE = "loaded", "loaded_older_version", "duplicate"


@dataclass(frozen=True)
class Applied:
    key: str
    outcome: str  # loaded, loaded_older_version or duplicate
    done_ts_ms: int


def connect(path: Path | str, memory_limit: str = "512MB") -> duckdb.DuckDBPyConnection:
    if str(path) != ":memory:":
        Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    conn.execute(f"set memory_limit='{memory_limit}'")
    conn.execute("set threads=2")
    conn.execute(SCHEMA)
    return conn


def known_keys(conn: duckdb.DuckDBPyConnection, keys: list[str]) -> set[str]:
    if not keys:
        return set()
    rows = conn.execute(
        "select accession || ':' || version from stg_filings "
        "where accession || ':' || version in (select unnest(?))",
        [keys],
    ).fetchall()
    return {r[0] for r in rows}


def _row(p: ParsedFiling, now_ms: int) -> tuple:
    return (
        p.accession, p.version, p.cik, p.company, p.form, p.base_form, p.version > 0,
        p.filed_date, p.period, p.acceptance_ts, p.sic, p.state_inc, p.fiscal_year_end,
        p.items_declared, p.n_docs, len(p.sections), sum(s.n_chars for s in p.sections),
        p.truncated, p.raw_bytes, p.parser_version, p.filing_key, p.order_key, now_ms,
    )  # fmt: skip


def apply_batch(conn: duckdb.DuckDBPyConnection, batch: list[ParsedFiling]) -> list[Applied]:
    """Load filings in one transaction.

    A filing already stored (same accession and version) is dropped as a duplicate. A filing
    with an older order key than the current one for its logical filing is kept in history
    but never replaces the current pointer, so arrival order does not matter.
    """
    results: list[Applied] = []
    seen = known_keys(conn, [p.key for p in batch])
    conn.execute("begin")
    try:
        for p in batch:
            now = int(time.time() * 1000)
            if p.key in seen:
                results.append(Applied(p.key, DUPLICATE, now))
                continue
            seen.add(p.key)
            conn.execute(f"insert into stg_filings values ({','.join('?' * 23)})", _row(p, now))
            if p.sections:
                sec_df = pd.DataFrame(
                    [(p.accession, p.version, s.item, s.title, s.ord, s.n_chars, s.text)
                     for s in p.sections],
                    columns=["accession", "version", "item", "title", "ord", "n_chars", "text"],
                )  # fmt: skip
                conn.register("sec_df", sec_df)
                conn.execute("insert into stg_sections select * from sec_df")
                conn.unregister("sec_df")
            cur = conn.execute(
                "select order_key from filing_current where filing_key = ?", [p.filing_key]
            ).fetchone()
            if cur is None:
                conn.execute(
                    "insert into filing_current values (?,?,?,?)",
                    [p.filing_key, p.accession, p.version, p.order_key],
                )
                outcome = LOADED
            elif p.order_key > cur[0]:
                conn.execute(
                    "update filing_current set accession=?, version=?, order_key=? "
                    "where filing_key=?",
                    [p.accession, p.version, p.order_key, p.filing_key],
                )
                outcome = LOADED
            else:
                outcome = STALE
            results.append(Applied(p.key, outcome, int(time.time() * 1000)))
        conn.execute("commit")
    except Exception:
        conn.execute("rollback")
        raise
    done = int(time.time() * 1000)  # rows are queryable once the commit returns
    return [Applied(r.key, r.outcome, done) for r in results]


def log_events(conn: duckdb.DuckDBPyConnection, rows: list[tuple]) -> None:
    """rows: run_id, accession, version, form, filed_date, outcome, reason, event_ms, done_ms."""
    if rows:
        conn.executemany("insert into event_log values (?,?,?,?,?,?,?,?,?)", rows)
