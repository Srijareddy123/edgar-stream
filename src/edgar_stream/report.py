"""Static HTML dashboard built from the warehouse: freshness, throughput, duplicates, quarantine."""

from __future__ import annotations

import html
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from .pipeline import QUARANTINE
from .store import ObjectStore

CSS = """
:root{--bg:#f4efe6;--ink:#1d1b18;--mute:#6b645a;--rule:#d9d1c2;--accent:#b4411f;--ok:#2f6b4f;
--bar:#1d1b18;--bar2:#b4411f}
@media(prefers-color-scheme:dark){:root{--bg:#181613;--ink:#efe9dd;--mute:#a39a8b;--rule:#3a352e;
--accent:#e0714d;--ok:#6fbf9a;--bar:#efe9dd;--bar2:#e0714d}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);
font:16px/1.5 Georgia,'Times New Roman',serif}
main{max-width:1040px;margin:0 auto;padding:40px 16px 64px}
h1{font-size:clamp(2rem,5vw,3.2rem);line-height:1.05;margin:0 0 6px;letter-spacing:-.02em}
h2{font:600 .78rem ui-monospace,Menlo,monospace;text-transform:uppercase;letter-spacing:.12em;
color:var(--mute);margin:44px 0 12px;border-top:2px solid var(--ink);padding-top:10px}
.sub{color:var(--mute);margin:0 0 28px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:0;
border-top:1px solid var(--rule);border-bottom:1px solid var(--rule)}
.kpi{padding:16px 14px;border-right:1px solid var(--rule)}.kpi:last-child{border-right:0}
.kpi b{display:block;font:600 2.1rem/1 ui-monospace,Menlo,monospace;letter-spacing:-.03em}
.kpi.hot b{color:var(--accent)}.kpi span{font-size:.82rem;color:var(--mute)}
table{width:100%;border-collapse:collapse;font:.88rem ui-monospace,Menlo,monospace}
th,td{text-align:right;padding:7px 8px;border-bottom:1px solid var(--rule)}
th:first-child,td:first-child{text-align:left}th{color:var(--mute);font-weight:600}
.row{display:grid;grid-template-columns:minmax(120px,240px) 1fr 56px;gap:10px;
align-items:center;margin:5px 0;font:.85rem ui-monospace,Menlo,monospace}
.row i{display:block;height:14px;background:var(--bar2)}.row.k i{background:var(--bar)}
.row em{font-style:normal;text-align:right}.cols{display:grid;gap:32px;
grid-template-columns:repeat(auto-fit,minmax(300px,1fr))}
.ok{color:var(--ok)}.bad{color:var(--accent)}.note{color:var(--mute);font-size:.85rem}
"""


def _bars(rows: list[tuple[str, float]], cls: str = "") -> str:
    top = max((v for _, v in rows), default=0) or 1
    return (
        "".join(
            f'<div class="row {cls}"><span>{html.escape(str(k))}</span>'
            f'<div><i style="width:{max(1.0, 100 * v / top):.1f}%"></i></div><em>{v:g}</em></div>'
            for k, v in rows
        )
        or '<p class="note">none</p>'
    )


def _held_quarantine(store: ObjectStore) -> dict[str, int]:
    out: dict[str, int] = {}
    for key in store.list(QUARANTINE):
        if key.endswith(".json"):
            reason = json.loads(store.get(key))["reason"]
            out[reason] = out.get(reason, 0) + 1
    return out


def build_site(conn: duckdb.DuckDBPyConnection, store: ObjectStore, site_dir: Path) -> Path:
    runs = [json.loads(r[0]) for r in conn.execute(
        "select payload from run_metrics order by created_at").fetchall()]  # fmt: skip
    held = _held_quarantine(store)
    gates = conn.execute(
        "select count(*), count(*) filter (where not success), coalesce(sum(n_expectations),0) "
        "from dq_results where scope='batch_gate'"
    ).fetchone()
    wh_checks = conn.execute(
        "select target, n_expectations, n_failed from dq_results where scope='warehouse' "
        "qualify row_number() over (partition by target order by checked_at desc) = 1 order by 1"
    ).fetchall()
    star = _star_counts(conn)
    daily = _daily(conn)
    tot = {k: sum(r[k] for r in runs) for k in ("messages", "duplicates_dropped")}
    last = runs[-1] if runs else {}

    def kpi(v, label, hot=False):
        return f'<div class="kpi{" hot" if hot else ""}"><b>{v}</b><span>{label}</span></div>'

    run_rows = "".join(
        f"<tr><td>{html.escape(r['label'])}</td><td>{len(r['days'])}</td><td>{r['messages']}</td>"
        f"<td>{r['throughput_per_s']}</td><td>{r['freshness_ms_p50']}</td>"
        f"<td>{r['freshness_ms_p95']}</td><td>{r['duplicates_dropped']}</td>"
        f"<td>{r['quarantined']}</td></tr>"
        for r in runs
    )
    reasons = _bars(sorted(held.items(), key=lambda kv: -kv[1]))
    body = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Edgar Stream</title>
<style>{CSS}</style></head><body><main>
<h1>Filings, as they land</h1>
<p class="sub">Freshness, throughput and data quality for the EDGAR filings pipeline.
Built {datetime.now(UTC):%Y-%m-%d %H:%M} UTC from the warehouse, nothing typed in.</p>
<div class="kpis">
{kpi(f"{last.get('freshness_ms_p50', 0)} ms", "freshness p50, latest run")}
{kpi(f"{last.get('freshness_ms_p95', 0)} ms", "freshness p95, latest run")}
{kpi(f"{last.get('throughput_per_s', 0)}/s", "events per second, latest run")}
{kpi(tot["duplicates_dropped"], "duplicates dropped, all runs", True)}
{kpi(sum(held.values()), "files held in quarantine", True)}
{kpi(f"{gates[0] - gates[1]}/{gates[0]}", "load batches passing the quality gate")}
</div>
<h2>Runs</h2>
<table><tr><th>run</th><th>days</th><th>events</th><th>events/s</th><th>p50 ms</th>
<th>p95 ms</th><th>dupes dropped</th><th>quarantined</th></tr>{run_rows}</table>
<p class="note">Freshness is the time from the producer publishing an event to the filing being
queryable in DuckDB. Burst runs include queue wait; paced runs show steady state.</p>
<div class="cols"><div><h2>Filings per day</h2>{_bars(daily, "k")}</div>
<div><h2>Quarantine, by reason</h2>{reasons}</div></div>
<h2>Star schema</h2>{_bars(star, "k")}
<h2>Great Expectations on the warehouse</h2>
<table><tr><th>table</th><th>checks</th><th>failed</th></tr>
{
        "".join(
            f"<tr><td>star.{t}</td><td>{n}</td><td class='{'bad' if f else 'ok'}'>{f}</td></tr>"
            for t, n, f in wh_checks
        )
    }</table>
<p class="note">{gates[2]} expectation checks ran inside the load batches as well.</p>
</main></body></html>"""
    site_dir.mkdir(parents=True, exist_ok=True)
    out = site_dir / "index.html"
    out.write_text(body, encoding="utf-8")
    return out


def _star_counts(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, float]]:
    try:
        return [
            (t, conn.execute(f"select count(*) from star.{t}").fetchone()[0])
            for t in ("fact_filings", "fact_sections", "dim_companies", "dim_forms", "dim_dates")
        ]
    except duckdb.Error:
        return []


def _daily(conn: duckdb.DuckDBPyConnection) -> list[tuple[str, float]]:
    return [
        (str(d), n)
        for d, n in conn.execute(
            "select filed_date, count(*) from stg_filings group by 1 order by 1"
        ).fetchall()
    ]
