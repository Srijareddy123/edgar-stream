"""Great Expectations checks: a gate on every load batch and a check on the warehouse tables."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from great_expectations.data_context.types.base import ProgressBarsConfig

from .events import TARGET_FORMS

logging.getLogger("great_expectations").setLevel(logging.WARNING)

DATE_RE = r"^\d{4}-\d{2}-\d{2}$"
ACCESSION_RE = r"^\d{10}-\d{2}-\d{6}$"


@dataclass
class GateResult:
    n_expectations: int
    n_failed: int
    bad_rows: dict[str, list[str]] = field(default_factory=dict)  # event key -> failed checks

    @property
    def success(self) -> bool:
        return self.n_failed == 0


def batch_suite() -> list:
    return [
        gxe.ExpectColumnValuesToNotBeNull(column="accession"),
        gxe.ExpectColumnValuesToMatchRegex(column="accession", regex=ACCESSION_RE),
        gxe.ExpectColumnValuesToBeUnique(column="event_key"),
        gxe.ExpectColumnValuesToBeBetween(column="cik", min_value=1, max_value=9_999_999_999),
        gxe.ExpectColumnValuesToBeInSet(column="form", value_set=list(TARGET_FORMS)),
        gxe.ExpectColumnValuesToNotBeNull(column="company"),
        gxe.ExpectColumnValuesToMatchRegex(column="filed_date", regex=DATE_RE),
        gxe.ExpectColumnValuesToBeBetween(column="n_sections", min_value=1),
        gxe.ExpectColumnValuesToBeBetween(column="n_chars", min_value=1),
    ]


class Validator:
    """Holds one ephemeral GE context and validates dataframes against a list of expectations."""

    def __init__(self) -> None:
        logging.getLogger("great_expectations").setLevel(logging.WARNING)
        self._ctx = gx.get_context(mode="ephemeral")
        self._ctx.variables.progress_bars = ProgressBarsConfig(globally=False)
        src = self._ctx.data_sources.add_pandas("edgar")
        self._asset = src.add_dataframe_asset("frame")
        self._defn = self._asset.add_batch_definition_whole_dataframe("whole")
        self._n = 0

    def run(self, df: pd.DataFrame, expectations: list, key_column: str | None = None):
        self._n += 1
        suite = self._ctx.suites.add(
            gx.ExpectationSuite(name=f"s{self._n}", expectations=list(expectations))
        )
        batch = self._defn.get_batch(batch_parameters={"dataframe": df})
        return batch.validate(suite, result_format={"result_format": "COMPLETE"})


def gate_batch(validator: Validator, rows: list[dict]) -> GateResult:
    """Validate parsed filing rows. Rows that break a check are named so they can be quarantined."""
    if not rows:
        return GateResult(0, 0)
    df = pd.DataFrame(rows)
    res = validator.run(df, batch_suite())
    bad: dict[str, list[str]] = {}
    failed = 0
    for r in res.results:
        if r.success:
            continue
        failed += 1
        name = f"{r.expectation_config.type}:{r.expectation_config.kwargs.get('column')}"
        idx = r.result.get("unexpected_index_list") or []
        if not idx and r.result.get("unexpected_count", 0):
            idx = list(df.index)  # table-wide failure: no row can be trusted
        for i in idx:
            bad.setdefault(df.loc[i, "event_key"], []).append(name)
    return GateResult(len(res.results), failed, bad)


def warehouse_suite() -> dict[str, list]:
    """Checks on the star schema tables after dbt has built them."""
    return {
        "fact_filings": [
            gxe.ExpectTableRowCountToBeBetween(min_value=1),
            gxe.ExpectColumnValuesToNotBeNull(column="filing_sk"),
            gxe.ExpectColumnValuesToBeUnique(column="filing_sk"),
            gxe.ExpectColumnValuesToNotBeNull(column="company_key"),
            gxe.ExpectColumnValuesToNotBeNull(column="filed_date_key"),
            gxe.ExpectColumnValuesToBeBetween(column="n_sections", min_value=1),
        ],
        "dim_companies": [
            gxe.ExpectColumnValuesToBeUnique(column="company_key"),
            gxe.ExpectColumnValuesToNotBeNull(column="company_name"),
        ],
        "dim_forms": [gxe.ExpectColumnValuesToBeUnique(column="form")],
        "dim_dates": [
            gxe.ExpectColumnValuesToBeUnique(column="date_key"),
            gxe.ExpectColumnValuesToBeBetween(column="month", min_value=1, max_value=12),
        ],
    }
