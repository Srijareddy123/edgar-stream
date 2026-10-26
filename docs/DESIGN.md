# Design detail

## Pipeline

The index gives one event per filing. The consumer fetches the file, parses the SEC header and `Item` sections, runs a Great Expectations gate on the batch, lands the raw file in MinIO by filing date, and commits the rows to DuckDB in one transaction. Kafka offsets are committed only after that commit. Rows that fail parsing or the gate go to `quarantine/` with a JSON file holding the reason. `make reprocess` runs them through the current parser, and files are deleted from quarantine only after they load. Airflow runs the daily DAG and a backfill DAG over a date range, with 3 retries.

## Choices

- **Dedupe key is accession plus version.** A repeated event is dropped. A filing is also versioned by form: an amended filing (10-K/A) is a newer version of the same company, form and period. Older versions stay in history, but the current pointer only moves forward, so arrival order does not matter (property tests check this).
- **8-Ks have no period, so they match on company, event date and item list.** I first keyed them on company and date only and found different 8-Ks filed on the same day were treated as versions of each other.
- **DuckDB instead of PySpark.** The data is a few thousand rows per day. DuckDB runs the transforms in one process and keeps memory under a few hundred MB next to the other services.
- **Great Expectations runs twice:** on each load batch before commit (bad rows are quarantined) and on the star tables after dbt.
- **A parser bug I found by replaying real days:** headings written with thin spaces (`Item&#8201;1.01`) did not match, so valid 8-Ks went to quarantine. The fix is in `parse.py` and has a test.
