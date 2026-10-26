# Results detail

Replay of 5 real EDGAR days (2026-10-19 to 2026-10-23, up to 200 filings sampled per day: 1000 filings plus 45 injected redeliveries = 1045 events) through Redpanda and MinIO on a laptop. Command: `make stack-up && make eval`, output in `results/replay_results.json`.

| Run | Events | Events/s | Freshness p50 | Freshness p95 | Duplicates dropped | Older version kept in history | Quarantined |
|---|---|---|---|---|---|---|---|
| Paced at 25 events/s | 1045 | 24.8 | 638 ms | 1840 ms | 45 | 2 | 2 |
| Burst, newest day first | 1045 | 36.2 | 14.3 s | 27.2 s | 45 | 6 | 2 |

- Freshness is event publish time to queryable in DuckDB. The burst run includes queue wait, so read the paced row as steady state.
- The 45 duplicates are 5 percent redelivery that the producer injects on purpose. The 2 quarantined files are real: `no_sections_found` (one 10-Q, one 8-K).
- Warehouse: 998 filing versions, 2924 sections, 839 companies, 6 forms, 317 dates. All 22 load batches passed Great Expectations, and 11 checks on the star tables passed after `dbt build` (21 nodes, 2.8 s).
- 285 of 998 filings were cut at 1.5 MB when downloaded (mostly 10-Q and 10-K), so their later sections are missing.
