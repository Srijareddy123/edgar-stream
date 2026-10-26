"""Streaming ingestion of SEC EDGAR filings into a DuckDB star schema."""

import os

os.environ.setdefault("TQDM_DISABLE", "1")  # keep Great Expectations progress bars quiet

__version__ = "0.1.0"
