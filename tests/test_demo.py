import json

from edgar_stream import warehouse as wh
from edgar_stream.cli import main


def test_demo_runs_on_the_committed_sample(tmp_path, monkeypatch):
    monkeypatch.setenv("EDGAR_HOME", str(tmp_path))
    monkeypatch.setenv("WAREHOUSE_PATH", str(tmp_path / "w.duckdb"))
    monkeypatch.setenv("OBJECTS_DIR", str(tmp_path / "obj"))
    monkeypatch.setenv("SITE_DIR", str(tmp_path / "site"))
    assert main(["demo"]) == 0
    c = wh.connect(tmp_path / "w.duckdb")
    assert c.execute("select count(*) from star.fact_filings").fetchone()[0] > 10
    payload = json.loads(c.execute("select payload from run_metrics").fetchone()[0])
    assert payload["duplicates_dropped"] > 0
    assert payload["quarantined"] >= 1
    assert (tmp_path / "site" / "index.html").read_text().startswith("<!doctype html>")


def test_stack_demo_uses_its_own_database():
    import re
    from pathlib import Path

    make = (Path(__file__).parent.parent / "Makefile").read_text()
    recipe = make.split("stack-demo:")[1].split("\n\n")[0]
    assert "rm -f $(STACK_DB)" in recipe
    assert len(re.findall(r"WAREHOUSE_PATH=\$\(STACK_DB\)", recipe)) == 3
