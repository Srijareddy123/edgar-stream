import json

import pytest
from conftest import make_event, make_submission, write_day

from edgar_stream import pipeline
from edgar_stream import warehouse as wh
from edgar_stream.parse import parse_submission as real_parse
from edgar_stream.pipeline import Loader, quarantine_key, raw_key


def loader(conn, store, source, validator):
    return Loader(conn, store, source, "test", validator)


def test_good_file_lands_in_raw_and_bad_file_in_quarantine_with_reason(
    tmp_path, conn, store, validator
):
    good, bad = make_event("0000001234-26-000001"), make_event("0000001234-26-000002")
    src = write_day(
        tmp_path / "days", "2026-01-05",
        [(good, make_submission(good.accession)),
         (bad, make_submission(bad.accession, body="<div>nothing</div>"))],
    )  # fmt: skip
    ld = loader(conn, store, src, validator)
    ld.load_events([(good, src.get(good)), (bad, src.get(bad))])
    assert store.exists(raw_key("2026-01-05", good.accession))
    assert not store.exists(raw_key("2026-01-05", bad.accession))
    meta = json.loads(store.get(quarantine_key("2026-01-05", bad.accession, "json")))
    assert meta["reason"] == "no_sections_found"
    assert store.exists(quarantine_key("2026-01-05", bad.accession, "txt"))


def test_missing_raw_file_is_quarantined_as_fetch_failed(tmp_path, conn, store, validator):
    e = make_event("0000001234-26-000003")
    src = write_day(tmp_path / "days", "2026-01-05", [(e, None)])
    ld = loader(conn, store, src, validator)
    ld.process_messages([pipeline.Message(e)])
    meta = json.loads(store.get(quarantine_key("2026-01-05", e.accession, "json")))
    assert meta["reason"] == "fetch_failed" and meta["has_raw"] is False


def test_reprocess_after_parser_fix_loses_no_data(tmp_path, conn, store, validator, monkeypatch):
    e = make_event("0000001234-26-000004")
    raw = make_submission(e.accession)
    src = write_day(tmp_path / "days", "2026-01-05", [(e, raw)])
    ld = loader(conn, store, src, validator)

    def broken(raw_bytes, event):  # a parser bug that rejects everything
        from edgar_stream.parse import ParseError

        raise ParseError("no_sections_found", "bug")

    monkeypatch.setattr(pipeline, "parse_submission", broken)
    ld.load_events([(e, raw)])
    assert conn.execute("select count(*) from stg_filings").fetchone()[0] == 0
    assert len(store.list("quarantine")) == 2

    monkeypatch.setattr(pipeline, "parse_submission", real_parse)  # the fix
    result = ld.reprocess_quarantine()
    assert result == {"quarantined_before": 1, "quarantined_after": 0}
    assert conn.execute("select count(*) from stg_filings").fetchone()[0] == 1
    assert store.get(raw_key("2026-01-05", e.accession)) == raw
    assert store.list("quarantine") == []


def test_reprocess_keeps_files_that_still_fail(tmp_path, conn, store, validator):
    e = make_event("0000001234-26-000005")
    raw = make_submission(e.accession, body="<div>nothing</div>")
    src = write_day(tmp_path / "days", "2026-01-05", [(e, raw)])
    ld = loader(conn, store, src, validator)
    ld.load_events([(e, raw)])
    assert ld.reprocess_quarantine()["quarantined_after"] == 1
    assert store.exists(quarantine_key("2026-01-05", e.accession, "txt"))


def test_a_row_failing_great_expectations_is_quarantined_not_loaded(
    tmp_path, conn, store, validator, monkeypatch
):
    e = make_event("0000001234-26-000006")
    raw = make_submission(e.accession)
    src = write_day(tmp_path / "days", "2026-01-05", [(e, raw)])

    original = pipeline._row_for_gate

    def odd_row(p):  # zero characters breaks the n_chars expectation
        return {**original(p), "n_chars": 0}

    monkeypatch.setattr(pipeline, "_row_for_gate", odd_row)
    ld = loader(conn, store, src, validator)
    ld.load_events([(e, raw)])
    meta = json.loads(store.get(quarantine_key("2026-01-05", e.accession, "json")))
    assert meta["reason"].startswith("ge:")
    assert conn.execute("select count(*) from stg_filings").fetchone()[0] == 0


def test_duplicate_event_is_dropped_and_not_reloaded(tmp_path, conn, store, validator):
    e = make_event("0000001234-26-000007")
    raw = make_submission(e.accession)
    src = write_day(tmp_path / "days", "2026-01-05", [(e, raw)])
    ld = loader(conn, store, src, validator)
    ld.load_events([(e, raw), (e, raw)])
    ld.load_events([(e, raw)])
    assert ld.stats.outcomes[wh.DUPLICATE] == 2
    assert conn.execute("select count(*) from stg_filings").fetchone()[0] == 1
    assert conn.execute("select count(*) from stg_sections").fetchone()[0] == 2


def test_local_store_rejects_path_escape(store):
    with pytest.raises(ValueError):
        store.put("../evil.txt", b"x")


def test_reprocess_picks_up_a_raw_file_that_arrived_after_a_fetch_failure(
    tmp_path, conn, store, validator
):
    e = make_event("0000001234-26-000008")
    src = write_day(tmp_path / "days", "2026-01-05", [(e, None)])
    ld = loader(conn, store, src, validator)
    ld.process_messages([pipeline.Message(e)])
    assert len(store.list("quarantine")) == 1
    (tmp_path / "days" / "2026-01-05" / "raw" / f"{e.accession}.txt").write_bytes(
        make_submission(e.accession)
    )
    assert ld.reprocess_quarantine()["quarantined_after"] == 0
    assert conn.execute("select count(*) from stg_filings").fetchone()[0] == 1


def test_day_source_finds_a_filing_listed_under_an_earlier_filed_date(tmp_path):
    e = make_event("0000001234-26-000009", filed="2026-01-02")
    src = write_day(tmp_path / "days", "2026-01-05", [(e, make_submission(e.accession))])
    assert src.get(e).startswith(b"<SEC-DOCUMENT>")
