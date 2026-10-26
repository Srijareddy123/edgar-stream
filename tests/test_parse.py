import pytest
from conftest import make_event, make_submission

from edgar_stream.parse import ParseError, html_to_text, parse_submission, split_sections


def test_parses_metadata_and_sections():
    p = parse_submission(
        make_submission("0000001234-26-000001"), make_event("0000001234-26-000001")
    )
    assert (p.cik, p.form, p.filed_date, p.period) == (1234, "8-K", "2026-01-05", "2026-01-02")
    assert [s.item for s in p.sections] == ["2.02", "9.01"]
    assert p.sections[0].title == "Results of Operations"
    assert not p.truncated and p.version == 0


def test_amendment_gets_version_one_and_same_filing_key_as_original():
    orig = parse_submission(
        make_submission("0000001234-26-000001"), make_event("0000001234-26-000001")
    )
    acc = "0000001234-26-000009"
    amend = parse_submission(
        make_submission(acc, form="8-K/A", filed="2026-01-09"),
        make_event(acc, form="8-K/A", filed="2026-01-09"),
    )
    assert amend.version == 1 and amend.filing_key == orig.filing_key
    assert amend.order_key > orig.order_key


def test_thin_space_after_item_is_recognised():
    # Real filings separate "Item" and the number with a thin space (U+2009).
    text = "Cover\nItem 1.01\nEntry into an Agreement\nBody text here"
    assert [s.item for s in split_sections(text)] == ["1.01"]


def test_table_of_contents_entry_loses_to_the_real_section():
    text = "Item 1. Business\nItem 2. Properties\nItem 1. Business\n" + "long text " * 50
    sections = split_sections(text)
    by_item = {s.item: s for s in sections}
    assert by_item["1"].n_chars > 100 and by_item["2"].n_chars < 20


def test_hidden_xbrl_header_is_dropped():
    out = html_to_text("<div><ix:header>secret</ix:header>visible</div>")
    assert out == "visible"


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (b"", "empty_file"),
        (b"just some text", "no_sec_header"),
        (
            make_submission("0000001234-26-000001").replace(b"FILED AS OF DATE", b"X"),
            "header_missing_field",
        ),
        (make_submission("0000009999-26-000001"), "accession_mismatch"),
        (make_submission("0000001234-26-000001", cik=77), "cik_mismatch"),
        (make_submission("0000001234-26-000001", form="10-Q"), "form_mismatch"),
        (make_submission("0000001234-26-000001", filed="2026-02-20"), "filed_date_mismatch"),
        (
            make_submission("0000001234-26-000001", body="<div>no headings</div>"),
            "no_sections_found",
        ),
    ],
)
def test_bad_files_raise_a_reason_code(raw, reason):
    with pytest.raises(ParseError) as exc:
        parse_submission(raw, make_event("0000001234-26-000001"))
    assert exc.value.reason == reason
