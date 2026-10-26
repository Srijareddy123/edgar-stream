import time

import pytest

from edgar_stream.edgar import EdgarClient, parse_form_index, select_events

INDEX = """Form Type   Company Name                  CIK
      Date Filed  File Name
-----------------------------------------------------------------------
8-K              ACME 3 CORP, INC.                  1084869     20261023    edgar/data/1084869/0001084869-26-000041.txt
10-K/A           Foo Bar Holdings                   99          20261023    edgar/data/99/0000000099-26-000002.txt
4                Some Insider                       5           20261023    edgar/data/5/0000000005-26-000003.txt
"""


def test_index_rows_are_parsed():
    events = parse_form_index(INDEX)
    assert [e.form for e in events] == ["8-K", "10-K/A", "4"]
    assert events[0].company == "ACME 3 CORP, INC." and events[0].cik == 1084869
    assert events[0].accession == "0001084869-26-000041" and events[0].filed_date == "2026-10-23"
    assert events[1].version == 1 and events[0].version == 0


def test_select_keeps_target_forms_and_is_stable():
    events = parse_form_index(INDEX)
    assert [e.form for e in select_events(events, 10)] == ["10-K/A", "8-K"]
    assert select_events(events, 1) == select_events(list(reversed(events)), 1)


def test_client_refuses_placeholder_or_empty_user_agent():
    for ua in ("", "Your Name your.email@example.com"):
        with pytest.raises(ValueError):
            EdgarClient(ua)


def test_client_stays_under_ten_requests_a_second():
    client = EdgarClient("Test Person test@example.org")
    start = time.monotonic()
    for _ in range(12):
        client._wait()
    assert 12 / (time.monotonic() - start) < 10
