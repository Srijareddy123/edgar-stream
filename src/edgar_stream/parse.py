"""Parse an EDGAR full submission text file into filing metadata and sections."""

from __future__ import annotations

import html
import re
from dataclasses import dataclass, field
from datetime import date

from .events import FilingEvent, base_form

PARSER_VERSION = "1.0"
MAX_SECTION_CHARS = 20_000

ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
DASHES = "".join(map(chr, (0x2013, 0x2014)))  # en and em dash
ITEM_RE = re.compile(
    r"(?im)^[^\S\n]*item\s+(\d{1,2}(?:\.\d{2})?[a-c]?)[^\S\n]*[.:" + DASHES + r"-]?[^\S\n]*(.*)$"
)
BLOCK_TAGS = re.compile(r"(?i)</(div|p|tr|li|h[1-6]|table)>|<br\s*/?>")
HIDDEN = re.compile(r"(?is)<ix:header>.*?</ix:header>|<(script|style)\b.*?</\1>")
TAGS = re.compile(r"(?s)<[^>]+>")


class ParseError(Exception):
    """Raised when a file cannot become a valid filing row. `reason` is a short code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class Section:
    item: str
    title: str
    ord: int
    n_chars: int
    text: str


@dataclass(frozen=True)
class ParsedFiling:
    accession: str
    version: int
    cik: int
    company: str
    form: str
    base_form: str
    filed_date: str
    period: str | None
    acceptance_ts: str | None
    sic: str | None
    state_inc: str | None
    fiscal_year_end: str | None
    items_declared: str
    n_docs: int
    truncated: bool
    raw_bytes: int
    sections: tuple[Section, ...] = field(default_factory=tuple)
    parser_version: str = PARSER_VERSION

    @property
    def key(self) -> str:
        return f"{self.accession}:{self.version}"

    @property
    def filing_key(self) -> str:
        """Identity of the logical filing that its versions replace.

        10-K and 10-Q: company, form and period. An 8-K has no period of its own, so it is
        matched on company, event date and the list of items it reports.
        """
        key = f"{self.cik}|{self.base_form}|{self.period or self.filed_date}"
        if self.base_form == "8-K":
            items = sorted(i.strip() for i in self.items_declared.split(";"))
            key += "|" + ";".join(items)
        return key

    @property
    def order_key(self) -> str:
        """Sortable rank: a larger string is a newer version of the same filing."""
        return f"{self.version:02d}|{self.filed_date}|{self.acceptance_ts or ''}|{self.accession}"


def _days_apart(a: str, b: str) -> int:
    return abs((date.fromisoformat(a) - date.fromisoformat(b)).days)


def _fmt_date(raw: str | None) -> str | None:
    if raw and re.fullmatch(r"\d{8}", raw):
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return None


def _fmt_ts(raw: str | None) -> str | None:
    if raw and re.fullmatch(r"\d{14}", raw):
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]} {raw[8:10]}:{raw[10:12]}:{raw[12:]}"
    return None


def parse_header(header: str) -> dict[str, str]:
    """Read `KEY: value` lines from the SEC header. Repeated keys are joined with `; `."""
    out: dict[str, list[str]] = {}
    for line in header.splitlines():
        if ":" not in line:
            continue
        key, _, value = line.partition(":")
        key, value = key.strip(), value.strip()
        if key and value and not key.startswith("<"):
            out.setdefault(key, []).append(value)
    return {k: "; ".join(v) for k, v in out.items()}


def html_to_text(markup: str) -> str:
    markup = HIDDEN.sub(" ", markup)
    markup = BLOCK_TAGS.sub("\n", markup)
    text = html.unescape(TAGS.sub(" ", markup))
    lines = (re.sub(r"[^\S\n]+", " ", ln).strip() for ln in text.splitlines())
    return "\n".join(ln for ln in lines if ln)


def _main_document(body: str, form: str) -> str | None:
    """Return the text of the first document whose TYPE matches the form."""
    for block in re.split(r"(?i)<DOCUMENT>", body)[1:]:
        m = re.search(r"(?im)^<TYPE>\s*(\S+)", block)
        if m and m[1].upper() == form.upper():
            t = re.search(r"(?is)<TEXT>(.*?)(?:</TEXT>|$)", block)
            return t[1] if t else None
    return None


def split_sections(text: str) -> list[Section]:
    """Split on `Item N` headings. A repeated item (table of contents) keeps its longest span."""
    hits = list(ITEM_RE.finditer(text))
    best: dict[str, tuple[int, int, str, str]] = {}
    for i, m in enumerate(hits):
        end = hits[i + 1].start() if i + 1 < len(hits) else len(text)
        body = text[m.end() : end].strip()
        title = m[2].strip()
        if not title and body:
            title, _, body = body.partition("\n")
        item = m[1].upper()
        prev = best.get(item)
        if prev is None or len(body) > prev[1]:
            best[item] = (m.start(), len(body), title[:200], body)
    ordered = sorted(best.items(), key=lambda kv: kv[1][0])
    return [
        Section(item, title, n, length, body[:MAX_SECTION_CHARS])
        for n, (item, (_, length, title, body)) in enumerate(ordered)
    ]


def parse_submission(raw: bytes, event: FilingEvent) -> ParsedFiling:
    """Turn raw bytes into a ParsedFiling or raise ParseError with a reason code."""
    if not raw.strip():
        raise ParseError("empty_file")
    text = raw.decode("utf-8", errors="replace")
    m = re.search(r"(?is)<SEC-HEADER>(.*?)</SEC-HEADER>", text)
    if not m:
        raise ParseError("no_sec_header")
    hdr = parse_header(m[1])
    for needed in (
        "ACCESSION NUMBER",
        "CONFORMED SUBMISSION TYPE",
        "FILED AS OF DATE",
        "CENTRAL INDEX KEY",
    ):
        if needed not in hdr:
            raise ParseError("header_missing_field", needed)
    accession = hdr["ACCESSION NUMBER"]
    if not ACCESSION_RE.match(accession):
        raise ParseError("bad_accession_format", accession)
    if accession != event.accession:
        raise ParseError("accession_mismatch", f"{accession} vs {event.accession}")
    if hdr["CONFORMED SUBMISSION TYPE"] != event.form:
        raise ParseError("form_mismatch", f"{hdr['CONFORMED SUBMISSION TYPE']} vs {event.form}")
    ciks = {c.strip().lstrip("0") for c in hdr["CENTRAL INDEX KEY"].split(";")}
    if str(event.cik) not in ciks:  # filings with co-registrants list several CIKs
        raise ParseError("cik_mismatch", f"{hdr['CENTRAL INDEX KEY']} vs {event.cik}")
    filed = _fmt_date(hdr["FILED AS OF DATE"])
    # The index date is the dissemination date, which can trail the filing date by a day or two.
    if (
        filed is None
        or abs((date.fromisoformat(filed) - date.fromisoformat(event.filed_date)).days) > 7
    ):
        raise ParseError("filed_date_mismatch", f"{filed} vs {event.filed_date}")

    body = text[m.end() :]
    doc = _main_document(body, event.form)
    if doc is None:
        raise ParseError("no_main_document", event.form)
    sections = split_sections(html_to_text(doc))
    if not sections:
        raise ParseError("no_sections_found", event.form)
    return ParsedFiling(
        accession=accession,
        version=event.version,
        cik=event.cik,
        company=hdr.get("COMPANY CONFORMED NAME", event.company),
        form=event.form,
        base_form=base_form(event.form),
        filed_date=filed,
        period=_fmt_date(hdr.get("CONFORMED PERIOD OF REPORT")),
        acceptance_ts=_fmt_ts(_acceptance(text)),
        sic=hdr.get("STANDARD INDUSTRIAL CLASSIFICATION"),
        state_inc=hdr.get("STATE OF INCORPORATION"),
        fiscal_year_end=hdr.get("FISCAL YEAR END"),
        items_declared=hdr.get("ITEM INFORMATION", ""),
        n_docs=len(re.findall(r"(?i)<DOCUMENT>", body)),
        truncated="</SEC-DOCUMENT>" not in text,
        raw_bytes=len(raw),
        sections=tuple(sections),
    )


def _acceptance(text: str) -> str | None:
    m = re.search(r"<ACCEPTANCE-DATETIME>\s*(\d{14})", text)
    return m[1] if m else None
