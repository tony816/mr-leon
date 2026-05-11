#!/usr/bin/env python3
r"""
build_uk_cache_db.py

UK Range Scan용 로컬 fundamentals cache DB builder.

목표:
- FCA NSM export CSV, manifest CSV/JSON/JSONL, 로컬 ESEF/iXBRL 파일/폴더를 입력으로 받음
- 공식 structured AFR(ESEF/iXBRL: .zip/.xbri/.xhtml/.html/.xml)만 파싱
- 회사별 여러 연도/여러 파일을 병합
- C:\mr-leon\data\uk_fundamentals_cache.jsonl 형식으로 1회사 1row 생성
- 기존 app.py의 UK Range Scan 캐시 로더와 호환되는 필드명을 유지

기본 사용 예시:
    python build_uk_cache_db.py --input-dir C:\data\uk_esef --output data\uk_fundamentals_cache.jsonl

NSM 검색 결과 CSV + ticker universe 매핑 사용:
    python build_uk_cache_db.py ^
      --nsm-csv C:\data\nsm_structured_afr_export.csv ^
      --universe-csv C:\data\uk_universe.csv ^
      --download-dir data\uk_filings ^
      --output data\uk_fundamentals_cache.jsonl

manifest 사용:
    python build_uk_cache_db.py --manifest C:\data\uk_manifest.csv --output data\uk_fundamentals_cache.jsonl

manifest CSV 권장 컬럼:
    ticker,name,isin,lei,url,file_path,fiscal_year

주의:
- 이 파일은 Yahoo를 재무 핵심지표 계산에 사용하지 않는다.
- 가격/PER/PBR 보완은 기존 app.py Range Scan의 Yahoo batch quote 단계가 담당한다.
- FCA NSM 자체가 SEC companyfacts 같은 정규화 JSON API를 제공하는 구조가 아니므로,
  NSM에서 export한 CSV 또는 공식 structured AFR 파일/URL을 입력으로 삼아 로컬 DB를 구축한다.
"""

from __future__ import annotations

import argparse
import csv
import dataclasses
import datetime as _dt
import hashlib
import html
import json
import os
import re
import shutil
import sys
import time
import urllib.parse
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import xml.etree.ElementTree as ET

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore


DEFAULT_OUTPUT = Path("data") / "uk_fundamentals_cache.jsonl"
DEFAULT_DOWNLOAD_DIR = Path("data") / "uk_filings"
DEFAULT_AUDIT_OUTPUT = Path("data") / "uk_cache_build_audit.csv"

ASSUME_ZERO_DEBT_WHEN_MISSING = os.getenv("NET_CASH_ASSUME_ZERO_DEBT", "1").lower() in (
    "1",
    "true",
    "yes",
    "y",
)

OFFICIAL_HOST_SUFFIXES = (
    "fca.org.uk",
    "data.fca.org.uk",
)

STRUCTURED_EXTENSIONS = (".zip", ".xbri", ".xhtml", ".html", ".xml")
PARSABLE_EXTENSIONS = (".zip", ".xbri", ".xhtml", ".html", ".xml")

# IFRS/ESEF 태그 후보. _local_name()으로 namespace prefix는 제거된다.
REVENUE_TAGS = (
    "Revenue",
    "RevenueFromContractsWithCustomers",
    "RevenueFromContractsWithCustomersExcludingAssessedTax",
    "SalesRevenueNet",
    "Turnover",
)
OP_INCOME_TAGS = (
    "OperatingProfitLoss",
    "ProfitLossFromOperatingActivities",
    "OperatingProfit",
    "OperatingLoss",
)
NET_INCOME_TAGS = (
    "ProfitLoss",
    "ProfitLossAttributableToOwnersOfParent",
    "ProfitLossFromContinuingOperations",
    "ProfitLossForPeriod",
)
CASH_TAGS = (
    "CashAndCashEquivalents",
    "CashAndCashEquivalentsAtCarryingValue",
    "CashAndCashEquivalentsAtEndOfPeriod",
)
EQUITY_TAGS = (
    "EquityAttributableToOwnersOfParent",
    "TotalEquity",
    "Equity",
)
LIABILITIES_TAGS = (
    "Liabilities",
    "TotalLiabilities",
)

# 차입금은 중복계산 위험이 있어 총액 후보와 구성요소 후보를 분리한다.
DEBT_TOTAL_TAGS = (
    "Borrowings",
    "LoansAndBorrowings",
    "InterestBearingLoansAndBorrowings",
    "FinancialLiabilitiesAtAmortisedCost",
    "FinancialLiabilities",
    "DebtSecuritiesInIssue",
    "DebtSecurities",
)
DEBT_CURRENT_TAGS = (
    "CurrentBorrowings",
    "CurrentLoansAndBorrowings",
    "CurrentInterestBearingLoansAndBorrowings",
    "CurrentFinancialLiabilities",
    "BankOverdrafts",
    "CurrentDebtSecuritiesInIssue",
)
DEBT_NONCURRENT_TAGS = (
    "NoncurrentBorrowings",
    "NoncurrentLoansAndBorrowings",
    "NoncurrentInterestBearingLoansAndBorrowings",
    "NoncurrentFinancialLiabilities",
    "NoncurrentDebtSecuritiesInIssue",
)
LEASE_TOTAL_TAGS = (
    "LeaseLiabilities",
    "FinanceLeaseLiabilities",
)
LEASE_CURRENT_TAGS = (
    "CurrentLeaseLiabilities",
    "CurrentFinanceLeaseLiabilities",
)
LEASE_NONCURRENT_TAGS = (
    "NoncurrentLeaseLiabilities",
    "NoncurrentFinanceLeaseLiabilities",
)
SHARES_TAGS = (
    "NumberOfSharesOutstanding",
    "WeightedAverageNumberOfOrdinarySharesOutstanding",
    "WeightedAverageNumberOfSharesOutstandingBasic",
    "IssuedCapitalNumberOfShares",
)


@dataclasses.dataclass
class FilingCandidate:
    ticker: str = ""
    name: str = ""
    isin: str = ""
    lei: str = ""
    url: str = ""
    file_path: str = ""
    fiscal_year: Optional[int] = None
    source_hint: str = ""
    row_source: str = ""


@dataclasses.dataclass
class ParsedDocument:
    source: str
    facts: Dict[str, List[Dict[str, Any]]]
    error: str = ""


@dataclasses.dataclass
class AuditRow:
    ticker: str
    name: str
    source: str
    status: str
    message: str
    parsed_facts: int = 0
    output_record: str = ""


def norm_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())


def first_value(row: Dict[str, Any], candidates: Sequence[str]) -> str:
    wanted = {norm_col(c) for c in candidates}
    for key, value in row.items():
        if norm_col(key) in wanted and value not in (None, ""):
            return str(value).strip()
    return ""


def any_value_by_contains(row: Dict[str, Any], needles: Sequence[str]) -> str:
    needles_norm = [norm_col(n) for n in needles]
    for key, value in row.items():
        k = norm_col(key)
        if any(n in k for n in needles_norm) and value not in (None, ""):
            return str(value).strip()
    return ""


def normalize_ticker(code: str) -> str:
    text = (code or "").strip().upper()
    for suffix in (".L", ".IL", ".GB"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    return text


def normalize_name(name: str) -> str:
    text = html.unescape(str(name or "")).upper()
    text = re.sub(r"\b(PLC|PUBLIC LIMITED COMPANY|LTD|LIMITED|GROUP|HOLDINGS|HOLDING|INC|CORP|COMPANY)\b", " ", text)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_int_year(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    text = str(value)
    match = re.search(r"(20\d{2}|19\d{2})", text)
    if not match:
        return None
    year = int(match.group(1))
    if 1990 <= year <= _dt.date.today().year + 1:
        return year
    return None


def read_table(path: Path) -> List[Dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        rows = []
        with path.open("r", encoding="utf-8-sig") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            for key in ("rows", "items", "data", "filings"):
                if isinstance(payload.get(key), list):
                    return [row for row in payload[key] if isinstance(row, dict)]
            return [payload]
        return []
    # csv/tsv auto sniff
    raw = path.read_text(encoding="utf-8-sig", errors="ignore")
    sample = raw[:4096]
    dialect = csv.Sniffer().sniff(sample, delimiters=",\t;|") if sample.strip() else csv.excel
    return list(csv.DictReader(raw.splitlines(), dialect=dialect))


def write_example_manifest(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ticker", "name", "isin", "lei", "url", "file_path", "fiscal_year"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "VOD",
                "name": "Vodafone Group",
                "isin": "",
                "lei": "",
                "url": "https://data.fca.org.uk/.../structured-afr.zip",
                "file_path": "",
                "fiscal_year": "2025",
            }
        )


def load_universe(path: Optional[Path]) -> Dict[str, Dict[str, str]]:
    """Return lookup maps: keys prefixed by ticker:/isin:/lei:/name:."""
    lookup: Dict[str, Dict[str, str]] = {}
    if not path or not path.exists():
        return lookup
    for row in read_table(path):
        ticker = normalize_ticker(first_value(row, ("ticker", "symbol", "code", "epic", "ric")))
        name = first_value(row, ("name", "company", "issuer", "issuer_name", "company_name", "security_name"))
        isin = first_value(row, ("isin", "ISIN"))
        lei = first_value(row, ("lei", "LEI"))
        item = {"ticker": ticker, "name": name, "isin": isin, "lei": lei}
        if ticker:
            lookup[f"ticker:{ticker}"] = item
        if isin:
            lookup[f"isin:{isin.upper()}"] = item
        if lei:
            lookup[f"lei:{lei.upper()}"] = item
        n = normalize_name(name)
        if n:
            lookup[f"name:{n}"] = item
    return lookup


def map_universe(row: FilingCandidate, lookup: Dict[str, Dict[str, str]]) -> FilingCandidate:
    if not lookup:
        return row
    keys = []
    if row.ticker:
        keys.append(f"ticker:{normalize_ticker(row.ticker)}")
    if row.isin:
        keys.append(f"isin:{row.isin.upper()}")
    if row.lei:
        keys.append(f"lei:{row.lei.upper()}")
    if row.name:
        keys.append(f"name:{normalize_name(row.name)}")
    found = None
    for key in keys:
        if key in lookup:
            found = lookup[key]
            break
    if not found and row.name:
        target = normalize_name(row.name)
        # 약한 fallback: NSM issuer name과 universe name이 서로 포함되는 경우만 허용.
        for key, item in lookup.items():
            if not key.startswith("name:"):
                continue
            n = key.split(":", 1)[1]
            if target and (target in n or n in target):
                found = item
                break
    if found:
        row.ticker = row.ticker or found.get("ticker", "")
        row.name = row.name or found.get("name", "")
        row.isin = row.isin or found.get("isin", "")
        row.lei = row.lei or found.get("lei", "")
    return row


def extract_urls(row: Dict[str, Any]) -> List[str]:
    urls: List[str] = []
    for value in row.values():
        text = str(value or "")
        for match in re.finditer(r"https?://[^\s,;\]\)\}\"']+", text):
            url = match.group(0).strip()
            if url not in urls:
                urls.append(url)
    return urls


def row_looks_like_structured_afr(row: Dict[str, Any]) -> bool:
    blob = " ".join(str(v or "") for v in row.values()).lower()
    positive = (
        "annual financial report",
        "structured",
        "esef",
        "xbrl",
        "ixbrl",
        "xhtml",
        "xbri",
    )
    # 너무 강하게 필터링하면 NSM CSV 컬럼명이 달라질 때 누락되므로 annual/report 또는 structured 쪽이면 통과.
    return any(p in blob for p in positive) or any(ext in blob for ext in STRUCTURED_EXTENSIONS)


def candidates_from_manifest(path: Path, universe: Dict[str, Dict[str, str]]) -> List[FilingCandidate]:
    out: List[FilingCandidate] = []
    for row in read_table(path):
        cand = FilingCandidate(
            ticker=normalize_ticker(first_value(row, ("ticker", "symbol", "code", "epic", "ric"))),
            name=first_value(row, ("name", "company", "issuer", "issuer_name", "company_name")),
            isin=first_value(row, ("isin",)),
            lei=first_value(row, ("lei",)),
            url=first_value(row, ("url", "download_url", "document_url", "source_url")),
            file_path=first_value(row, ("file", "file_path", "path", "local_path")),
            fiscal_year=parse_int_year(first_value(row, ("fiscal_year", "year", "period", "accounting_year"))),
            source_hint="manifest",
            row_source=str(path),
        )
        if not cand.url:
            urls = extract_urls(row)
            cand.url = urls[0] if urls else ""
        cand = map_universe(cand, universe)
        if cand.file_path or cand.url:
            out.append(cand)
    return out


def candidates_from_nsm_csv(path: Path, universe: Dict[str, Dict[str, str]]) -> List[FilingCandidate]:
    out: List[FilingCandidate] = []
    for row in read_table(path):
        if not row_looks_like_structured_afr(row):
            continue
        urls = extract_urls(row)
        ticker = normalize_ticker(first_value(row, ("ticker", "symbol", "code", "epic", "ric")))
        name = first_value(
            row,
            (
                "issuer_name",
                "issuer",
                "company_name",
                "company",
                "organisation_name",
                "organization_name",
                "name",
            ),
        ) or any_value_by_contains(row, ("issuer", "company", "organisation", "organization"))
        isin = first_value(row, ("isin",)) or any_value_by_contains(row, ("isin",))
        lei = first_value(row, ("lei",)) or any_value_by_contains(row, ("lei",))
        fy = parse_int_year(any_value_by_contains(row, ("date", "year", "period")))
        if not urls:
            continue
        for url in urls:
            cand = FilingCandidate(
                ticker=ticker,
                name=name,
                isin=isin,
                lei=lei,
                url=url,
                fiscal_year=fy,
                source_hint="nsm_csv",
                row_source=str(path),
            )
            cand = map_universe(cand, universe)
            if cand.ticker:
                out.append(cand)
            else:
                # universe 매핑이 없어도 name 기준으로 나중에 진단 가능하게 남김.
                out.append(cand)
    return out


def candidates_from_inputs(inputs: Sequence[str]) -> List[FilingCandidate]:
    out: List[FilingCandidate] = []
    for item in inputs:
        p = Path(item)
        if p.exists():
            out.append(
                FilingCandidate(
                    ticker=normalize_ticker(p.stem.split("_")[0].split("-")[0]),
                    name=p.stem,
                    file_path=str(p),
                    source_hint="input",
                    row_source=str(p),
                )
            )
    return out


def candidates_from_dirs(dirs: Sequence[str]) -> List[FilingCandidate]:
    out: List[FilingCandidate] = []
    for raw in dirs:
        root = Path(raw)
        if not root.exists():
            continue
        for p in root.rglob("*"):
            if p.is_file() and p.suffix.lower() in PARSABLE_EXTENSIONS:
                parent = p.parent.name
                ticker_guess = normalize_ticker(parent if len(parent) <= 12 else p.stem.split("_")[0].split("-")[0])
                out.append(
                    FilingCandidate(
                        ticker=ticker_guess,
                        name=parent,
                        file_path=str(p),
                        source_hint="input_dir",
                        row_source=str(root),
                    )
                )
    return out


def is_official_url(url: str) -> bool:
    host = urllib.parse.urlparse(url).hostname or ""
    host = host.lower()
    return any(host == suffix or host.endswith("." + suffix) for suffix in OFFICIAL_HOST_SUFFIXES)


def safe_filename(text: str, limit: int = 120) -> str:
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", text).strip("._-")
    return text[:limit] or "file"


def url_ext(url: str, content_type: str = "") -> str:
    path = urllib.parse.urlparse(url).path.lower()
    for ext in STRUCTURED_EXTENSIONS + (".pdf",):
        if path.endswith(ext):
            return ext
    ct = content_type.lower()
    if "zip" in ct:
        return ".zip"
    if "html" in ct:
        return ".html"
    if "xml" in ct:
        return ".xml"
    return ".bin"


def request_get(url: str, timeout: int):
    if requests is None:
        raise RuntimeError("requests is required. Run: pip install requests")
    headers = {
        "User-Agent": "mr-leon-uk-cache-builder/1.0 (+local research tool)",
        "Accept": "text/html,application/xhtml+xml,application/xml,application/zip,*/*",
    }
    resp = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    resp.raise_for_status()
    return resp


def extract_structured_links(base_url: str, text: str) -> List[str]:
    links: List[str] = []
    # href="..." 우선 추출
    for href in re.findall(r"href=[\"']([^\"']+)[\"']", text, flags=re.I):
        href = html.unescape(href)
        absolute = urllib.parse.urljoin(base_url, href)
        low = absolute.lower()
        if any(ext in low for ext in STRUCTURED_EXTENSIONS):
            if absolute not in links:
                links.append(absolute)
    # 본문 URL fallback
    for match in re.finditer(r"https?://[^\s\"'<>]+", text):
        absolute = html.unescape(match.group(0))
        low = absolute.lower()
        if any(ext in low for ext in STRUCTURED_EXTENSIONS) and absolute not in links:
            links.append(absolute)
    return links


def download_candidate(cand: FilingCandidate, download_dir: Path, timeout: int, strict_official: bool) -> Path:
    if not cand.url:
        raise ValueError("No URL to download")
    if strict_official and not is_official_url(cand.url):
        raise ValueError(f"Non-official URL skipped under --strict-official: {cand.url}")
    download_dir.mkdir(parents=True, exist_ok=True)
    resp = request_get(cand.url, timeout)
    ctype = resp.headers.get("Content-Type", "")
    ext = url_ext(resp.url, ctype)

    # NSM 상세 페이지 등 HTML wrapper인 경우 내부 structured 파일 링크를 한 번 더 따라간다.
    if ext == ".html" and not resp.url.lower().endswith((".html", ".xhtml")):
        links = extract_structured_links(resp.url, resp.text)
        if links:
            next_url = links[0]
            if strict_official and not is_official_url(next_url):
                raise ValueError(f"Structured link is not official: {next_url}")
            resp = request_get(next_url, timeout)
            ctype = resp.headers.get("Content-Type", "")
            ext = url_ext(resp.url, ctype)

    digest = hashlib.sha1(resp.url.encode("utf-8")).hexdigest()[:10]
    base = safe_filename("_".join(x for x in (cand.ticker, str(cand.fiscal_year or ""), Path(urllib.parse.urlparse(resp.url).path).name) if x))
    if not base.lower().endswith(ext):
        base += ext
    target = download_dir / f"{digest}_{base}"
    if target.exists() and target.stat().st_size > 0:
        return target
    target.write_bytes(resp.content)
    return target


def _local_name(tag: str) -> str:
    text = str(tag or "")
    if "}" in text:
        text = text.rsplit("}", 1)[-1]
    if ":" in text:
        text = text.rsplit(":", 1)[-1]
    return text


def parse_xbrl_number(value: Any, scale: Optional[str] = None, sign: Optional[str] = None) -> Optional[float]:
    if value in (None, "", "-", "NaN"):
        return None
    text = html.unescape(str(value))
    text = re.sub(r"\s+", "", text).replace(",", "")
    text = text.replace("−", "-").replace("—", "-")
    if not text:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    # 문장 속 숫자가 섞인 경우 첫 숫자만 방어적으로 추출
    if not re.fullmatch(r"[-+]?\d+(\.\d+)?", text):
        m = re.search(r"[-+]?\d+(?:\.\d+)?", text)
        if not m:
            return None
        text = m.group(0)
    try:
        number = float(text)
    except Exception:
        return None
    try:
        if scale not in (None, ""):
            number *= 10 ** int(scale)
    except Exception:
        pass
    if str(sign or "").strip() == "-":
        number = -abs(number)
    return number


def parse_iso_ordinal(date_text: str) -> int:
    try:
        return _dt.date.fromisoformat(str(date_text)[:10]).toordinal()
    except Exception:
        return 0


def year_from_iso(date_text: str) -> Optional[int]:
    try:
        return _dt.date.fromisoformat(str(date_text)[:10]).year
    except Exception:
        return None


def parse_xbrl_xml(xml_text: str, source_rank: int = 0) -> Dict[str, List[Dict[str, Any]]]:
    # 일부 XHTML 앞의 BOM/쓰레기 문자를 방어적으로 제거
    start = min([i for i in [xml_text.find("<html"), xml_text.find("<?xml"), xml_text.find("<xbrl")] if i >= 0] or [0])
    xml_text = xml_text[start:]
    parser = ET.XMLParser()
    root = ET.fromstring(xml_text.encode("utf-8", errors="ignore"), parser=parser)
    contexts: Dict[str, Dict[str, Any]] = {}
    units: Dict[str, str] = {}
    facts: Dict[str, List[Dict[str, Any]]] = {}

    for elem in root.iter():
        lname = _local_name(elem.tag)
        if lname == "context":
            context_id = elem.attrib.get("id")
            if not context_id:
                continue
            ctx: Dict[str, Any] = {}
            for child in elem.iter():
                child_name = _local_name(child.tag)
                text = (child.text or "").strip()
                if child_name == "instant":
                    ctx["instant"] = text
                    ctx["year"] = year_from_iso(text)
                    ctx["end"] = text
                elif child_name == "startDate":
                    ctx["start"] = text
                elif child_name == "endDate":
                    ctx["end"] = text
                    ctx["year"] = year_from_iso(text)
            contexts[context_id] = ctx
        elif lname == "unit":
            unit_id = elem.attrib.get("id")
            if not unit_id:
                continue
            measures = []
            for child in elem.iter():
                if _local_name(child.tag) == "measure" and child.text:
                    measures.append(_local_name(child.text.strip()))
            units[unit_id] = ",".join(measures)

    ignored = {"html", "body", "div", "span", "context", "unit", "measure", "schemaRef", "resources"}
    for elem in root.iter():
        lname = _local_name(elem.tag)
        concept = elem.attrib.get("name") if lname in ("nonFraction", "nonNumeric") else elem.tag
        if not concept:
            continue
        concept_name = _local_name(concept)
        if not concept_name or concept_name in ignored:
            continue
        context_ref = elem.attrib.get("contextRef")
        if not context_ref:
            continue
        value = parse_xbrl_number("".join(elem.itertext()), elem.attrib.get("scale"), elem.attrib.get("sign"))
        if value is None:
            continue
        ctx = contexts.get(context_ref, {})
        unit_ref = elem.attrib.get("unitRef") or ""
        start_date = ctx.get("start")
        end_date = ctx.get("end") or ctx.get("instant")
        facts.setdefault(concept_name, []).append(
            {
                "value": value,
                "year": ctx.get("year"),
                "start": start_date,
                "end": end_date,
                "end_ord": parse_iso_ordinal(end_date or ""),
                "duration_days": max(0, parse_iso_ordinal(end_date or "") - parse_iso_ordinal(start_date or ""))
                if start_date and end_date
                else 0,
                "unit": units.get(unit_ref, unit_ref),
                "source_rank": source_rank,
            }
        )
    return facts


def read_esef_documents(path: Path) -> List[Tuple[str, str]]:
    if path.is_dir():
        docs: List[Tuple[str, str]] = []
        for child in path.rglob("*"):
            if child.is_file() and child.suffix.lower() in PARSABLE_EXTENSIONS:
                docs.extend(read_esef_documents(child))
        return docs
    suffix = path.suffix.lower()
    if suffix in (".zip", ".xbri"):
        docs = []
        with zipfile.ZipFile(path) as zf:
            for name in zf.namelist():
                lower = name.lower()
                if lower.endswith((".html", ".xhtml", ".xml")):
                    try:
                        docs.append((f"{path.name}!{name}", zf.read(name).decode("utf-8", errors="ignore")))
                    except Exception:
                        continue
        return docs
    if suffix in (".html", ".xhtml", ".xml"):
        return [(str(path), path.read_text(encoding="utf-8", errors="ignore"))]
    return []


def merge_facts(target: Dict[str, List[Dict[str, Any]]], new_facts: Dict[str, List[Dict[str, Any]]]) -> None:
    for key, items in new_facts.items():
        target.setdefault(key, []).extend(items)


def latest_fact(facts: Dict[str, List[Dict[str, Any]]], tags: Sequence[str], annual: Optional[bool] = None) -> Optional[float]:
    candidates: List[Tuple[int, int, int, float]] = []
    tag_rank = {tag: idx for idx, tag in enumerate(tags)}
    for tag in tags:
        for fact in facts.get(tag, []):
            value = fact.get("value")
            if value is None:
                continue
            duration = int(fact.get("duration_days") or 0)
            if annual is True and duration and not (300 <= duration <= 400):
                continue
            if annual is False and duration > 10:
                continue
            year = int(fact.get("year") or 0)
            candidates.append((year, int(fact.get("end_ord") or 0), -tag_rank[tag], float(value)))
    if not candidates:
        return None
    return max(candidates, key=lambda x: (x[0], x[1], x[2]))[3]


def fact_series(facts: Dict[str, List[Dict[str, Any]]], tags: Sequence[str]) -> Dict[int, float]:
    series: Dict[int, Tuple[int, int, int, float]] = {}
    tag_rank = {tag: idx for idx, tag in enumerate(tags)}
    for tag in tags:
        for fact in facts.get(tag, []):
            year = fact.get("year")
            value = fact.get("value")
            if not year or value is None:
                continue
            duration = int(fact.get("duration_days") or 0)
            if duration and not (300 <= duration <= 400):
                continue
            candidate = (
                int(fact.get("source_rank") or 0),
                int(fact.get("end_ord") or 0),
                -tag_rank[tag],
                float(value),
            )
            existing = series.get(int(year))
            if existing is None or candidate[:3] > existing[:3]:
                series[int(year)] = candidate
    return {year: value for year, (_, _, _, value) in series.items()}


def sum_components(facts: Dict[str, List[Dict[str, Any]]], groups: Sequence[Sequence[str]]) -> Optional[float]:
    values = [latest_fact(facts, group, annual=False) for group in groups]
    values = [v for v in values if v is not None]
    return sum(values) if values else None


def choose_debt(facts: Dict[str, List[Dict[str, Any]]]) -> Optional[float]:
    borrowings_total = latest_fact(facts, DEBT_TOTAL_TAGS, annual=False)
    if borrowings_total is None:
        borrowings_total = sum_components(facts, (DEBT_CURRENT_TAGS, DEBT_NONCURRENT_TAGS))
    lease_total = latest_fact(facts, LEASE_TOTAL_TAGS, annual=False)
    if lease_total is None:
        lease_total = sum_components(facts, (LEASE_CURRENT_TAGS, LEASE_NONCURRENT_TAGS))
    values = [v for v in (borrowings_total, lease_total) if v is not None]
    return sum(values) if values else None


def build_recent_year_window(values_by_year: Dict[int, Optional[float]], window_years: int) -> List[Tuple[int, Optional[float]]]:
    valid_years = [year for year, value in values_by_year.items() if value is not None]
    if not valid_years:
        return []
    max_year = max(valid_years)
    start_year = max_year - window_years
    return [(year, values_by_year.get(year)) for year in range(start_year, max_year + 1)]


def compute_yoy_average_stats(values_by_year: Dict[int, Optional[float]], window_years: int = 5) -> Tuple[Optional[float], int, List[str]]:
    series = build_recent_year_window(values_by_year, window_years)
    if len(series) < 2:
        return None, 0, []
    positive_rates: List[float] = []
    transitions: List[str] = []
    for (prev_year, prev_val), (curr_year, curr_val) in zip(series, series[1:]):
        if prev_val is None or curr_val is None:
            continue
        if prev_val > 0 and curr_val > 0:
            positive_rates.append((curr_val - prev_val) / prev_val)
        else:
            if prev_val <= 0 < curr_val:
                transitions.append(f"적자→흑자 {prev_year}→{curr_year}")
            elif prev_val > 0 >= curr_val:
                transitions.append(f"흑자→적자 {prev_year}→{curr_year}")
    avg_pct = (sum(positive_rates) / len(positive_rates) * 100) if positive_rates else None
    return avg_pct, len(positive_rates), transitions


def build_yoy_average_text(avg_pct: Optional[float], count: int, transitions: List[str]) -> str:
    parts = []
    if avg_pct is not None:
        avg_text = f"{avg_pct:,.2f}%"
        parts.append(f"양(+) 구간 {count}개 평균")
    else:
        avg_text = "N/A"
        parts.append("양(+) 구간 없음")
    if transitions:
        parts.append(f"특이: {', '.join(transitions)}")
    return f"{avg_text} ({'; '.join(parts)})"


def format_amount(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{int(round(value)):,}"
    except Exception:
        return "N/A"


def format_ratio(value: Optional[float]) -> str:
    return f"{value:,.2f}" if value is not None else "N/A"


def format_per_share(cash_value: Optional[float], shares: Optional[float]) -> str:
    if cash_value is None or shares in (None, 0):
        return "N/A"
    return f"{cash_value / shares:,.2f}"


def compute_net_cash(cash: Optional[float], debt: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    if cash is None:
        return None, debt
    if debt is None:
        if ASSUME_ZERO_DEBT_WHEN_MISSING:
            return cash, 0.0
        return None, None
    return cash - debt, debt


def build_cache_record(ticker: str, name: str, facts: Dict[str, List[Dict[str, Any]]], source_files: Sequence[str], window_years: int) -> Dict[str, Any]:
    revenue = latest_fact(facts, REVENUE_TAGS, annual=True)
    op_income = latest_fact(facts, OP_INCOME_TAGS, annual=True)
    net_income = latest_fact(facts, NET_INCOME_TAGS, annual=True)
    cash = latest_fact(facts, CASH_TAGS, annual=False)
    equity = latest_fact(facts, EQUITY_TAGS, annual=False)
    liabilities = latest_fact(facts, LIABILITIES_TAGS, annual=False)
    debt = choose_debt(facts)
    shares = latest_fact(facts, SHARES_TAGS, annual=False)

    net_cash, debt_used = compute_net_cash(cash, debt)
    liabilities_ratio = (liabilities / equity * 100) if liabilities is not None and equity not in (None, 0) else None
    debt_ratio = (debt_used / equity * 100) if debt_used is not None and equity not in (None, 0) else None
    net_cash_ps = (net_cash / shares) if net_cash is not None and shares not in (None, 0) else None

    sales_avg, sales_count, sales_transitions = compute_yoy_average_stats(fact_series(facts, REVENUE_TAGS), window_years)
    op_avg, op_count, op_transitions = compute_yoy_average_stats(fact_series(facts, OP_INCOME_TAGS), window_years)
    net_avg, net_count, net_transitions = compute_yoy_average_stats(fact_series(facts, NET_INCOME_TAGS), window_years)

    years = [int(f.get("year")) for items in facts.values() for f in items if f.get("year")]
    bsns_year = str(max(years)) if years else "-"
    ticker = normalize_ticker(ticker)

    coverage = {
        "revenue": revenue is not None,
        "operating_income": op_income is not None,
        "net_income": net_income is not None,
        "cash": cash is not None,
        "equity": equity is not None,
        "liabilities": liabilities is not None,
        "interest_bearing_debt": debt_used is not None,
        "shares": shares is not None,
    }

    return {
        "country": "UK",
        "code": ticker,
        "name": name or ticker,
        "price": "N/A",
        "per": "N/A",
        "pbr": "N/A",
        "liabilities_ratio": format_ratio(liabilities_ratio),
        "interest_bearing_debt_ratio": format_ratio(debt_ratio),
        "net_cash_per_share": format_per_share(net_cash, shares),
        "net_cash_per_share_ratio": "N/A",
        "net_cash_per_share_value": net_cash_ps,
        "sales": format_amount(revenue),
        "op_income": format_amount(op_income),
        "equity": format_amount(equity),
        "sales_growth_5y": build_yoy_average_text(sales_avg, sales_count, sales_transitions),
        "op_growth_5y": build_yoy_average_text(op_avg, op_count, op_transitions),
        "net_income_growth_5y": build_yoy_average_text(net_avg, net_count, net_transitions),
        "sales_growth_5y_avg_pct": sales_avg,
        "op_growth_5y_avg_pct": op_avg,
        "net_income_growth_5y_avg_pct": net_avg,
        "liabilities_ratio_value": liabilities_ratio,
        "interest_bearing_debt_ratio_value": debt_ratio,
        "liquid_funds_total": cash,
        "liquid_funds": cash,
        "interest_bearing_debt": debt_used,
        "net_cash": net_cash,
        "quote_source": "yahoo",
        "fundamentals_source": "official-esef",
        "source_file": ";".join(source_files[:20]),
        "source_file_count": len(source_files),
        "coverage": coverage,
        "bsns_year": bsns_year,
        "updated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }


def load_existing_records(path: Path) -> Dict[str, Dict[str, Any]]:
    records: Dict[str, Dict[str, Any]] = {}
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            code = normalize_ticker(str(row.get("code") or ""))
            if code:
                records[code] = row
    return records


def write_jsonl_atomic(path: Path, records: Dict[str, Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for code in sorted(records):
            f.write(json.dumps(records[code], ensure_ascii=False) + "\n")
    if path.exists():
        backup = path.with_suffix(path.suffix + ".bak")
        shutil.copy2(path, backup)
    tmp.replace(path)


def write_audit(path: Path, rows: Sequence[AuditRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ticker", "name", "source", "status", "message", "parsed_facts", "output_record"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(dataclasses.asdict(row))


def universe_placeholder_record(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ticker = normalize_ticker(first_value(row, ("ticker", "symbol", "code", "epic", "ric")))
    name = first_value(row, ("name", "company", "issuer", "issuer_name", "company_name", "security_name")) or ticker
    if not ticker:
        return None
    isin = first_value(row, ("isin", "ISIN"))
    lei = first_value(row, ("lei", "LEI"))
    market = first_value(row, ("market", "lse_market", "segment", "admission_market"))
    instrument_type = first_value(row, ("instrument_type", "type", "security_type", "asset_class", "category"))
    return {
        "country": "UK",
        "code": ticker,
        "name": name,
        "price": "N/A",
        "per": "N/A",
        "pbr": "N/A",
        "liabilities_ratio": "N/A",
        "interest_bearing_debt_ratio": "N/A",
        "net_cash_per_share": "N/A",
        "net_cash_per_share_ratio": "N/A",
        "net_cash_per_share_value": None,
        "sales": "N/A",
        "op_income": "N/A",
        "equity": "N/A",
        "sales_growth_5y": "N/A",
        "op_growth_5y": "N/A",
        "net_income_growth_5y": "N/A",
        "sales_growth_5y_avg_pct": None,
        "op_growth_5y_avg_pct": None,
        "net_income_growth_5y_avg_pct": None,
        "liabilities_ratio_value": None,
        "interest_bearing_debt_ratio_value": None,
        "liquid_funds_total": None,
        "liquid_funds": None,
        "interest_bearing_debt": None,
        "net_cash": None,
        "quote_source": "yahoo",
        "fundamentals_source": "missing",
        "fundamentals_status": "missing_official_fundamentals",
        "quote_status": "unknown",
        "isin": isin,
        "lei": lei,
        "lse_market": market,
        "instrument_type": instrument_type,
        "source_file": "",
        "source_file_count": 0,
        "coverage": {
            "revenue": False,
            "operating_income": False,
            "net_income": False,
            "cash": False,
            "equity": False,
            "liabilities": False,
            "interest_bearing_debt": False,
            "shares": False,
        },
        "bsns_year": "-",
        "updated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }


def seed_universe_placeholders(records: Dict[str, Dict[str, Any]], path: Optional[str], audit: List[AuditRow]) -> int:
    if not path:
        return 0
    count = 0
    for row in read_table(Path(path)):
        record = universe_placeholder_record(row)
        if not record:
            continue
        code = record["code"]
        if code not in records:
            records[code] = record
            count += 1
            audit.append(AuditRow(code, record.get("name", code), str(path), "universe_placeholder", "no official fundamentals matched yet"))
    return count


def dedupe_candidates(candidates: Iterable[FilingCandidate]) -> List[FilingCandidate]:
    seen = set()
    out = []
    for cand in candidates:
        key = (normalize_ticker(cand.ticker), cand.url, str(Path(cand.file_path)).lower() if cand.file_path else "")
        if key in seen:
            continue
        seen.add(key)
        out.append(cand)
    return out


def build(args: argparse.Namespace) -> int:
    universe = load_universe(Path(args.universe_csv)) if args.universe_csv else {}
    candidates: List[FilingCandidate] = []
    if args.manifest:
        for p in args.manifest:
            candidates.extend(candidates_from_manifest(Path(p), universe))
    if args.nsm_csv:
        for p in args.nsm_csv:
            candidates.extend(candidates_from_nsm_csv(Path(p), universe))
    if args.input:
        candidates.extend(candidates_from_inputs(args.input))
    if args.input_dir:
        candidates.extend(candidates_from_dirs(args.input_dir))

    candidates = dedupe_candidates(candidates)
    if args.limit:
        candidates = candidates[: args.limit]
    if not candidates and not args.universe_all_csv:
        print("No candidates. Provide --manifest, --nsm-csv, --input, or --input-dir.", file=sys.stderr)
        return 2

    output_path = Path(args.output)
    records = {} if args.force else load_existing_records(output_path)
    company_facts: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
    company_names: Dict[str, str] = {}
    company_sources: Dict[str, List[str]] = {}
    audit: List[AuditRow] = []

    parsed_docs = 0
    for idx, cand in enumerate(candidates, 1):
        ticker = normalize_ticker(cand.ticker)
        if not ticker and not args.allow_missing_ticker:
            audit.append(AuditRow(ticker, cand.name, cand.url or cand.file_path, "skipped", "missing ticker; provide --universe-csv or ticker in manifest"))
            continue
        if ticker and not args.force and ticker in records and not args.rebuild_existing:
            audit.append(AuditRow(ticker, cand.name, cand.url or cand.file_path, "skipped", "already in output; use --rebuild-existing or --force"))
            continue

        local_path: Optional[Path] = Path(cand.file_path) if cand.file_path else None
        try:
            if cand.url and not local_path:
                local_path = download_candidate(cand, Path(args.download_dir), args.timeout, args.strict_official)
                if args.sleep:
                    time.sleep(args.sleep)
            if not local_path or not local_path.exists():
                raise FileNotFoundError(cand.file_path or cand.url or "no local path")
            docs = read_esef_documents(local_path)
            if not docs:
                raise ValueError(f"no parsable ESEF/iXBRL document in {local_path}")
            doc_success = 0
            for source, text in docs:
                try:
                    facts = parse_xbrl_xml(text, source_rank=idx)
                except Exception as exc:
                    audit.append(AuditRow(ticker, cand.name, source, "parse_error", str(exc)))
                    continue
                fact_count = sum(len(v) for v in facts.values())
                if fact_count == 0:
                    audit.append(AuditRow(ticker, cand.name, source, "empty", "parsed 0 numeric facts"))
                    continue
                key = ticker or normalize_ticker(cand.name)
                if not key:
                    key = safe_filename(local_path.stem).upper()
                merge_facts(company_facts.setdefault(key, {}), facts)
                company_names[key] = cand.name or company_names.get(key) or key
                company_sources.setdefault(key, []).append(source)
                parsed_docs += 1
                doc_success += 1
                audit.append(AuditRow(key, company_names[key], source, "parsed", "ok", fact_count))
            if doc_success == 0:
                audit.append(AuditRow(ticker, cand.name, str(local_path), "failed", "no document parsed successfully"))
        except Exception as exc:
            audit.append(AuditRow(ticker, cand.name, cand.url or cand.file_path, "failed", str(exc)))

    built = 0
    for code, facts in company_facts.items():
        try:
            record = build_cache_record(code, company_names.get(code, code), facts, company_sources.get(code, []), args.years)
            record["fundamentals_status"] = "official_fundamentals_loaded"
            records[code] = record
            built += 1
            audit.append(AuditRow(code, record.get("name", code), record.get("source_file", ""), "record_built", "ok", output_record=json.dumps(record.get("coverage", {}), ensure_ascii=False)))
        except Exception as exc:
            audit.append(AuditRow(code, company_names.get(code, code), "merged_facts", "record_error", str(exc)))

    placeholder_count = seed_universe_placeholders(records, getattr(args, "universe_all_csv", None), audit)

    if built or placeholder_count:
        write_jsonl_atomic(output_path, records)
    write_audit(Path(args.audit_output), audit)

    print(f"Candidates: {len(candidates)}")
    print(f"Parsed documents: {parsed_docs}")
    print(f"Built/updated records: {built}")
    if placeholder_count:
        print(f"Missing official fundamentals placeholders: {placeholder_count}")
    print(f"Output: {output_path}")
    print(f"Audit: {args.audit_output}")
    return 0 if built or placeholder_count else 1


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build UK official ESEF/iXBRL fundamentals cache DB for mr-leon Range Scan")
    parser.add_argument("--manifest", action="append", help="CSV/JSON/JSONL manifest with ticker/name/url/file_path/isin/lei")
    parser.add_argument("--nsm-csv", action="append", help="FCA NSM exported CSV search results containing structured AFR rows and URLs")
    parser.add_argument("--universe-csv", help="Ticker mapping CSV with ticker,name,isin,lei columns")
    parser.add_argument("--universe-all-csv", help="Full exchange universe CSV; rows without matched official filings are written as missing placeholders")
    parser.add_argument("--input", action="append", help="Local ESEF/iXBRL .zip/.xbri/.xhtml/.html/.xml file; repeatable")
    parser.add_argument("--input-dir", action="append", help="Directory to recursively scan for local ESEF/iXBRL files; repeatable")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT), help="Output JSONL path; default data/uk_fundamentals_cache.jsonl")
    parser.add_argument("--download-dir", default=str(DEFAULT_DOWNLOAD_DIR), help="Where downloaded FCA files are stored")
    parser.add_argument("--audit-output", default=str(DEFAULT_AUDIT_OUTPUT), help="Audit CSV output path")
    parser.add_argument("--years", type=int, default=5, help="Growth window in years; default 5")
    parser.add_argument("--limit", type=int, help="Limit candidates for smoke test")
    parser.add_argument("--force", action="store_true", help="Replace output cache instead of updating existing JSONL")
    parser.add_argument("--rebuild-existing", action="store_true", help="Re-parse and overwrite records that already exist in output")
    parser.add_argument("--allow-missing-ticker", action="store_true", help="Build records using inferred name key when ticker is missing; not recommended for Range Scan")
    parser.add_argument("--strict-official", action=argparse.BooleanOptionalAction, default=True, help="Allow downloads only from FCA official domains; default true")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    parser.add_argument("--sleep", type=float, default=0.2, help="Sleep seconds between downloads")
    parser.add_argument("--write-example-manifest", help="Write an example manifest CSV then exit")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    if args.write_example_manifest:
        write_example_manifest(Path(args.write_example_manifest))
        print(f"Wrote example manifest: {args.write_example_manifest}")
        return 0
    return build(args)


if __name__ == "__main__":
    raise SystemExit(main())
