#!/usr/bin/env python3
r"""
collect_uk_ch_and_build_cache.py

mr-leon UK Range Scan용 공식 재무 원천파일 수집기 + 상장 universe 자동 생성기 + 캐시 빌더 실행기.

v2 핵심 변경:
1) LSE Reports 페이지에서 Issuer/Instrument list를 자동 다운로드한다.
2) LSE 상장 issuer/instrument 이름을 Companies House Public Data API로 검색해 company_number를 매핑한다.
3) 확실한 매칭은 uk_universe.csv, 애매한 매칭은 uk_universe_review.csv로 분리한다.
4) 생성된 universe를 기반으로 Companies House Accounts Data Product ZIP에서 iXBRL/XML accounts 파일을 추출한다.
5) build_uk_cache_db.py를 호출해 data/uk_fundamentals_cache.jsonl까지 생성할 수 있다.

중요한 설계 원칙:
- 핵심 재무지표는 공식/구조화 데이터만 사용한다.
- Yahoo는 사용하지 않는다. 가격/PER/PBR 보완은 기존 app.py Range Scan 단계의 역할이다.
- LSE ticker/issuer universe와 Companies House company_number 매핑은 100% 자동 확정이 어렵다.
  따라서 score threshold 이상만 자동 채택하고, 나머지는 review CSV로 남긴다.

Companies House API key:
- --auto-universe를 API 기반으로 쓰려면 Companies House API key가 필요하다.
- 권장: 환경변수 COMPANIES_HOUSE_API_KEY 설정.

PowerShell 사용 예:

    cd C:\mr-leon

    # 1) LSE 전체 상장 universe 자동 생성 + CH accounts 수집 + 캐시 빌드
    python collect_uk_ch_and_build_cache.py `
      --auto-universe `
      --work-dir "C:\Users\tony960816\OneDrive - 특허법인무한\SP" `
      --from-year 2021 --to-year 2026 `
      --include-monthly --include-daily `
      --build-cache

    # 2) 먼저 universe만 생성해서 검토
    python collect_uk_ch_and_build_cache.py `
      --auto-universe `
      --work-dir "C:\Users\tony960816\OneDrive - 특허법인무한\SP" `
      --universe-only

    # 3) 기존처럼 직접 작성한 universe 사용
    python collect_uk_ch_and_build_cache.py `
      --universe-csv "C:\data\uk_universe.csv" `
      --work-dir "C:\Users\tony960816\OneDrive - 특허법인무한\SP" `
      --include-monthly --build-cache

주의:
- Companies House 월별 accounts ZIP은 크다. 전체 기간을 처음부터 돌리기 전에 --max-zip-files 1로 테스트하라.
- LSE/Companies House 명칭 매칭은 법인명 차이, 해외 법인, ETF/펀드/채권, 투자신탁 때문에 누락/오매칭이 발생할 수 있다.
- review CSV는 반드시 확인해야 한다. 자동 채택된 universe만으로도 MVP는 가능하지만, 전체 커버리지는 점진 보정이 필요하다.
"""

from __future__ import annotations

import argparse
import base64
import csv
import dataclasses
import datetime as dt
import difflib
import html
import html.parser
import io
import json
import os
import posixpath
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

# -----------------------------------------------------------------------------
# Official source pages
# -----------------------------------------------------------------------------

CH_DAILY_PAGE = "https://download.companieshouse.gov.uk/en_accountsdata.html"
CH_MONTHLY_PAGE = "https://download.companieshouse.gov.uk/historicmonthlyaccountsdata.html"
LSE_REPORTS_PAGE = "https://www.londonstockexchange.com/reports"
CH_SEARCH_ENDPOINT = "https://api.company-information.service.gov.uk/search/companies"
FCA_NSM_SEARCH_API = "https://api.data.fca.org.uk/search"
FCA_ARTEFACT_BASE = "https://data.fca.org.uk/artefacts/"
LSE_AUTOCOMPLETE_URL = "https://api.londonstockexchange.com/api/gw/lse/search/autocomplete"
LSE_API_BASE = "https://api.londonstockexchange.com"

USER_AGENT = "mr-leon-uk-cache-builder/2.0 (+official LSE/Companies House collector)"
DEFAULT_WORK_DIR = Path(r"C:\Users\tony960816\OneDrive - 특허법인무한\SP\mr-leon-uk-cache")

ZIP_RE = re.compile(r"\.zip(?:$|[?#])", re.I)
DAILY_DATE_RE = re.compile(r"Accounts[_-]Bulk[_-]Data[-_](\d{4})-(\d{2})-(\d{2})\.zip", re.I)
MONTHLY_RE = re.compile(r"Accounts[_-]Monthly[_-]Data[-_]([A-Za-z]+)(\d{4})\.zip", re.I)
MONTHLY_YEAR_RE = re.compile(r"Accounts[_-]Monthly[_-]Data[-_]Jan(?:uary)?ToDec(\d{4})\.zip", re.I)
INNER_ACCOUNT_RE = re.compile(r"(?:^|[_-])(\d{8})(?:[_-])(\d{8})(?:\.|[_-]).*\.(html|xml|zip)$", re.I)

TABLE_EXTENSIONS = (".csv", ".tsv", ".txt", ".xlsx", ".zip")

MONTHS = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12,
}

LEGAL_WORDS = {
    "PLC", "P.L.C", "PUBLIC", "LIMITED", "COMPANY", "LTD", "GROUP", "HOLDINGS", "HOLDING",
    "INC", "CORP", "CORPORATION", "SA", "AG", "NV", "SE", "LP", "LLP", "THE", "CO",
}

NON_EQUITY_HINTS = (
    "BOND", "BONDS", "NOTE", "NOTES", "DEBT", "WARRANT", "CERTIFICATE", "GILT", "TREASURY",
    "ETF", "ETC", "ETN", "EQUITY TRADED FUND", "EXCHANGE TRADED", "STRUCTURED PRODUCT",
    "COVERED WARRANT", "PREFERENCE", "PREF", "RIGHTS", "UNIT", "FUND", "TRUST", "REIT",
)

EQUITY_HINTS = (
    "ORDINARY", "ORD", "COMMON", "EQUITY", "SHARE", "SHARES", "PLC", "PUBLIC LIMITED COMPANY",
)

# -----------------------------------------------------------------------------
# Data classes
# -----------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Company:
    ticker: str
    company_number: str
    name: str = ""
    isin: str = ""
    lei: str = ""
    lse_market: str = ""
    lse_source: str = ""
    match_score: float = 0.0


@dataclasses.dataclass(frozen=True)
class BulkZip:
    url: str
    kind: str  # daily | monthly | yearly
    date: dt.date
    label: str


@dataclasses.dataclass
class ExtractedAccount:
    company: Company
    source_zip: str
    inner_name: str
    file_path: str
    fiscal_year: Optional[int]
    url: str = ""


@dataclasses.dataclass
class LseRow:
    ticker: str = ""
    name: str = ""
    isin: str = ""
    lei: str = ""
    market: str = ""
    instrument_type: str = ""
    raw: Dict[str, str] = dataclasses.field(default_factory=dict)
    source: str = ""


@dataclasses.dataclass
class ChCandidate:
    company_number: str
    title: str
    company_status: str = ""
    company_type: str = ""
    score: float = 0.0
    address_snippet: str = ""


@dataclasses.dataclass
class UniverseBuildResult:
    accepted: List[Company]
    review_rows: List[Dict[str, Any]]
    raw_rows: List[LseRow]


class LinkParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: List[Tuple[str, str]] = []
        self._in_a = False
        self._href = ""
        self._text_parts: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag.lower() != "a":
            return
        self._in_a = True
        self._href = ""
        self._text_parts = []
        for key, value in attrs:
            if key.lower() == "href" and value:
                self._href = value

    def handle_data(self, data: str) -> None:
        if self._in_a:
            self._text_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "a" and self._in_a:
            if self._href:
                self.links.append((self._href, " ".join(self._text_parts).strip()))
            self._in_a = False
            self._href = ""
            self._text_parts = []

# -----------------------------------------------------------------------------
# Basic helpers
# -----------------------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def norm_col(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())


def normalize_company_number(value: str) -> str:
    text = str(value or "").strip().upper()
    text = re.sub(r"[^0-9A-Z]", "", text)
    if text.isdigit():
        return text.zfill(8)[-8:]
    return text


def normalize_ticker(value: str) -> str:
    text = str(value or "").strip().upper()
    for suffix in (".L", ".IL", ".GB"):
        if text.endswith(suffix):
            text = text[: -len(suffix)]
    text = text.rstrip(".")
    return re.sub(r"\s+", "", text)


def safe_name(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value or "").strip())
    return text[:180] or "file"


def normalize_name_for_match(name: str) -> str:
    text = html.unescape(str(name or "")).upper()
    text = text.replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    words = [w for w in text.split() if w and w not in LEGAL_WORDS]
    return " ".join(words)


def compact_name(name: str) -> str:
    return re.sub(r"[^A-Z0-9]+", "", normalize_name_for_match(name))


def name_matches_company(source_name: str, target_name: str) -> bool:
    return nsm_company_match_score(source_name, target_name) >= 0.82


def normalize_nsm_company_name(name: str) -> str:
    text = html.unescape(str(name or "")).upper()
    text = text.replace("&", " AND ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return " ".join(text.split())


def nsm_company_match_score(source_name: str, target_name: str) -> float:
    source = normalize_nsm_company_name(source_name).rstrip(";")
    target = normalize_nsm_company_name(target_name).rstrip(";")
    if not source or not target:
        return 0.0
    if source == target:
        return 1.0
    compact_source = re.sub(r"[^A-Z0-9]+", "", source)
    compact_target = re.sub(r"[^A-Z0-9]+", "", target)
    if compact_source == compact_target:
        return 0.99
    return difflib.SequenceMatcher(None, source, target).ratio()


def nsm_report_key(report: Dict[str, Any]) -> str:
    lei = str(report.get("lei") or "").strip().upper()
    if lei:
        return f"lei:{lei}"
    return f"name:{compact_name(str(report.get('company') or ''))}"


def ticker_from_lse_url(url: str) -> str:
    match = re.search(r"/stock/([^/]+)/", str(url or ""), re.I)
    return normalize_ticker(match.group(1)) if match else ""


def lse_autocomplete(query: str, timeout: int, size: int = 8) -> Dict[str, Any]:
    params = urllib.parse.urlencode({"q": query, "size": str(size)})
    req = urllib.request.Request(
        f"{LSE_AUTOCOMPLETE_URL}?{params}",
        headers={
            "User-Agent": USER_AGENT,
            "Origin": "https://www.londonstockexchange.com",
            "Referer": "https://www.londonstockexchange.com/",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def choose_lse_ticker_from_autocomplete(payload: Dict[str, Any], company_name: str) -> Tuple[str, str, float]:
    candidates: List[Tuple[float, str, str]] = []
    for item in payload.get("instruments") or []:
        category = str(item.get("category") or "").upper()
        if category and category != "EQUITY":
            continue
        ticker = normalize_ticker(item.get("tidm") or item.get("code") or ticker_from_lse_url(item.get("url", "")))
        if not ticker or ":" in ticker:
            continue
        item_name = str(item.get("issuername") or item.get("description") or "")
        score = nsm_company_match_score(item_name, company_name)
        if score >= 0.72:
            lse_bonus = 0.08 if item.get("islse") is True else 0.0
            candidates.append((1.0 + score + lse_bonus, ticker, "lse_instrument"))
    for item in payload.get("issuers") or []:
        ticker = normalize_ticker(item.get("tidm") or item.get("symbol") or ticker_from_lse_url(item.get("url", "")))
        if not ticker or ":" in ticker:
            continue
        score = nsm_company_match_score(str(item.get("name") or ""), company_name)
        if score >= 0.72:
            candidates.append((0.85 + score, ticker, "lse_issuer"))
    for item in payload.get("news") or []:
        ticker = normalize_ticker(item.get("companycode") or "")
        if not ticker or ":" in ticker or ticker == "MARKET-NEWS":
            continue
        score = nsm_company_match_score(str(item.get("companyname") or ""), company_name)
        if score >= 0.82:
            candidates.append((0.65 + score, ticker, "lse_news"))
    if not candidates:
        return "", "not_found", 0.0
    candidates.sort(key=lambda row: row[0], reverse=True)
    score, ticker, source = candidates[0]
    return ticker, source, score


def resolve_lse_ticker(company_name: str, timeout: int) -> Tuple[str, str, float]:
    for query in query_variants_for_lse_name(company_name):
        try:
            ticker, source, score = choose_lse_ticker_from_autocomplete(lse_autocomplete(query, timeout), company_name)
            if ticker:
                return ticker, source, score
        except Exception:
            continue
    return "", "not_found", 0.0


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


def parse_date(value: Optional[str]) -> Optional[dt.date]:
    if not value:
        return None
    return dt.date.fromisoformat(value)


def make_request(url: str, timeout: int, headers: Optional[Dict[str, str]] = None) -> urllib.request.Request:
    merged = {"User-Agent": USER_AGENT}
    if headers:
        merged.update(headers)
    return urllib.request.Request(url, headers=merged)


def read_url_bytes(url: str, timeout: int, headers: Optional[Dict[str, str]] = None) -> bytes:
    req = make_request(url, timeout=timeout, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def read_text_url(url: str, timeout: int, headers: Optional[Dict[str, str]] = None) -> str:
    req = make_request(url, timeout=timeout, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        charset = resp.headers.get_content_charset() or "utf-8"
        return data.decode(charset, errors="replace")


def download_file(url: str, dest: Path, timeout: int, chunk_size: int = 1024 * 1024, headers: Optional[Dict[str, str]] = None) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    req = make_request(url, timeout=timeout, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp, tmp.open("wb") as f:
        shutil.copyfileobj(resp, f, length=chunk_size)
    tmp.replace(dest)

# -----------------------------------------------------------------------------
# LSE universe discovery and parsing
# -----------------------------------------------------------------------------

def discover_links(page_url: str, timeout: int) -> List[Tuple[str, str]]:
    text = read_text_url(page_url, timeout)
    parser = LinkParser()
    parser.feed(text)
    seen = set()
    out: List[Tuple[str, str]] = []
    for href, label in parser.links:
        url = urllib.parse.urljoin(page_url, href)
        key = (url, label)
        if key in seen:
            continue
        seen.add(key)
        out.append((url, label))
    return out


def discover_lse_report_url(kind: str, timeout: int) -> str:
    """Best-effort discovery of current LSE issuer/instrument report URL."""
    links = discover_links(LSE_REPORTS_PAGE, timeout)
    kind = kind.lower().strip()
    scored: List[Tuple[int, str, str]] = []
    for url, label in links:
        text = f"{label} {url}".lower()
        if not any(ext in text for ext in (".csv", ".xlsx", ".xls", ".zip", "download")):
            continue
        score = 0
        if kind.startswith("instrument"):
            if "instrument" in text:
                score += 20
            if "issuer" in text:
                score += 5
        else:
            if "issuer" in text:
                score += 20
            if "instrument" in text:
                score += 5
        if "archive" in text or re.search(r"20\d{2}\.zip", text):
            score -= 10
        if "download" in text:
            score += 3
        if score > 0:
            scored.append((score, url, label))
    if not scored:
        api_url = discover_lse_report_url_from_api(kind, timeout)
        if api_url:
            return api_url
        raise RuntimeError("Could not discover LSE report download URL. Pass --lse-report-file or --lse-report-url.")
    scored.sort(key=lambda x: x[0], reverse=True)
    return scored[0][1]


def lse_api_json(method: str, path: str, timeout: int, payload: Optional[Dict[str, Any]] = None) -> Any:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(
        urllib.parse.urljoin(LSE_API_BASE, path),
        data=data,
        headers={
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Origin": "https://www.londonstockexchange.com",
            "Referer": "https://www.londonstockexchange.com/reports",
        },
        method=method.upper(),
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def discover_lse_report_url_from_api(kind: str, timeout: int) -> str:
    page = lse_api_json("GET", "/api/v1/pages?path=reports&parameters=", timeout)
    target_label = "Instruments" if kind.startswith("instrument") else "Issuers"
    target = None
    for component in page.get("components") or []:
        for content in component.get("content") or []:
            value = content.get("value") or {}
            for group in value.get("reportsFilterToggleFilters") or []:
                for sub in group.get("subFilters") or []:
                    if str(sub.get("label") or "").strip().lower() == target_label.lower():
                        modules = sub.get("modules") or []
                        if modules:
                            target = {
                                "tab_id": sub.get("tabId"),
                                "module_id": modules[0].get("moduleId"),
                            }
                            break
                if target:
                    break
        if target:
            break
    if not target:
        return ""
    payload = {
        "path": "reports",
        "parameters": urllib.parse.urlencode({"tab": target_label.lower(), "tabId": target["tab_id"]}),
        "components": [{"componentId": target["module_id"], "parameters": None}],
    }
    refreshed = lse_api_json("POST", "/api/v1/components/refresh", timeout, payload=payload)
    for component in refreshed or []:
        for content in component.get("content") or []:
            value = content.get("value") or {}
            for item in value.get("ctaItems") or []:
                title = str(item.get("ctaTitle") or "").lower()
                button = item.get("ctaButton") or {}
                link = str(button.get("link") or "")
                if link and target_label.lower().rstrip("s") in title:
                    return link
    return ""


def download_lse_report(args: argparse.Namespace) -> Path:
    work_dir = Path(args.work_dir)
    lse_dir = work_dir / "lse_reports"
    lse_dir.mkdir(parents=True, exist_ok=True)

    if args.lse_report_file:
        path = Path(args.lse_report_file)
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    url = args.lse_report_url
    if not url:
        log(f"Discovering LSE {args.lse_report_kind} report URL...")
        url = discover_lse_report_url(args.lse_report_kind, args.timeout)
    parsed = urllib.parse.urlparse(url)
    name = Path(parsed.path).name or f"lse_{args.lse_report_kind}_report"
    if not Path(name).suffix:
        name += ".download"
    dest = lse_dir / safe_name(name)
    if not dest.exists() or dest.stat().st_size == 0 or args.force_lse_download:
        log(f"Downloading LSE report: {url}")
        download_file(url, dest, args.timeout)
    else:
        log(f"Reusing LSE report: {dest}")
    return dest


def decode_csv_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("latin-1", errors="replace")


def sniff_delimiter(text: str) -> str:
    sample = text[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"]).delimiter
    except Exception:
        return "\t" if "\t" in sample and sample.count("\t") > sample.count(",") else ","


def read_csv_table(path: Path, data: Optional[bytes] = None) -> List[Dict[str, str]]:
    raw = data if data is not None else path.read_bytes()
    text = decode_csv_bytes(raw)
    delimiter = sniff_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    return [{str(k or "").strip(): str(v or "").strip() for k, v in row.items()} for row in reader]


def read_xlsx_shared_strings(zf: zipfile.ZipFile) -> List[str]:
    try:
        root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
    except KeyError:
        return []
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    out = []
    for si in root.findall("m:si", ns):
        parts = []
        for t in si.findall(".//m:t", ns):
            parts.append(t.text or "")
        out.append("".join(parts))
    return out


def column_letters_to_index(ref: str) -> int:
    letters = re.sub(r"[^A-Z]", "", ref.upper())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return max(idx - 1, 0)


def read_xlsx_first_sheet(path: Path, data: Optional[bytes] = None) -> List[Dict[str, str]]:
    raw = data if data is not None else path.read_bytes()
    with zipfile.ZipFile(io.BytesIO(raw)) as zf:
        shared = read_xlsx_shared_strings(zf)
        # Prefer first worksheet. LSE reports usually put the table there.
        sheet_names = sorted([n for n in zf.namelist() if re.match(r"xl/worksheets/sheet\d+\.xml$", n)])
        if not sheet_names:
            return []
        root = ET.fromstring(zf.read(sheet_names[0]))
        ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        rows: List[List[str]] = []
        for row_el in root.findall(".//m:sheetData/m:row", ns):
            row_vals: List[str] = []
            for c in row_el.findall("m:c", ns):
                ref = c.attrib.get("r", "A1")
                idx = column_letters_to_index(ref)
                while len(row_vals) <= idx:
                    row_vals.append("")
                cell_type = c.attrib.get("t", "")
                v = c.find("m:v", ns)
                if v is None or v.text is None:
                    value = ""
                elif cell_type == "s":
                    try:
                        value = shared[int(v.text)]
                    except Exception:
                        value = v.text
                elif cell_type == "inlineStr":
                    value = "".join(t.text or "" for t in c.findall(".//m:t", ns))
                else:
                    value = v.text
                row_vals[idx] = str(value).strip()
            if any(row_vals):
                rows.append(row_vals)
    if not rows:
        return []
    # Find header row by scoring likely column names.
    best_i = 0
    best_score = -1
    for i, row in enumerate(rows[:30]):
        joined = " ".join(row).lower()
        score = sum(word in joined for word in ["issuer", "instrument", "company", "name", "isin", "ticker", "sedol", "code"])
        if score > best_score:
            best_score = score
            best_i = i
    header = [str(x or "").strip() for x in rows[best_i]]
    out: List[Dict[str, str]] = []
    for row in rows[best_i + 1:]:
        d = {}
        for j, h in enumerate(header):
            if not h:
                h = f"col_{j+1}"
            d[h] = row[j] if j < len(row) else ""
        if any(v for v in d.values()):
            out.append(d)
    return out


def read_table_file(path: Path) -> List[Dict[str, str]]:
    suffix = path.suffix.lower()
    if suffix in (".csv", ".tsv", ".txt"):
        return read_csv_table(path)
    if suffix == ".xlsx":
        return read_xlsx_first_sheet(path)
    if suffix == ".zip":
        rows: List[Dict[str, str]] = []
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if not n.endswith("/")]
            candidates = [n for n in names if Path(n).suffix.lower() in (".csv", ".tsv", ".txt", ".xlsx")]
            # Prefer issuer/instrument looking files.
            candidates.sort(key=lambda n: (0 if re.search(r"issuer|instrument", n, re.I) else 1, n))
            for name in candidates[:3]:
                data = zf.read(name)
                ext = Path(name).suffix.lower()
                try:
                    if ext == ".xlsx":
                        rows.extend(read_xlsx_first_sheet(Path(name), data=data))
                    else:
                        rows.extend(read_csv_table(Path(name), data=data))
                except Exception as exc:
                    log(f"Could not parse {name} inside {path.name}: {exc}")
        return rows
    raise ValueError(f"Unsupported LSE report file: {path}")


def row_to_lse_row(row: Dict[str, str], source: str) -> LseRow:
    ticker = first_value(row, [
        "ticker", "tidm", "tradable instrument display mnemonic", "mnemonic", "symbol", "code", "trading code",
        "stock symbol", "epic",
    ])
    name = first_value(row, [
        "issuer name", "issuer", "company name", "company", "name", "security name", "instrument name",
        "issuer/instrument name",
    ])
    if not name:
        name = any_value_by_contains(row, ["issuer", "company", "instrumentname", "securityname"])
    isin = first_value(row, ["isin", "isin code", "instrument isin"])
    lei = first_value(row, ["lei", "issuer lei", "legal entity identifier"])
    market = first_value(row, [
        "market", "market name", "lse market", "segment", "trading service", "exchange market size",
        "admission market", "market segment code", "market sector code", "fca listing category",
    ])
    instrument_type = first_value(row, [
        "instrument type", "security type", "type", "asset class", "category", "classification",
        "mifir identifier name", "mifir identifier code", "instrument name",
    ])
    return LseRow(
        ticker=normalize_ticker(ticker),
        name=name.strip(),
        isin=isin.strip(),
        lei=lei.strip(),
        market=market.strip(),
        instrument_type=instrument_type.strip(),
        raw=row,
        source=source,
    )


def is_equity_like_lse_row(item: LseRow, include_funds: bool, include_non_equity: bool) -> bool:
    if include_non_equity:
        return True
    text = " ".join([item.name, item.instrument_type, item.market, " ".join(item.raw.values())]).upper()
    if not include_funds and any(hint in text for hint in ("ETF", "ETC", "ETN", "FUND", "TRUST", "REIT")):
        return False
    if any(hint in text for hint in ("BOND", "NOTE", "DEBT", "WARRANT", "GILT", "TREASURY", "CERTIFICATE")):
        return False
    # If the report has no explicit type, keep rows with a plausible issuer name.
    if not item.instrument_type:
        return bool(item.name)
    if any(hint in text for hint in EQUITY_HINTS):
        return True
    # Avoid over-filtering: LSE issuer reports may not include type; keep named rows.
    return bool(item.name and not any(hint in text for hint in NON_EQUITY_HINTS))


def load_lse_rows(args: argparse.Namespace) -> List[LseRow]:
    report_path = download_lse_report(args)
    raw_rows = read_table_file(report_path)
    log(f"LSE report rows parsed: {len(raw_rows)} from {report_path}")
    out: List[LseRow] = []
    seen = set()
    for row in raw_rows:
        item = row_to_lse_row(row, source=str(report_path))
        if not item.name:
            continue
        if not is_equity_like_lse_row(item, args.include_funds, args.include_non_equity):
            continue
        key = (compact_name(item.name), item.isin or item.ticker)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    log(f"LSE equity-like issuer/instrument rows: {len(out)}")
    return out

# -----------------------------------------------------------------------------
# Companies House mapping
# -----------------------------------------------------------------------------

def ch_auth_header(api_key: str) -> Dict[str, str]:
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def score_company_match(lse_name: str, ch_title: str) -> float:
    a = normalize_name_for_match(lse_name)
    b = normalize_name_for_match(ch_title)
    if not a or not b:
        return 0.0
    seq = difflib.SequenceMatcher(None, a, b).ratio()
    ac = compact_name(lse_name)
    bc = compact_name(ch_title)
    prefix_bonus = 0.0
    if ac and bc and (ac.startswith(bc[: min(len(bc), 8)]) or bc.startswith(ac[: min(len(ac), 8)])):
        prefix_bonus = 0.05
    words_a = set(a.split())
    words_b = set(b.split())
    jacc = len(words_a & words_b) / max(len(words_a | words_b), 1)
    exact_bonus = 0.1 if ac == bc else 0.0
    return min(1.0, (seq * 0.65) + (jacc * 0.30) + prefix_bonus + exact_bonus)


def search_companies_house(api_key: str, query: str, timeout: int, items_per_page: int = 5) -> List[ChCandidate]:
    q = urllib.parse.urlencode({"q": query, "items_per_page": str(items_per_page), "restrictions": "active-companies"})
    url = f"{CH_SEARCH_ENDPOINT}?{q}"
    headers = ch_auth_header(api_key)
    text = read_text_url(url, timeout=timeout, headers=headers)
    data = json.loads(text)
    out: List[ChCandidate] = []
    for item in data.get("items", []) or []:
        title = str(item.get("title") or "")
        cn = normalize_company_number(str(item.get("company_number") or ""))
        if not cn:
            continue
        address = item.get("address_snippet") or ""
        out.append(ChCandidate(
            company_number=cn,
            title=title,
            company_status=str(item.get("company_status") or ""),
            company_type=str(item.get("company_type") or ""),
            address_snippet=str(address),
        ))
    return out


def query_variants_for_lse_name(name: str) -> List[str]:
    variants = []
    raw = re.sub(r"\s+", " ", str(name or "")).strip()
    if raw:
        variants.append(raw)
    cleaned = normalize_name_for_match(raw)
    if cleaned and cleaned not in variants:
        variants.append(cleaned)
    # Often LSE names include share class or currency tails; try a shorter stem.
    stem = re.split(r"\b(?:ORD|ORDINARY|SHARES?|GBP|GBX|USD|EUR|CLASS|A|B|C)\b", raw, maxsplit=1, flags=re.I)[0].strip()
    if stem and stem not in variants:
        variants.append(stem)
    # Keep only first 3 meaningful variants.
    deduped = []
    for v in variants:
        v = v.strip()
        if len(v) >= 3 and v not in deduped:
            deduped.append(v)
    return deduped[:3]


def build_universe_from_lse(args: argparse.Namespace) -> UniverseBuildResult:
    api_key = args.ch_api_key or os.getenv(args.ch_api_key_env or "COMPANIES_HOUSE_API_KEY", "")
    rows = load_lse_rows(args)

    raw_output = Path(args.lse_raw_output) if args.lse_raw_output else Path(args.work_dir) / "uk_lse_raw_rows.csv"
    write_lse_raw_rows(raw_output, rows)
    log(f"LSE raw rows: {raw_output}")

    if not api_key:
        raise SystemExit(
            "--auto-universe requires a Companies House API key for company_number mapping. "
            "Set COMPANIES_HOUSE_API_KEY or pass --ch-api-key. Raw LSE rows were written for review."
        )

    accepted: List[Company] = []
    review_rows: List[Dict[str, Any]] = []
    seen_company_numbers = set()
    seen_lse_keys = set()

    limit = args.max_lse_rows or len(rows)
    log(f"Mapping LSE rows to Companies House company_number: {min(limit, len(rows))} rows")

    for idx, item in enumerate(rows[:limit], 1):
        if idx % 50 == 0:
            log(f"  mapped {idx}/{min(limit, len(rows))}...")
        lse_key = (compact_name(item.name), item.isin or item.ticker)
        if lse_key in seen_lse_keys:
            continue
        seen_lse_keys.add(lse_key)

        candidates: List[ChCandidate] = []
        last_error = ""
        for query in query_variants_for_lse_name(item.name):
            try:
                candidates = search_companies_house(api_key, query, args.timeout, items_per_page=args.ch_items_per_page)
                if candidates:
                    break
            except urllib.error.HTTPError as exc:
                last_error = f"HTTP {exc.code}"
                if exc.code == 401:
                    raise SystemExit("Companies House API key rejected: HTTP 401 Unauthorized")
                time.sleep(args.ch_sleep)
            except Exception as exc:
                last_error = str(exc)
                time.sleep(args.ch_sleep)

        for c in candidates:
            c.score = score_company_match(item.name, c.title)
        candidates.sort(key=lambda x: x.score, reverse=True)
        best = candidates[0] if candidates else None

        if best and best.score >= args.match_threshold and best.company_number not in seen_company_numbers:
            seen_company_numbers.add(best.company_number)
            accepted.append(Company(
                ticker=item.ticker or best.company_number,
                company_number=best.company_number,
                name=best.title or item.name,
                isin=item.isin,
                lei=item.lei,
                lse_market=item.market,
                lse_source=item.source,
                match_score=best.score,
            ))
        else:
            review_rows.append({
                "ticker": item.ticker,
                "lse_name": item.name,
                "isin": item.isin,
                "lei": item.lei,
                "market": item.market,
                "instrument_type": item.instrument_type,
                "status": "no_match" if not best else "low_score_or_duplicate",
                "best_company_number": best.company_number if best else "",
                "best_title": best.title if best else "",
                "best_score": f"{best.score:.4f}" if best else "",
                "best_status": best.company_status if best else "",
                "error": last_error,
                "candidates_json": json.dumps([dataclasses.asdict(c) for c in candidates[:5]], ensure_ascii=False),
            })
        if args.ch_sleep:
            time.sleep(args.ch_sleep)

    return UniverseBuildResult(accepted=accepted, review_rows=review_rows, raw_rows=rows)


def map_lse_rows_to_companies_house(
    args: argparse.Namespace,
    rows: Sequence[LseRow],
    *,
    accepted_output: Optional[Path] = None,
    review_output: Optional[Path] = None,
) -> UniverseBuildResult:
    api_key = args.ch_api_key or os.getenv(args.ch_api_key_env or "COMPANIES_HOUSE_API_KEY", "")
    if not api_key:
        raise SystemExit(
            "Companies House backfill requires a Companies House API key. "
            "Set COMPANIES_HOUSE_API_KEY or pass --ch-api-key."
        )

    accepted: List[Company] = []
    review_rows: List[Dict[str, Any]] = []
    seen_company_numbers = set()
    seen_lse_keys = set()
    limit = args.max_ch_backfill_rows or len(rows)
    total = min(limit, len(rows))
    log(f"CH backfill mapping missing LSE rows to company_number: {total} rows")

    for idx, item in enumerate(rows[:limit], 1):
        if idx % 50 == 0:
            log(f"  CH backfill mapped {idx}/{total}...")
        lse_key = (compact_name(item.name), item.isin or item.ticker)
        if lse_key in seen_lse_keys:
            continue
        seen_lse_keys.add(lse_key)

        candidates: List[ChCandidate] = []
        last_error = ""
        for query in query_variants_for_lse_name(item.name):
            try:
                candidates = search_companies_house(api_key, query, args.timeout, items_per_page=args.ch_items_per_page)
                if candidates:
                    break
            except urllib.error.HTTPError as exc:
                last_error = f"HTTP {exc.code}"
                if exc.code == 401:
                    raise SystemExit("Companies House API key rejected: HTTP 401 Unauthorized")
                time.sleep(args.ch_sleep)
            except Exception as exc:
                last_error = str(exc)
                time.sleep(args.ch_sleep)

        for c in candidates:
            c.score = score_company_match(item.name, c.title)
        candidates.sort(key=lambda x: x.score, reverse=True)
        best = candidates[0] if candidates else None

        if best and best.score >= args.match_threshold and best.company_number not in seen_company_numbers:
            seen_company_numbers.add(best.company_number)
            accepted.append(Company(
                ticker=item.ticker or best.company_number,
                company_number=best.company_number,
                name=best.title or item.name,
                isin=item.isin,
                lei=item.lei,
                lse_market=item.market,
                lse_source=item.source,
                match_score=best.score,
            ))
        else:
            review_rows.append({
                "ticker": item.ticker,
                "lse_name": item.name,
                "isin": item.isin,
                "lei": item.lei,
                "market": item.market,
                "instrument_type": item.instrument_type,
                "status": "no_ch_match" if not best else "low_score_or_duplicate",
                "best_company_number": best.company_number if best else "",
                "best_title": best.title if best else "",
                "best_score": f"{best.score:.4f}" if best else "",
                "best_status": best.company_status if best else "",
                "error": last_error,
                "candidates_json": json.dumps([dataclasses.asdict(c) for c in candidates[:5]], ensure_ascii=False),
            })
        if args.ch_sleep:
            time.sleep(args.ch_sleep)

    if accepted_output:
        write_universe_csv(accepted_output, accepted)
        log(f"CH backfill accepted universe: {len(accepted)} -> {accepted_output}")
    if review_output:
        write_review_csv(review_output, review_rows)
        log(f"CH backfill review rows: {len(review_rows)} -> {review_output}")

    return UniverseBuildResult(accepted=accepted, review_rows=review_rows, raw_rows=list(rows))


def write_lse_raw_rows(path: Path, rows: Sequence[LseRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    base_fields = ["ticker", "name", "isin", "lei", "market", "instrument_type", "source"]
    raw_keys: List[str] = []
    seen = set()
    for item in rows[:5000]:
        for k in item.raw.keys():
            if k not in seen:
                seen.add(k)
                raw_keys.append(k)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=base_fields + [f"raw_{k}" for k in raw_keys])
        writer.writeheader()
        for item in rows:
            row = {
                "ticker": item.ticker,
                "name": item.name,
                "isin": item.isin,
                "lei": item.lei,
                "market": item.market,
                "instrument_type": item.instrument_type,
                "source": item.source,
            }
            for k in raw_keys:
                row[f"raw_{k}"] = item.raw.get(k, "")
            writer.writerow(row)


def lse_row_key(item: LseRow) -> str:
    if item.isin:
        return f"isin:{item.isin.upper()}"
    if item.ticker:
        return f"ticker:{item.ticker}"
    return f"name:{compact_name(item.name)}"


def match_nsm_report_for_lse_row(item: LseRow, reports: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    indexes = build_nsm_match_indexes(reports)
    return match_nsm_report_for_lse_row_indexed(item, indexes)


def latest_nsm_reports(reports: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest: Dict[str, Dict[str, Any]] = {}
    for report in reports:
        if not str(report.get("download_link") or "").strip():
            continue
        key = nsm_report_key(report)
        if key and key not in latest:
            latest[key] = report
    return list(latest.values())


def build_nsm_match_indexes(reports: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    latest = latest_nsm_reports(reports)
    by_lei: Dict[str, Dict[str, Any]] = {}
    by_compact: Dict[str, Dict[str, Any]] = {}
    compact_pairs: List[Tuple[str, Dict[str, Any]]] = []
    for report in latest:
        lei = str(report.get("lei") or "").strip().upper()
        name = str(report.get("company") or "").strip().rstrip(";")
        compact = compact_name(name)
        if lei:
            by_lei.setdefault(lei, report)
        if compact:
            by_compact.setdefault(compact, report)
            compact_pairs.append((compact, report))
    return {"by_lei": by_lei, "by_compact": by_compact, "compact_pairs": compact_pairs}


def match_nsm_report_for_lse_row_indexed(item: LseRow, indexes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    lei = item.lei.strip().upper()
    if lei and lei in indexes["by_lei"]:
        return indexes["by_lei"][lei]
    target = compact_name(item.name)
    if not target:
        return None
    exact = indexes["by_compact"].get(target)
    if exact:
        return exact
    for report_compact, report in indexes["compact_pairs"]:
        if len(target) >= 8 and len(report_compact) >= 8 and (target in report_compact or report_compact in target):
            return report
    return None


def collect_lse_full_nsm_esef_accounts(
    lse_rows: Sequence[LseRow],
    reports: Sequence[Dict[str, Any]],
    audit_path: Path,
) -> List[ExtractedAccount]:
    extracted: List[ExtractedAccount] = []
    audit_rows: List[Dict[str, Any]] = []
    indexes = build_nsm_match_indexes(reports)
    seen_lse = set()
    total = len(lse_rows)
    for idx, item in enumerate(lse_rows, 1):
        if idx % 500 == 0:
            log(f"LSE full universe matching... {idx}/{total}")
        key = lse_row_key(item)
        if key in seen_lse:
            continue
        seen_lse.add(key)
        report = match_nsm_report_for_lse_row_indexed(item, indexes)
        if not report:
            audit_rows.append(
                {
                    "ticker": item.ticker,
                    "name": item.name,
                    "isin": item.isin,
                    "lei": item.lei,
                    "market": item.market,
                    "instrument_type": item.instrument_type,
                    "status": "missing_nsm_tagged_esef",
                    "matched_company": "",
                    "matched_lei": "",
                    "url": "",
                }
            )
            continue
        raw_url = str(report.get("download_link") or "").strip()
        url = urllib.parse.urljoin(FCA_ARTEFACT_BASE, raw_url)
        audit_status = "matched_nsm_tagged_esef"
        fiscal_year = None
        document_date = str(report.get("document_date") or "")
        if re.match(r"\d{4}", document_date):
            fiscal_year = int(document_date[:4])
        extracted.append(
            ExtractedAccount(
                company=Company(
                    ticker=item.ticker or str(report.get("lei") or "").strip().upper() or normalize_ticker(item.name),
                    company_number="",
                    name=str(report.get("company") or item.name).strip().rstrip(";"),
                    isin=item.isin,
                    lei=str(report.get("lei") or item.lei).strip(),
                    lse_market=item.market,
                    lse_source=item.source,
                ),
                source_zip="fca-nsm",
                inner_name=str(report.get("disclosure_id") or report.get("seq_id") or ""),
                file_path="",
                fiscal_year=fiscal_year,
                url=url,
            )
        )
        audit_rows.append(
            {
                "ticker": item.ticker,
                "name": item.name,
                "isin": item.isin,
                "lei": item.lei,
                "market": item.market,
                "instrument_type": item.instrument_type,
                "status": audit_status,
                "matched_company": str(report.get("company") or "").strip().rstrip(";"),
                "matched_lei": str(report.get("lei") or "").strip(),
                "url": url,
            }
        )
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    with audit_path.open("w", encoding="utf-8-sig", newline="") as f:
        fields = ["ticker", "name", "isin", "lei", "market", "instrument_type", "status", "matched_company", "matched_lei", "url"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in audit_rows:
            writer.writerow(row)
    log(f"LSE full universe audit: {audit_path}")
    return extracted


def lse_rows_missing_nsm_esef(lse_rows: Sequence[LseRow], reports: Sequence[Dict[str, Any]]) -> List[LseRow]:
    indexes = build_nsm_match_indexes(reports)
    missing: List[LseRow] = []
    seen_lse = set()
    for item in lse_rows:
        key = lse_row_key(item)
        if key in seen_lse:
            continue
        seen_lse.add(key)
        if not match_nsm_report_for_lse_row_indexed(item, indexes):
            missing.append(item)
    return missing


def write_universe_csv(path: Path, companies: Sequence[Company]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ticker", "name", "company_number", "isin", "lei", "lse_market", "match_score", "lse_source"],
        )
        writer.writeheader()
        for c in companies:
            writer.writerow({
                "ticker": c.ticker,
                "name": c.name,
                "company_number": c.company_number,
                "isin": c.isin,
                "lei": c.lei,
                "lse_market": c.lse_market,
                "match_score": f"{c.match_score:.4f}" if c.match_score else "",
                "lse_source": c.lse_source,
            })


def write_review_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "ticker", "lse_name", "isin", "lei", "market", "instrument_type", "status",
        "best_company_number", "best_title", "best_score", "best_status", "error", "candidates_json",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})

# -----------------------------------------------------------------------------
# Universe CSV handling
# -----------------------------------------------------------------------------

def read_universe(path: Path) -> Dict[str, Company]:
    companies: Dict[str, Company] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            lowered = {norm_col(k): v for k, v in row.items()}
            ticker = normalize_ticker(lowered.get("ticker", "") or lowered.get("symbol", ""))
            company_number = normalize_company_number(
                lowered.get("companynumber", "") or lowered.get("companyno", "") or lowered.get("chcompanynumber", "")
            )
            if not company_number:
                continue
            name = (lowered.get("name", "") or lowered.get("companyname", "") or "").strip()
            isin = (lowered.get("isin", "") or "").strip()
            lei = (lowered.get("lei", "") or "").strip()
            market = (lowered.get("lsemarket", "") or lowered.get("market", "") or "").strip()
            try:
                match_score = float(lowered.get("matchscore", "") or 0.0)
            except ValueError:
                match_score = 0.0
            if not ticker:
                ticker = company_number
            companies[company_number] = Company(
                ticker=ticker,
                company_number=company_number,
                name=name,
                isin=isin,
                lei=lei,
                lse_market=market,
                match_score=match_score,
            )
    return companies


def write_example_universe(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ticker", "name", "company_number", "isin", "lei"])
        writer.writerow(["VOD", "Vodafone Group Public Limited Company", "01833679", "", ""])
        writer.writerow(["TSCO", "Tesco PLC", "00445790", "", ""])
        writer.writerow(["", "여기에 FTSE100/350 회사번호 매핑 추가", "", "", ""])

# -----------------------------------------------------------------------------
# Companies House bulk accounts discovery and extraction
# -----------------------------------------------------------------------------

def discover_zip_links(page_url: str, timeout: int) -> List[str]:
    links = discover_links(page_url, timeout)
    out: List[str] = []
    for href, _label in links:
        if ZIP_RE.search(href):
            out.append(urllib.parse.urljoin(page_url, href))
    seen = set()
    deduped = []
    for url in out:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


def parse_bulk_zip(url: str) -> Optional[BulkZip]:
    name = Path(urllib.parse.urlparse(url).path).name
    m = DAILY_DATE_RE.search(name)
    if m:
        y, mo, d = map(int, m.groups())
        return BulkZip(url=url, kind="daily", date=dt.date(y, mo, d), label=name)
    m = MONTHLY_RE.search(name)
    if m:
        mon_text, year_text = m.groups()
        month = MONTHS.get(mon_text.lower())
        if month:
            return BulkZip(url=url, kind="monthly", date=dt.date(int(year_text), month, 1), label=name)
    m = MONTHLY_YEAR_RE.search(name)
    if m:
        return BulkZip(url=url, kind="yearly", date=dt.date(int(m.group(1)), 1, 1), label=name)
    return None


def collect_bulk_zips(include_daily: bool, include_monthly: bool, timeout: int) -> List[BulkZip]:
    zips: List[BulkZip] = []
    if include_daily:
        for url in discover_zip_links(CH_DAILY_PAGE, timeout):
            item = parse_bulk_zip(url)
            if item:
                zips.append(item)
    if include_monthly:
        for url in discover_zip_links(CH_MONTHLY_PAGE, timeout):
            item = parse_bulk_zip(url)
            if item:
                zips.append(item)
    zips.sort(key=lambda x: (x.date, x.kind, x.label))
    seen = set()
    out = []
    for item in zips:
        if item.url not in seen:
            seen.add(item.url)
            out.append(item)
    return out


def filter_zips(items: Iterable[BulkZip], from_year: int, to_year: int, from_date: Optional[dt.date], to_date: Optional[dt.date]) -> List[BulkZip]:
    out: List[BulkZip] = []
    for item in items:
        if item.date.year < from_year or item.date.year > to_year:
            continue
        if from_date and item.date < from_date:
            continue
        if to_date and item.date > to_date:
            continue
        out.append(item)
    return out


def extract_matching_accounts(zip_path: Path, extract_dir: Path, companies: Dict[str, Company]) -> List[ExtractedAccount]:
    extracted: List[ExtractedAccount] = []
    wanted = set(companies.keys())
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.is_dir():
                continue
            name = info.filename.replace("\\", "/")
            match = INNER_ACCOUNT_RE.search(Path(name).name)
            if not match:
                continue
            company_number = normalize_company_number(match.group(1))
            if company_number not in wanted:
                continue
            fiscal_year = None
            date_text = match.group(2)
            if re.match(r"\d{8}$", date_text):
                fiscal_year = int(date_text[:4])
            company = companies[company_number]
            out_name = f"{company.ticker}_{company_number}_{date_text}_{safe_name(Path(name).name)}"
            out_path = extract_dir / company.ticker / out_name
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if not out_path.exists() or out_path.stat().st_size != info.file_size:
                with zf.open(info, "r") as src, out_path.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            extracted.append(ExtractedAccount(company=company, source_zip=str(zip_path), inner_name=name, file_path=str(out_path), fiscal_year=fiscal_year))
    return extracted


def collect_companies_house_accounts(args: argparse.Namespace, companies: Dict[str, Company]) -> List[ExtractedAccount]:
    if not companies:
        return []
    log("Discovering Companies House bulk ZIP links for CH backfill...")
    all_zips = collect_bulk_zips(args.include_daily, args.include_monthly, args.timeout)
    filtered = filter_zips(all_zips, args.from_year, args.to_year, parse_date(args.from_date), parse_date(args.to_date))
    if args.max_zip_files:
        filtered = filtered[: args.max_zip_files]
    log(f"CH backfill bulk ZIP candidates: {len(filtered)}")
    for item in filtered[:20]:
        log(f"  {item.date} [{item.kind}] {item.label}")
    if len(filtered) > 20:
        log(f"  ... +{len(filtered) - 20} more")
    if args.dry_run:
        return []
    if not filtered and not args.skip_download:
        raise SystemExit("No Companies House bulk ZIP candidates after filters.")

    extracted: List[ExtractedAccount] = []
    args.download_dir.mkdir(parents=True, exist_ok=True)
    args.extract_dir.mkdir(parents=True, exist_ok=True)

    local_zips: List[Path] = []
    if args.skip_download:
        local_zips = sorted(args.download_dir.glob("*.zip"))
        log(f"Using existing Companies House ZIPs: {len(local_zips)}")
    else:
        for idx, item in enumerate(filtered, 1):
            zip_path = args.download_dir / item.label
            if not zip_path.exists() or zip_path.stat().st_size == 0:
                log(f"[CH {idx}/{len(filtered)}] Downloading {item.label}")
                try:
                    download_file(item.url, zip_path, args.timeout)
                    time.sleep(args.sleep)
                except urllib.error.HTTPError as exc:
                    log(f"HTTP error {exc.code}; skipped {item.url}")
                    continue
                except Exception as exc:
                    log(f"Download failed; skipped {item.url}: {exc}")
                    continue
            else:
                log(f"[CH {idx}/{len(filtered)}] Reusing {zip_path.name}")
            local_zips.append(zip_path)

    for zip_path in local_zips:
        try:
            rows = extract_matching_accounts(zip_path, args.extract_dir, companies)
            if rows:
                log(f"  extracted {len(rows)} CH matching accounts from {zip_path.name}")
            extracted.extend(rows)
        except zipfile.BadZipFile:
            log(f"  bad ZIP skipped: {zip_path}")
    return extracted


def fca_nsm_search_tagged_annual_reports(timeout: int, page_size: int = 1000) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    headers = {
        "User-Agent": USER_AGENT,
        "Content-Type": "application/json",
        "Referer": "https://data.fca.org.uk/",
    }
    base_payload: Dict[str, Any] = {
        "from": 0,
        "size": page_size,
        "sort": "submitted_date",
        "sortorder": "desc",
        "criteriaObj": {
            "criteria": [
                {"name": "type_code", "value": ["acs", "err:acs"]},
                {"name": "tag_esef", "value": ["Tagged"]},
            ],
            "dateCriteria": None,
        },
    }
    offset = 0
    while True:
        payload = dict(base_payload)
        payload["from"] = offset
        req = urllib.request.Request(
            f"{FCA_NSM_SEARCH_API}?index=fca-nsm-searchdata",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break
        rows.extend([hit.get("_source", {}) for hit in hits])
        if len(hits) < page_size:
            break
        offset += page_size
    return rows


def collect_nsm_esef_accounts(companies: Dict[str, Company], timeout: int) -> List[ExtractedAccount]:
    reports = fca_nsm_search_tagged_annual_reports(timeout)
    extracted: List[ExtractedAccount] = []
    seen_tickers = set()
    for company in companies.values():
        matches: List[Tuple[float, Dict[str, Any]]] = []
        for report in reports:
            report_lei = str(report.get("lei") or "").strip().upper()
            report_company = str(report.get("company") or "").strip()
            if company.lei and report_lei == company.lei.upper():
                matches.append((2.0, report))
            else:
                score = nsm_company_match_score(report_company, company.name or company.ticker)
                if score >= 0.82:
                    matches.append((score, report))
        if not matches:
            continue
        matches.sort(key=lambda item: item[0], reverse=True)
        best = matches[0][1]
        raw_url = str(best.get("download_link") or "").strip()
        if not raw_url:
            continue
        url = urllib.parse.urljoin(FCA_ARTEFACT_BASE, raw_url)
        fiscal_year = None
        document_date = str(best.get("document_date") or "")
        if re.match(r"\d{4}", document_date):
            fiscal_year = int(document_date[:4])
        extracted.append(
            ExtractedAccount(
                company=Company(
                    ticker=company.ticker,
                    company_number=company.company_number,
                    name=str(best.get("company") or company.name).strip().rstrip(";"),
                    isin=company.isin,
                    lei=str(best.get("lei") or company.lei).strip(),
                    lse_market=company.lse_market,
                    lse_source=company.lse_source,
                    match_score=company.match_score,
                ),
                source_zip="fca-nsm",
                inner_name=str(best.get("disclosure_id") or best.get("seq_id") or ""),
                file_path="",
                fiscal_year=fiscal_year,
                url=url,
            )
        )
        seen_tickers.add(company.ticker)
    missing = [c.ticker for c in companies.values() if c.ticker not in seen_tickers]
    if missing:
        log(f"NSM ESEF missing companies: {len(missing)} ({', '.join(missing[:10])}{'...' if len(missing) > 10 else ''})")
    return extracted


def collect_all_nsm_esef_accounts(timeout: int, limit: Optional[int] = None) -> List[ExtractedAccount]:
    reports = fca_nsm_search_tagged_annual_reports(timeout)
    return collect_all_nsm_esef_accounts_from_reports(reports, limit=limit)


def collect_all_nsm_esef_accounts_from_reports(
    reports: Sequence[Dict[str, Any]],
    *,
    limit: Optional[int] = None,
    ticker_map: Optional[Dict[str, str]] = None,
) -> List[ExtractedAccount]:
    latest_by_key: Dict[str, Dict[str, Any]] = {}
    for report in reports:
        raw_url = str(report.get("download_link") or "").strip()
        if not raw_url:
            continue
        key = nsm_report_key(report)
        if not key:
            continue
        if key not in latest_by_key:
            latest_by_key[key] = report

    rows: List[ExtractedAccount] = []
    for key, report in latest_by_key.items():
        if limit is not None and len(rows) >= limit:
            break
        lei = str(report.get("lei") or "").strip().upper()
        company_name = str(report.get("company") or key).strip().rstrip(";")
        raw_url = str(report.get("download_link") or "").strip()
        ticker = (ticker_map or {}).get(key) or lei or normalize_ticker(company_name)
        fiscal_year = None
        document_date = str(report.get("document_date") or "")
        if re.match(r"\d{4}", document_date):
            fiscal_year = int(document_date[:4])
        rows.append(
            ExtractedAccount(
                company=Company(
                    ticker=ticker,
                    company_number="",
                    name=company_name,
                    lei=lei,
                ),
                source_zip="fca-nsm",
                inner_name=str(report.get("disclosure_id") or report.get("seq_id") or ""),
                file_path="",
                fiscal_year=fiscal_year,
                url=urllib.parse.urljoin(FCA_ARTEFACT_BASE, raw_url),
            )
        )
    return rows


def load_lse_ticker_map(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    out: Dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            key = str(row.get("key") or "").strip()
            ticker = normalize_ticker(row.get("ticker") or "")
            if key and ticker:
                out[key] = ticker
    return out


def write_lse_ticker_map(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = ["key", "lei", "name", "ticker", "source", "score", "status", "updated_at"]
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def build_lse_ticker_map_for_reports(
    reports: Sequence[Dict[str, Any]],
    output_path: Path,
    *,
    timeout: int,
    sleep_seconds: float,
    limit: Optional[int] = None,
) -> Dict[str, str]:
    existing = load_lse_ticker_map(output_path)
    rows_by_key: Dict[str, Dict[str, Any]] = {}
    if output_path.exists():
        with output_path.open("r", encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                key = str(row.get("key") or "").strip()
                if key:
                    rows_by_key[key] = row

    latest: Dict[str, Dict[str, Any]] = {}
    for report in reports:
        key = nsm_report_key(report)
        if key and key not in latest and str(report.get("download_link") or "").strip():
            latest[key] = report

    items = list(latest.items())
    if limit is not None:
        items = items[:limit]
    for idx, (key, report) in enumerate(items, 1):
        if key in existing:
            continue
        name = str(report.get("company") or "").strip().rstrip(";")
        lei = str(report.get("lei") or "").strip().upper()
        ticker, source, score = resolve_lse_ticker(name, timeout)
        rows_by_key[key] = {
            "key": key,
            "lei": lei,
            "name": name,
            "ticker": ticker,
            "source": source,
            "score": f"{score:.4f}" if score else "",
            "status": "mapped" if ticker else "not_found",
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        if idx % 25 == 0:
            write_lse_ticker_map(output_path, list(rows_by_key.values()))
            log(f"LSE ticker mapping... {idx}/{len(items)}")
        if sleep_seconds:
            time.sleep(sleep_seconds)
    write_lse_ticker_map(output_path, list(rows_by_key.values()))
    mapped = {key: normalize_ticker(row.get("ticker") or "") for key, row in rows_by_key.items() if normalize_ticker(row.get("ticker") or "")}
    log(f"LSE ticker mapped: {len(mapped)}/{len(items)} -> {output_path}")
    return mapped


def write_manifest(path: Path, rows: Sequence[ExtractedAccount]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["ticker", "name", "company_number", "isin", "lei", "url", "file_path", "fiscal_year", "source_zip", "inner_name"],
        )
        writer.writeheader()
        seen = set()
        for item in rows:
            key = (item.company.ticker, item.url or item.file_path)
            if key in seen:
                continue
            seen.add(key)
            writer.writerow({
                "ticker": item.company.ticker,
                "name": item.company.name,
                "company_number": item.company.company_number,
                "isin": item.company.isin,
                "lei": item.company.lei,
                "url": item.url,
                "file_path": item.file_path,
                "fiscal_year": item.fiscal_year or "",
                "source_zip": item.source_zip,
                "inner_name": item.inner_name,
            })


def run_builder(args: argparse.Namespace, manifest_path: Path) -> int:
    builder = Path(args.builder)
    if not builder.exists():
        raise FileNotFoundError(f"builder not found: {builder}. Put build_uk_cache_db.py in project root or pass --builder.")
    cmd = [
        sys.executable,
        str(builder),
        "--manifest",
        str(manifest_path),
        "--output",
        str(args.cache_output),
        "--audit-output",
        str(args.audit_output),
        "--download-dir",
        str(args.builder_download_dir),
        "--years",
        str(args.growth_years),
    ]
    if args.force:
        cmd.append("--force")
    if args.rebuild_existing:
        cmd.append("--rebuild-existing")
    if args.allow_missing_ticker:
        cmd.append("--allow-missing-ticker")
    if getattr(args, "universe_all_csv", None):
        cmd.extend(["--universe-all-csv", str(args.universe_all_csv)])
    log("Running builder:")
    log(" ".join(f'\"{c}\"' if " " in c else c for c in cmd))
    return subprocess.call(cmd)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Collect UK listed Companies House iXBRL accounts and build mr-leon UK Range Scan cache")

    # Universe options
    parser.add_argument("--universe-csv", help="CSV with columns ticker,name,company_number[,isin,lei]. If omitted, use --auto-universe.")
    parser.add_argument("--write-example-universe", help="Write example manual universe CSV and exit")
    parser.add_argument("--auto-universe", action="store_true", help="Download LSE report and map issuers/instruments to Companies House company_number")
    parser.add_argument("--universe-output", help="Auto-created accepted universe CSV; default <work-dir>/uk_universe.csv")
    parser.add_argument("--review-output", help="Auto-created ambiguous/no-match review CSV; default <work-dir>/uk_universe_review.csv")
    parser.add_argument("--lse-raw-output", help="Raw parsed LSE rows CSV; default <work-dir>/uk_lse_raw_rows.csv")
    parser.add_argument("--lse-full-universe", action="store_true", help="Use LSE issuer/instrument report as the full UK universe and audit missing official fundamentals")
    parser.add_argument("--lse-full-audit-output", help="LSE full universe match audit CSV; default <work-dir>/uk_lse_full_universe_audit.csv")
    parser.add_argument("--ch-backfill-missing-lse", action="store_true", help="After NSM ESEF matching, map missing LSE rows to Companies House and add CH iXBRL accounts to the same build")
    parser.add_argument("--ch-backfill-universe-output", help="Accepted Companies House backfill universe CSV; default <work-dir>/uk_ch_backfill_universe.csv")
    parser.add_argument("--ch-backfill-review-output", help="Companies House backfill no/low match review CSV; default <work-dir>/uk_ch_backfill_review.csv")
    parser.add_argument("--max-ch-backfill-rows", type=int, help="Limit missing LSE rows mapped to Companies House for smoke tests")
    parser.add_argument("--universe-only", action="store_true", help="Only build universe/review CSV; do not collect CH accounts")

    # LSE options
    parser.add_argument("--lse-report-kind", choices=["instruments", "issuers"], default="instruments", help="Which LSE report to discover/use")
    parser.add_argument("--lse-report-url", help="Direct LSE report URL if auto discovery fails")
    parser.add_argument("--lse-report-file", help="Local LSE report file CSV/XLSX/ZIP")
    parser.add_argument("--force-lse-download", action="store_true", help="Redownload LSE report")
    parser.add_argument("--include-funds", action="store_true", help="Keep funds/REITs/ETF-like rows in LSE universe")
    parser.add_argument("--include-non-equity", action="store_true", help="Do not filter out non-equity instruments")
    parser.add_argument("--max-lse-rows", type=int, help="Limit LSE rows for smoke test")

    # Companies House API mapping options
    parser.add_argument("--ch-api-key", help="Companies House API key. Prefer env var instead.")
    parser.add_argument("--ch-api-key-env", default="COMPANIES_HOUSE_API_KEY", help="Environment variable containing CH API key")
    parser.add_argument("--match-threshold", type=float, default=0.78, help="Minimum name-match score for automatic company_number acceptance")
    parser.add_argument("--ch-items-per-page", type=int, default=5, help="Candidates fetched per CH search")
    parser.add_argument("--ch-sleep", type=float, default=0.15, help="Sleep seconds between CH API searches")

    # Work/output options
    parser.add_argument("--work-dir", default=str(DEFAULT_WORK_DIR), help="Working root for LSE reports, downloads, extracts, manifests")
    parser.add_argument("--download-dir", help="Companies House bulk ZIP download directory; default <work-dir>/bulk_zips")
    parser.add_argument("--extract-dir", help="Extracted matching account files directory; default <work-dir>/extracted_accounts")
    parser.add_argument("--manifest-output", help="Manifest CSV path; default <work-dir>/uk_ch_manifest.csv")
    parser.add_argument("--cache-output", default="data/uk_fundamentals_cache.jsonl", help="Final cache JSONL path")
    parser.add_argument("--audit-output", default="data/uk_cache_build_audit.csv", help="Builder audit CSV path")
    parser.add_argument("--builder", default="build_uk_cache_db.py", help="Path to existing build_uk_cache_db.py")
    parser.add_argument("--builder-download-dir", default=str(DEFAULT_WORK_DIR / "uk_filings"), help="Download dir passed to build_uk_cache_db.py")

    # CH bulk accounts options
    parser.add_argument("--source", choices=["ch", "nsm"], default="ch", help="Official filings source: Companies House bulk or FCA NSM tagged ESEF")
    parser.add_argument("--nsm-all", action="store_true", help="Build manifest from all FCA NSM tagged annual financial reports; ticker falls back to LEI when no universe is supplied")
    parser.add_argument("--nsm-limit", type=int, help="Limit NSM all-company manifest rows for smoke tests")
    parser.add_argument("--nsm-map-lse-tickers", action="store_true", help="Resolve NSM all-company rows to LSE tickers before building the cache")
    parser.add_argument("--lse-ticker-map-output", help="CSV path for reusable NSM company to LSE ticker mapping; default <work-dir>/uk_lse_ticker_map.csv")
    parser.add_argument("--lse-search-sleep", type=float, default=0.15, help="Sleep seconds between LSE autocomplete ticker searches")
    parser.add_argument("--include-daily", action="store_true", help="Include recent daily Companies House bulk files")
    parser.add_argument("--include-monthly", action="store_true", help="Include historic monthly/yearly Companies House bulk files")
    parser.add_argument("--from-year", type=int, default=2021)
    parser.add_argument("--to-year", type=int, default=dt.date.today().year)
    parser.add_argument("--from-date", help="Optional lower date YYYY-MM-DD based on bulk file date")
    parser.add_argument("--to-date", help="Optional upper date YYYY-MM-DD based on bulk file date")
    parser.add_argument("--max-zip-files", type=int, help="Limit number of bulk ZIPs for smoke test")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--sleep", type=float, default=0.5, help="Sleep seconds between bulk downloads")
    parser.add_argument("--dry-run", action="store_true", help="Only list matching bulk ZIPs; do not download/extract/build")
    parser.add_argument("--skip-download", action="store_true", help="Use already downloaded ZIPs in download-dir")
    parser.add_argument("--build-cache", action="store_true", help="Call build_uk_cache_db.py after manifest creation")
    parser.add_argument("--allow-missing-ticker", action="store_true", help="Pass --allow-missing-ticker to builder")
    parser.add_argument("--growth-years", type=int, default=5, help="Growth window passed to builder")
    parser.add_argument("--force", action="store_true", help="Pass --force to builder")
    parser.add_argument("--rebuild-existing", action="store_true", help="Pass --rebuild-existing to builder")

    args = parser.parse_args(argv)

    if args.write_example_universe:
        write_example_universe(Path(args.write_example_universe))
        log(f"Wrote example universe: {args.write_example_universe}")
        return 0

    work_dir = Path(args.work_dir)
    args.download_dir = Path(args.download_dir) if args.download_dir else work_dir / "bulk_zips"
    args.extract_dir = Path(args.extract_dir) if args.extract_dir else work_dir / "extracted_accounts"
    manifest_path = Path(args.manifest_output) if args.manifest_output else work_dir / "uk_ch_manifest.csv"
    universe_output = Path(args.universe_output) if args.universe_output else work_dir / "uk_universe.csv"
    review_output = Path(args.review_output) if args.review_output else work_dir / "uk_universe_review.csv"
    lse_full_audit_output = Path(args.lse_full_audit_output) if args.lse_full_audit_output else work_dir / "uk_lse_full_universe_audit.csv"
    ch_backfill_universe_output = Path(args.ch_backfill_universe_output) if args.ch_backfill_universe_output else work_dir / "uk_ch_backfill_universe.csv"
    ch_backfill_review_output = Path(args.ch_backfill_review_output) if args.ch_backfill_review_output else work_dir / "uk_ch_backfill_review.csv"
    args.cache_output = Path(args.cache_output)
    args.audit_output = Path(args.audit_output)
    args.builder_download_dir = Path(args.builder_download_dir)

    if args.auto_universe:
        result = build_universe_from_lse(args)
        write_universe_csv(universe_output, result.accepted)
        write_review_csv(review_output, result.review_rows)
        log(f"Accepted universe: {len(result.accepted)} -> {universe_output}")
        log(f"Review needed: {len(result.review_rows)} -> {review_output}")
        args.universe_csv = str(universe_output)
        if args.universe_only:
            return 0 if result.accepted else 1
    elif not args.universe_csv and not (args.source == "nsm" and (args.nsm_all or args.lse_full_universe)):
        parser.error("Use --universe-csv or --auto-universe. Use --write-example-universe for a manual template.")

    if args.source == "nsm":
        if args.lse_full_universe:
            reports = fca_nsm_search_tagged_annual_reports(args.timeout)
            lse_rows = load_lse_rows(args)
            lse_universe_path = universe_output
            write_lse_raw_rows(lse_universe_path, lse_rows)
            args.universe_all_csv = lse_universe_path
            log(f"LSE full universe rows: {len(lse_rows)} -> {lse_universe_path}")
            extracted = collect_lse_full_nsm_esef_accounts(lse_rows, reports, lse_full_audit_output)
            nsm_extracted_count = len(extracted)
            if args.ch_backfill_missing_lse:
                missing_lse_rows = lse_rows_missing_nsm_esef(lse_rows, reports)
                log(f"LSE rows missing NSM ESEF before CH backfill: {len(missing_lse_rows)}")
                ch_result = map_lse_rows_to_companies_house(
                    args,
                    missing_lse_rows,
                    accepted_output=ch_backfill_universe_output,
                    review_output=ch_backfill_review_output,
                )
                ch_companies = {company.company_number: company for company in ch_result.accepted if company.company_number}
                ch_extracted = collect_companies_house_accounts(args, ch_companies)
                log(f"CH backfill extracted account files: {len(ch_extracted)}")
                extracted.extend(ch_extracted)
            if args.nsm_limit:
                extracted = extracted[: args.nsm_limit]
            log(f"LSE full universe NSM ESEF matches: {nsm_extracted_count}/{len(lse_rows)}")
            if args.ch_backfill_missing_lse:
                log(f"LSE full universe total official filing candidates after CH backfill: {len(extracted)}/{len(lse_rows)}")
        elif args.nsm_all:
            reports = fca_nsm_search_tagged_annual_reports(args.timeout)
            ticker_map: Dict[str, str] = {}
            if args.nsm_map_lse_tickers:
                ticker_map_path = Path(args.lse_ticker_map_output) if args.lse_ticker_map_output else work_dir / "uk_lse_ticker_map.csv"
                ticker_map = build_lse_ticker_map_for_reports(
                    reports,
                    ticker_map_path,
                    timeout=args.timeout,
                    sleep_seconds=args.lse_search_sleep,
                    limit=args.nsm_limit,
                )
            extracted = collect_all_nsm_esef_accounts_from_reports(
                reports,
                limit=args.nsm_limit,
                ticker_map=ticker_map,
            )
            log(f"NSM all-company mode: {len(extracted)} latest tagged ESEF reports")
        else:
            companies = read_universe(Path(args.universe_csv))
            if not companies:
                raise SystemExit("No valid companies in universe CSV. Need company_number at minimum.")
            log(f"Universe companies: {len(companies)}")
            extracted = collect_nsm_esef_accounts(companies, args.timeout)
        write_manifest(manifest_path, extracted)
        log(f"NSM ESEF manifest rows: {len(extracted)}")
        log(f"Manifest: {manifest_path}")
        if not extracted and not getattr(args, "universe_all_csv", None):
            log("No NSM ESEF matches; builder not run.")
            return 2
        if args.build_cache:
            return run_builder(args, manifest_path)
        return 0

    companies = read_universe(Path(args.universe_csv))
    if not companies:
        raise SystemExit("No valid companies in universe CSV. Need company_number at minimum.")
    log(f"Universe companies: {len(companies)}")

    if not args.include_daily and not args.include_monthly:
        if args.universe_only:
            return 0
        parser.error("Choose at least one of --include-daily or --include-monthly for account collection")

    log("Discovering Companies House bulk ZIP links...")
    all_zips = collect_bulk_zips(args.include_daily, args.include_monthly, args.timeout)
    filtered = filter_zips(all_zips, args.from_year, args.to_year, parse_date(args.from_date), parse_date(args.to_date))
    if args.max_zip_files:
        filtered = filtered[: args.max_zip_files]
    log(f"Bulk ZIP candidates: {len(filtered)}")
    for item in filtered[:20]:
        log(f"  {item.date} [{item.kind}] {item.label}")
    if len(filtered) > 20:
        log(f"  ... +{len(filtered) - 20} more")

    if args.dry_run:
        return 0
    if not filtered and not args.skip_download:
        raise SystemExit("No bulk ZIP candidates after filters.")

    extracted: List[ExtractedAccount] = []
    args.download_dir.mkdir(parents=True, exist_ok=True)
    args.extract_dir.mkdir(parents=True, exist_ok=True)

    if args.skip_download:
        local_zips = sorted(args.download_dir.glob("*.zip"))
        log(f"Using existing downloaded ZIPs: {len(local_zips)}")
        for zip_path in local_zips:
            try:
                rows = extract_matching_accounts(zip_path, args.extract_dir, companies)
                if rows:
                    log(f"Extracted {len(rows)} matching accounts from {zip_path.name}")
                extracted.extend(rows)
            except zipfile.BadZipFile:
                log(f"Bad ZIP skipped: {zip_path}")
    else:
        for idx, item in enumerate(filtered, 1):
            zip_path = args.download_dir / item.label
            if not zip_path.exists() or zip_path.stat().st_size == 0:
                log(f"[{idx}/{len(filtered)}] Downloading {item.label}")
                try:
                    download_file(item.url, zip_path, args.timeout)
                    time.sleep(args.sleep)
                except urllib.error.HTTPError as exc:
                    log(f"HTTP error {exc.code}; skipped {item.url}")
                    continue
                except Exception as exc:
                    log(f"Download failed; skipped {item.url}: {exc}")
                    continue
            else:
                log(f"[{idx}/{len(filtered)}] Reusing {zip_path.name}")
            try:
                rows = extract_matching_accounts(zip_path, args.extract_dir, companies)
                if rows:
                    log(f"  extracted {len(rows)} matching accounts")
                extracted.extend(rows)
            except zipfile.BadZipFile:
                log(f"  bad ZIP skipped: {zip_path}")

    write_manifest(manifest_path, extracted)
    log(f"Extracted account files: {len(extracted)}")
    log(f"Manifest: {manifest_path}")

    if args.build_cache:
        if not extracted:
            log("No extracted accounts; builder not run.")
            return 1
        return run_builder(args, manifest_path)
    return 0 if extracted else 1


if __name__ == "__main__":
    raise SystemExit(main())
