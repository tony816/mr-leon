import argparse
import io
import json
import os
import platform
import re
import sys
import threading
import time
import zipfile
import datetime
from pathlib import Path
from dataclasses import dataclass
from functools import lru_cache
from html import unescape
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv

# Load .env so users can keep keys out of the code.
load_dotenv(dotenv_path=".env")

ASSUME_ZERO_DEBT_WHEN_MISSING = os.getenv("NET_CASH_ASSUME_ZERO_DEBT", "").lower() in ("1", "true", "yes", "y")

KRX_LISTING_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download"
DART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DART_MULTI_ACNT_URL = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
DART_STOCK_TOT_URL = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
DART_SINGLE_ACNT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
YAHOO_QUOTE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
STOOQ_QUOTE_URL = "https://stooq.pl/q/l/"
SEC_FORM_PRIORITY = ("10-K", "20-F", "40-F", "10-Q", "10-Q/A", "8-K", "6-K")
SUBMISSIONS_INDEX_FILENAME = "submissions_index.jsonl"

# (reprt_code, release_month, release_year_offset_from_bsns_year)
REPORT_SCHEDULE = (
    ("11014", 11, 0),  # 3분기보고서
    ("11012", 8, 0),   # 반기/2분기보고서
    ("11013", 5, 0),   # 1분기보고서
    ("11011", 3, 1),   # 사업보고서 (다음 해 3월 공시)
)


class KisError(Exception):
    """Raised when the Korea Investment API returns an error."""


class DartError(Exception):
    """Raised when the OpenDART API returns an error."""


class EdgarError(Exception):
    """Raised when EDGAR data fetch fails."""


def normalize_name(text: str) -> str:
    return "".join((text or "").lower().split())


@lru_cache(maxsize=1)
def load_name_map() -> Dict[str, str]:
    """Download KRX listing HTML and build a company-name -> 6-digit code map."""
    resp = requests.get(KRX_LISTING_URL, timeout=10)
    if resp.status_code != 200:
        raise KisError(f"Failed to load KRX listing: HTTP {resp.status_code}")

    html = resp.content.decode("euc-kr", errors="ignore")
    mapping: Dict[str, str] = {}

    # Parse table rows; first row is header, data rows contain name/code at positions 0/2.
    for tr in re.findall(r"<tr>(.*?)</tr>", html, flags=re.S):
        cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.S)
        if len(cells) < 3:
            continue
        raw_name = unescape(re.sub(r"<.*?>", "", cells[0])).strip()
        raw_code = unescape(re.sub(r"<.*?>", "", cells[2])).strip()
        if not raw_name or not raw_code or not raw_code.isdigit():
            continue
        mapping[normalize_name(raw_name)] = raw_code.zfill(6)

    if not mapping:
        raise KisError("KRX listing loaded but empty.")
    return mapping


def lookup_code_by_name(name: str) -> Optional[str]:
    """Resolve a company name to its 6-digit code via KRX listing."""
    if not name:
        return None
    mapping = load_name_map()
    norm = normalize_name(name)
    direct = mapping.get(norm)
    if direct:
        return direct
    # Fallback: partial match for spacing differences.
    for key, code in mapping.items():
        if norm in key or key in norm:
            return code
    return None


def get_dart_key() -> str:
    key = os.getenv("DART_KEY")
    if not key:
        raise DartError("Set DART_KEY in your environment or .env file.")
    return key


@lru_cache(maxsize=1)
def load_dart_corp_map() -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    """Download OpenDART corp codes and build lookup maps.

    Returns:
        name_to_code: normalized corp name -> corp_code
        stock_to_code: 6-digit stock code -> corp_code
        code_to_name: corp_code -> original corp_name
    """
    resp = requests.get(DART_CORP_CODE_URL, params={"crtfc_key": get_dart_key()}, timeout=15)
    if resp.status_code != 200:
        raise DartError(f"Failed to load DART corp codes: HTTP {resp.status_code}")

    try:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            names = zf.namelist()
            if not names:
                raise DartError("DART corp code zip is empty.")
            xml_bytes = zf.read(names[0])
    except zipfile.BadZipFile as exc:
        raise DartError(f"Invalid corp code zip file: {exc}") from exc

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise DartError(f"Failed to parse corp code XML: {exc}") from exc

    name_to_code: Dict[str, str] = {}
    stock_to_code: Dict[str, str] = {}
    code_to_name: Dict[str, str] = {}

    for item in root.findall("list"):
        corp_name = (item.findtext("corp_name") or "").strip()
        corp_code = (item.findtext("corp_code") or "").strip()
        stock_code = (item.findtext("stock_code") or "").strip()
        norm_name = normalize_name(corp_name)

        if corp_code and norm_name:
            name_to_code.setdefault(norm_name, corp_code)
            code_to_name.setdefault(corp_code, corp_name)
        if stock_code:
            stock_to_code.setdefault(stock_code.zfill(6), corp_code)

    if not name_to_code:
        raise DartError("DART corp code mapping is empty.")
    return name_to_code, stock_to_code, code_to_name


def resolve_dart_corp(user_text: str) -> Tuple[str, str]:
    """Resolve user input to (corp_code, corp_name) using DART corp list."""
    if not user_text:
        raise DartError("회사명을 입력하세요.")

    name_map, stock_map, code_to_name = load_dart_corp_map()
    trimmed = user_text.strip()
    digits = "".join(ch for ch in trimmed if ch.isdigit())

    if len(digits) >= 8:
        corp_code = digits[:8]
        return corp_code, code_to_name.get(corp_code, trimmed)

    if len(digits) == 6:
        corp_code = stock_map.get(digits)
        if corp_code:
            return corp_code, code_to_name.get(corp_code, trimmed)

    norm = normalize_name(trimmed)
    direct = name_map.get(norm)
    if direct:
        return direct, code_to_name.get(direct, trimmed)

    for key, corp_code in name_map.items():
        if norm in key or key in norm:
            return corp_code, code_to_name.get(corp_code, trimmed)

    raise DartError("회사명을 찾을 수 없습니다. 정식명 또는 상장사 명칭을 입력하세요.")


def _pad_cik(value: str) -> str:
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits.zfill(10) if digits else ""


def sec_headers() -> Dict[str, str]:
    ua = os.getenv("SEC_USER_AGENT") or os.getenv("EDGAR_USER_AGENT") or "mr-leon-app/0.1"
    return {
        "User-Agent": ua,
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate",
    }


def find_local_companyfacts_file(cik_padded: str) -> Optional[Path]:
    filename = f"CIK{cik_padded}.json"
    candidates = []
    configured = os.getenv("SEC_COMPANYFACTS_DIR") or os.getenv("EDGAR_COMPANYFACTS_DIR") or os.getenv("COMPANYFACTS_DIR")
    if configured:
        candidates.append(Path(configured))
    candidates.append(Path.cwd() / "companyfacts")
    try:
        candidates.append(Path(__file__).resolve().parent / "companyfacts")
    except Exception:
        pass

    for directory in candidates:
        try:
            path = directory / filename
        except Exception:
            continue
        if path.is_file():
            return path
    return None


def find_local_submissions_dir() -> Optional[Path]:
    configured = os.getenv("SEC_SUBMISSIONS_DIR") or os.getenv("EDGAR_SUBMISSIONS_DIR") or os.getenv("SUBMISSIONS_DIR")
    candidates: List[Path] = []
    if configured:
        candidates.append(Path(configured))
    candidates.append(Path.cwd() / "submissions")
    try:
        candidates.append(Path(__file__).resolve().parent / "submissions")
    except Exception:
        pass
    for directory in candidates:
        try:
            if directory.is_dir():
                return directory
        except Exception:
            continue
    return None


def submissions_index_path() -> Path:
    configured = os.getenv("SEC_SUBMISSIONS_INDEX") or os.getenv("EDGAR_SUBMISSIONS_INDEX") or os.getenv("SUBMISSIONS_INDEX")
    if configured:
        return Path(configured)
    return Path.cwd() / SUBMISSIONS_INDEX_FILENAME


def _normalize_form_name(value: str) -> str:
    text = (value or "").upper().strip()
    if text.startswith("FORM "):
        text = text[5:]
    return text.strip()


def choose_primary_ticker(tickers: List[str]) -> Optional[str]:
    cleaned = [t.strip().upper() for t in (tickers or []) if str(t).strip()]
    if not cleaned:
        return None

    def score(ticker: str):
        bad_suffixes = ("WS", "W", "WT", "U", "R")
        suffix_penalty = 10 if ticker.endswith(bad_suffixes) else 0
        weird_punct_penalty = 10 if any(ch in ticker for ch in ("^", "/", "=")) else 0
        punct_penalty = sum(1 for ch in ticker if not ch.isalnum())
        return (suffix_penalty + weird_punct_penalty, punct_penalty, len(ticker), ticker)

    return min(cleaned, key=score)


def _iter_cik_json_files(directory: Path) -> Iterable[Path]:
    try:
        with os.scandir(directory) as it:
            for entry in it:
                if not entry.is_file():
                    continue
                name = entry.name
                if not (name.startswith("CIK") and name.endswith(".json")):
                    continue
                yield Path(entry.path)
    except FileNotFoundError:
        return


def parse_submissions_metadata(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    cik = _pad_cik(str(payload.get("cik") or ""))
    if not cik:
        return None

    name = (payload.get("name") or payload.get("entityName") or "").strip() or f"CIK{cik}"
    entity_type = (payload.get("entityType") or "").strip().lower()

    tickers_raw = payload.get("tickers") or []
    tickers = sorted({str(t).upper().strip() for t in tickers_raw if str(t).strip()})
    exchanges_raw = payload.get("exchanges") or []
    exchanges = sorted({str(x).strip() for x in exchanges_raw if str(x).strip()})

    primary = choose_primary_ticker(tickers)
    has_companyfacts = bool(find_local_companyfacts_file(cik))

    is_foreign = False
    try:
        addresses = payload.get("addresses") or {}
        mailing = addresses.get("mailing") or {}
        business = addresses.get("business") or {}
        is_foreign = bool(mailing.get("isForeignLocation")) or bool(business.get("isForeignLocation"))
    except Exception:
        is_foreign = False

    forms = (payload.get("filings") or {}).get("recent", {}).get("form") or []
    normalized_forms = {_normalize_form_name(f) for f in forms if str(f).strip()}
    if any(f.startswith(("20-F", "40-F")) or f.startswith("6-K") for f in normalized_forms):
        is_foreign = True

    is_fund = entity_type not in ("", "operating")
    if not is_fund:
        fund_prefixes = ("N-", "NPORT", "N-CEN", "NCSR", "N-CSR")
        fund_starts = ("485", "497")
        if any(f.startswith(fund_starts) or f.startswith(fund_prefixes) for f in normalized_forms):
            is_fund = True

    return {
        "cik": cik,
        "name": name,
        "entity_type": entity_type,
        "tickers": tickers,
        "primary_ticker": primary,
        "exchanges": exchanges,
        "is_foreign": is_foreign,
        "is_fund": is_fund,
        "has_companyfacts": has_companyfacts,
    }


def build_submissions_index(
    submissions_dir: Path,
    index_path: Path,
    status_cb: Optional[Callable[[str], None]] = None,
) -> None:
    tmp_path = index_path.with_suffix(index_path.suffix + ".tmp")
    processed = 0
    written = 0

    try:
        tmp_path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    with tmp_path.open("w", encoding="utf-8") as handle:
        for path in _iter_cik_json_files(submissions_dir):
            processed += 1
            if status_cb and processed % 500 == 0:
                status_cb(f"Indexing submissions... {processed}")
            try:
                with path.open("r", encoding="utf-8") as in_handle:
                    payload = json.load(in_handle)
            except Exception:
                continue

            meta = parse_submissions_metadata(payload)
            if not meta:
                continue
            if not meta.get("tickers"):
                continue
            handle.write(json.dumps(meta, ensure_ascii=False) + "\n")
            written += 1

    tmp_path.replace(index_path)
    if status_cb:
        status_cb(f"Index ready: {written} entries")


@lru_cache(maxsize=1)
def load_submissions_index() -> Dict[str, Any]:
    path = submissions_index_path()
    if not path.exists():
        submissions_dir = find_local_submissions_dir()
        if not submissions_dir:
            raise EdgarError("submissions folder not found. Put SEC submissions JSONs in ./submissions or set SEC_SUBMISSIONS_DIR.")
        build_submissions_index(submissions_dir, path)

    entries: List[Dict[str, Any]] = []
    cik_map: Dict[str, Dict[str, Any]] = {}
    ticker_map: Dict[str, Dict[str, str]] = {}
    name_index: Dict[str, str] = {}

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                meta = json.loads(line)
            except Exception:
                continue
            cik = _pad_cik(meta.get("cik") or "")
            if not cik:
                continue
            tickers = meta.get("tickers") or []
            primary = meta.get("primary_ticker")
            name = (meta.get("name") or "").strip()

            meta["cik"] = cik
            entries.append(meta)
            cik_map[cik] = meta

            for ticker in tickers:
                t = str(ticker).upper().strip()
                if not t:
                    continue
                ticker_map[t] = {"cik": cik, "title": name or t}

            if name and primary:
                name_index[normalize_name(name)] = str(primary).upper().strip()

    if not entries:
        raise EdgarError(f"submissions index is empty: {path}")
    return {"entries": entries, "cik_map": cik_map, "ticker_map": ticker_map, "name_index": name_index}


def ensure_submissions_index(status_cb: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    path = submissions_index_path()
    if path.exists():
        return load_submissions_index()

    submissions_dir = find_local_submissions_dir()
    if not submissions_dir:
        raise EdgarError("submissions folder not found. Put SEC submissions JSONs in ./submissions or set SEC_SUBMISSIONS_DIR.")
    build_submissions_index(submissions_dir, path, status_cb=status_cb)
    load_submissions_index.cache_clear()
    return load_submissions_index()


@lru_cache(maxsize=1)
def load_edgar_ticker_map() -> Tuple[Dict[str, Dict[str, str]], Dict[str, str]]:
    """Load SEC ticker -> CIK mapping and a normalized name index."""
    resp = requests.get(SEC_TICKERS_URL, headers=sec_headers(), timeout=15)
    if resp.status_code != 200:
        if resp.status_code == 403:
            raise EdgarError(
                "Failed to load SEC ticker list: HTTP 403. Register a contact info User-Agent via SEC_USER_AGENT or EDGAR_USER_AGENT environment variable."
            )
        raise EdgarError(f"Failed to load SEC ticker list: HTTP {resp.status_code}")
    try:
        data = resp.json()
    except Exception as exc:
        raise EdgarError(f"Invalid SEC ticker list response: {exc}") from exc

    ticker_map: Dict[str, Dict[str, str]] = {}
    name_index: Dict[str, str] = {}
    items = data.values() if isinstance(data, dict) else []
    for item in items:
        ticker = str(item.get("ticker") or "").upper().strip()
        cik = _pad_cik(item.get("cik_str") or item.get("cik") or "")
        title = (item.get("title") or "").strip()
        if not ticker or not cik:
            continue
        ticker_map[ticker] = {"cik": cik, "title": title}
        if title:
            name_index[normalize_name(title)] = ticker

    if not ticker_map:
        raise EdgarError("SEC ticker list is empty.")
    return ticker_map, name_index


def resolve_edgar_company(user_text: str) -> Dict[str, str]:
    """Resolve user input to ticker/CIK/company name using local submissions index (preferred) or SEC ticker list."""
    if not user_text:
        raise EdgarError("Enter a ticker, CIK, or company name.")

    text = user_text.strip()
    cleaned_ticker = re.sub(r"[^A-Za-z0-9\.-]", "", text).upper()
    digits = "".join(ch for ch in text if ch.isdigit())

    local = None
    try:
        local = load_submissions_index()
    except Exception:
        local = None

    if local:
        ticker_map = local.get("ticker_map", {})
        cik_map = local.get("cik_map", {})
        name_index = local.get("name_index", {})

        if cleaned_ticker:
            info = ticker_map.get(cleaned_ticker)
            if info and info.get("cik"):
                return {
                    "ticker": cleaned_ticker,
                    "cik": info.get("cik", ""),
                    "name": info.get("title") or cleaned_ticker,
                }

        if digits:
            cik = _pad_cik(digits)
            meta = cik_map.get(cik)
            if meta:
                tickers = meta.get("tickers") or []
                ticker = meta.get("primary_ticker") or (tickers[0] if tickers else cleaned_ticker or digits)
                return {"ticker": str(ticker).upper().strip(), "cik": cik, "name": meta.get("name") or text}
            return {"ticker": cleaned_ticker or digits, "cik": cik, "name": text}

        norm_name = normalize_name(text)
        if norm_name:
            direct = name_index.get(norm_name)
            if direct:
                info = ticker_map.get(direct, {})
                return {"ticker": direct, "cik": info.get("cik", ""), "name": info.get("title") or direct}
            for name_norm, ticker in name_index.items():
                if norm_name in name_norm or name_norm in norm_name:
                    info = ticker_map.get(ticker, {})
                    return {"ticker": ticker, "cik": info.get("cik", ""), "name": info.get("title") or ticker}

    ticker_map, name_index = load_edgar_ticker_map()

    if cleaned_ticker:
        info = ticker_map.get(cleaned_ticker)
        if info:
            return {"ticker": cleaned_ticker, "cik": info["cik"], "name": info.get("title") or cleaned_ticker}

    if digits:
        cik = _pad_cik(digits)
        for ticker, info in ticker_map.items():
            if info.get("cik") == cik:
                return {"ticker": ticker, "cik": cik, "name": info.get("title") or ticker}
        return {"ticker": cleaned_ticker or digits, "cik": cik, "name": text}

    norm_name = normalize_name(text)
    for name_norm, ticker in name_index.items():
        if norm_name == name_norm or norm_name in name_norm or name_norm in norm_name:
            info = ticker_map.get(ticker, {})
            return {"ticker": ticker, "cik": info.get("cik", ""), "name": info.get("title") or ticker}

    raise EdgarError("Company not found in SEC ticker list.")


ACCOUNT_SYNONYMS = {
    "매출액": {"매출액", "영업수익", "수익(매출)", "매출수익"},
    "영업이익": {"영업이익"},
    "당기순이익": {"당기순이익", "분기순이익", "지배기업의 소유주에게 귀속되는 당기순이익"},
    "자산총계": {"자산총계", "자산총액"},
    "부채총계": {"부채총계", "부채총액"},
    "자본총계": {"자본총계", "자본총액"},
    "단기금융상품": {"단기금융상품", "shorttermfinancialproducts"},
    "단기상각후원가금융자산": {"단기상각후원가금융자산", "amortizedcostshorttermfinancialassets"},
    "단기당기손익-공정가치금융자산": {
        "단기당기손익-공정가치금융자산",
        "단기당기손익공정가치금융자산",
        "단기당기손익-공정가치-금융자산",
        "shorttermfvplfinancialassets",
    },
    "현금및현금성자산": {
        "현금및현금성자산",
        "현금및현금성자산및예치금",
        "현금및현금성자산(유동)",
        "현금및현금성자산(비유동)",
        "현금및현금성자산및단기금융상품",
        "cashandcashequivalents",
        "cash_and_cash_equivalents",
    },
    "단기차입금": {"단기차입금", "shorttermborrowings"},
    "유동성장기부채": {"유동성장기부채", "currentportionoflongtermliabilities"},
    "유동성장기차입금": {"유동성장기차입금", "currentportionoflongtermborrowings"},
    "유동성사채": {"유동성사채", "currentportionofbonds"},
    "사채": {"사채", "회사채", "bonds"},
    "장기차입금": {"장기차입금", "longtermborrowings"},
}

ACCOUNT_ALIAS_MAP = {normalize_name(alias): key for key, aliases in ACCOUNT_SYNONYMS.items() for alias in aliases}


def parse_amount(value) -> Optional[int]:
    if value in (None, "", "-", "NaN"):
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        return int(float(text))
    except ValueError:
        return None


def parse_float(value) -> Optional[float]:
    if value in (None, "", "-", "NaN"):
        return None
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]
    try:
        return float(text)
    except ValueError:
        return None


def format_amount(value) -> str:
    amount = parse_amount(value)
    if amount is None:
        return "N/A"
    return f"{amount:,}"


def format_usd_with_krw(amount: Optional[int], usdkrw_rate: Optional[float]) -> str:
    if amount is None:
        return "N/A"
    usd_text = f"${amount:,}"
    if usdkrw_rate is None:
        return usd_text
    try:
        krw = int(amount * usdkrw_rate)
        return f"{usd_text} (₩{krw:,} @ {usdkrw_rate:,.2f})"
    except Exception:
        return usd_text


def format_per_share(cash_value: Optional[int], shares: Optional[int]) -> str:
    if cash_value is None or shares is None or shares <= 0:
        return "N/A"
    return f"{cash_value / shares:,.2f}"


def in_range(value: Optional[float], min_value: Optional[float], max_value: Optional[float]) -> bool:
    """Return True if value is within [min, max] when bounds are provided."""
    if value is None:
        # If bounds exist but value is missing, treat as not matching.
        return min_value is None and max_value is None
    if min_value is not None and value < min_value:
        return False
    if max_value is not None and value > max_value:
        return False
    return True


def find_account_amount(entries, target_key: str) -> Optional[int]:
    """Find the first matching account amount for the target key using alias map."""
    if not entries:
        return None
    normalized_target = normalize_name(target_key)
    for row in entries:
        account_nm = (row.get("account_nm") or "").strip()
        if not account_nm:
            continue
        normalized_name = normalize_name(account_nm)
        key = ACCOUNT_ALIAS_MAP.get(normalized_name)
        # Allow direct normalized match even when alias map is missing.
        if not ((key and key == target_key) or (normalized_name == normalized_target)):
            continue
        val = row.get("thstrm_amount") or row.get("thstrm_add_amount")
        amt = parse_amount(val)
        if amt is not None:
            return amt
    return None


def summarize_accounts(entries) -> Dict[str, str]:
    summary = {key: "N/A" for key in ACCOUNT_SYNONYMS}
    for row in entries or []:
        account_nm = (row.get("account_nm") or "").strip()
        if not account_nm:
            continue
        label = ACCOUNT_ALIAS_MAP.get(normalize_name(account_nm))
        if not label or summary[label] != "N/A":
            continue
        summary[label] = format_amount(row.get("thstrm_amount") or row.get("thstrm_add_amount"))
    return summary


def fetch_dart_single_accounts(corp_code: str, bsns_year: str, reprt_code: str):
    params = {
        "crtfc_key": get_dart_key(),
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": "CFS",
    }
    resp = requests.get(DART_SINGLE_ACNT_URL, params=params, timeout=15)
    if resp.status_code != 200:
        raise DartError(f"단일계정 조회 실패: HTTP {resp.status_code}")
    payload = resp.json()
    if payload.get("status") != "000":
        raise DartError(f"단일계정 조회 오류: {payload.get('status')} {payload.get('message', '')}".strip())
    return payload.get("list") or []


def _fetch_dart_annual_values(
    corp_code: str, bsns_year: str
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    params = {
        "crtfc_key": get_dart_key(),
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": "11011",
    }
    resp = requests.get(DART_MULTI_ACNT_URL, params=params, timeout=15)
    if resp.status_code != 200:
        return None, None
    payload = resp.json()
    if payload.get("status") != "000":
        return None, None
    entries = payload.get("list") or []
    if not entries:
        return None, None
    single_entries = []
    try:
        single_entries = fetch_dart_single_accounts(corp_code, bsns_year, "11011")
    except Exception:
        single_entries = []
    combined = (single_entries or []) + (entries or [])
    revenue_val = find_account_amount(combined, "매출액")
    op_income_val = find_account_amount(combined, "영업이익")
    net_income_val = find_account_amount(combined, "당기순이익")
    return revenue_val, op_income_val, net_income_val


def collect_dart_annual_series(
    corp_code: str, window_years: int = 5
) -> Tuple[Dict[int, Optional[int]], Dict[int, Optional[int]], Dict[int, Optional[int]]]:
    today = datetime.date.today()
    current_year = today.year
    target_years: List[int] = []
    for year in range(current_year, current_year - 12, -1):
        release_date = datetime.date(year + 1, 3, 1)
        if release_date > today:
            continue
        target_years.append(year)
        if len(target_years) >= window_years + 1:
            break

    revenue_by_year: Dict[int, Optional[int]] = {year: None for year in target_years}
    op_by_year: Dict[int, Optional[int]] = {year: None for year in target_years}
    net_by_year: Dict[int, Optional[int]] = {year: None for year in target_years}

    for year in target_years:
        try:
            revenue_val, op_income_val, net_income_val = _fetch_dart_annual_values(corp_code, str(year))
        except Exception:
            continue
        revenue_by_year[year] = revenue_val
        op_by_year[year] = op_income_val
        net_by_year[year] = net_income_val

    return revenue_by_year, op_by_year, net_by_year


def _parse_int(value) -> Optional[int]:
    try:
        return int(float(str(value).replace(",", "")))
    except Exception:
        return None


def _sum_or_none(values) -> Optional[int]:
    filtered = [v for v in values if v is not None]
    return sum(filtered) if filtered else None


def compute_net_cash(
    liquid_funds: Optional[int],
    interest_bearing_debt: Optional[int],
    assume_zero_debt_when_missing: bool = False,
):
    """Return (net_cash, debt_value) with optional conservative zero-debt fallback."""
    if liquid_funds is None:
        return None, interest_bearing_debt
    if interest_bearing_debt is None:
        if assume_zero_debt_when_missing:
            return liquid_funds, 0
        return None, None
    return liquid_funds - interest_bearing_debt, interest_bearing_debt


def parse_stock_totals(entries) -> Optional[int]:
    """Compute 유통주식수(Ⅵ) preferring distb_stock_co and falling back to (발행주식 - 자사주)."""
    if not entries:
        return None
    chosen = None
    for entry in entries:
        se = str(entry.get("se", "")).lower()
        if "보통" in se or "common" in se:
            chosen = entry
            break
    if not chosen:
        chosen = entries[0]
    entry = chosen or {}

    distb_stock = _parse_int(entry.get("distb_stock_co"))
    if distb_stock and distb_stock > 0:
        return distb_stock

    now_to_isu = _parse_int(entry.get("now_to_isu_stock_totqy"))
    now_to_dcrs = _parse_int(entry.get("now_to_dcrs_stock_totqy")) or 0
    tesstk = _parse_int(entry.get("tesstk_co")) or 0

    if now_to_isu is None:
        return None

    issued_total = now_to_isu - now_to_dcrs
    if issued_total <= 0:
        return None

    float_shares = issued_total - tesstk
    return float_shares if float_shares > 0 else None


def fetch_dart_stock_totals(corp_code: str, bsns_year: str, reprt_code: str) -> Optional[int]:
    params = {
        "crtfc_key": get_dart_key(),
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
    }
    resp = requests.get(DART_STOCK_TOT_URL, params=params, timeout=15)
    if resp.status_code != 200:
        raise DartError(f"주식 총수 조회 실패: HTTP {resp.status_code}")

    payload = resp.json()
    if payload.get("status") != "000":
        raise DartError(f"주식 총수 조회 오류: {payload.get('status')} {payload.get('message', '')}".strip())

    entries = payload.get("list") or []
    return parse_stock_totals(entries)


def build_report_periods(
    bsns_year: Optional[str] = None,
    today: Optional[datetime.date] = None,
    years_back: int = 4,
) -> Tuple[Tuple[str, str], ...]:
    """Return (year, reprt_code) pairs ordered from most recent release backward.

    - Includes 분기/반기/3분기 + 사업보고서.
    - Skips unreleased periods when bsns_year is not specified (auto mode).
    """
    current_date = today or datetime.date.today()
    candidates = []

    def collect_for_year(year: int, skip_unreleased: bool = True):
        year_entries = []
        for code, release_month, year_offset in REPORT_SCHEDULE:
            release_date = datetime.date(year + year_offset, release_month, 1)
            if skip_unreleased and release_date > current_date:
                continue
            year_entries.append((release_date, str(year), code))
        return year_entries

    try:
        year_value = int(bsns_year)
    except (TypeError, ValueError):
        current_year = current_date.year
        current_entries = collect_for_year(current_year, skip_unreleased=True)
        if current_entries:
            candidates.extend(current_entries)

        required_prev_years = max(0, years_back)
        if not current_entries:
            required_prev_years = max(1, required_prev_years)

        year = current_year - 1
        max_lookback = current_year - 12
        added_prev = 0
        while added_prev < required_prev_years and year > max_lookback:
            entries = collect_for_year(year, skip_unreleased=True)
            if entries:
                candidates.extend(entries)
                added_prev += 1
            year -= 1
    else:
        candidates.extend(collect_for_year(year_value, skip_unreleased=False))

    candidates.sort(key=lambda item: item[0], reverse=True)
    return tuple((year, code) for _, year, code in candidates)


def fetch_dart_financials(
    user_text: str,
    bsns_year: Optional[str] = None,
    reprt_code: Optional[str] = None,
    fallback_listed_shares: Optional[int] = None,
    market_price: Optional[float] = None,
) -> Dict[str, str]:
    """Fetch DART financials prioritizing the most recent available report.

    If reprt_code is omitted, it tries quarters/half/3Q/business report by release
    recency. Passing reprt_code forces that report type.
    """
    corp_code, corp_name = resolve_dart_corp(user_text)
    now_year = time.localtime().tm_year
    sales_growth_5y = "N/A"
    op_growth_5y = "N/A"
    net_income_growth_5y = "N/A"
    try:
        revenue_series, op_series, net_series = collect_dart_annual_series(corp_code, window_years=5)
        sales_growth_5y = format_yoy_average(revenue_series, window_years=5)
        op_growth_5y = format_yoy_average(op_series, window_years=5)
        net_income_growth_5y = format_yoy_average(net_series, window_years=5)
    except Exception:
        pass
    if reprt_code:
        years_to_try = [str(bsns_year)] if bsns_year else [str(now_year - i) for i in range(4)]
        periods = [(year, reprt_code) for year in years_to_try]
    else:
        periods = list(build_report_periods(bsns_year=bsns_year, years_back=4))
        if not periods:
            periods = [(str(now_year), "11013")]

    last_error = None
    for year, report_code in periods:
        params = {
            "crtfc_key": get_dart_key(),
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": report_code,
        }
        resp = requests.get(DART_MULTI_ACNT_URL, params=params, timeout=15)
        if resp.status_code != 200:
            last_error = f"HTTP {resp.status_code}"
            continue
        payload = resp.json()
        status = payload.get("status")
        if status != "000":
            last_error = f"{status} {payload.get('message', '')}".strip()
            continue
        entries = payload.get("list") or []
        if not entries:
            last_error = "빈 응답"
            continue
        single_entries = []
        try:
            single_entries = fetch_dart_single_accounts(corp_code, year, report_code)
        except Exception:
            single_entries = []

        summary = summarize_accounts(entries)
        combined = (single_entries or []) + (entries or [])
        cash_equivalents = find_account_amount(combined, "현금및현금성자산")
        short_term_products = find_account_amount(combined, "단기금융상품")
        amortized_assets = find_account_amount(combined, "단기상각후원가금융자산")
        fvpl_assets = find_account_amount(combined, "단기당기손익-공정가치금융자산")
        equity_value = find_account_amount(combined, "자본총계")

        if cash_equivalents is not None:
            summary["현금및현금성자산"] = format_amount(cash_equivalents)

        liquid_funds = _sum_or_none([cash_equivalents, short_term_products, amortized_assets, fvpl_assets])

        short_borrowings = find_account_amount(combined, "단기차입금")
        current_long_term_debt = find_account_amount(combined, "유동성장기부채")
        current_long_term_borrowings = find_account_amount(combined, "유동성장기차입금")
        current_bonds = find_account_amount(combined, "유동성사채")
        bonds = find_account_amount(combined, "사채")
        long_borrowings = find_account_amount(combined, "장기차입금")

        if current_long_term_debt is None:
            current_long_term_debt = _sum_or_none([current_long_term_borrowings, current_bonds])

        interest_bearing_debt = _sum_or_none(
            [short_borrowings, current_long_term_debt, bonds, long_borrowings]
        )

        net_cash, debt_value = compute_net_cash(
            liquid_funds, interest_bearing_debt, assume_zero_debt_when_missing=ASSUME_ZERO_DEBT_WHEN_MISSING
        )

        ib_debt_ratio_pct = None
        if debt_value is not None and equity_value not in (None, 0):
            try:
                ib_debt_ratio_pct = (debt_value / equity_value) * 100
            except Exception:
                ib_debt_ratio_pct = None

        float_shares = None
        try:
            float_shares = fetch_dart_stock_totals(corp_code, year, report_code)
        except Exception:
            float_shares = None

        used_kis_fallback = False
        if float_shares is None and fallback_listed_shares and fallback_listed_shares > 0:
            float_shares = fallback_listed_shares
            used_kis_fallback = True

        net_cash_per_share_value = None
        if net_cash is not None and float_shares:
            try:
                net_cash_per_share_value = net_cash / float_shares
            except Exception:
                net_cash_per_share_value = None

        net_cash_per_share = format_per_share(net_cash, float_shares)
        if used_kis_fallback and net_cash_per_share != "N/A":
            net_cash_per_share = f"{net_cash_per_share} (KIS 상장주식수)"

        net_cash_per_share_ratio = "N/A"
        if net_cash_per_share_value is not None and market_price and market_price > 0:
            try:
                ratio = (net_cash_per_share_value / market_price) * 100
                net_cash_per_share_ratio = f"{ratio:,.2f}%"
            except Exception:
                net_cash_per_share_ratio = "N/A"

        net_cash_display = format_amount(net_cash) if net_cash is not None else "N/A"
        float_shares_display = format_amount(float_shares) if float_shares is not None else None
        ib_debt_ratio_text = f"{ib_debt_ratio_pct:,.2f}" if ib_debt_ratio_pct is not None else "N/A"

        return {
            "corp_name": corp_name,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": report_code,
            "summary": summary,
            "sales_growth_5y": sales_growth_5y,
            "op_growth_5y": op_growth_5y,
            "net_income_growth_5y": net_income_growth_5y,
            "cash_equivalents": format_amount(cash_equivalents) if cash_equivalents is not None else "N/A",
            "liquid_funds": liquid_funds,
            "interest_bearing_debt": debt_value,
            "net_cash": net_cash,
            "net_cash_display": net_cash_display,
            "float_shares": float_shares,
            "float_shares_display": float_shares_display,
            "net_cash_per_share": net_cash_per_share,
            "net_cash_per_share_ratio": net_cash_per_share_ratio,
            "equity": equity_value,
            "interest_bearing_debt_ratio": ib_debt_ratio_text,
            "interest_bearing_debt_ratio_value": ib_debt_ratio_pct,
        }

    raise DartError(last_error or "조회 가능한 연도가 없습니다.")



@lru_cache(maxsize=128)
def load_company_facts(cik: str) -> Dict:
    cik_padded = _pad_cik(cik)
    if not cik_padded:
        raise EdgarError("CIK is required for EDGAR lookup.")

    local_path = find_local_companyfacts_file(cik_padded)
    if local_path:
        try:
            with local_path.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception as exc:
            raise EdgarError(f"Failed to read local company facts: {local_path}: {exc}") from exc

    resp = requests.get(SEC_FACTS_URL.format(cik=cik_padded), headers=sec_headers(), timeout=15)
    if resp.status_code != 200:
        if resp.status_code == 403:
            raise EdgarError(
                "Failed to fetch company facts: HTTP 403. Set SEC_USER_AGENT (contact info) or use local companyfacts data (extract companyfacts.zip into ./companyfacts)."
            )
        raise EdgarError(f"Failed to fetch company facts: HTTP {resp.status_code}")
    try:
        return resp.json()
    except Exception as exc:
        raise EdgarError(f"Invalid company facts payload: {exc}") from exc


def _parse_iso_date(date_text: str) -> int:
    try:
        return datetime.date.fromisoformat(date_text).toordinal()
    except Exception:
        return 0


def _extract_latest_fact(
    facts: Dict,
    key: str,
    units=("USD",),
    forms_priority=SEC_FORM_PRIORITY,
) -> Optional[float]:
    facts_root = (facts or {}).get("facts", {}).get("us-gaap", {})
    entry = facts_root.get(key) or {}
    unit_map = entry.get("units") or {}
    candidates = []
    priority_map = {form: idx for idx, form in enumerate(forms_priority)}

    today_ord = datetime.date.today().toordinal()

    for unit in units:
        for item in unit_map.get(unit, []):
            val = item.get("val")
            if val in (None, "", "-", "NaN"):
                continue
            try:
                val_num = float(val)
            except Exception:
                continue
            form = item.get("form", "")
            priority = priority_map.get(form, len(forms_priority))
            end_ts = _parse_iso_date(item.get("end") or "") or _parse_iso_date(item.get("filed") or "")
            # Skip future-dated facts that can appear in companyfacts payloads.
            if end_ts and end_ts > today_ord:
                continue
            candidates.append((priority, end_ts, val_num))

    if not candidates:
        return None
    best = min(candidates, key=lambda c: (c[0], -c[1]))
    return best[2]


def _extract_latest_fact_any(
    facts: Dict,
    keys,
    units=("USD",),
    forms_priority=SEC_FORM_PRIORITY,
) -> Optional[float]:
    """Try multiple fact keys and return the first available latest value."""
    for key in keys:
        val = _extract_latest_fact(facts, key, units=units, forms_priority=forms_priority)
        if val is not None:
            return val
    return None


def _extract_latest_fact_multi(
    facts: Dict,
    keys,
    units=("USD",),
    forms_priority=SEC_FORM_PRIORITY,
) -> Optional[float]:
    """Choose the latest/most-prioritized fact across multiple keys."""
    facts_root = (facts or {}).get("facts", {}).get("us-gaap", {})
    priority_map = {form: idx for idx, form in enumerate(forms_priority)}
    candidates = []
    today_ord = datetime.date.today().toordinal()

    for key in keys:
        entry = facts_root.get(key) or {}
        unit_map = entry.get("units") or {}
        for unit in units:
            for item in unit_map.get(unit, []):
                val = item.get("val")
                if val in (None, "", "-", "NaN"):
                    continue
                try:
                    val_num = float(val)
                except Exception:
                    continue
                form = item.get("form", "")
                priority = priority_map.get(form, len(forms_priority))
                end_ts = _parse_iso_date(item.get("end") or "") or _parse_iso_date(item.get("filed") or "")
                if end_ts and end_ts > today_ord:
                    continue
                candidates.append((priority, end_ts, val_num))

    if not candidates:
        return None
    best = min(candidates, key=lambda c: (c[0], -c[1]))
    return best[2]


def _coerce_year(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _year_from_iso_date(date_text: str) -> Optional[int]:
    try:
        return datetime.date.fromisoformat(date_text).year
    except Exception:
        return None


def _extract_annual_series(
    facts: Dict,
    keys,
    units=("USD",),
    forms_priority=SEC_FORM_PRIORITY,
) -> Dict[int, float]:
    facts_root = (facts or {}).get("facts", {}).get("us-gaap", {})
    priority_map = {form: idx for idx, form in enumerate(forms_priority)}
    key_priority = {key: idx for idx, key in enumerate(keys)}
    today_ord = datetime.date.today().toordinal()
    per_year: Dict[int, Tuple[int, int, int, float]] = {}

    for key in keys:
        entry = facts_root.get(key) or {}
        unit_map = entry.get("units") or {}
        for unit in units:
            for item in unit_map.get(unit, []):
                val = item.get("val")
                if val in (None, "", "-", "NaN"):
                    continue
                try:
                    val_num = float(val)
                except Exception:
                    continue
                fp = item.get("fp")
                if fp and fp != "FY":
                    continue
                end_ts = _parse_iso_date(item.get("end") or "") or _parse_iso_date(item.get("filed") or "")
                if end_ts and end_ts > today_ord:
                    continue
                year = _coerce_year(item.get("fy")) or _year_from_iso_date(item.get("end") or "")
                if not year:
                    continue
                form = item.get("form", "")
                priority = priority_map.get(form, len(forms_priority))
                key_rank = key_priority.get(key, len(keys))
                candidate = (priority, key_rank, end_ts, val_num)
                existing = per_year.get(year)
                if existing is None:
                    per_year[year] = candidate
                    continue
                if candidate[0] < existing[0]:
                    per_year[year] = candidate
                elif candidate[0] == existing[0]:
                    if candidate[1] < existing[1]:
                        per_year[year] = candidate
                    elif candidate[1] == existing[1] and candidate[2] > existing[2]:
                        per_year[year] = candidate

    return {year: data[3] for year, data in per_year.items()}


def _build_recent_year_window(
    values_by_year: Dict[int, Optional[float]], window_years: int
) -> List[Tuple[int, Optional[float]]]:
    if not values_by_year:
        return []
    valid_years = [year for year, value in values_by_year.items() if value is not None]
    if not valid_years:
        return []
    max_year = max(valid_years)
    start_year = max_year - window_years
    return [(year, values_by_year.get(year)) for year in range(start_year, max_year + 1)]


def format_yoy_average(values_by_year: Dict[int, Optional[float]], window_years: int = 5) -> str:
    series = _build_recent_year_window(values_by_year, window_years)
    if len(series) < 2:
        return "N/A"
    positive_rates: List[float] = []
    transitions: List[str] = []
    for (prev_year, prev_val), (curr_year, curr_val) in zip(series, series[1:]):
        if prev_val is None or curr_val is None:
            continue
        if prev_val > 0 and curr_val > 0:
            try:
                positive_rates.append((curr_val - prev_val) / prev_val)
            except Exception:
                continue
        else:
            if prev_val <= 0 and curr_val > 0:
                transitions.append(f"적자→흑자 {prev_year}→{curr_year}")
            elif prev_val > 0 and curr_val <= 0:
                transitions.append(f"흑자→적자 {prev_year}→{curr_year}")

    parts = []
    if positive_rates:
        avg_rate = sum(positive_rates) / len(positive_rates)
        avg_text = f"{avg_rate * 100:,.2f}%"
        parts.append(f"양(+) 구간 {len(positive_rates)}개 평균")
    else:
        avg_text = "N/A"
        parts.append("양(+) 구간 없음")
    if transitions:
        parts.append(f"특이: {', '.join(transitions)}")
    suffix = "; ".join(parts)
    return f"{avg_text} ({suffix})" if suffix else avg_text


def yahoo_symbol_for_ticker(ticker: str) -> str:
    symbol = (ticker or "").strip().upper()
    if "." in symbol and "-" not in symbol:
        return symbol.replace(".", "-")
    return symbol


def fetch_yahoo_quotes_batch(tickers: List[str], *, max_retries: int = 3) -> Dict[str, Dict[str, Optional[float]]]:
    symbols = [yahoo_symbol_for_ticker(t) for t in (tickers or []) if str(t).strip()]
    if not symbols:
        return {}

    last_status = None
    for attempt in range(max_retries + 1):
        resp = requests.get(YAHOO_QUOTE_URL, params={"symbols": ",".join(symbols)}, timeout=10)
        last_status = resp.status_code
        if resp.status_code == 200:
            break
        if resp.status_code in (429, 500, 502, 503, 504):
            retry_after = resp.headers.get("Retry-After")
            try:
                wait_sec = int(float(retry_after)) if retry_after else 0
            except Exception:
                wait_sec = 0
            if wait_sec <= 0:
                wait_sec = min(30, 2 ** attempt)
            time.sleep(wait_sec)
            continue
        raise EdgarError(f"Quote request failed: HTTP {resp.status_code}")

    if resp.status_code != 200:
        raise EdgarError(f"Quote request failed: HTTP {last_status}")

    try:
        result = resp.json().get("quoteResponse", {}).get("result", [])
    except Exception as exc:
        raise EdgarError(f"Invalid quote response: {exc}") from exc

    quotes: Dict[str, Dict[str, Optional[float]]] = {}
    for entry in result or []:
        symbol = str(entry.get("symbol") or "").upper().strip()
        if not symbol:
            continue
        quotes[symbol] = {
            "price": entry.get("regularMarketPrice"),
            "per": entry.get("trailingPE"),
            "pbr": entry.get("priceToBook"),
            "currency": entry.get("currency"),
            "source": "yahoo",
        }
    return quotes


def fetch_yahoo_quote(ticker: str) -> Dict[str, Optional[float]]:
    last_error = None
    for symbol in (ticker, yahoo_symbol_for_ticker(ticker)):
        symbol = str(symbol or "").strip()
        if not symbol:
            continue
        try:
            resp = requests.get(YAHOO_QUOTE_URL, params={"symbols": symbol}, timeout=10)
            if resp.status_code != 200:
                raise EdgarError(f"Quote request failed: HTTP {resp.status_code}")
            try:
                result = resp.json().get("quoteResponse", {}).get("result", [])
            except Exception as exc:
                raise EdgarError(f"Invalid quote response: {exc}") from exc

            if not result:
                raise EdgarError("Quote not found for ticker.")
            entry = result[0]
            return {
                "price": entry.get("regularMarketPrice"),
                "per": entry.get("trailingPE"),
                "pbr": entry.get("priceToBook"),
                "currency": entry.get("currency"),
                "source": "yahoo",
            }
        except Exception as exc:
            last_error = str(exc)

    return fetch_stooq_quote(ticker, last_error)


@lru_cache(maxsize=1)
def fetch_usdkrw_rate() -> Optional[float]:
    """Fetch USD/KRW on each lookup with optional .env override fallback."""
    env_rate = parse_float(os.getenv("USD_KRW_RATE"))
    if env_rate:
        return env_rate
    try:
        resp = requests.get(YAHOO_QUOTE_URL, params={"symbols": "USDKRW=X"}, timeout=10)
        if resp.status_code != 200:
            raise EdgarError(f"USD/KRW request failed: HTTP {resp.status_code}")
        result = resp.json().get("quoteResponse", {}).get("result", [])
        if not result:
            raise EdgarError("USD/KRW quote not found")
        price = result[0].get("regularMarketPrice")
        if price is not None:
            return float(price)
    except Exception:
        pass

    # Fallback: Stooq daily close for usdkrw.
    try:
        resp = requests.get(STOOQ_QUOTE_URL, params={"s": "usdkrw", "i": "d"}, timeout=10)
        if resp.status_code != 200:
            return env_rate
        lines = resp.text.strip().splitlines()
        if not lines or "," not in lines[0]:
            return env_rate
        parts = lines[0].split(",")
        if len(parts) >= 7:
            close_price = parse_float(parts[6])
            if close_price:
                return close_price
    except Exception:
        return env_rate

    return env_rate


def fetch_stooq_quote(ticker: str, yahoo_error: Optional[str] = None) -> Dict[str, Optional[float]]:
    symbol = f"{yahoo_symbol_for_ticker(ticker).lower()}.us"
    resp = requests.get(STOOQ_QUOTE_URL, params={"s": symbol, "i": "d"}, timeout=10)
    if resp.status_code != 200:
        raise EdgarError(
            f"Quote request failed (Stooq fallback HTTP {resp.status_code}) after Yahoo error: {yahoo_error or 'N/A'}"
        )
    lines = resp.text.strip().splitlines()
    if not lines or "," not in lines[0]:
        raise EdgarError("Quote not found for ticker (Stooq fallback).")
    parts = lines[0].split(",")
    if len(parts) < 7:
        raise EdgarError("Unexpected Stooq quote format.")
    try:
        close_price = float(parts[6])
    except Exception:
        raise EdgarError("Invalid close price from Stooq.")

    return {
        "price": close_price,
        "per": None,
        "pbr": None,
        "currency": "USD",
        "source": "stooq",
    }


def clean_number(val: str) -> str:
    try:
        return f"{float(val):,}"
    except (ValueError, TypeError):
        return str(val)


@dataclass
class PriceSnapshot:
    name: str
    code: str
    price: str
    per: str
    pbr: str
    cash: str = "N/A"
    debt_ratio: str = "N/A"
    listed_shares: Optional[int] = None
    net_cash_per_share_ratio: Optional[str] = None


class KisClient:
    """Minimal client for Korea Investment OpenAPI to get price/PER/PBR and simple financials."""

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        base_url: Optional[str] = None,
    ):
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = base_url.rstrip("/") if base_url else "https://openapivts.koreainvestment.com:29443"
        self.session = requests.Session()
        self._token: Optional[str] = None
        self._token_expiry: float = 0

    def _token_url(self) -> str:
        # tokenP for paper trading; switch to token for production if needed.
        return f"{self.base_url}/oauth2/tokenP"

    def _price_url(self) -> str:
        return f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price"

    def _financial_ratio_url(self) -> str:
        return f"{self.base_url}/uapi/domestic-stock/v1/finance/financial-ratio"

    def _balance_sheet_url(self) -> str:
        return f"{self.base_url}/uapi/domestic-stock/v1/finance/balance-sheet"

    def _ensure_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expiry - 30:
            return self._token

        payload = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        resp = self.session.post(self._token_url(), json=payload, timeout=10)
        if resp.status_code != 200:
            raise KisError(f"Token request failed: HTTP {resp.status_code} {resp.text}")

        data = resp.json()
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 0)
        if not access_token:
            raise KisError(f"Token response missing access_token: {data}")

        self._token = access_token
        self._token_expiry = now + int(expires_in or 0)
        return access_token

    def _authorized_headers(self, tr_id: str) -> Dict[str, str]:
        token = self._ensure_token()
        return {
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

    def get_price_snapshot(self, stock_code: str) -> PriceSnapshot:
        headers = self._authorized_headers("FHKST01010100")  # price lookup TR
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # stock
            "FID_INPUT_ISCD": stock_code,
        }
        resp = self.session.get(self._price_url(), headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            raise KisError(f"Price request failed: HTTP {resp.status_code} {resp.text}")

        data = resp.json()
        output = data.get("output", {}) if isinstance(data, dict) else {}
        if not output:
            raise KisError(f"Unexpected price response payload: {data}")

        name = output.get("hts_kor_isnm", "").strip() or output.get("prdt_name", "")
        price = output.get("stck_prpr", "")
        per = output.get("per", "")
        pbr = output.get("pbr", "")
        listed_shares = _parse_int(output.get("lstn_stcn"))
        net_cash_ratio = None

        return PriceSnapshot(
            name=name or "N/A",
            code=stock_code,
            price=clean_number(price),
            per=per if per != "" else "N/A",
            pbr=pbr if pbr != "" else "N/A",
            listed_shares=listed_shares,
            net_cash_per_share_ratio=net_cash_ratio,
        )

    def _first_in_output(self, payload) -> Dict:
        if isinstance(payload, list) and payload:
            return payload[0] or {}
        if isinstance(payload, dict):
            return payload
        return {}

    def get_financial_highlights(self, stock_code: str) -> Tuple[str, str]:
        """Return (cash, debt_ratio) for the latest period."""
        cash_display = "N/A"
        debt_display = "N/A"

        # Financial ratio for debt ratio.
        ratio_params = {
            "FID_DIV_CLS_CODE": "0",  # 0: year
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code,
        }
        try:
            headers = self._authorized_headers("FHKST66430300")
            resp = self.session.get(self._financial_ratio_url(), headers=headers, params=ratio_params, timeout=10)
            if resp.status_code == 200:
                payload = resp.json().get("output", {})
                entry = self._first_in_output(payload)
                debt_candidates = [
                    "lblt_rate",  # liabilities (debt) ratio
                    "lblt_rto",
                    "lblt_rt",
                    "debt_rto",
                    "debt_ratio",
                    "debt_rt",
                ]
                debt_display = self._pick_number(entry, debt_candidates, default="N/A")
        except Exception:
            pass

        # Balance sheet for cash.
        bs_params = {
            "FID_DIV_CLS_CODE": "0",  # 0: year
            "fid_cond_mrkt_div_code": "J",
            "fid_input_iscd": stock_code,
        }
        try:
            headers = self._authorized_headers("FHKST66430100")
            resp = self.session.get(self._balance_sheet_url(), headers=headers, params=bs_params, timeout=10)
            if resp.status_code == 200:
                payload = resp.json().get("output", {})
                entry = self._first_in_output(payload)
                cash_display = self._pick_cash(entry, default="N/A")
        except Exception:
            pass

        return cash_display, debt_display

    def _pick_number(self, entry: Dict, candidates, default: str) -> str:
        for key in candidates:
            if key in entry and entry[key] not in ("", None):
                return clean_number(entry[key])
        # fallback: only consider fields that clearly look like debt/liability ratios.
        for key, val in entry.items():
            if val in ("", None):
                continue
            lower = key.lower()
            if any(token in lower for token in ("lblt", "debt", "liab", "부채")):
                return clean_number(val)
        return default

    def _pick_cash(self, entry: Dict, default: str) -> str:
        """Try to find a cash or cash-equivalent field in the balance sheet output."""
        for key, val in entry.items():
            if val in ("", None):
                continue
            lower = key.lower()
            if any(token in lower for token in ("cash", "csh", "현금")):
                return clean_number(val)
        # fallback: pick first numeric-ish field if it looks like a large asset number
        for key, val in entry.items():
            if val in ("", None):
                continue
            if isinstance(val, (int, float)):
                return clean_number(val)
            if isinstance(val, str):
                digits = "".join(ch for ch in val if ch.isdigit() or ch == ".")
                if digits:
                    return clean_number(val)
        return default

    def get_snapshot_with_financials(self, stock_code: str) -> PriceSnapshot:
        snapshot = self.get_price_snapshot(stock_code)
        cash, debt_ratio = self.get_financial_highlights(stock_code)
        snapshot.cash = cash
        snapshot.debt_ratio = debt_ratio
        return snapshot


def extract_edgar_scan_fundamentals(facts: Dict) -> Dict[str, Any]:
    """Extract EDGAR fundamentals needed for US range scanning (no quote-dependent metrics)."""

    def pick_fact(tags) -> Optional[int]:
        for tag in tags:
            val = _extract_latest_fact(facts, tag)
            if val is not None:
                return _parse_int(val)
        return None

    cash_val = pick_fact(("CashAndCashEquivalentsAtCarryingValue",))
    current_marketable = pick_fact(("MarketableSecuritiesCurrent", "ShortTermInvestments"))
    noncurrent_marketable = pick_fact(
        (
            "MarketableSecuritiesNoncurrent",
            "AvailableForSaleSecuritiesDebtSecuritiesNoncurrent",
            "LongTermInvestments",
            "LongTermMarketableSecurities",
            "OtherInvestmentsNoncurrent",
        )
    )
    liquid_funds_total = _sum_or_none([cash_val, current_marketable, noncurrent_marketable])

    debt_current_base = pick_fact(
        (
            "DebtCurrent",
            "LongTermDebtCurrent",
            "CurrentPortionOfLongTermDebt",
            "CurrentPortionOfLongTermDebtAndCapitalLeaseObligations",
            "ShortTermBorrowings",
        )
    )
    commercial_paper_val = _parse_int(_extract_latest_fact(facts, "CommercialPaper"))
    debt_noncurrent = pick_fact(
        (
            "LongTermDebtNoncurrent",
            "DebtNoncurrent",
            "LongTermBorrowings",
            "LongTermLoansPayable",
            "LongTermNotesPayable",
            "LongTermConvertibleDebt",
        )
    )
    debt_total_only = pick_fact(
        (
            "LongTermDebt",
            "DebtAndCapitalLeaseObligations",
            "LongTermDebtAndCapitalLeaseObligations",
        )
    )

    current_debt_total = None
    if debt_current_base is None:
        current_debt_total = commercial_paper_val
    elif commercial_paper_val is None:
        current_debt_total = debt_current_base
    else:
        try:
            rel_diff = abs(debt_current_base - commercial_paper_val) / max(debt_current_base, commercial_paper_val)
        except Exception:
            rel_diff = 0
        if rel_diff <= 0.01:
            current_debt_total = max(debt_current_base, commercial_paper_val)
        else:
            current_debt_total = debt_current_base + commercial_paper_val

    total_debt = None
    if current_debt_total is not None or debt_noncurrent is not None:
        total_debt = _sum_or_none([current_debt_total, debt_noncurrent])
    elif debt_total_only is not None:
        total_debt = debt_total_only

    net_cash, debt_value = compute_net_cash(
        liquid_funds_total, total_debt, assume_zero_debt_when_missing=ASSUME_ZERO_DEBT_WHEN_MISSING
    )

    shares = _parse_int(_extract_latest_fact(facts, "CommonStockSharesOutstanding", units=("shares",)))
    net_income = _parse_int(_extract_latest_fact(facts, "NetIncomeLoss"))
    equity = _parse_int(
        _extract_latest_fact(facts, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
        or _extract_latest_fact(facts, "StockholdersEquity")
    )
    liabilities = _parse_int(_extract_latest_fact(facts, "Liabilities"))

    liabilities_ratio_value = None
    if liabilities is not None and equity not in (None, 0):
        try:
            liabilities_ratio_value = (liabilities / equity) * 100
        except Exception:
            liabilities_ratio_value = None

    interest_bearing_ratio_value = None
    if debt_value is not None and equity not in (None, 0):
        try:
            interest_bearing_ratio_value = (debt_value / equity) * 100
        except Exception:
            interest_bearing_ratio_value = None

    net_cash_per_share_value = None
    if net_cash is not None and shares:
        try:
            net_cash_per_share_value = net_cash / shares
        except Exception:
            net_cash_per_share_value = None

    return {
        "liquid_funds_total": liquid_funds_total,
        "interest_bearing_debt": debt_value,
        "net_cash": net_cash,
        "shares": shares,
        "net_income": net_income,
        "equity": equity,
        "liabilities": liabilities,
        "liabilities_ratio_value": liabilities_ratio_value,
        "interest_bearing_debt_ratio_value": interest_bearing_ratio_value,
        "net_cash_per_share_value": net_cash_per_share_value,
    }


def fetch_edgar_financials(user_text: str) -> Tuple[PriceSnapshot, Dict[str, str]]:
    company = resolve_edgar_company(user_text)
    ticker = company.get("ticker") or user_text
    cik = company.get("cik") or ""
    name = company.get("name") or ticker

    facts = load_company_facts(cik)

    def pick_fact(tags):
        for tag in tags:
            val = _extract_latest_fact(facts, tag)
            if val is not None:
                return _parse_int(val), tag
        return None, None

    cash_val, cash_tag = pick_fact(("CashAndCashEquivalentsAtCarryingValue",))
    current_marketable, current_marketable_tag = pick_fact(
        ("MarketableSecuritiesCurrent", "ShortTermInvestments")
    )
    noncurrent_marketable, noncurrent_marketable_tag = pick_fact(
        (
            "MarketableSecuritiesNoncurrent",
            "AvailableForSaleSecuritiesDebtSecuritiesNoncurrent",
            "LongTermInvestments",
            "LongTermMarketableSecurities",
            "OtherInvestmentsNoncurrent",
        )
    )

    liquid_funds_current = _sum_or_none([cash_val, current_marketable])
    liquid_funds_total = _sum_or_none([cash_val, current_marketable, noncurrent_marketable])

    # Debt: prefer explicit current/noncurrent; fall back to total-only tag.
    debt_current_base, debt_current_base_tag = pick_fact(
        (
            "DebtCurrent",
            "LongTermDebtCurrent",
            "CurrentPortionOfLongTermDebt",
            "CurrentPortionOfLongTermDebtAndCapitalLeaseObligations",
            "ShortTermBorrowings",
        )
    )
    commercial_paper_val = _parse_int(_extract_latest_fact(facts, "CommercialPaper"))
    debt_noncurrent, debt_noncurrent_tag = pick_fact(
        (
            "LongTermDebtNoncurrent",
            "DebtNoncurrent",
            "LongTermBorrowings",
            "LongTermLoansPayable",
            "LongTermNotesPayable",
            "LongTermConvertibleDebt",
        )
    )
    debt_total_only, debt_total_tag = pick_fact(
        (
            "LongTermDebt",
            "DebtAndCapitalLeaseObligations",
            "LongTermDebtAndCapitalLeaseObligations",
        )
    )

    # Combine current debt base + commercial paper with de-duplication tolerance.
    current_debt_total = None
    if debt_current_base is None:
        current_debt_total = commercial_paper_val
    elif commercial_paper_val is None:
        current_debt_total = debt_current_base
    else:
        try:
            rel_diff = abs(debt_current_base - commercial_paper_val) / max(debt_current_base, commercial_paper_val)
        except Exception:
            rel_diff = 0
        if rel_diff <= 0.01:
            current_debt_total = max(debt_current_base, commercial_paper_val)
        else:
            current_debt_total = debt_current_base + commercial_paper_val

    total_debt = None
    debt_tags_used = {}
    if current_debt_total is not None or debt_noncurrent is not None:
        total_debt = _sum_or_none([current_debt_total, debt_noncurrent])
        if debt_current_base_tag:
            debt_tags_used["current_base"] = debt_current_base_tag
        if commercial_paper_val is not None:
            debt_tags_used["commercial_paper"] = "CommercialPaper"
        if debt_noncurrent_tag:
            debt_tags_used["noncurrent"] = debt_noncurrent_tag
    elif debt_total_only is not None:
        total_debt = debt_total_only
        if debt_total_tag:
            debt_tags_used["total_only"] = debt_total_tag

    net_cash, debt_value = compute_net_cash(
        liquid_funds_total, total_debt, assume_zero_debt_when_missing=ASSUME_ZERO_DEBT_WHEN_MISSING
    )

    shares = _parse_int(_extract_latest_fact(facts, "CommonStockSharesOutstanding", units=("shares",)))

    revenue_keys = (
        "Revenues",
        "SalesRevenueNet",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "SalesRevenueGoodsNet",
        "SalesRevenueServicesNet",
    )
    revenue = _parse_int(_extract_latest_fact_multi(facts, revenue_keys))
    op_income = _parse_int(_extract_latest_fact(facts, "OperatingIncomeLoss"))
    net_income = _parse_int(_extract_latest_fact(facts, "NetIncomeLoss"))
    equity = _parse_int(
        _extract_latest_fact(facts, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest")
        or _extract_latest_fact(facts, "StockholdersEquity")
    )
    sales_growth_5y = "N/A"
    op_growth_5y = "N/A"
    net_income_growth_5y = "N/A"
    try:
        revenue_series = _extract_annual_series(facts, revenue_keys)
        op_income_series = _extract_annual_series(facts, ("OperatingIncomeLoss",))
        net_income_series = _extract_annual_series(facts, ("NetIncomeLoss",))
        sales_growth_5y = format_yoy_average(revenue_series, window_years=5)
        op_growth_5y = format_yoy_average(op_income_series, window_years=5)
        net_income_growth_5y = format_yoy_average(net_income_series, window_years=5)
    except Exception:
        pass
    usdkrw_rate = fetch_usdkrw_rate()

    quote = {}
    quote_error = None
    try:
        quote = fetch_yahoo_quote(ticker)
    except Exception as exc:
        quote_error = str(exc)
        quote = {}

    price_val = quote.get("price")
    per_val = quote.get("per")
    pbr_val = quote.get("pbr")
    quote_source = quote.get("source")

    market_price = float(price_val) if price_val is not None else None
    market_cap = market_price * shares if market_price is not None and shares else None

    # Compute PER/PBR from EDGAR fundamentals when quote source lacks ratios.
    if per_val is None and market_cap is not None and net_income not in (None, 0):
        try:
            per_val = market_cap / net_income
        except Exception:
            per_val = None
    if pbr_val is None and market_cap is not None and equity not in (None, 0):
        try:
            pbr_val = market_cap / equity
        except Exception:
            pbr_val = None

    liabilities = _parse_int(_extract_latest_fact(facts, "Liabilities"))
    liabilities_ratio_pct = None
    if liabilities is not None and equity not in (None, 0):
        try:
            liabilities_ratio_pct = (liabilities / equity) * 100
        except Exception:
            liabilities_ratio_pct = None
    liabilities_ratio_text = f"{liabilities_ratio_pct:,.2f}" if liabilities_ratio_pct is not None else "N/A"

    debt_ratio_pct = None
    if debt_value is not None and equity not in (None, 0):
        try:
            debt_ratio_pct = (debt_value / equity) * 100
        except Exception:
            debt_ratio_pct = None
    debt_ratio_text = f"{debt_ratio_pct:,.2f}" if debt_ratio_pct is not None else "N/A"

    net_cash_per_share_value = None
    if net_cash is not None and shares:
        try:
            net_cash_per_share_value = net_cash / shares
        except Exception:
            net_cash_per_share_value = None

    net_cash_per_share = format_per_share(net_cash, shares)
    net_cash_per_share_ratio = "N/A"
    if net_cash_per_share_value is not None and market_price and market_price > 0:
        try:
            ratio = (net_cash_per_share_value / market_price) * 100
            net_cash_per_share_ratio = f"{ratio:,.2f}%"
        except Exception:
            net_cash_per_share_ratio = "N/A"

    snapshot = PriceSnapshot(
        name=name,
        code=ticker,
        price=clean_number(price_val) if price_val is not None else "N/A",
        per=clean_number(per_val) if per_val is not None else "N/A",
        pbr=clean_number(pbr_val) if pbr_val is not None else "N/A",
        cash=format_amount(liquid_funds_total) if liquid_funds_total is not None else "N/A",
        debt_ratio=liabilities_ratio_text,
        listed_shares=shares,
        net_cash_per_share_ratio=net_cash_per_share_ratio,
    )

    detail_summary = {
        "매출액": format_usd_with_krw(revenue, usdkrw_rate),
        "영업이익": format_usd_with_krw(op_income, usdkrw_rate),
        "자본총계": format_usd_with_krw(equity, usdkrw_rate),
    }

    detail = {
        "corp_name": name,
        "corp_code": ticker,
        "cik": cik,
        "bsns_year": "-",  # EDGAR facts API is period-agnostic; surface aggregate only.
        "summary": detail_summary,
        "sales_growth_5y": sales_growth_5y,
        "op_growth_5y": op_growth_5y,
        "net_income_growth_5y": net_income_growth_5y,
        "liquid_funds_total": liquid_funds_total,
        "liquid_funds_current": liquid_funds_current,
        "liquid_funds_noncurrent": noncurrent_marketable,
        "interest_bearing_debt": debt_value,
        "net_cash": net_cash,
        "net_cash_display": format_amount(net_cash) if net_cash is not None else "N/A",
        "float_shares": shares,
        "float_shares_display": format_amount(shares) if shares is not None else None,
        "net_cash_per_share": net_cash_per_share,
        "net_cash_per_share_ratio": net_cash_per_share_ratio,
        "liabilities_ratio_value": liabilities_ratio_pct,
        "interest_bearing_debt_ratio": debt_ratio_text,
        "interest_bearing_debt_ratio_value": debt_ratio_pct,
        "quote_source": quote_source,
        "quote_error": quote_error,
        "debt_ratio": liabilities_ratio_text,
        "usd_krw_rate": usdkrw_rate,
        "edgar_liquid_breakdown": {
            "cash_and_equivalents": cash_val,
            "marketable_securities_current": current_marketable,
            "marketable_securities_noncurrent": noncurrent_marketable,
            "total_current": liquid_funds_current,
            "total_including_noncurrent": liquid_funds_total,
        },
        "edgar_liquid_tags_used": {
            "cash": cash_tag,
            "marketable_current": current_marketable_tag,
            "marketable_noncurrent": noncurrent_marketable_tag,
        },
        "edgar_debt_tags_used": debt_tags_used,
    }

    return snapshot, detail


def resolve_code(user_text: str) -> Optional[str]:
    if not user_text:
        return None
    trimmed = user_text.strip()
    digits = "".join(ch for ch in trimmed if ch.isdigit())
    if len(digits) >= 6:
        return digits[:6]

    return lookup_code_by_name(trimmed)


def load_keys() -> Tuple[str, str, Optional[str]]:
    app_key = os.getenv("KIS_APP_KEY")
    app_secret = os.getenv("KIS_APP_SECRET")
    base_url = os.getenv("KIS_BASE_URL")
    if not app_key or not app_secret:
        raise KisError("Set KIS_APP_KEY and KIS_APP_SECRET in your environment or .env file.")
    return app_key, app_secret, base_url


def run_cli(symbol: Optional[str]) -> int:
    prompt = "Enter company name or 6-digit code (e.g., Samsung Electronics or 005930): "
    user_input = symbol or input(prompt).strip()

    try:
        code = resolve_code(user_input)
    except KisError as exc:
        print(f"Name lookup failed: {exc}", file=sys.stderr)
        return 1

    if not code:
        print("Input error: provide a valid company name or 6-digit code.", file=sys.stderr)
        return 1

    try:
        app_key, app_secret, base_url = load_keys()
        client = KisClient(app_key, app_secret, base_url=base_url)
        snapshot = client.get_snapshot_with_financials(code)
    except Exception as exc:  # broad catch for a simple CLI
        print(f"Lookup failed: {exc}", file=sys.stderr)
        return 1

    print(f"{snapshot.name} ({snapshot.code})")
    print(f"Price: {snapshot.price}")
    print(f"PER: {snapshot.per}")
    print(f"PBR: {snapshot.pbr}")
    print(f"Cash: {snapshot.cash}")
    print(f"Debt ratio: {snapshot.debt_ratio}")
    return 0


def run_dart_cli(symbol: Optional[str], year: Optional[str]) -> int:
    prompt = "회사명을 입력하세요 (예: 삼성전자): "
    user_input = symbol or input(prompt).strip()
    fallback_listed_shares = None
    market_price = None

    # Try KIS to get 상장주식수 for fallback when DART 유통주식수 is missing.
    try:
        stock_code = resolve_code(user_input)
        app_key = os.getenv("KIS_APP_KEY")
        app_secret = os.getenv("KIS_APP_SECRET")
        base_url = os.getenv("KIS_BASE_URL")
        if stock_code and app_key and app_secret:
            kis_client = KisClient(app_key, app_secret, base_url=base_url)
            price_snapshot = kis_client.get_price_snapshot(stock_code)
            fallback_listed_shares = price_snapshot.listed_shares
            market_price = parse_amount(price_snapshot.price)
    except Exception:
        fallback_listed_shares = None

    try:
        result = fetch_dart_financials(
            user_input,
            bsns_year=year,
            fallback_listed_shares=fallback_listed_shares,
            market_price=market_price,
        )
    except DartError as exc:
        print(f"DART 조회 실패: {exc}", file=sys.stderr)
        return 1

    corp_name = result.get("corp_name", "-")
    corp_code = result.get("corp_code", "-")
    bsns_year = result.get("bsns_year", "-")
    summary = result.get("summary", {})
    ncs = result.get("net_cash_per_share", "N/A")
    ncs_ratio = result.get("net_cash_per_share_ratio", "N/A")
    net_cash_display = result.get("net_cash_display", "N/A")
    ib_debt_ratio = result.get("interest_bearing_debt_ratio", "N/A")

    print(f"{corp_name} ({corp_code}) - 사업연도 {bsns_year}")
    print(f"주당 순현금: {ncs}")
    print(f"주당 순현금/주가: {ncs_ratio}")
    print(f"순현금(총액): {net_cash_display}")
    print(f"이자부채/자본: {ib_debt_ratio}")
    for label in ("매출액", "영업이익", "당기순이익", "자산총계", "부채총계", "자본총계"):
        print(f"{label}: {summary.get(label, 'N/A')}")
    return 0


def gui_supported() -> Tuple[bool, Optional[str]]:
    if sys.platform != "darwin":
        return True, None

    release = platform.mac_ver()[0] or ""
    parts = release.split(".")
    try:
        major = int(parts[0])
        minor = int(parts[1]) if len(parts) > 1 else 0
    except ValueError:
        return True, None

    if major == 14 and minor < 7:
        return False, f"Tk GUI is unstable on macOS {release}. Use --cli mode instead."
    return True, None


def build_gui():
    import tkinter as tk
    from tkinter import ttk, messagebox

    root = tk.Tk()
    root.title("KIS/DART/EDGAR Viewer")
    root.geometry("640x490")
    root.resizable(False, False)

    root.configure(padx=14, pady=12, bg="#f7f7f7")
    style = ttk.Style()
    style.theme_use("clam")
    for widget in ("TLabel", "TButton", "TEntry"):
        style.configure(widget, font=("Segoe UI", 10))
    style.configure("TButton", padding=6)

    country_var = tk.StringVar(value="US")
    input_var = tk.StringVar()
    status_var = tk.StringVar(
        value="Select a country, then enter a company. KR requires KIS/DART keys; US uses EDGAR."
    )
    name_var = tk.StringVar(value="-")
    price_var = tk.StringVar(value="-")
    per_var = tk.StringVar(value="-")
    pbr_var = tk.StringVar(value="-")
    debt_var = tk.StringVar(value="-")
    debt_label_var = tk.StringVar(value="Liabilities/Equity (EDGAR, %)")
    ib_debt_var = tk.StringVar(value="-")
    ib_debt_label_var = tk.StringVar(value="Interest-bearing debt/Equity (EDGAR, %)")
    dart_year_var = tk.StringVar(value="-")
    dart_net_cash_ps_var = tk.StringVar(value="-")
    dart_net_cash_ps_ratio_var = tk.StringVar(value="-")
    dart_sales_var = tk.StringVar(value="-")
    dart_op_var = tk.StringVar(value="-")
    dart_sales_growth_var = tk.StringVar(value="-")
    dart_op_growth_var = tk.StringVar(value="-")
    dart_net_income_growth_var = tk.StringVar(value="-")
    dart_equity_var = tk.StringVar(value="-")

    def open_scan_modal():
        selected_country = country_var.get()
        modal = tk.Toplevel(root)
        modal.title(f"Range Scan ({selected_country})")
        modal.geometry("720x520")
        modal.resizable(True, True)

        per_min_var = tk.StringVar()
        per_max_var = tk.StringVar()
        pbr_min_var = tk.StringVar()
        pbr_max_var = tk.StringVar()
        debt_min_var = tk.StringVar()
        debt_max_var = tk.StringVar()
        ib_debt_min_var = tk.StringVar()
        ib_debt_max_var = tk.StringVar()
        ncs_ratio_min_var = tk.StringVar()
        ncs_ratio_max_var = tk.StringVar()
        scan_status_var = tk.StringVar(value="범위를 입력하거나 All을 체크 후 Scan을 눌러주세요.")

        controls = ttk.Frame(modal, padding=(8, 8))
        controls.pack(fill="x")

        def add_field(row, label, min_var, max_var):
            ttk.Label(controls, text=label).grid(row=row, column=0, sticky="w", pady=2)
            min_entry = ttk.Entry(controls, textvariable=min_var, width=10)
            min_entry.grid(row=row, column=1, sticky="w", padx=(4, 8))
            ttk.Label(controls, text="~").grid(row=row, column=2, sticky="w")
            max_entry = ttk.Entry(controls, textvariable=max_var, width=10)
            max_entry.grid(row=row, column=3, sticky="w", padx=(4, 12))

            all_var = tk.BooleanVar(value=False)
            prev = {"min": "", "max": ""}

            def toggle_all():
                if all_var.get():
                    prev["min"] = min_var.get()
                    prev["max"] = max_var.get()
                    min_var.set("all")
                    max_var.set("all")
                    min_entry.configure(state="disabled")
                    max_entry.configure(state="disabled")
                else:
                    restore_min = prev.get("min", "")
                    restore_max = prev.get("max", "")
                    min_var.set("" if str(restore_min).strip().lower() == "all" else restore_min)
                    max_var.set("" if str(restore_max).strip().lower() == "all" else restore_max)
                    min_entry.configure(state="normal")
                    max_entry.configure(state="normal")

            ttk.Checkbutton(controls, text="All", variable=all_var, command=toggle_all).grid(row=row, column=4, sticky="w")

        debt_label = "부채비율(KIS)" if selected_country == "KR" else "Liabilities/Equity (EDGAR, %)"
        ib_label = "이자부채/자본(%)" if selected_country == "KR" else "Interest-bearing debt/Equity (EDGAR, %)"
        ncs_label = "주당 순현금/주가(%)" if selected_country == "KR" else "Net cash/share ÷ Price (%)"
        ncs_col_label = "주당순현금/주가" if selected_country == "KR" else "Net cash/share ÷ Price"

        add_field(0, "PER", per_min_var, per_max_var)
        add_field(1, "PBR", pbr_min_var, pbr_max_var)
        add_field(2, debt_label, debt_min_var, debt_max_var)
        add_field(3, ib_label, ib_debt_min_var, ib_debt_max_var)
        add_field(4, ncs_label, ncs_ratio_min_var, ncs_ratio_max_var)

        ttk.Button(controls, text="Scan", command=lambda: start_scan()).grid(row=0, column=5, rowspan=3, padx=4)
        ttk.Button(controls, text="Close", command=modal.destroy).grid(row=3, column=5, rowspan=2, padx=4)

        columns = ("name", "code", "per", "pbr", "debt", "net_cash_ratio")
        tree = ttk.Treeview(modal, columns=columns, show="headings", height=16)
        for col, text, width in (
            ("name", "Name", 180),
            ("code", "Code", 80),
            ("per", "PER", 80),
            ("pbr", "PBR", 80),
            ("debt", debt_label, 110),
            ("net_cash_ratio", ncs_col_label, 140),
        ):
            tree.heading(col, text=text)
            tree.column(col, width=width, anchor="center")
        tree.pack(fill="both", expand=True, padx=8, pady=(4, 2))

        scrollbar = ttk.Scrollbar(tree, orient="vertical", command=tree.yview)
        tree.configure(yscroll=scrollbar.set)
        scrollbar.pack(side="right", fill="y")

        ttk.Label(modal, textvariable=scan_status_var, anchor="w").pack(fill="x", padx=8, pady=(0, 6))

        def start_scan():
            try:
                per_min = parse_float(per_min_var.get())
                per_max = parse_float(per_max_var.get())
                pbr_min = parse_float(pbr_min_var.get())
                pbr_max = parse_float(pbr_max_var.get())
                debt_min = parse_float(debt_min_var.get())
                debt_max = parse_float(debt_max_var.get())
                ib_debt_min = parse_float(ib_debt_min_var.get())
                ib_debt_max = parse_float(ib_debt_max_var.get())
                ncsr_min = parse_float(ncs_ratio_min_var.get())
                ncsr_max = parse_float(ncs_ratio_max_var.get())
            except Exception:
                scan_status_var.set("입력 파싱 오류")
                return

            for item in tree.get_children():
                tree.delete(item)
            scan_status_var.set("Preparing scan...")

            def worker():
                def set_scan_status(text: str):
                    try:
                        root.after(0, lambda: scan_status_var.set(text))
                    except Exception:
                        pass

                try:
                    if selected_country != "KR":
                        index = ensure_submissions_index(status_cb=set_scan_status)
                        entries = index.get("entries", []) if isinstance(index, dict) else []
                        targets = []
                        for meta in entries:
                            try:
                                if meta.get("is_foreign") or meta.get("is_fund"):
                                    continue
                                if not meta.get("has_companyfacts"):
                                    continue
                                ticker = (meta.get("primary_ticker") or "").strip().upper()
                                cik = meta.get("cik")
                                if not ticker or not cik:
                                    continue
                                targets.append(
                                    {
                                        "ticker": ticker,
                                        "cik": cik,
                                        "name": (meta.get("name") or ticker).strip(),
                                    }
                                )
                            except Exception:
                                continue

                        total = len(targets)
                        if not total:
                            set_scan_status("No US targets found (check submissions/companyfacts filters).")
                            return

                        set_scan_status(f"Loading fundamentals... 0/{total}")
                        candidates = []
                        processed = 0
                        matched = 0
                        last_error = None

                        for item in targets:
                            processed += 1
                            try:
                                facts = load_company_facts(item["cik"])
                                fundamentals = extract_edgar_scan_fundamentals(facts)
                                liabilities_ratio_val = fundamentals.get("liabilities_ratio_value")
                                ib_ratio_val = fundamentals.get("interest_bearing_debt_ratio_value")
                                if not (
                                    in_range(liabilities_ratio_val, debt_min, debt_max)
                                    and in_range(ib_ratio_val, ib_debt_min, ib_debt_max)
                                ):
                                    continue
                                fundamentals["ticker"] = item["ticker"]
                                fundamentals["cik"] = item["cik"]
                                fundamentals["name"] = item["name"]
                                candidates.append(fundamentals)
                            except Exception as exc:
                                last_error = str(exc)
                            if processed % 200 == 0 or processed == total:
                                set_scan_status(
                                    f"Loading fundamentals... {processed}/{total}, candidates {len(candidates)}"
                                )

                        total_quotes = len(candidates)
                        if not total_quotes:
                            set_scan_status("No candidates after fundamentals filters.")
                            return

                        set_scan_status(f"Fetching quotes... 0/{total_quotes}")
                        chunk_size = 100
                        processed_quotes = 0
                        use_yahoo = os.getenv("US_RANGE_SCAN_QUOTE_SOURCE", "").lower() != "stooq"
                        for offset in range(0, total_quotes, chunk_size):
                            chunk = candidates[offset : offset + chunk_size]
                            yahoo_symbols = [yahoo_symbol_for_ticker(c.get("ticker", "")) for c in chunk]
                            quotes = {}
                            if use_yahoo:
                                try:
                                    quotes = fetch_yahoo_quotes_batch(yahoo_symbols)
                                except Exception as exc:
                                    last_error = str(exc)
                                    if "HTTP 429" in last_error:
                                        use_yahoo = False
                                        set_scan_status("Yahoo rate-limited; switching to Stooq-only quotes...")
                                    quotes = {}

                            for cand in chunk:
                                processed_quotes += 1
                                try:
                                    ticker = cand.get("ticker", "")
                                    symbol = yahoo_symbol_for_ticker(ticker)
                                    quote = quotes.get(symbol)
                                    if not quote:
                                        quote = fetch_stooq_quote(ticker)

                                    price_val = quote.get("price")
                                    per_val = quote.get("per")
                                    pbr_val = quote.get("pbr")
                                    market_price = float(price_val) if price_val is not None else None

                                    shares = cand.get("shares")
                                    market_cap = market_price * shares if market_price is not None and shares else None
                                    net_income = cand.get("net_income")
                                    equity = cand.get("equity")

                                    if per_val is None and market_cap is not None and net_income not in (None, 0):
                                        try:
                                            per_val = market_cap / net_income
                                        except Exception:
                                            per_val = None
                                    if pbr_val is None and market_cap is not None and equity not in (None, 0):
                                        try:
                                            pbr_val = market_cap / equity
                                        except Exception:
                                            pbr_val = None

                                    net_cash_ps_val = cand.get("net_cash_per_share_value")
                                    net_cash_ratio_val = None
                                    net_cash_ratio_text = "N/A"
                                    if net_cash_ps_val is not None and market_price and market_price > 0:
                                        try:
                                            ratio = (net_cash_ps_val / market_price) * 100
                                            net_cash_ratio_val = ratio
                                            net_cash_ratio_text = f"{ratio:,.2f}%"
                                        except Exception:
                                            net_cash_ratio_val = None
                                            net_cash_ratio_text = "N/A"

                                    liabilities_ratio_val = cand.get("liabilities_ratio_value")
                                    ib_ratio_val = cand.get("interest_bearing_debt_ratio_value")
                                    if not (
                                        in_range(per_val, per_min, per_max)
                                        and in_range(pbr_val, pbr_min, pbr_max)
                                        and in_range(liabilities_ratio_val, debt_min, debt_max)
                                        and in_range(net_cash_ratio_val, ncsr_min, ncsr_max)
                                        and in_range(ib_ratio_val, ib_debt_min, ib_debt_max)
                                    ):
                                        continue

                                    matched += 1
                                    liabilities_ratio_text = (
                                        f"{liabilities_ratio_val:,.2f}" if liabilities_ratio_val is not None else "N/A"
                                    )
                                    values = (
                                        cand.get("name") or "N/A",
                                        ticker,
                                        clean_number(per_val) if per_val is not None else "N/A",
                                        clean_number(pbr_val) if pbr_val is not None else "N/A",
                                        liabilities_ratio_text,
                                        net_cash_ratio_text,
                                    )
                                    root.after(0, lambda vals=values: tree.insert("", "end", values=vals))
                                except Exception as exc:
                                    last_error = str(exc)

                            if processed_quotes % 200 == 0 or processed_quotes == total_quotes:
                                set_scan_status(
                                    f"Scanning... {processed_quotes}/{total_quotes}, matched {matched}"
                                )

                        if last_error:
                            set_scan_status(
                                f"완료: {matched}개 매치 / {total_quotes}개 처리 (마지막 오류: {last_error})"
                            )
                        else:
                            set_scan_status(f"완료: {matched}개 매치 / {total_quotes}개 처리")
                        return

                    app_key = os.getenv("KIS_APP_KEY")
                    app_secret = os.getenv("KIS_APP_SECRET")
                    base_url = os.getenv("KIS_BASE_URL")
                    if not app_key or not app_secret:
                        set_scan_status("KIS 키를 설정하세요.")
                        return

                    set_scan_status("KRX/DART 목록 불러오는 중...")
                    kis_client = KisClient(app_key, app_secret, base_url=base_url)
                    _, stock_map, code_to_name = load_dart_corp_map()
                    krx_codes = set(load_name_map().values())
                    targets = [(code, corp_code) for code, corp_code in stock_map.items() if code in krx_codes]
                    if not targets:
                        set_scan_status("대상 종목이 없습니다 (KRX 필터 이후 비어 있음)")
                        return

                    total = len(targets)
                    matched = 0
                    processed = 0
                    last_error = None

                    set_scan_status(f"Scanning... 0/{total}")
                    for stock_code, corp_code in targets:
                        processed += 1
                        try:
                            snapshot = kis_client.get_snapshot_with_financials(stock_code)
                            price_val = parse_amount(snapshot.price)
                            per_val = parse_float(snapshot.per)
                            pbr_val = parse_float(snapshot.pbr)
                            debt_val = parse_float(snapshot.debt_ratio)

                            dart_data = fetch_dart_financials(
                                corp_code,
                                fallback_listed_shares=snapshot.listed_shares,
                                market_price=price_val,
                            )
                            ncs_ratio_text = dart_data.get("net_cash_per_share_ratio", "N/A")
                            ncs_ratio_val = parse_float(ncs_ratio_text)
                            ib_de_ratio_val = dart_data.get("interest_bearing_debt_ratio_value")

                            if not (
                                in_range(per_val, per_min, per_max)
                                and in_range(pbr_val, pbr_min, pbr_max)
                                and in_range(debt_val, debt_min, debt_max)
                                and in_range(ncs_ratio_val, ncsr_min, ncsr_max)
                                and in_range(ib_de_ratio_val, ib_debt_min, ib_debt_max)
                            ):
                                continue

                            matched += 1
                            name = code_to_name.get(corp_code, snapshot.name or "N/A")
                            values = (
                                name,
                                stock_code,
                                snapshot.per,
                                snapshot.pbr,
                                snapshot.debt_ratio,
                                ncs_ratio_text,
                            )
                            root.after(0, lambda vals=values: tree.insert("", "end", values=vals))
                        except Exception as exc:
                            last_error = str(exc)
                        if processed % 10 == 0 or processed == total:
                            set_scan_status(f"Scanning... {processed}/{total}, matched {matched}")

                    if last_error:
                        set_scan_status(f"완료: {matched}개 매치 / {total}개 처리 (마지막 오류: {last_error})")
                    else:
                        set_scan_status(f"완료: {matched}개 매치 / {total}개 처리")
                except Exception as exc:
                    set_scan_status(f"오류: {exc}")

            threading.Thread(target=worker, daemon=True).start()

    def set_status(text: str):
        try:
            root.after(0, lambda: status_var.set(text))
        except Exception:
            pass

    def update_view(snapshot: PriceSnapshot, dart_data=None):
        debt_display = snapshot.debt_ratio
        ib_ratio_display = "-"
        try:
            if dart_data:
                ib_ratio = dart_data.get("interest_bearing_debt_ratio")
                if ib_ratio not in (None, "N/A", ""):
                    ib_ratio_display = f"{ib_ratio}%"
        except Exception:
            ib_ratio_display = "-"
        try:
            root.after(
                0,
                lambda: (
                    name_var.set(snapshot.name),
                    price_var.set(snapshot.price),
                    per_var.set(snapshot.per),
                    pbr_var.set(snapshot.pbr),
                    debt_var.set(debt_display),
                    ib_debt_var.set(ib_ratio_display),
                ),
            )
        except Exception:
            return
        try:
            if dart_data:
                summary = dart_data.get("summary", {}) if isinstance(dart_data, dict) else {}
                root.after(
                    0,
                    lambda: (
                        dart_year_var.set(dart_data.get("bsns_year", "-")),
                        dart_net_cash_ps_var.set(dart_data.get("net_cash_per_share", "N/A")),
                        dart_net_cash_ps_ratio_var.set(dart_data.get("net_cash_per_share_ratio", "N/A")),
                        dart_sales_var.set(summary.get("매출액", "N/A")),
                        dart_op_var.set(summary.get("영업이익", "N/A")),
                        dart_sales_growth_var.set(dart_data.get("sales_growth_5y", "N/A")),
                        dart_op_growth_var.set(dart_data.get("op_growth_5y", "N/A")),
                        dart_net_income_growth_var.set(dart_data.get("net_income_growth_5y", "N/A")),
                        dart_equity_var.set(summary.get("자본총계", "N/A")),
                    ),
                )
            else:
                root.after(
                    0,
                    lambda: (
                        dart_year_var.set("-"),
                        dart_net_cash_ps_var.set("-"),
                        dart_net_cash_ps_ratio_var.set("-"),
                        dart_sales_var.set("-"),
                        dart_op_var.set("-"),
                        dart_sales_growth_var.set("-"),
                        dart_op_growth_var.set("-"),
                        dart_net_income_growth_var.set("-"),
                        dart_equity_var.set("-"),
                    ),
                )
        except Exception:
            pass

    def do_fetch():
        user_input = input_var.get().strip()
        selected_country = country_var.get()

        if not user_input:
            messagebox.showerror("Input error", "Enter a ticker or company name.")
            return

        if selected_country == "KR":
            try:
                code = resolve_code(user_input)
            except KisError as exc:
                messagebox.showerror("Name lookup failed", str(exc))
                return

            if not code:
                messagebox.showerror("Input error", "Enter a valid company name or 6-digit code.")
                return

            app_key = os.getenv("KIS_APP_KEY")
            app_secret = os.getenv("KIS_APP_SECRET")
            base_url = os.getenv("KIS_BASE_URL")
            kis_enabled = bool(app_key and app_secret)
            client = KisClient(app_key, app_secret, base_url=base_url) if kis_enabled else None

            def worker():
                set_status("Fetching (KR)...")
                dart_data = None
                dart_error = None
                snapshot = PriceSnapshot(name="N/A", code=code, price="N/A", per="N/A", pbr="N/A")
                try:
                    if kis_enabled and client:
                        snapshot = client.get_snapshot_with_financials(code)
                except Exception as exc:  # broad catch to show UI errors
                    set_status("KIS 실패, DART만 표시")
                    snapshot = PriceSnapshot(name="N/A", code=code, price="N/A", per="N/A", pbr="N/A")

                market_price = parse_amount(snapshot.price)

                try:
                    dart_data = fetch_dart_financials(
                        user_input,
                        fallback_listed_shares=snapshot.listed_shares,
                        market_price=market_price,
                    )
                    if dart_data and dart_data.get("corp_name"):
                        snapshot.name = dart_data.get("corp_name")
                    if dart_data and dart_data.get("corp_code"):
                        snapshot.code = dart_data.get("corp_code")
                except Exception as exc:
                    dart_error = str(exc)

                update_view(snapshot, dart_data)
                if dart_error:
                    set_status(f"DART 실패: {dart_error}")
                else:
                    set_status("Done")

            threading.Thread(target=worker, daemon=True).start()
            return

        def worker():
            set_status("Fetching (US)...")
            try:
                snapshot, detail = fetch_edgar_financials(user_input)
            except Exception as exc:
                messagebox.showerror("EDGAR lookup failed", str(exc))
                update_view(PriceSnapshot(name="N/A", code=user_input or "-", price="N/A", per="N/A", pbr="N/A"), None)
                set_status(f"EDGAR 실패: {exc}")
                return

            update_view(snapshot, detail)
            set_status("Done")

        threading.Thread(target=worker, daemon=True).start()

    ttk.Label(root, text="Company name or code").grid(row=0, column=0, sticky="w")
    ttk.Label(root, text="Country").grid(row=0, column=1, sticky="e")
    country_combo = ttk.Combobox(root, textvariable=country_var, values=("US", "KR"), state="readonly", width=8)
    country_combo.grid(row=0, column=2, sticky="ew")
    entry = ttk.Entry(root, textvariable=input_var)
    entry.grid(row=1, column=0, sticky="ew", padx=(0, 8))
    entry.focus()
    ttk.Button(root, text="Lookup", command=do_fetch).grid(row=1, column=1, sticky="ew")
    scan_button = ttk.Button(root, text="Range Scan", command=open_scan_modal)
    scan_button.grid(row=1, column=2, sticky="ew", padx=(8, 0))

    def update_controls_for_country(event=None):
        if country_var.get() == "KR":
            scan_button.state(["!disabled"])
            debt_label_var.set("부채비율(KIS)")
            ib_debt_label_var.set("Interest-bearing debt/Equity (DART, %)")
        else:
            scan_button.state(["!disabled"])
            debt_label_var.set("Liabilities/Equity (EDGAR, %)")
            ib_debt_label_var.set("Interest-bearing debt/Equity (EDGAR, %)")

    country_combo.bind("<<ComboboxSelected>>", update_controls_for_country)
    update_controls_for_country()

    root.grid_columnconfigure(0, weight=1)
    root.grid_columnconfigure(1, weight=0)
    root.grid_columnconfigure(2, weight=0)

    info_frame = ttk.Frame(root)
    info_frame.grid(row=2, column=0, columnspan=2, pady=(12, 8), sticky="ew")
    info_frame.grid_columnconfigure(1, weight=1)

    def add_row(label_text: str, var: tk.StringVar, row_idx: int):
        ttk.Label(info_frame, text=label_text, width=12).grid(row=row_idx, column=0, sticky="w", pady=2)
        ttk.Label(info_frame, textvariable=var, width=32).grid(row=row_idx, column=1, sticky="w", pady=2)

    add_row("Name", name_var, 0)
    add_row("Price", price_var, 1)
    add_row("PER", per_var, 2)
    add_row("PBR", pbr_var, 3)
    ttk.Label(info_frame, textvariable=debt_label_var, width=12).grid(row=4, column=0, sticky="w", pady=2)
    ttk.Label(info_frame, textvariable=debt_var, width=32).grid(row=4, column=1, sticky="w", pady=2)
    ttk.Label(info_frame, textvariable=ib_debt_label_var, width=12).grid(row=5, column=0, sticky="w", pady=2)
    ttk.Label(info_frame, textvariable=ib_debt_var, width=32).grid(row=5, column=1, sticky="w", pady=2)
    add_row("사업연도(DART)", dart_year_var, 6)
    add_row("주당 순현금", dart_net_cash_ps_var, 7)
    add_row("주당 순현금/주가", dart_net_cash_ps_ratio_var, 8)
    add_row("매출액", dart_sales_var, 9)
    add_row("영업이익", dart_op_var, 10)
    add_row("매출성장률(5Y)", dart_sales_growth_var, 11)
    add_row("영업이익성장률(5Y)", dart_op_growth_var, 12)
    add_row("당기순이익성장률(5Y)", dart_net_income_growth_var, 13)
    add_row("자본총계", dart_equity_var, 14)

    status_bar = ttk.Label(root, textvariable=status_var, anchor="w", relief="sunken")
    status_bar.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))

    root.mainloop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Korea Investment PER/PBR viewer")
    parser.add_argument("--cli", action="store_true", help="Run in CLI mode")
    parser.add_argument("--dart", action="store_true", help="Run DART financial summary lookup (CLI)")
    parser.add_argument("--dart-year", dest="dart_year", help="Business year (YYYY) for DART lookup")
    parser.add_argument("--symbol", help="Symbol or code to use in CLI mode")
    args = parser.parse_args()

    if args.dart:
        sys.exit(run_dart_cli(args.symbol, args.dart_year))

    can_gui, reason = gui_supported()
    if args.cli or not can_gui:
        if reason and not args.cli:
            print(reason)
        sys.exit(run_cli(args.symbol))

    try:
        build_gui()
    except Exception as exc:
        print(f"GUI failed: {exc}\nFalling back to --cli mode.")
        sys.exit(run_cli(args.symbol))
