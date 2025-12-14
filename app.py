import argparse
import io
import os
import platform
import re
import sys
import threading
import time
import zipfile
import datetime
from dataclasses import dataclass
from functools import lru_cache
from html import unescape
from typing import Dict, Optional, Tuple

import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv

# Load .env so users can keep keys out of the code.
load_dotenv(dotenv_path=".env")

KRX_LISTING_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download"
DART_CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DART_MULTI_ACNT_URL = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
DART_STOCK_TOT_URL = "https://opendart.fss.or.kr/api/stockTotqySttus.json"
DART_SINGLE_ACNT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"

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


def format_amount(value) -> str:
    amount = parse_amount(value)
    if amount is None:
        return "N/A"
    return f"{amount:,}"


def format_per_share(cash_value: Optional[int], shares: Optional[int]) -> str:
    if cash_value is None or shares is None or shares <= 0:
        return "N/A"
    return f"{cash_value / shares:,.2f}"


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


def _parse_int(value) -> Optional[int]:
    try:
        return int(float(str(value).replace(",", "")))
    except Exception:
        return None


def _sum_or_none(values) -> Optional[int]:
    filtered = [v for v in values if v is not None]
    return sum(filtered) if filtered else None


def compute_net_cash(liquid_funds: Optional[int], interest_bearing_debt: Optional[int]):
    """Return (net_cash, debt_value) applying debt=0 fallback when liquid_funds exists."""
    if liquid_funds is None:
        return None, interest_bearing_debt
    debt_value = interest_bearing_debt if interest_bearing_debt is not None else 0
    return liquid_funds - debt_value, debt_value


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
) -> Dict[str, str]:
    """Fetch DART financials prioritizing the most recent available report.

    If reprt_code is omitted, it tries quarters/half/3Q/business report by release
    recency. Passing reprt_code forces that report type.
    """
    corp_code, corp_name = resolve_dart_corp(user_text)
    now_year = time.localtime().tm_year
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

        net_cash, debt_value = compute_net_cash(liquid_funds, interest_bearing_debt)

        float_shares = None
        try:
            float_shares = fetch_dart_stock_totals(corp_code, year, report_code)
        except Exception:
            float_shares = None

        used_kis_fallback = False
        if float_shares is None and fallback_listed_shares and fallback_listed_shares > 0:
            float_shares = fallback_listed_shares
            used_kis_fallback = True

        net_cash_per_share = format_per_share(net_cash, float_shares)
        if used_kis_fallback and net_cash_per_share != "N/A":
            net_cash_per_share = f"{net_cash_per_share} (KIS 상장주식수)"

        net_cash_display = format_amount(net_cash) if net_cash is not None else "N/A"
        float_shares_display = format_amount(float_shares) if float_shares is not None else None

        return {
            "corp_name": corp_name,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": report_code,
            "summary": summary,
            "cash_equivalents": format_amount(cash_equivalents) if cash_equivalents is not None else "N/A",
            "liquid_funds": liquid_funds,
            "interest_bearing_debt": debt_value,
            "net_cash": net_cash,
            "net_cash_display": net_cash_display,
            "float_shares": float_shares,
            "float_shares_display": float_shares_display,
            "net_cash_per_share": net_cash_per_share,
        }

    raise DartError(last_error or "조회 가능한 연도가 없습니다.")



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

        return PriceSnapshot(
            name=name or "N/A",
            code=stock_code,
            price=clean_number(price),
            per=per if per != "" else "N/A",
            pbr=pbr if pbr != "" else "N/A",
            listed_shares=listed_shares,
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
    except Exception:
        fallback_listed_shares = None

    try:
        result = fetch_dart_financials(
            user_input,
            bsns_year=year,
            fallback_listed_shares=fallback_listed_shares,
        )
    except DartError as exc:
        print(f"DART 조회 실패: {exc}", file=sys.stderr)
        return 1

    corp_name = result.get("corp_name", "-")
    corp_code = result.get("corp_code", "-")
    bsns_year = result.get("bsns_year", "-")
    summary = result.get("summary", {})
    ncs = result.get("net_cash_per_share", "N/A")
    net_cash_display = result.get("net_cash_display", "N/A")

    print(f"{corp_name} ({corp_code}) - 사업연도 {bsns_year}")
    print(f"주당 순현금: {ncs}")
    print(f"순현금(총액): {net_cash_display}")
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
    root.title("KIS + DART Viewer")
    root.geometry("540x430")
    root.resizable(False, False)

    root.configure(padx=14, pady=12, bg="#f7f7f7")
    style = ttk.Style()
    style.theme_use("clam")
    for widget in ("TLabel", "TButton", "TEntry"):
        style.configure(widget, font=("Segoe UI", 10))
    style.configure("TButton", padding=6)

    input_var = tk.StringVar()
    status_var = tk.StringVar(value="Set KIS_APP_KEY and KIS_APP_SECRET in .env, then enter a company name.")
    name_var = tk.StringVar(value="-")
    code_var = tk.StringVar(value="-")
    price_var = tk.StringVar(value="-")
    per_var = tk.StringVar(value="-")
    pbr_var = tk.StringVar(value="-")
    cash_var = tk.StringVar(value="-")
    debt_var = tk.StringVar(value="-")
    dart_year_var = tk.StringVar(value="-")
    dart_net_cash_ps_var = tk.StringVar(value="-")
    dart_net_cash_var = tk.StringVar(value="-")
    dart_sales_var = tk.StringVar(value="-")
    dart_op_var = tk.StringVar(value="-")
    dart_net_var = tk.StringVar(value="-")
    dart_asset_var = tk.StringVar(value="-")
    dart_debt_var = tk.StringVar(value="-")
    dart_equity_var = tk.StringVar(value="-")

    def set_status(text: str):
        try:
            root.after(0, lambda: status_var.set(text))
        except Exception:
            pass

    def update_view(snapshot: PriceSnapshot, dart_data=None):
        try:
            root.after(
                0,
                lambda: (
                    name_var.set(snapshot.name),
                    code_var.set(snapshot.code),
                    price_var.set(snapshot.price),
                    per_var.set(snapshot.per),
                    pbr_var.set(snapshot.pbr),
                    debt_var.set(snapshot.debt_ratio),
                ),
            )
        except Exception:
            return
        try:
            if dart_data:
                summary = dart_data.get("summary", {}) if isinstance(dart_data, dict) else {}
                liquid_display = format_amount(dart_data.get("liquid_funds"))
                root.after(
                    0,
                    lambda: (
                        dart_year_var.set(dart_data.get("bsns_year", "-")),
                        cash_var.set(liquid_display),
                        dart_net_cash_ps_var.set(dart_data.get("net_cash_per_share", "N/A")),
                        dart_net_cash_var.set(dart_data.get("net_cash_display", "N/A")),
                        dart_sales_var.set(summary.get("매출액", "N/A")),
                        dart_op_var.set(summary.get("영업이익", "N/A")),
                        dart_net_var.set(summary.get("당기순이익", "N/A")),
                        dart_asset_var.set(summary.get("자산총계", "N/A")),
                        dart_debt_var.set(summary.get("부채총계", "N/A")),
                        dart_equity_var.set(summary.get("자본총계", "N/A")),
                    ),
                )
            else:
                root.after(
                    0,
                    lambda: (
                        dart_year_var.set("-"),
                        cash_var.set("-"),
                        dart_net_cash_ps_var.set("-"),
                        dart_net_cash_var.set("-"),
                        dart_sales_var.set("-"),
                        dart_op_var.set("-"),
                        dart_net_var.set("-"),
                        dart_asset_var.set("-"),
                        dart_debt_var.set("-"),
                        dart_equity_var.set("-"),
                    ),
                )
        except Exception:
            pass

    def do_fetch():
        user_input = input_var.get()
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
            set_status("Fetching...")
            dart_data = None
            dart_error = None
            snapshot = PriceSnapshot(name="N/A", code=code, price="N/A", per="N/A", pbr="N/A")
            try:
                if kis_enabled and client:
                    snapshot = client.get_snapshot_with_financials(code)
            except Exception as exc:  # broad catch to show UI errors
                set_status("KIS 실패, DART만 표시")
                snapshot = PriceSnapshot(name="N/A", code=code, price="N/A", per="N/A", pbr="N/A")

            try:
                dart_data = fetch_dart_financials(user_input, fallback_listed_shares=snapshot.listed_shares)
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

    ttk.Label(root, text="Company name or code").grid(row=0, column=0, sticky="w")
    entry = ttk.Entry(root, textvariable=input_var)
    entry.grid(row=1, column=0, sticky="ew", padx=(0, 8))
    entry.focus()
    ttk.Button(root, text="Lookup", command=do_fetch).grid(row=1, column=1, sticky="ew")

    root.grid_columnconfigure(0, weight=1)

    info_frame = ttk.Frame(root)
    info_frame.grid(row=2, column=0, columnspan=2, pady=(12, 8), sticky="ew")
    info_frame.grid_columnconfigure(1, weight=1)

    def add_row(label_text: str, var: tk.StringVar, row_idx: int):
        ttk.Label(info_frame, text=label_text, width=12).grid(row=row_idx, column=0, sticky="w", pady=2)
        ttk.Label(info_frame, textvariable=var, width=32).grid(row=row_idx, column=1, sticky="w", pady=2)

    add_row("Name", name_var, 0)
    add_row("Code", code_var, 1)
    add_row("Price", price_var, 2)
    add_row("PER", per_var, 3)
    add_row("PBR", pbr_var, 4)
    add_row("현금성자산(DART)", cash_var, 5)
    add_row("Debt ratio", debt_var, 6)
    add_row("사업연도(DART)", dart_year_var, 7)
    add_row("주당 순현금", dart_net_cash_ps_var, 8)
    add_row("순현금", dart_net_cash_var, 9)
    add_row("매출액", dart_sales_var, 10)
    add_row("영업이익", dart_op_var, 11)
    add_row("당기순이익", dart_net_var, 12)
    add_row("자산총계", dart_asset_var, 13)
    add_row("부채총계", dart_debt_var, 14)
    add_row("자본총계", dart_equity_var, 15)

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
