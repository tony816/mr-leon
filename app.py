import argparse
import io
import os
import platform
import re
import sys
import threading
import time
import zipfile
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
    "현금및현금성자산": {
        "현금및현금성자산",
        "현금및현금성자산및예치금",
        "현금및현금성자산(유동)",
        "현금및현금성자산(비유동)",
        "현금및현금성자산및단기금융상품",
        "cashandcashequivalents",
        "cash_and_cash_equivalents",
    },
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


def fetch_dart_cash_equivalents(corp_code: str, bsns_year: str, reprt_code: str) -> Optional[int]:
    """Fetch cash & cash equivalents from DART single-account-all API."""
    params = {
        "crtfc_key": get_dart_key(),
        "corp_code": corp_code,
        "bsns_year": bsns_year,
        "reprt_code": reprt_code,
        "fs_div": "CFS",
    }
    resp = requests.get(DART_SINGLE_ACNT_URL, params=params, timeout=15)
    if resp.status_code != 200:
        raise DartError(f"현금및현금성자산 조회 실패: HTTP {resp.status_code}")
    payload = resp.json()
    if payload.get("status") != "000":
        raise DartError(f"현금및현금성자산 조회 오류: {payload.get('status')} {payload.get('message', '')}".strip())

    entries = payload.get("list") or []
    for row in entries:
        account_nm = (row.get("account_nm") or "").strip()
        if not account_nm:
            continue
        label = ACCOUNT_ALIAS_MAP.get(normalize_name(account_nm))
        if label == "현금및현금성자산":
            return parse_amount(row.get("thstrm_amount") or row.get("thstrm_add_amount"))
    return None


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
    if not entries:
        return None
    entry = entries[0] or {}

    def _parse(val):
        try:
            return int(float(str(val).replace(",", "")))
        except Exception:
            return None

    issued = _parse(entry.get("now_to_isu_stock_totqy"))
    decreased = _parse(entry.get("now_to_dcrs_stock_totqy"))
    istc_totqy = _parse(entry.get("istc_totqy"))

    if issued is not None and decreased is not None:
        shares = issued - decreased
        if shares > 0:
            return shares
    if istc_totqy is not None and istc_totqy > 0:
        return istc_totqy
    return None


def fetch_dart_financials(user_text: str, bsns_year: Optional[str] = None, reprt_code: str = "11011") -> Dict[str, str]:
    corp_code, corp_name = resolve_dart_corp(user_text)
    now_year = time.localtime().tm_year
    years_to_try = []
    if bsns_year:
        years_to_try.append(str(bsns_year))
    else:
        years_to_try.extend([str(now_year), str(now_year - 1), str(now_year - 2), str(now_year - 3)])

    last_error = None
    for year in years_to_try:
        params = {
            "crtfc_key": get_dart_key(),
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": reprt_code,
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
        summary = summarize_accounts(entries)
        cash_value = parse_amount(summary.get("현금및현금성자산"))
        if cash_value is None:
            try:
                cash_value = fetch_dart_cash_equivalents(corp_code, year, reprt_code)
            except Exception:
                cash_value = None
            if cash_value is not None:
                summary["현금및현금성자산"] = format_amount(cash_value)
        shares = None
        cash_per_share = None
        try:
            shares = fetch_dart_stock_totals(corp_code, year, reprt_code)
            cash_per_share = format_per_share(cash_value, shares)
        except Exception:
            pass
        return {
            "corp_name": corp_name,
            "corp_code": corp_code,
            "bsns_year": year,
            "reprt_code": reprt_code,
            "summary": summary,
            "cash_equivalents": format_amount(cash_value) if cash_value is not None else "N/A",
            "shares_outstanding": shares,
            "cash_per_share": cash_per_share or "N/A",
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

        return PriceSnapshot(
            name=name or "N/A",
            code=stock_code,
            price=clean_number(price),
            per=per if per != "" else "N/A",
            pbr=pbr if pbr != "" else "N/A",
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
    try:
        result = fetch_dart_financials(user_input, bsns_year=year)
    except DartError as exc:
        print(f"DART 조회 실패: {exc}", file=sys.stderr)
        return 1

    corp_name = result.get("corp_name", "-")
    corp_code = result.get("corp_code", "-")
    bsns_year = result.get("bsns_year", "-")
    summary = result.get("summary", {})
    cps = result.get("cash_per_share", "N/A")

    print(f"{corp_name} ({corp_code}) - 사업연도 {bsns_year}")
    print(f"1주당 현금: {cps}")
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
    root.geometry("520x380")
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
    dart_sales_var = tk.StringVar(value="-")
    dart_op_var = tk.StringVar(value="-")
    dart_net_var = tk.StringVar(value="-")
    dart_asset_var = tk.StringVar(value="-")
    dart_debt_var = tk.StringVar(value="-")
    dart_equity_var = tk.StringVar(value="-")

    def set_status(text: str):
        root.after(0, lambda: status_var.set(text))

    def update_view(snapshot: PriceSnapshot, dart_data=None):
        root.after(
            0,
            lambda: (
                name_var.set(snapshot.name),
                code_var.set(snapshot.code),
                price_var.set(snapshot.price),
                per_var.set(snapshot.per),
                pbr_var.set(snapshot.pbr),
                cash_var.set(snapshot.cash),
                debt_var.set(snapshot.debt_ratio),
            ),
        )
        if dart_data:
            summary = dart_data.get("summary", {}) if isinstance(dart_data, dict) else {}
            root.after(
                0,
                lambda: (
                    dart_year_var.set(dart_data.get("bsns_year", "-")),
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
                    dart_sales_var.set("-"),
                    dart_op_var.set("-"),
                    dart_net_var.set("-"),
                    dart_asset_var.set("-"),
                    dart_debt_var.set("-"),
                    dart_equity_var.set("-"),
                ),
            )

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
        if not app_key or not app_secret:
            messagebox.showerror("Missing keys", "Set KIS_APP_KEY and KIS_APP_SECRET in your environment or .env file.")
            return

        client = KisClient(app_key, app_secret, base_url=base_url)

        def worker():
            set_status("Fetching...")
            dart_data = None
            dart_error = None
            try:
                snapshot = client.get_snapshot_with_financials(code)
                try:
                    dart_data = fetch_dart_financials(user_input)
                    if dart_data and dart_data.get("cash_per_share"):
                        snapshot.cash = dart_data.get("cash_per_share")
                except Exception as exc:
                    dart_error = str(exc)
            except Exception as exc:  # broad catch to show UI errors
                set_status("Error")
                messagebox.showerror("Lookup failed", str(exc))
                return

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
    add_row("1주당 현금", cash_var, 5)
    add_row("Debt ratio", debt_var, 6)
    add_row("사업연도(DART)", dart_year_var, 7)
    add_row("매출액", dart_sales_var, 8)
    add_row("영업이익", dart_op_var, 9)
    add_row("당기순이익", dart_net_var, 10)
    add_row("자산총계", dart_asset_var, 11)
    add_row("부채총계", dart_debt_var, 12)
    add_row("자본총계", dart_equity_var, 13)

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
