import argparse
import os
import platform
import re
import sys
import threading
import time
from dataclasses import dataclass
from functools import lru_cache
from html import unescape
from typing import Dict, Optional, Tuple

import requests
from dotenv import load_dotenv

# Load .env so users can keep keys out of the code.
load_dotenv()

KRX_LISTING_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download"


class KisError(Exception):
    """Raised when the Korea Investment API returns an error."""


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
    root.title("KIS PER/PBR Viewer")
    root.geometry("460x260")
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

    def set_status(text: str):
        root.after(0, lambda: status_var.set(text))

    def update_view(snapshot: PriceSnapshot):
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
            try:
                snapshot = client.get_snapshot_with_financials(code)
            except Exception as exc:  # broad catch to show UI errors
                set_status("Error")
                messagebox.showerror("Lookup failed", str(exc))
                return

            update_view(snapshot)
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
    add_row("Cash", cash_var, 5)
    add_row("Debt ratio", debt_var, 6)

    status_bar = ttk.Label(root, textvariable=status_var, anchor="w", relief="sunken")
    status_bar.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))

    root.mainloop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Korea Investment PER/PBR viewer")
    parser.add_argument("--cli", action="store_true", help="Run in CLI mode")
    parser.add_argument("--symbol", help="Symbol or code to use in CLI mode")
    args = parser.parse_args()

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
