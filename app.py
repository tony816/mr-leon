import os
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional

import requests
from dotenv import load_dotenv

# Load .env if present so users can keep keys out of the code.
load_dotenv()


# Simple in-memory mapping for quick name-based lookups.
# Extend this list to include the symbols you care about.
NAME_TO_CODE: Dict[str, str] = {
    "삼성전자": "005930",
    "sk하이닉스": "000660",
    "에스케이하이닉스": "000660",
    "네이버": "035420",
    "카카오": "035720",
    "현대차": "005380",
    "기아": "000270",
    "lg에너지솔루션": "373220",
    "posco홀딩스": "005490",
}


class KisError(Exception):
    """Raised when the 한국투자 API returns an error."""


@dataclass
class PriceSnapshot:
    name: str
    code: str
    price: str
    per: str
    pbr: str


class KisClient:
    """Minimal client for 한국투자 OpenAPI to get price/PER/PBR."""

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
            raise KisError(f"토큰 요청 실패: HTTP {resp.status_code} {resp.text}")

        data = resp.json()
        access_token = data.get("access_token")
        expires_in = data.get("expires_in", 0)
        if not access_token:
            raise KisError(f"토큰 응답에 access_token 없음: {data}")

        self._token = access_token
        self._token_expiry = now + int(expires_in or 0)
        return access_token

    def get_price_snapshot(self, stock_code: str) -> PriceSnapshot:
        token = self._ensure_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST01010100",  # 현재가 조회용 TR
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",  # 주식
            "FID_INPUT_ISCD": stock_code,
        }
        resp = self.session.get(self._price_url(), headers=headers, params=params, timeout=10)
        if resp.status_code != 200:
            raise KisError(f"시세 조회 실패: HTTP {resp.status_code} {resp.text}")

        data = resp.json()
        output = data.get("output", {}) if isinstance(data, dict) else {}
        if not output:
            raise KisError(f"시세 응답 포맷 오류: {data}")

        # Defensive parsing with fallbacks.
        name = output.get("hts_kor_isnm", "").strip() or output.get("prdt_name", "")
        price = output.get("stck_prpr", "")
        per = output.get("per", "")
        pbr = output.get("pbr", "")

        def clean_number(val: str) -> str:
            try:
                return f"{float(val):,}"
            except (ValueError, TypeError):
                return str(val)

        return PriceSnapshot(
            name=name or "N/A",
            code=stock_code,
            price=clean_number(price),
            per=per if per != "" else "N/A",
            pbr=pbr if pbr != "" else "N/A",
        )


def resolve_code(user_text: str) -> Optional[str]:
    if not user_text:
        return None
    trimmed = user_text.strip()
    digits = "".join(ch for ch in trimmed if ch.isdigit())
    if len(digits) >= 6:
        return digits[:6]

    lowered = trimmed.lower().replace(" ", "")
    return NAME_TO_CODE.get(lowered)


def build_gui():
    import tkinter as tk
    from tkinter import ttk, messagebox

    root = tk.Tk()
    root.title("한국투자 PER/PBR 뷰어")
    root.geometry("420x240")
    root.resizable(False, False)

    # Theme basics
    root.configure(padx=14, pady=12, bg="#f7f7f7")
    style = ttk.Style()
    style.theme_use("clam")

    for widget in ("TLabel", "TButton", "TEntry"):
        style.configure(widget, font=("Segoe UI", 10))
    style.configure("TButton", padding=6)

    input_var = tk.StringVar()
    status_var = tk.StringVar(value="API 키를 .env에 설정하세요.")
    name_var = tk.StringVar(value="-")
    code_var = tk.StringVar(value="-")
    price_var = tk.StringVar(value="-")
    per_var = tk.StringVar(value="-")
    pbr_var = tk.StringVar(value="-")

    def set_status(text: str):
        root.after(0, lambda: status_var.set(text))

    def update_view(snapshot: PriceSnapshot):
        root.after(0, lambda: (
            name_var.set(snapshot.name),
            code_var.set(snapshot.code),
            price_var.set(snapshot.price),
            per_var.set(snapshot.per),
            pbr_var.set(snapshot.pbr),
        ))

    def do_fetch():
        user_input = input_var.get()
        code = resolve_code(user_input)
        if not code:
            messagebox.showerror("입력 오류", "종목코드 6자리 또는 사전 등록된 이름을 입력하세요.")
            return

        app_key = os.getenv("KIS_APP_KEY")
        app_secret = os.getenv("KIS_APP_SECRET")
        base_url = os.getenv("KIS_BASE_URL")
        if not app_key or not app_secret:
            messagebox.showerror("설정 필요", ".env 파일 또는 환경변수에 KIS_APP_KEY, KIS_APP_SECRET을 설정하세요.")
            return

        client = KisClient(app_key, app_secret, base_url=base_url)

        def worker():
            set_status("조회 중...")
            try:
                snapshot = client.get_price_snapshot(code)
            except Exception as exc:  # broad catch to show UI errors
                set_status("오류 발생")
                messagebox.showerror("조회 실패", str(exc))
                return

            update_view(snapshot)
            set_status("완료")

        threading.Thread(target=worker, daemon=True).start()

    # Layout
    ttk.Label(root, text="종목 검색 (이름 또는 코드)").grid(row=0, column=0, sticky="w")
    entry = ttk.Entry(root, textvariable=input_var)
    entry.grid(row=1, column=0, sticky="ew", padx=(0, 8))
    entry.focus()
    ttk.Button(root, text="조회", command=do_fetch).grid(row=1, column=1, sticky="ew")

    root.grid_columnconfigure(0, weight=1)

    info_frame = ttk.Frame(root)
    info_frame.grid(row=2, column=0, columnspan=2, pady=(12, 8), sticky="ew")
    info_frame.grid_columnconfigure(1, weight=1)

    def add_row(label_text: str, var: tk.StringVar, row_idx: int):
        ttk.Label(info_frame, text=label_text, width=10).grid(row=row_idx, column=0, sticky="w", pady=2)
        ttk.Label(info_frame, textvariable=var, width=30).grid(row=row_idx, column=1, sticky="w", pady=2)

    add_row("이름", name_var, 0)
    add_row("코드", code_var, 1)
    add_row("현재가", price_var, 2)
    add_row("PER", per_var, 3)
    add_row("PBR", pbr_var, 4)

    status_bar = ttk.Label(root, textvariable=status_var, anchor="w", relief="sunken")
    status_bar.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(8, 0))

    root.mainloop()


if __name__ == "__main__":
    build_gui()
