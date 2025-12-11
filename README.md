# 한국투자 PER/PBR 뷰어

`한국투자` OpenAPI(코딩도우미 MCP)로 종목 현재가와 PER/PBR을 조회하는 간단한 Tkinter GUI입니다.

## 준비물
- Python 3.10+
- 한국투자 OpenAPI 앱키/시크릿

루트에 `.env` 파일을 만들어 키를 넣어주세요:
```
KIS_APP_KEY=your_app_key
KIS_APP_SECRET=your_app_secret
# 실거래로 바꾸려면 BASE_URL만 교체
# KIS_BASE_URL=https://openapi.koreainvestment.com:9443
```

## 설치와 실행
```
python -m venv .venv
.\\.venv\\Scripts\\activate
pip install -r requirements.txt
python app.py
```

## 사용법
- 상단 입력창에 종목코드(6자리) 또는 사전 등록된 이름(예: 삼성전자, 네이버, 카카오 등)을 입력 후 **조회**.
- 현재가, PER, PBR이 하단에 표시됩니다.
- 이름→코드 매핑은 `app.py`의 `NAME_TO_CODE` dict를 원하는 종목으로 늘리면 됩니다.
