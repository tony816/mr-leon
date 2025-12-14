# KIS + OpenDART Viewer

한국투자 OpenAPI로 현재가/PER/PBR을, OpenDART로 재무계정과 주당 순현금을 조회하는 Tkinter GUI/CLI입니다.

## 준비물
- Python 3.10+
- 한국투자 OpenAPI 앱키/시크릿 (`KIS_APP_KEY`, `KIS_APP_SECRET`)
- OpenDART 인증키 (`DART_KEY`)

`.env` 예시:
```
KIS_APP_KEY=your_app_key
KIS_APP_SECRET=your_app_secret
# KIS_BASE_URL=https://openapi.koreainvestment.com:9443  # 실거래시
DART_KEY=your_dart_key
```

## 설치와 실행
```
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py          # GUI
python app.py --dart   # CLI DART 모드
```

## 주당 순현금 정의
- **주당 순현금 = (순현금) ÷ (유통주식수(Ⅵ))**
- **순현금(Net Cash) = 유동자금 − 차입금(사채 포함)**
  - 유동자금 = 현금및현금성자산 + 단기금융상품 + (있으면) 단기상각후원가금융자산 + (있으면) 단기당기손익-공정가치금융자산
  - 차입금(사채 포함) = 단기차입금 + 유동성장기차입금 + 유동성사채 + 장기차입금 + 사채  
    (DART에 `유동성장기부채`로 합산돼 있으면 그 값을 사용하고, 없으면 유동성장기차입금+유동성사채를 합산)
- 분모는 OpenDART `stockTotqySttus`의 `distb_stock_co`(Ⅵ. 유통주식수, 보통주 우선)를 사용한다. 값이 없으면  
  `유통주식수(Ⅵ) = (now_to_isu_stock_totqy − now_to_dcrs_stock_totqy) − tesstk_co` 로 대체 산출한다.
- DART 유통주식수가 비어 있을 때는 KIS 시세 API의 상장주식수(`lstn_stcn`)를 분모로 사용하며, 이 경우 주당 순현금 값 뒤에 `(KIS 상장주식수)`를 표기한다.

## 보고서 선택 기준
- DART 조회는 자동 모드에서 가장 최근 공시된 보고서를 우선 사용합니다: 3분기 → 2분기/반기 → 1분기 → 사업보고서 순으로, 최신 공시일 역순으로 내려갑니다. 공시되지 않은 분기는 건너뛰며, 사업보고서도 최신 공시 대상에 포함됩니다.

## 삼성전자 2024 예시
- 사업보고서 Net Cash(유동자금-차입금) = **93,321,606 (백만원)** → 코드에서는 **93,321,606,000,000원**
- 유통주식수(Ⅵ, 보통주) = **5,940,082,550주**
- 주당 순현금 ≈ **15,710.49원**

## 사용법
- 입력창에 회사명(한글 자연어)이나 6자리 종목코드를 입력 후 조회
- 출력: 현재가, PER, PBR, **주당 순현금**, **순현금(총액)**, DART 재무 요약(매출/영업이익/당기순이익/자산·부채·자본총계)

## 테스트
```
source .venv/bin/activate
python -m unittest
```
