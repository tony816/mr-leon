# Country DB Build Insights

Last updated: 2026-05-19

이 문서는 UK fundamentals cache 문제를 해결하면서 얻은 인사이트를 다른 국가 DB 구축에도 재사용하기 위한 메모다. 핵심은 "전체 상장 universe", "공식 재무 소스", "보조/대체 소스", "스캔 가능한 최종 캐시"를 분리해서 설계하는 것이다.

## 1. Universe 정의를 먼저 고정한다

국가별 DB에서 가장 먼저 결정해야 할 것은 "무엇을 전체 universe로 볼 것인가"다. 거래소 원본 파일에는 보통 보통주 회사뿐 아니라 ETF, ETN, fund, depositary receipt, preference share, debt instrument, warrant, suspended/delisted-like row, 중복 instrument가 섞인다.

UK에서 확인한 포인트:

- LSE instruments report의 전체 equity-like 행은 3,902까지 나왔지만, 여기에는 ETF/펀드/비회사성 instrument가 섞여 있었다.
- `--lse-company-shares-only`를 적용하면 listed operating company 중심으로 줄어들었다.
- `--lse-uk-incorporated-only`는 UK 법인만 남기므로 너무 좁다. LSE 상장사는 해외 법인도 포함하므로 "LSE listed companies"를 만들 때는 UK incorporated 필터를 기본값으로 두면 안 된다.
- issuer 수와 instrument row 수는 다르다. 한 회사가 여러 share class, currency line, market segment를 가질 수 있다.

다른 국가에도 적용할 원칙:

- 거래소 원본 universe는 raw로 보존한다.
- 스캔 대상 universe는 별도 필터로 만든다.
- ETF/ETN/fund 제거 기준과 foreign issuer 포함 여부를 CLI 옵션으로 분리한다.
- 전체 목표 수는 "raw row", "company share row", "issuer dedup row" 중 무엇인지 로그에 명확히 찍는다.

## 2. 공식 재무와 fallback 재무를 상태로 분리한다

캐시 성공 여부를 단순히 row count로 보면 문제를 놓친다. 각 row는 어떤 경로로 재무가 채워졌는지 상태를 가져야 한다.

UK에서 유효했던 상태:

- `official_fundamentals_loaded`: NSM/ESEF 같은 공식 공시에서 파싱 성공
- `fallback_fundamentals_loaded`: Yahoo fundamentals-timeseries 등 보조 소스에서 채움
- `manual_fundamentals_loaded`: 자동 소스로는 실패했지만 수동 검증 CSV로 채움
- `unavailable_fundamentals`: 청산/상장폐지/공개 재무 부재 등으로 스캔 대상에서 제외해야 하는 항목
- `missing_official_fundamentals`: 아직 해결되지 않은 진짜 실패

다른 국가에도 적용할 원칙:

- `missing`을 최종 상태로 방치하지 않는다.
- 자동 공식 소스 실패 후 fallback을 붙인다.
- fallback도 실패하면 수동 override 또는 unavailable marker로 귀결시킨다.
- range scan에서는 `missing_official_fundamentals`와 `unavailable_fundamentals`를 제외한다.

## 3. "공식 coverage"와 "스캔 가능성"은 다르다

공식 공시에서 못 가져온다고 스캔 불가능한 것은 아니다. 투자용 필터에서 필요한 것은 보통 다음 값이다.

- price
- cash or liquid funds
- interest-bearing debt
- liabilities
- equity
- shares
- revenue
- operating income
- net income
- report currency and quote currency

UK에서 706개 공식 누락 중 대부분은 Yahoo fundamentals-timeseries로 순현금/재무 스캔 가능 상태가 됐다. 즉 공식 coverage와 캐시 실용 coverage는 별도 지표로 봐야 한다.

다른 국가에도 적용할 원칙:

- 공식 공시 파서 coverage와 최종 스캔 coverage를 따로 집계한다.
- `coverage` object에 필드별 채움 여부를 저장한다.
- net cash 계산에 필요한 최소 필드는 cash/liquid funds, debt, shares다.
- liabilities ratio와 debt ratio는 equity가 0 또는 음수일 때도 값의 의미를 조심해서 해석해야 한다.

## 4. Fallback에는 ticker alias와 primary listing 탐색이 필요하다

거래소 ticker가 Yahoo/외부 데이터 ticker와 1:1로 맞지 않는 경우가 많다.

UK에서 확인한 예:

- LSE ticker가 Yahoo `.L`에서 404일 수 있다.
- secondary listing 회사는 본국 primary symbol에서 재무가 제공될 수 있다.
- 회사명 변경/재상장으로 과거 ticker와 현재 ticker가 다를 수 있다.
- 일부 AIM/LSE 종목은 Yahoo search가 엉뚱한 심볼을 반환하거나 숫자 재무가 비어 있다.

다른 국가에도 적용할 원칙:

- ticker normalization과 alias table을 별도 계층으로 둔다.
- exchange ticker, Yahoo ticker, primary listing ticker, company registry id를 구분한다.
- fallback은 "search result가 있었다"가 아니라 "필수 numeric fundamentals가 있었다"로 성공 판단한다.
- audit에 fallback symbol과 source URL을 남긴다.

## 5. 수동 override CSV는 마지막 안전장치로 둔다

자동 수집만으로 100%를 강제하면 비정상 issuer 몇 개 때문에 전체 파이프라인이 불안정해진다. 수동 override CSV는 작은 잔여 실패를 처리하는 장치로 유효하다.

UK에서 만든 패턴:

- `data/uk_manual_fundamentals_overrides.csv`: 자동 소스 실패 종목의 재무 수치 직접 보정
- `data/uk_unavailable_fundamentals.csv`: 청산/가용 재무 없음 종목을 명시적으로 표시
- 빌더 마지막 단계에서 missing record에만 적용
- 기존 official/fallback 성공 record는 기본적으로 덮어쓰지 않음
- 필요할 때만 `--manual-fundamentals-override-existing` 같은 명시 옵션으로 덮어쓰기 허용

다른 국가에도 적용할 원칙:

- 수동 CSV는 국가별로 분리한다.
- 수동 보정은 source, source_file, notes를 반드시 남긴다.
- 숫자는 원 단위/천 단위/백만 단위를 명확히 변환해서 저장한다.
- 수동 보정은 적은 수의 예외 처리용이지, 주 수집 경로가 되면 안 된다.

## 6. unavailable은 실패가 아니라 명시적 제외 상태다

상장 DB에는 청산, 거래정지, 상장폐지 절차, reverse takeover shell, cash shell, 계정 미제출 회사가 섞일 수 있다. 이런 종목은 재무가 없거나 투자 필터 의미가 약하다.

UK에서 3개는 `unavailable_fundamentals`로 처리했다. 이 상태는 다음 의미를 가진다.

- 캐시에는 row를 유지한다.
- audit에는 왜 제외됐는지 남긴다.
- range scan 결과에는 나오지 않는다.
- missing count에는 잡히지 않는다.

다른 국가에도 적용할 원칙:

- "데이터 없음"과 "데이터가 없어야 정상인 상태"를 구분한다.
- unavailable reason을 사람이 읽을 수 있게 저장한다.
- scan layer에서 unavailable을 제외한다.
- universe completeness를 볼 때는 unavailable도 accounted row로 계산한다.

## 7. 통화와 단위를 절대 암묵적으로 처리하지 않는다

UK에서는 GBX/GBp quote와 GBP report currency가 섞인다. price는 pence인데 재무제표는 pounds인 경우가 흔하다. 이 문제를 놓치면 PER/PBR/net cash ratio가 100배 틀어진다.

다른 국가에도 적용할 원칙:

- quote currency와 report currency를 별도 필드로 저장한다.
- price major unit을 따로 계산한다.
- PER/PBR/net cash ratio는 통화 단위가 맞을 때만 계산한다.
- 원본 값과 변환 값을 구분한다.
- CSV 수동 보정에도 `report_currency`, `quote_currency`, `price`를 명시한다.

## 8. audit은 실패 분석용 DB다

캐시 출력만으로는 왜 실패했는지 알기 어렵다. audit에는 source별 결과와 실패 이유가 있어야 한다.

UK에서 유용했던 audit 항목:

- universe matching count
- official filing candidate count
- parsed document count
- placeholder count
- Yahoo fallback attempted/filled
- manual override applied
- unavailable marked
- source_file
- coverage object

다른 국가에도 적용할 원칙:

- 각 단계의 input count/output count를 로그와 audit에 모두 남긴다.
- 실패는 예외 메시지보다 "분류 가능한 reason"이 더 중요하다.
- 최종 missing list를 code/name/status/error/source 단위로 쉽게 뽑을 수 있게 한다.

## 9. 테스트는 전체 캐싱이 아니라 작은 표본으로 한다

전체 캐싱은 오래 걸리고 외부 API/네트워크 상태에 민감하다. 구조 변경 검증은 작은 표본 또는 기존 cache에 대한 후처리로 충분해야 한다.

UK에서 사용한 방식:

- 전체 캐싱은 사용자가 실행
- 개발 중에는 current cache에 manual/unavailable 적용을 메모리로 테스트
- 필요할 때만 현재 cache에 후처리 업데이트 적용
- range scan tests로 missing/unavailable 제외를 검증

다른 국가에도 적용할 원칙:

- full rebuild 명령과 smoke test 명령을 분리한다.
- fallback limit 옵션을 둔다.
- current cache 후처리만 하는 경로를 제공한다.
- test fixture로 missing placeholder, manual override, unavailable marker를 각각 검증한다.

## 10. 최종 성공 기준을 명확히 한다

국가별 DB 구축의 목표는 "공식 공시 100% 파싱"이 아니라 "정의한 universe 전체가 accounted 상태가 되는 것"이어야 한다.

권장 성공 기준:

- target universe row count가 로그에 명확하다.
- `missing_official_fundamentals`가 0이다.
- official/fallback/manual/unavailable 합계가 target universe와 일치한다.
- range scan은 missing/unavailable을 제외하고 정상 record만 반환한다.
- net cash, liabilities ratio, debt ratio, PER/PBR 필터가 캐시 전체에서 예외 없이 동작한다.
- 남은 수동/unavailable 항목은 사람이 검토 가능한 CSV로 관리된다.

